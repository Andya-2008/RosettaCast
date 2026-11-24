// firestore/functions/onFlushStaleBuffers.js
import { onSchedule } from "firebase-functions/v2/scheduler";
import { getFirestore } from "firebase-admin/firestore";

// Backstop only. The Cloud Tasks path in onFragmentArrived flushes at ~3 s;
// this sweep exists to catch buffers that path missed (task creation failed,
// flush-handler down, etc). Keep the threshold well above 3 s so the two
// paths cannot flush the same buffer at the same moment and double-write.
const STALE_THRESHOLD_MS = 30000; // flush if no new fragment for 30 seconds

export const onFlushStaleBuffers = onSchedule("every 1 minutes", async () => {
  const db = getFirestore();
  const now = Date.now();

  const staleSnap = await db
    .collection("session_buffers")
    .where("count", ">", 0)
    .get();

  if (staleSnap.empty) return;

  const flushes = [];

  for (const doc of staleSnap.docs) {
    const buffer = doc.data();

    // Check when the last fragment arrived
    const lastFragment = buffer.fragments
      .sort((a, b) => b.timestamp - a.timestamp)[0];

    const lastTimestamp = lastFragment?.timestamp ?? 0;
    const age = now - lastTimestamp;

    if (age >= STALE_THRESHOLD_MS) {
      flushes.push({ ref: doc.ref, buffer });
    }
  }

  if (flushes.length === 0) return;

  const batch = db.batch();

  for (const { ref, buffer } of flushes) {
    // Write the partial batch for AI processing
    const batchRef = db.collection("fragment_batches").doc();
    batch.set(batchRef, {
      sessionId: ref.id,
      speaker: buffer.speaker,
      sourceLang: buffer.sourceLang,
      fragments: buffer.fragments,
      createdAt: now,
      status: "pending",
	  expiresAt: new Date(now + 24 * 60 * 60 * 1000), // add this
      flushedEarly: true,       // flag so you know it was a partial batch
    });

    // Reset the buffer
    batch.set(ref, {
      fragments: [],
      count: 0,
      speaker: buffer.speaker,
      sourceLang: buffer.sourceLang,
      lastFlushedAt: now,
	  expiresAt: new Date(now + 24 * 60 * 60 * 1000), // add this
    });
  }

  await batch.commit();

  console.log(`🔄 Flushed ${flushes.length} stale buffer(s)`);
});