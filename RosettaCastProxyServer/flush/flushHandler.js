// cloudrun/flushHandler.js
// Deploy this as a Cloud Run service (or add as a route in your existing Cloud Run app)
import express from "express";
import { getFirestore } from "firebase-admin/firestore";
import { initializeApp, getApps } from "firebase-admin/app";

if (!getApps().length) initializeApp();

const app = express();
app.use(express.json());

app.post("/flush", async (req, res) => {
  const { sessionId, lastTimestamp } = req.body;

  if (!sessionId || !lastTimestamp) {
    return res.status(400).send("Missing sessionId or lastTimestamp");
  }

  const db = getFirestore();
  const bufferRef = db.collection("session_buffers").doc(sessionId);
  const bufferSnap = await bufferRef.get();

  if (!bufferSnap.exists) {
    console.log(`session=${sessionId} buffer gone, skipping`);
    return res.status(200).send("No buffer");
  }

  const buffer = bufferSnap.data();

  // Guard: if a newer fragment arrived after this task was scheduled, 
  // skip — the newer task will handle it
  const actualLast = [...(buffer.fragments ?? [])]
    .sort((a, b) => b.timestamp - a.timestamp)[0];
  const actualLastTimestamp = actualLast?.timestamp ?? 0;

  if (actualLastTimestamp > lastTimestamp) {
    console.log(
      `session=${sessionId} newer fragment arrived (${actualLastTimestamp} > ${lastTimestamp}), skipping`
    );
    return res.status(200).send("Superseded");
  }

  // Guard: buffer already empty (was flushed by another path)
  if (!buffer.count || buffer.count === 0) {
    console.log(`session=${sessionId} already flushed, skipping`);
    return res.status(200).send("Already flushed");
  }

  // Perform the flush
  const now = Date.now();
  const batchRef = db.collection("fragment_batches").doc();

  const firestoreBatch = db.batch();

  firestoreBatch.set(batchRef, {
    sessionId,
    speaker: buffer.speaker,
    sourceLang: buffer.sourceLang,
    fragments: buffer.fragments,
    createdAt: now,
    status: "pending",
    flushedEarly: true,
	expiresAt: new Date(now + 24 * 60 * 60 * 1000), // add this
  });

  firestoreBatch.set(bufferRef, {
    fragments: [],
    count: 0,
    speaker: buffer.speaker,
    sourceLang: buffer.sourceLang,
    lastFlushedAt: now,
	expiresAt: new Date(now + 24 * 60 * 60 * 1000), // add this
  });

  await firestoreBatch.commit();

  console.log(`✅ Flushed session=${sessionId} (${buffer.fragments.length} fragments)`);
  return res.status(200).send("Flushed");
});

const PORT = process.env.PORT || 8080;
app.listen(PORT, () => console.log(`Flush handler listening on ${PORT}`));