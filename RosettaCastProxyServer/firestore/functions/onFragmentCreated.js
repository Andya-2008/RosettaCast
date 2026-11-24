// firestore/functions/onFragmentCreated.js
import { onDocumentCreated } from "firebase-functions/v2/firestore";
import { getFirestore } from "firebase-admin/firestore";

const FRAGMENT_BATCH_SIZE = 3;

export const onFragmentCreated = onDocumentCreated(
  "speech_transcripts/{docId}",
  async (event) => {
    const db = getFirestore();
    const fragment = event.data?.data();
    if (!fragment) return;

    const { sessionId, speaker, text, language, timestamp } = fragment;

    // speaker.html writes a session-start marker with no sessionId; it exists
    // only so the listener can list speakers. Without this guard, .doc(undefined)
    // throws and the invocation fails once per session start.
    if (!sessionId) {
      console.log(`Skipping doc with no sessionId | speaker=${speaker}`);
      return;
    }

    const sourceLang = language?.split("-")[0] ?? "en";
    const bufferRef = db.collection("session_buffers").doc(sessionId);

    await db.runTransaction(async (tx) => {
      const bufferSnap = await tx.get(bufferRef);
      const buffer = bufferSnap.exists
        ? bufferSnap.data()
        : { fragments: [], count: 0, speaker, sourceLang };

      const updatedFragments = [
        ...buffer.fragments,
        { text, timestamp },
      ];
      const updatedCount = updatedFragments.length;

      if (updatedCount >= FRAGMENT_BATCH_SIZE) {
		  
        tx.set(
          db.collection("fragment_batches").doc(),
          {
            sessionId,
            speaker,
            sourceLang,
            fragments: updatedFragments,
            createdAt: Date.now(),
            status: "pending",
			expiresAt: new Date(Date.now() + 24 * 60 * 60 * 1000), // add this
          }
        );
        tx.set(bufferRef, {
          fragments: [],
          count: 0,
          speaker,
          sourceLang,
          lastFlushedAt: Date.now(),
		  expiresAt: new Date(Date.now() + 24 * 60 * 60 * 1000), // add this
        });
      } else {
        tx.set(bufferRef, {
          fragments: updatedFragments,
          count: updatedCount,
          speaker,
          sourceLang,
		  expiresAt: new Date(Date.now() + 24 * 60 * 60 * 1000), // add this
        });
      }
    });

    console.log(`📝 Fragment buffered | session=${sessionId}`);
  }
);