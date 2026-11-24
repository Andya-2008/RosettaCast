import functions from "firebase-functions";
import admin from "firebase-admin";

admin.initializeApp();
const db = admin.firestore();

/* =====================================================
   Trigger: NEW fragment created
===================================================== */

/*
gcloud functions deploy onFragmentCreated --gen2 --runtime=nodejs20 --region=us-central1 --trigger-event-filters="type=google.cloud.firestore.document.v1.created" --trigger-event-filters-path-pattern="document=speech_transcripts/{fragmentId}"


*/

export const onFragmentCreated = functions.firestore
  .document("speech_transcripts/{fragmentId}")
  .onCreate(async (snap, context) => {
    // 1️⃣ Read the new fragment
    const fragment = snap.data();
    if (!fragment) return;

    const {
      sessionId,
      speaker = "unknown",
      text,
      language = "en-US",
    } = fragment;

    if (!sessionId || !text) return;

    console.log(
      `🧩 New fragment | session=${sessionId} | speaker=${speaker}`
    );

    // 2️⃣ Load session accumulator
    const sessionRef = db.collection("speech_sessions").doc(sessionId);
    const sessionSnap = await sessionRef.get();

    const pending =
      sessionSnap.exists && sessionSnap.data().pendingFragment
        ? sessionSnap.data().pendingFragment
        : "";

    // 3️⃣ Combine pending + new fragment
    const combined = `${pending} ${text}`.trim();

    // 4️⃣ (Placeholder) sentence split logic
    // For now, just store combined as pending
    await sessionRef.set(
      {
        pendingFragment: combined,
        updatedAt: Date.now(),
      },
      { merge: true }
    );

    // 🔥 Later, this is where you call Gemini / OpenAI
    // and emit completed sentences
  });