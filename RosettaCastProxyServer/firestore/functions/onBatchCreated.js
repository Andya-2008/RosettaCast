// firestore/functions/onBatchCreated.js
import { onDocumentCreated } from "firebase-functions/v2/firestore";
import { getFirestore } from "firebase-admin/firestore";
import { GoogleGenerativeAI } from "@google/generative-ai";

const genAI = new GoogleGenerativeAI(process.env.GEMINI_API_KEY);
const model = genAI.getGenerativeModel({ model: "gemini-2.5-flash-lite" });

function normalize(text) {
  return text.toLowerCase().replace(/[^a-z0-9\s]/g, "").replace(/\s+/g, " ").trim();
}

export const onBatchCreated = onDocumentCreated(
  {
    document: "fragment_batches/{docId}",
    secrets: ["GEMINI_API_KEY"],
  },
  async (event) => {
    const db = getFirestore();
    const batch = event.data?.data();
    if (!batch || batch.status !== "pending") return;

    const { sessionId, speaker, sourceLang, fragments } = batch;
    const batchRef = event.data.ref;
    await batchRef.update({ status: "processing" });

    const lastSentenceSnap = await db
      .collection("speech_sentences")
      .where("sessionId", "==", sessionId)
      .orderBy("timestamp", "desc")
      .limit(1)
      .get();

    const lastSentence =
      lastSentenceSnap.empty ? null : lastSentenceSnap.docs[0].data();

    const rawText = fragments
      .sort((a, b) => a.timestamp - b.timestamp)
      .map((f) => f.text)
      .join(" ");

    const priorContext = lastSentence
      ? `Previous sentence (may be incomplete — include it if the new fragments continue the same thought):
"${lastSentence.text}"
`
      : "";

    const prompt = `You are a transcription editor for a live sermon from the pulpit of the Church of Jesus Christ of Latter-Day Saints.
The source language is: ${sourceLang}
${priorContext}The following is a new sequence of raw speech-to-text fragments in ${sourceLang}.
They may contain repeated words, filler words (um, uh, and equivalents in ${sourceLang}), incomplete thoughts
from mid-sentence STT restarts, or minor grammatical errors.  There might be transcription errors of common LDS names, places, and practices.
Your job:
1. If the previous sentence (if provided) appears to be an incomplete thought that continues
   into these fragments, merge them into one or more complete sentences
2. Otherwise, treat the fragments as a fresh continuation and clean them on their own
3. Remove filler words and false starts
4. Fix grammar.  There are no punctuations.  Add punctuation where it makes most sense.
5. Preserve the speaker's original meaning and intent, including making corrections for transcription errors of LDS specific words.
6. Do NOT add content, elaborate, or summarize — only clean what is there
7. Return ONLY the final cleaned sentence(s) in ${sourceLang}, no explanation, no preamble, no translation
New fragments:
"${rawText}"`;

    const result = await model.generateContent(prompt);
    const polishedText = result.response.text().trim();
    const now = Date.now();

    if (lastSentence && !lastSentenceSnap.empty) {
      const lastDocRef = lastSentenceSnap.docs[0].ref;
      const lastPolished = lastSentence.text;

      // Normalize both texts — strip punctuation and lowercase before comparing
      // For non-Latin scripts, just check if new text contains a significant
      // portion of the last sentence
      const lastNorm = normalize(lastPolished).slice(0, 40);
      const newNorm = normalize(polishedText);

      // For non-Latin languages (Korean, Chinese, Japanese etc.), use raw text comparison
      const isLatinScript = /^[a-zA-Z]/.test(lastPolished.trim());
      let wasMerged = false;

      if (isLatinScript) {
        wasMerged = lastNorm.length > 5 && newNorm.includes(lastNorm);
      } else {
        // For non-Latin: check if the new polished text starts with or contains
        // the first 10 characters of the last sentence
        const lastPrefix = lastPolished.trim().slice(0, 10);
        wasMerged = lastPrefix.length > 2 && polishedText.includes(lastPrefix);
      }

      if (wasMerged) {
        await lastDocRef.update({
          text: polishedText,
          rawText: (lastSentence.rawText ? lastSentence.rawText + " " : "") + rawText,
          fragmentCount: (lastSentence.fragmentCount ?? 1) + fragments.length,
          mergedAt: now,
          mergedFromBatch: batchRef.id,
        });
        console.log(`Merged into previous sentence | session=${sessionId} | lang=${sourceLang}`);
        await batchRef.update({ status: "done" });
        return;
      }
    }

    await db.collection("speech_sentences").add({
      sessionId,
      speaker,
      sourceLang,
      text: polishedText,
      rawText,
      fragmentCount: fragments.length,
      timestamp: fragments[0].timestamp,
      createdAt: now,
      expiresAt: new Date(now + 24 * 60 * 60 * 1000),
    });

    await batchRef.update({ status: "done" });
    console.log(`Polished | session=${sessionId} | lang=${sourceLang} | "${polishedText.slice(0, 60)}..."`);
  }
);