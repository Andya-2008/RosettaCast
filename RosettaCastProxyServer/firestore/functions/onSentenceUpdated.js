// firestore/functions/onSentenceUpdated.js
import { onDocumentUpdated } from "firebase-functions/v2/firestore";
import { TranslationServiceClient } from "@google-cloud/translate";
import { getFirestore } from "firebase-admin/firestore";

const translationClient = new TranslationServiceClient();
const PROJECT_ID = process.env.GCLOUD_PROJECT;

async function getRequestedLanguages(db, speaker, sourceLang) {
  const snap = await db.collection("language_requests")
    .where("speaker", "==", speaker)
    .get();

  const languages = new Set();

  // English is always included by default
  languages.add("en");

  const now = Date.now();

  for (const doc of snap.docs) {
    const data = doc.data();
    const expiresAt = data.expiresAt?.toMillis?.() ?? 0;
    if (expiresAt < now) continue;
    if (data.language) {
      languages.add(data.language);
    }
  }

  // Remove the source language — but always keep English
  if (sourceLang !== "en") {
    languages.delete(sourceLang);
  }

  return [...languages];
}

export const onSentenceUpdated = onDocumentUpdated(
  "speech_sentences/{docId}",
  async (event) => {
    const db = getFirestore();
    const before = event.data.before.data();
    const after  = event.data.after.data();

    // Only re-translate if the text actually changed
    if (before.text === after.text) return;

    const sentenceId = event.params.docId;
    const { sessionId, speaker, sourceLang, text, timestamp } = after;

    console.log(`Sentence updated | sentenceId=${sentenceId} | text="${text.slice(0, 80)}"`);

    const targetLanguages = await getRequestedLanguages(db, speaker, sourceLang);

    if (targetLanguages.length === 0) {
      console.log(`No target languages for speaker=${speaker}, skipping`);
      return;
    }

    const results = await Promise.all(
      targetLanguages
        .filter((targetLang) => targetLang !== sourceLang)
        .map(async (targetLang) => {
          const [response] = await translationClient.translateText({
            parent: `projects/${PROJECT_ID}/locations/global`,
            contents: [text],
            mimeType: "text/plain",
            sourceLanguageCode: sourceLang,
            targetLanguageCode: targetLang,
          });
          return {
            targetLang,
            translatedText: response.translations[0].translatedText,
          };
        })
    );

    const translations = { [sourceLang]: text };
    for (const { targetLang, translatedText } of results) {
      translations[targetLang] = translatedText;
    }

    const now = Date.now();

    // Look up by sentenceId — reliable, no timestamp type mismatch issues
    const existingSnap = await db
      .collection("speech_translations")
      .where("sentenceId", "==", sentenceId)
      .limit(1)
      .get();

    if (!existingSnap.empty) {
      await existingSnap.docs[0].ref.update({
        translations,
        processedAt: now,
      });
      console.log(`Re-translated (merge) | sentenceId=${sentenceId} | ${sourceLang} -> [${targetLanguages.join(", ")}]`);
    } else {
      // No existing translation found — write a new one
      await db.collection("speech_translations").add({
        sessionId,
        sentenceId,
        speaker,
        sourceLang,
        translations,
        timestamp,
        processedAt: now,
        expiresAt: new Date(now + 24 * 60 * 60 * 1000),
      });
      console.log(`Translated (new) | sentenceId=${sentenceId} | ${sourceLang} -> [${targetLanguages.join(", ")}]`);
    }
  }
);