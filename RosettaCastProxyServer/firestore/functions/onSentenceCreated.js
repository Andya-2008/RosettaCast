// firestore/functions/onSentenceCreated.js
import { onDocumentCreated } from "firebase-functions/v2/firestore";
import { TranslationServiceClient } from "@google-cloud/translate";
import { getFirestore } from "firebase-admin/firestore";
import { GoogleGenAI } from "@google/genai";
import { readFileSync } from "fs";

const translationClient = new TranslationServiceClient();
const PROJECT_ID = process.env.GCLOUD_PROJECT;

// TRANSLATION_ENGINE=cloud reverts to Cloud Translation without a rebuild
const TRANSLATION_ENGINE = process.env.TRANSLATION_ENGINE === "cloud" ? "cloud" : "gemini";
const TRANSLATION_MODEL = process.env.TRANSLATION_MODEL ?? "gemini-3.5-flash-lite";
// How many earlier sentences of the same talk the translator is shown
const CONTEXT_SENTENCES = Number(process.env.TRANSLATION_CONTEXT ?? 3);

const LANGUAGE_NAMES = {
  en: "English", ko: "Korean", es: "Spanish", fr: "French", de: "German",
};

/* ======================================================
   Glossary

   138 official term sets in en/ko/fr/es/de, as supplied. Sending all of them
   with every sentence would be ~11KB of prompt per request and would bury the
   handful that matter, so only the terms that actually appear in the sentence
   are injected -- usually none to a few.

   Latin-script terms match on word boundaries: "ward" must not fire inside
   "awkward" or "toward". Korean has no such boundaries, so it matches as a
   substring, which is what you want for an agglutinative language where 회복
   legitimately appears inside 회복하다.
====================================================== */
const GLOSSARY_PATH = new URL("./glossary.csv", import.meta.url);
const MAX_GLOSSARY_TERMS = Number(process.env.MAX_GLOSSARY_TERMS ?? 12);
const NON_LATIN = /[\u3131-\uD79D\u3040-\u30FF\u4E00-\u9FFF]/;

function parseCsv(text) {
  const rows = [];
  let field = "", row = [], quoted = false;
  for (let i = 0; i < text.length; i++) {
    const c = text[i];
    if (quoted) {
      if (c === '"' && text[i + 1] === '"') { field += '"'; i++; }
      else if (c === '"') quoted = false;
      else field += c;
    } else if (c === '"') quoted = true;
    else if (c === ",") { row.push(field); field = ""; }
    else if (c === "\n") { row.push(field); rows.push(row); row = []; field = ""; }
    else if (c !== "\r") field += c;
  }
  if (field || row.length) { row.push(field); rows.push(row); }
  return rows.filter((r) => r.some((cell) => cell.trim()));
}

const GLOSSARY = (() => {
  try {
    const raw = readFileSync(GLOSSARY_PATH, "utf8").replace(/^\uFEFF/, "");
    const [header, ...rows] = parseCsv(raw);
    const langs = header.map((h) => h.trim());
    const entries = rows.map((cells) => {
      const terms = {};
      langs.forEach((lang, i) => {
        const term = (cells[i] ?? "").trim();
        if (term) terms[lang] = term;
      });
      const matchers = Object.entries(terms).map(([lang, term]) => ({
        lang,
        term,
        test: NON_LATIN.test(term)
          ? (text) => text.includes(term)
          : (() => {
              const re = new RegExp(`\\b${term.replace(/[.*+?^${}()|[\]\\]/g, "\\$&")}\\b`, "i");
              return (text) => re.test(text);
            })(),
      }));
      return { terms, matchers };
    });
    console.log(`Glossary loaded: ${entries.length} term sets (${langs.join(", ")})`);
    return entries;
  } catch (err) {
    console.error("Glossary could not be loaded, continuing without it:", err.message);
    return [];
  }
})();

// The terms this sentence actually uses, rendered for the languages wanted
export function glossaryFor(text, sourceLang, targetLanguages) {
  const hits = [];
  for (const entry of GLOSSARY) {
    const candidates = sourceLang
      ? entry.matchers.filter((m) => m.lang === sourceLang)
      : entry.matchers;
    const hit = candidates.find((m) => m.test(text));
    if (!hit) continue;
    const renderings = targetLanguages
      .filter((l) => entry.terms[l] && l !== hit.lang)
      .map((l) => `${l}: ${entry.terms[l]}`);
    if (renderings.length) hits.push({ term: hit.term, line: `${hit.term} -> ${renderings.join(" | ")}` });
  }
  // Longer terms are the more specific ones, so they win the budget
  hits.sort((a, b) => b.term.length - a.term.length);
  const seen = new Set();
  return hits.filter((h) => !seen.has(h.line) && seen.add(h.line)).slice(0, MAX_GLOSSARY_TERMS);
}

const SYSTEM_INSTRUCTION = `You are translating a talk given in a sacrament meeting of The Church of Jesus Christ of Latter-day Saints. It arrives one sentence at a time, as it is spoken.

Rules:
- Translate the ENTIRE new passage you are given, from its first word to its last. It is often several sentences long; translate every one of them.
- Translate ONLY that passage. The earlier passages are context for pronouns, register and terminology -- never re-translate or repeat them.
- Keep the speaker's voice. Korean honorific speech becomes natural, respectful English, not stilted literal phrasing.
- Where the prompt gives required renderings for specific terms, use them exactly.
- Elsewhere use the Church's official terminology rather than everyday equivalents.
- Proper nouns and scripture references keep their standard Church spelling in the target language.
- If a sentence is cut off mid-thought, leave it cut off. Never complete it, and never add anything the speaker did not say.
- Output JSON only. No commentary, no code fences.`;

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
  if (sourceLang && sourceLang !== "en") {
    languages.delete(sourceLang);
  }

  return [...languages];
}

/* ======================================================
   Context: what was said just before this sentence

   A sentence translated alone loses the antecedent of every pronoun and the
   register the speaker has been using, and terminology drifts line to line.
   Showing the translator the last few sentences of the same talk, with the
   translations already published for them, holds all three steady. Failing to
   fetch context is not fatal -- it just means translating without it.
====================================================== */
async function recentContext(db, sessionId) {
  if (CONTEXT_SENTENCES <= 0) return [];
  try {
    const snap = await db.collection("speech_translations")
      .where("sessionId", "==", sessionId)
      .orderBy("timestamp", "desc")
      .limit(CONTEXT_SENTENCES)
      .get();
    return snap.docs
      .map((d) => d.data())
      .reverse()
      .map((d) => ({ sourceLang: d.sourceLang, translations: d.translations ?? {} }));
  } catch (err) {
    console.warn("Context lookup failed, translating without it:", err.message);
    return [];
  }
}

function buildPrompt({ text, targetLanguages, context, sourceLang, glossary }) {
  const history = context.length
    ? "Earlier in this same talk, in order:\n" + context.map((c, i) => {
        const spoken = c.translations?.[c.sourceLang] ?? "";
        const rendered = Object.entries(c.translations ?? {})
          .filter(([lang]) => lang !== c.sourceLang)
          .map(([lang, t]) => `     ${lang}: ${t}`)
          .join("\n");
        return `  ${i + 1}. spoken: ${spoken}${rendered ? "\n" + rendered : ""}`;
      }).join("\n") + "\n\n"
    : "";

  const wanted = targetLanguages
    .map((l) => `"${l}" (${LANGUAGE_NAMES[l] ?? l})`)
    .join(", ");

  const terms = glossary?.length
    ? `Required renderings for terms used in this passage:\n${glossary.map((g) => `  ${g.line}`).join("\n")}\n\n`
    : "";

  return `${history}${terms}New passage to translate (translate all of it, however many sentences it contains):
"""
${text}
"""
${sourceLang
    ? `It is spoken in ${LANGUAGE_NAMES[sourceLang] ?? sourceLang}.`
    : "Identify which language it is spoken in."}

Translate the whole passage into: ${wanted}.
Respond with JSON of the form {"detectedSource":"<code>","translations":{"<code>":"<translation>"}} containing exactly those target codes.`;
}

async function translateWithGemini({ text, targetLanguages, context, sourceLang }) {
  const ai = new GoogleGenAI({ apiKey: process.env.GEMINI_API_KEY });
  const glossary = glossaryFor(text, sourceLang, targetLanguages);
  const response = await ai.models.generateContent({
    model: TRANSLATION_MODEL,
    contents: buildPrompt({ text, targetLanguages, context, sourceLang, glossary }),
    config: {
      systemInstruction: SYSTEM_INSTRUCTION,
      responseMimeType: "application/json",
      temperature: 0,
    },
  });

  const raw = (response.text ?? "").trim().replace(/^```(?:json)?/, "").replace(/```$/, "").trim();
  const parsed = JSON.parse(raw);
  const translations = parsed.translations ?? {};
  const detected = String(parsed.detectedSource ?? sourceLang ?? "en").split("-")[0];

  const got = targetLanguages.filter(
    (l) => typeof translations[l] === "string" && translations[l].trim());
  if (!got.length) throw new Error("model returned no usable translations");

  return {
    translations, detected, glossaryUsed: glossary.length,
    missing: targetLanguages.filter((l) => !got.includes(l)),
  };
}

async function translateWithCloud({ text, targetLanguages, sourceLang, trustSourceLang }) {
  const results = await Promise.all(
    targetLanguages
      .filter((targetLang) => !trustSourceLang || targetLang !== sourceLang)
      .map(async (targetLang) => {
        const [response] = await translationClient.translateText({
          parent: `projects/${PROJECT_ID}/locations/global`,
          contents: [text],
          mimeType: "text/plain",
          // omitted when unknown, so the API detects and reports it back
          ...(trustSourceLang ? { sourceLanguageCode: sourceLang } : {}),
          targetLanguageCode: targetLang,
        });
        const result = response.translations[0];
        return {
          targetLang,
          translatedText: result.translatedText,
          detected: result.detectedLanguageCode,
        };
      })
  );

  const detected = trustSourceLang
    ? sourceLang
    : (results.find((r) => r.detected)?.detected?.split("-")[0] ?? sourceLang);

  const translations = {};
  for (const { targetLang, translatedText } of results) {
    if (targetLang !== detected) translations[targetLang] = translatedText;
  }
  return { translations, detected };
}

export const onSentenceCreated = onDocumentCreated(
  {
    document: "speech_sentences/{docId}",
    secrets: ["GEMINI_API_KEY"],
  },
  async (event) => {
    const db = getFirestore();
    const sentence = event.data?.data();
    if (!sentence) return;

    const sentenceId = event.params.docId;
    const { sessionId, speaker, text, sourceLang, timestamp, langSource } = sentence;

    // Script settles the language where the alphabet gives it away; otherwise
    // sourceLang is only the transcriber's echoed hint and the translator
    // identifies it instead.
    const trustSourceLang = langSource === "script" || !langSource;
    const targetLanguages = await getRequestedLanguages(
      db, speaker, trustSourceLang ? sourceLang : null);

    if (targetLanguages.length === 0) {
      console.log(`No target languages for speaker=${speaker}, storing source only`);
      const now = Date.now();
      await db.collection("speech_translations").add({
        sessionId,
        sentenceId,
        speaker,
        sourceLang,
        translations: { [sourceLang]: text },
        timestamp,
        processedAt: now,
        expiresAt: new Date(now + 24 * 60 * 60 * 1000),
      });
      return;
    }

    let translations;
    let actualSource;
    let engine = TRANSLATION_ENGINE;
    let contextUsed = 0;
    let glossaryUsed = 0;

    if (TRANSLATION_ENGINE === "gemini") {
      try {
        const context = await recentContext(db, sessionId);
        contextUsed = context.length;
        const out = await translateWithGemini({
          text,
          targetLanguages,
          context,
          sourceLang: trustSourceLang ? sourceLang : null,
        });
        translations = out.translations;
        actualSource = out.detected;
        glossaryUsed = out.glossaryUsed;

        if (out.missing.length) {
          console.warn(`Model skipped [${out.missing.join(", ")}], filling those from Cloud Translation`);
          const fill = await translateWithCloud({
            text,
            targetLanguages: out.missing,
            sourceLang: actualSource,
            trustSourceLang: true,
          });
          translations = { ...translations, ...fill.translations };
        }
      } catch (err) {
        // A sentence is never dropped because the model misbehaved
        console.error(`Gemini translation failed (${err.message}) -- falling back to Cloud Translation`);
        engine = "cloud-fallback";
        const out = await translateWithCloud({ text, targetLanguages, sourceLang, trustSourceLang });
        translations = out.translations;
        actualSource = out.detected;
      }
    } else {
      const out = await translateWithCloud({ text, targetLanguages, sourceLang, trustSourceLang });
      translations = out.translations;
      actualSource = out.detected;
    }

    // The spoken text is stored under whichever language it was spoken in
    const stored = { [actualSource]: text };
    for (const [lang, value] of Object.entries(translations)) {
      if (lang !== actualSource && typeof value === "string" && value.trim()) {
        stored[lang] = value.trim();
      }
    }

    const now = Date.now();
    await db.collection("speech_translations").add({
      sessionId,
      sentenceId,
      speaker,
      sourceLang: actualSource,
      translations: stored,
      engine,
      timestamp,
      processedAt: now,
      expiresAt: new Date(now + 24 * 60 * 60 * 1000),
    });

    const into = Object.keys(stored).filter((l) => l !== actualSource).join(", ");
    console.log(`Translated | session=${sessionId} | ${actualSource} -> [${into}] | engine=${engine} | context=${contextUsed} | glossary=${glossaryUsed}`);
  }
);
