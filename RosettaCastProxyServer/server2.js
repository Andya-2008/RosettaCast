import WebSocket, { WebSocketServer } from "ws";
import speech from "@google-cloud/speech";
import { Firestore } from "@google-cloud/firestore";
import crypto from "crypto";

/* ======================================================
   Setup — STT v2 with auto language detection
   Deploy as a separate Cloud Run service:
   gcloud run deploy rosettacast2 --source . --region us-central1 --allow-unauthenticated --set-env-vars GCLOUD_PROJECT=rosettacast
====================================================== */

const db = new Firestore();
const PROJECT_ID = process.env.GCLOUD_PROJECT || "rosettacast";
/*
const { SpeechClient } = speech.v2;

const speechClient = new SpeechClient();
*/
const { SpeechClient } = speech.v2;
const speechClient = new SpeechClient({
  projectId: "114230502988",
});

const PORT = process.env.PORT || 8080;
const wss = new WebSocketServer({ port: PORT });

const INTERIM_FLUSH_WORD_THRESHOLD = 15;
const RECOGNIZER = `projects/114230502988/locations/global/recognizers/_`;
//const RECOGNIZER = `projects/${PROJECT_ID}/locations/global/recognizers/_`;

console.log(`Rosetta Cast v2 listening on port ${PORT} | recognizer=${RECOGNIZER}`);

/* ======================================================
   Helper: word count
====================================================== */

function wordCount(text) {
  return text.trim().split(/\s+/).filter(Boolean).length;
}

/* ======================================================
   Helper: Firestore write
====================================================== */

async function storeFinalTranscript({ sessionId, speaker, text, language }) {
  const now = Date.now();
  return db.collection("speech_transcripts").add({
    sessionId,
    speaker,
    text,
    language,
    timestamp: now,
    expiresAt: new Date(now + 24 * 60 * 60 * 1000),
  });
}

/* ======================================================
   WebSocket Connection
====================================================== */

wss.on("connection", (ws) => {
  console.log("Client connected");

  const sessionId = crypto.randomUUID();

  let recognizeStream = null;
  let initialized = false;

  // Session-scoped metadata
  let sessionSpeaker = "unknown";

  // Interim flush tracking — reset per isFinal
  let lastFlushedWordCount = 0;

  // Auto-disconnect after 130 minutes to prevent runaway charges
  const MAX_SESSION_MS = 130 * 60 * 1000;
  const sessionTimeout = setTimeout(() => {
    console.log(`Session timeout (130 min) | session=${sessionId} | speaker=${sessionSpeaker}`);
    try { recognizeStream?.end(); } catch {}
    try { ws.close(); } catch {}
  }, MAX_SESSION_MS);

  ws.on("message", (msg) => {
    /* --------------------------------------------------
       1. First message: config JSON
    -------------------------------------------------- */
    if (!initialized) {
      let cfg;

      try {
        cfg = JSON.parse(msg.toString());
      } catch {
        console.error("Invalid JSON config");
        ws.close();
        return;
      }

      if (cfg.type !== "config") {
        console.error("Missing config type");
        ws.close();
        return;
      }

      sessionSpeaker =
        typeof cfg.speaker === "string" && cfg.speaker.length > 0
          ? cfg.speaker
          : "unknown";

      console.log(`Starting STT v2 | session=${sessionId} | speaker=${sessionSpeaker}`);

      /* --------------------------------------------------
         2. Start Google STT v2 streamingRecognize
         - No languageCode needed — auto-detects from audio
         - Supports up to 3 language candidates
      -------------------------------------------------- */
      recognizeStream = speechClient.streamingRecognize();

      // First write must be the config request
      recognizeStream.write({
        recognizer: RECOGNIZER,
        streamingConfig: {
          config: {
            explicitDecodingConfig: {
              encoding: "LINEAR16",
              sampleRateHertz: 16000,
              audioChannelCount: 1,
            },
            languageCodes: ["en-US", "ko-KR", "es-US", "fr-FR", "de-DE"],
            model: "long",
            features: {
              enableAutomaticPunctuation: false,
            },
          },
          interimResults: true,
        },
      });

      recognizeStream
        .on("data", (data) => {
          const result = data.results?.[0];
          const alt = result?.alternatives?.[0];
          if (!alt?.transcript) return;

          const transcript = alt.transcript;
          // v2 returns detected language on result, not alt
          const detectedLanguage = result.languageCode || "en-US";

          if (result.isFinal) {
            const words = transcript.trim().split(/\s+/);
            const newChunk = words.slice(lastFlushedWordCount).join(" ");
            lastFlushedWordCount = 0;

            console.log(`Final | lang=${detectedLanguage} | "${newChunk.slice(0, 60)}"`);

            const payload = {
              text: newChunk,
              isFinal: true,
              language: detectedLanguage,
              speaker: sessionSpeaker,
            };
            try { ws.send(JSON.stringify(payload)); } catch {}

            storeFinalTranscript({
              sessionId,
              speaker: sessionSpeaker,
              text: newChunk,
              language: detectedLanguage,
            }).catch((err) =>
              console.error("Firestore write failed:", err.message)
            );

          } else {
            // Interim result — flush every 15 new words
            const totalWords = wordCount(transcript);
            const newWordsSinceFlush = totalWords - lastFlushedWordCount;

            if (newWordsSinceFlush >= INTERIM_FLUSH_WORD_THRESHOLD) {
              const words = transcript.trim().split(/\s+/);
              const newChunk = words.slice(lastFlushedWordCount).join(" ");
              lastFlushedWordCount = totalWords;

              console.log(`Interim flush | words=${totalWords} | "${newChunk}"`);

              const interimPayload = {
                text: newChunk,
                isFinal: true,
                language: detectedLanguage,
                speaker: sessionSpeaker,
              };
              try { ws.send(JSON.stringify(interimPayload)); } catch {}

              storeFinalTranscript({
                sessionId,
                speaker: sessionSpeaker,
                text: newChunk,
                language: detectedLanguage,
              }).catch((err) =>
                console.error("Firestore write failed (interim):", err.message)
              );

            } else {
              // Below threshold — send to client but do not write to Firestore
              const interimPayload = {
                text: transcript,
                isFinal: false,
                language: detectedLanguage,
                speaker: sessionSpeaker,
              };
              try { ws.send(JSON.stringify(interimPayload)); } catch {}
            }
          }
        })
        .on("error", (err) => {
          console.error("Google STT v2 error:", err);
          try { ws.send(JSON.stringify({ error: err.message })); } catch {}
          try { ws.close(); } catch {}
        });

      initialized = true;
      return;
    }

    /* --------------------------------------------------
       3. After init: audio only (raw PCM)
       v2 wraps audio in { audio: { content: ... } }
    -------------------------------------------------- */
    if (recognizeStream) {
      recognizeStream.write({ audio: { content: msg } });
    }
  });

  ws.on("close", () => {
    clearTimeout(sessionTimeout);
    console.log(`Client disconnected | session=${sessionId}`);
    try { recognizeStream?.end(); } catch {}
  });

  ws.on("error", (err) => {
    console.error("WebSocket error:", err);
    try { recognizeStream?.end(); } catch {}
  });
});