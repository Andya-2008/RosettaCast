import WebSocket, { WebSocketServer } from "ws";
import speech from "@google-cloud/speech";
import { Firestore } from "@google-cloud/firestore";
import crypto from "crypto";

/* ======================================================
   Setup
   Deploy:
   gcloud run deploy rosettacast --source . --region us-central1 --allow-unauthenticated
   firebase deploy --only functions
====================================================== */

const speechClient = new speech.SpeechClient();
const db = new Firestore();

const PORT = process.env.PORT || 8080;
const wss = new WebSocketServer({ port: PORT });

const INTERIM_FLUSH_WORD_THRESHOLD = 15;

console.log(`Rosetta Cast listening on port ${PORT}`);

/* ======================================================
   Helper: word count
====================================================== */

function wordCount(text) {
  return text.trim().split(/\s+/).filter(Boolean).length;
}

/* ======================================================
   Helper: Firestore write
====================================================== */

async function storeFinalTranscript({
  sessionId,
  speaker,
  text,
  language,
}) {
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
  let sessionLanguage = null;
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

      if (cfg.type !== "config" || !cfg.language) {
        console.error("Missing config or language");
        ws.close();
        return;
      }

      sessionLanguage = cfg.language;
      sessionSpeaker =
        typeof cfg.speaker === "string" && cfg.speaker.length > 0
          ? cfg.speaker
          : "unknown";

      console.log(
        `Starting STT | session=${sessionId} | language=${sessionLanguage} | speaker=${sessionSpeaker}`
      );

      /* --------------------------------------------------
         2. Start Google streamingRecognize
      -------------------------------------------------- */
      recognizeStream = speechClient
        .streamingRecognize({
          config: {
            encoding: "LINEAR16",
            sampleRateHertz: 16000,
            languageCode: sessionLanguage,
            enableAutomaticPunctuation: false,
            model: "latest_long",
          },
          interimResults: true,
        })
        .on("data", (data) => {
          const result = data.results?.[0];
          const alt = result?.alternatives?.[0];
          if (!alt?.transcript) return;

          const transcript = alt.transcript;
          const detectedLanguage = alt.languageCode || sessionLanguage;

          if (result.isFinal) {
			const totalWords = wordCount(transcript);
			const newWordsSinceFlush = totalWords - lastFlushedWordCount;
            // Reset interim tracking for next sentence
            const words = transcript.trim().split(/\s+/);
			
            const newChunk = words.slice(lastFlushedWordCount).join(" ");

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
			lastFlushedWordCount = 0;

          } else {
            // Interim result — flush every 15 new words
            const totalWords = wordCount(transcript);
            const newWordsSinceFlush = totalWords - lastFlushedWordCount;

            if (newWordsSinceFlush >= INTERIM_FLUSH_WORD_THRESHOLD) {
              // Extract only the new words since last flush
              const words = transcript.trim().split(/\s+/);
              const newChunk = words.slice(lastFlushedWordCount).join(" ");

              lastFlushedWordCount = totalWords;

              console.log(`Interim flush | words=${totalWords} | "${newChunk}"`);

              const interimPayload = {
                text: newChunk,
                isFinal: true,  //changed this.
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
				// if not hit threshold.
				const interimPayload = {
                text: transcript,
                isFinal: false,  //changed this.
                language: detectedLanguage,
                speaker: sessionSpeaker,
              };
			  try { ws.send(JSON.stringify(interimPayload)); } catch {}
			}
          }
        })
        .on("error", (err) => {
          console.error("Google STT error:", err);
          try { ws.send(JSON.stringify({ error: err.message })); } catch {}
          try { ws.close(); } catch {}
        });

      initialized = true;
      return;
    }

    /* --------------------------------------------------
       3. After init: audio only (raw PCM)
    -------------------------------------------------- */
    if (recognizeStream) {
      recognizeStream.write(msg);
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