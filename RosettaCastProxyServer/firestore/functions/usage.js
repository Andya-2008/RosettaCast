// firestore/functions/usage.js
import { onRequest } from "firebase-functions/v2/https";
import { getFirestore, FieldValue, Timestamp } from "firebase-admin/firestore";
import { createHash, randomBytes } from "crypto";

// Usage tracking for the listener and speaker pages, served at /api/usage/*
// through a Firebase Hosting rewrite so it shares the pages' address.
//
// Each browser is identified by a random secret in a server-set HttpOnly
// cookie. Safari wipes storage that page scripts can write after 7 days
// without a visit, which would make a Sunday-only listener look new every
// week; a cookie set by the server is not subject to that. Firebase Hosting
// passes only the cookie named __session through to functions.
//
// Rows store a hash of the secret, never the secret itself, so the table can
// be readable without letting anyone pose as another browser. Only this
// function writes usage_sessions.
//
//   POST /api/usage/start {role, speakerCode, language, touch} -> {id}
//   POST /api/usage/beat  {id, language}
//   POST /api/usage/end   {id, language, reason}
//
// start opens a row per connection; beat (every 5 min, and whenever the page
// is hidden or the language changes) keeps it live and collects every
// language used; end closes it. A beat or end for a row that is closed or
// belongs to another browser gets 409, and the page opens a fresh row.

const COOKIE = "__session";
const COOKIE_MAX_AGE_S = 365 * 24 * 60 * 60;
const RETENTION_MS = 2 * 365 * 24 * 60 * 60 * 1000;   // rows expire via the TTL policy on expiresAt
const ROLES = new Set(["listener", "speaker"]);

const hashOf = (secret) => createHash("sha256").update(secret).digest("hex").slice(0, 16);
const clean = (v, max) => (typeof v === "string" ? v.trim().slice(0, max) : "");

function readSecret(req) {
  for (const part of (req.headers.cookie ?? "").split(";")) {
    const [name, ...rest] = part.trim().split("=");
    if (name === COOKIE && /^[0-9a-f]{32}$/.test(rest.join("="))) return rest.join("=");
  }
  return null;
}

// Coarse labels only; the full user agent is not stored. iPads ask for the
// desktop site and claim to be Macs, so the page reports whether the screen
// is touch-capable.
function deviceOf(ua, touch) {
  if (/iPhone|iPod/.test(ua)) return "iPhone";
  if (/iPad/.test(ua) || (/Macintosh/.test(ua) && touch)) return "iPad";
  if (/Android/.test(ua)) return "Android";
  if (/Windows/.test(ua)) return "Windows";
  if (/CrOS/.test(ua)) return "Chromebook";
  if (/Macintosh|Mac OS X/.test(ua)) return "Mac";
  return "Other";
}

function browserOf(ua) {
  // In-app browsers first: they also claim to be Safari or Chrome
  const inApp = [
    [/KAKAOTALK/i, "KakaoTalk"], [/NAVER\(inapp/i, "Naver"], [/Line\//, "LINE"],
    [/FBAN|FBAV|FB_IAB/, "Facebook"], [/Instagram/, "Instagram"], [/GSA\//, "Google app"],
  ].find(([re]) => re.test(ua));
  if (inApp) return `In-app (${inApp[1]})`;
  if (/SamsungBrowser/.test(ua)) return "Samsung Internet";
  if (/Edg(e|A|iOS)?\//.test(ua)) return "Edge";
  if (/FxiOS|Firefox\//.test(ua)) return "Firefox";
  if (/CriOS|Chrome\//.test(ua)) return "Chrome";
  if (/Safari\//.test(ua)) return "Safari";
  return "Other";
}

export const usage = onRequest(
  { region: "us-central1", memory: "256MiB", maxInstances: 10, invoker: "public" },
  async (req, res) => {
    res.set("Cache-Control", "no-store");   // keep the CDN from caching a Set-Cookie
    if (req.method !== "POST") return res.status(405).end();

    let body = req.body;
    // sendBeacon posts its JSON as text/plain
    if (typeof body === "string" || Buffer.isBuffer(body)) {
      try { body = JSON.parse(body.toString()); } catch { body = {}; }
    }
    body ??= {};

    const action = req.path.split("/").filter(Boolean).pop();
    const db = getFirestore();
    const rows = db.collection("usage_sessions");
    const language = clean(body.language, 12).split("-")[0].toLowerCase();   // ko-KR -> ko

    if (action === "start") {
      const role = ROLES.has(body.role) ? body.role : "listener";
      const speakerCode = clean(body.speakerCode, 64);
      if (!speakerCode) return res.status(400).json({ error: "speakerCode required" });

      const secret = readSecret(req) ?? randomBytes(16).toString("hex");
      // Re-sent on every start, so the cookie lasts a year from the last visit
      res.cookie(COOKIE, secret, {
        httpOnly: true, secure: true, sameSite: "lax", path: "/", maxAge: COOKIE_MAX_AGE_S * 1000,
      });

      const ua = req.headers["user-agent"] ?? "";
      const now = Timestamp.now();
      const ref = await rows.add({
        role, speakerCode,
        clientUid: hashOf(secret),
        language, languages: language ? [language] : [],
        device: deviceOf(ua, body.touch === true),
        browser: browserOf(ua),
        connectedAt: now, lastSeenAt: now,
        disconnectedAt: null, endReason: null,
        expiresAt: Timestamp.fromMillis(now.toMillis() + RETENTION_MS),
      });
      return res.json({ id: ref.id });
    }

    if (action === "beat" || action === "end") {
      const secret = readSecret(req);
      const id = clean(body.id, 40);
      if (!secret || !id) return res.status(409).end();

      const ref = rows.doc(id);
      const ok = await db.runTransaction(async (tx) => {
        const snap = await tx.get(ref);
        const row = snap.data();
        if (!row || row.clientUid !== hashOf(secret) || row.disconnectedAt) return false;
        const now = Timestamp.now();
        const update = { lastSeenAt: now };
        if (language) {
          update.language = language;
          update.languages = FieldValue.arrayUnion(language);
        }
        if (action === "end") {
          update.disconnectedAt = now;
          update.endReason = clean(body.reason, 32) || "closed";
        }
        tx.update(ref, update);
        return true;
      });
      return ok ? res.status(204).end() : res.status(409).end();
    }

    return res.status(404).end();
  }
);
