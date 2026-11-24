// Usage tracking shared by the listener and speaker pages. Reports to the
// usage function at /api/usage/* (see RosettaCastProxyServer/firestore/
// functions/usage.js), which identifies the browser by a cookie it sets.
//
//   const usage = new UsageTracker("listener");
//   usage.open(speakerCode, language);   // on connect -- closes any open row first
//   usage.setLanguage(language);         // on a language switch
//   usage.close("disconnected");         // on disconnect
//
// A heartbeat every 5 minutes keeps the row live; an extra one goes out when
// the page is hidden (the usual start of a silent exit on a phone), and a
// goodbye when the page is closed. Nothing here may break the page, so every
// failure is swallowed.
(function () {
  const BASE = "/api/usage/";
  const HEARTBEAT_MS = 5 * 60 * 1000;

  const lang2 = (lang) => (lang ?? "").split("-")[0].toLowerCase();

  // sendBeacon survives the page closing; it posts the JSON as text/plain
  function beacon(action, body) {
    try {
      if (navigator.sendBeacon?.(BASE + action, JSON.stringify(body))) return;
    } catch {}
    post(action, body, true);
  }

  async function post(action, body, keepalive = false) {
    try {
      const res = await fetch(BASE + action, {
        method: "POST", credentials: "same-origin", keepalive,
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(body),
      });
      return res;
    } catch (err) {
      console.warn(`usage ${action} failed:`, err.message);
      return null;
    }
  }

  class UsageTracker {
    constructor(role) {
      this.role = role;
      this.id = null;
      this.speakerCode = "";
      this.language = "";
      this.timer = null;
      this.gen = 0;              // bumped by every close(), so a slow start() knows it was superseded
      this.restorable = false;   // closed by pagehide, reopen if the page comes back

      document.addEventListener("visibilitychange", () => {
        if (!this.id) return;
        if (document.visibilityState === "hidden") beacon("beat", this.payload());
        else this.beat();
      });
      window.addEventListener("pagehide", () => {
        if (!this.id) return;
        this.close("page-closed");
        this.restorable = true;
      });
      // Back/forward cache: the page was frozen, not closed
      window.addEventListener("pageshow", (e) => {
        if (e.persisted && this.restorable) this.open(this.speakerCode, this.language);
      });
    }

    payload(extra) {
      return { id: this.id, language: this.language, ...extra };
    }

    async open(speakerCode, language) {
      this.close("switched");
      this.restorable = false;
      this.speakerCode = speakerCode;
      this.language = lang2(language);
      if (!speakerCode) return;

      const gen = this.gen;
      const res = await post("start", {
        role: this.role, speakerCode, language: this.language,
        touch: navigator.maxTouchPoints > 1,
      });
      if (!res?.ok) return;
      let id;
      try { id = (await res.json()).id; } catch { return; }
      // A later open() or close() ran while this one was waiting: the row it
      // opened is already out of date
      if (gen !== this.gen) {
        beacon("end", { id, language: this.language, reason: "switched" });
        return;
      }
      this.id = id;
      this.timer = setInterval(() => this.beat(), HEARTBEAT_MS);
    }

    setLanguage(language) {
      const next = lang2(language);
      if (next === this.language) return;
      this.language = next;
      this.beat();
    }

    async beat() {
      if (!this.id) return;
      const id = this.id;
      const res = await post("beat", this.payload());
      // 409: the row was closed (or the cookie lost) -- start a fresh one
      if (res?.status === 409 && this.id === id) {
        this.id = null;
        this.open(this.speakerCode, this.language);
      }
    }

    close(reason) {
      this.gen++;
      clearInterval(this.timer);
      this.timer = null;
      if (this.id) beacon("end", this.payload({ reason }));
      this.id = null;
    }
  }

  window.UsageTracker = UsageTracker;
})();
