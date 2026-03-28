const LIVE_TRACKER_API_URL = "http://127.0.0.1:8080/api/live-heartbeat";
const LIVE_TRACKER_INTERVAL_MS = 20000;

function getLiveTrackerFingerprint() {
  const parts = [
    navigator.userAgent,
    navigator.language,
    navigator.platform,
    screen.width,
    screen.height,
    window.devicePixelRatio,
    Intl.DateTimeFormat().resolvedOptions().timeZone
  ];

  return btoa(parts.join("|")).replace(/=+$/, "");
}

function sendLiveHeartbeat() {
  const payload = {
    page: document.body?.dataset.livePage || document.title || "unknown",
    pathName: window.location.pathname,
    title: document.title,
    fingerprint: getLiveTrackerFingerprint(),
    browserName: navigator.userAgent,
    platform: navigator.platform
  };

  fetch(LIVE_TRACKER_API_URL, {
    method: "POST",
    headers: {
      "Content-Type": "application/json"
    },
    keepalive: true,
    body: JSON.stringify(payload)
  }).catch(() => {});
}

sendLiveHeartbeat();
window.setInterval(sendLiveHeartbeat, LIVE_TRACKER_INTERVAL_MS);
document.addEventListener("visibilitychange", () => {
  if (document.visibilityState === "visible") {
    sendLiveHeartbeat();
  }
});
