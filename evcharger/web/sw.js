// Uygulamayi ana ekrana eklenebilir kilan minimal servis calisani.
//
// Bilerek "once ag" davraniyor: sarj durumu canli veridir, onbellekten
// eski bir ekran gostermek yanlis bilgi vermek olur. Onbellek yalnizca
// sunucuya hic ulasilamadiginda arayuzun acilabilmesi icin var.

const CACHE = "ev-sarj-v1";
const SHELL = ["/", "/manifest.json", "/icon.svg"];

self.addEventListener("install", (event) => {
  event.waitUntil(caches.open(CACHE).then((c) => c.addAll(SHELL)).then(() => self.skipWaiting()));
});

self.addEventListener("activate", (event) => {
  event.waitUntil(
    caches.keys()
      .then((keys) => Promise.all(keys.filter((k) => k !== CACHE).map((k) => caches.delete(k))))
      .then(() => self.clients.claim())
  );
});

self.addEventListener("fetch", (event) => {
  const { request } = event;
  // API ve WebSocket trafigi asla onbelleklenmez.
  if (request.method !== "GET" || new URL(request.url).pathname.startsWith("/api/")) return;

  event.respondWith(
    fetch(request)
      .then((response) => {
        const copy = response.clone();
        caches.open(CACHE).then((c) => c.put(request, copy)).catch(() => {});
        return response;
      })
      .catch(() => caches.match(request).then((hit) => hit || caches.match("/")))
  );
});
