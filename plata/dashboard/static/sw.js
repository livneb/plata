// Service worker for Plata: handles incoming Web Push events.
// Registered by base.html on page load if push is supported.

self.addEventListener('install', () => self.skipWaiting());
self.addEventListener('activate', (e) => e.waitUntil(self.clients.claim()));

self.addEventListener('push', (event) => {
  let data = {};
  try { data = event.data ? event.data.json() : {}; } catch { data = { title: 'Plata', body: event.data ? event.data.text() : '' }; }
  const title = data.title || 'Plata';
  const options = {
    body: data.body || '',
    icon: '/static/icon.svg',
    badge: '/static/icon.svg',
    tag: data.tag || 'plata',
    data: { url: data.url || '/' },
    requireInteraction: !!data.requireInteraction,
  };
  event.waitUntil(self.registration.showNotification(title, options));
});

self.addEventListener('notificationclick', (event) => {
  event.notification.close();
  const url = (event.notification.data && event.notification.data.url) || '/';
  event.waitUntil(
    self.clients.matchAll({ type: 'window' }).then((wins) => {
      for (const w of wins) {
        if (w.url.includes(url) && 'focus' in w) return w.focus();
      }
      if (self.clients.openWindow) return self.clients.openWindow(url);
    })
  );
});
