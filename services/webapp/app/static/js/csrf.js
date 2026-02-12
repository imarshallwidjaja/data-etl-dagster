/**
 * CSRF Token Injection for fetch() and HTMX.
 *
 * Reads the token from <meta name="csrf-token" content="..."> and:
 *  - Patches window.fetch to inject X-CSRF-Token on unsafe methods.
 *  - Listens to htmx:configRequest to add the header on HTMX requests.
 */
(function () {
  "use strict";

  function getCsrfToken() {
    var meta = document.querySelector('meta[name="csrf-token"]');
    return meta ? meta.getAttribute("content") : "";
  }

  // --- Patch fetch ---
  var _originalFetch = window.fetch;
  window.fetch = function (input, init) {
    init = init || {};
    var method = (init.method || "GET").toUpperCase();
    if (method !== "GET" && method !== "HEAD" && method !== "OPTIONS") {
      var token = getCsrfToken();
      if (token) {
        // Ensure headers is a plain object (or Headers instance).
        if (init.headers instanceof Headers) {
          if (!init.headers.has("X-CSRF-Token")) {
            init.headers.set("X-CSRF-Token", token);
          }
        } else {
          init.headers = Object.assign({}, init.headers || {});
          if (!init.headers["X-CSRF-Token"]) {
            init.headers["X-CSRF-Token"] = token;
          }
        }
      }
    }
    return _originalFetch.call(this, input, init);
  };

  // --- HTMX integration ---
  document.addEventListener("htmx:configRequest", function (event) {
    var token = getCsrfToken();
    if (token) {
      event.detail.headers["X-CSRF-Token"] = token;
    }
  });
})();
