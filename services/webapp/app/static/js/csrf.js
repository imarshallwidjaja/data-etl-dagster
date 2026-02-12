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
    var method = "GET";

    // init.method takes precedence when provided.
    if (init.method) {
      method = init.method;
    } else if (input instanceof Request && input.method) {
      // If input is a Request object, the method lives on input.method.
      method = input.method;
    }

    method = method.toUpperCase();
    if (method !== "GET" && method !== "HEAD" && method !== "OPTIONS") {
      var token = getCsrfToken();
      if (token) {
        // Preserve Request headers when init.headers is absent.
        var headers;
        if (init.headers) {
          headers = new Headers(init.headers);
        } else if (input instanceof Request) {
          headers = new Headers(input.headers);
        } else {
          headers = new Headers();
        }

        if (!headers.has("X-CSRF-Token")) {
          headers.set("X-CSRF-Token", token);
        }

        init.headers = headers;
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
