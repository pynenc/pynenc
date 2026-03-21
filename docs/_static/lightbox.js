/* === Pynenc Docs — Lightweight Image Lightbox === */
(function () {
  "use strict";

  if (window.matchMedia("(prefers-reduced-motion: reduce)").matches) return;

  /* ── Build overlay DOM ── */
  var overlay = document.createElement("div");
  overlay.className = "pynenc-lightbox";
  overlay.setAttribute("role", "dialog");
  overlay.setAttribute("aria-modal", "true");
  overlay.setAttribute("aria-label", "Image preview");

  var img = document.createElement("img");
  img.className = "pynenc-lightbox__img";
  img.alt = "";

  var close = document.createElement("button");
  close.className = "pynenc-lightbox__close";
  close.setAttribute("aria-label", "Close image preview");
  close.innerHTML = "&times;";

  overlay.appendChild(img);
  overlay.appendChild(close);
  document.body.appendChild(overlay);

  /* ── Open ── */
  function open(src, alt) {
    img.src = src;
    img.alt = alt || "";
    overlay.classList.add("active");
    document.body.style.overflow = "hidden";
    close.focus();
  }

  /* ── Close ── */
  function dismiss() {
    overlay.classList.remove("active");
    document.body.style.overflow = "";
    img.src = "";
  }

  /* Close on overlay click (but not on the image itself) */
  overlay.addEventListener("click", function (e) {
    if (e.target !== img) dismiss();
  });

  /* Close button */
  close.addEventListener("click", function (e) {
    e.stopPropagation();
    dismiss();
  });

  /* ESC key */
  document.addEventListener("keydown", function (e) {
    if (e.key === "Escape" && overlay.classList.contains("active")) {
      dismiss();
    }
  });

  /* ── Attach to all article images ── */
  function attach() {
    var images = document.querySelectorAll(
      "article img:not(.pynenc-lightbox-ready):not(.pynenc-spin-accent):not(.pynenc-float):not(.pynenc-peek):not(.hero-logo)"
    );
    images.forEach(function (el) {
      el.classList.add("pynenc-lightbox-ready");
      el.style.cursor = "zoom-in";
      el.addEventListener("click", function (e) {
        e.preventDefault();
        open(el.src, el.alt);
      });
    });
  }

  /* Run on DOMContentLoaded and also observe for late-loaded images */
  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", attach);
  } else {
    attach();
  }

  var mo = new MutationObserver(function () {
    attach();
  });
  mo.observe(document.body, { childList: true, subtree: true });
})();
