/* === Pynenc Docs — Playful Logo Interactions === */
(function () {
  "use strict";

  /* Bail out if user prefers reduced motion */
  if (window.matchMedia("(prefers-reduced-motion: reduce)").matches) return;

  /* Derive the logo URL from this script's own absolute src so it works
     at any URL depth (e.g. monitoring/index.html, getting_started/index.html). */
  const _scriptSrc = document.currentScript ? document.currentScript.src : null;
  const LOGO_URL = _scriptSrc
    ? _scriptSrc.replace(/logo-animations\.js(\?.*)?$/, "logo.png")
    : "_static/logo.png";

  /* ── Inject floating accent logos ── */
  function createFloat(cls) {
    const img = document.createElement("img");
    img.src = LOGO_URL;
    img.alt = "";
    img.setAttribute("aria-hidden", "true");
    img.className = "pynenc-float " + cls;
    document.body.appendChild(img);
    return img;
  }

  const floats = [
    createFloat("pynenc-float--top-right"),
    createFloat("pynenc-float--bottom-left"),
    createFloat("pynenc-float--mid-right"),
  ];

  /* ── Inject peek-a-boo logo ── */
  const peek = document.createElement("img");
  peek.src = LOGO_URL;
  peek.alt = "Pynenc";
  peek.className = "pynenc-peek";
  peek.setAttribute("aria-hidden", "true");
  document.body.appendChild(peek);

  /* ── Scroll state ── */
  let lastScroll = 0;
  let peekTimer = null;
  let scrollTicking = false;

  function onScroll() {
    if (scrollTicking) return;
    scrollTicking = true;
    requestAnimationFrame(function () {
      const y = window.scrollY;
      const docHeight =
        document.documentElement.scrollHeight - window.innerHeight;
      const pct = docHeight > 0 ? y / docHeight : 0;

      /* Floating logos: appear after scrolling past 15% */
      floats.forEach(function (f, i) {
        const threshold = 0.15 + i * 0.2;
        f.classList.toggle("visible", pct > threshold);
      });

      /* Peek-a-boo: show when scrolling up quickly */
      const delta = y - lastScroll;
      if (delta < -40) {
        peek.classList.add("peeking");
        clearTimeout(peekTimer);
        peekTimer = setTimeout(function () {
          peek.classList.remove("peeking");
        }, 2500);
      }
      if (y < 50) {
        peek.classList.remove("peeking");
      }

      lastScroll = y;
      scrollTicking = false;
    });
  }

  window.addEventListener("scroll", onScroll, { passive: true });

  /* ── Spin-in accents on h2 headers when they enter viewport ── */
  function setupH2Accents() {
    const headers = document.querySelectorAll("article h2");
    if (!headers.length) return;

    const observer = new IntersectionObserver(
      function (entries) {
        entries.forEach(function (entry) {
          const accent = entry.target.querySelector(".pynenc-spin-accent");
          if (!accent) return;
          accent.classList.toggle("visible", entry.isIntersecting);
        });
      },
      { threshold: 0.3 }
    );

    headers.forEach(function (h) {
      /* Don't double-add */
      if (h.querySelector(".pynenc-spin-accent")) return;
      const img = document.createElement("img");
      img.src = LOGO_URL;
      img.alt = "";
      img.setAttribute("aria-hidden", "true");
      img.className = "pynenc-spin-accent";
      h.prepend(img);
      observer.observe(h);
    });
  }

  /* ── Scroll-reveal for grid items and cards ── */
  function setupScrollReveal() {
    const items = document.querySelectorAll(".sd-card, .sd-grid-item");
    if (!items.length) return;

    const observer = new IntersectionObserver(
      function (entries) {
        entries.forEach(function (entry) {
          if (entry.isIntersecting) {
            entry.target.classList.add("revealed");
          }
        });
      },
      { threshold: 0.1, rootMargin: "0px 0px -40px 0px" }
    );

    items.forEach(function (el) {
      el.classList.add("scroll-reveal");
      observer.observe(el);
    });
  }

  /* ── Hero logo click easter-egg: pop animation ── */
  function setupHeroClick() {
    const hero = document.querySelector(".hero-logo");
    if (!hero) return;
    hero.style.cursor = "pointer";
    hero.addEventListener("click", function () {
      hero.classList.remove("pop");
      /* Force reflow to restart the animation */
      void hero.offsetWidth;
      hero.classList.add("pop");
    });
  }

  /* ── Mini divider logos between horizontal rules ── */
  function setupDividerLogos() {
    const hrs = document.querySelectorAll("article hr");
    hrs.forEach(function (hr) {
      const img = document.createElement("img");
      img.src = LOGO_URL;
      img.alt = "";
      img.setAttribute("aria-hidden", "true");
      img.className = "pynenc-divider";
      /* Random small size variation */
      const size = 24 + Math.floor(Math.random() * 20);
      img.style.height = size + "px";
      hr.after(img);
    });
  }

  /* ── Boot everything once DOM is ready ── */
  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", init);
  } else {
    init();
  }

  function init() {
    setupH2Accents();
    setupScrollReveal();
    setupHeroClick();
    setupDividerLogos();
  }
})();
