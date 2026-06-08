(function () {
  'use strict';

  function setNavOpen(open) {
    const phone = document.getElementById('phone');
    const btn = document.getElementById('mobileNavBtn');
    const backdrop = document.getElementById('mobileNavBackdrop');
    const sheet = document.getElementById('mobileNavSheet');
    if (!phone || !btn || !backdrop || !sheet) return;

    phone.classList.toggle('nav-open', open);
    btn.setAttribute('aria-expanded', open ? 'true' : 'false');
    backdrop.setAttribute('aria-hidden', open ? 'false' : 'true');
    sheet.setAttribute('aria-hidden', open ? 'false' : 'true');
  }

  function markActiveLink() {
    let path = window.location.pathname.replace(/\/$/, '');
    if (path === '' || path === '/main-view-hybrid') path = path === '/main-view-hybrid' ? '/m/main-view' : '/m';
    document.querySelectorAll('.mobile-nav-link').forEach(link => {
      const href = (link.getAttribute('href') || '').replace(/\/$/, '') || '/m';
      link.classList.toggle('active', href === path);
    });
  }

  function init() {
    const btn = document.getElementById('mobileNavBtn');
    const closeBtn = document.getElementById('mobileNavClose');
    const backdrop = document.getElementById('mobileNavBackdrop');

    if (!btn) return;

    btn.addEventListener('click', () => setNavOpen(true));
    closeBtn?.addEventListener('click', () => setNavOpen(false));
    backdrop?.addEventListener('click', () => setNavOpen(false));

    document.querySelectorAll('.mobile-nav-link').forEach(link => {
      link.addEventListener('click', () => setNavOpen(false));
    });

    markActiveLink();
  }

  document.addEventListener('DOMContentLoaded', init);
})();
