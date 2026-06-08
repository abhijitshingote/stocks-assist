(function () {
  'use strict';

  window.MobileMasterDetail = {
    init(rootId) {
      const root = document.getElementById(rootId);
      if (!root) return { showDetail() {}, showList() {}, root: null };

      root.querySelectorAll('[data-md-back]').forEach(btn => {
        btn.addEventListener('click', () => root.classList.remove('show-detail'));
      });

      return {
        showDetail() { root.classList.add('show-detail'); },
        showList() { root.classList.remove('show-detail'); },
        root,
      };
    },
  };
})();
