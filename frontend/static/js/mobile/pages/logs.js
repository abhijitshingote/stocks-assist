(function () {
  'use strict';

  let currentLogFile = null;
  let autoRefreshInterval = null;
  const md = window.MobileMasterDetail.init('logsLayout');

  document.addEventListener('DOMContentLoaded', () => {
    document.getElementById('btnRefreshList').addEventListener('click', loadLogList);
    document.getElementById('btnAutoRefresh').addEventListener('click', toggleAutoRefresh);
    document.getElementById('btnScrollTop').addEventListener('click', scrollToTop);
    document.getElementById('btnScrollBottom').addEventListener('click', scrollToBottom);
    loadLogList();
  });

  async function loadLogList() {
    const listEl = document.getElementById('logList');
    listEl.innerHTML = '<div class="md-loading">Loading…</div>';

    try {
      const response = await fetch('/api/frontend/logs');
      const data = await response.json();

      if (data.logs && data.logs.length > 0) {
        document.getElementById('logCount').textContent = `${data.logs.length} files`;
        listEl.innerHTML = data.logs.map(log => {
          const isInit = log.name.includes('_init_');
          const isUpdate = log.name.includes('_update_');
          const isProd = log.name.startsWith('prod_');
          const isDev = log.name.startsWith('dev_');
          const envBadge = isProd
            ? '<span class="log-badge prod">prod</span>'
            : isDev ? '<span class="log-badge dev">dev</span>' : '';
          const typeBadge = isInit
            ? '<span class="log-badge init">init</span>'
            : isUpdate ? '<span class="log-badge update">update</span>' : '';
          const active = currentLogFile === log.name ? ' active' : '';
          return `<div class="md-item${active}" data-log="${escapeAttr(log.name)}">
            <div class="md-item-title">${escapeHtml(log.name)}</div>
            <div class="md-item-meta">${envBadge}${typeBadge}${formatBytes(log.size)} · ${escapeHtml(log.modified_str)}</div>
          </div>`;
        }).join('');

        listEl.querySelectorAll('.md-item').forEach(item => {
          item.addEventListener('click', () => loadLog(item.dataset.log));
        });
      } else {
        document.getElementById('logCount').textContent = '0 files';
        listEl.innerHTML = '<div class="md-empty">No log files found</div>';
      }
    } catch (error) {
      listEl.innerHTML = `<div class="md-empty">Error: ${escapeHtml(error.message)}</div>`;
    }
  }

  async function loadLog(filename) {
    currentLogFile = filename;
    document.getElementById('currentLogName').textContent = filename;
    const contentEl = document.getElementById('logContent');
    contentEl.innerHTML = '<div class="md-loading">Loading…</div>';

    document.querySelectorAll('.md-item').forEach(el => {
      el.classList.toggle('active', el.dataset.log === filename);
    });

    md.showDetail();

    try {
      const response = await fetch(`/api/frontend/logs/${encodeURIComponent(filename)}`);
      const data = await response.json();

      if (data.content) {
        const lines = data.content.split('\n');
        const formatted = lines.map((line, i) =>
          `<div class="line"><span class="line-number">${i + 1}</span>${formatLogLine(line)}</div>`
        ).join('');
        contentEl.innerHTML = `<pre>${formatted}</pre>`;
        setTimeout(scrollToBottom, 50);
      } else {
        contentEl.innerHTML = '<div class="md-empty">Log file is empty</div>';
      }
    } catch (error) {
      contentEl.innerHTML = `<div class="md-empty">Error: ${escapeHtml(error.message)}</div>`;
    }
  }

  function formatLogLine(line) {
    return escapeHtml(line)
      .replace(/(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}(?:,\d{3})?)/g, '<span class="timestamp">$1</span>')
      .replace(/- INFO -/g, '- <span class="level-info">INFO</span> -')
      .replace(/- WARNING -/g, '- <span class="level-warning">WARNING</span> -')
      .replace(/- ERROR -/g, '- <span class="level-error">ERROR</span> -');
  }

  function formatBytes(bytes) {
    if (bytes === 0) return '0 B';
    const k = 1024;
    const sizes = ['B', 'KB', 'MB', 'GB'];
    const i = Math.floor(Math.log(bytes) / Math.log(k));
    return `${parseFloat((bytes / Math.pow(k, i)).toFixed(1))} ${sizes[i]}`;
  }

  function scrollToBottom() {
    const el = document.getElementById('logContent');
    el.scrollTop = el.scrollHeight;
  }

  function scrollToTop() {
    document.getElementById('logContent').scrollTop = 0;
  }

  function toggleAutoRefresh() {
    const btn = document.getElementById('btnAutoRefresh');
    if (autoRefreshInterval) {
      clearInterval(autoRefreshInterval);
      autoRefreshInterval = null;
      btn.classList.remove('active');
      btn.textContent = 'Auto';
    } else {
      btn.classList.add('active');
      btn.textContent = 'Auto ✓';
      autoRefreshInterval = setInterval(() => {
        if (currentLogFile) loadLog(currentLogFile);
        loadLogList();
      }, 3000);
    }
  }

  function escapeHtml(text) {
    const div = document.createElement('div');
    div.textContent = text == null ? '' : text;
    return div.innerHTML;
  }

  function escapeAttr(s) {
    return String(s == null ? '' : s)
      .replace(/&/g, '&amp;')
      .replace(/"/g, '&quot;')
      .replace(/</g, '&lt;');
  }
})();
