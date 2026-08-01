(function () {
  'use strict';

  let currentDate = null;
  let currentData = null;
  let currentRunStatus = 'complete';
  let currentLosersRunStatus = 'empty';
  let costPollTimer = null;
  let losersPollTimer = null;
  let briefTodayDate = null;

  document.addEventListener('DOMContentLoaded', () => {
    document.getElementById('btnRun').addEventListener('click', () => runBrief(currentDate));
    document.getElementById('btnPdf').addEventListener('click', exportBriefPdf);
    document.getElementById('btnLosers').addEventListener('click', () => runLosersBrief(currentDate));
    loadDateList();
  });

  function briefToday() {
    return new Date().toLocaleDateString('en-CA', { timeZone: 'America/New_York' });
  }

  function escapeHtml(text) {
    const div = document.createElement('div');
    div.textContent = text == null ? '' : text;
    return div.innerHTML;
  }

  function formatStepLabel(step) {
    if (!step) return 'Running…';
    const labels = {
      queued: 'Starting pipeline',
      ingest: 'Fetching Benzinga news',
      ingest_done: 'Ingest complete',
      synthesis: 'Writing losers brief',
      step3_fact_extraction: 'Extracting facts',
      step4_synthesis: 'Writing brief',
      done: 'Complete',
    };
    if (labels[step]) return labels[step];
    return step.replace(/^step3_/, 'Step 3 · ').replace(/^step4_/, 'Step 4 · ').replace(/_/g, ' ');
  }

  function inlineMarkdown(text) {
    let html = escapeHtml(text);
    html = html.replace(/\*\*(.+?)\*\*/g, '<strong>$1</strong>');
    html = html.replace(/`([^`]+)`/g, '<code>$1</code>');
    return html;
  }

  function renderMarkdownTable(tableLines) {
    const rows = tableLines
      .filter(l => !/^\|[\s\-:|]+\|$/.test(l.trim()))
      .map(l => l.split('|').slice(1, -1).map(c => c.trim()));
    if (!rows.length) return '';
    const headers = rows[0].map(h => h.replace(/\*\*/g, '').trim());
    let html = '<div class="brief-table-wrap"><table class="brief-data-table"><thead><tr>';
    rows[0].forEach(c => { html += '<th>' + inlineMarkdown(c) + '</th>'; });
    html += '</tr></thead><tbody>';
    for (let r = 1; r < rows.length; r++) {
      html += '<tr>';
      rows[r].forEach((c, ci) => {
        html += '<td data-label="' + escapeHtml(headers[ci] || '') + '">' + inlineMarkdown(c) + '</td>';
      });
      html += '</tr>';
    }
    html += '</tbody></table></div>';
    return html;
  }

  function simpleMarkdown(md) {
    const lines = md.split('\n');
    const out = [];
    let i = 0;
    while (i < lines.length) {
      const line = lines[i];
      if (line.trim().startsWith('|') && line.includes('|')) {
        const tableLines = [];
        while (i < lines.length && lines[i].trim().startsWith('|')) {
          tableLines.push(lines[i]);
          i++;
        }
        out.push(renderMarkdownTable(tableLines));
        continue;
      }
      if (/^### /.test(line)) out.push('<h3>' + escapeHtml(line.slice(4)) + '</h3>');
      else if (/^## /.test(line)) out.push('<h2>' + escapeHtml(line.slice(3)) + '</h2>');
      else if (/^# /.test(line)) out.push('<h1>' + escapeHtml(line.slice(2)) + '</h1>');
      else if (/^---+$/.test(line.trim())) out.push('<hr>');
      else if (/^- /.test(line)) out.push('<li>' + inlineMarkdown(line.slice(2)) + '</li>');
      else if (line.trim() === '') out.push('');
      else out.push('<p>' + inlineMarkdown(line) + '</p>');
      i++;
    }
    return out.join('\n');
  }

  function updatePdfButton() {
    document.getElementById('btnPdf').disabled = !(currentData && currentData.markdown);
  }

  function updateLosersButton() {
    const btn = document.getElementById('btnLosers');
    if (currentLosersRunStatus === 'running') {
      btn.textContent = 'Losers…';
      btn.disabled = true;
    } else {
      btn.disabled = false;
      btn.textContent = 'Losers';
    }
  }

  function updateRunButton() {
    const btn = document.getElementById('btnRun');
    if (currentRunStatus === 'running') {
      btn.textContent = 'Running…';
      btn.disabled = true;
    } else {
      btn.disabled = false;
      btn.textContent = 'Generate';
    }
  }

  async function loadDateList() {
    const strip = document.getElementById('dateStrip');
    strip.innerHTML = '<span class="md-chip">Loading…</span>';

    try {
      const response = await fetch('/api/frontend/market-brief/dates');
      const data = await response.json();
      briefTodayDate = data.today || briefToday();

      if (data.dates && data.dates.length > 0) {
        strip.innerHTML = data.dates.map(item => {
          const st = item.status || 'complete';
          const statusLabel = st === 'running' ? 'Running'
            : st === 'complete' ? 'Ready'
            : st === 'ready' ? 'Source'
            : st === 'empty' ? 'Empty'
            : st === 'failed' || st === 'error' ? 'Failed'
            : '—';
          const active = currentDate === item.date ? ' active' : '';
          const isToday = item.is_today || item.date === briefTodayDate;
          return `<button type="button" class="md-chip ${st}${active}" data-date="${item.date}" data-status="${st}">
            ${item.date}${isToday ? ' · today' : ''}
            <span class="status">${statusLabel}</span>
          </button>`;
        }).join('');

        strip.querySelectorAll('.md-chip').forEach(chip => {
          chip.addEventListener('click', () => loadBrief(chip.dataset.date, chip.dataset.status));
        });

        if (!currentDate) {
          const todayEntry = data.dates.find(d => d.date === briefTodayDate);
          loadBrief(briefTodayDate, todayEntry ? (todayEntry.status || 'empty') : 'empty');
        }
      } else {
        briefTodayDate = briefTodayDate || briefToday();
        strip.innerHTML = `<button type="button" class="md-chip active empty" data-date="${briefTodayDate}" data-status="empty">
          ${briefTodayDate} · today<span class="status">Empty</span>
        </button>`;
        strip.querySelector('.md-chip').addEventListener('click', () => loadBrief(briefTodayDate, 'empty'));
        if (!currentDate) loadBrief(briefTodayDate, 'empty');
      }
    } catch (error) {
      strip.innerHTML = `<span class="md-empty">Error: ${escapeHtml(error.message)}</span>`;
    }
  }

  async function loadBrief(dateStr, status) {
    currentDate = dateStr;
    currentRunStatus = status || 'complete';
    const contentEl = document.getElementById('briefContent');
    const titleEl = document.getElementById('currentBriefDate');
    const todayTag = dateStr === (briefTodayDate || briefToday()) ? ' (today)' : '';
    titleEl.textContent = dateStr + todayTag;

    document.querySelectorAll('#dateStrip .md-chip').forEach(chip => {
      chip.classList.toggle('active', chip.dataset.date === dateStr);
    });

    stopRunPolling();
    stopLosersPolling();
    updatePdfButton();
    updateLosersButton();
    updateRunButton();
    contentEl.innerHTML = '<div class="md-loading">Loading…</div>';

    try {
      const response = await fetch(`/api/frontend/market-brief/${dateStr}`);
      if (!response.ok && response.status !== 404) throw new Error(`HTTP ${response.status}`);

      if (response.ok) {
        currentData = await response.json();
        currentRunStatus = currentData.status || status;
        currentLosersRunStatus = currentData.losers_status || 'empty';
      } else {
        currentData = { date: dateStr, status: status };
        currentLosersRunStatus = 'empty';
      }

      updateLosersButton();
      updateRunButton();
      if (currentLosersRunStatus === 'running') startLosersPolling(dateStr);

      const stage = currentData.run_status?.stage;
      renderProgressBanner(stage, currentRunStatus, currentData.run_status);

      if (currentRunStatus === 'running' || status === 'running') {
        startRunPolling(dateStr);
        contentEl.innerHTML = '<div class="md-empty">Pipeline running — usually 15–25 minutes.</div>';
        updatePdfButton();
        return;
      }

      if (currentRunStatus === 'failed' || currentRunStatus === 'error') {
        contentEl.innerHTML = '<div class="md-empty">This run did not finish. Tap Generate to retry.</div>';
        updatePdfButton();
        return;
      }

      if (currentRunStatus === 'empty' || status === 'empty') {
        contentEl.innerHTML = `<div class="generate-prompt">
          <p>No brief for <strong>${escapeHtml(dateStr)}</strong> yet.</p>
          <button type="button" id="btnGenerateInline">Generate brief</button>
        </div>`;
        document.getElementById('btnGenerateInline').addEventListener('click', () => runBrief(dateStr));
        updatePdfButton();
        return;
      }

      if (!currentData.markdown && (currentRunStatus === 'ready' || currentData.has_source)) {
        contentEl.innerHTML = `<div class="generate-prompt">
          <p>Source data ready — brief not generated yet.</p>
          <button type="button" id="btnGenerateInline">Generate brief</button>
        </div>`;
        document.getElementById('btnGenerateInline').addEventListener('click', () => runBrief(dateStr));
        updatePdfButton();
        return;
      }

      if (!currentData.markdown) {
        contentEl.innerHTML = '<div class="md-empty">No data for this date.</div>';
        updatePdfButton();
        return;
      }

      renderFormatted();
      updatePdfButton();
    } catch (error) {
      contentEl.innerHTML = `<div class="md-empty">Error: ${escapeHtml(error.message)}</div>`;
      updatePdfButton();
    }
  }

  function renderProgressBanner(stage, status, runStatus) {
    const banner = document.getElementById('runProgressBanner');
    if (status !== 'running') {
      banner.innerHTML = '';
      return;
    }
    const updated = runStatus?.updated_at ? new Date(runStatus.updated_at).toLocaleTimeString() : '';
    banner.innerHTML = `<div class="brief-progress">
      <strong>${escapeHtml(formatStepLabel(stage))}</strong>
      ${updated ? ` · updated ${updated}` : ''}
    </div>`;
  }

  function renderLosersBriefSection() {
    if (!currentData) return '';
    let progress = '';
    if (currentLosersRunStatus === 'running') {
      progress = `<p class="md-item-meta">Losers pipeline running — ${escapeHtml(formatStepLabel(currentData.losers_run_status?.stage))}</p>`;
    } else if (currentLosersRunStatus === 'failed' || currentLosersRunStatus === 'error') {
      progress = '<p class="md-item-meta">Last losers run failed.</p>';
    }

    if (currentData.has_losers_brief && currentData.losers_markdown) {
      return `<div class="section-h2">R1D Losers Brief</div>${progress}
        <div class="markdown-body">${simpleMarkdown(currentData.losers_markdown)}</div>`;
    }
    return `<div class="section-h2">R1D Losers Brief</div>${progress}
      <p class="md-item-meta">No losers brief for this date.</p>`;
  }

  function renderFormatted() {
    const contentEl = document.getElementById('briefContent');
    const json = currentData.json;

    if (!json || json._error || json._parse_error || !json.tldr) {
      const md = currentData.markdown || '(No content)';
      contentEl.innerHTML = `<div class="markdown-body">${simpleMarkdown(md)}</div>${renderLosersBriefSection()}`;
      return;
    }

    let html = '';
    if (json.tldr && json.tldr.length > 0) {
      html += '<div class="tldr-section"><div class="tldr-title">TL;DR</div>';
      json.tldr.forEach(item => {
        const tickers = (item.tickers || []).map(t =>
          `<a href="/stock/${t}" class="ticker-tag">${t}</a>`
        ).join(' ');
        html += `<div class="tldr-item">${escapeHtml(item.text)} ${tickers}</div>`;
      });
      html += '</div>';
    }

    if (json.topics && json.topics.length > 0) {
      html += '<div class="section-h2">Topics</div>';
      json.topics.forEach(topic => {
        html += `<div class="topic-card"><div class="topic-title">${escapeHtml(topic.name)}</div>`;
        (topic.bullets || []).forEach(bullet => {
          const tickers = (bullet.tickers || []).map(t =>
            `<a href="/stock/${t}" class="ticker-tag">${t}</a>`
          ).join(' ');
          html += `<div class="topic-bullet">${escapeHtml(bullet.text)} ${tickers}</div>`;
        });
        html += '</div>';
      });
    }

    if (json.callouts && json.callouts.length > 0) {
      html += '<div class="section-h2">Stock Callouts</div>';
      json.callouts.forEach(c => {
        html += `<div class="callout-card">
          <a href="/stock/${c.ticker}" class="callout-ticker">${c.ticker}</a>
          <div class="callout-summary">${escapeHtml(c.summary)}</div>
        </div>`;
      });
    }

    if (json.watch_tomorrow && json.watch_tomorrow.length > 0) {
      html += '<div class="section-h2">Watch Next Session</div>';
      json.watch_tomorrow.forEach(item => {
        const tickers = (item.tickers || []).map(t =>
          `<a href="/stock/${t}" class="ticker-tag">${t}</a>`
        ).join(' ');
        html += `<div class="watch-item">${escapeHtml(item.text)} ${tickers}</div>`;
      });
    }

    html += renderLosersBriefSection();
    contentEl.innerHTML = html || '<div class="md-empty">No structured data available</div>';
  }

  function stopRunPolling() {
    if (costPollTimer) { clearInterval(costPollTimer); costPollTimer = null; }
  }

  function startRunPolling(dateStr) {
    stopRunPolling();
    costPollTimer = setInterval(() => refreshRunProgress(dateStr), 3000);
  }

  async function refreshRunProgress(dateStr) {
    try {
      const response = await fetch(`/api/frontend/market-brief/${dateStr}/costs`);
      if (!response.ok) return;
      const data = await response.json();
      currentRunStatus = data.status || currentRunStatus;
      renderProgressBanner(data.run_status?.stage, currentRunStatus, data.run_status);
      if (currentRunStatus === 'complete' || currentRunStatus === 'failed' || currentRunStatus === 'error') {
        stopRunPolling();
        await loadBrief(dateStr, currentRunStatus);
      }
    } catch (e) {
      console.warn('run progress poll failed', e);
    }
  }

  function stopLosersPolling() {
    if (losersPollTimer) { clearInterval(losersPollTimer); losersPollTimer = null; }
  }

  function startLosersPolling(dateStr) {
    stopLosersPolling();
    losersPollTimer = setInterval(() => refreshLosersProgress(dateStr), 3000);
  }

  async function refreshLosersProgress(dateStr) {
    try {
      const response = await fetch(`/api/frontend/market-brief-losers/${dateStr}/costs`);
      if (!response.ok) return;
      const data = await response.json();
      currentLosersRunStatus = data.status || currentLosersRunStatus;
      updateLosersButton();
      if (currentLosersRunStatus === 'complete' || currentLosersRunStatus === 'failed' || currentLosersRunStatus === 'error') {
        stopLosersPolling();
        if (currentDate === dateStr) await loadBrief(dateStr, currentRunStatus);
      }
    } catch (e) {
      console.warn('losers progress poll failed', e);
    }
  }

  async function runBrief(forDate) {
    const btn = document.getElementById('btnRun');
    btn.disabled = true;
    btn.textContent = 'Starting…';
    stopRunPolling();

    const asof = forDate || briefTodayDate || briefToday();
    try {
      const response = await fetch('/api/frontend/market-brief/generate', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ asof }),
      });
      const data = await response.json();

      if (response.status === 409) {
        alert(data.message || 'A brief for this date is already running.');
        await loadDateList();
        if (data.asof) await loadBrief(data.asof, 'running');
        return;
      }
      if (response.status === 400) {
        alert(data.message || data.error || 'Could not start pipeline.');
        updateRunButton();
        return;
      }
      if (data.status === 'started') {
        const targetDate = data.asof || asof;
        await loadDateList();
        if (targetDate) await loadBrief(targetDate, 'running');
      } else {
        alert('Failed to start: ' + (data.error || data.message || 'Unknown error'));
        updateRunButton();
      }
    } catch (error) {
      alert('Error: ' + error.message);
      updateRunButton();
    }
  }

  async function runLosersBrief(forDate) {
    const dateStr = forDate || currentDate;
    if (!dateStr) return;
    const btn = document.getElementById('btnLosers');
    btn.disabled = true;
    btn.textContent = 'Starting…';

    try {
      const response = await fetch('/api/frontend/market-brief-losers/generate', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ asof: dateStr }),
      });
      const data = await response.json();
      if (response.status === 409) {
        alert(data.message || 'Losers brief already running.');
        currentLosersRunStatus = 'running';
        startLosersPolling(dateStr);
        updateLosersButton();
        return;
      }
      if (data.status === 'started') {
        currentLosersRunStatus = 'running';
        startLosersPolling(dateStr);
        updateLosersButton();
        await loadBrief(dateStr, currentRunStatus);
      } else {
        alert('Failed: ' + (data.error || data.message || 'Unknown error'));
        updateLosersButton();
      }
    } catch (error) {
      alert('Error: ' + error.message);
      updateLosersButton();
    }
  }

  async function exportBriefPdf() {
    if (!currentDate || !(currentData && currentData.markdown)) return;
    const btn = document.getElementById('btnPdf');
    const label = btn.textContent;
    btn.disabled = true;
    btn.textContent = '…';
    try {
      const response = await fetch(`/api/frontend/market-brief/${currentDate}/pdf`);
      if (!response.ok) {
        let msg = `HTTP ${response.status}`;
        try {
          const err = await response.json();
          if (err.error) msg = err.error;
        } catch (_) { /* ignore */ }
        throw new Error(msg);
      }
      const blob = await response.blob();
      const url = URL.createObjectURL(blob);
      const a = document.createElement('a');
      a.href = url;
      a.download = `market-brief-${currentDate}.pdf`;
      document.body.appendChild(a);
      a.click();
      a.remove();
      URL.revokeObjectURL(url);
    } catch (error) {
      alert('PDF export failed: ' + error.message);
    } finally {
      btn.textContent = label;
      updatePdfButton();
    }
  }
})();
