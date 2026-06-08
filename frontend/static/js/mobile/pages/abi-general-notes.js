(function () {
  'use strict';

  let allNotes = [];
  let currentNoteId = null;
  let isEditing = false;
  let searchTimeout = null;
  let activeTag = null;
  const md = window.MobileMasterDetail.init('notesLayout');

  document.addEventListener('DOMContentLoaded', () => {
    document.getElementById('btnNewNote').addEventListener('click', createNewNote);
    document.getElementById('searchBox').addEventListener('input', debounceSearch);
    document.getElementById('startDate').addEventListener('change', loadNotes);
    document.getElementById('endDate').addEventListener('change', loadNotes);
    document.getElementById('saveBtn').addEventListener('click', saveNote);
    document.getElementById('deleteBtn').addEventListener('click', deleteNote);
    document.getElementById('cancelBtn').addEventListener('click', cancelEdit);
    loadNotes();
    loadTags();
  });

  function debounceSearch() {
    clearTimeout(searchTimeout);
    searchTimeout = setTimeout(loadNotes, 300);
  }

  async function loadNotes() {
    const listEl = document.getElementById('notesList');
    listEl.innerHTML = '<div class="md-loading">Loading…</div>';

    const params = new URLSearchParams();
    const search = document.getElementById('searchBox').value;
    const startDate = document.getElementById('startDate').value;
    const endDate = document.getElementById('endDate').value;
    if (search) params.append('search', search);
    if (startDate) params.append('start_date', startDate);
    if (endDate) params.append('end_date', endDate);
    if (activeTag) params.append('tag', activeTag);

    try {
      const response = await fetch(`/api/frontend/abi-general-notes?${params.toString()}`);
      if (!response.ok) throw new Error(`HTTP ${response.status}`);
      const data = await response.json();
      if (data.error) throw new Error(data.error);
      allNotes = Array.isArray(data) ? data : [];
      renderNotesList();
      document.getElementById('notesCount').textContent =
        `${allNotes.length} note${allNotes.length !== 1 ? 's' : ''}`;
    } catch (error) {
      console.error('Error loading notes:', error);
      allNotes = [];
      listEl.innerHTML = '<div class="md-empty">No notes yet. Tap New to create one.</div>';
      document.getElementById('notesCount').textContent = '0 notes';
    }
  }

  async function loadTags() {
    try {
      const response = await fetch('/api/frontend/abi-general-notes/tags');
      if (!response.ok) return;
      const data = await response.json();
      const tagsEl = document.getElementById('tagsFilter');
      if (data.tags && data.tags.length > 0) {
        tagsEl.innerHTML = data.tags.map(tag =>
          `<span class="md-tag${activeTag === tag ? ' active' : ''}" data-tag="${escapeAttr(tag)}">${escapeHtml(tag)}</span>`
        ).join('');
        tagsEl.querySelectorAll('.md-tag').forEach(chip => {
          chip.addEventListener('click', () => {
            const tag = chip.dataset.tag;
            activeTag = activeTag === tag ? null : tag;
            loadNotes();
            loadTags();
          });
        });
      } else {
        tagsEl.innerHTML = '';
      }
    } catch (error) {
      console.error('Error loading tags:', error);
    }
  }

  function renderNotesList() {
    const listEl = document.getElementById('notesList');
    if (allNotes.length === 0) {
      listEl.innerHTML = '<div class="md-empty">No notes match your filters.</div>';
      return;
    }
    listEl.innerHTML = allNotes.map(note => {
      const tags = note.tags ? note.tags.split(',').map(t => t.trim()).filter(Boolean) : [];
      const tagsHtml = tags.map(t => `<span class="md-tag">${escapeHtml(t)}</span>`).join(' ');
      return `<div class="md-item${currentNoteId === note.id ? ' active' : ''}" data-id="${note.id}">
        <div class="md-item-meta">${formatDate(note.note_date)}</div>
        <div class="md-item-title">${escapeHtml(note.title) || 'Untitled'}</div>
        <div class="md-item-preview">${escapeHtml((note.content || '').substring(0, 120)) || 'No content'}</div>
        ${tags.length ? `<div class="md-tags" style="margin-top:6px">${tagsHtml}</div>` : ''}
      </div>`;
    }).join('');

    listEl.querySelectorAll('.md-item').forEach(item => {
      item.addEventListener('click', () => selectNote(parseInt(item.dataset.id, 10)));
    });
  }

  function selectNote(noteId) {
    const note = allNotes.find(n => n.id === noteId);
    if (!note) return;
    currentNoteId = noteId;
    isEditing = true;
    renderNotesList();
    showNoteEditor(note);
    md.showDetail();
  }

  function createNewNote() {
    currentNoteId = null;
    isEditing = true;
    renderNotesList();
    const today = new Date().toISOString().split('T')[0];
    showNoteEditor({ note_date: today, title: '', content: '', tags: '' });
    document.getElementById('editorTitle').textContent = 'New Note';
    document.getElementById('deleteBtn').style.display = 'none';
    md.showDetail();
  }

  function showNoteEditor(note) {
    document.getElementById('noteEditor').innerHTML = `
      <div class="md-form-row">
        <div class="md-form-group">
          <label for="noteDate">Date</label>
          <input type="date" id="noteDate" value="${escapeAttr(note.note_date || '')}" required>
        </div>
      </div>
      <div class="md-form-group">
        <label for="noteTitle">Title</label>
        <input type="text" id="noteTitle" value="${escapeAttr(note.title || '')}" placeholder="Note title…">
      </div>
      <div class="md-form-group">
        <label for="noteTags">Tags (comma-separated)</label>
        <input type="text" id="noteTags" value="${escapeAttr(note.tags || '')}" placeholder="market, idea…">
      </div>
      <div class="md-form-group">
        <label for="noteContentInput">Content</label>
        <textarea id="noteContentInput" placeholder="Write your thoughts…">${escapeHtml(note.content || '')}</textarea>
      </div>`;

    document.getElementById('editorTitle').textContent = currentNoteId ? 'Edit Note' : 'New Note';
    document.getElementById('noteActions').style.display = 'flex';
    document.getElementById('deleteBtn').style.display = currentNoteId ? '' : 'none';
    if (!currentNoteId) setTimeout(() => document.getElementById('noteTitle').focus(), 100);
  }

  async function saveNote() {
    const noteDate = document.getElementById('noteDate').value;
    const title = document.getElementById('noteTitle').value;
    const content = document.getElementById('noteContentInput').value;
    const tags = document.getElementById('noteTags').value;
    if (!noteDate) { showToast('Date is required', 'error'); return; }

    const data = { note_date: noteDate, title: title || null, content: content || null, tags: tags || null };
    const saveBtn = document.getElementById('saveBtn');
    saveBtn.disabled = true;
    saveBtn.textContent = 'Saving…';

    try {
      let response;
      if (currentNoteId) {
        response = await fetch(`/api/frontend/abi-general-notes/${currentNoteId}`, {
          method: 'PUT',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify(data),
        });
      } else {
        response = await fetch('/api/frontend/abi-general-notes', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify(data),
        });
      }
      const result = await response.json();
      if (response.ok) {
        showToast(currentNoteId ? 'Note updated!' : 'Note created!', 'success');
        currentNoteId = result.id;
        await loadNotes();
        await loadTags();
        selectNote(currentNoteId);
      } else {
        showToast(result.error || 'Error saving note', 'error');
      }
    } catch (error) {
      showToast('Error saving note: ' + error.message, 'error');
    } finally {
      saveBtn.disabled = false;
      saveBtn.textContent = 'Save';
    }
  }

  async function deleteNote() {
    if (!currentNoteId || !confirm('Delete this note?')) return;
    try {
      const response = await fetch(`/api/frontend/abi-general-notes/${currentNoteId}`, { method: 'DELETE' });
      if (response.ok) {
        showToast('Note deleted', 'success');
        cancelEdit();
        await loadNotes();
        await loadTags();
      } else {
        const result = await response.json();
        showToast(result.error || 'Error deleting note', 'error');
      }
    } catch (error) {
      showToast('Error deleting note', 'error');
    }
  }

  function cancelEdit() {
    currentNoteId = null;
    isEditing = false;
    renderNotesList();
    document.getElementById('noteEditor').innerHTML = '<div class="md-empty">Select a note or create a new one</div>';
    document.getElementById('editorTitle').textContent = 'Note';
    document.getElementById('noteActions').style.display = 'none';
    md.showList();
  }

  function formatDate(dateStr) {
    if (!dateStr) return '';
    const date = new Date(dateStr + 'T00:00:00');
    return date.toLocaleDateString('en-US', { weekday: 'short', month: 'short', day: 'numeric', year: 'numeric' });
  }

  function escapeHtml(text) {
    if (!text) return '';
    const div = document.createElement('div');
    div.textContent = text;
    return div.innerHTML;
  }

  function escapeAttr(s) {
    return String(s == null ? '' : s)
      .replace(/&/g, '&amp;')
      .replace(/"/g, '&quot;')
      .replace(/</g, '&lt;');
  }

  function showToast(message, type) {
    const toast = document.getElementById('toast');
    toast.textContent = message;
    toast.className = `md-toast ${type || ''} show`;
    setTimeout(() => toast.classList.remove('show'), 3000);
  }
})();
