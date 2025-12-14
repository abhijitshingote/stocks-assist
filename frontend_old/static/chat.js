/* Chatbot frontend logic */
(function () {
  const messagesEl = document.getElementById('chatMessages');
  const inputEl = document.getElementById('chatInput');
  const sendBtn = document.getElementById('sendChatBtn');

  if (!messagesEl || !inputEl || !sendBtn) return; // safety

  function appendMessage(content, from) {
    const wrapper = document.createElement('div');
    wrapper.className = from === 'user' ? 'text-end mb-3' : 'text-start mb-3';
    wrapper.innerHTML = `
      <div class="d-inline-block p-2 rounded ${from === 'user' ? 'bg-primary text-white' : 'bg-light'}" style="max-width: 80%; white-space: pre-wrap;">
        ${content}
      </div>
    `;
    messagesEl.appendChild(wrapper);
    messagesEl.scrollTop = messagesEl.scrollHeight;
  }

  async function sendPrompt() {
    const prompt = inputEl.value.trim();
    if (!prompt) return;
    appendMessage(prompt, 'user');
    inputEl.value = '';
    inputEl.disabled = true;
    sendBtn.disabled = true;
    appendMessage('<span class="text-muted">Thinking...</span>', 'bot');

    try {
      const res = await fetch('/api/frontend/perplexity', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ prompt })
      });
      const data = await res.json();
      messagesEl.lastChild.remove(); // remove thinking message
      if (!res.ok) {
        appendMessage(`<span class="text-danger">Error: ${data.error || 'Unknown error'}</span>`, 'bot');
      } else {
        appendMessage(data.response || 'No response', 'bot');
      }
    } catch (err) {
      messagesEl.lastChild.remove();
      appendMessage(`<span class="text-danger">Network error</span>`, 'bot');
    } finally {
      inputEl.disabled = false;
      sendBtn.disabled = false;
      inputEl.focus();
    }
  }

  sendBtn.addEventListener('click', sendPrompt);
  inputEl.addEventListener('keypress', function (e) {
    if (e.key === 'Enter') {
      e.preventDefault();
      sendPrompt();
    }
  });
})(); 