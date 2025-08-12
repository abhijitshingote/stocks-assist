import { useEffect, useState } from 'react'
import { useParams } from 'react-router-dom'
import api from '../services/api'

export default function TickerComments() {
  const { ticker } = useParams()
  const [comments, setComments] = useState([])
  const [text, setText] = useState('')
  const [error, setError] = useState('')

  async function load() {
    const { data } = await api.get(`/tickers/${ticker}/comments`)
    setComments(data)
  }

  useEffect(() => {
    load()
  }, [ticker])

  async function addComment() {
    setError('')
    try {
      await api.post(`/tickers/${ticker}/comments`, { comment_text: text })
      setText('')
      load()
    } catch (e) {
      setError('Login required or error adding comment')
    }
  }

  return (
    <div>
      <h2>{ticker} comments</h2>
      <div className="card">
        <textarea rows="3" value={text} onChange={(e) => setText(e.target.value)} placeholder="Add a comment" />
        <button onClick={addComment}>Post</button>
        {error && <div style={{ color: 'tomato' }}>{error}</div>}
      </div>
      {comments.map((c) => (
        <div key={c.id} className="card">
          <div style={{ opacity: 0.75, fontSize: 12 }}>{c.created_at}</div>
          <div>{c.comment_text}</div>
          <div style={{ opacity: 0.6, fontSize: 12 }}>{c.comment_type} {c.status}</div>
        </div>
      ))}
    </div>
  )
}

