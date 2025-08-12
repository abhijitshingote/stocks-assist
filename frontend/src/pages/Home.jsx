import { useEffect, useState } from 'react'
import api from '../services/api'

export default function Home() {
  const [latest, setLatest] = useState('...')

  useEffect(() => {
    api.get('/market/latest-date').then(({ data }) => setLatest(data.latest_date || 'n/a'))
  }, [])

  return (
    <div>
      <h2>Stocks Assist</h2>
      <div className="card">Latest market date: {latest}</div>
    </div>
  )
}

