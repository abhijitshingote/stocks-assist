import { useState } from 'react'
import { useNavigate } from 'react-router-dom'
import useAuth from '../hooks/useAuth'

export default function Login() {
  const [username, setUsername] = useState('admin')
  const [password, setPassword] = useState('admin')
  const [error, setError] = useState('')
  const navigate = useNavigate()
  const { login } = useAuth()

  async function onSubmit(e) {
    e.preventDefault()
    try {
      await login(username, password)
      navigate('/')
    } catch (err) {
      setError('Login failed')
    }
  }

  return (
    <form onSubmit={onSubmit} className="card" style={{ maxWidth: 360 }}>
      <h3>Login</h3>
      {error && <div style={{ color: 'tomato' }}>{error}</div>}
      <label>Username</label>
      <input value={username} onChange={(e) => setUsername(e.target.value)} />
      <label>Password</label>
      <input type="password" value={password} onChange={(e) => setPassword(e.target.value)} />
      <button type="submit">Sign in</button>
    </form>
  )
}

