import { useEffect, useState } from 'react'
import api from '../services/api'

export default function useAuth() {
  const [token, setToken] = useState(localStorage.getItem('access_token'))

  useEffect(() => {
    if (token) localStorage.setItem('access_token', token)
    else localStorage.removeItem('access_token')
  }, [token])

  async function login(username, password) {
    const { data } = await api.post('/auth/login', { username, password })
    setToken(data.access_token)
  }

  function logout() {
    setToken(null)
  }

  return { token, login, logout }
}

