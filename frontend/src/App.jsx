import { Routes, Route, Navigate, Link } from 'react-router-dom'
import Home from './pages/Home'
import Login from './pages/Login'
import TickerComments from './pages/TickerComments'
import useAuth from './hooks/useAuth'

function App() {
  const { token, logout } = useAuth()

  return (
    <div className="container">
      <nav className="nav">
        <Link to="/">Home</Link>
        <Link to="/ticker/CRDO/comments">CRDO</Link>
        {token ? (
          <button onClick={logout}>Logout</button>
        ) : (
          <Link to="/login">Login</Link>
        )}
      </nav>
      <Routes>
        <Route path="/" element={<Home />} />
        <Route path="/login" element={<Login />} />
        <Route path="/ticker/:ticker/comments" element={<TickerComments />} />
        <Route path="*" element={<Navigate to="/" replace />} />
      </Routes>
    </div>
  )
}

export default App

