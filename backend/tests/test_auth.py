def test_login_success(client, monkeypatch):
    monkeypatch.setenv("ADMIN_USERNAME", "user")
    monkeypatch.setenv("ADMIN_PASSWORD", "pass")
    res = client.post("/api/auth/login", json={"username": "user", "password": "pass"})
    assert res.status_code == 200
    assert "access_token" in res.get_json()


def test_login_failure(client, monkeypatch):
    monkeypatch.setenv("ADMIN_USERNAME", "user")
    monkeypatch.setenv("ADMIN_PASSWORD", "pass")
    res = client.post("/api/auth/login", json={"username": "user", "password": "wrong"})
    assert res.status_code == 401

