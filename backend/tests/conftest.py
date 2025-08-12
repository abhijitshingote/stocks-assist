import os
import sys
import pathlib
import pytest


@pytest.fixture()
def app():
    # Configure test env BEFORE importing the app factory
    os.environ["SECRET_KEY"] = "test-secret"
    os.environ["JWT_SECRET_KEY"] = "test-jwt-secret"
    # Use in-memory SQLite to avoid external DB
    os.environ["DATABASE_URL"] = "sqlite+pysqlite:///:memory:"
    # Ensure backend root is on sys.path for `import app`
    backend_root = pathlib.Path(__file__).resolve().parents[1]
    if str(backend_root) not in sys.path:
        sys.path.insert(0, str(backend_root))
    from app import create_app  # imported after env is set
    application = create_app()
    yield application


@pytest.fixture()
def client(app):
    return app.test_client()

