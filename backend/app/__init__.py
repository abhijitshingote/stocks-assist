from flask import Flask
from flask_cors import CORS
from flask_jwt_extended import JWTManager

from .config.settings import Settings
from .models.db import SessionLocal, engine
from .models.entities import Base
from .routes import register_blueprints


def create_app() -> Flask:
    app = Flask(__name__)

    # Config
    app.config["SECRET_KEY"] = Settings.SECRET_KEY
    app.config["JWT_SECRET_KEY"] = Settings.JWT_SECRET_KEY

    # CORS
    CORS(
        app,
        resources={r"/api/*": {"origins": Settings.CORS_ORIGINS.split(",") if Settings.CORS_ORIGINS else "*"}},
        supports_credentials=True,
    )

    # JWT
    JWTManager(app)

    # DB session lifecycle
    @app.teardown_appcontext
    def remove_session(exception=None):  # noqa: D401
        """Remove scoped session at the end of request/app context."""
        SessionLocal.remove()

    # Ensure tables exist (safe for dev; in prod use migrations)
    with app.app_context():
        Base.metadata.create_all(bind=engine)

    # Routes
    register_blueprints(app)

    return app

