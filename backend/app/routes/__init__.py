from flask import Flask

from .health import health_bp
from .auth import auth_bp
from .comments import comments_bp
from .market import market_bp


def register_blueprints(app: Flask) -> None:
    app.register_blueprint(health_bp, url_prefix="/api")
    app.register_blueprint(auth_bp, url_prefix="/api/auth")
    app.register_blueprint(comments_bp, url_prefix="/api")
    app.register_blueprint(market_bp, url_prefix="/api")

