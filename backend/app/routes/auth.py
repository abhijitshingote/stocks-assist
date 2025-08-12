from flask import Blueprint, jsonify, request
from flask_jwt_extended import create_access_token
import os


auth_bp = Blueprint("auth", __name__)


@auth_bp.post("/login")
def login():
    data = request.get_json() or {}
    username = data.get("username")
    password = data.get("password")
    # Read credentials at request time to allow dynamic env overrides (e.g., tests)
    expected_user = os.getenv("ADMIN_USERNAME", "admin")
    expected_pass = os.getenv("ADMIN_PASSWORD", "admin")
    if username == expected_user and password == expected_pass:
        token = create_access_token(identity=username)
        return jsonify({"access_token": token})
    return jsonify({"error": "Invalid credentials"}), 401

