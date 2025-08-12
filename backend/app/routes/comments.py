from flask import Blueprint, jsonify, request
from flask_jwt_extended import jwt_required

from ..models.db import SessionLocal
from ..services import comments_service as svc
from ..schemas.comment import AddCommentSchema, ReviewSchema


comments_bp = Blueprint("comments", __name__)


@comments_bp.get("/tickers/<string:ticker>/comments")
def get_comments(ticker: str):
    session = SessionLocal()
    try:
        rows = svc.list_visible_comments(session, ticker.upper())
        return jsonify(
            [
                {
                    "id": c.id,
                    "ticker": c.ticker,
                    "comment_text": c.comment_text,
                    "comment_type": c.comment_type,
                    "status": c.status,
                    "ai_source": c.ai_source,
                    "created_at": c.created_at.strftime("%Y-%m-%d %H:%M:%S"),
                }
                for c in rows
            ]
        )
    finally:
        session.close()


@comments_bp.post("/tickers/<string:ticker>/comments")
@jwt_required()
def add_comment(ticker: str):
    session = SessionLocal()
    try:
        data = request.get_json() or {}
        payload = AddCommentSchema.model_validate(data)
        new_comment = svc.add_user_comment(session, ticker.upper(), payload.comment_text)
        return (
            jsonify(
                {
                    "id": new_comment.id,
                    "ticker": new_comment.ticker,
                    "comment_text": new_comment.comment_text,
                    "comment_type": new_comment.comment_type,
                    "status": new_comment.status,
                    "ai_source": new_comment.ai_source,
                    "created_at": new_comment.created_at.strftime("%Y-%m-%d %H:%M:%S"),
                }
            ),
            201,
        )
    except ValueError as e:
        return jsonify({"error": str(e)}), 404
    finally:
        session.close()


@comments_bp.get("/tickers/<string:ticker>/ai-comments")
def get_ai_comments(ticker: str):
    status = request.args.get("status", "pending").lower()
    if status not in {"pending", "approved", "rejected"}:
        status = "pending"
    session = SessionLocal()
    try:
        rows = svc.list_ai_comments(session, ticker.upper(), status)
        return jsonify(
            [
                {
                    "id": c.id,
                    "ticker": c.ticker,
                    "comment_text": c.comment_text,
                    "comment_type": c.comment_type,
                    "status": c.status,
                    "ai_source": c.ai_source,
                    "created_at": c.created_at.strftime("%Y-%m-%d %H:%M:%S"),
                }
                for c in rows
            ]
        )
    finally:
        session.close()


@comments_bp.post("/tickers/<string:ticker>/ai-comments")
@jwt_required()
def add_ai_comment(ticker: str):
    session = SessionLocal()
    try:
        data = request.get_json() or {}
        payload = AddCommentSchema.model_validate(data)
        new_comment = svc.add_ai_comment(
            session,
            ticker.upper(),
            payload.comment_text,
            ai_source=payload.ai_source or "unknown",
            status="approved",
        )
        return (
            jsonify(
                {
                    "id": new_comment.id,
                    "ticker": new_comment.ticker,
                    "comment_text": new_comment.comment_text,
                    "comment_type": new_comment.comment_type,
                    "status": new_comment.status,
                    "ai_source": new_comment.ai_source,
                    "created_at": new_comment.created_at.strftime("%Y-%m-%d %H:%M:%S"),
                }
            ),
            201,
        )
    except ValueError as e:
        return jsonify({"error": str(e)}), 404
    finally:
        session.close()


@comments_bp.post("/ai-comments/<int:comment_id>/review")
@jwt_required()
def review_ai_comment(comment_id: int):
    session = SessionLocal()
    try:
        data = request.get_json() or {}
        payload = ReviewSchema.model_validate(data)
        updated = svc.review_ai_comment(
            session, comment_id, payload.action, payload.reviewed_by or "user"
        )
        return jsonify(
            {
                "id": updated.id,
                "ticker": updated.ticker,
                "comment_text": updated.comment_text,
                "comment_type": updated.comment_type,
                "status": updated.status,
                "ai_source": updated.ai_source,
                "reviewed_by": updated.reviewed_by,
                "created_at": updated.created_at.strftime("%Y-%m-%d %H:%M:%S"),
            }
        )
    except LookupError as e:
        return jsonify({"error": str(e)}), 404
    finally:
        session.close()

