from typing import List
from sqlalchemy.orm import Session
from sqlalchemy import desc

from ..models.entities import Comment, Stock


def list_visible_comments(session: Session, ticker: str) -> List[Comment]:
    return (
        session.query(Comment)
        .filter(
            Comment.ticker == ticker,
            (
                (Comment.comment_type == "user") & (Comment.status == "active")
            )
            | (
                (Comment.comment_type == "ai") & (Comment.status == "approved")
            )
        )
        .order_by(desc(Comment.created_at))
        .all()
    )


def add_user_comment(session: Session, ticker: str, text: str) -> Comment:
    if not session.query(Stock).filter(Stock.ticker == ticker).first():
        raise ValueError("Ticker not found")
    new_comment = Comment(
        ticker=ticker,
        comment_text=text.strip(),
        comment_type="user",
        status="active",
    )
    session.add(new_comment)
    session.commit()
    session.refresh(new_comment)
    return new_comment


def list_ai_comments(session: Session, ticker: str, status: str = "pending") -> List[Comment]:
    return (
        session.query(Comment)
        .filter(
            Comment.ticker == ticker,
            Comment.comment_type == "ai",
            Comment.status == status,
        )
        .order_by(desc(Comment.created_at))
        .all()
    )


def add_ai_comment(session: Session, ticker: str, text: str, ai_source: str = "unknown", status: str = "approved") -> Comment:
    if not session.query(Stock).filter(Stock.ticker == ticker).first():
        raise ValueError("Ticker not found")
    new_comment = Comment(
        ticker=ticker,
        comment_text=text.strip(),
        comment_type="ai",
        status=status,
        ai_source=ai_source,
    )
    session.add(new_comment)
    session.commit()
    session.refresh(new_comment)
    return new_comment


def review_ai_comment(session: Session, comment_id: int, action: str, reviewed_by: str = "user") -> Comment:
    comment = (
        session.query(Comment)
        .filter(
            Comment.id == comment_id,
            Comment.comment_type == "ai",
            Comment.status == "pending",
        )
        .first()
    )
    if not comment:
        raise LookupError("Pending AI comment not found")

    comment.status = "approved" if action == "approve" else "rejected"
    comment.reviewed_by = reviewed_by
    # reviewed_at is auto-set via DB default function if using existing logic; keeping simple here
    session.commit()
    session.refresh(comment)
    return comment

