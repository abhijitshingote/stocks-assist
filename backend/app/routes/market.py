from flask import Blueprint, jsonify
from sqlalchemy import func
import pytz

from ..models.db import SessionLocal
from ..models.entities import Price


market_bp = Blueprint("market", __name__)


@market_bp.get("/market/latest-date")
def get_latest_date():
    session = SessionLocal()
    try:
        latest_date = session.query(func.max(Price.date)).scalar()
        if not latest_date:
            return jsonify({"latest_date": None})

        latest_timestamp = (
            session.query(func.max(Price.last_traded_timestamp))
            .filter(Price.date == latest_date)
            .scalar()
        )

        if latest_timestamp:
            eastern = pytz.timezone("US/Eastern")
            if latest_timestamp.tzinfo is None:
                eastern_ts = eastern.localize(latest_timestamp)
            else:
                eastern_ts = latest_timestamp.astimezone(eastern)
            formatted = eastern_ts.strftime("%y/%m/%d %I:%M %p EST")
        else:
            formatted = latest_date.strftime("%y/%m/%d")

        return jsonify({"latest_date": formatted})
    finally:
        session.close()

