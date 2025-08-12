from pydantic import BaseModel, Field
from typing import Optional


class AddCommentSchema(BaseModel):
    comment_text: str = Field(min_length=1)
    ai_source: Optional[str] = None


class ReviewSchema(BaseModel):
    action: str = Field(pattern=r"^(approve|reject)$")
    reviewed_by: Optional[str] = None

