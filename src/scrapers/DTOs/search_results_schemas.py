from dataclasses import dataclass
from typing import Dict, Any


@dataclass
class AuthorProfile:
    user_id: str
    username: str
    display_name: str
    avatar_url: str
    verified: bool
    follower_count: int
    following_count: int
    heart_count: int
    video_count: int
    raw_author_data: Dict[str, Any] | None = None