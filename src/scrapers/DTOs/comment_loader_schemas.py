from dataclasses import dataclass
from typing import Dict, Any

from src.scrapers.DTOs.profile_loader_schemas import ProfileLoadConfig


@dataclass
class Comment:
    cid: str  # Comment ID
    create_time: int  # Timestamp (for freshness/recency)
    post_id: str  # Link to parent post
    user_id: str  # Link to profile owner
    raw_comment_data: Dict[str, Any]


@dataclass
class CommentsLoadConfig(ProfileLoadConfig):
    """Configuration for comments loading behavior extending ProfileLoadConfig"""

    # Comment collection limits
    max_comments_per_post: int = 100
    max_scroll_attempts: int = 20
    max_posts_per_profile: int = 50

    # Comment section scrolling behavior
    comment_scroll_pause_min: float = 1.0
    comment_scroll_pause_max: float = 3.0
    comment_scroll_amount_base: int = 500
    comment_scroll_amount_variation: int = 100

    # Post navigation and loading
    post_load_wait_min: float = 10.0
    post_load_wait_max: float = 15.0
    post_close_wait_min: float = 2.0
    post_close_wait_max: float = 5.0

    # Video link detection
    video_link_search_timeout: int = 10
    video_link_scroll_attempts: int = 5
    video_link_scroll_pause: float = 2.0

    # Comment section detection
    comment_section_wait_timeout: int = 15