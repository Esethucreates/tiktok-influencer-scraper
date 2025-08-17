from dataclasses import dataclass
from typing import Dict, Any


@dataclass
class PostData:
    """Data structure for individual TikTok post information"""
    post_id: str
    raw_post_data: Dict[str, Any]


@dataclass
class ProfileLoadConfig:
    """Configuration for profile loading behavior with human-like interaction settings"""
    # Post collection limits
    max_posts_per_profile: int = 700

    # Scrolling behavior
    scroll_count: int = 25
    scroll_pause_min: float = 2.0
    scroll_pause_max: float = 4.0
    scroll_amount_base: int = 800
    scroll_amount_variation: int = 200

    # Page loading and navigation
    page_load_wait_min: float = 8.0
    page_load_wait_max: float = 15.0
    profile_navigation_delay_min: float = 3.0
    profile_navigation_delay_max: float = 6.0

    # Inter-profile delays
    profile_load_delay_min: float = 8.0
    profile_load_delay_max: float = 15.0

    # Human-like interaction settings
    reading_pause_probability: float = 0.3  # 30% chance of extra reading pause
    reading_pause_min: float = 1.0
    reading_pause_max: float = 3.0

    # Scroll variation settings
    scroll_direction_change_probability: float = 0.1  # 10% chance to scroll up briefly
    scroll_up_amount: int = 200

    # Session management
    max_concurrent_profiles: int = 1  # Process profiles sequentially by default
