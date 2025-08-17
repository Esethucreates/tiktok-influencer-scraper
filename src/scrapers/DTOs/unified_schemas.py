from dataclasses import dataclass
import time
from typing import Optional, List

from src.scrapers.scraper_parts.commentLoader import CommentsLoadConfig
from src.scrapers.scraper_parts.profileLoader import ProfileLoadConfig


@dataclass
class UnifiedScraperConfig:
    """Master configuration combining all scraper configs"""

    # Search Configuration
    max_profiles_per_hashtag: int = 50
    search_scroll_count: int = 10
    search_scroll_pause: int = 3

    # Profile Loading Configuration
    max_posts_per_profile: int = 50
    profile_scroll_count: int = 25
    profile_scroll_pause_min: float = 2.0
    profile_scroll_pause_max: float = 4.0
    profile_scroll_amount_base: int = 800
    profile_scroll_amount_variation: int = 200

    # Page loading and navigation
    page_load_wait_min: float = 10.0
    page_load_wait_max: float = 15.0
    profile_navigation_delay_min: float = 3.0
    profile_navigation_delay_max: float = 6.0

    # Inter-operation delays
    profile_load_delay_min: float = 10.0
    profile_load_delay_max: float = 15.0

    # Comments Loading Configuration
    max_comments_per_post: int = 100
    max_scroll_attempts_comments: int = 20
    comment_scroll_pause_min: float = 2.0
    comment_scroll_pause_max: float = 5.0
    comment_scroll_amount_base: int = 500
    comment_scroll_amount_variation: int = 100

    # Post navigation and loading
    post_load_wait_min: float = 10.0
    post_load_wait_max: float = 15.0
    post_close_wait_min: float = 2.0
    post_close_wait_max: float = 5.0

    # Human-like interaction settings (shared)
    reading_pause_probability: float = 0.3
    reading_pause_min: float = 1.0
    reading_pause_max: float = 3.0
    scroll_direction_change_probability: float = 0.4
    scroll_up_amount: int = 200

    # Video link detection
    video_link_search_timeout: int = 10
    video_link_scroll_attempts: int = 5
    video_link_scroll_pause: float = 2.0

    # Comment section detection
    comment_section_wait_timeout: int = 15

    # Timing and Resume Configuration
    session_duration_minutes: int = 15  # Maximum session duration in minutes
    grace_period_seconds: int = 60  # Grace period to finish current operation
    check_for_saved_data: bool = True  # Whether to check for saved data on startup
    saved_data_directory: str = "../fileExports/jsonFiles/savedData"

    def to_profile_config(self) -> ProfileLoadConfig:
        """Convert to ProfileLoadConfig"""
        return ProfileLoadConfig(
            max_posts_per_profile=self.max_posts_per_profile,
            scroll_count=self.profile_scroll_count,
            scroll_pause_min=self.profile_scroll_pause_min,
            scroll_pause_max=self.profile_scroll_pause_max,
            scroll_amount_base=self.profile_scroll_amount_base,
            scroll_amount_variation=self.profile_scroll_amount_variation,
            page_load_wait_min=self.page_load_wait_min,
            page_load_wait_max=self.page_load_wait_max,
            profile_navigation_delay_min=self.profile_navigation_delay_min,
            profile_navigation_delay_max=self.profile_navigation_delay_max,
            profile_load_delay_min=self.profile_load_delay_min,
            profile_load_delay_max=self.profile_load_delay_max,
            reading_pause_probability=self.reading_pause_probability,
            reading_pause_min=self.reading_pause_min,
            reading_pause_max=self.reading_pause_max,
            scroll_direction_change_probability=self.scroll_direction_change_probability,
            scroll_up_amount=self.scroll_up_amount
        )

    def to_comments_config(self) -> CommentsLoadConfig:
        """Convert to CommentsLoadConfig"""
        return CommentsLoadConfig(
            max_comments_per_post=self.max_comments_per_post,
            max_scroll_attempts=self.max_scroll_attempts_comments,
            max_posts_per_profile=self.max_posts_per_profile,
            comment_scroll_pause_min=self.comment_scroll_pause_min,
            comment_scroll_pause_max=self.comment_scroll_pause_max,
            comment_scroll_amount_base=self.comment_scroll_amount_base,
            comment_scroll_amount_variation=self.comment_scroll_amount_variation,
            post_load_wait_min=self.post_load_wait_min,
            post_load_wait_max=self.post_load_wait_max,
            post_close_wait_min=self.post_close_wait_min,
            post_close_wait_max=self.post_close_wait_max,
            video_link_search_timeout=self.video_link_search_timeout,
            video_link_scroll_attempts=self.video_link_scroll_attempts,
            video_link_scroll_pause=self.video_link_scroll_pause,
            comment_section_wait_timeout=self.comment_section_wait_timeout,
            reading_pause_probability=self.reading_pause_probability,
            reading_pause_min=self.reading_pause_min,
            reading_pause_max=self.reading_pause_max,
            scroll_direction_change_probability=self.scroll_direction_change_probability,
            scroll_up_amount=self.scroll_up_amount
        )


@dataclass
class SessionState:
    """Track current session state for pause/resume functionality"""
    # Timing
    session_start_time: float
    session_duration_limit: float  # in seconds
    grace_period_start: Optional[float] = None
    should_stop: bool = False

    # Current position tracking
    current_phase: str = "idle"
    hashtags_to_process: List[str] = None
    current_hashtag_index: int = 0
    profiles_to_process: List[str] = None  # user_ids
    current_profile_index: int = 0
    posts_to_process: List[str] = None  # post_ids
    current_post_index: int = 0

    # Resume flags
    resumed_from_saved_data: bool = False
    original_hashtags: List[str] = None

    def time_remaining(self) -> float:
        """Get remaining time in seconds"""
        elapsed = time.time() - self.session_start_time
        return max(0, int(self.session_duration_limit - elapsed))

    def should_continue(self) -> bool:
        """Check if session should continue"""
        if self.should_stop:
            return False

        time_left = self.time_remaining()

        # If we're in grace period, allow completion
        if self.grace_period_start is not None:
            grace_elapsed = time.time() - self.grace_period_start
            return grace_elapsed < 60  # 60 second grace period

        # If time is up, enter grace period
        if time_left <= 0:
            if self.grace_period_start is None:
                self.grace_period_start = time.time()
                print(f"⏰ Time limit reached. Entering grace period to finish current operation...")
            return True  # Allow current operation to finish

        return True


@dataclass
class ProgressTracker:
    """Track progress across all phases"""
    # Current phase
    current_phase: str = "idle"  # idle, search, profiles, posts, comments

    # Search phase
    total_hashtags: int = 0
    current_hashtag_index: int = 0
    current_hashtag: str = ""

    # Profile phase
    total_profiles: int = 0
    current_profile_index: int = 0
    current_profile_username: str = ""

    # Posts phase
    total_posts: int = 0
    current_post_index: int = 0
    current_post_id: str = ""

    # Comments phase
    total_comments_collected: int = 0

    def get_progress_string(self) -> str:
        """Get human-readable progress string"""
        if self.current_phase == "search":
            return f"🔍 Search: {self.current_hashtag_index + 1}/{self.total_hashtags} hashtags ({self.current_hashtag})"
        elif self.current_phase == "profiles":
            return f"👤 Profiles: {self.current_profile_index + 1}/{self.total_profiles} (@{self.current_profile_username})"
        elif self.current_phase == "posts":
            return f"🎬 Posts: {self.current_post_index + 1}/{self.total_posts} (Post: {self.current_post_id})"
        elif self.current_phase == "comments":
            return f"💬 Comments: {self.total_comments_collected} collected"
        else:
            return f"⏸️ {self.current_phase.title()}"