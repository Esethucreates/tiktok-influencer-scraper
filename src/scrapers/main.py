import csv
import os
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Set

# Assuming imports from your existing modules
from browserConfig import OptimizedNoDriver
from src.scrapers.commentLoader import Comment, CommentsLoadConfig, TikTokCommentsLoader
from src.scrapers.profileLoader import PostData, ProfileLoadConfig, TikTokProfileLoader
from src.scrapers.searchResultsScraper import TikTokSearchScraper, AuthorProfile
from src.services.tiktokAuth import *


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
            return f"🔍 Search: {self.current_hashtag_index + 1}/{self.total_hashtags} hashtags (#{self.current_hashtag})"
        elif self.current_phase == "profiles":
            return f"👤 Profiles: {self.current_profile_index + 1}/{self.total_profiles} (@{self.current_profile_username})"
        elif self.current_phase == "posts":
            return f"🎬 Posts: {self.current_post_index + 1}/{self.total_posts} (Post: {self.current_post_id})"
        elif self.current_phase == "comments":
            return f"💬 Comments: {self.total_comments_collected} collected"
        else:
            return f"⏸️ {self.current_phase.title()}"


class UnifiedTikTokScraper:
    """
    Unified TikTok Scraper that manages the complete workflow:
    Search → Profiles → Posts → Comments in one browser session

    Maintains all relationships and provides multiple entry points.
    """

    def __init__(self, config: UnifiedScraperConfig = None):
        """Initialize the unified scraper"""
        self.config = config or UnifiedScraperConfig()

        # Initialize component scrapers with session management disabled
        # Search scraper will be initialized when hashtags are available
        self.search_scraper = None
        self.profile_loader = TikTokProfileLoader(self.config.to_profile_config())
        self.comments_loader = TikTokCommentsLoader(self.config.to_comments_config())

        # Session state
        self.session_active = False
        self.browser_initialized = False

        # Progress tracking
        self.progress = ProgressTracker()

        # Data collections with cross-references
        self.hashtags_data: Dict[str, Dict[str, Any]] = {}  # hashtag -> metadata
        self.profiles_data: Dict[str, Any] = {}  # user_id -> profile
        self.posts_data: Dict[str, PostData] = {}  # post_id -> post
        self.comments_data: Dict[str, Comment] = {}  # comment_id -> comment

        # Relationship mappings (bidirectional)
        self.hashtag_to_profiles: Dict[str, Set[str]] = {}  # hashtag -> user_ids
        self.profile_to_hashtags: Dict[str, Set[str]] = {}  # user_id -> hashtags
        self.profile_to_posts: Dict[str, Set[str]] = {}  # user_id -> post_ids
        self.post_to_profile: Dict[str, str] = {}  # post_id -> user_id
        self.post_to_comments: Dict[str, Set[str]] = {}  # post_id -> comment_ids
        self.comment_to_post: Dict[str, str] = {}  # comment_id -> post_id
        self.comment_to_profile: Dict[str, str] = {}  # comment_id -> user_id (post owner)

        # Error tracking
        self.failed_hashtags: List[str] = []
        self.failed_profiles: List[str] = []
        self.failed_posts: List[str] = []
        self.error_log: List[Dict[str, Any]] = []

    def _initialize_search_scraper(self, hashtags: List[str]):
        """Initialize search scraper with hashtags and matching config"""
        if self.search_scraper is None:
            print(f"🔧 Initializing search scraper with {len(hashtags)} hashtags")

            # Use TikTok search URL pattern from the original class
            search_url_pattern = r"https://www\.tiktok\.com/api/search/general/full/\?[^ ]+"

            self.search_scraper = TikTokSearchScraper(
                hashtags=hashtags,
                max_profiles_per_hashtag=self.config.max_profiles_per_hashtag,
                search_url_pattern=search_url_pattern,
                scroll_count=self.config.search_scroll_count,
                scroll_pause=self.config.search_scroll_pause
            )
            print("✅ Search scraper initialized")

    async def start_session(self):
        """Start unified browser session"""
        if self.session_active:
            print("🔄 Unified scraper session already active")
            return

        try:
            print("🚀 Starting unified TikTok scraping session...")

            # Start session using profile loader (which handles authentication)
            await self.profile_loader.start_session()

            # Share browser session with other scrapers
            await self._share_browser_session()

            self.session_active = True
            self.browser_initialized = True

            print("✅ Unified scraping session started successfully")

        except Exception as e:
            print(f"❌ Error starting unified session: {e}")
            await self.cleanup_session()
            raise

    async def _share_browser_session(self):
        """Share browser session across all component scrapers"""
        try:
            # Share with search scraper (if initialized)
            if self.search_scraper is not None:
                await self._share_session_with_search_scraper()

            # Share with comments loader (using inheritance method)
            self.comments_loader.inherit_session_from_profile_loader(self.profile_loader)

            print("✅ Browser session shared across all scrapers")

        except Exception as e:
            print(f"❌ Error sharing browser session: {e}")
            raise

    async def _share_session_with_search_scraper(self):
        """Share session specifically with search scraper"""
        if self.search_scraper is None:
            return

        try:
            # Share browser components
            self.search_scraper.browser = self.profile_loader.browser
            self.search_scraper.page = self.profile_loader.page
            self.search_scraper.auth = self.profile_loader.auth
            self.search_scraper.is_authenticated = self.profile_loader.is_authenticated
            self.search_scraper.is_running = True

            # Disable search scraper's own session management
            self.search_scraper.session_active = True
            self.search_scraper.session_initialized = True

            # Set up response monitoring for search scraper
            if hasattr(self.search_scraper, 'page') and self.search_scraper.page:
                self.search_scraper.page.add_handler(
                    uc.cdp.network.ResponseReceived,
                    self.search_scraper.on_response_received
                )

            print("✅ Session shared with search scraper")

        except Exception as e:
            print(f"❌ Error sharing session with search scraper: {e}")
            raise

    async def end_session(self):
        """End unified browser session"""
        if not self.session_active:
            print("No active unified session to end")
            return

        try:
            print("🔄 Ending unified scraping session...")
            await self.cleanup_session()
            print("✅ Unified scraping session ended successfully")

        except Exception as e:
            print(f"❌ Error ending unified session: {e}")
            await self.cleanup_session()

    async def cleanup_session(self):
        """Clean up all session resources"""
        self.session_active = False
        self.browser_initialized = False

        # Clean up search scraper session state (but don't call its cleanup methods)
        if self.search_scraper is not None:
            self.search_scraper.session_active = False
            self.search_scraper.session_initialized = False

        # Clean up profile loader (which owns the browser)
        if hasattr(self.profile_loader, 'session_active') and self.profile_loader.session_active:
            await self.profile_loader.end_session()

    def _log_error(self, phase: str, error_type: str, message: str,
                   item_id: str = None, exception: Exception = None):
        """Log error for later analysis"""
        error_entry = {
            'timestamp': datetime.now().isoformat(),
            'phase': phase,
            'error_type': error_type,
            'message': message,
            'item_id': item_id,
            'exception_str': str(exception) if exception else None
        }
        self.error_log.append(error_entry)
        print(f"❌ {phase.upper()} - {error_type}: {message}")

    def _update_progress(self, phase: str, **kwargs):
        """Update progress tracker"""
        self.progress.current_phase = phase
        for key, value in kwargs.items():
            if hasattr(self.progress, key):
                setattr(self.progress, key, value)
        print(f"📊 {self.progress.get_progress_string()}")

    def _add_relationships(self, hashtag: str = None, profile: AuthorProfile = None,
                           post: PostData = None, comment: Comment = None):
        """Add bidirectional relationships between entities"""

        # Hashtag ↔ Profile relationships
        if hashtag and profile:
            user_id = profile.user_id

            if hashtag not in self.hashtag_to_profiles:
                self.hashtag_to_profiles[hashtag] = set()
            self.hashtag_to_profiles[hashtag].add(user_id)

            if user_id not in self.profile_to_hashtags:
                self.profile_to_hashtags[user_id] = set()
            self.profile_to_hashtags[user_id].add(hashtag)

        # Profile ↔ Post relationships
        if profile and post:
            user_id = profile.user_id
            post_id = post.post_id

            if user_id not in self.profile_to_posts:
                self.profile_to_posts[user_id] = set()
            self.profile_to_posts[user_id].add(post_id)

            self.post_to_profile[post_id] = user_id

        # Post ↔ Comment relationships
        if post and comment:
            post_id = post.post_id
            comment_id = comment.cid

            if post_id not in self.post_to_comments:
                self.post_to_comments[post_id] = set()
            self.post_to_comments[post_id].add(comment_id)

            self.comment_to_post[comment_id] = post_id

            # Comment → Profile (post owner) relationship
            if post_id in self.post_to_profile:
                profile_user_id = self.post_to_profile[post_id]
                self.comment_to_profile[comment_id] = profile_user_id

    async def run_full_workflow(self, hashtags: List[str]) -> Dict[str, Any]:
        """
        Complete end-to-end workflow: Search → Profiles → Posts → Comments

        Args:
            hashtags: List of hashtags to search

        Returns:
            Complete results dictionary
        """
        if not self.session_active:
            raise RuntimeError("Session not active. Call start_session() first.")

        print(f"\n🚀 Starting full TikTok scraping workflow for {len(hashtags)} hashtags")
        print(f"🏷️  Hashtags: {', '.join(hashtags)}")

        try:
            # Initialize search scraper with hashtags
            self._initialize_search_scraper(hashtags)

            # Share session with newly initialized search scraper
            await self._share_session_with_search_scraper()

            # Phase 1: Search
            self._update_progress("search", total_hashtags=len(hashtags))
            await self._run_search_phase(hashtags)

            # Phase 2: Load Profiles & Posts
            await self._run_profiles_phase()

            # Phase 3: Load Comments
            await self._run_comments_phase()

            print(f"\n🎉 Full workflow completed successfully!")
            return self.get_complete_results()

        except Exception as e:
            self._log_error("WORKFLOW", "CRITICAL_ERROR", f"Full workflow failed: {e}", exception=e)
            raise

    async def run_from_profiles(self, profiles: List[AuthorProfile],
                                hashtag_mapping: Dict[str, List[str]] = None) -> Dict[str, Any]:
        """
        Workflow starting from profiles: Profiles → Posts → Comments

        Args:
            profiles: List of AuthorProfile objects
            hashtag_mapping: Optional mapping of user_id to hashtags

        Returns:
            Results dictionary
        """
        if not self.session_active:
            raise RuntimeError("Session not active. Call start_session() first.")

        print(f"\n🚀 Starting workflow from {len(profiles)} profiles")

        try:
            # Set up profiles data
            for profile in profiles:
                self.profiles_data[profile.user_id] = profile

                # Add hashtag relationships if provided
                if hashtag_mapping and profile.user_id in hashtag_mapping:
                    hashtags = hashtag_mapping[profile.user_id]
                    for hashtag in hashtags:
                        self._add_relationships(hashtag=hashtag, profile=profile)

            # Phase 2: Load Posts
            await self._run_profiles_phase()

            # Phase 3: Load Comments
            await self._run_comments_phase()

            print(f"\n🎉 Profile-based workflow completed successfully!")
            return self.get_complete_results()

        except Exception as e:
            self._log_error("WORKFLOW", "CRITICAL_ERROR", f"Profile workflow failed: {e}", exception=e)
            raise

    async def run_from_posts(self, posts: List[PostData],
                             profile_mapping: Dict[str, AuthorProfile] = None) -> Dict[str, Any]:
        """
        Workflow starting from posts: Comments only

        Args:
            posts: List of PostData objects
            profile_mapping: Optional mapping of user_id to AuthorProfile

        Returns:
            Results dictionary
        """
        if not self.session_active:
            raise RuntimeError("Session not active. Call start_session() first.")

        print(f"\n🚀 Starting comments-only workflow for {len(posts)} posts")

        try:
            # Set up posts data
            for post in posts:
                self.posts_data[post.post_id] = post

                # Add profile relationships if provided
                if profile_mapping:
                    # Find profile for this post (you may need to adjust this logic)
                    for user_id, profile in profile_mapping.items():
                        self.profiles_data[user_id] = profile
                        self._add_relationships(profile=profile, post=post)

            # Phase 3: Load Comments
            await self._run_comments_phase()

            print(f"\n🎉 Comments-only workflow completed successfully!")
            return self.get_complete_results()

        except Exception as e:
            self._log_error("WORKFLOW", "CRITICAL_ERROR", f"Comments workflow failed: {e}", exception=e)
            raise

    async def _run_search_phase(self, hashtags: List[str]):
        """Execute search phase using TikTokSearchScraper"""
        print(f"\n{'=' * 60}")
        print(f"🔍 PHASE 1: SEARCHING HASHTAGS")
        print(f"{'=' * 60}")

        if self.search_scraper is None:
            raise RuntimeError("Search scraper not initialized. This should not happen.")

        try:
            # Use the search scraper's search_all_hashtags method
            print(f"🏷️  Searching all {len(hashtags)} hashtags...")
            search_results = await self.search_scraper.search_all_hashtags()

            # DEBUG: Print what we got back
            print(f"🔍 Search results type: {type(search_results)}")
            print(
                f"🔍 Search results keys: {list(search_results.keys()) if isinstance(search_results, dict) else 'Not a dict'}")

            total_profiles_found = 0

            # Process results from each hashtag

            for i, hashtag in enumerate(hashtags):
                self._update_progress("search", current_hashtag_index=i, current_hashtag=hashtag)

                try:
                    # Get profiles for this hashtag from search results
                    tag = f"#{hashtag}"
                    print(f"This is the current hashtag {tag}")
                    profiles_for_hashtag = search_results.get(tag, [])

                    # DEBUG: Print what we found
                    print(f"🔍 Found {len(profiles_for_hashtag)} profiles for #{hashtag}")

                    # Store hashtag metadata
                    self.hashtags_data[hashtag] = {
                        'search_timestamp': datetime.now().isoformat(),
                        'profiles_found': len(profiles_for_hashtag)
                    }

                    # Process found profiles
                    if profiles_for_hashtag:
                        for profile in profiles_for_hashtag:
                            user_id = profile.user_id
                            # Store profile data with timestamp
                            self.profiles_data[user_id] = {
                                'profile': profile,
                                'search_timestamp': datetime.now().isoformat(),
                                'profile_url': f"https://www.tiktok.com/@{profile.username}",
                                'found_under_hashtags': set()  # Will be populated via relationships
                            }
                            self._add_relationships(hashtag=hashtag, profile=profile)
                            total_profiles_found += 1

                        print(f"✅ Stored {len(profiles_for_hashtag)} profiles for #{hashtag}")
                    else:
                        print(f"❌ No profiles found for #{hashtag}")
                        self.failed_hashtags.append(hashtag)

                except Exception as e:
                    self._log_error("SEARCH", "HASHTAG_PROCESS_ERROR", f"Failed to process #{hashtag}", hashtag, e)
                    self.failed_hashtags.append(hashtag)
                    continue

            # Update found_under_hashtags for each profile
            for user_id in self.profiles_data:
                hashtags_for_profile = self.profile_to_hashtags.get(user_id, set())
                self.profiles_data[user_id]['found_under_hashtags'] = hashtags_for_profile

            # DEBUG: Final count
            print(f"🔍 Total profiles stored: {len(self.profiles_data)}")
            print(f"🔍 Profile user_ids: {list(self.profiles_data.keys())[:5]}...")  # Show first 5

        except Exception as e:
            self._log_error("SEARCH", "SEARCH_PHASE_ERROR", f"Search phase failed: {e}", exception=e)
            raise

        total_profiles = len(self.profiles_data)
        print(f"\n🎯 Search phase completed: {total_profiles} unique profiles found")

    async def _run_profiles_phase(self):
        """Execute profiles & posts loading phase"""
        if not self.profiles_data:
            print("❌ No profiles to process")
            return

        print(f"\n{'=' * 60}")
        print(f"👤 PHASE 2: LOADING PROFILES & POSTS")
        print(f"{'=' * 60}")

        # Extract actual AuthorProfile objects from stored data
        profiles_list = [profile_data['profile'] for profile_data in self.profiles_data.values()]
        self._update_progress("profiles", total_profiles=len(profiles_list))

        # Set up profile loader
        self.profile_loader.set_profiles_to_load(profiles_list, self.profile_to_hashtags)

        for i, profile_data in enumerate(self.profiles_data.values()):
            profile = profile_data['profile']
            user_id = profile.user_id

            self._update_progress("profiles", current_profile_index=i,
                                  current_profile_username=profile.username)

            try:
                print(f"\n👤 Loading posts for @{profile.username}")

                # Track load timestamp
                load_start_time = datetime.now().isoformat()

                # Load posts for this profile
                posts = await self.profile_loader.load_profile_posts(profile)

                if posts:
                    # Update profile data with load info
                    self.profiles_data[user_id].update({
                        'posts_count': len(posts),
                        'load_timestamp': load_start_time
                    })

                    # Store posts and relationships
                    for post in posts:
                        self.posts_data[post.post_id] = post
                        self._add_relationships(profile=profile, post=post)

                    print(f"✅ Loaded {len(posts)} posts for @{profile.username}")
                else:
                    # Still update load info even if no posts
                    self.profiles_data[user_id].update({
                        'posts_count': 0,
                        'load_timestamp': load_start_time
                    })
                    print(f"❌ No posts loaded for @{profile.username}")
                    self.failed_profiles.append(user_id)

            except Exception as e:
                self._log_error("PROFILES", "PROFILE_ERROR", f"Failed to load @{profile.username}",
                                user_id, e)
                # Still update with error info
                self.profiles_data[user_id].update({
                    'posts_count': 0,
                    'load_timestamp': datetime.now().isoformat(),
                    'load_error': str(e)
                })
                self.failed_profiles.append(user_id)
                continue

        total_posts = len(self.posts_data)
        print(f"\n🎯 Profiles phase completed: {total_posts} posts loaded")

    async def _run_comments_phase(self):
        """Execute comments loading phase"""
        if not self.posts_data:
            print("❌ No posts to process for comments")
            return

        print(f"\n{'=' * 60}")
        print(f"💬 PHASE 3: LOADING COMMENTS")
        print(f"{'=' * 60}")

        posts_list = list(self.posts_data.values())
        self._update_progress("comments", total_posts=len(posts_list))

        # Group posts by profile for efficient processing
        posts_by_profile = {}
        for post in posts_list:
            if post.post_id in self.post_to_profile:
                user_id = self.post_to_profile[post.post_id]
                if user_id not in posts_by_profile:
                    posts_by_profile[user_id] = []
                posts_by_profile[user_id].append(post)

        # Process comments for each profile's posts
        for user_id, user_posts in posts_by_profile.items():
            if user_id not in self.profiles_data:
                continue

            profile_data = self.profiles_data[user_id]
            profile = profile_data['profile']

            print(f"\n💬 Loading comments for @{profile.username} posts")

            try:
                # Set up comments loader for this profile
                self.comments_loader.load_posts_from_profile_loader(self.profile_loader, user_id)

                # Load comments for all posts
                comments_results = await self.comments_loader.load_all_comments()

                # Process and store comments
                for post_id, comments in comments_results.items():
                    if post_id in self.posts_data:
                        post = self.posts_data[post_id]

                        for comment in comments:
                            self.comments_data[comment.cid] = comment
                            self._add_relationships(post=post, comment=comment)

                        print(f"💬 Loaded {len(comments)} comments for post {post_id}")

                total_comments_for_profile = sum(len(comments) for comments in comments_results.values())
                self._update_progress("comments",
                                      total_comments_collected=self.progress.total_comments_collected + total_comments_for_profile)

            except Exception as e:
                self._log_error("COMMENTS", "COMMENTS_ERROR", f"Failed to load comments for @{profile.username}",
                                user_id, e)
                continue

        total_comments = len(self.comments_data)
        print(f"\n🎯 Comments phase completed: {total_comments} comments loaded")

    def get_relationship_summary(self) -> Dict[str, Any]:
        """Get summary of all relationships"""
        return {
            'hashtags_count': len(self.hashtags_data),
            'profiles_count': len(self.profiles_data),
            'posts_count': len(self.posts_data),
            'comments_count': len(self.comments_data),
            'relationships': {
                'hashtag_to_profiles': {ht: len(profiles) for ht, profiles in self.hashtag_to_profiles.items()},
                'profiles_with_posts': len([uid for uid in self.profile_to_posts.keys()]),
                'posts_with_comments': len([pid for pid in self.post_to_comments.keys()]),
                'avg_posts_per_profile': len(self.posts_data) / len(self.profiles_data) if self.profiles_data else 0,
                'avg_comments_per_post': len(self.comments_data) / len(self.posts_data) if self.posts_data else 0,
            },
            'failed_items': {
                'failed_hashtags': len(self.failed_hashtags),
                'failed_profiles': len(self.failed_profiles),
                'failed_posts': len(self.failed_posts),
                'total_errors': len(self.error_log)
            }
        }

    def get_flattened_data(self) -> Dict[str, Any]:
        """Get data flattened for database storage"""

        # Flatten profiles data
        profiles_flat = []
        for user_id, profile_data in self.profiles_data.items():
            profile = profile_data['profile']
            flat_profile = {
                'user_id': user_id,
                'username': profile.username,
                'display_name': profile.display_name,
                'avatar_url': profile.avatar_url,
                'verified': profile.verified,
                'follower_count': profile.follower_count,
                'following_count': profile.following_count,
                'heart_count': profile.heart_count,
                'video_count': profile.video_count,
                'posts_count': profile_data.get('posts_count', 0),
                'load_timestamp': profile_data.get('load_timestamp', profile_data.get('search_timestamp')),
                'profile_url': profile_data.get('profile_url', f"https://www.tiktok.com/@{profile.username}"),
                'found_under_hashtags': list(profile_data.get('found_under_hashtags', [])),
                'raw_author_data': getattr(profile, 'raw_author_data', None),
                'raw_author_stats': getattr(profile, 'raw_author_stats', None)
            }
            profiles_flat.append(flat_profile)

        # Flatten posts data
        posts_flat = []
        for post_id, post in self.posts_data.items():
            user_id = self.post_to_profile.get(post_id)
            flat_post = {
                'post_id': post.post_id,
                'user_id': user_id,
                'author_stats': getattr(post, 'author_stats', None),
                'author_stats_v2': getattr(post, 'author_stats_v2', None),
                'contents': getattr(post, 'contents', None),
                'challenges': getattr(post, 'challenges', None),
                'text_extra': getattr(post, 'text_extra', None),
                'raw_post_data': getattr(post, 'raw_post_data', None)
            }
            posts_flat.append(flat_post)

        # Profile-hashtag relationships
        profile_hashtag_relations = []
        for user_id, hashtags in self.profile_to_hashtags.items():
            for hashtag in hashtags:
                profile_hashtag_relations.append({
                    'user_id': user_id,
                    'hashtag': hashtag
                })

        # Flatten comments data (CSV-friendly format)
        comments_flat = []
        for comment_id, comment in self.comments_data.items():
            post_id = self.comment_to_post.get(comment_id)
            profile_user_id = self.comment_to_profile.get(comment_id)
            profile_data = self.profiles_data.get(profile_user_id, {})
            profile = profile_data.get('profile') if profile_data else None

            flat_comment = {
                'comment_id': comment.cid,
                'post_id': post_id,
                'user_id': getattr(comment, 'user_id', None),
                'comment_text': comment.text,
                'create_time': comment.create_time,
                'digg_count': comment.digg_count,
                'is_author_digged': getattr(comment, 'is_author_digged', None),
                'is_comment_translatable': getattr(comment, 'is_comment_translatable', None),
                'comment_language': getattr(comment, 'comment_language', None),
                'commenter_uid': getattr(comment.user, 'uid', None) if hasattr(comment, 'user') else None,
                'commenter_nickname': getattr(comment.user, 'nickname', None) if hasattr(comment, 'user') else None,
                'commenter_sec_uid': getattr(comment.user, 'sec_uid', None) if hasattr(comment, 'user') else None,
                'commenter_unique_id': getattr(comment.user, 'unique_id', None) if hasattr(comment, 'user') else None,
                'profile_username': profile.username if profile else '',
                'profile_display_name': profile.display_name if profile else ''
            }
            comments_flat.append(flat_comment)

        return {
            'profiles': profiles_flat,
            'posts': posts_flat,
            'comments': comments_flat,
            'profile_hashtag_relations': profile_hashtag_relations,
            'load_summary': self.get_relationship_summary()
        }

    def get_complete_results(self) -> Dict[str, Any]:
        """Get complete results with all data and relationships"""
        return {
            'metadata': {
                'scrape_timestamp': datetime.now().isoformat(),
                'config_used': self.config.__dict__,
                'progress_final': self.progress.__dict__
            },
            'summary': self.get_relationship_summary(),
            'data': {
                'hashtags': self.hashtags_data,
                'profiles': {uid: profile['profile'].__dict__ for uid, profile in self.profiles_data.items()},
                'posts': {pid: post.__dict__ for pid, post in self.posts_data.items()},
                'comments': {cid: comment.__dict__ for cid, comment in self.comments_data.items()}
            },
            'relationships': {
                'hashtag_to_profiles': {ht: list(profiles) for ht, profiles in self.hashtag_to_profiles.items()},
                'profile_to_hashtags': {uid: list(hashtags) for uid, hashtags in self.profile_to_hashtags.items()},
                'profile_to_posts': {uid: list(posts) for uid, posts in self.profile_to_posts.items()},
                'post_to_profile': self.post_to_profile,
                'post_to_comments': {pid: list(comments) for pid, comments in self.post_to_comments.items()},
                'comment_to_post': self.comment_to_post,
                'comment_to_profile': self.comment_to_profile
            },
            'errors': {
                'failed_hashtags': self.failed_hashtags,
                'failed_profiles': self.failed_profiles,
                'failed_posts': self.failed_posts,
                'error_log': self.error_log
            }
        }

    @staticmethod
    def save_as_csv_files(unified_scraper_results: Dict[str, Any], output_dir: str = "tiktok_csv_export") -> Dict[
        str, str]:
        """
        Convert unified scraper JSON results to multiple CSV files

        Args:
            unified_scraper_results: Results from UnifiedTikTokScraper.get_flattened_data()
            output_dir: Directory to save CSV files

        Returns:
            Dictionary mapping entity type to CSV file path
        """
        # Create output directory
        Path(output_dir).mkdir(exist_ok=True)
        csv_files = {}

        try:
            # 1. Profiles CSV
            if 'profiles' in unified_scraper_results and unified_scraper_results['profiles']:
                profiles_file = os.path.join(output_dir, "profiles.csv")
                with open(profiles_file, 'w', newline='', encoding='utf-8') as f:
                    if unified_scraper_results['profiles']:
                        writer = csv.DictWriter(f, fieldnames=unified_scraper_results['profiles'][0].keys())
                        writer.writeheader()
                        writer.writerows(unified_scraper_results['profiles'])
                csv_files['profiles'] = profiles_file
                print(f"✅ Saved {len(unified_scraper_results['profiles'])} profiles to {profiles_file}")

            # 2. Posts CSV
            if 'posts' in unified_scraper_results and unified_scraper_results['posts']:
                posts_file = os.path.join(output_dir, "posts.csv")
                with open(posts_file, 'w', newline='', encoding='utf-8') as f:
                    if unified_scraper_results['posts']:
                        writer = csv.DictWriter(f, fieldnames=unified_scraper_results['posts'][0].keys())
                        writer.writeheader()
                        writer.writerows(unified_scraper_results['posts'])
                csv_files['posts'] = posts_file
                print(f"✅ Saved {len(unified_scraper_results['posts'])} posts to {posts_file}")

            # 3. Comments CSV
            if 'comments' in unified_scraper_results and unified_scraper_results['comments']:
                comments_file = os.path.join(output_dir, "comments.csv")
                with open(comments_file, 'w', newline='', encoding='utf-8') as f:
                    if unified_scraper_results['comments']:
                        writer = csv.DictWriter(f, fieldnames=unified_scraper_results['comments'][0].keys())
                        writer.writeheader()
                        writer.writerows(unified_scraper_results['comments'])
                csv_files['comments'] = comments_file
                print(f"✅ Saved {len(unified_scraper_results['comments'])} comments to {comments_file}")

            # 4. Profile-Hashtag Relations CSV
            if 'profile_hashtag_relations' in unified_scraper_results and unified_scraper_results[
                'profile_hashtag_relations']:
                relations_file = os.path.join(output_dir, "profile_hashtag_relations.csv")
                with open(relations_file, 'w', newline='', encoding='utf-8') as f:
                    if unified_scraper_results['profile_hashtag_relations']:
                        writer = csv.DictWriter(f, fieldnames=unified_scraper_results['profile_hashtag_relations'][
                            0].keys())
                        writer.writeheader()
                        writer.writerows(unified_scraper_results['profile_hashtag_relations'])
                csv_files['profile_hashtag_relations'] = relations_file
                print(
                    f"✅ Saved {len(unified_scraper_results['profile_hashtag_relations'])} relations to {relations_file}")

            # 5. Summary CSV
            if 'load_summary' in unified_scraper_results:
                summary_file = os.path.join(output_dir, "scrape_summary.csv")
                summary_data = unified_scraper_results['load_summary']

                # Flatten summary for CSV
                flattened_summary = []
                for key, value in summary_data.items():
                    if isinstance(value, dict):
                        for sub_key, sub_value in value.items():
                            flattened_summary.append({
                                'category': key,
                                'metric': sub_key,
                                'value': sub_value
                            })
                    else:
                        flattened_summary.append({
                            'category': 'general',
                            'metric': key,
                            'value': value
                        })

                with open(summary_file, 'w', newline='', encoding='utf-8') as f:
                    writer = csv.DictWriter(f, fieldnames=['category', 'metric', 'value'])
                    writer.writeheader()
                    writer.writerows(flattened_summary)
                csv_files['summary'] = summary_file
                print(f"✅ Saved scrape summary to {summary_file}")

            print(f"\n🎉 All CSV files saved to directory: {output_dir}")
            return csv_files

        except Exception as e:
            print(f"❌ Error saving CSV files: {e}")
            raise

    def save_results(self, filename: str = "unified_tiktok_scrape_results.json"):
        """Save complete flattened results to JSON file"""
        results = self.get_flattened_data()
        OptimizedNoDriver.save_json_to_file(results, filename)
        self.save_as_csv_files(results)
        print(f"💾 Flattened results saved to {filename}")
        return results

    async def run_complete_session(self, hashtags: List[str]) -> Dict[str, Any] | None:
        """
        Convenience method that manages complete session lifecycle

        Args:
            hashtags: List of hashtags to search

        Returns:
            Complete results dictionary
        """
        try:
            await self.start_session()
            results = await self.run_full_workflow(hashtags)
            return results
        finally:
            await self.end_session()


# Usage example:
async def main():
    # Initialize unified scraper
    config = UnifiedScraperConfig(

        # Search results
        max_profiles_per_hashtag=2,
        search_scroll_count=3,

        # Profile scraping
        max_posts_per_profile=2,
        profile_scroll_count=2,
        profile_load_delay_min=10.0,
        profile_load_delay_max=20.0,
        page_load_wait_min=15.0,
        page_load_wait_max=20.0,
        reading_pause_probability=0.8,

        max_comments_per_post=10,
        max_scroll_attempts_comments=3,

        # Comment section scrolling behavior
        comment_scroll_pause_min=2.0,
        comment_scroll_pause_max=5.0,
        comment_scroll_amount_base=500,
        comment_scroll_amount_variation=100,
    )

    scraper = UnifiedTikTokScraper(config)

    # Option 1: Complete workflow
    hashtags = ["travel"]
    results = await scraper.run_complete_session(hashtags)
    scraper.save_results()


if __name__ == "__main__":
    asyncio.run(main())
