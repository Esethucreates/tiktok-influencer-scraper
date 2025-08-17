import csv
import glob
import json
import os
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Set

# Assuming imports from your existing modules

from src.scrapers.DTOs.unified_schemas import UnifiedScraperConfig, ProgressTracker, SessionState
from src.scrapers.core_parts.browserConfig import OptimizedNoDriver
from src.scrapers.scraper_parts.commentLoader import Comment, TikTokCommentsLoader
from src.scrapers.scraper_parts.profileLoader import PostData, TikTokProfileLoader
from src.scrapers.scraper_parts.searchResultsScraper import TikTokSearchScraper, AuthorProfile
from src.services.tiktokAuth import *


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

        # NEW: Add session state tracking
        self.session_state: Optional[SessionState] = None
        self.saved_data_file: Optional[str] = None

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

    def _initialize_session_state(self, hashtags: List[str]):
        """Initialize session state for timing and progress tracking"""
        duration_seconds = self.config.session_duration_minutes * 60

        self.session_state = SessionState(
            session_start_time=time.time(),
            session_duration_limit=duration_seconds,
            hashtags_to_process=hashtags.copy(),
            original_hashtags=hashtags.copy()
        )

        print(f"⏰ Session initialized with {self.config.session_duration_minutes} minute limit")

    def _check_time_and_save_if_needed(self) -> bool:
        """Check if we should continue or save and stop. Returns False if should stop."""
        if not self.session_state or self.session_state.should_continue():
            return True

        print(f"💾 Time limit reached. Saving current progress...")
        self._save_current_state()
        return False

    def _get_saved_data_files(self) -> List[str]:
        """Get list of saved data files sorted by timestamp (newest first)"""
        import os

        saved_dir = self.config.saved_data_directory
        if not os.path.exists(saved_dir):
            return []

        pattern = os.path.join(saved_dir, "tiktok_scraper_state_*.json")
        files = glob.glob(pattern)

        # Sort by modification time (newest first)
        files.sort(key=os.path.getmtime, reverse=True)
        return files

    def _load_saved_state(self) -> bool:
        """Load the most recent saved state. Returns True if data was loaded."""
        if not self.config.check_for_saved_data:
            print("🔄 Skipping saved data check (disabled in config)")
            return False

        saved_files = self._get_saved_data_files()
        if not saved_files:
            print("🔄 No saved data found. Starting fresh scrape.")
            return False

        try:
            latest_file = saved_files[0]
            print(f"📂 Found saved data: {os.path.basename(latest_file)}")

            with open(latest_file, 'r', encoding='utf-8') as f:
                saved_data = json.load(f)

            # Restore data collections
            self._restore_from_saved_data(saved_data)

            # Set the file reference for cleanup later
            self.saved_data_file = latest_file

            print(f"✅ Successfully loaded saved state from {os.path.basename(latest_file)}")
            return True

        except Exception as e:
            print(f"❌ Error loading saved data: {e}")
            print("🔄 Starting fresh scrape instead.")
            return False

    def _restore_from_saved_data(self, saved_data: dict):
        """Restore scraper state from saved data"""

        # Restore data collections
        metadata = saved_data.get('metadata', {})
        data = saved_data.get('data', {})
        relationships = saved_data.get('relationships', {})

        # Restore hashtags data
        self.hashtags_data = data.get('hashtags', {})

        # Restore profiles data (need to reconstruct AuthorProfile objects)
        profiles_data = data.get('profiles', {})
        for user_id, profile_dict in profiles_data.items():
            # Create AuthorProfile from saved dict
            profile = AuthorProfile(
                user_id=profile_dict['user_id'],
                username=profile_dict['username'],
                display_name=profile_dict['display_name'],
                avatar_url=profile_dict.get('avatar_url'),
                verified=profile_dict.get('verified', False),
                follower_count=profile_dict.get('follower_count', 0),
                following_count=profile_dict.get('following_count', 0),
                heart_count=profile_dict.get('heart_count', 0),
                video_count=profile_dict.get('video_count', 0),
                raw_author_data=None
            )

            self.profiles_data[user_id] = {
                'profile': profile,
                'search_timestamp': metadata.get('scrape_timestamp'),
                'profile_url': f"https://www.tiktok.com/@{profile.username}",
                'found_under_hashtags': set()
            }

        # Restore posts data (need to reconstruct PostData objects)
        posts_data = data.get('posts', {})
        for post_id, post_dict in posts_data.items():
            post = PostData(
                post_id=post_dict['post_id'],
                raw_post_data=post_dict.get('raw_post_data')
            )
            self.posts_data[post_id] = post

        # Restore comments data (need to reconstruct Comment objects)
        comments_data = data.get('comments', {})
        for comment_id, comment_dict in comments_data.items():
            post_id = self.comment_to_post.get(comment_id)
            comment = Comment(
                post_id=post_id,
                user_id=getattr(comment_dict, 'user_id', None),
                cid=comment_dict['cid'],
                raw_comment_data=comment_dict.get('raw_comment_data'),
                create_time=comment_dict.get('create_time')
            )
            self.comments_data[comment_id] = comment

        # Restore relationships
        for attr_name, relationship_dict in relationships.items():
            if hasattr(self, attr_name):
                if attr_name in ['hashtag_to_profiles', 'profile_to_posts', 'post_to_comments']:
                    # Convert lists back to sets
                    setattr(self, attr_name, {k: set(v) for k, v in relationship_dict.items()})
                elif attr_name in ['profile_to_hashtags']:
                    # Convert lists back to sets
                    setattr(self, attr_name, {k: set(v) for k, v in relationship_dict.items()})
                else:
                    # Direct assignment for simple dict mappings
                    setattr(self, attr_name, relationship_dict)

        # Update found_under_hashtags for profiles
        for user_id in self.profiles_data:
            hashtags_for_profile = self.profile_to_hashtags.get(user_id, set())
            self.profiles_data[user_id]['found_under_hashtags'] = hashtags_for_profile

        print(
            f"📊 Restored: {len(self.profiles_data)} profiles, {len(self.posts_data)} posts, {len(self.comments_data)} comments")

    def _save_current_state(self):
        """Save current scraper state to JSON file"""
        try:
            # Create saved data directory
            Path(self.config.saved_data_directory).mkdir(parents=True, exist_ok=True)

            # Generate timestamp filename
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"tiktok_scraper_state_{timestamp}.json"
            filepath = os.path.join(self.config.saved_data_directory, filename)

            # Get current complete results
            state_data = self.get_complete_results()

            # Add session state information
            if self.session_state:
                state_data['session_metadata'] = {
                    'session_start_time': self.session_state.session_start_time,
                    'time_elapsed_minutes': (time.time() - self.session_state.session_start_time) / 60,
                    'current_phase': self.session_state.current_phase,
                    'current_hashtag_index': self.session_state.current_hashtag_index,
                    'current_profile_index': self.session_state.current_profile_index,
                    'current_post_index': self.session_state.current_post_index,
                    'hashtags_to_process': self.session_state.hashtags_to_process,
                    'original_hashtags': self.session_state.original_hashtags,
                    'resumed_from_saved_data': self.session_state.resumed_from_saved_data
                }

            # Save to file
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(state_data, f, indent=2, ensure_ascii=False, default=str)

            print(f"💾 State saved to: {filename}")

            # Clean up old saved files (keep only the 3 most recent)
            self._cleanup_old_saved_files()

        except Exception as e:
            print(f"❌ Error saving state: {e}")

    def _cleanup_old_saved_files(self, keep_count: int = 3):
        """Clean up old saved state files, keeping only the most recent ones"""
        try:
            saved_files = self._get_saved_data_files()

            # Remove files beyond keep_count
            for old_file in saved_files[keep_count:]:
                os.remove(old_file)
                print(f"🗑️  Cleaned up old save file: {os.path.basename(old_file)}")

        except Exception as e:
            print(f"⚠️  Warning: Could not clean up old files: {e}")

    def _cleanup_current_saved_file(self):
        """Remove the saved file that was used for resuming (successful completion)"""
        if self.saved_data_file and os.path.exists(self.saved_data_file):
            try:
                os.remove(self.saved_data_file)
                print(f"🗑️  Removed completed save file: {os.path.basename(self.saved_data_file)}")
            except Exception as e:
                print(f"⚠️  Warning: Could not remove save file: {e}")

    def _determine_resume_point(self, original_hashtags: List[str]) -> tuple:
        """Determine where to resume scraping based on current data state"""

        # Determine what hashtags still need processing
        hashtags_to_process = []
        for hashtag in original_hashtags:
            if hashtag not in self.hashtags_data:
                hashtags_to_process.append(hashtag)

        if hashtags_to_process:
            print(f"🔄 Resuming search phase: {len(hashtags_to_process)} hashtags remaining")
            return "search", hashtags_to_process, 0

        # Check for profiles that need post loading
        profiles_needing_posts = []
        for user_id, profile_data in self.profiles_data.items():
            if 'posts_count' not in profile_data or profile_data.get('posts_count', 0) == 0:
                # Profile exists but posts weren't loaded
                profiles_needing_posts.append(user_id)

        if profiles_needing_posts:
            print(f"🔄 Resuming profiles phase: {len(profiles_needing_posts)} profiles need post loading")
            return "profiles", profiles_needing_posts, 0

        # Check for posts that need comment loading
        posts_needing_comments = []
        for post_id in self.posts_data.keys():
            if post_id not in self.post_to_comments or len(self.post_to_comments[post_id]) == 0:
                posts_needing_comments.append(post_id)

        if posts_needing_comments:
            print(f"🔄 Resuming comments phase: {len(posts_needing_comments)} posts need comment loading")
            return "comments", posts_needing_comments, 0

        print("✅ All data appears complete. Starting fresh scrape.")
        return "complete", [], 0

    async def start_session(self):
        """Start unified browser session with saved data check"""
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
        """
        Enhanced session sharing with proper CDPXHRMonitor initialization

        This method ensures that when we share the browser session, we also
        properly initialize all the network monitoring capabilities that
        CDPXHRMonitor needs to function correctly.
        """
        if self.search_scraper is None:
            return

        try:
            print("🔧 Setting up enhanced session sharing with search scraper...")

            # Step 1: Share core browser components
            self.search_scraper.browser = self.profile_loader.browser
            self.search_scraper.page = self.profile_loader.page
            self.search_scraper.auth = self.profile_loader.auth
            self.search_scraper.is_authenticated = self.profile_loader.is_authenticated
            self.search_scraper.is_running = True

            # Step 2: Set session state flags to prevent duplicate initialization
            self.search_scraper.session_active = True
            self.search_scraper.session_initialized = True

            # Step 3: CRITICAL - Set up network event handlers that CDPXHRMonitor needs
            # This is what was missing and causing the monitoring to fail

            # Network monitoring should already be enabled by profile_loader.start_session()
            # But we need to ensure the search scraper's event handlers are registered

            if hasattr(self.search_scraper, 'page') and self.search_scraper.page:
                # For CDPXHRMonitor (v2) - Enhanced version with multiple handlers
                if hasattr(self.search_scraper, 'on_request_will_be_sent'):
                    self.search_scraper.page.add_handler(
                        uc.cdp.network.RequestWillBeSent,
                        self.search_scraper.on_request_will_be_sent
                    )
                    print("✅ RequestWillBeSent handler registered")

                if hasattr(self.search_scraper, 'on_response_received'):
                    self.search_scraper.page.add_handler(
                        uc.cdp.network.ResponseReceived,
                        self.search_scraper.on_response_received
                    )
                    print("✅ ResponseReceived handler registered")

                if hasattr(self.search_scraper, 'on_loading_finished'):
                    self.search_scraper.page.add_handler(
                        uc.cdp.network.LoadingFinished,
                        self.search_scraper.on_loading_finished
                    )
                    print("✅ LoadingFinished handler registered")

                if hasattr(self.search_scraper, 'on_loading_failed'):
                    self.search_scraper.page.add_handler(
                        uc.cdp.network.LoadingFailed,
                        self.search_scraper.on_loading_failed
                    )
                    print("✅ LoadingFailed handler registered")

                if hasattr(self.search_scraper, 'on_data_received'):
                    self.search_scraper.page.add_handler(
                        uc.cdp.network.DataReceived,
                        self.search_scraper.on_data_received
                    )
                    print("✅ DataReceived handler registered")

            # Step 4: Initialize tracking systems for CDPXHRMonitor v2
            if hasattr(self.search_scraper, 'tracked_requests'):
                # Reset tracking dictionaries to ensure clean state
                self.search_scraper.tracked_requests = {}
                self.search_scraper.matched_responses = []
                print("✅ Tracking systems initialized")

            print("✅ Enhanced session sharing completed successfully")

        except Exception as e:
            print(f"❌ Error in enhanced session sharing: {e}")
            raise

    async def end_session(self):
        """End unified browser session"""
        if not self.session_active:
            print("No active unified session to end")
            return

        try:
            print("Waiting before page closes")
            await asyncio.sleep(10)
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

    # MODIFY: run_full_workflow method
    async def run_full_workflow(self, hashtags: List[str]) -> Dict[str, Any]:
        """
        Complete end-to-end workflow with pause/resume capability
        """
        if not self.session_active:
            raise RuntimeError("Session not active. Call start_session() first.")

        # Check for saved data and load if available
        resumed = self._load_saved_state()

        if resumed:
            # Determine resume point
            session_meta = getattr(self, '_last_session_metadata', {})
            original_hashtags = session_meta.get('original_hashtags', hashtags)
            resume_phase, items_to_process, start_index = self._determine_resume_point(original_hashtags)

            # Initialize session state for resumed session
            self._initialize_session_state(original_hashtags)
            self.session_state.resumed_from_saved_data = True

            if resume_phase == "complete":
                print("🎉 Previous scrape was complete!")
                self._cleanup_current_saved_file()
                return self.get_complete_results()

        else:
            # Fresh scrape
            self._initialize_session_state(hashtags)
            resume_phase = "search"
            items_to_process = hashtags
            start_index = 0

        print(f"\n🚀 Starting{'(resumed)' if resumed else ''} TikTok scraping workflow")
        print(f"⏰ Session time limit: {self.config.session_duration_minutes} minutes")

        try:
            # Initialize search scraper if needed
            if resume_phase == "search":
                self._initialize_search_scraper(items_to_process)
                await self._share_session_with_search_scraper()

            # Execute phases based on resume point
            if resume_phase == "search":
                if not self._check_time_and_save_if_needed():
                    return self.get_complete_results()
                await self._run_search_phase_with_timing(items_to_process)

            if resume_phase in ["search", "profiles"]:
                if not self._check_time_and_save_if_needed():
                    return self.get_complete_results()
                await self._run_profiles_phase_with_timing()

            if resume_phase in ["search", "profiles", "comments"]:
                if not self._check_time_and_save_if_needed():
                    return self.get_complete_results()
                await self._run_comments_phase_with_timing()

            # If we completed successfully, clean up save file
            if resumed:
                self._cleanup_current_saved_file()

            print(f"\n🎉 Full workflow completed successfully!")
            return self.get_complete_results()

        except Exception as e:
            # Save state on any error
            self._save_current_state()
            self._log_error("WORKFLOW", "CRITICAL_ERROR", f"Workflow failed: {e}", exception=e)
            raise

    async def run_complete_session_with_resume(self, hashtags: List[str]) -> Dict[str, Any]:
        """
        Convenience method with automatic error handling and state saving
        """
        try:
            await self.start_session()
            results = await self.run_full_workflow(hashtags)
            return results
        except KeyboardInterrupt:
            print("\n⏹️  Manual interruption detected. Saving current state...")
            self._save_current_state()
            raise
        except Exception as e:
            print(f"\n❌ Unexpected error: {e}")
            self._save_current_state()
            raise
        finally:
            await self.end_session()

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
            await self._run_profiles_phase_with_timing()

            # Phase 3: Load Comments
            await self._run_comments_phase_with_timing()

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
            await self._run_comments_phase_with_timing()

            print(f"\n🎉 Comments-only workflow completed successfully!")
            return self.get_complete_results()

        except Exception as e:
            self._log_error("WORKFLOW", "CRITICAL_ERROR", f"Comments workflow failed: {e}", exception=e)
            raise

    # MODIFY: _run_search_phase method (add timing checks)
    async def _run_search_phase_with_timing(self, hashtags: List[str]):
        """Execute search phase with timing checks"""
        print(f"\n{'=' * 60}")
        print(f"🔍 PHASE 1: SEARCHING HASHTAGS")
        print(f"{'=' * 60}")

        self.session_state.current_phase = "search"

        try:
            search_results = await self.search_scraper.search_all_hashtags()
            total_profiles_found = 0

            for i, hashtag in enumerate(hashtags):
                # Check timing before processing each hashtag
                if not self._check_time_and_save_if_needed():
                    return

                self.session_state.current_hashtag_index = i
                self._update_progress("search", current_hashtag_index=i, current_hashtag=hashtag)

                try:
                    profiles_for_hashtag = search_results.get(hashtag, [])
                    print(f"🔍 Found {len(profiles_for_hashtag)} profiles for {hashtag}")

                    # Store hashtag metadata
                    self.hashtags_data[hashtag] = {
                        'search_timestamp': datetime.now().isoformat(),
                        'profiles_found': len(profiles_for_hashtag)
                    }

                    # Process found profiles
                    if profiles_for_hashtag:
                        for profile in profiles_for_hashtag:
                            user_id = profile.user_id
                            self.profiles_data[user_id] = {
                                'profile': profile,
                                'search_timestamp': datetime.now().isoformat(),
                                'profile_url': f"https://www.tiktok.com/@{profile.username}",
                                'found_under_hashtags': set()
                            }
                            self._add_relationships(hashtag=hashtag, profile=profile)
                            total_profiles_found += 1

                        print(f"✅ Stored {len(profiles_for_hashtag)} profiles for {hashtag}")
                    else:
                        print(f"❌ No profiles found for {hashtag}")
                        self.failed_hashtags.append(hashtag)

                except Exception as e:
                    self._log_error("SEARCH", "HASHTAG_PROCESS_ERROR", f"Failed to process {hashtag}", hashtag, e)
                    self.failed_hashtags.append(hashtag)
                    continue

            # Update found_under_hashtags for each profile
            for user_id in self.profiles_data:
                hashtags_for_profile = self.profile_to_hashtags.get(user_id, set())
                self.profiles_data[user_id]['found_under_hashtags'] = hashtags_for_profile

        except Exception as e:
            self._log_error("SEARCH", "SEARCH_PHASE_ERROR", f"Search phase failed: {e}", exception=e)
            raise

        total_profiles = len(self.profiles_data)
        print(f"\n🎯 Search phase completed: {total_profiles} unique profiles found")

    # MODIFY: _run_profiles_phase method (add timing checks)
    async def _run_profiles_phase_with_timing(self):
        """Execute profiles & posts loading phase with timing checks"""
        if not self.profiles_data:
            print("❌ No profiles to process")
            return

        print(f"\n{'=' * 60}")
        print(f"👤 PHASE 2: LOADING PROFILES & POSTS")
        print(f"{'=' * 60}")

        self.session_state.current_phase = "profiles"

        # Extract profiles that need post loading
        profiles_to_process = []
        for user_id, profile_data in self.profiles_data.items():
            if 'posts_count' not in profile_data or profile_data.get('posts_count', 0) == 0:
                profiles_to_process.append(profile_data['profile'])

        if not profiles_to_process:
            print("✅ All profiles already have posts loaded")
            return

        self._update_progress("profiles", total_profiles=len(profiles_to_process))
        self.profile_loader.set_profiles_to_load(profiles_to_process, self.profile_to_hashtags)

        for i, profile in enumerate(profiles_to_process):
            # Check timing before processing each profile
            if not self._check_time_and_save_if_needed():
                return

            self.session_state.current_profile_index = i
            user_id = profile.user_id

            self._update_progress("profiles", current_profile_index=i,
                                  current_profile_username=profile.username)

            try:
                print(f"\n👤 Loading posts for @{profile.username}")
                load_start_time = datetime.now().isoformat()

                posts = await self.profile_loader.load_profile_posts(profile)

                if posts:
                    self.profiles_data[user_id].update({
                        'posts_count': len(posts),
                        'load_timestamp': load_start_time
                    })

                    for post in posts:
                        self.posts_data[post.post_id] = post
                        self._add_relationships(profile=profile, post=post)

                    print(f"✅ Loaded {len(posts)} posts for @{profile.username}")
                else:
                    self.profiles_data[user_id].update({
                        'posts_count': 0,
                        'load_timestamp': load_start_time
                    })
                    print(f"❌ No posts loaded for @{profile.username}")
                    self.failed_profiles.append(user_id)

            except Exception as e:
                self._log_error("PROFILES", "PROFILE_ERROR", f"Failed to load @{profile.username}",
                                user_id, e)
                self.profiles_data[user_id].update({
                    'posts_count': 0,
                    'load_timestamp': datetime.now().isoformat(),
                    'load_error': str(e)
                })
                self.failed_profiles.append(user_id)
                continue

        total_posts = len(self.posts_data)
        print(f"\n🎯 Profiles phase completed: {total_posts} posts loaded")

    # MODIFY: _run_comments_phase method (add timing checks)
    async def _run_comments_phase_with_timing(self):
        """Execute comments loading phase with timing checks"""
        if not self.posts_data:
            print("❌ No posts to process for comments")
            return

        print(f"\n{'=' * 60}")
        print(f"💬 PHASE 3: LOADING COMMENTS")
        print(f"{'=' * 60}")

        self.session_state.current_phase = "comments"

        # Find posts that need comment loading
        posts_needing_comments = []
        for post_id, post in self.posts_data.items():
            if post_id not in self.post_to_comments or len(self.post_to_comments[post_id]) == 0:
                posts_needing_comments.append(post)

        if not posts_needing_comments:
            print("✅ All posts already have comments loaded")
            return

        self._update_progress("comments", total_posts=len(posts_needing_comments))

        # Group posts by profile for efficient processing
        posts_by_profile = {}
        for post in posts_needing_comments:
            if post.post_id in self.post_to_profile:
                user_id = self.post_to_profile[post.post_id]
                if user_id not in posts_by_profile:
                    posts_by_profile[user_id] = []
                posts_by_profile[user_id].append(post)

        # Process comments for each profile's posts
        for user_id, user_posts in posts_by_profile.items():
            # Check timing before processing each profile's posts
            if not self._check_time_and_save_if_needed():
                return

            if user_id not in self.profiles_data:
                continue

            profile_data = self.profiles_data[user_id]
            profile = profile_data['profile']

            print(f"\n💬 Loading comments for @{profile.username} posts")

            try:
                self.comments_loader.load_posts_from_profile_loader(self.profile_loader, user_id)
                comments_results = await self.comments_loader.load_all_comments()

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
            }
            profiles_flat.append(flat_profile)

        # Flatten posts data
        posts_flat = []
        for post_id, post in self.posts_data.items():
            user_id = self.post_to_profile.get(post_id)
            flat_post = {
                'post_id': post.post_id,
                'user_id': user_id,
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
                'profile_username': profile.username if profile else '',
                'profile_display_name': profile.display_name if profile else '',
                'raw_comment_data': comment.raw_comment_data,
                'create_time': comment.create_time
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
    def save_as_csv_files(unified_scraper_results: Dict[str, Any],
                          output_dir: str = "../fileExports/tiktok_csv_export") -> Dict[str, str]:
        """
        Convert unified scraper JSON results to multiple CSV files with timestamp versioning
        Args:
            unified_scraper_results: Results from UnifiedTikTokScraper.get_flattened_data()
            output_dir: Directory to save CSV files

        Returns:
            Dictionary mapping entity type to CSV file path
        """
        # Create timestamp for file versioning
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

        # Create output directory (keeping it simple)
        Path(output_dir).mkdir(parents=True, exist_ok=True)
        csv_files = {}

        try:
            # 1. Profiles CSV
            if 'profiles' in unified_scraper_results and unified_scraper_results['profiles']:
                profiles_file = os.path.join(output_dir, f"profiles_{timestamp}.csv")
                with open(profiles_file, 'w', newline='', encoding='utf-8') as f:
                    if unified_scraper_results['profiles']:
                        writer = csv.DictWriter(f, fieldnames=unified_scraper_results['profiles'][0].keys())
                        writer.writeheader()
                        writer.writerows(unified_scraper_results['profiles'])
                csv_files['profiles'] = profiles_file
                print(f"✅ Saved {len(unified_scraper_results['profiles'])} profiles to {profiles_file}")

            # 2. Posts CSV
            if 'posts' in unified_scraper_results and unified_scraper_results['posts']:
                posts_file = os.path.join(output_dir, f"posts_{timestamp}.csv")
                with open(posts_file, 'w', newline='', encoding='utf-8') as f:
                    if unified_scraper_results['posts']:
                        writer = csv.DictWriter(f, fieldnames=unified_scraper_results['posts'][0].keys())
                        writer.writeheader()
                        writer.writerows(unified_scraper_results['posts'])
                csv_files['posts'] = posts_file
                print(f"✅ Saved {len(unified_scraper_results['posts'])} posts to {posts_file}")

            # 3. Comments CSV
            if 'comments' in unified_scraper_results and unified_scraper_results['comments']:
                comments_file = os.path.join(output_dir, f"comments_{timestamp}.csv")
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
                relations_file = os.path.join(output_dir, f"profile_hashtag_relations_{timestamp}.csv")
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
                summary_file = os.path.join(output_dir, f"scrape_summary_{timestamp}.csv")
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

            # 6. Create a metadata file with export information
            metadata_file = os.path.join(output_dir, f"export_metadata_{timestamp}.csv")
            export_time = datetime.now()
            metadata = [{
                'export_timestamp': export_time.strftime("%Y-%m-%d %H:%M:%S"),
                'export_date': export_time.strftime("%Y-%m-%d"),
                'export_time': export_time.strftime("%H:%M:%S"),
                'file_version': timestamp,
                'total_files_created': len(csv_files),
                'profiles_count': len(unified_scraper_results.get('profiles', [])),
                'posts_count': len(unified_scraper_results.get('posts', [])),
                'comments_count': len(unified_scraper_results.get('comments', [])),
                'relations_count': len(unified_scraper_results.get('profile_hashtag_relations', []))
            }]

            with open(metadata_file, 'w', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=metadata[0].keys())
                writer.writeheader()
                writer.writerows(metadata)
            csv_files['metadata'] = metadata_file

            print(f"\n🎉 All CSV files saved to directory: {output_dir}")
            print(f"📅 File version: {timestamp}")
            return csv_files

        except Exception as e:
            print(f"❌ Error saving CSV files: {e}")
            raise

    def save_results(self, filename: str = "../fileExports/jsonFiles/unified_tiktok_scrape_results.json"):
        """Save complete flattened results to JSON file with timestamp versioning"""

        # Create timestamp for file versioning
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

        # Parse the original filename to add versioning
        file_path = Path(filename)
        directory = file_path.parent
        file_stem = file_path.stem  # filename without extension
        file_extension = file_path.suffix  # .json

        # Create versioned filename
        versioned_filename = directory / f"{file_stem}_{timestamp}{file_extension}"

        # Ensure directory exists
        directory.mkdir(parents=True, exist_ok=True)

        # Get results and save
        results = self.get_flattened_data()
        OptimizedNoDriver.save_json_to_file(results, str(versioned_filename))
        self.save_as_csv_files(results)

        print(f"💾 Flattened results saved to {versioned_filename}")
        print(f"📅 File version: {timestamp}")

        return results
