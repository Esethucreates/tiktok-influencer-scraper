import json
import random
from dataclasses import dataclass
from datetime import datetime
from typing import Dict, List

from browserConfig import OptimizedNoDriver
from src.scrapers.profileLoader import PostData, ProfileLoadConfig, TikTokProfileLoader
from src.scrapers.requestMonitor import CDPXHRMonitor
from src.scrapers.searchResultsScraper import TikTokSearchScraper
from src.services.tiktokAuth import *


@dataclass
class User:
    uid: str  # Unique user ID (for tracking behavior)
    nickname: str  # Username (for display / label purposes)
    sec_uid: str  # Secure UID (if needed for user-level analysis)
    unique_id: str  # Public identifier


@dataclass
class ReplyComment:
    cid: str  # Comment ID
    text: str  # Comment text (main for sentiment analysis)
    create_time: int  # Timestamp (for recency analysis)
    digg_count: int  # Likes (indicator of engagement)
    is_author_digged: bool  # Did the creator like it?
    is_comment_translatable: bool
    comment_language: str
    user: User  # Commenter identity
    reply_id: str  # Thread ID (helps cluster conversations)


@dataclass
class Comment:
    cid: str  # Comment ID
    text: str  # Text content (core for NLP/sentiment)
    create_time: int  # Timestamp (for freshness/recency)
    digg_count: int  # Engagement score
    is_author_digged: bool  # Did author like it?
    is_comment_translatable: bool
    comment_language: str  # Language of the comment
    user: User  # Identity of the commenter
    reply_comment: List[ReplyComment]  # Threaded replies (sentiment in context)
    post_id: str  # Link to parent post
    user_id: str  # Link to profile owner


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


class TikTokCommentsLoader(CDPXHRMonitor):
    """
    TikTok Comments Loader that scrapes comments from posts using an existing ProfileLoader session.

    This class:
    1. Takes over an active browser session from ProfileLoader
    2. Extracts video links from loaded PostData objects
    3. Navigates to each post and scrapes comments
    4. Maintains human-like interaction patterns
    5. Handles various error scenarios gracefully
    """

    def __init__(self, config: CommentsLoadConfig = None):
        """
        Initialize the TikTok Comments Loader

        Args:
            config: CommentsLoadConfig object with loading parameters
        """
        self.config = config or CommentsLoadConfig()

        # Initialize parent with TikTok comments API pattern
        super().__init__(
            target_url="https://www.tiktok.com",
            regex_pattern=r"https://www\.tiktok\.com/api/comment/list/\?[^ ]+",
            scroll_count=self.config.scroll_count,
            scroll_pause=int(self.config.comment_scroll_pause_min),
            timeout=30
        )

        # Session state from ProfileLoader
        self.profile_loader = None
        self.session_inherited = False

        # Data from ProfileLoader
        self.current_profile = None
        self.posts_to_process: List[PostData] = []

        # Comments collection
        self.profile_comments: Dict[str, List[Comment]] = {}  # user_id -> comments
        self.post_comments: Dict[str, List[Comment]] = {}  # post_id -> comments

        # Processing state
        self.current_post_index: int = 0
        self.total_posts_to_process: int = 0
        self.processed_posts: List[str] = []
        self.failed_posts: List[str] = []

        # Error tracking
        self.error_log: List[Dict[str, Any]] = []

    def inherit_session_from_profile_loader(self, profile_loader: 'TikTokProfileLoader') -> None:
        """
        Inherit the active browser session from a ProfileLoader instance

        Args:
            profile_loader: Active TikTokProfileLoader instance with established session
        """
        if not profile_loader.session_active:
            raise RuntimeError("ProfileLoader session is not active")

        # Inherit session components
        self.browser = profile_loader.browser
        self.page = profile_loader.page
        self.auth = profile_loader.auth
        self.is_authenticated = profile_loader.is_authenticated
        self.is_running = True

        # Set up comment monitoring
        self.page.add_handler(uc.cdp.network.ResponseReceived, self.on_response_received)

        self.profile_loader = profile_loader
        self.session_inherited = True

        print("✅ Inherited browser session from ProfileLoader")

    def load_posts_from_profile_loader(self, profile_loader: 'TikTokProfileLoader', user_id: str = None) -> None:
        """
        Load posts from ProfileLoader for comment scraping
        FIXED: Better profile context management

        Args:
            profile_loader: ProfileLoader instance with loaded posts
            user_id: Specific user_id to process, or None for all users
        """
        if user_id:
            if user_id not in profile_loader.profile_posts:
                raise ValueError(f"User ID {user_id} not found in ProfileLoader results")

            user_posts = profile_loader.profile_posts[user_id]
            self.current_profile = profile_loader.loaded_profiles[user_id]['profile']
            self.posts_to_process = user_posts[:self.config.max_posts_per_profile]

        else:
            # Process all loaded profiles (take first one for now, extend later for multiple)
            if not profile_loader.profile_posts:
                raise ValueError("No posts found in ProfileLoader")

            first_user_id = list(profile_loader.profile_posts.keys())[0]
            user_posts = profile_loader.profile_posts[first_user_id]
            self.current_profile = profile_loader.loaded_profiles[first_user_id]['profile']
            self.posts_to_process = user_posts[:self.config.max_posts_per_profile]

        self.total_posts_to_process = len(self.posts_to_process)

        print(f"🎯 Loaded {self.total_posts_to_process} posts for comment scraping")
        print(f"👤 Profile: @{self.current_profile.username}")
        print(f"🆔 Profile User ID: {self.current_profile.user_id}")

        # Clear any previous state to avoid cross-contamination
        self.profile_comments.clear()
        self.post_comments.clear()
        self.processed_posts.clear()
        self.failed_posts.clear()
        self.current_post_index = 0

    async def _ensure_on_profile_page(self) -> bool:
        """
        Ensure the browser is on the correct profile page before looking for video links

        Returns:
            bool: True if successfully on profile page, False otherwise
        """
        if not self.current_profile:
            self._log_error("NAVIGATION_ERROR", "No current profile set")
            return False

        try:
            profile_url = f"https://www.tiktok.com/@{self.current_profile.username}"
            current_url = await self.page.evaluate("window.location.href")

            # Check if we're already on the profile page
            if profile_url in current_url or f"@{self.current_profile.username}" in current_url:
                print(f"✅ Already on profile page: @{self.current_profile.username}")
                return True

            print(f"🔄 Navigating to profile page: {profile_url}")
            await self.page.get(profile_url)

            # Wait for profile page to load
            load_wait = random.uniform(
                self.config.page_load_wait_min,
                self.config.page_load_wait_max
            )
            print(f"⏳ Waiting for profile page to load: {load_wait:.1f}s")
            await asyncio.sleep(load_wait)

            # Verify we're on the right page
            final_url = await self.page.evaluate("window.location.href")
            if profile_url in final_url or f"@{self.current_profile.username}" in final_url:
                print(f"✅ Successfully navigated to profile page")
                return True
            else:
                self._log_error("NAVIGATION_ERROR", f"Failed to navigate to profile page. Current URL: {final_url}")
                return False

        except Exception as e:
            self._log_error("NAVIGATION_ERROR", f"Error navigating to profile page: {e}", exception=e)
            return False

    def _build_video_url(self, post: PostData) -> str:
        """Build video URL from PostData"""
        username = self.current_profile.username
        if not username.startswith('@'):
            username = f'@{username}'

        return f"https://www.tiktok.com/{username}/video/{post.post_id}"

    def _log_error(self, error_type: str, message: str, post_id: str = None,
                   exception: Exception = None) -> None:
        """Log error for later analysis"""
        error_entry = {
            'timestamp': datetime.now().isoformat(),
            'error_type': error_type,
            'message': message,
            'post_id': post_id,
            'exception_str': str(exception) if exception else None
        }
        self.error_log.append(error_entry)
        print(f"❌ {error_type}: {message}")

    async def _simulate_human_reading(self):
        """Simulate human-like reading behavior"""
        if random.random() < self.config.reading_pause_probability:
            pause_time = random.uniform(
                self.config.reading_pause_min,
                self.config.reading_pause_max
            )
            print(f"👀 Simulating reading pause: {pause_time:.1f}s")
            await asyncio.sleep(pause_time)

    async def _find_video_link_element(self, post: PostData) -> Optional[Any]:
        """
        Find video link element in the DOM with fallback strategies
        FIXED: Now ensures we're on the correct profile page first

        Args:
            post: PostData object containing post_id

        Returns:
            Page element or None if not found
        """
        # CRITICAL FIX: Ensure we're on the correct profile page first
        if not await self._ensure_on_profile_page():
            self._log_error("NAVIGATION_ERROR", f"Cannot navigate to profile page for post {post.post_id}",
                            post.post_id)
            return None

        video_url = self._build_video_url(post)

        # Primary strategy: exact href match
        try:
            print(f"🔍 Looking for video link: {video_url}")
            video_element = await self.page.query_selector(f'a[href="{video_url}"]')

            if video_element:
                print("✅ Found video element with exact href match")
                return video_element

        except Exception as e:
            self._log_error("DOM_ERROR", f"Error finding exact video link for {post.post_id}", post.post_id, e)

        # Fallback strategy: scroll and search with general selector
        print("🔄 Exact link not found, trying fallback with scrolling...")

        for scroll_attempt in range(self.config.video_link_scroll_attempts):
            try:
                # Try general video link selector but filter by current profile
                video_elements = await self.page.query_selector_all('a[href*="/video/"]')

                for element in video_elements:
                    try:
                        href = await element.get_attribute('href')
                        if href and post.post_id in href and f"@{self.current_profile.username}" in href:
                            print(f"✅ Found video element with fallback strategy: {href}")
                            return element
                    except:
                        continue

                # Scroll to load more content
                if scroll_attempt < self.config.video_link_scroll_attempts - 1:
                    print(f"📜 Scrolling to find more links (attempt {scroll_attempt + 1})")
                    await self.page.evaluate("window.scrollBy(0, window.innerHeight * 0.8);")
                    await asyncio.sleep(self.config.video_link_scroll_pause)

            except Exception as e:
                self._log_error("DOM_ERROR", f"Error in fallback search for {post.post_id}", post.post_id, e)
                continue

        self._log_error("DOM_ERROR",
                        f"Video link not found for post {post.post_id} on profile @{self.current_profile.username}",
                        post.post_id)
        return None

    async def _simulate_reading_pause(self):
        """Simulate human reading/viewing behavior"""
        if random.random() < self.config.reading_pause_probability:
            pause_time = random.uniform(
                self.config.reading_pause_min,
                self.config.reading_pause_max
            )
            print(f"👁️  Reading pause: {pause_time:.1f}s")
            await asyncio.sleep(pause_time)

    async def _simulate_inter_profile_delay(self):
        """Simulate human-like delay between profile loads"""
        delay = random.uniform(
            self.config.profile_load_delay_min,
            self.config.profile_load_delay_max
        )
        print(f"⏳ Inter-profile delay: {delay:.1f}s")
        await asyncio.sleep(delay)

    async def _simulate_scroll_variation(self):
        """Simulate natural scroll variations"""
        # Occasionally scroll up briefly (like humans do)
        if random.random() < self.config.scroll_direction_change_probability:
            print("⬆️  Brief upward scroll")
            await self.page.evaluate(f"""
                        window.scrollBy(0, -{self.config.scroll_up_amount});
                    """)
            await asyncio.sleep(random.uniform(0.5, 1.0))

    async def _navigate_to_post(self, post: PostData) -> bool:
        """
        Navigate to a specific post by finding and clicking its link

        Args:
            post: PostData object to navigate to

        Returns:
            bool: Success status
        """
        try:
            # Find video link element
            video_element = await self._find_video_link_element(post)

            if not video_element:
                self._log_error("DOM_ERROR", f"Cannot find video link for post {post.post_id}", post.post_id)
                return False

            print(f"🎬 Clicking on video post: {post.post_id}")
            await video_element.click()

            # Wait for post page to load
            load_wait = random.uniform(
                self.config.post_load_wait_min,
                self.config.post_load_wait_max
            )
            print(f"⏳ Waiting for post to load: {load_wait:.1f}s")
            await asyncio.sleep(load_wait)

            return True

        except Exception as e:
            self._log_error("NETWORK_ERROR", f"Failed to navigate to post {post.post_id}", post.post_id, e)
            return False

    async def _wait_for_comment_section(self) -> bool:
        """
        Wait for comment section to appear and be ready with proper error handling

        Returns:
            bool: True if comment section found, False otherwise
        """
        try:
            print("🔍 Looking for comment section...")

            start_time = asyncio.get_event_loop().time()
            retry_count = 0
            max_retries = 3

            while (asyncio.get_event_loop().time() - start_time) < self.config.comment_section_wait_timeout:
                try:
                    # Check if page is still valid before querying
                    if not self.page or not hasattr(self.page, 'query_selector'):
                        self._log_error("DOM_ERROR", "Page object is invalid")
                        return False

                    # Verify we're still on a valid TikTok page
                    try:
                        current_url = await self.page.evaluate("window.location.href")
                        if not current_url or "tiktok.com" not in current_url:
                            self._log_error("DOM_ERROR", f"Invalid page URL: {current_url}")
                            return False
                    except Exception as url_error:
                        self._log_error("DOM_ERROR", f"Cannot get current URL: {url_error}")
                        return False

                    # Try to find comment section with error handling
                    comment_section = await self.page.query_selector('div[data-e2e="search-comment-container"]')

                    if comment_section:
                        print("✅ Found comment section")
                        return True

                except Exception as query_error:
                    error_msg = str(query_error)
                    retry_count += 1

                    # Handle specific InvalidStateError
                    if "invalid state" in error_msg.lower():
                        print(f"⚠️  Invalid state error (attempt {retry_count}/{max_retries}): {error_msg}")

                        if retry_count >= max_retries:
                            self._log_error("DOM_ERROR", f"Max retries exceeded for invalid state error: {error_msg}")
                            return False

                        # Wait longer before retry for invalid state
                        await asyncio.sleep(2.0)
                        continue

                    # Handle other DOM errors
                    elif "target closed" in error_msg.lower() or "context destroyed" in error_msg.lower():
                        self._log_error("DOM_ERROR", f"Page context destroyed: {error_msg}")
                        return False

                    else:
                        print(f"⚠️  Query error (attempt {retry_count}/{max_retries}): {error_msg}")
                        if retry_count >= max_retries:
                            self._log_error("DOM_ERROR", f"Max retries exceeded: {error_msg}")
                            return False

                # Standard wait before next attempt
                await asyncio.sleep(1)

            print("❌ Comment section not found within timeout")
            return False

        except Exception as e:
            self._log_error("DOM_ERROR", f"Critical error waiting for comment section: {e}", exception=e)
            return False

    async def _scroll_comment_section(self) -> int:
        """
        Scroll through comment section to load comments with human-like behavior

        Returns:
            int: Number of comments captured
        """
        comments_captured = 0

        try:
            for scroll_num in range(self.config.max_scroll_attempts):
                if not self.is_running:
                    break

                print(f"📜 Comment scroll {scroll_num + 1}/{self.config.max_scroll_attempts}")

                # Human-like scroll variation (similar to profile scrolling)


                await self._simulate_scroll_variation()

                # Scroll comment section
                scroll_amount = (
                        self.config.comment_scroll_amount_base +
                        random.randint(-self.config.comment_scroll_amount_variation,
                                       self.config.comment_scroll_amount_variation)
                )

                print("Running the scroll function!!")
                await self.page.evaluate(
                    f"if(document.querySelector('[data-e2e=\"search-comment-container\"]') && document.querySelector('[data-e2e=\"search-comment-container\"]').firstElementChild) {{ document.querySelector('[data-e2e=\"search-comment-container\"]').firstElementChild.scrollTo({{top: document.querySelector('[data-e2e=\"search-comment-container\"]').firstElementChild.scrollTop + {scroll_amount}, behavior: 'smooth'}}); }}"
                )

                # Human-like pause with variation
                pause_time = random.uniform(
                    self.config.comment_scroll_pause_min,
                    self.config.comment_scroll_pause_max
                )
                await asyncio.sleep(pause_time)

                # Simulate reading behavior
                await self._simulate_reading_pause()

                # Check how many comments we've captured so far
                comments_captured = len([resp for resp in self.matched_responses
                                         if self._extract_comments_from_response(resp)])

                print(f"💬 Comments captured so far: {comments_captured}")

                # Stop if we've reached our target
                if comments_captured >= self.config.max_comments_per_post:
                    print(f"🎯 Reached target of {self.config.max_comments_per_post} comments")
                    break

        except Exception as e:
            self._log_error("DOM_ERROR", "Error during comment section scrolling", exception=e)

        return comments_captured

    async def _close_post(self) -> bool:
        """
        Close the current post and return to profile page

        Returns:
            bool: Success status
        """
        try:
            print("🔄 Looking for close button...")
            close_button = await self.page.query_selector("[data-e2e='browse-close']")

            if close_button:
                print("✅ Found close button, closing post")
                await close_button.click()

                # Wait for page to return to profile
                close_wait = random.uniform(
                    self.config.post_close_wait_min,
                    self.config.post_close_wait_max
                )
                await asyncio.sleep(close_wait)

                return True
            else:
                print("❌ Close button not found")
                self._log_error("DOM_ERROR", "Close button not found")
                return False

        except Exception as e:
            self._log_error("DOM_ERROR", "Error closing post", exception=e)
            return False

    @staticmethod
    def _extract_user_from_data(user_data: Dict[str, Any]) -> User:
        """Extract User object from API data"""
        return User(
            uid=user_data.get('uid', ''),
            nickname=user_data.get('nickname', ''),
            sec_uid=user_data.get('sec_uid', ''),
            unique_id=user_data.get('unique_id', '')
        )

    def _extract_comments_from_response(self, response_data: Dict[str, Any]) -> List[Comment]:
        """
        Extract comments from API response data

        Args:
            response_data: Response data from matched network request

        Returns:
            List of Comment objects
        """
        comments = []

        try:
            body = response_data.get('body', {})

            if isinstance(body, str):
                body = json.loads(body)

            comments_data = body.get('comments', [])

            if not isinstance(comments_data, list):
                self._log_error("API_ERROR", "Comments data is not a list")
                return comments

            for comment_data in comments_data:
                try:
                    user_data = comment_data.get('user', {})
                    user = self._extract_user_from_data(user_data)

                    comment = Comment(
                        cid=comment_data.get('cid', ''),
                        text=comment_data.get('text', ''),
                        create_time=comment_data.get('create_time', 0),
                        digg_count=comment_data.get('digg_count', 0),
                        is_author_digged=comment_data.get('is_author_digged', False),
                        is_comment_translatable=comment_data.get('is_comment_translatable', False),
                        comment_language=comment_data.get('comment_language', ''),
                        user=user,
                        reply_comment=[],  # Only top-level comments for now
                        post_id=self.posts_to_process[self.current_post_index].post_id if self.current_post_index < len(
                            self.posts_to_process) else '',
                        user_id=self.current_profile.user_id if self.current_profile else ''
                    )

                    comments.append(comment)

                except Exception as e:
                    self._log_error("API_ERROR", f"Error extracting individual comment", exception=e)
                    continue

        except Exception as e:
            self._log_error("API_ERROR", "Error parsing comments response", exception=e)

        return comments

    async def _process_post_comments(self, post: PostData) -> List[Comment]:
        """
        Process comments for a single post

        Args:
            post: PostData object to process

        Returns:
            List of Comment objects
        """
        print(f"\n🎬 Processing comments for post: {post.post_id}")

        try:
            # Clear previous responses to avoid duplication
            self.matched_responses.clear()

            # Navigate to post
            if not await self._navigate_to_post(post):
                return []

            # Wait for comment section
            if not await self._wait_for_comment_section():
                self._log_error("DOM_ERROR", f"Comment section not found for post {post.post_id}", post.post_id)
                await self._close_post()  # Try to close anyway
                return []

            # Scroll and capture comments
            print("📜 Starting comment section scrolling...")
            comments_captured = await self._scroll_comment_section()

            print(f"⏳ Final wait for remaining API responses...")
            await asyncio.sleep(2)

            # Process captured responses
            all_comments = []
            for response in self.matched_responses:
                comments = self._extract_comments_from_response(response)
                all_comments.extend(comments)

            # Deduplicate comments by cid
            unique_comments = {}
            for comment in all_comments:
                if comment.cid not in unique_comments:
                    unique_comments[comment.cid] = comment

            comments_list = list(unique_comments.values())

            # Limit to configured maximum
            if len(comments_list) > self.config.max_comments_per_post:
                comments_list = comments_list[:self.config.max_comments_per_post]

            print(f"💬 Successfully captured {len(comments_list)} unique comments for post {post.post_id}")

            # Close post and return to profile
            await self._close_post()

            return comments_list

        except Exception as e:
            self._log_error("NETWORK_ERROR", f"Critical error processing post {post.post_id}", post.post_id, e)
            await self._close_post()  # Try to close anyway
            return []

    async def load_all_comments(self) -> Dict[str, List[Comment]]:
        """
        Load comments for all configured posts

        Returns:
            Dictionary mapping post_id to list of Comment objects
        """
        if not self.session_inherited:
            raise RuntimeError("No inherited session. Call inherit_session_from_profile_loader() first.")

        if not self.posts_to_process:
            raise RuntimeError("No posts to process. Call load_posts_from_profile_loader() first.")

        print(f"\n🚀 Starting to load comments for {self.total_posts_to_process} posts...")

        for i, post in enumerate(self.posts_to_process):
            self.current_post_index = i

            print(f"\n{'=' * 60}")
            print(f"📋 Progress: {i + 1}/{self.total_posts_to_process} posts")
            print(f"{'=' * 60}")

            try:
                comments = await self._process_post_comments(post)

                if comments:
                    self.post_comments[post.post_id] = comments

                    # Also organize by user_id
                    if self.current_profile.user_id not in self.profile_comments:
                        self.profile_comments[self.current_profile.user_id] = []
                    self.profile_comments[self.current_profile.user_id].extend(comments)

                    self.processed_posts.append(post.post_id)
                    print(f"✅ Completed processing post {post.post_id}")
                else:
                    self.failed_posts.append(post.post_id)
                    print(f"❌ Failed to process post {post.post_id}")

                # Add delay between posts (except for last post)
                if i < len(self.posts_to_process) - 1:
                    await self._simulate_inter_profile_delay()  # Reuse from parent

            except Exception as e:
                self._log_error("NETWORK_ERROR", f"Critical error processing post {post.post_id}", post.post_id, e)
                self.failed_posts.append(post.post_id)
                continue

        return self.post_comments

    def get_comments_summary(self) -> Dict[str, Any]:
        """Get summary of comments loading results"""
        total_comments = sum(len(comments) for comments in self.post_comments.values())
        successful_posts = len(self.processed_posts)
        failed_posts = len(self.failed_posts)

        return {
            'total_posts_attempted': self.total_posts_to_process,
            'successful_posts': successful_posts,
            'failed_posts': failed_posts,
            'success_rate': (
                    successful_posts / self.total_posts_to_process * 100) if self.total_posts_to_process > 0 else 0,
            'total_comments_collected': total_comments,
            'average_comments_per_post': total_comments / successful_posts if successful_posts > 0 else 0,
            'processed_post_ids': self.processed_posts,
            'failed_post_ids': self.failed_posts,
            'error_count': len(self.error_log),
            'config_used': {
                'max_comments_per_post': self.config.max_comments_per_post,
                'max_scroll_attempts': self.config.max_scroll_attempts,
                'max_posts_per_profile': self.config.max_posts_per_profile
            }
        }

    def get_csv_friendly_data(self) -> List[Dict[str, Any]]:
        """Get comments data flattened for CSV export"""
        csv_data = []

        for post_id, comments in self.post_comments.items():
            for comment in comments:
                csv_row = {
                    'comment_id': comment.cid,
                    'post_id': comment.post_id,
                    'user_id': comment.user_id,
                    'comment_text': comment.text,
                    'create_time': comment.create_time,
                    'digg_count': comment.digg_count,
                    'is_author_digged': comment.is_author_digged,
                    'is_comment_translatable': comment.is_comment_translatable,
                    'comment_language': comment.comment_language,
                    'commenter_uid': comment.user.uid,
                    'commenter_nickname': comment.user.nickname,
                    'commenter_sec_uid': comment.user.sec_uid,
                    'commenter_unique_id': comment.user.unique_id,
                    'profile_username': self.current_profile.username if self.current_profile else '',
                    'profile_display_name': self.current_profile.display_name if self.current_profile else ''
                }
                csv_data.append(csv_row)

        return csv_data

    def save_results(self, filename: str = "tiktok_comments.json"):
        """Save comments loading results to JSON file"""
        output_data = {
            'load_metadata': {
                'total_posts_processed': len(self.processed_posts),
                'total_comments_collected': sum(len(comments) for comments in self.post_comments.values()),
                'timestamp': datetime.now().isoformat(),
                'profile_processed': {
                    'username': self.current_profile.username if self.current_profile else None,
                    'user_id': self.current_profile.user_id if self.current_profile else None
                },
                'config': self.config.__dict__
            },
            'comments_summary': self.get_comments_summary(),
            'csv_friendly_data': self.get_csv_friendly_data(),
            'error_log': self.error_log
        }

        OptimizedNoDriver.save_json_to_file(output_data, filename)
        return output_data
