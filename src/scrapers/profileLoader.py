import asyncio
import random
from dataclasses import dataclass
from datetime import datetime
from typing import List, Dict, Any, Optional

import zendriver as uc

from browserConfig import OptimizedNoDriver
from requestMonitor import CDPXHRMonitor
from searchResultsScraper import AuthorProfile, TikTokSearchScraper
from src.utils.exceptions import AuthenticationError


@dataclass
class PostData:
    """Data structure for individual TikTok post information"""
    post_id: str
    author_stats: Dict[str, Any]
    author_stats_v2: Dict[str, Any]
    contents: Dict[str, Any]
    challenges: List[Dict[str, Any]]  # hashtag info
    text_extra: List[Dict[str, Any]]  # hashtags used
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


class TikTokProfileLoader(CDPXHRMonitor):
    """
    Enhanced TikTok Profile Loader with session management and human-like interactions.

    This class:
    1. Maintains browser session across profile loads
    2. Implements configurable human-like behaviors
    3. Separates all interaction methods for easy customization
    4. Captures API responses containing post data
    5. Maintains hashtag categorization for profiles
    """

    def __init__(self, config: ProfileLoadConfig = None):
        """
        Initialize the TikTok Profile Loader with session management

        Args:
            config: ProfileLoadConfig object with loading parameters
        """
        self.config = config or ProfileLoadConfig()

        # Initialize parent with base TikTok URL and profile post API pattern
        super().__init__(
            target_url="https://www.tiktok.com",
            regex_pattern=r"https://www\.tiktok\.com/api/post/item_list/\?[^ ]+",
            scroll_count=self.config.scroll_count,
            scroll_pause=int(self.config.scroll_pause_min),  # Base pause, we'll add variation
            timeout=30  # Longer timeout for profile loading
        )

        # Session management state
        self.session_active = False
        self.session_initialized = False

        # Storage for loaded data
        self.profiles_to_load: List[AuthorProfile] = []
        self.loaded_profiles: Dict[str, Dict[str, Any]] = {}  # user_id -> profile data
        self.profile_posts: Dict[str, List[PostData]] = {}  # user_id -> posts
        self.failed_profiles: List[str] = []  # Track failed loads for retry

        # Hashtag categorization
        self.profile_hashtag_mapping: Dict[str, List[str]] = {}  # user_id -> hashtags they were found under
        self.hashtag_to_profiles: Dict[str, List[str]] = {}  # hashtag -> user_ids

        # Progress tracking
        self.current_profile_index: int = 0
        self.total_profiles_to_load: int = 0

    def load_profiles_from_search_results(self, search_scraper: TikTokSearchScraper) -> None:
        """
        Load profiles from TikTokSearchScraper results

        Args:
            search_scraper: TikTokSearchScraper instance with completed search results
        """
        self.profiles_to_load = []
        self.profile_hashtag_mapping = {}
        self.hashtag_to_profiles = search_scraper.hashtag_to_profiles.copy()

        # Build reverse mapping: user_id -> hashtags they were found under
        for hashtag, user_ids in search_scraper.hashtag_to_profiles.items():
            for user_id in user_ids:
                if user_id not in self.profile_hashtag_mapping:
                    self.profile_hashtag_mapping[user_id] = []
                self.profile_hashtag_mapping[user_id].append(hashtag)

        # Collect all unique profiles from search results
        for user_id, profile in search_scraper.author_profiles.items():
            self.profiles_to_load.append(profile)

        self.total_profiles_to_load = len(self.profiles_to_load)
        print(f"🎯 Loaded {self.total_profiles_to_load} profiles from search results")

        # Print hashtag categorization summary
        hashtag_counts = {ht: len(profiles) for ht, profiles in self.hashtag_to_profiles.items()}
        print(f"📊 Hashtag categorization: {hashtag_counts}")

    def set_profiles_to_load(self, profiles: List[AuthorProfile], hashtag_mapping: Dict[str, List[str]] = None) -> None:
        """
        Directly set profiles to load (alternative to loading from search results)

        Args:
            profiles: List of AuthorProfile objects to load
            hashtag_mapping: Optional mapping of user_id to hashtags they were found under
        """
        self.profiles_to_load = profiles.copy()
        self.total_profiles_to_load = len(self.profiles_to_load)

        if hashtag_mapping:
            self.profile_hashtag_mapping = hashtag_mapping.copy()

        print(f"🎯 Set {self.total_profiles_to_load} profiles to load")

    async def start_session(self):
        """
        Start a browser session for profile loading.
        This method initializes the browser and authentication once.
        """
        if self.session_active:
            print("Profile loader session already active")
            return

        try:
            print("🚀 Starting TikTok profile loading session...")

            # Start browser and ensure authentication using parent's method
            await self.start_browser()

            if not self.is_authenticated:
                raise AuthenticationError("Failed to establish authenticated session")

            # Set up response monitoring
            self.page.add_handler(uc.cdp.network.ResponseReceived, self.on_response_received)

            self.session_active = True
            self.session_initialized = True

            print("✅ Profile loading session started successfully")

        except Exception as e:
            print(f"❌ Error starting profile loading session: {e}")
            await self.cleanup_session()
            raise

    async def end_session(self):
        """End the browser session and cleanup resources"""
        if not self.session_active:
            print("No active profile loading session to end")
            return

        try:
            print("🔄 Ending TikTok profile loading session...")
            await self.cleanup_session()
            print("✅ Profile loading session ended successfully")

        except Exception as e:
            print(f"❌ Error ending profile loading session: {e}")
            # Force cleanup even if there's an error
            await self.cleanup_session()

    async def cleanup_session(self):
        """Clean up session resources"""
        self.session_active = False
        self.session_initialized = False

        # Use parent's cleanup method
        await self.stop_browser()

    def _build_profile_url(self, profile: AuthorProfile) -> str:
        """Build TikTok profile URL from AuthorProfile"""
        username = profile.username
        if not username.startswith('@'):
            username = f'@{username}'
        return f"https://www.tiktok.com/{username}"

    async def _reset_browser_state(self):
        """Reset browser state between profile loads"""
        try:
            # Clear any existing alerts/popups
            try:
                await self.page.evaluate("document.querySelectorAll('[role=\"dialog\"]').forEach(el => el.remove());")
            except:
                pass

            # Reset scroll position
            await self.page.evaluate("window.scrollTo(0, 0);")

            # Clear any cached elements by forcing a small scroll and back
            await self.page.evaluate("window.scrollBy(0, 100);")
            await asyncio.sleep(0.5)
            await self.page.evaluate("window.scrollTo(0, 0);")

            # Small delay to ensure state is reset
            await asyncio.sleep(1)

            print("🔄 Browser state reset completed")

        except Exception as e:
            print(f"⚠️ Error resetting browser state: {e}")

    async def _navigate_to_profile(self, profile: AuthorProfile) -> str:
        """Navigate to a specific profile page with human-like behavior"""
        if not self.session_active:
            raise RuntimeError("Session not active. Call start_session() first.")

        profile_url = self._build_profile_url(profile)
        print(f"📍 Navigating to profile: {profile_url}")

        try:
            # Reset browser state before navigation
            await self._reset_browser_state()

            # Pre-navigation delay to simulate human behavior
            await self._simulate_pre_navigation_delay()

            # Navigate to profile
            await self.page.get(profile_url)
            print("✅ Profile navigation successful")

            # Post-navigation human-like behavior
            await self._simulate_post_navigation_behavior()

            return profile_url

        except Exception as e:
            print(f"❌ Profile navigation failed: {e}")
            raise

    async def _simulate_pre_navigation_delay(self):
        """Simulate human-like delay before navigation"""
        delay = random.uniform(
            self.config.profile_navigation_delay_min,
            self.config.profile_navigation_delay_max
        )
        print(f"⏳ Pre-navigation delay: {delay:.1f}s")
        await asyncio.sleep(delay)

    async def _simulate_post_navigation_behavior(self):
        """Simulate human-like behavior after page navigation"""
        # Wait for page to load with randomization
        load_wait = random.uniform(
            self.config.page_load_wait_min,
            self.config.page_load_wait_max
        )
        print(f"⏳ Page load wait: {load_wait:.1f}s")
        await asyncio.sleep(load_wait)

    async def _simulate_reading_pause(self):
        """Simulate human reading/viewing behavior"""
        if random.random() < self.config.reading_pause_probability:
            pause_time = random.uniform(
                self.config.reading_pause_min,
                self.config.reading_pause_max
            )
            print(f"👁️  Reading pause: {pause_time:.1f}s")
            await asyncio.sleep(pause_time)

    async def _simulate_scroll_variation(self):
        """Simulate natural scroll variations"""
        # Occasionally scroll up briefly (like humans do)
        if random.random() < self.config.scroll_direction_change_probability:
            print("⬆️  Brief upward scroll")
            await self.page.evaluate(f"""
                window.scrollBy(0, -{self.config.scroll_up_amount});
            """)
            await asyncio.sleep(random.uniform(0.5, 1.0))

    async def _human_like_profile_scroll(self):
        """
        Perform human-like scrolling specific to profile pages with all configurable behaviors
        """
        try:
            for i in range(self.config.scroll_count):
                if not self.is_running:
                    break

                print(f"📜 Profile scroll {i + 1}/{self.config.scroll_count}")

                # Simulate scroll variation
                await self._simulate_scroll_variation()

                # Variable scroll amount with base + variation
                scroll_amount = (
                        self.config.scroll_amount_base +
                        random.randint(-self.config.scroll_amount_variation, self.config.scroll_amount_variation) +
                        (i * 50)  # Gradually increase scroll distance
                )

                # Perform the scroll
                await self.page.evaluate(f"""
                    window.scrollBy(0, {scroll_amount});
                """)

                # Human-like pause with variation
                pause_time = random.uniform(
                    self.config.scroll_pause_min,
                    self.config.scroll_pause_max
                )
                await asyncio.sleep(pause_time)

                # Simulate reading/viewing behavior
                await self._simulate_reading_pause()

        except Exception as e:
            print(f"Error during profile scrolling: {e}")

    async def _simulate_inter_profile_delay(self):
        """Simulate human-like delay between profile loads"""
        delay = random.uniform(
            self.config.profile_load_delay_min,
            self.config.profile_load_delay_max
        )
        print(f"⏳ Inter-profile delay: {delay:.1f}s")
        await asyncio.sleep(delay)

    @staticmethod
    def _extract_post_from_item(item_data: Dict[str, Any]) -> Optional[PostData]:
        """
        Extract post information from TikTok API item data

        Args:
            item_data: Raw post data from API response

        Returns:
            PostData object or None if extraction fails
        """
        try:
            # Extract only the required fields
            post_id = item_data.get('id', '')
            author_stats = item_data.get('authorStats', {})
            author_stats_v2 = item_data.get('authorStatsV2', {})
            contents = item_data.get('contents', {})
            challenges = item_data.get('challenges', [])
            text_extra = item_data.get('textExtra', [])

            post_data = PostData(
                post_id=post_id,
                author_stats=author_stats,
                author_stats_v2=author_stats_v2,
                contents=contents,
                challenges=challenges,
                text_extra=text_extra,
                raw_post_data=item_data
            )

            return post_data

        except Exception as e:
            print(f"Error extracting post data: {e}")
            return None

    def _process_profile_response(self, response_data: Dict[str, Any], profile: AuthorProfile) -> List[PostData]:
        """
        Process API response for a profile and extract posts

        Args:
            response_data: Captured API response data
            profile: AuthorProfile being processed

        Returns:
            List of extracted PostData objects
        """
        posts = []

        try:
            body = response_data.get('body', {})
            item_list = body.get('itemList', [])

            if not isinstance(item_list, list):
                print(f"Unexpected itemList structure for {profile.username}: {type(item_list)}")
                return posts

            print(f"Processing {len(item_list)} posts for @{profile.username}")

            for item in item_list:
                post_data = self._extract_post_from_item(item)
                if post_data:
                    posts.append(post_data)

            print(f"✅ Extracted {len(posts)} valid posts for @{profile.username}")

        except Exception as e:
            print(f"Error processing profile response for @{profile.username}: {e}")

        return posts

    async def load_profile_posts(self, profile: AuthorProfile) -> List[PostData]:
        """
        Load posts for a specific profile using session management

        Args:
            profile: AuthorProfile to load posts for

        Returns:
            List of PostData objects
        """
        if not self.session_active:
            raise RuntimeError("Session not active. Call start_session() first.")

        print(f"\n🔍 Loading posts for profile: @{profile.username}")
        print(f"👤 {profile.display_name} | Followers: {profile.follower_count:,} | Hearts: {profile.heart_count:,}")

        # Show which hashtags this profile was found under
        hashtags = self.profile_hashtag_mapping.get(profile.user_id, [])
        if hashtags:
            print(f"🏷️  Found under hashtags: {', '.join(hashtags)}")

        try:
            # Clear previous responses
            self.matched_responses = []

            # Navigate to profile with human-like behavior
            profile_url = await self._navigate_to_profile(profile)

            print(f"📊 Responses after navigation: {len(self.matched_responses)}")

            # Perform human-like scrolling
            print("🔄 Starting profile scrolling...")
            await self._human_like_profile_scroll()

            # Final wait for any remaining responses
            print("⏳ Final wait for remaining responses...")
            await asyncio.sleep(3)

            # Process captured responses
            captured_responses = self.matched_responses.copy()

            if not captured_responses:
                print(f"❌ No API responses captured for @{profile.username}")
                self.failed_profiles.append(profile.user_id)
                return []

            print(f"✅ Captured {len(captured_responses)} API responses for @{profile.username}")

            # Process all captured responses
            all_posts = []
            for response in captured_responses:
                posts = self._process_profile_response(response, profile)
                all_posts.extend(posts)

            # Deduplicate posts by post_id
            unique_posts = {}
            for post in all_posts:
                if post.post_id not in unique_posts:
                    unique_posts[post.post_id] = post

            posts_list = list(unique_posts.values())

            # Limit posts to configured maximum
            posts_count = len(posts_list)
            if posts_count > self.config.max_posts_per_profile:
                posts_list = posts_list[:self.config.max_posts_per_profile]
                print(f"✂️  Limited to {self.config.max_posts_per_profile} posts for @{profile.username}")

            # Store results
            self.profile_posts[profile.user_id] = posts_list
            self.loaded_profiles[profile.user_id] = {
                'profile': profile,
                'posts_count': len(posts_list),
                'load_timestamp': datetime.now().isoformat(),
                'profile_url': profile_url,
                'found_under_hashtags': hashtags
            }

            print(f"🎯 Successfully loaded {len(posts_list)} posts for @{profile.username}")
            return posts_list

        except Exception as e:
            print(f"❌ Error loading profile @{profile.username}: {e}")
            self.failed_profiles.append(profile.user_id)
            return []

    async def load_all_profiles(self) -> Dict[str, List[PostData]]:
        """
        Load posts for all configured profiles using session management

        Returns:
            Dictionary mapping user_id to list of PostData objects
        """

        if not self.session_active:
            raise RuntimeError("Session not active. Call start_session() first.")

        if not self.profiles_to_load:
            print("❌ No profiles to load. Use load_profiles_from_search_results() or set_profiles_to_load() first.")
            return {}

        print(f"\n🚀 Starting to load {self.total_profiles_to_load} profiles using session...")

        for i, profile in enumerate(self.profiles_to_load):
            self.current_profile_index = i

            print(f"\n{'=' * 60}")
            print(f"📋 Progress: {i + 1}/{self.total_profiles_to_load} profiles")
            print(f"{'=' * 60}")

            try:
                await self.load_profile_posts(profile)

                # Add delay between profile loads (except for last profile)
                if i < len(self.profiles_to_load) - 1:
                    await self._simulate_inter_profile_delay()

            except Exception as e:
                print(f"❌ Critical error loading profile @{profile.username}: {e}")
                self.failed_profiles.append(profile.user_id)
                continue

        return self.profile_posts

    async def run_profile_loading_session(self) -> Dict[str, List[PostData]]:
        """
        Convenience method that manages the entire profile loading session lifecycle.
        This is the main entry point for the profile loader.
        """
        try:
            # Start session
            await self.start_session()

            # Load all profiles
            results = await self.load_all_profiles()

            return results

        finally:
            # Always end session, even if there's an error
            await self.end_session()

    def get_load_summary(self) -> Dict[str, Any]:
        """Get summary of profile loading results"""
        successful_loads = len(self.loaded_profiles)
        failed_loads = len(self.failed_profiles)
        total_posts = sum(len(posts) for posts in self.profile_posts.values())

        # Calculate average posts per profile
        avg_posts = total_posts / successful_loads if successful_loads > 0 else 0

        # Get hashtag distribution
        hashtag_distribution = {}
        for user_id, hashtags in self.profile_hashtag_mapping.items():
            for hashtag in hashtags:
                if hashtag not in hashtag_distribution:
                    hashtag_distribution[hashtag] = 0
                hashtag_distribution[hashtag] += 1

        return {
            'total_profiles_attempted': self.total_profiles_to_load,
            'successful_loads': successful_loads,
            'failed_loads': failed_loads,
            'success_rate': (
                    successful_loads / self.total_profiles_to_load * 100) if self.total_profiles_to_load > 0 else 0,
            'total_posts_collected': total_posts,
            'average_posts_per_profile': avg_posts,
            'hashtag_distribution': hashtag_distribution,
            'failed_profile_ids': self.failed_profiles,
            'config_used': {
                'max_posts_per_profile': self.config.max_posts_per_profile,
                'scroll_count': self.config.scroll_count,
                'profile_load_delay_range': f"{self.config.profile_load_delay_min}-{self.config.profile_load_delay_max}s",
                'human_like_settings': {
                    'reading_pause_probability': self.config.reading_pause_probability,
                }
            }
        }

    def get_db_friendly_data(self) -> Dict[str, Any]:
        """Get data structured for database storage"""

        # Flatten profile data for DB storage
        profiles_flat = []
        for user_id, profile_data in self.loaded_profiles.items():
            profile = profile_data['profile']
            profiles_flat.append({
                'user_id': user_id,
                'username': profile.username,
                'display_name': profile.display_name,
                'avatar_url': profile.avatar_url,
                'verified': profile.verified,
                'follower_count': profile.follower_count,
                'following_count': profile.following_count,
                'heart_count': profile.heart_count,
                'video_count': profile.video_count,
                'posts_count': profile_data['posts_count'],
                'load_timestamp': profile_data['load_timestamp'],
                'profile_url': profile_data['profile_url'],
                'found_under_hashtags': profile_data['found_under_hashtags'],
                'raw_author_data': profile.raw_author_data,
                'raw_author_stats': profile.raw_author_stats
            })

        # Flatten posts data for DB storage
        posts_flat = []
        for user_id, posts in self.profile_posts.items():
            for post in posts:
                posts_flat.append({
                    'post_id': post.post_id,
                    'user_id': user_id,
                    'author_stats': post.author_stats,
                    'author_stats_v2': post.author_stats_v2,
                    'contents': post.contents,
                    'challenges': post.challenges,
                    'text_extra': post.text_extra,
                    'raw_post_data': post.raw_post_data
                })

        # Profile-hashtag relationships for DB storage
        profile_hashtag_relations = []
        for user_id, hashtags in self.profile_hashtag_mapping.items():
            for hashtag in hashtags:
                profile_hashtag_relations.append({
                    'user_id': user_id,
                    'hashtag': hashtag
                })

        return {
            'profiles': profiles_flat,
            'posts': posts_flat,
            'profile_hashtag_relations': profile_hashtag_relations,
            'load_summary': self.get_load_summary()
        }

    def save_results(self, filename: str = "tiktok_profile_posts.json"):
        """Save profile loading results to JSON file with DB-friendly structure"""
        output_data = {
            'load_metadata': {
                'total_profiles_loaded': len(self.loaded_profiles),
                'timestamp': datetime.now().isoformat(),
                'config': self.config.__dict__
            },
            'load_summary': self.get_load_summary(),
            'db_friendly_data': self.get_db_friendly_data()
        }

        OptimizedNoDriver.save_json_to_file(output_data, filename)
        return output_data


async def main():
    # After running search
    search_scraper = TikTokSearchScraper(['#fitness'], max_profiles_per_hashtag=3, scroll_count=4)
    search_results = await search_scraper.run_search_session()
    search_scraper.save_results()

    stats = search_scraper.get_summary_stats()
    print(f"Total profiles found: {stats['total_unique_profiles']}")

    # Configure and run profile loading
    config = ProfileLoadConfig(
        max_posts_per_profile=10,
        scroll_count=5,
        profile_load_delay_min=10.0,
        profile_load_delay_max=20.0,
        page_load_wait_min=15.0,
        page_load_wait_max=20.0,
        reading_pause_probability=0.8,
    )

    profile_loader = TikTokProfileLoader(config)
    profile_loader.load_profiles_from_search_results(search_scraper)
    profile_posts = await profile_loader.run_profile_loading_session()

    # Save results
    profile_loader.save_results("profile_posts_results.json")

    # Get DB-friendly data
    # db_data = profile_loader.get_db_friendly_data()
    # print("DB-friendly data structure ready for database insertion")


if __name__ == "__main__":
    asyncio.run(main())
