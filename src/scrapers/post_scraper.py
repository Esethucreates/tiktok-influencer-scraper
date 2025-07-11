import asyncio
import re
from dataclasses import dataclass
from datetime import datetime
from typing import List, Dict, Any, Optional

from base_scraper import CDPXHRMonitor
from browser_config import OptimizedNoDriver
from profile_scraper import AuthorProfile, TikTokSearchScraper


@dataclass
class PostData:
    """Data structure for individual TikTok post information"""
    post_id: str
    desc: str
    create_time: int
    author_id: str
    author_username: str
    stats: Dict[str, Any]  # playCount, shareCount, commentCount, diggCount
    video_url: str
    music_info: Dict[str, Any]
    hashtags: List[str]
    raw_post_data: Dict[str, Any]

    def __post_init__(self):
        """Calculate engagement score after initialization"""
        self.engagement_score = self._calculate_engagement_score()

    def _calculate_engagement_score(self) -> float:
        """Calculate engagement score based on stats"""
        stats = self.stats or {}
        return (
                stats.get('diggCount', 0) * 3 +  # Likes weighted higher
                stats.get('commentCount', 0) * 5 +  # Comments weighted highest
                stats.get('shareCount', 0) * 4 +  # Shares weighted high
                stats.get('playCount', 0) * 0.1  # Views weighted lower
        )


@dataclass
class ProfileLoadConfig:
    """Configuration for profile loading behavior"""
    min_posts_per_profile: int = 500
    max_posts_per_profile: int = 700
    scroll_count: int = 25
    scroll_pause_min: float = 2.0
    scroll_pause_max: float = 4.0
    profile_load_delay_min: float = 8.0
    profile_load_delay_max: float = 15.0
    prioritize_top_liked: bool = True
    max_concurrent_profiles: int = 1  # Process profiles sequentially by default


class TikTokProfileLoader(CDPXHRMonitor):
    """
    TikTok Profile Loader that extends CDPXHRMonitor to load individual profiles
    and capture their posts via API responses.

    This class:
    1. Takes discovered profiles from TikTokSearchScraper
    2. Loads each profile page and scrolls to trigger post loading
    3. Captures API responses containing post data
    4. Extracts and processes post information
    5. Limits posts per profile (min 500, max 700)
    6. Prioritizes top-liked posts for quality
    """

    def __init__(self, config: ProfileLoadConfig = None):
        """
        Initialize the TikTok Profile Loader

        Args:
            config: ProfileLoadConfig object with loading parameters
        """
        self.perform_scrolling = None
        self.config = config or ProfileLoadConfig()

        # Initialize parent with profile post API pattern
        super().__init__(
            target_url="https://www.tiktok.com",  # Will be set dynamically
            regex_pattern=r"https://www\.tiktok\.com/api/post/item_list/\?[^ ]+",
            scroll_count=self.config.scroll_count,
            scroll_pause=int(self.config.scroll_pause_min),  # Base pause, we'll add variation
            timeout=30  # Longer timeout for profile loading
        )

        # Storage for loaded data
        self.profiles_to_load: List[AuthorProfile] = []
        self.loaded_profiles: Dict[str, Dict[str, Any]] = {}  # user_id -> profile data
        self.profile_posts: Dict[str, List[PostData]] = {}  # user_id -> posts
        self.failed_profiles: List[str] = []  # Track failed loads for retry

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

        # Collect all unique profiles from search results
        for user_id, profile in search_scraper.author_profiles.items():
            self.profiles_to_load.append(profile)

        self.total_profiles_to_load = len(self.profiles_to_load)
        print(f"🎯 Loaded {self.total_profiles_to_load} profiles from search results")

        # Sort by engagement if prioritization is enabled
        if self.config.prioritize_top_liked:
            self.profiles_to_load.sort(
                key=lambda p: p.follower_count + p.heart_count,
                reverse=True
            )
            print("📊 Profiles sorted by engagement (followers + hearts)")

    def set_profiles_to_load(self, profiles: List[AuthorProfile]) -> None:
        """
        Directly set profiles to load (alternative to loading from search results)

        Args:
            profiles: List of AuthorProfile objects to load
        """
        self.profiles_to_load = profiles.copy()
        self.total_profiles_to_load = len(self.profiles_to_load)
        print(f"🎯 Set {self.total_profiles_to_load} profiles to load")

    def _build_profile_url(self, profile: AuthorProfile) -> str:
        """Build TikTok profile URL from AuthorProfile"""
        username = profile.username
        if not username.startswith('@'):
            username = f'@{username}'
        return f"https://www.tiktok.com/{username}"

    def _extract_post_from_item(self, item_data: Dict[str, Any]) -> Optional[PostData]:
        """
        Extract post information from TikTok API item data

        Args:
            item_data: Raw post data from API response

        Returns:
            PostData object or None if extraction fails
        """
        try:
            # Extract basic post info
            post_id = item_data.get('id', '')
            desc = item_data.get('desc', '')
            create_time = item_data.get('createTime', 0)

            # Extract author info
            author = item_data.get('author', {})
            author_id = str(author.get('id', ''))
            author_username = author.get('uniqueId', '')

            # Extract stats
            stats = item_data.get('stats', {})

            # Extract video info
            video = item_data.get('video', {})
            video_url = video.get('playAddr', '')

            # Extract music info
            music = item_data.get('music', {})

            # Extract hashtags from description
            hashtags = re.findall(r'#(\w+)', desc)

            post_data = PostData(
                post_id=post_id,
                desc=desc,
                create_time=create_time,
                author_id=author_id,
                author_username=author_username,
                stats=stats,
                video_url=video_url,
                music_info=music,
                hashtags=hashtags,
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

    async def _human_like_profile_scroll(self):
        """
        Perform human-like scrolling specific to profile pages
        """
        try:
            for i in range(self.config.scroll_count):
                if not self.is_running:
                    break

                print(f"📜 Profile scroll {i + 1}/{self.config.scroll_count}")

                # Variable scroll amount
                scroll_amount = 800 + (i * 100)  # Increase scroll distance as we go deeper

                await self.page.evaluate(f"""
                    window.scrollBy(0, {scroll_amount});
                """)

                # Human-like pause with variation
                import random
                pause_time = random.uniform(
                    self.config.scroll_pause_min,
                    self.config.scroll_pause_max
                )
                await asyncio.sleep(pause_time)

                # Occasional longer pause (simulating reading/viewing)
                if random.random() < 0.3:  # 30% chance
                    extra_pause = random.uniform(1.0, 3.0)
                    print(f"👁️  Extra pause {extra_pause:.1f}s (viewing content)")
                    await asyncio.sleep(extra_pause)

        except Exception as e:
            print(f"Error during profile scrolling: {e}")

    async def load_profile_posts(self, profile: AuthorProfile) -> List[PostData]:
        """
        Load posts for a specific profile

        Args:
            profile: AuthorProfile to load posts for

        Returns:
            List of PostData objects
        """
        print(f"\n🔍 Loading posts for profile: @{profile.username}")
        print(f"👤 {profile.display_name} | Followers: {profile.follower_count:,} | Hearts: {profile.heart_count:,}")

        # Build profile URL and set as target
        profile_url = self._build_profile_url(profile)
        self.target_url = profile_url

        print(f"📍 Navigating to: {profile_url}")

        # Clear previous responses
        self.matched_responses = []

        # Override scrolling method for profile-specific behavior
        original_perform_scrolling = self.perform_scrolling
        self.perform_scrolling = self._human_like_profile_scroll

        try:
            # Use parent's run() method to capture responses
            captured_responses = await self.run()

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

            # Sort by engagement if prioritization is enabled
            if self.config.prioritize_top_liked:
                posts_list.sort(key=lambda p: p.engagement_score, reverse=True)

            # Limit posts to configured range
            posts_count = len(posts_list)
            if posts_count < self.config.min_posts_per_profile:
                print(
                    f"⚠️  Only found {posts_count} posts for @{profile.username} (min: {self.config.min_posts_per_profile})")
            elif posts_count > self.config.max_posts_per_profile:
                posts_list = posts_list[:self.config.max_posts_per_profile]
                print(f"✂️  Limited to {self.config.max_posts_per_profile} posts for @{profile.username}")

            # Store results
            self.profile_posts[profile.user_id] = posts_list
            self.loaded_profiles[profile.user_id] = {
                'profile': profile,
                'posts_count': len(posts_list),
                'load_timestamp': datetime.now().isoformat(),
                'profile_url': profile_url
            }

            print(f"🎯 Successfully loaded {len(posts_list)} posts for @{profile.username}")
            return posts_list

        except Exception as e:
            print(f"❌ Error loading profile @{profile.username}: {e}")
            self.failed_profiles.append(profile.user_id)
            return []

        finally:
            # Restore original scrolling method
            self.perform_scrolling = original_perform_scrolling

    async def load_all_profiles(self) -> Dict[str, List[PostData]]:
        """
        Load posts for all configured profiles with delays between loads

        Returns:
            Dictionary mapping user_id to list of PostData objects
        """
        if not self.profiles_to_load:
            print("❌ No profiles to load. Use load_profiles_from_search_results() or set_profiles_to_load() first.")
            return {}

        print(f"\n🚀 Starting to load {self.total_profiles_to_load} profiles...")

        for i, profile in enumerate(self.profiles_to_load):
            self.current_profile_index = i

            print(f"\n{'=' * 60}")
            print(f"📋 Progress: {i + 1}/{self.total_profiles_to_load} profiles")
            print(f"{'=' * 60}")

            try:
                await self.load_profile_posts(profile)

                # Add delay between profile loads (except for last profile)
                if i < len(self.profiles_to_load) - 1:
                    import random
                    delay = random.uniform(
                        self.config.profile_load_delay_min,
                        self.config.profile_load_delay_max
                    )
                    print(f"⏳ Waiting {delay:.1f}s before next profile (human-like delay)...")
                    await asyncio.sleep(delay)

            except Exception as e:
                print(f"❌ Critical error loading profile @{profile.username}: {e}")
                self.failed_profiles.append(profile.user_id)
                continue

        return self.profile_posts

    def get_load_summary(self) -> Dict[str, Any]:
        """Get summary of profile loading results"""
        successful_loads = len(self.loaded_profiles)
        failed_loads = len(self.failed_profiles)
        total_posts = sum(len(posts) for posts in self.profile_posts.values())

        # Calculate average posts per profile
        avg_posts = total_posts / successful_loads if successful_loads > 0 else 0

        # Get top profiles by engagement
        top_profiles = []
        for user_id, profile_data in self.loaded_profiles.items():
            profile = profile_data['profile']
            posts_count = profile_data['posts_count']
            top_profiles.append({
                'username': profile.username,
                'display_name': profile.display_name,
                'followers': profile.follower_count,
                'hearts': profile.heart_count,
                'posts_loaded': posts_count
            })

        # Sort by engagement
        top_profiles.sort(key=lambda p: p['followers'] + p['hearts'], reverse=True)

        return {
            'total_profiles_attempted': self.total_profiles_to_load,
            'successful_loads': successful_loads,
            'failed_loads': failed_loads,
            'success_rate': (
                    successful_loads / self.total_profiles_to_load * 100) if self.total_profiles_to_load > 0 else 0,
            'total_posts_collected': total_posts,
            'average_posts_per_profile': avg_posts,
            'top_profiles': top_profiles[:10],  # Top 10 by engagement
            'failed_profile_ids': self.failed_profiles,
            'config_used': {
                'min_posts_per_profile': self.config.min_posts_per_profile,
                'max_posts_per_profile': self.config.max_posts_per_profile,
                'scroll_count': self.config.scroll_count,
                'profile_load_delay_range': f"{self.config.profile_load_delay_min}-{self.config.profile_load_delay_max}s"
            }
        }

    def save_results(self, filename: str = "tiktok_profile_posts.json"):
        """Save profile loading results to JSON file"""
        output_data = {
            'load_metadata': {
                'total_profiles_loaded': len(self.loaded_profiles),
                'timestamp': datetime.now().isoformat(),
                'config': self.config.__dict__
            },
            'load_summary': self.get_load_summary(),
            'loaded_profiles': self.loaded_profiles,
            'profile_posts': {
                user_id: [
                    {
                        'post_id': post.post_id,
                        'desc': post.desc,
                        'create_time': post.create_time,
                        'author_id': post.author_id,
                        'author_username': post.author_username,
                        'stats': post.stats,
                        'video_url': post.video_url,
                        'music_info': post.music_info,
                        'hashtags': post.hashtags,
                        'engagement_score': post.engagement_score,
                        'raw_post_data': post.raw_post_data
                    }
                    for post in posts
                ]
                for user_id, posts in self.profile_posts.items()
            }
        }

        OptimizedNoDriver.save_json_to_file(output_data, filename)
        return output_data


async def main():
    # After running Step 1 search
    search_scraper = TikTokSearchScraper(['#fitness', '#nutrition'])
    search_results = await search_scraper.search_all_hashtags()

    # Configure and run Step 2 profile loading
    config = ProfileLoadConfig(
        min_posts_per_profile=50,
        max_posts_per_profile=70,
        scroll_count=2,
        profile_load_delay_min=10.0,
        profile_load_delay_max=20.0
    )

    profile_loader = TikTokProfileLoader(config)
    profile_loader.load_profiles_from_search_results(search_scraper)
    profile_posts = await profile_loader.load_all_profiles()

    # Save results
    profile_loader.save_results("profile_posts_results.json")


if __name__ == "__main__":
    asyncio.run(main())
