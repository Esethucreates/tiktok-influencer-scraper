import asyncio
import urllib.parse
from dataclasses import dataclass
from typing import Dict, List, Optional, Any

import zendriver as uc

from browser_config import OptimizedNoDriver
from src.scrapers.main_scaper import CDPXHRMonitor
from src.utils.exceptions import AuthenticationError


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
    raw_author_data: Dict[str, Any]
    raw_author_stats: Dict[str, Any]


class TikTokSearchScraper(CDPXHRMonitor):
    """
    TikTok Search Scraper that extends CDPXHRMonitor to capture hashtag search results.

    This class implements session management to reuse browser instances across
    multiple hashtag searches, improving performance and reducing detection risk.
    """

    def __init__(self, hashtags: List[str], max_profiles_per_hashtag: int = 100,
                 search_url_pattern: str = r"https://www\.tiktok\.com/api/search/general/full/\?[^ ]+",
                 scroll_count: int = 10, scroll_pause: int = 3):
        """
        Initialize the TikTok Search Scraper with session management capabilities.

        Args:
            hashtags: List of hashtags to search (with or without # symbol)
            max_profiles_per_hashtag: Maximum profiles to collect per hashtag
            search_url_pattern: Regex pattern for TikTok search API endpoints
            scroll_count: Number of scrolls to perform per search
            scroll_pause: Seconds to wait between scrolls
        """
        # Initialize with placeholder URL - will be set dynamically
        super().__init__(
            target_url="https://www.tiktok.com",
            regex_pattern=search_url_pattern,
            scroll_count=scroll_count,
            scroll_pause=scroll_pause
        )

        self.hashtags = self._normalize_hashtags(hashtags)
        self.max_profiles_per_hashtag = max_profiles_per_hashtag

        # Session management state
        self.session_active = False
        self.session_initialized = False

        # Storage for extracted data
        self.search_results: Dict[str, List[Dict]] = {}  # hashtag -> raw results
        self.author_profiles: Dict[str, AuthorProfile] = {}  # user_id -> profile
        self.hashtag_to_profiles: Dict[str, List[str]] = {}  # hashtag -> user_ids

    @staticmethod
    def _normalize_hashtags(hashtags: List[str]) -> List[str]:
        """Normalize hashtags by ensuring they start with #"""
        normalized = []
        for tag in hashtags:
            tag = tag.strip()
            if not tag.startswith('#'):
                tag = f'#{tag}'
            normalized.append(tag)
        return normalized

    @staticmethod
    def _build_search_url(hashtag: str) -> str:
        """Build TikTok search URL for a specific hashtag"""
        search_term = hashtag.lstrip('#')
        encoded_term = urllib.parse.quote(search_term)

        import time
        timestamp = int(time.time() * 1000)

        search_url = f"https://www.tiktok.com/search?q={encoded_term}&t={timestamp}"
        return search_url

    @staticmethod
    def _extract_author_from_item(item_data: Dict[str, Any]) -> Optional[AuthorProfile]:
        """Extract author profile information from TikTok search result item"""
        try:
            author = item_data.get('author', {})
            author_stats = item_data.get('authorStats', {})

            if not author or not author.get('id'):
                return None

            profile = AuthorProfile(
                user_id=str(author.get('id', '')),
                username=author.get('uniqueId', ''),
                display_name=author.get('nickname', ''),
                avatar_url=author.get('avatarMedium', ''),
                verified=author.get('verified', False),
                follower_count=int(author_stats.get('followerCount', 0)),
                following_count=int(author_stats.get('followingCount', 0)),
                heart_count=int(author_stats.get('heartCount', 0)),
                video_count=int(author_stats.get('videoCount', 0)),
                raw_author_data=author,
                raw_author_stats=author_stats
            )

            return profile

        except Exception as e:
            print(f"Error extracting author data: {e}")
            return None

    def _process_search_response(self, response_data: Dict[str, Any], hashtag: str) -> List[AuthorProfile]:
        """Process a TikTok search API response and extract author profiles"""
        profiles = []

        try:
            body = response_data.get('body', {})
            data = body.get('data', [])

            if not isinstance(data, list):
                print(f"Unexpected data structure for {hashtag}: {type(data)}")
                return profiles

            print(f"Processing {len(data)} results for hashtag: {hashtag}")

            for result in data:
                item = result.get('item', {})
                if not item:
                    continue

                profile = self._extract_author_from_item(item)
                if profile:
                    profiles.append(profile)
                    self.author_profiles[profile.user_id] = profile

            print(f"Extracted {len(profiles)} valid profiles for hashtag: {hashtag}")

        except Exception as e:
            print(f"Error processing search response for {hashtag}: {e}")

        return profiles

    async def start_session(self):
        """
        Start a browser session for hashtag searching.
        This method initializes the browser and authentication once.
        """
        if self.session_active:
            print("Session already active")
            return

        try:
            print("🚀 Starting TikTok search session...")

            # Start browser and ensure authentication using parent's method
            await self.start_browser()

            if not self.is_authenticated:
                raise AuthenticationError("Failed to establish authenticated session")

            # Set up response monitoring
            self.page.add_handler(uc.cdp.network.ResponseReceived, self.on_response_received)

            self.session_active = True
            self.session_initialized = True

            print("✅ Session started successfully with authenticated browser")

        except Exception as e:
            print(f"❌ Error starting session: {e}")
            await self.cleanup_session()
            raise

    async def end_session(self):
        """End the browser session and cleanup resources"""
        if not self.session_active:
            print("No active session to end")
            return

        try:
            print("🔄 Ending TikTok search session...")
            await self.cleanup_session()
            print("✅ Session ended successfully")

        except Exception as e:
            print(f"❌ Error ending session: {e}")
            # Force cleanup even if there's an error
            await self.cleanup_session()

    async def cleanup_session(self):
        """Clean up session resources"""
        self.session_active = False
        self.session_initialized = False

        # Use parent's cleanup method
        await self.stop_browser()

    async def navigate_to_hashtag(self, hashtag: str) -> str:
        """Navigate to a specific hashtag search page"""
        if not self.session_active:
            raise RuntimeError("Session not active. Call start_session() first.")

        search_url = self._build_search_url(hashtag)
        print(f"📍 Navigating to: {search_url}")

        try:
            await self.page.get(search_url)
            print("✅ Navigation successful")
            return search_url

        except Exception as e:
            print(f"❌ Navigation failed: {e}")
            raise

    async def search_hashtag(self, hashtag: str) -> List[AuthorProfile]:
        """
        Search for a specific hashtag and extract author profiles.
        This method now reuses the existing browser session.
        """
        if not self.session_active:
            raise RuntimeError("Session not active. Call start_session() first.")

        print(f"\n🔍 Searching hashtag: {hashtag}")

        try:
            # Clear previous responses for this search
            self.matched_responses = []

            # Navigate to the hashtag search page
            await self.navigate_to_hashtag(hashtag)

            # Wait for initial page load
            print("⏳ Waiting for initial page load...")
            await asyncio.sleep(8)

            print(f"📊 Responses after initial load: {len(self.matched_responses)}")

            # Perform scrolling to load more content
            print("🔄 Starting scrolling sequence...")
            await self.perform_scrolling()

            # Final wait for any remaining responses
            print("⏳ Final wait for remaining responses...")
            await asyncio.sleep(3)

            # Process captured responses
            captured_responses = self.matched_responses.copy()

            if not captured_responses:
                print(f"❌ No API responses captured for hashtag: {hashtag}")
                return []

            print(f"✅ Captured {len(captured_responses)} API responses for {hashtag}")

            # Process all captured responses
            all_profiles = []
            for response in captured_responses:
                profiles = self._process_search_response(response, hashtag)
                all_profiles.extend(profiles)

            # Store results
            self.search_results[hashtag] = captured_responses.copy()

            # Deduplicate and limit profiles
            unique_profiles = {}
            for profile in all_profiles:
                if profile.user_id not in unique_profiles:
                    unique_profiles[profile.user_id] = profile

            # Sort by engagement and limit
            sorted_profiles = sorted(
                unique_profiles.values(),
                key=lambda p: p.follower_count + p.heart_count,
                reverse=True
            )[:self.max_profiles_per_hashtag]

            # Store hashtag to profile mapping
            self.hashtag_to_profiles[hashtag] = [p.user_id for p in sorted_profiles]

            print(f"🎯 Found {len(sorted_profiles)} unique profiles for {hashtag}")
            return sorted_profiles

        except Exception as e:
            print(f"❌ Error searching hashtag {hashtag}: {e}")
            # Skip this hashtag and continue with next one
            return []

    async def search_all_hashtags(self) -> Dict[str, List[AuthorProfile]]:
        """
        Search all configured hashtags using the active session.
        This method manages the entire search process using a single browser session.
        """
        if not self.session_active:
            raise RuntimeError("Session not active. Call start_session() first.")

        print(f"\n🎯 Starting search for {len(self.hashtags)} hashtags")
        results = {}

        for i, hashtag in enumerate(self.hashtags, 1):
            print(f"\n📈 Progress: {i}/{len(self.hashtags)} hashtags")

            try:
                profiles = await self.search_hashtag(hashtag)
                results[hashtag] = profiles

                # Add delay between hashtag searches (except for the last one)
                if hashtag != self.hashtags[-1]:
                    print("⏳ Waiting before next hashtag search...")
                    await OptimizedNoDriver.human_like_page_load_wait(5.0, 25.0)

            except Exception as e:
                print(f"❌ Error searching hashtag {hashtag}: {e}")
                results[hashtag] = []

        print(f"\n🎉 Search completed! Processed {len(results)} hashtags")
        return results

    async def run_search_session(self) -> Dict[str, List[AuthorProfile]]:
        """
        Convenience method that manages the entire search session lifecycle.
        This is the main entry point for the scraper.
        """
        try:
            # Start session
            await self.start_session()

            # Perform all searches
            results = await self.search_all_hashtags()

            return results

        finally:
            # Always end session, even if there's an error
            await self.end_session()

    def get_summary_stats(self) -> Dict[str, Any]:
        """Get summary statistics of the search results"""
        total_profiles = len(self.author_profiles)
        hashtag_counts = {ht: len(profiles) for ht, profiles in self.hashtag_to_profiles.items()}

        if self.author_profiles:
            followers = [p.follower_count for p in self.author_profiles.values()]
            hearts = [p.heart_count for p in self.author_profiles.values()]

            stats = {
                'total_unique_profiles': total_profiles,
                'hashtag_profile_counts': hashtag_counts,
                'avg_followers': sum(followers) / len(followers),
                'max_followers': max(followers),
                'avg_hearts': sum(hearts) / len(hearts),
                'max_hearts': max(hearts),
                'total_api_responses': sum(len(responses) for responses in self.search_results.values())
            }
        else:
            stats = {
                'total_unique_profiles': 0,
                'hashtag_profile_counts': hashtag_counts,
                'total_api_responses': 0
            }

        return stats

    def save_results(self, filename: str = "tiktok_search_results.json"):
        """Save all search results to JSON file"""
        output_data = {
            'search_metadata': {
                'hashtags_searched': self.hashtags,
                'max_profiles_per_hashtag': self.max_profiles_per_hashtag,
                'timestamp': asyncio.get_event_loop().time()
            },
            'summary_stats': self.get_summary_stats(),
            'hashtag_to_profiles': self.hashtag_to_profiles,
            'author_profiles': {
                user_id: {
                    'user_id': profile.user_id,
                    'username': profile.username,
                    'display_name': profile.display_name,
                    'avatar_url': profile.avatar_url,
                    'verified': profile.verified,
                    'follower_count': profile.follower_count,
                    'following_count': profile.following_count,
                    'heart_count': profile.heart_count,
                    'video_count': profile.video_count,
                    'raw_author_data': profile.raw_author_data,
                    'raw_author_stats': profile.raw_author_stats
                }
                for user_id, profile in self.author_profiles.items()
            },
            'raw_search_responses': self.search_results
        }

        OptimizedNoDriver.save_json_to_file(output_data, filename)
        return output_data


# Usage example:

async def main():
    hashtags = ["#technology", "#coding"]
    scraper = TikTokSearchScraper(hashtags, max_profiles_per_hashtag=20)

    # Method 1: Use the convenience method (recommended)
    results = await scraper.run_search_session()

    # Method 2: Manual session management (for more control)
    # await scraper.start_session()
    # try:
    #     results = await scraper.search_all_hashtags()
    # finally:
    #     await scraper.end_session()

    # Save results
    scraper.save_results("my_tiktok_results.json")

    # Print summary
    stats = scraper.get_summary_stats()
    print(f"Total profiles found: {stats['total_unique_profiles']}")


if __name__ == "__main__":
    asyncio.run(main())
