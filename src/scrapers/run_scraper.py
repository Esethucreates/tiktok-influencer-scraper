import asyncio

from src.scrapers.DTOs.unified_schemas import UnifiedScraperConfig
from src.scrapers.playground import UnifiedTikTokScraper


async def main():
    config = UnifiedScraperConfig(
        # Search Configuration - Efficient but natural
        max_profiles_per_hashtag=20,  # Your requirement: 200 profiles per hashtag
        search_scroll_count=20,  # Increased to find 200 profiles per hashtag
        search_scroll_pause=3,  # Faster search scrolling (users scroll quickly through search)

        # Profile Loading - Streamlined for volume
        max_posts_per_profile=25,  # Your requirement: 25 posts per profile
        profile_scroll_count=5,  # Sufficient to load 25 posts
        profile_scroll_pause_min=3.5,  # Faster profile browsing (users skim quickly)
        profile_scroll_pause_max=5.3,  # Still natural but efficient
        profile_scroll_amount_base=500,
        profile_scroll_amount_variation=200,

        # Page Loading - Optimized for automation detection avoidance
        page_load_wait_min=10.0,  # Shorter waits to speed up process
        page_load_wait_max=17.0,  # But still realistic
        profile_navigation_delay_min=10.1,  # Quick profile switches
        profile_navigation_delay_max=16.5,  # Occasional longer pauses

        # Inter-operation delays - Balanced efficiency
        profile_load_delay_min=12.0,  # Faster transitions between profiles
        profile_load_delay_max=18.2,  # Random longer pauses to seem natural

        # Comments Loading - Critical for your 1.25M comment requirement
        max_comments_per_post=50,  # Your requirement: 50 comments per post
        max_scroll_attempts_comments=13,  # Sufficient to load 50 comments
        comment_scroll_pause_min=4.5,  # Fast comment scrolling (users scroll quickly through comments)
        comment_scroll_pause_max=9.5,  # Brief pauses
        comment_scroll_amount_base=600,
        comment_scroll_amount_variation=150,

        # Post Navigation - Efficient video browsing
        post_load_wait_min=5.0,  # Quick post loading
        post_load_wait_max=10.0,  # Account for video loading
        post_close_wait_min=5.0,  # Fast post transitions
        post_close_wait_max=10.0,  # Brief closing delays

        # Human-like Interaction - Reduced frequency for efficiency
        reading_pause_probability=0.8,  # Lower probability (30%) to speed up
        reading_pause_min=3.0,  # Quick reading pauses
        reading_pause_max=5.0,  # Shorter max reading time
        scroll_direction_change_probability=0.3,  # 20% chance (less frequent)
        scroll_up_amount=150,  # Smaller scroll backs

        # Video and Comment Detection - Optimized timeouts
        video_link_search_timeout=8,  # Shorter timeout
        video_link_scroll_attempts=3,  # Fewer attempts
        video_link_scroll_pause=1.5,  # Faster scrolling
        comment_section_wait_timeout=10,  # Shorter wait for comment sections

        # Session time
        session_duration_minutes=45
    )
    scraper = UnifiedTikTokScraper(config)

    hashtags = ["#FitTok", "#WellnessTok", "#Gains", "#FitFam", "#FitInspo"]
    results = await scraper.run_complete_session(hashtags)
    scraper.save_results()


if __name__ == "__main__":
    asyncio.run(main())
