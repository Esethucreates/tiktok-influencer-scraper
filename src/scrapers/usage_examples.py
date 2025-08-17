# capture requests examples
import asyncio

from src.scrapers.DTOs.profile_loader_schemas import ProfileLoadConfig
from src.scrapers.core_parts.browserConfig import OptimizedNoDriver
from src.scrapers.core_parts.requestMonitor import CDPXHRMonitor
from src.scrapers.scraper_parts.profileLoader import TikTokProfileLoader
from src.scrapers.scraper_parts.searchResultsScraper import TikTokSearchScraper


async def main():
    TARGET_PAGE = "https://www.tiktok.com/@kabelo_manyiki"
    REGEX_PATTERN = r"https:\/\/www\.tiktok\.com\/api\/post\/item_list\/\?[^ ]+"

    monitor = CDPXHRMonitor(
        target_url=TARGET_PAGE,
        regex_pattern=REGEX_PATTERN,
        scroll_count=3,
        scroll_pause=3
    )

    try:
        results = await monitor.run()

        print(f"\n=== RESULTS ===")
        if not results:
            print("No matching responses found!")
        else:
            for i, item in enumerate(results, 1):
                print(f"\nResponse {i}:")
                print(f"URL: {item['url']}")
                print(f"Status: {item['status']}")
                print(f"Body available: {item['body'] is not None}")
                print(f"Is large response: {item.get('is_large_response', False)}")
                print(f"Attempts needed: {item.get('attempts_needed', 'N/A')}")

                if item['body'] is not None:
                    OptimizedNoDriver.save_json_to_file(item['body'])
                    if isinstance(item['body'], dict):
                        print(f"Body keys: {list(item['body'].keys())}")
                    elif isinstance(item['body'], str):
                        print(f"Body length: {len(item['body'])} characters")
                else:
                    print(f"Error: {item.get('error', 'Unknown error')}")

    except Exception as e:
        print(f"Error in main: {e}")
        import traceback
        traceback.print_exc()


# Profile scraper
async def profile_scraper():
    # After running search
    search_scraper = TikTokSearchScraper(['#fitness'], max_profiles_per_hashtag=3, scroll_count=4)
    search_results = await search_scraper.run_search_session()
    search_scraper.save_results()

    stats = search_scraper.get_summary_stats()
    print(f"Total profiles found: {stats['total_unique_profiles']}")

    # Configure and run profile loading
    config = ProfileLoadConfig(
        max_posts_per_profile=3,
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


# Search results usage
async def search_results():
    hashtags = ["#technology", "#coding"]
    scraper = TikTokSearchScraper(hashtags, max_profiles_per_hashtag=2, scroll_count=2, scroll_pause=3)

    # Method 1: Use the convenience method (recommended)
    results = await scraper.run_search_session()

    # Save results
    scraper.save_results("../fileExports/jsonFiles/my_tiktok_results.json")

    # Print summary
    stats = scraper.get_summary_stats()
    print(f"Total profiles found: {stats['total_unique_profiles']}")


if __name__ == "__main__":
    asyncio.run(main())
