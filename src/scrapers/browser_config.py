import asyncio
import json

import zendriver as uc
import time
import random
from typing import List, Optional


class OptimizedNoDriver:
    """
    Optimized nodriver configuration balancing speed and stealth
    """

    @staticmethod
    def get_stealth_args() -> List[str]:
        """
        Chrome arguments that improve speed while maintaining stealth
        """
        return [
            # Performance optimizations
            '--disable-blink-features=AutomationControlled',
            '--disable-features=VizDisplayCompositor',
            '--disable-extensions',
            '--disable-plugins',
            '--disable-images',  # Skip loading images for speed
            '--disable-gpu',
            '--disable-dev-shm-usage',
            '--disable-software-rasterizer',

            # Memory optimizations
            '--memory-pressure-off',
            '--max_old_space_size=4096',
            '--disable-background-timer-throttling',
            '--disable-renderer-backgrounding',
            '--disable-backgrounding-occluded-windows',

            # Network optimizations
            '--aggressive-cache-discard',
            '--disable-background-networking',
            '--disable-default-apps',
            '--disable-sync',

            # Stealth (avoid detection)
            '--disable-blink-features=AutomationControlled',
            '--exclude-switches=enable-automation',
            '--disable-infobars',
            '--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',

            # Window management
            '--window-size=1920,1080',
        ]

    @staticmethod
    def get_minimal_args() -> List[str]:
        """
        Minimal args for maximum speed (less stealthy)
        """
        return [
            '--disable-gpu',
            '--disable-dev-shm-usage',
            '--disable-images',
            '--disable-plugins',
            '--disable-extensions',
            '--memory-pressure-off',
            '--window-size=1920,1080',
        ]

    @staticmethod
    async def create_optimized_browser(stealth_mode: bool = True) -> uc.Browser:
        """
        Create optimized browser instance
        """
        args = OptimizedNoDriver.get_stealth_args() if stealth_mode else OptimizedNoDriver.get_minimal_args()

        config = uc.Config()
        for arg in args:
            config.add_argument(arg)

        # Browser-level optimizations
        config.user_data_dir = None  # Don't save user data

        browser = await uc.start(config=config)
        return browser

    @staticmethod
    async def add_human_like_delays(page, min_delay: float = 0.1, max_delay: float = 0.3):
        """
        Add human-like random delays
        """
        delay = random.uniform(min_delay, max_delay)
        await asyncio.sleep(delay)

    @staticmethod
    async def human_like_page_load_wait(min_wait: float = 12.0, max_wait: float = 18.0):
        """
        Human-like wait after page load - varies the initial 15s wait
        """
        wait_time = random.uniform(min_wait, max_wait)
        print(f"⏳ Waiting {wait_time:.1f}s for page to settle (human-like)...")
        await asyncio.sleep(wait_time)

    @staticmethod
    async def human_like_scroll(page, scroll_amount_base: int = 80, variation: int = 20):
        """
        Scroll with human-like variation in scroll amount
        """
        scroll_amount = random.randint(
            scroll_amount_base - variation,
            scroll_amount_base + variation
        )
        print(f"📜 Scrolling {scroll_amount}px (human-like)")
        await page.scroll_down(scroll_amount)

    @staticmethod
    async def human_like_scroll_pause(base_pause: float = 2.0, variation: float = 1.0):
        """
        Variable pause between scrolls to mimic human reading/browsing
        """
        pause_time = random.uniform(
            max(0.5, base_pause - variation),
            base_pause + variation
        )
        print(f"⏸️  Pausing {pause_time:.1f}s (human-like)")
        await asyncio.sleep(pause_time)

    @staticmethod
    async def human_like_scroll_sequence(page, scroll_count: int = 5, base_pause: float = 2.0):
        """
        Complete human-like scrolling sequence
        """
        for i in range(scroll_count):
            print(f"\n📜 --- Human-like Scroll {i + 1}/{scroll_count} ---")

            # Human-like scroll with variation
            await OptimizedNoDriver.human_like_scroll(page)

            # Variable pause between scrolls
            await OptimizedNoDriver.human_like_scroll_pause(base_pause)

            # Occasional longer pause (like reading something interesting)
            if random.random() < 0.3:  # 30% chance
                extra_pause = random.uniform(1.0, 3.0)
                print(f"👁️  Extra pause {extra_pause:.1f}s (reading content)")
                await asyncio.sleep(extra_pause)

    @staticmethod
    async def human_like_response_processing_delay():
        """
        Small delay during response processing to mimic human reaction time
        """
        delay = random.uniform(0.05, 0.15)
        await asyncio.sleep(delay)

    @staticmethod
    def save_json_to_file(json_data, file_name="output.json"):
        """
        Saves parsed JSON data to a file.

        Parameters:
        - data (dict or list): The JSON data to save.
        - file_name (str): The name of the JSON file to write to.
        """
        try:
            with open(file_name, "w+", encoding="utf-8-sig") as file:
                json.dump(json_data, file, indent=4, ensure_ascii=False)
            print(f"✓ Data saved to {file_name}")
        except Exception as e:
            print(f"✗ Failed to save JSON: {e}")
