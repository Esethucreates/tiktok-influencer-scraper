import asyncio
import json
import random
from typing import List

import zendriver as uc


class OptimizedNoDriver:
    """
    A utility class for creating and managing optimized web browser automation.

    This class helps you control a web browser programmatically (like Chrome) in a way that:
    - Runs faster by disabling unnecessary features
    - Mimics human behavior to avoid detection by websites
    - Provides tools for realistic scrolling and timing

    Think of it as a "robot browser" that can visit websites, scroll pages, and extract
    information while trying to look like a real human user rather than an automated script.

    The class contains only static methods, meaning you don't need to create an instance
    of the class to use it - you can call the methods directly.
    """

    @staticmethod
    def get_stealth_args() -> List[str]:
        """
        Gets a list of Chrome browser settings that make automation look more human-like.

        This method returns special command-line arguments (settings) that tell Chrome
        to run in a way that websites are less likely to detect as automated browsing.
        It balances being stealthy (undetectable) with running efficiently.

        Think of these as "disguise settings" for your robot browser - they help it
        blend in with real human users while still running reasonably fast.

        Returns:
            List[str]: A list of Chrome command-line arguments (settings) as text strings.
                      Each string is a specific instruction like '--disable-images'
                      that tells Chrome to behave in a certain way.

        Example:
            args = OptimizedNoDriver.get_stealth_args()
            # Returns something like: ['--disable-images', '--disable-gpu', ...]
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
        Gets a minimal list of Chrome browser settings focused purely on speed.

        This method returns the bare minimum settings needed to make Chrome run as
        fast as possible. Unlike get_stealth_args(), this prioritizes speed over
        being undetectable, so websites might more easily recognize this as automation.

        Use this when you need maximum speed and don't care about stealth, or when
        you're testing on websites that don't try to block automated browsing.

        Returns:
            List[str]: A shorter list of Chrome command-line arguments focused on speed.
                      Contains only the most essential performance settings.

        Example:
            args = OptimizedNoDriver.get_minimal_args()
            # Returns fewer arguments than get_stealth_args(), focused on speed
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
        Creates and starts a new browser instance with optimized settings.

        This is like opening a new Chrome window, but configured specifically for
        automation tasks. The browser will be set up with either stealth settings
        (to avoid detection) or minimal settings (for maximum speed).

        The 'async' keyword means this function can run alongside other tasks and
        won't block your program while the browser is starting up.

        Parameters:
            stealth_mode (bool): Whether to use stealth settings or speed settings.
                               - True (default): Uses stealth_args for undetectable browsing
                               - False: Uses minimal_args for fastest possible browsing

        Returns:
            uc.Browser: A browser object that you can use to visit websites, click buttons,
                       fill forms, and extract information. This is your main tool for
                       controlling the automated browser.

        Example:
            # Create a stealthy browser (default)
            browser = await OptimizedNoDriver.create_optimized_browser()

            # Create a fast browser (less stealthy)
            browser = await OptimizedNoDriver.create_optimized_browser(stealth_mode=False)
        """
        args = OptimizedNoDriver.get_stealth_args() if stealth_mode else OptimizedNoDriver.get_minimal_args()

        config = uc.Config(
        )
        for arg in args:
            config.add_argument(arg)

        # Browser-level optimizations
        config.user_data_dir = None  # Don't save user data

        browser = await uc.start(config=config)
        return browser

    @staticmethod
    async def add_human_like_delays(page, min_delay: float = 0.1, max_delay: float = 0.3):
        """
        Adds a small random delay to mimic human reaction time.

        Real humans don't click buttons or perform actions instantly - there's always
        a small delay as we process information and move our mouse. This function
        adds realistic delays to make your automation look more human.

        The delay time is random between min_delay and max_delay, so it's not
        predictably the same every time (which would look robotic).

        Parameters:
            page: The webpage object you're currently working with (from the browser)
            min_delay (float): Minimum delay time in seconds. Default is 0.1 seconds.
            max_delay (float): Maximum delay time in seconds. Default is 0.3 seconds.

        Returns:
            None: This function doesn't return anything, it just creates a pause.

        Example:
            # Add a random delay between 0.1 and 0.3 seconds
            await OptimizedNoDriver.add_human_like_delays(page)

            # Add a longer delay between 0.5 and 1.0 seconds
            await OptimizedNoDriver.add_human_like_delays(page, 0.5, 1.0)
        """
        delay = random.uniform(min_delay, max_delay)
        await asyncio.sleep(delay)

    @staticmethod
    async def human_like_page_load_wait(min_wait: float = 12.0, max_wait: float = 18.0):
        """
        Waits for a webpage to fully load, with human-like timing variation.

        When humans visit a webpage, they typically wait for it to fully load and
        settle before doing anything. This function mimics that behavior by waiting
        a random amount of time between min_wait and max_wait seconds.

        This is especially important for complex websites that take time to load
        all their content, or websites that might be suspicious of users who
        interact too quickly after page load.

        Parameters:
            min_wait (float): Minimum wait time in seconds. Default is 12.0 seconds.
            max_wait (float): Maximum wait time in seconds. Default is 18.0 seconds.

        Returns:
            None: This function doesn't return anything, it just creates a waiting period.

        Example:
            # Wait 12-18 seconds (default)
            await OptimizedNoDriver.human_like_page_load_wait()

            # Wait 5-10 seconds (faster)
            await OptimizedNoDriver.human_like_page_load_wait(5.0, 10.0)
        """
        wait_time = random.uniform(min_wait, max_wait)
        print(f"⏳ Waiting {wait_time:.1f}s for page to settle (human-like)...")
        await asyncio.sleep(wait_time)

    @staticmethod
    async def human_like_scroll(page, scroll_amount_base: int = 80, variation: int = 20):
        """
        Scrolls down a webpage with human-like variation in scroll distance.

        Real humans don't scroll the exact same distance every time - sometimes
        they scroll a little, sometimes a lot. This function mimics that natural
        variation by randomly adjusting how far to scroll each time.

        The actual scroll distance will be somewhere between
        (scroll_amount_base - variation) and (scroll_amount_base + variation).

        Parameters:
            page: The webpage object you want to scroll (from the browser)
            scroll_amount_base (int): Base scroll distance in pixels. Default is 80 pixels.
            variation (int): How much the scroll distance can vary. Default is 20 pixels.
                           This means scrolling will be between 60-100 pixels by default.

        Returns:
            None: This function doesn't return anything, it just scrolls the page.

        Example:
            # Scroll 60-100 pixels (default variation)
            await OptimizedNoDriver.human_like_scroll(page)

            # Scroll 180-220 pixels (larger scrolls)
            await OptimizedNoDriver.human_like_scroll(page, 200, 20)
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
        Creates a pause between scrolls that mimics human reading/browsing behavior.

        When humans scroll through a webpage, they don't scroll continuously -
        they pause to read content, look at images, or process information.
        This function creates realistic pauses with natural variation.

        The pause time will be random between (base_pause - variation) and
        (base_pause + variation), but never less than 0.5 seconds.

        Parameters:
            base_pause (float): Average pause time in seconds. Default is 2.0 seconds.
            variation (float): How much the pause can vary. Default is 1.0 seconds.
                             This means pauses will be between 1.0-3.0 seconds by default.

        Returns:
            None: This function doesn't return anything, it just creates a pause.

        Example:
            # Pause 1-3 seconds (default)
            await OptimizedNoDriver.human_like_scroll_pause()

            # Pause 0.5-2.5 seconds (faster browsing)
            await OptimizedNoDriver.human_like_scroll_pause(1.5, 1.0)
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
        Performs a complete sequence of human-like scrolling with realistic behavior.

        This function combines multiple scrolls with pauses to simulate how a real
        person would browse through a long webpage. It includes:
        - Variable scroll distances
        - Random pauses between scrolls
        - Occasional longer pauses (like when reading interesting content)
        - Progress messages so you can see what's happening

        Parameters:
            page: The webpage object you want to scroll through (from the browser)
            scroll_count (int): How many times to scroll. Default is 5 scrolls.
            base_pause (float): Average pause time between scrolls in seconds. Default is 2.0.

        Returns:
            None: This function doesn't return anything, it just performs the scrolling sequence.

        Example:
            # Scroll 5 times with 2-second average pauses (default)
            await OptimizedNoDriver.human_like_scroll_sequence(page)

            # Scroll 10 times with faster 1-second average pauses
            await OptimizedNoDriver.human_like_scroll_sequence(page, 10, 1.0)
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
        Adds a tiny delay to mimic human response processing time.

        When humans read information or see results on a screen, there's always
        a brief moment of processing time before they react. This function adds
        that realistic delay to make automation look more natural.

        The delay is very short (0.05 to 0.15 seconds) - just enough to avoid
        looking like instant computer responses.

        Parameters:
            None: This function takes no parameters.

        Returns:
            None: This function doesn't return anything, it just creates a brief pause.

        Example:
            # Add a tiny human-like processing delay
            await OptimizedNoDriver.human_like_response_processing_delay()
        """
        delay = random.uniform(0.05, 0.15)
        await asyncio.sleep(delay)

    @staticmethod
    def save_json_to_file(json_data, file_name: str = "output.json"):
        """
        Saves data in JSON format to a file on your computer.

        JSON is a common format for storing structured data (like lists and dictionaries).
        This function takes your data and saves it as a readable JSON file that you
        can open later, share with others, or import into other programs.

        The file will be saved in the same folder as your Python script, with proper
        formatting (indented and readable) and support for international characters.

        Parameters:
            json_data (dict or list): The data you want to save. This can be:
                                     - A dictionary (like {'name': 'John', 'age': 30})
                                     - A list (like [1, 2, 3] or [{'id': 1}, {'id': 2}])
                                     - Any combination of nested dictionaries and lists
            file_name (str): The name for your output file. Default is "output.json".
                           Should end with ".json" extension.

        Returns:
            None: This function doesn't return anything, but it prints success/error messages.

        Example:
            # Save a simple dictionary
            data = {'name': 'John', 'age': 30}
            OptimizedNoDriver.save_json_to_file(data, "person.json")

            # Save a list of items
            items = [{'id': 1, 'title': 'Item 1'}, {'id': 2, 'title': 'Item 2'}]
            OptimizedNoDriver.save_json_to_file(items, "items.json")
        """
        try:
            with open(file_name, "w+", encoding="utf-8-sig") as file:
                json.dump(json_data, file, indent=4, ensure_ascii=False)
            print(f"✓ Data saved to {file_name}")
        except Exception as e:
            print(f"✗ Failed to save JSON: {e}")
