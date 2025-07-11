import base64
import json
import re

# Local Imports
from browser_config import OptimizedNoDriver
from src.services.tiktokAuth import *
from src.utils.exceptions import *


class CDPXHRMonitor:
    """
    Enhanced CDPXHRMonitor with modular authentication system.
    """

    def __init__(self, target_url: str, regex_pattern: str, scroll_count: int = 5,
                 scroll_pause: int = 2, timeout: int = 10, accounts_file: str = "../misc/scraping_accounts.json"):
        """
        Initialize the monitor with authentication capabilities.

        Args:
            target_url (str): The website URL to visit and monitor.
            regex_pattern (str): Regular expression pattern to filter XHR request URLs.
            scroll_count (int, optional): Number of times to scroll the page. Default is 5.
            scroll_pause (int, optional): Delay between each scroll in seconds. Default is 2.
            timeout (int, optional): Maximum time to wait for operations (in seconds). Default is 10.
            accounts_file (str, optional): Path to accounts JSON file. Default is "misc/scraping_accounts.json".
        """
        self.target_url = target_url
        self.regex_pattern = regex_pattern
        self.scroll_count = scroll_count
        self.scroll_pause = scroll_pause
        self.timeout = timeout
        self.accounts_file = accounts_file

        self.browser = None
        self.page = None
        self.auth = None
        self.is_authenticated = False

        self.matched_responses = []
        self.is_running = True

        # Initialize authentication based on target URL
        self._initialize_auth()

    def _initialize_auth(self):
        """Initialize the appropriate authentication handler based on target URL"""
        if "tiktok.com" in self.target_url.lower():
            self.auth = TikTokAuth(self.accounts_file)
        else:
            raise ValueError(f"Unsupported platform for URL: {self.target_url}")

    async def start_browser(self):
        """
        Start browser and ensure user is authenticated before proceeding.
        Raises AuthenticationError if no valid session can be established.
        """
        print("Starting browser...")
        self.browser = await OptimizedNoDriver.create_optimized_browser()
        self.page = await self.browser.get("about:blank")

        # Attempt to establish authenticated session
        await self._ensure_authenticated_session()

        if not self.is_authenticated:
            await self.stop_browser()
            raise AuthenticationError("Failed to establish authenticated session")

        # Enable network monitoring
        await self.page.send(uc.cdp.network.enable())
        await self.page.send(uc.cdp.network.set_cache_disabled(cache_disabled=True))

        print("Browser started with authenticated session")

    async def _ensure_authenticated_session(self):
        """Ensure we have a valid authenticated session"""
        try:
            # Try to load existing cookies first
            if await self.auth.load_cookies(self.browser):
                print("Existing cookies loaded, validating session...")
                if await self.auth.validate_session(self.browser, self.page):
                    print("Session validation successful")
                    self.is_authenticated = True
                    return
                else:
                    print("Session validation failed, need to login")

            # If no valid session, attempt login with available accounts
            await self._login_with_account_rotation()

        except Exception as e:
            print(f"Error ensuring authenticated session: {e}")
            self.is_authenticated = False

    async def _login_with_account_rotation(self):
        """Attempt login with account rotation and cooldown management"""
        working_accounts = self.auth.get_working_accounts()

        if not working_accounts:
            print("No working accounts available")
            self.is_authenticated = False
            return

        for account in working_accounts:
            username = account.get('username')
            password = account.get('password')

            if not username or not password:
                print(f"Invalid account configuration: {account}")
                continue

            print(f"Attempting login with account: {username}")

            # Create a new tab for login
            login_page = await self.browser.get("about:blank")

            try:
                # Attempt login
                login_success = await self.auth.perform_login(self.browser, login_page, username, password)

                if login_success:
                    print(f"Login successful for {username}")

                    # Save cookies
                    await self.auth.save_cookies(self.browser)

                    # Mark account as working
                    self.auth.mark_account_working(username)

                    # Close login tab
                    await login_page.close()

                    # Create new page for main session
                    self.page = await self.browser.get("about:blank")

                    # Load cookies on main page
                    await self.auth.load_cookies(self.browser)

                    self.is_authenticated = True
                    return
                else:
                    print(f"Login failed for {username}")
                    # Mark account as failed
                    self.auth.mark_account_failed(username)

                    # Close login tab
                    await login_page.close()

                    # Wait before trying next account (cooldown)
                    await asyncio.sleep(5)

            except Exception as e:
                print(f"Error during login attempt for {username}: {e}")
                self.auth.mark_account_failed(username)
                await login_page.close()
                continue

        print("All login attempts failed")
        self.is_authenticated = False

    async def stop_browser(self):
        """Stop the browser cleanly and safely."""
        self.is_running = False
        if self.browser:
            try:
                await asyncio.sleep(1)
                await self.browser.stop()
            except Exception as e:
                print(f"Error stopping browser: {e}")

    async def safe_get_response_body(self, request_id: str, url: str) -> str | None:
        """Safely fetch response body for a network request."""
        if not self.is_running:
            return None

        try:
            response = await asyncio.wait_for(
                self.page.send(uc.cdp.network.get_response_body(request_id=request_id)),
                timeout=2
            )

            if not self.is_running:
                return None

            body, is_base64 = response
            decoded_body = (
                base64.b64decode(body).decode("utf-8") if is_base64 else body
            )
            return decoded_body

        except asyncio.TimeoutError:
            print(f"Timeout getting response body for: {url}")
            return None
        except Exception as e:
            print(f"Error getting response body for {url}: {e}")
            return None

    async def on_response_received(self, event):
        """Event handler for network responses."""
        if not self.is_running:
            return

        url = event.response.url
        request_id = event.request_id

        if re.search(self.regex_pattern, url, re.IGNORECASE):
            print(f"Matched URL: {url}")
            asyncio.create_task(self.process_matched_response(event))

    async def process_matched_response(self, event):
        """Process a matched response event."""
        if not self.is_running:
            return

        url = event.response.url
        request_id = event.request_id

        await asyncio.sleep(0.5)

        if not self.is_running:
            return

        body_content = await self.safe_get_response_body(request_id, url)

        if body_content is None:
            print(f"Could not retrieve body for: {url}")
            return

        try:
            body_data = json.loads(body_content)
            final_body = body_data[0] if isinstance(body_data, list) and body_data else body_data
        except json.JSONDecodeError:
            final_body = body_content

        response_data = {
            "url": url,
            "status": event.response.status,
            "headers": dict(event.response.headers) if hasattr(event.response, 'headers') else {},
            "body": final_body,
            "timestamp": asyncio.get_event_loop().time()
        }

        self.matched_responses.append(response_data)
        print(f"Successfully captured response from: {url}")
        print(f"Status: {event.response.status}")
        print(f"Body type: {type(final_body)}")

    async def perform_scrolling(self):
        """Scroll through the page to trigger more requests."""
        try:
            for i in range(self.scroll_count):
                if not self.is_running:
                    break

                print(f"Scroll {i + 1}/{self.scroll_count}")

                await self.page.evaluate("""
                    window.scrollBy(0, window.innerHeight * 0.8);
                """)
                await asyncio.sleep(self.scroll_pause)

                try:
                    if hasattr(OptimizedNoDriver, 'human_like_scroll_sequence'):
                        await OptimizedNoDriver.human_like_scroll_sequence(
                            self.page,
                            scroll_count=1,
                            base_pause=self.scroll_pause
                        )
                except Exception as scroll_error:
                    print(f"OptimizedNoDriver scroll failed: {scroll_error}")
                    await self.page.evaluate("window.scrollBy(0, 500);")

                await asyncio.sleep(1)

        except Exception as e:
            print(f"Error during scrolling: {e}")

    async def run(self) -> list[Any] | None:
        """
        Main execution method that captures XHR responses.

        Returns:
            list[Any] | None: Captured response data or None if failed.

        Raises:
            AuthenticationError: If no authenticated session can be established.
        """
        try:
            # Start browser and ensure authentication
            await self.start_browser()

            if not self.is_authenticated:
                raise AuthenticationError("No authenticated session available")

            print(f"Browser started successfully with authenticated session")

            # Set up response monitoring
            self.page.add_handler(uc.cdp.network.ResponseReceived, self.on_response_received)

            print(f"Navigating to: {self.target_url}")
            await self.page.get(self.target_url)

            print("Waiting for initial page load...")
            await asyncio.sleep(8)

            print(f"Responses after initial load: {len(self.matched_responses)}")
            print("Starting scrolling sequence...")
            await self.perform_scrolling()

            print("Final wait for remaining responses...")
            await asyncio.sleep(3)

        except Exception as e:
            print(f"Error during execution: {e}")
            import traceback
            traceback.print_exc()
            raise
        finally:
            print("Cleaning up...")
            await self.stop_browser()

        print(f"Total responses captured: {len(self.matched_responses)}")
        return self.matched_responses


async def main():
    TARGET_PAGE = "https://www.tiktok.com/@kabelo_manyiki"
    REGEX_PATTERN = r"https:\/\/www\.tiktok\.com\/api\/post\/item_list\/\?[^ ]+"

    takealot_url: str = "https://www.takealot.com/all?custom=fd-gifting&sort=Rating%20Descending"
    takealot_regex: str = r"https://api\.takealot\.com/rest/v-1-14-0/searches/products,filters,facets,sort_options,breadcrumbs,slots_audience,context,seo,layout\?"

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
            print("This could mean:")
            print("1. The regex pattern doesn't match the actual API calls")
            print("2. The site uses different API endpoints")
            print("3. The requests are made differently (not XHR)")
        else:
            for i, item in enumerate(results, 1):
                print(f"\nResponse {i}:")
                print(f"URL: {item['url']}")
                print(f"Status: {item['status']}")
                print(f"Body type: {type(item['body'])}")
                OptimizedNoDriver.save_json_to_file(item['body'])
                if isinstance(item['body'], dict):
                    print(f"Body keys: {list(item['body'].keys())}")
                elif isinstance(item['body'], str):
                    print(f"Body length: {len(item['body'])} characters")

    except Exception as e:
        print(f"Error in main: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    asyncio.run(main())
