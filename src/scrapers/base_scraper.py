import asyncio
import base64
import json
import re
from typing import Any, Coroutine

import zendriver as uc
from browser_config import OptimizedNoDriver


class CDPXHRMonitor:
    """
    A reusable class to capture XHR responses using zendriver.
    Matches XHRs based on regex, fetches their bodies safely using Chrome CDP.
    """

    def __init__(self, target_url: str, regex_pattern: str, scroll_count: int = 5,
                 scroll_pause: int = 2, timeout: int = 10):
        self.target_url = target_url
        self.regex_pattern = regex_pattern
        self.scroll_count = scroll_count
        self.scroll_pause = scroll_pause
        self.timeout = timeout

        self.browser = None
        self.page = None

        # Internal trackers
        self.matched_responses = []
        self.is_running = True

    async def start_browser(self):
        """Initializes the zendriver browser and page"""
        self.browser = await OptimizedNoDriver.create_optimized_browser()
        self.page = await self.browser.get("about:blank")
        await self.page.send(uc.cdp.network.enable())
        await self.page.send(uc.cdp.network.set_cache_disabled(cache_disabled=True))

    async def stop_browser(self):
        """Stops the browser instance cleanly"""
        self.is_running = False
        if self.browser:
            try:
                # Give any pending operations a moment to complete
                await asyncio.sleep(1)
                await self.browser.stop()
            except Exception as e:
                print(f"Error stopping browser: {e}")

    async def safe_get_response_body(self, request_id: str, url: str):
        """Safely get response body with proper error handling"""
        if not self.is_running:
            return None

        try:
            # Use a shorter timeout and check if we're still running
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
        """Capture matching XHRs - simplified approach"""
        if not self.is_running:
            return

        url = event.response.url
        request_id = event.request_id

        # Check if this URL matches our pattern
        if re.search(self.regex_pattern, url, re.IGNORECASE):
            print(f"Matched URL: {url}")

            # Create a background task to get the response body
            # Don't await it directly to avoid blocking the event handler
            asyncio.create_task(self.process_matched_response(event))

    async def process_matched_response(self, event):
        """Process a matched response in the background"""
        if not self.is_running:
            return

        url = event.response.url
        request_id = event.request_id

        # Wait a bit for the response to be fully available
        await asyncio.sleep(0.5)

        if not self.is_running:
            return

        # Try to get the response body
        body_content = await self.safe_get_response_body(request_id, url)

        if body_content is None:
            print(f"Could not retrieve body for: {url}")
            return

        # Try to parse as JSON
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
        """Perform scrolling with better error handling"""
        try:
            for i in range(self.scroll_count):
                if not self.is_running:
                    break

                print(f"Scroll {i + 1}/{self.scroll_count}")

                # Simple scroll down
                await self.page.evaluate("""
                    window.scrollBy(0, window.innerHeight * 0.8);
                """)

                await asyncio.sleep(self.scroll_pause)

                # Alternative scrolling method if the above doesn't work
                # Try using the OptimizedNoDriver method if it exists
                try:
                    if hasattr(OptimizedNoDriver, 'human_like_scroll_sequence'):
                        await OptimizedNoDriver.human_like_scroll_sequence(
                            self.page,
                            scroll_count=1,
                            base_pause=self.scroll_pause
                        )
                except Exception as scroll_error:
                    print(f"OptimizedNoDriver scroll failed: {scroll_error}")
                    # Fallback to simple scroll
                    await self.page.evaluate("window.scrollBy(0, 500);")

                await asyncio.sleep(1)

        except Exception as e:
            print(f"Error during scrolling: {e}")

    async def run(self) -> list[Any] | None:
        """
        Main execution method with improved error handling.
        """
        try:
            await self.start_browser()
            print(f"Browser started successfully")

            # Add the response handler
            self.page.add_handler(uc.cdp.network.ResponseReceived, self.on_response_received)

            print(f"Navigating to: {self.target_url}")
            await self.page.get(self.target_url)

            # Wait for initial page load
            print("Waiting for initial page load...")
            await asyncio.sleep(8)

            # Check if we got any responses during initial load
            print(f"Responses after initial load: {len(self.matched_responses)}")

            # Perform scrolling
            print("Starting scrolling sequence...")
            await self.perform_scrolling()

            # Final wait for any remaining responses
            print("Final wait for remaining responses...")
            await asyncio.sleep(3)

        except Exception as e:
            print(f"Error during execution: {e}")
            import traceback
            traceback.print_exc()
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
