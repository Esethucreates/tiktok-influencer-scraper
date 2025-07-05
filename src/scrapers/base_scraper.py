import asyncio
import base64
import json
import re
from typing import Any
from browser_config import OptimizedNoDriver

import zendriver as uc


class CDPXHRMonitor:
    """
    CDPXHRMonitor is a reusable class used to capture and save specific
    background web requests (XHRs) from a webpage using the Chrome DevTools Protocol (CDP).

    It can detect certain requests based on a regular expression pattern,
    scroll through the page to trigger more requests, and safely retrieve
    and store the data sent in those requests.

    Example Use Case:
        - You can use this to scrape background API responses from a site like TikTok or Takealot
          without needing direct API access.

    Attributes:
        target_url (str): The URL of the website to monitor.
        regex_pattern (str): A regular expression used to match specific request URLs.
        scroll_count (int): Number of scrolls to perform on the page (to trigger more requests).
        scroll_pause (int): Seconds to wait between scrolls.
        timeout (int): Timeout value used for operations like fetching data.

        browser: Internal browser instance (set during runtime).
        page: Internal page/tab instance in the browser (set during runtime).
        matched_responses (list): Stores the captured responses that matched the pattern.
        is_running (bool): A flag to control whether the monitor is active.
    """

    def __init__(self, target_url: str, regex_pattern: str, scroll_count: int = 5,
                 scroll_pause: int = 2, timeout: int = 10):
        """
        Sets up a new instance of CDPXHRMonitor with user-defined settings.

        Args:
            target_url (str): The website URL to visit and monitor.
            regex_pattern (str): Regular expression pattern to filter XHR request URLs.
            scroll_count (int, optional): Number of times to scroll the page. Default is 5.
            scroll_pause (int, optional): Delay between each scroll in seconds. Default is 2.
            timeout (int, optional): Maximum time to wait for operations (in seconds). Default is 10.
        """
        self.target_url = target_url
        self.regex_pattern = regex_pattern
        self.scroll_count = scroll_count
        self.scroll_pause = scroll_pause
        self.timeout = timeout

        self.browser = None
        self.page = None

        self.matched_responses = []
        self.is_running = True
        self.cookies_file = "tiktok.session.dat"

    async def start_browser(self):
        """
        Starts the headless browser and opens a blank tab using a custom driver (OptimizedNoDriver).

        This sets up the internal browser and page that will be used to monitor network activity.
        """
        # First run the create_session function

        self.browser = await OptimizedNoDriver.create_optimized_browser()
        self.page = await self.browser.get("about:blank")

        # Load cookies found into browser, if not available, call the other create_session()
        await self.create_session(current_browser=self.browser, current_page=self.page)
        # If they login-button still available, or didn't work throw an exception. It must not continue to run

        await self.page.send(uc.cdp.network.enable())  # Enables network monitoring
        await self.page.send(uc.cdp.network.set_cache_disabled(cache_disabled=True))  # Disable caching

    async def stop_browser(self):
        """
        Stops the browser cleanly and safely.

        It ensures all pending tasks are wrapped up before shutting down the browser instance.
        """
        self.is_running = False
        if self.browser:
            try:
                await asyncio.sleep(1)  # Short delay to finish any ongoing actions
                await self.browser.stop()
            except Exception as e:
                print(f"Error stopping browser: {e}")

    async def safe_get_response_body(self, request_id: str, url: str) -> str | None:
        """
        Safely fetches the response body for a network request using the request ID.

        Args:
            request_id (str): The unique identifier for the XHR request.
            url (str): The full URL of the request.

        Returns:
            str | None: Returns the decoded response body (usually JSON or HTML), or None if failed.
        """
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
        """
        Event handler that is triggered when a network response is received.

        It checks if the request URL matches the regex pattern and processes it if matched.

        Args:
            event: The network response event object containing response details.
        """
        if not self.is_running:
            return

        url = event.response.url
        request_id = event.request_id

        if re.search(self.regex_pattern, url, re.IGNORECASE):
            print(f"Matched URL: {url}")
            asyncio.create_task(self.process_matched_response(event))  # Background task

    async def process_matched_response(self, event):
        """
        Processes a matched response event by fetching and decoding the body content.

        Extracts useful data such as the URL, status code, headers, and response body.

        Args:
            event: The network response event with URL and request_id.
        """
        if not self.is_running:
            return

        url = event.response.url
        request_id = event.request_id

        await asyncio.sleep(0.5)  # Give time for full response

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
        """
        Scrolls down the webpage multiple times to trigger background XHR requests.

        Uses both a basic scroll and an optional 'human-like' scroll if available.
        """
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

    async def create_session(self, current_browser, current_page, account_name: str = "peleh44149@ranpets.com",
                             account_password: str = "?!Gramm1001*=",
                             initial_load_page_time: int = 50,
                             cookie_file_name: str = "tiktok.session.dat",
                             ):
        """
        This function logs in, and returns cookies for a loaded account
        :return: A cookie for session
        """

        # It should open up main page, find log in button, login in with credentials.
        # Return list of cookies and upload them on next
        # TODO: Load the session cookies first. It must take in a browser, load the cookies for the browser and then return the browser.
        # TODO: If there are no cookies, create them first, extract, and then load them in the browser and repeat the process
        async def type_text(element, text):
            for char in text:
                await element.send_keys(char)

        async def login_with_credentials(page):
            print('finding the "create account" button')
            login_account = await page.select("button#header-login-button") if await page.select(
                "button#header-login-button") is None else await page.select("button#top-right-action-bar-login-button")

            print('"login account" => click')
            await login_account.click()

            await asyncio.sleep(2)

            print("finding the email input field")
            email = await page.select_all(" [data-e2e='channel-item']")
            await email[1].click()
            await asyncio.sleep(3)

            logging_in = await page.find("Log in with email or username", best_match=True)
            await logging_in.click()
            await asyncio.sleep(3)

            email_field = await page.select("input[type=text]")
            password_field = await page.select("input[type=password]")

            if not email_field or not password_field:
                return False

            print("Logging in with credentials...")
            await type_text(email_field, account_name)
            await type_text(password_field, account_password)

            login_button = await page.select("[data-e2e='login-button']")
            await asyncio.sleep(5)
            if login_button:
                await login_button.click()
                return True
            return False

        async def load_cookies(browser, page):
            try:
                await browser.cookies.load(cookie_file_name)
                await page.reload()
                print("Cookies loaded.")
                return True
            except (json.JSONDecodeError, ValueError) as e:
                print(f"Failed to load cookies: {e}")
            except FileNotFoundError:
                print("Cookie file does not exist.")
            return False

        async def save_cookies(browser):
            try:
                await browser.cookies.save(cookie_file_name)
                print("Cookies saved.")
            except Exception as e:
                print(f"Failed to save cookies: {e}")

        # ------------------------------------------------------------------------------------
        try:
            browser = await OptimizedNoDriver.create_optimized_browser()
            page = await browser.get('https://www.tiktok.com/')

            await asyncio.sleep(initial_load_page_time)

            if not await load_cookies(current_browser, current_page):
                if not await login_with_credentials(page):
                    print("Login failed.")
                    return False

                await save_cookies(browser)
                print("Logged in with credentials and cookies saved.")

                await page.close()
                return None

            else:
                print("Logged in with cookies.")
                await page.close()
                return None

            # browser.stop()
        except Exception as e:
            print(f"Error during login: {e}")
            raise Exception("Error during login")
        # TODO: Load cookies, take from https://stackoverflow.com/questions/78700829/how-so-save-a-session-with-nodriver-python

    async def run(self) -> list[Any] | None:
        """
        The main execution method that starts the browser, navigates to the URL,
        captures all matching XHR responses, and then stops the browser.

        Returns:
            list[Any] | None: A list of captured response data (URL, status, headers, body, etc.),
                              or None if nothing was found or an error occurred.
        """
        try:
            await self.start_browser()
            print(f"Browser started successfully")

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
