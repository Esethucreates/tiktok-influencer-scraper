import base64
import json
import re
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Dict, List

from src.scrapers.core_parts.browserConfig import OptimizedNoDriver
# Local Imports

from src.services.tiktokAuth import *
from src.utils.exceptions import *


class RequestState(Enum):
    INITIATED = "initiated"
    RESPONSE_RECEIVED = "response_received"
    LOADING_FINISHED = "loading_finished"
    FAILED = "failed"
    BODY_RETRIEVED = "body_retrieved"


@dataclass
class RequestInfo:
    request_id: str
    url: str
    state: RequestState = RequestState.INITIATED
    response_data: Optional[dict] = None
    response_body: Optional[str] = None
    content_length: Optional[int] = None
    encoding_type: Optional[str] = None
    timestamp: float = field(default_factory=time.time)
    retry_count: int = 0
    data_chunks: List[str] = field(default_factory=list)
    is_large_response: bool = False


class CDPXHRMonitor:
    """
    Enhanced CDPXHRMonitor with robust response body handling for large payloads.
    """

    def __init__(self, target_url: str, regex_pattern: str, scroll_count: int = 5,
                 scroll_pause: int = 2, timeout: int = 10,
                 accounts_file: str = "../fileExports/scraping_accounts.json"):
        """
        Initialize the monitor with enhanced authentication and response handling capabilities.
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

        # Enhanced tracking systems
        self.tracked_requests: Dict[str, RequestInfo] = {}
        self.matched_responses = []
        self.is_running = True

        # Configuration for large response handling
        self.large_response_threshold = 1024 * 1024  # 1MB
        self.max_retry_attempts = 3
        self.base_retry_delay = 0.5
        self.body_retrieval_delay = 1.0  # Delay before attempting body retrieval

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
        """
        print("Starting browser...")
        self.browser = await OptimizedNoDriver.create_optimized_browser()
        self.page = await self.browser.get("about:blank")

        # Attempt to establish authenticated session
        await self._ensure_authenticated_session()

        if not self.is_authenticated:
            await self.stop_browser()
            raise AuthenticationError("Failed to establish authenticated session")

        # Enable comprehensive network monitoring
        await self.page.send(uc.cdp.network.enable())
        await self.page.send(uc.cdp.network.set_cache_disabled(cache_disabled=True))

        # Set up enhanced event handlers
        self.page.add_handler(uc.cdp.network.RequestWillBeSent, self.on_request_will_be_sent)
        self.page.add_handler(uc.cdp.network.ResponseReceived, self.on_response_received)
        self.page.add_handler(uc.cdp.network.LoadingFinished, self.on_loading_finished)
        self.page.add_handler(uc.cdp.network.LoadingFailed, self.on_loading_failed)
        self.page.add_handler(uc.cdp.network.DataReceived, self.on_data_received)

        print("Browser started with authenticated session and enhanced monitoring")

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

            # TODO: Break at the first sight of a working account
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
                    # TODO: The error is occurring here. It mistakenly throws an error even if account is working
                    # It cannot find the file containing accounts or checked for exception before task was complete
                    self.auth.mark_account_working(username)

                    # Close login tab
                    await login_page.close()

                    # Create new page for main session
                    self.page = await self.browser.get("about:blank")

                    # Load cookies on main page
                    await self.auth.load_cookies(self.browser)

                    self.is_authenticated = True
                    break
                # Supposed to break here
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

    async def on_request_will_be_sent(self, event):
        """Track new requests that match our pattern"""
        if not self.is_running:
            return

        url = event.request.url
        request_id = event.request_id

        if re.search(self.regex_pattern, url, re.IGNORECASE):
            print(f"Tracking request: {url}")
            self.tracked_requests[request_id] = RequestInfo(
                request_id=request_id,
                url=url,
                state=RequestState.INITIATED
            )

    async def on_response_received(self, event):
        """Enhanced response handler with large payload detection"""
        if not self.is_running:
            return

        request_id = event.request_id
        url = event.response.url

        if request_id in self.tracked_requests:
            request_info = self.tracked_requests[request_id]
            request_info.state = RequestState.RESPONSE_RECEIVED
            request_info.response_data = {
                "status": event.response.status,
                "headers": dict(event.response.headers) if hasattr(event.response, 'headers') else {},
                "url": url
            }

            # Detect large responses
            content_length = request_info.response_data["headers"].get("content-length")
            if content_length:
                try:
                    request_info.content_length = int(content_length)
                    request_info.is_large_response = request_info.content_length > self.large_response_threshold
                    if request_info.is_large_response:
                        print(f"Large response detected: {request_info.content_length} bytes for {url}")
                except ValueError:
                    pass

            # Get encoding type
            content_encoding = request_info.response_data["headers"].get("content-encoding", "")
            request_info.encoding_type = content_encoding

            print(f"Response received for: {url} (Status: {event.response.status})")

    async def on_data_received(self, event):
        """Capture data chunks for large responses"""
        if not self.is_running:
            return

        request_id = event.request_id
        if request_id in self.tracked_requests:
            request_info = self.tracked_requests[request_id]

            # For large responses, we'll try to get the full body later
            # This event helps us know data is still being received
            if request_info.is_large_response:
                print(f"Data chunk received for large response: {event.data_length} bytes")

    async def on_loading_finished(self, event):
        """Handle loading finished - optimal time to get response body"""
        if not self.is_running:
            return

        request_id = event.request_id
        if request_id in self.tracked_requests:
            request_info = self.tracked_requests[request_id]
            request_info.state = RequestState.LOADING_FINISHED

            print(f"Loading finished for: {request_info.url}")

            # Schedule body retrieval with delay for large responses
            delay = self.body_retrieval_delay if request_info.is_large_response else 0.2
            asyncio.create_task(self.retrieve_response_body_with_retry(request_info, delay))

    async def on_loading_failed(self, event):
        """Handle loading failures"""
        if not self.is_running:
            return

        request_id = event.request_id
        if request_id in self.tracked_requests:
            request_info = self.tracked_requests[request_id]
            request_info.state = RequestState.FAILED
            print(f"Loading failed for: {request_info.url}")

    async def retrieve_response_body_with_retry(self, request_info: RequestInfo, initial_delay: float = 0):
        """Enhanced response body retrieval with intelligent retry logic"""
        if not self.is_running:
            return

        # Initial delay before first attempt
        if initial_delay > 0:
            await asyncio.sleep(initial_delay)

        for attempt in range(self.max_retry_attempts + 1):
            if not self.is_running:
                return

            try:
                print(
                    f"Attempting to get response body (attempt {attempt + 1}/{self.max_retry_attempts + 1}) for: {request_info.url}")

                # Calculate timeout based on response size and attempt
                timeout = self.timeout
                if request_info.is_large_response:
                    timeout = min(timeout * 2, 30)  # Increase timeout for large responses

                response = await asyncio.wait_for(
                    self.page.send(uc.cdp.network.get_response_body(request_id=request_info.request_id)),
                    timeout=timeout
                )

                body, is_base64 = response
                decoded_body = (
                    base64.b64decode(body).decode("utf-8") if is_base64 else body
                )

                # Parse JSON if possible
                try:
                    body_data = json.loads(decoded_body)
                    final_body = body_data[0] if isinstance(body_data, list) and body_data else body_data
                except json.JSONDecodeError:
                    final_body = decoded_body

                # Create final response data
                response_data = {
                    "url": request_info.url,
                    "status": request_info.response_data["status"],
                    "headers": request_info.response_data["headers"],
                    "body": final_body,
                    "timestamp": time.time(),
                    "content_length": request_info.content_length,
                    "is_large_response": request_info.is_large_response,
                    "attempts_needed": attempt + 1
                }

                self.matched_responses.append(response_data)
                request_info.state = RequestState.BODY_RETRIEVED
                request_info.response_body = decoded_body

                print(f"✅ Successfully retrieved response body for: {request_info.url}")
                print(f"   Size: {len(decoded_body)} characters")
                print(f"   Attempts needed: {attempt + 1}")
                print(f"   Body type: {type(final_body)}")

                return

            except asyncio.TimeoutError:
                print(f"⏱️  Timeout on attempt {attempt + 1} for: {request_info.url}")
                if attempt < self.max_retry_attempts:
                    # Exponential backoff with jitter
                    delay = self.base_retry_delay * (2 ** attempt) + (0.1 * attempt)
                    print(f"   Retrying in {delay:.1f} seconds...")
                    await asyncio.sleep(delay)
                    continue
                else:
                    print(f"❌ Final timeout for: {request_info.url}")
                    break

            except Exception as e:
                error_msg = str(e)
                print(f"❌ Error on attempt {attempt + 1} for: {request_info.url}")
                print(f"   Error: {error_msg}")

                # Check if it's the specific -32000 error
                if "-32000" in error_msg or "No resource with given identifier" in error_msg:
                    if attempt < self.max_retry_attempts:
                        # For -32000 errors, try different delay strategies
                        if request_info.is_large_response:
                            # For large responses, wait longer
                            delay = self.base_retry_delay * (3 ** attempt) + (0.2 * attempt)
                        else:
                            # For regular responses, shorter exponential backoff
                            delay = self.base_retry_delay * (2 ** attempt)

                        print(f"   Resource not found error - retrying in {delay:.1f} seconds...")
                        await asyncio.sleep(delay)
                        continue
                    else:
                        print(f"   Resource permanently unavailable after {self.max_retry_attempts + 1} attempts")
                        break
                else:
                    # For other errors, don't retry
                    print(f"   Non-retryable error: {error_msg}")
                    break

        # If we get here, all attempts failed
        print(
            f"🔴 Failed to retrieve response body for: {request_info.url} after {self.max_retry_attempts + 1} attempts")

        # Still add to results but mark as failed
        response_data = {
            "url": request_info.url,
            "status": request_info.response_data["status"] if request_info.response_data else "unknown",
            "headers": request_info.response_data["headers"] if request_info.response_data else {},
            "body": None,
            "error": "Failed to retrieve response body",
            "timestamp": time.time(),
            "content_length": request_info.content_length,
            "is_large_response": request_info.is_large_response,
            "attempts_needed": self.max_retry_attempts + 1
        }
        self.matched_responses.append(response_data)

    async def stop_browser(self):
        """Stop the browser cleanly and safely."""
        self.is_running = False
        if self.browser:
            try:
                await asyncio.sleep(1)
                await self.browser.stop()
            except Exception as e:
                print(f"Error stopping browser: {e}")

    async def perform_scrolling(self):
        """Enhanced scrolling with better timing for large response handling"""
        try:
            for i in range(self.scroll_count):
                if not self.is_running:
                    break

                print(f"Scroll {i + 1}/{self.scroll_count}")

                # Scroll down
                await self.page.evaluate("""
                    window.scrollBy(0, window.innerHeight * 0.8);
                """)

                # Wait for initial requests to start
                await asyncio.sleep(self.scroll_pause)

                # Additional wait for large responses
                if i == 0:  # First scroll often triggers large initial load
                    print("   Waiting extra time for initial large responses...")
                    await asyncio.sleep(3)

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

                # Wait for responses to be processed
                await asyncio.sleep(2)

                # Log current status
                pending_requests = sum(1 for req in self.tracked_requests.values()
                                       if req.state in [RequestState.INITIATED, RequestState.RESPONSE_RECEIVED,
                                                        RequestState.LOADING_FINISHED])
                print(f"   Responses captured so far: {len(self.matched_responses)}")
                print(f"   Pending requests: {pending_requests}")

        except Exception as e:
            print(f"Error during scrolling: {e}")

    async def run(self) -> List[Dict[str, Any]] | None:
        """
        Main execution method with enhanced error handling and monitoring.
        """
        try:
            # Start browser and ensure authentication
            await self.start_browser()

            if not self.is_authenticated:
                raise AuthenticationError("No authenticated session available")

            print(f"Browser started successfully with authenticated session")
            print(f"Large response threshold: {self.large_response_threshold / 1024 / 1024:.1f}MB")
            print(f"Max retry attempts: {self.max_retry_attempts}")

            print(f"Navigating to: {self.target_url}")
            await self.page.get(self.target_url)

            print("Waiting for initial page load...")
            await asyncio.sleep(8)

            print(f"Tracked requests after initial load: {len(self.tracked_requests)}")
            print(f"Responses captured after initial load: {len(self.matched_responses)}")

            print("Starting scrolling sequence...")
            await self.perform_scrolling()

            print("Final wait for remaining responses...")
            await asyncio.sleep(5)  # Longer wait for large responses

            # Wait for any pending body retrievals to complete
            pending_requests = [req for req in self.tracked_requests.values()
                                if req.state == RequestState.LOADING_FINISHED]
            if pending_requests:
                print(f"Waiting for {len(pending_requests)} pending body retrievals...")
                await asyncio.sleep(3)

            # Summary statistics
            total_requests = len(self.tracked_requests)
            successful_responses = len([r for r in self.matched_responses if r.get("body") is not None])
            failed_responses = len([r for r in self.matched_responses if r.get("body") is None])
            large_responses = len([r for r in self.matched_responses if r.get("is_large_response", False)])

            print(f"\n=== EXECUTION SUMMARY ===")
            print(f"Total tracked requests: {total_requests}")
            print(f"Successful responses: {successful_responses}")
            print(f"Failed responses: {failed_responses}")
            print(f"Large responses handled: {large_responses}")
            print(
                f"Success rate: {(successful_responses / total_requests * 100) if total_requests > 0 else 0:.1f}%")

            return self.matched_responses

        except Exception as e:
            print(f"Error during execution: {e}")
            import traceback
            traceback.print_exc()
            raise
        finally:
            print("Cleaning up...")
            await self.stop_browser()
