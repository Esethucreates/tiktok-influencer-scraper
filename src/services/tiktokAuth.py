import asyncio
from typing import Any, Optional
import zendriver as uc
from src.services.AbstractAuthentication import BaseAuth


class TikTokAuth(BaseAuth):
    """TikTok-specific authentication implementation"""

    #
    def __init__(self, accounts_file: str = "../fileExports/scraping_accounts.json"):
        super().__init__("tiktok", accounts_file)

    def get_platform_url(self) -> str:
        return "https://www.tiktok.com/login"

    async def verify_login_status(self, page: uc.Tab) -> bool:
        """
        Check if user is logged in using a hierarchical approach:
        1. First check URL to see if redirected from login page
        2. If URL is ambiguous, check for login buttons
        3. If no login buttons, check for other login page elements
        4. If all checks pass, user is logged in
        """
        try:
            # Wait a bit for page to fully load
            await asyncio.sleep(30)

            # Step 1: Check URL first to determine current page
            print("Step 1: Checking URL...")
            try:
                current_url = await page.evaluate("window.location.href")
                print(f"Current URL: {current_url}")

                if current_url == "https://www.tiktok.com/login":
                    print("Still on login page URL")
                    return False  # Still on login page = not logged in
                elif current_url == "https://www.tiktok.com/":
                    print("Redirected to main page - likely logged in, but verifying...")
                    # Continue to button/element checks to be sure
                else:
                    print(f"On different URL: {current_url} - proceeding with element checks...")

            except Exception as e:
                print(f"Error checking URL: {e} - proceeding with element checks...")

            print("Proceeding to step 2...")

            # Step 2: Check for login buttons
            login_button_selectors = [
                "button#header-login-button",
                "button#top-right-action-bar-login-button",
                "[data-e2e='top-login-button']",
            ]

            print("Step 2: Checking for login buttons...")
            for selector in login_button_selectors:
                try:
                    login_button = await page.select(selector)
                    if login_button:
                        print(f"Login button found with selector: {selector}")
                        return False  # Login button exists = not logged in
                except Exception:
                    continue

            print("No login button found, proceeding to step 3...")

            # Step 3: Check for other login page elements (data attributes and text)
            login_element_selectors = [
                "[data-e2e='channel-item']",  # Login methods still present
                "div#login-modal-title",
                "[data-e2e='login-desc']",
                "[data-e2e='bottom-sign-up']"
            ]

            print("Step 3: Checking for login page elements...")
            for selector in login_element_selectors:
                try:
                    login_element = await page.select(selector)
                    if login_element:
                        print(f"Login element found with selector: {selector}")
                        return False  # Login elements exist = not logged in
                except Exception:
                    continue

            # Check for specific login text
            try:
                login_text_element = await page.find(text="Use phone / email / username", best_match=True)
                if login_text_element:
                    print("Login text 'Use phone / email / username' found")
                    return False  # Login text exists = not logged in
            except Exception:
                pass

            print("All login checks passed - user appears to be logged in")
            return True

        except Exception as e:
            print(f"Error verifying login status: {e}")
            # In case of error, assume not logged in for safety
            return False

    async def perform_login(self, browser, page, username: str, password: str) -> bool:
        """Perform TikTok login process"""
        try:
            print(f"Attempting login for user: {username}")

            # Navigate to TikTok homepage
            await page.get(self.get_platform_url())

            print("Loading initial page for login")
            await asyncio.sleep(30)  # Wait for page to load

            # Select email login option
            if not await self._select_email_login(page):
                print("Failed to select email login option")
                return False

            # Enter credentials
            if not await self._enter_credentials(page, username, password):
                print("Failed to enter credentials")
                return False

            # Submit login form
            if not await self._submit_login(page):
                print("Failed to submit login form")
                return False

            # Wait for login to complete and verify
            await asyncio.sleep(10)

            # Verify login was successful
            if await self.verify_login_status(page):
                print("Login successful!")
                return True
            else:
                print("Login failed - still showing login button")
                return False

        except Exception as e:
            print(f"Error during login process: {e}")
            return False

    @staticmethod
    async def _select_email_login(page) -> bool:
        """Select email login option"""
        try:
            # Wait for login modal to appear
            await asyncio.sleep(5)

            # Look for email login options
            email_options = await page.select_all("[role='link']")
            if email_options and len(email_options) > 1:
                await email_options[1].click()  # Usually the email option
                await asyncio.sleep(3)

                # Click "Log in with email or username"
                try:
                    email_login_button = await page.find("Log in with email or username", best_match=True)
                    if email_login_button:
                        await email_login_button.click()
                        await asyncio.sleep(3)
                        return True
                except:
                    pass

            return False

        except Exception as e:
            print(f"Error selecting email login: {e}")
            return False

    async def _enter_credentials(self, page, username: str, password: str) -> bool:
        """Enter login credentials"""
        try:
            # Find email and password fields
            email_field = await page.select("input[type=text]")
            password_field = await page.select("input[type=password]")

            if not email_field or not password_field:
                print("Could not find email or password fields")
                return False

            print("Entering credentials...")

            # Type email
            await self._type_text_slowly(email_field, username)
            await asyncio.sleep(2)

            # Type password
            await self._type_text_slowly(password_field, password)
            await asyncio.sleep(2)

            return True

        except Exception as e:
            print(f"Error entering credentials: {e}")
            return False

    @staticmethod
    async def _submit_login(page) -> bool:
        """Submit the login form"""
        try:
            login_button = await page.select("[data-e2e='login-button']")
            if login_button:
                await asyncio.sleep(5)  # Wait before clicking
                await login_button.click()
                return True
            else:
                print("Login submit button not found")
                return False

        except Exception as e:
            print(f"Error submitting login: {e}")
            return False

    @staticmethod
    async def _type_text_slowly(element, text: str) -> None:
        """Type text slowly to mimic human behavior"""
        for char in text:
            await element.send_keys(char)
            await asyncio.sleep(0.14)  # Small delay between characters
