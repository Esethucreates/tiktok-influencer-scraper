import asyncio
from typing import Any, Optional

import zendriver as uc

from src.services.AbstractAuthentication import BaseAuth


class TikTokAuth(BaseAuth):
    """TikTok-specific authentication implementation"""

    #
    def __init__(self, accounts_file: str = "../misc/scraping_accounts.json"):
        super().__init__("tiktok", accounts_file)

    def get_platform_url(self) -> str:
        return "https://www.tiktok.com/"

    async def verify_login_status(self, page) -> bool:
        """Check if user is logged in by looking for login button"""
        try:
            # Wait a bit for page to fully load
            await asyncio.sleep(30)

            # Look for login buttons - if they exist, user is not logged in
            login_selectors = [
                "button#header-login-button",
                "button#top-right-action-bar-login-button",
                "[data-e2e='top-login-button']",
                "button:contains('Log in')",
                "a:contains('Log in')"
            ]

            for selector in login_selectors:
                try:
                    login_button = await page.select(selector)
                    if login_button:
                        print(f"Login button found with selector: {selector}")
                        return False  # Login button exists = not logged in
                except Exception:
                    continue

            print("No login button found - user appears to be logged in")
            return True  # No login button found = logged in

        except Exception as e:
            print(f"Error verifying login status: {e}")
            return False

    async def perform_login(self, browser, page, username: str, password: str) -> bool:
        """Perform TikTok login process"""
        try:
            print(f"Attempting login for user: {username}")

            # Navigate to TikTok homepage
            await page.get(self.get_platform_url())
            # TODO: Due to network constraints, logging in will require more time to load. Fix this!!
            print("Loading initial page for login")
            await asyncio.sleep(90)  # Wait for page to load

            # Find and click login button
            login_button = await self._find_login_button(page)
            if not login_button:
                print("Login button not found")
                return False

            print("Clicking login button...")
            await login_button.click()
            await asyncio.sleep(5)

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

    async def _find_login_button(self, page: uc.Tab) -> Optional[Any]:
        """Find the login button on TikTok homepage"""
        # selectors = [
        #     "button#header-login-button",
        #     "button#top-right-action-bar-login-button",
        #     "[data-e2e='top-login-button']"
        # ]
        print("Looking for the login button")
        btn = await page.find("Log in", best_match=True)

        if btn is None:
            raise Exception(f"Login button not found")
        return btn

        # for selector in selectors:
        #     try:
        #         button = await page.find(selector, "Log in", 15)
        #         if button:
        #             return button
        #     except:
        #         continue
        #
        # return None

    async def _select_email_login(self, page) -> bool:
        """Select email login option"""
        try:
            # Wait for login modal to appear
            await asyncio.sleep(3)

            # Look for email login options
            # FIXME: Instead, look for text containing email
            email_options = await page.select_all("[data-e2e='channel-item']")
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

    async def _submit_login(self, page) -> bool:
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

    async def _type_text_slowly(self, element, text: str) -> None:
        """Type text slowly to mimic human behavior"""
        for char in text:
            await element.send_keys(char)
            await asyncio.sleep(0.1)  # Small delay between characters
