import asyncio
import json
import os
from abc import ABC, abstractmethod
from datetime import datetime, timedelta
from typing import Dict, List


class BaseAuth(ABC):
    """
    Abstract base class for platform-specific authentication.
    Provides common functionality for session management and account rotation.
    """

    def __init__(self, platform: str, accounts_file: str = "../misc/scraping_accounts.json"):
        self.platform = platform
        self.accounts_file = accounts_file
        self.session_file = f"{platform}.session.dat"
        self.cooldown_period = timedelta(minutes=10)

    @abstractmethod
    async def perform_login(self, browser, page, username: str, password: str) -> bool:
        """Platform-specific login implementation"""
        pass

    @abstractmethod
    async def verify_login_status(self, page) -> bool:
        """Platform-specific login verification"""
        pass

    def load_accounts(self) -> List[Dict]:
        """Load accounts from JSON file"""
        try:
            with open(self.accounts_file, 'r') as f:
                data = json.load(f)
                return data.get(self.platform, [])
        except FileNotFoundError:
            print(f"Accounts file not found: {self.accounts_file}")
            return []
        except json.JSONDecodeError as e:
            print(f"Error parsing accounts file: {e}")
            return []

    def save_accounts(self, accounts: List[Dict]) -> None:
        """Save accounts back to JSON file"""
        try:
            # Load existing data
            data = {}
            print("Finding if file exists")
            if os.path.exists(self.accounts_file):
                with open(self.accounts_file, 'r') as f:
                    data = json.load(f)

            # Update platform accounts
            data[self.platform] = accounts

            # Save back to file
            with open(self.accounts_file, 'w') as f:
                json.dump(data, f, indent=2)
        except Exception as e:
            print(f"Error saving accounts: {e}")

    def get_working_accounts(self) -> List[Dict]:
        """Get accounts that are currently working and not in cooldown"""
        accounts = self.load_accounts()
        working_accounts = []
        current_time = datetime.now()

        print("Working accounts:")
        for account in accounts:
            if account.get('isWorking', True):
                # Check if account is in cooldown
                last_failure = account.get('lastFailure')
                if last_failure:
                    failure_time = datetime.fromisoformat(last_failure)
                    if current_time - failure_time < self.cooldown_period:
                        continue
                working_accounts.append(account)

        return working_accounts

    def mark_account_failed(self, username: str) -> None:
        """Mark an account as failed and set cooldown"""
        accounts = self.load_accounts()
        print(f"Marking account {username} as failed")
        for account in accounts:
            if account.get('username') == username:
                account['isWorking'] = False
                account['lastFailure'] = datetime.now().isoformat()
                break
        self.save_accounts(accounts)

    def mark_account_working(self, username: str) -> None:
        """Mark an account as working"""
        accounts = self.load_accounts()
        print(f"Marking account {username} as working")
        for account in accounts:
            if account.get('username') == username:
                account['isWorking'] = True
                if 'lastFailure' in account:
                    del account['lastFailure']
                break
        self.save_accounts(accounts)

    async def load_cookies(self, browser) -> bool:
        """Load cookies from session file"""
        try:
            print("Loading cookies")
            if os.path.exists(self.session_file):
                await browser.cookies.load(self.session_file)
                print(f"Cookies loaded from {self.session_file}")
                return True
            else:
                print(f"Session file not found: {self.session_file}")
                return False
        except Exception as e:
            print(f"Failed to load cookies: {e}")
            return False

    async def save_cookies(self, browser) -> bool:
        """Save cookies to session file"""
        try:
            print("Saving cookies")
            await browser.cookies.save(self.session_file)
            print(f"Cookies saved to {self.session_file}")
            return True
        except Exception as e:
            print(f"Failed to save cookies: {e}")
            return False

    async def validate_session(self, browser, page) -> bool:
        """Validate if current session is working"""
        try:
            # Navigate to platform homepage
            await page.get(self.get_platform_url())
            await asyncio.sleep(5)  # Wait for page to load

            # Check if logged in
            is_logged_in = await self.verify_login_status(page)
            return is_logged_in
        except Exception as e:
            print(f"Session validation failed: {e}")
            return False

    @abstractmethod
    def get_platform_url(self) -> str:
        """Get the platform's main URL"""
        pass
