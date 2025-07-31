import json
import logging
import pandas as pd
from pathlib import Path
from datetime import datetime
from typing import Dict, Any, List, Optional, Tuple
from collections import defaultdict, Counter
from pydantic import ValidationError, TypeAdapter, HttpUrl

from dataValidation import TikTokInfluencer, TikTokPost, TikTokComment, ReplyComment, PostHashtag


class TikTokDataCleaner:
    """
    Comprehensive TikTok data cleaning and normalization pipeline.
    Converts JSON data into structured CSV files for influencer analysis.
    """

    def __init__(self, json_path: str = "../fileExports/jsonFiles/unified_tiktok_scrape_results_20250730_223024.json",
                 output_dir: str = "../fileExports/cleaned_tiktok_data"):
        self.logger = None
        self.json_path = json_path
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(exist_ok=True)

        # Setup logging for validation errors
        self.setup_logging()

        # Data containers
        self.raw_data = None
        self.cleaned_influencers = []
        self.cleaned_posts = []
        self.cleaned_comments = []
        self.cleaned_reply_comments = []
        self.cleaned_post_hashtags = []
        self.hashtag_frequency = defaultdict(Counter)  # influencer_id -> {hashtag: count}

    def setup_logging(self):
        """Setup logging for validation errors and processing info"""
        log_file = self.output_dir / "cleaning_errors.log"
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(log_file),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)

    def load_json(self) -> Dict[str, Any]:
        """Load and return JSON data"""
        try:
            with open(self.json_path, 'r', encoding='utf-8-sig') as file:
                self.raw_data = json.load(file)
            return self.raw_data
        except Exception as e:
            self.logger.error(f"Failed to load JSON file: {e}")
            raise

    @staticmethod
    def _convert_string_to_url(url: str) -> Optional[HttpUrl]:
        """Convert string to HttpUrl with error handling"""
        if not url or not isinstance(url, str):
            return None
        try:
            return TypeAdapter(HttpUrl).validate_strings(url)
        except Exception:
            return None

    @staticmethod
    def _convert_timestamp(timestamp: int) -> datetime:
        """Convert Unix timestamp to datetime"""
        try:
            return datetime.fromtimestamp(timestamp)
        except (ValueError, TypeError):
            return datetime.now()

    @staticmethod
    def _safe_get(data: dict, keys: str, default=None):
        """Safely get nested dictionary values"""
        try:
            keys_list = keys.split('.')
            result = data
            for key in keys_list:
                result = result[key]
            return result if result is not None else default
        except (KeyError, TypeError):
            return default

    def _flatten_profile_data(self) -> List[TikTokInfluencer]:
        """Extract and validate influencer profile data"""
        influencers = []
        profile_list = self.raw_data.get('profiles', [])

        for i, profile in enumerate(profile_list):
            try:
                author_data = profile.get('raw_author_data', {})

                # Extract hashtags found under
                hashtags = profile.get('found_under_hashtags', [])

                influencer_data = {
                    'tiktok_id': profile.get('user_id', ''),
                    'username': profile.get('username', ''),
                    'sec_uid': author_data.get('secUid', ''),
                    'display_name': profile.get('display_name', ''),
                    'profile_url': self._convert_string_to_url(profile.get('profile_url')),

                    'bio_text': author_data.get('signature', ''),
                    'is_verified': author_data.get('verified', False),
                    'is_private': author_data.get('privateAccount', False),

                    'avatar_thumb': self._convert_string_to_url(author_data.get('avatarThumb')),
                    'avatar_medium': self._convert_string_to_url(author_data.get('avatarMedium')),
                    'avatar_large': self._convert_string_to_url(author_data.get('avatarLarger')),

                    'follower_count': profile.get('follower_count', 0),
                    'following_count': profile.get('following_count', 0),
                    'total_likes': profile.get('heart_count', 0),
                    'video_count': profile.get('video_count', 0),

                    'comment_setting': author_data.get('commentSetting'),
                    'duet_setting': author_data.get('duetSetting'),
                    'stitch_setting': author_data.get('stitchSetting'),
                    'download_setting': author_data.get('downloadSetting'),

                    'hashtags': hashtags
                }

                influencer = TikTokInfluencer(**influencer_data)
                influencers.append(influencer)

                # Track hashtags for frequency analysis
                for hashtag in hashtags:
                    clean_hashtag = hashtag.replace('#', '').lower()
                    self.hashtag_frequency[influencer_data['tiktok_id']][clean_hashtag] += 1

            except ValidationError as e:
                self.logger.error(f"Validation error for profile {i}: {e}")
                continue
            except Exception as e:
                self.logger.error(f"Unexpected error processing profile {i}: {e}")
                continue

        return influencers

    def _flatten_posts_data(self) -> Tuple[List[TikTokPost], List[PostHashtag]]:
        """Extract and validate post data with hashtags"""
        posts = []
        post_hashtags = []
        post_list = self.raw_data.get('posts', [])

        for i, post in enumerate(post_list):
            try:
                post_data = post.get('raw_post_data', {})
                stats = post_data.get('stats', {}) or post_data.get('statsV2', {})

                # Handle different stats formats
                view_count = self._safe_get(stats, 'playCount', 0)
                like_count = self._safe_get(stats, 'diggCount', 0)
                comment_count = self._safe_get(stats, 'commentCount', 0)
                share_count = self._safe_get(stats, 'shareCount', 0)
                collect_count = self._safe_get(stats, 'collectCount', 0)

                # Convert string numbers to integers if needed
                if isinstance(view_count, str):
                    view_count = int(view_count.replace(',', '')) if view_count.replace(',', '').isdigit() else 0
                if isinstance(like_count, str):
                    like_count = int(like_count.replace(',', '')) if like_count.replace(',', '').isdigit() else 0
                if isinstance(comment_count, str):
                    comment_count = int(comment_count.replace(',', '')) if comment_count.replace(',',
                                                                                                 '').isdigit() else 0
                if isinstance(share_count, str):
                    share_count = int(share_count.replace(',', '')) if share_count.replace(',', '').isdigit() else 0
                if isinstance(collect_count, str):
                    collect_count = int(collect_count.replace(',', '')) if collect_count.replace(',',
                                                                                                 '').isdigit() else 0

                post_obj_data = {
                    'post_id': post.get('post_id', ''),
                    'influencer_tiktok_id': post.get('user_id', ''),
                    'description': post_data.get('desc', ''),
                    'duration': self._safe_get(post_data, 'video.duration', 30),

                    'view_count': view_count,
                    'like_count': like_count,
                    'comment_count': comment_count,
                    'share_count': share_count,
                    'collect_count': collect_count,

                    'is_ad': post_data.get('isAd', False),
                    'is_pinned': post_data.get('isPinnedItem', False),

                    'posted_at': self._convert_timestamp(post_data.get('createTime', 0))
                }

                post_obj = TikTokPost(**post_obj_data)
                posts.append(post_obj)

                # Extract hashtags from post
                text_extra = post_data.get('textExtra', [])
                influencer_id = post.get('user_id', '')

                for hashtag_data in text_extra:
                    if hashtag_data.get('type') == 1:  # Type 1 is hashtag
                        hashtag_name = hashtag_data.get('hashtagName', '').lower()
                        if hashtag_name:
                            # Create PostHashtag record
                            post_hashtag = PostHashtag(
                                post_id=post.get('post_id', ''),
                                hashtag=hashtag_name
                            )
                            post_hashtags.append(post_hashtag)

                            # Update frequency tracking
                            self.hashtag_frequency[influencer_id][hashtag_name] += 1

            except ValidationError as e:
                self.logger.error(f"Validation error for post {i}: {e}")
                continue
            except Exception as e:
                self.logger.error(f"Unexpected error processing post {i}: {e}")
                continue

        return posts, post_hashtags

    def _flatten_comments_data(self) -> Tuple[List[TikTokComment], List[ReplyComment]]:
        """Extract and validate comment and reply data"""
        comments = []
        reply_comments = []
        comment_list = self.raw_data.get('comments', [])

        for i, comment in enumerate(comment_list):
            try:
                comment_data = comment.get('raw_comment_data', {})
                share_info = comment_data.get('share_info', {})

                comment_obj_data = {
                    'comment_id': comment.get('comment_id', ''),
                    'post_id': comment.get('post_id', ''),
                    'influencer_tiktok_id': comment.get('user_id', ''),
                    'create_time': self._convert_timestamp(comment_data.get('create_time', 0)),
                    'comment_text': comment_data.get('text', ''),
                    'digg_count': comment_data.get('digg_count', 0),
                    'reply_comment_total': comment_data.get('reply_comment_total', 0),
                    'share_title': share_info.get('title', ''),
                    'share_desc': share_info.get('desc', '')
                }

                comment_obj = TikTokComment(**comment_obj_data)
                comments.append(comment_obj)

                # Process reply comments
                reply_list = comment_data.get('reply_comment')

                # Handle different reply_comment structures
                if reply_list is not None:
                    # If reply_list is a single dict (one reply), convert to list
                    if isinstance(reply_list, dict):
                        reply_list = [reply_list]

                    # Process all replies in the list
                    for reply in reply_list:
                        try:
                            reply_user = reply.get('user', {})
                            reply_obj_data = {
                                'main_comment_id': comment.get('comment_id', ''),
                                'reply_comment_id': reply.get('cid', ''),
                                'replier_id': reply_user.get('uid', ''),
                                'reply_text': reply.get('text', ''),
                                'digg_count': reply.get('digg_count', 0),
                                'label_text': reply.get('label_text')
                            }

                            reply_obj = ReplyComment(**reply_obj_data)
                            reply_comments.append(reply_obj)

                        except ValidationError as e:
                            self.logger.error(f"Validation error for reply in comment {i}: {e}")
                            continue

            except ValidationError as e:
                self.logger.error(f"Validation error for comment {i}: {e}")
                continue
            except Exception as e:
                self.logger.error(f"Unexpected error processing comment {i}: {e}")
                continue

        return comments, reply_comments

    def _create_hashtag_frequency_data(self) -> List[Dict]:
        """Create hashtag frequency lookup table"""
        hashtag_freq_data = []

        for influencer_id, hashtag_counts in self.hashtag_frequency.items():
            for hashtag, count in hashtag_counts.items():
                hashtag_freq_data.append({
                    'influencer_tiktok_id': influencer_id,
                    'hashtag': hashtag,
                    'frequency_count': count
                })

        return hashtag_freq_data

    def process_all_data(self):
        """Main processing pipeline - clean and validate all data"""
        self.logger.info("Starting TikTok data cleaning process...")

        # Load raw data
        self.load_json()

        # Process each data type
        self.logger.info("Processing influencer profiles...")
        self.cleaned_influencers = self._flatten_profile_data()

        self.logger.info("Processing posts and hashtags...")
        self.cleaned_posts, self.cleaned_post_hashtags = self._flatten_posts_data()

        self.logger.info("Processing comments and replies...")
        self.cleaned_comments, self.cleaned_reply_comments = self._flatten_comments_data()

        self.logger.info(f"Processed: {len(self.cleaned_influencers)} influencers, "
                         f"{len(self.cleaned_posts)} posts, {len(self.cleaned_comments)} comments")

    def save_to_csv(self):
        """Save all cleaned data to separate CSV files with proper column ordering and timestamp"""

        # Generate timestamp for unique file naming
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

        # Define column orders for consistent output
        influencer_columns = [
            'tiktok_id', 'username', 'sec_uid', 'display_name', 'bio_text',
            'is_verified', 'is_private', 'profile_url',
            'avatar_thumb', 'avatar_medium', 'avatar_large',
            'follower_count', 'following_count', 'total_likes', 'video_count',
            'comment_setting', 'duet_setting', 'stitch_setting', 'download_setting',
            'created_at', 'updated_at'
        ]

        post_columns = [
            'post_id', 'influencer_tiktok_id', 'description', 'duration',
            'view_count', 'like_count', 'comment_count', 'share_count', 'collect_count',
            'is_ad', 'is_pinned', 'posted_at', 'created_at'
        ]

        comment_columns = [
            'comment_id', 'post_id', 'influencer_tiktok_id', 'create_time',
            'comment_text', 'digg_count', 'reply_comment_total',
            'share_title', 'share_desc'
        ]

        reply_columns = [
            'main_comment_id', 'reply_comment_id', 'replier_id',
            'reply_text', 'digg_count', 'label_text'
        ]

        post_hashtag_columns = ['post_id', 'hashtag']

        hashtag_freq_columns = ['influencer_tiktok_id', 'hashtag', 'frequency_count']

        # Save influencers
        if self.cleaned_influencers:
            influencer_df = pd.DataFrame([inf.model_dump() for inf in self.cleaned_influencers])
            # Remove hashtags column as it's not in the model's final output
            influencer_df = influencer_df.reindex(columns=influencer_columns, fill_value=None)
            filename = f"influencers_{timestamp}.csv"
            influencer_df.to_csv(self.output_dir / filename, index=False)
            self.logger.info(f"Saved {len(influencer_df)} influencers to {filename}")

        # Save posts
        if self.cleaned_posts:
            posts_df = pd.DataFrame([post.model_dump() for post in self.cleaned_posts])
            posts_df = posts_df.reindex(columns=post_columns, fill_value=None)
            filename = f"posts_{timestamp}.csv"
            posts_df.to_csv(self.output_dir / filename, index=False)
            self.logger.info(f"Saved {len(posts_df)} posts to {filename}")

        # Save comments
        if self.cleaned_comments:
            comments_df = pd.DataFrame([comment.model_dump() for comment in self.cleaned_comments])
            comments_df = comments_df.reindex(columns=comment_columns, fill_value=None)
            filename = f"comments_{timestamp}.csv"
            comments_df.to_csv(self.output_dir / filename, index=False)
            self.logger.info(f"Saved {len(comments_df)} comments to {filename}")

        # Save reply comments
        if self.cleaned_reply_comments:
            replies_df = pd.DataFrame([reply.model_dump() for reply in self.cleaned_reply_comments])
            replies_df = replies_df.reindex(columns=reply_columns, fill_value=None)
            filename = f"reply_comments_{timestamp}.csv"
            replies_df.to_csv(self.output_dir / filename, index=False)
            self.logger.info(f"Saved {len(replies_df)} reply comments to {filename}")

        # Save post hashtags
        if self.cleaned_post_hashtags:
            post_hashtags_df = pd.DataFrame([ph.model_dump() for ph in self.cleaned_post_hashtags])
            post_hashtags_df = post_hashtags_df.reindex(columns=post_hashtag_columns, fill_value=None)
            filename = f"post_hashtags_{timestamp}.csv"
            post_hashtags_df.to_csv(self.output_dir / filename, index=False)
            self.logger.info(f"Saved {len(post_hashtags_df)} post hashtags to {filename}")

        # Save hashtag frequency lookup table
        hashtag_freq_data = self._create_hashtag_frequency_data()
        if hashtag_freq_data:
            hashtag_freq_df = pd.DataFrame(hashtag_freq_data)
            hashtag_freq_df = hashtag_freq_df.reindex(columns=hashtag_freq_columns, fill_value=None)
            filename = f"hashtag_frequency_{timestamp}.csv"
            hashtag_freq_df.to_csv(self.output_dir / filename, index=False)
            self.logger.info(f"Saved {len(hashtag_freq_df)} hashtag frequency records to {filename}")

    def run_complete_pipeline(self):
        """Execute the complete data cleaning pipeline"""
        try:
            self.process_all_data()
            self.save_to_csv()
            self.logger.info("TikTok data cleaning pipeline completed successfully!")

            # Print summary
            print(f"\n=== Data Cleaning Summary ===")
            print(f"✅ Influencers processed: {len(self.cleaned_influencers)}")
            print(f"✅ Posts processed: {len(self.cleaned_posts)}")
            print(f"✅ Comments processed: {len(self.cleaned_comments)}")
            print(f"✅ Reply comments processed: {len(self.cleaned_reply_comments)}")
            print(f"✅ Post hashtags processed: {len(self.cleaned_post_hashtags)}")
            print(
                f"✅ Unique hashtag-influencer combinations: {sum(len(counts) for counts in self.hashtag_frequency.values())}")
            print(f"📁 Output directory: {self.output_dir}")
            print(f"📋 Check cleaning_errors.log for any validation issues")

        except Exception as e:
            self.logger.error(f"Pipeline failed: {e}")
            raise


# Usage Example
if __name__ == "__main__":
    # Initialize cleaner
    cleaner = TikTokDataCleaner()

    # Run complete pipeline
    cleaner.run_complete_pipeline()
