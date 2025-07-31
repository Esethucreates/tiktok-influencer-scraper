from datetime import datetime
from typing import Optional, List
from pydantic import BaseModel, Field, HttpUrl, field_validator


# Core Influencer Profile Model
class TikTokInfluencer(BaseModel):
    """
    Core influencer profile data for discovery and analysis with validation
    """
    # Basic Profile Information
    tiktok_id: str = Field(..., min_length=1, description="TikTok internal user ID")
    username: str = Field(..., min_length=1, max_length=50, description="TikTok username")
    sec_uid: str = Field(..., description="TikTok security UID")
    display_name: str = Field(..., min_length=1, max_length=100, description="Display name")

    # Profile Details
    bio_text: Optional[str] = Field(None, description="Profile bio/signature")
    is_verified: bool = Field(default=False, description="Verification status")
    is_private: bool = Field(default=False, description="Account privacy setting")
    profile_url: Optional[HttpUrl] = Field(None, description="Profile URL")

    # Avatar/Profile Images
    avatar_thumb: Optional[HttpUrl] = Field(None, description="Profile picture thumbnail URL")
    avatar_medium: Optional[HttpUrl] = Field(None, description="Profile picture medium URL")
    avatar_large: Optional[HttpUrl] = Field(None, description="Profile picture large URL")

    # Follower Metrics (Critical for influencer qualification)
    follower_count: int = Field(..., ge=0, description="Number of followers")
    following_count: int = Field(..., ge=0, description="Number of accounts following")
    video_count: int = Field(..., ge=0, description="Total number of videos posted")
    total_likes: int = Field(..., ge=0, description="Total likes across all content")

    # Privacy & Content Settings (Important for brand alignment)
    comment_setting: Optional[int] = Field(None, ge=0, le=3, description="Comment permissions setting")
    duet_setting: Optional[int] = Field(None, ge=0, le=3, description="Duet permissions setting")
    stitch_setting: Optional[int] = Field(None, ge=0, le=3, description="Stitch permissions setting")
    download_setting: Optional[int] = Field(None, ge=0, le=3, description="Download permissions setting")

    # Timestamps
    created_at: datetime = Field(default_factory=datetime.now)
    updated_at: datetime = Field(default_factory=datetime.now)

    hashtags: Optional[List[str]] = Field(...)


# Individual Post/Content Analysis Model
class TikTokPost(BaseModel):
    """
    Individual post data for content analysis and engagement tracking with validation
    """
    # Post Identification
    post_id: str = Field(..., min_length=1, description="TikTok post ID")
    influencer_tiktok_id: str = Field(..., min_length=1, description="Associated influencer TikTok ID")

    # Content Details
    description: str = Field(..., max_length=2000, description="Post caption/description")
    duration: int = Field(..., ge=1, le=600, description="Video duration in seconds (max 10 min)")

    # Engagement Metrics (Critical for scoring)
    view_count: int = Field(..., ge=0, description="Number of views")
    like_count: int = Field(..., ge=0, description="Number of likes")
    comment_count: int = Field(..., ge=0, description="Number of comments")
    share_count: int = Field(..., ge=0, description="Number of shares")
    collect_count: int = Field(..., ge=0, description="Number of bookmarks/saves")

    # Content Classification
    is_ad: bool = Field(default=False, description="Whether post is sponsored content")
    is_pinned: bool = Field(default=False, description="Whether post is pinned")

    # Timestamps
    posted_at: datetime = Field(..., description="When the post was published")
    created_at: datetime = Field(default_factory=datetime.now)


# Hashtag Analysis Model
class PostHashtag(BaseModel):
    """
    Hashtag usage tracking for niche and trend analysis with validation
    """
    post_id: str = Field(..., min_length=1, description="Associated post ID")
    hashtag: str = Field(..., min_length=1, max_length=100, pattern=r'^[a-zA-Z0-9_]+$',
                         description="Hashtag text without #")

    @field_validator('hashtag')
    @classmethod
    def validate_hashtag_format(cls, v):
        if v.startswith('#'):
            raise ValueError('Hashtag should not include the # symbol')
        return v.lower()


class TikTokComment(BaseModel):
    comment_id: str = Field(..., description="Unique Post ID")
    post_id: str = Field(..., description="Post ID")
    influencer_tiktok_id: str = Field(..., description="Influencer UID")
    create_time: datetime = Field(description="Post creation timestamp")
    comment_text: str = Field(..., description="Main comment content")
    digg_count: int = Field(..., ge=0, description="Likes count")
    reply_comment_total: int = Field(default=0, description="Total number of replies")
    share_title: str = Field(default=None, description="Title containing hashtags")
    share_desc: str = Field(default=None, description="Description for content relevancy check")


class ReplyComment(BaseModel):
    main_comment_id: str = Field(..., description="Comment ID")
    reply_comment_id: str = Field(..., description="Reply comment ID")
    replier_id: str = Field(..., description="User ID of replier")
    reply_text: str = Field(..., description="Reply content text")
    digg_count: int = Field(..., ge=0, description="Reply likes count")
    label_text: Optional[str] = Field(default=None, description="User role label, e.g., creator")
