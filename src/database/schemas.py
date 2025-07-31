import logging
from datetime import datetime, date
from typing import Optional

from sqlalchemy import Column, Text
from sqlmodel import SQLModel, Field

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class Influencer(SQLModel, table=True):
    """Influencer/Profile model"""
    __tablename__ = "influencers"

    # Primary key (auto-increment)
    id: Optional[int] = Field(default=None, primary_key=True)

    # TikTok user identifier (unique)
    user_id: str = Field(unique=True, index=True)
    username: str = Field(max_length=100, unique=True, index=True)
    display_name: Optional[str] = Field(default=None, max_length=200)
    bio_text: Optional[str] = Field(default=None)

    # Profile metrics
    follower_count: Optional[int] = Field(default=0, ge=0)
    following_count: Optional[int] = Field(default=0, ge=0)
    heart_count: Optional[int] = Field(default=0, ge=0)
    video_count: Optional[int] = Field(default=0, ge=0)
    total_posts: Optional[int] = Field(default=0, ge=0)

    # Profile details from scraper
    avatar_url: Optional[str] = Field(default=None)
    profile_url: Optional[str] = Field(default=None)
    verified: Optional[bool] = Field(default=False)

    # Timestamps (never updated after creation)
    first_discovered: date = Field(default_factory=date.today)
    last_updated: datetime = Field(default_factory=datetime.now)

    # Additional data from scraper
    load_timestamp: Optional[str] = Field(default=None)


class Post(SQLModel, table=True):
    """Post model"""
    __tablename__ = "posts"

    # Primary key (auto-increment)
    id: Optional[int] = Field(default=None, primary_key=True)

    # Foreign key to influencer
    influencer_id: int = Field(foreign_key="influencers.id", index=True)

    # TikTok post identifier (unique with influencer_id)
    platform_post_id: str = Field(max_length=100, index=True)
    post_date: Optional[datetime] = Field(default=None)

    # Engagement metrics
    likes_count: int = Field(default=0, ge=0)
    comments_count: int = Field(default=0, ge=0)
    shares_count: int = Field(default=0, ge=0)
    views_count: int = Field(default=0, ge=0)

    # Content
    caption_text: Optional[str] = Field(default=None, sa_column=Column(Text))

    # Additional data from scraper
    author_stats: Optional[str] = Field(default=None, sa_column=Column(Text))
    author_stats_v2: Optional[str] = Field(default=None, sa_column=Column(Text))
    contents: Optional[str] = Field(default=None, sa_column=Column(Text))
    challenges: Optional[str] = Field(default=None, sa_column=Column(Text))
    text_extra: Optional[str] = Field(default=None, sa_column=Column(Text))
    raw_post_data: Optional[str] = Field(default=None, sa_column=Column(Text))

    # Timestamps
    created_at: datetime = Field(default_factory=datetime.now)
    updated_at: datetime = Field(default_factory=datetime.now)

class Comment(SQLModel, table=True):
    """Comment model"""
    __tablename__ = "comments"

    # Primary key (auto-increment)
    id: Optional[int] = Field(default=None, primary_key=True)

    # Foreign key to post
    post_id: int = Field(foreign_key="posts.id", index=True)

    # TikTok comment identifier
    comment_id: str = Field(unique=True, index=True)

    # Comment content
    comment_text: Optional[str] = Field(default=None, sa_column=Column(Text))
    create_time: Optional[int] = Field(default=None)  # Unix timestamp

    # Engagement
    digg_count: int = Field(default=0, ge=0)
    is_author_digged: Optional[bool] = Field(default=None)

    # Comment metadata
    is_comment_translatable: Optional[bool] = Field(default=None)
    comment_language: Optional[str] = Field(default=None, max_length=10)

    # Commenter information
    commenter_uid: Optional[str] = Field(default=None)
    commenter_nickname: Optional[str] = Field(default=None, max_length=200)
    commenter_sec_uid: Optional[str] = Field(default=None)
    commenter_unique_id: Optional[str] = Field(default=None)

    # Profile context (from post owner)
    profile_username: Optional[str] = Field(default=None, max_length=100)
    profile_display_name: Optional[str] = Field(default=None, max_length=200)

    # Timestamps
    created_at: datetime = Field(default_factory=datetime.now)
    updated_at: datetime = Field(default_factory=datetime.now)


class Hashtag(SQLModel, table=True):
    """Hashtag model"""
    __tablename__ = "hashtags"

    id: Optional[int] = Field(default=None, primary_key=True)
    hashtag: str = Field(unique=True, max_length=100, index=True)
    search_timestamp: Optional[str] = Field(default=None)
    profiles_found: int = Field(default=0)
    created_at: datetime = Field(default_factory=datetime.now)


class InfluencerHashtag(SQLModel, table=True):
    """Junction table for influencer-hashtag relationships"""
    __tablename__ = "influencer_hashtags"

    id: Optional[int] = Field(default=None, primary_key=True)
    influencer_id: int = Field(foreign_key="influencers.id", index=True)
    hashtag_id: int = Field(foreign_key="hashtags.id", index=True)
    created_at: datetime = Field(default_factory=datetime.now)


class AnalysisCache(SQLModel, table=True):
    """Analysis cache model"""
    __tablename__ = "analysis_cache"

    id: Optional[int] = Field(default=None, primary_key=True)
    influencer_id: int = Field(foreign_key="influencers.id", index=True)
    engagement_rate: Optional[float] = Field(default=None)
    authenticity_score: Optional[float] = Field(default=None)
    composite_score: Optional[float] = Field(default=None)
    calculated_date: date = Field(default_factory=date.today)
    created_at: datetime = Field(default_factory=datetime.now)
