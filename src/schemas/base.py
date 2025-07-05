from datetime import datetime
from decimal import Decimal
from typing import Optional, List

from pydantic import BaseModel, Field


# Base schemas for common patterns
class BaseSchema(BaseModel):
    """Base schema with common configuration"""

    class Config:
        from_attributes = True  # For Pydantic v2 (use orm_mode = True for v1)
        validate_assignment = True
        str_strip_whitespace = True


class TimestampMixin(BaseModel):
    """Mixin for timestamp fields"""
    created_at: datetime
    updated_at: Optional[datetime] = None


# User Schemas
class UserBase(BaseSchema):
    """Base user fields for creation/update"""
    tiktok_id: str = Field(..., min_length=1, max_length=100)
    username: str = Field(..., min_length=1, max_length=100)
    display_name: Optional[str] = Field(None, max_length=200)
    follower_count: Optional[int] = Field(None, ge=0)
    following_count: Optional[int] = Field(None, ge=0)
    bio: Optional[str] = None
    profile_image_url: Optional[str] = None
    verification_status: bool = False
    engagement_rate: Optional[Decimal] = Field(None, ge=0, le=100, decimal_places=2)
    authenticity_score: Optional[Decimal] = Field(None, ge=0, le=10, decimal_places=2)
    last_scraped_at: Optional[datetime] = None


class UserCreate(UserBase):
    """Schema for creating a user"""
    pass


class UserUpdate(BaseSchema):
    """Schema for updating a user (all fields optional)"""
    username: Optional[str] = Field(None, min_length=1, max_length=100)
    display_name: Optional[str] = Field(None, max_length=200)
    follower_count: Optional[int] = Field(None, ge=0)
    following_count: Optional[int] = Field(None, ge=0)
    bio: Optional[str] = None
    profile_image_url: Optional[str] = None
    verification_status: Optional[bool] = None
    engagement_rate: Optional[Decimal] = Field(None, ge=0, le=100, decimal_places=2)
    authenticity_score: Optional[Decimal] = Field(None, ge=0, le=10, decimal_places=2)
    last_scraped_at: Optional[datetime] = None


class UserResponse(UserBase, TimestampMixin):
    """Schema for user response with all fields"""
    id: int


# Post Schemas
class PostBase(BaseSchema):
    """Base post fields for creation/update"""
    tiktok_post_id: str = Field(..., min_length=1, max_length=100)
    description: Optional[str] = None
    hashtags: Optional[List[str]] = Field(None, max_length=50)
    view_count: Optional[int] = Field(None, ge=0)
    like_count: Optional[int] = Field(None, ge=0)
    comment_count: Optional[int] = Field(None, ge=0)
    share_count: Optional[int] = Field(None, ge=0)
    post_url: Optional[str] = None
    posted_at: Optional[datetime] = None


class PostCreate(PostBase):
    """Schema for creating a post"""
    user_id: int = Field(..., gt=0)


class PostUpdate(BaseSchema):
    """Schema for updating a post (all fields optional)"""
    description: Optional[str] = None
    hashtags: Optional[List[str]] = Field(None,)
    view_count: Optional[int] = Field(None, ge=0)
    like_count: Optional[int] = Field(None, ge=0)
    comment_count: Optional[int] = Field(None, ge=0)
    share_count: Optional[int] = Field(None, ge=0)
    post_url: Optional[str] = None
    posted_at: Optional[datetime] = None


class PostResponse(PostBase):
    """Schema for post response with all fields"""
    id: int
    user_id: int
    created_at: datetime


# Comment Schemas
class CommentBase(BaseSchema):
    """Base comment fields for creation/update"""
    tiktok_comment_id: Optional[str] = Field(None, max_length=100)
    username: str = Field(..., min_length=1, max_length=100)
    comment_text: Optional[str] = None
    like_count: int = Field(0, ge=0)
    is_verified: bool = False
    authenticity_score: Optional[Decimal] = Field(None, ge=0, le=10, decimal_places=2)


class CommentCreate(CommentBase):
    """Schema for creating a comment"""
    post_id: int = Field(..., gt=0)


class CommentUpdate(BaseSchema):
    """Schema for updating a comment (all fields optional)"""
    username: Optional[str] = Field(None, min_length=1, max_length=100)
    comment_text: Optional[str] = None
    like_count: Optional[int] = Field(None, ge=0)
    is_verified: Optional[bool] = None
    authenticity_score: Optional[Decimal] = Field(None, ge=0, le=10, decimal_places=2)


class CommentResponse(CommentBase):
    """Schema for comment response with all fields"""
    id: int
    post_id: int
    created_at: datetime


# Nested response schemas for related data
class UserWithPosts(UserResponse):
    """User schema with related posts"""
    posts: List[PostResponse] = []


class PostWithComments(PostResponse):
    """Post schema with related comments"""
    comments: List[CommentResponse] = []


class PostWithUser(PostResponse):
    """Post schema with related user"""
    user: UserResponse


class CommentWithPost(CommentResponse):
    """Comment schema with related post"""
    post: PostResponse


# Analytics schemas
class UserAnalytics(BaseSchema):
    """Schema for user analytics data"""
    user_id: int
    total_posts: int
    total_views: int
    total_likes: int
    total_comments: int
    total_shares: int
    avg_engagement_rate: Optional[Decimal] = None
    avg_authenticity_score: Optional[Decimal] = None


class PostAnalytics(BaseSchema):
    """Schema for post analytics data"""
    post_id: int
    engagement_rate: Optional[Decimal] = None
    comment_to_like_ratio: Optional[Decimal] = None
    viral_score: Optional[Decimal] = None


# Bulk operation schemas
class BulkUserCreate(BaseSchema):
    """Schema for bulk user creation"""
    users: List[UserCreate] = Field(..., min_length=1, max_length=1000)


class BulkPostCreate(BaseSchema):
    """Schema for bulk post creation"""
    posts: List[PostCreate] = Field(..., min_length=1, max_length=1000)


class BulkCommentCreate(BaseSchema):
    """Schema for bulk comment creation"""
    comments: List[CommentCreate] = Field(..., min_length=1, max_length=1000)
