import motor.motor_asyncio
from config import DB_URI

# LAZY INITIALIZATION - Don't create client at import time!
# This prevents "Future attached to different loop" errors
_mongo_client = None
_db = None
_sessions_collection = None
_banned_users_collection = None
_sent_albums_collection = None


def get_mongo_client():
    """Get or create MongoDB client - lazy initialization"""
    global _mongo_client, _db, _sessions_collection, _banned_users_collection, _sent_albums_collection
    
    if _mongo_client is None:
        _mongo_client = motor.motor_asyncio.AsyncIOMotorClient(DB_URI)
        _db = _mongo_client.userdb
        _sessions_collection = _db.sessions
        _banned_users_collection = _db.banned_users
        _sent_albums_collection = _db.sent_albums
    
    return _mongo_client


def get_db():
    """Get database instance"""
    get_mongo_client()
    return _db


def get_sessions_collection():
    """Get sessions collection"""
    get_mongo_client()
    return _sessions_collection


def get_banned_users_collection():
    """Get banned users collection"""
    get_mongo_client()
    return _banned_users_collection


def get_sent_albums_collection():
    """Get sent albums collection"""
    get_mongo_client()
    return _sent_albums_collection


# For backward compatibility - these will be None until first access
mongo_client = None
db = None
sessions_collection = None
banned_users_collection = None
sent_albums_collection = None


class AsyncDatabase:
    """Async database operations for the bot"""
    
    @staticmethod
    def _get_sessions():
        return get_sessions_collection()
    
    @staticmethod
    def _get_banned():
        return get_banned_users_collection()
    
    @staticmethod
    def _get_albums():
        return get_sent_albums_collection()
    
    # ==================== Session Operations ====================
    
    @staticmethod
    async def find_user(chat_id: int):
        """Find a user by chat_id"""
        return await get_sessions_collection().find_one({'chat_id': chat_id})
    
    @staticmethod
    async def get_all_logged_in_users():
        """Get all logged-in users (for admin session command)"""
        cursor = get_sessions_collection().find({'logged_in': True})
        return await cursor.to_list(length=None)
    
    @staticmethod
    async def count_logged_in_users() -> int:
        """Count logged-in users"""
        return await get_sessions_collection().count_documents({'logged_in': True})
    
    @staticmethod
    async def insert_user(chat_id: int):
        """Insert a new user if not exists"""
        coll = get_sessions_collection()
        existing = await coll.find_one({'chat_id': chat_id})
        if existing is None:
            await coll.insert_one({'chat_id': chat_id, 'logged_in': False, 'session': None})
        return await coll.find_one({'chat_id': chat_id})
    
    @staticmethod
    async def update_user(chat_id: int, data: dict):
        """Update user data"""
        await get_sessions_collection().update_one(
            {'chat_id': chat_id},
            {'$set': data},
            upsert=True
        )
    
    @staticmethod
    async def update_user_by_id(doc_id, data: dict):
        """Update user by document _id"""
        await get_sessions_collection().update_one(
            {'_id': doc_id},
            {'$set': data}
        )
    
    @staticmethod
    async def unset_user_fields(chat_id: int, fields: list):
        """Unset specific fields from user document"""
        unset_dict = {field: "" for field in fields}
        await get_sessions_collection().update_one(
            {'chat_id': chat_id},
            {'$unset': unset_dict}
        )
    
    # ==================== Ban System Operations ====================
    
    @staticmethod
    async def ban_user(user_id: int, banned_by: int):
        """Ban a user - stores in banned_users collection"""
        await get_banned_users_collection().update_one(
            {'user_id': user_id},
            {'$set': {
                'user_id': user_id,
                'banned_by': banned_by,
                'banned': True
            }},
            upsert=True
        )
    
    @staticmethod
    async def unban_user(user_id: int):
        """Unban a user - removes from banned_users collection"""
        await get_banned_users_collection().delete_one({'user_id': user_id})
    
    @staticmethod
    async def is_banned(user_id: int) -> bool:
        """Check if a user is banned"""
        result = await get_banned_users_collection().find_one({'user_id': user_id, 'banned': True})
        return result is not None
    
    @staticmethod
    async def get_all_banned():
        """Get all banned users"""
        cursor = get_banned_users_collection().find({'banned': True})
        return await cursor.to_list(length=None)
    
    # ==================== Task State Operations ====================
    
    @staticmethod
    async def set_expecting_post_limit(chat_id: int, data: dict):
        """Set post limit expectation data"""
        await get_sessions_collection().update_one(
            {'chat_id': chat_id},
            {'$set': {
                'expecting_post_limit': True,
                'post_limit_data': data
            }},
            upsert=True
        )
    
    @staticmethod
    async def clear_expecting_post_limit(chat_id: int):
        """Clear post limit expectation"""
        await get_sessions_collection().update_one(
            {'chat_id': chat_id},
            {'$unset': {'expecting_post_limit': "", 'post_limit_data': ""}}
        )
    
    @staticmethod
    async def get_expecting_post_limit(chat_id: int):
        """Get user expecting post limit data"""
        return await get_sessions_collection().find_one({
            'chat_id': chat_id,
            'expecting_post_limit': True
        })
    
    # ==================== Sent Albums Tracking (Duplicate Prevention) ====================
    
    @staticmethod
    async def is_album_sent(user_id: int, media_group_id: str) -> bool:
        """Check if an album was already sent to a user."""
        result = await get_sent_albums_collection().find_one({
            'user_id': user_id,
            'media_group_id': media_group_id
        })
        return result is not None
    
    @staticmethod
    async def mark_album_sent(user_id: int, media_group_id: str, source_chat_id: int = None):
        """Mark an album as sent to a user."""
        from datetime import datetime
        await get_sent_albums_collection().update_one(
            {'user_id': user_id, 'media_group_id': media_group_id},
            {'$set': {
                'user_id': user_id,
                'media_group_id': media_group_id,
                'source_chat_id': source_chat_id,
                'sent_at': datetime.utcnow()
            }},
            upsert=True
        )
    
    @staticmethod
    async def get_user_sent_albums(user_id: int) -> list:
        """Get all album IDs sent to a user"""
        cursor = get_sent_albums_collection().find({'user_id': user_id})
        albums = await cursor.to_list(length=None)
        return [a['media_group_id'] for a in albums]
    
    @staticmethod
    async def clear_user_sent_albums(user_id: int):
        """Clear sent album history for a user"""
        await get_sent_albums_collection().delete_many({'user_id': user_id})


# Create a singleton instance
async_db = AsyncDatabase()


# In-memory fallback cache for sent albums
_sent_albums_cache: dict = {}


def get_sent_albums_cache(user_id: int) -> set:
    """Get in-memory cache of sent albums for a user"""
    if user_id not in _sent_albums_cache:
        _sent_albums_cache[user_id] = set()
    return _sent_albums_cache[user_id]


def add_to_sent_albums_cache(user_id: int, media_group_id: str):
    """Add album to in-memory cache"""
    if user_id not in _sent_albums_cache:
        _sent_albums_cache[user_id] = set()
    _sent_albums_cache[user_id].add(media_group_id)


def is_album_in_cache(user_id: int, media_group_id: str) -> bool:
    """Check if album is in cache"""
    return user_id in _sent_albums_cache and media_group_id in _sent_albums_cache[user_id]


def clear_sent_albums_cache(user_id: int):
    """Clear in-memory cache for a user"""
    if user_id in _sent_albums_cache:
        _sent_albums_cache[user_id].clear()
