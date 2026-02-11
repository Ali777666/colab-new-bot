
# Don't Remove Credit Tg - @VJ_Botz
# Subscribe YouTube Channel For Amazing Bot https://youtube.com/@Tech_VJ
# Ask Doubt on telegram @KingVJ01

import asyncio 
import random
import pyrogram
from pyrogram import Client, filters
from pyrogram.errors import FloodWait, UserIsBlocked, InputUserDeactivated, UserAlreadyParticipant, InviteHashExpired, UsernameNotOccupied
from pyrogram.types import (
    InlineKeyboardMarkup, InlineKeyboardButton, Message,
    InputMediaPhoto, InputMediaVideo, InputMediaDocument, InputMediaAudio,
)
from core.reply_compat import build_reply_kwargs_from_message, build_link_preview_kwargs
from core.safe_send import (
    safe_send_message as core_safe_send,
    safe_reply as core_safe_reply,
    safe_edit_message,
)
from core.message_utils import (
    split_caption as split_caption_tuple,
    normalize_poll_to_text,
    MAX_MESSAGE_LENGTH,
    MAX_CAPTION_LENGTH,
)
from pyrogram.enums import ChatType, ParseMode, UserStatus, PollType, MessageMediaType, MessageEntityType
from pyrogram.raw import functions, types
import time
import os
import shutil
import re
import logging
from typing import List, Tuple, Optional, Any

logger = logging.getLogger(__name__)
from config import API_ID, API_HASH, OWNER_ID, OWNER_USERNAME, BANNED_MESSAGE, TEMP_DOWNLOAD_DIR
from database.async_db import async_db
from TechVJ.strings import strings, HELP_TXT

from TechVJ.file_splitter import (
    needs_splitting, split_file, cleanup_chunks, 
    get_chunk_info, format_size, MAX_TELEGRAM_FILE_SIZE,
    split_large_video, is_video_file
)
from TechVJ.task_manager import task_manager, StopSafePipeline, sanitize_filename

# User download queue (one active per user)
from TechVJ.user_queue import user_queue, check_and_queue, format_queue_status

# Import refactored modules (v2)
from TechVJ.session_handler import (
    create_user_session, 
    SessionInvalidError, 
    SessionConnectionError
)
from TechVJ.album_collector_v2 import (
    AlbumAwareIterator,
    detect_album_boundary,
    process_album_with_session,
)

# Import download engine from core
from core.downloader import DownloadEngine, get_engine, progress_tracker

# Maximum retries for connection issues
MAX_RETRIES = 3
RETRY_DELAY = 2


# ==================== URL PARSING (SECTION A) ====================

class ParsedURL:
    """Parsed URL result containing channel_id, post_ids, and optional topic/thread info"""
    def __init__(
        self, 
        channel_id: int, 
        post_ids: List[int], 
        url_type: str = "private",
        topic_id: int = None,
        thread_id: int = None
    ):
        self.channel_id = channel_id
        self.post_ids = post_ids
        self.url_type = url_type  # "private", "public", "bot", "topic", "thread"
        self.topic_id = topic_id  # For topic links: https://t.me/c/CHAT/TOPIC/MSG
        self.thread_id = thread_id  # For thread links: ?thread=ID
    
    @property
    def is_topic(self) -> bool:
        return self.topic_id is not None
    
    @property
    def is_thread(self) -> bool:
        return self.thread_id is not None
    
    def __repr__(self):
        extra = ""
        if self.topic_id:
            extra = f", topic_id={self.topic_id}"
        if self.thread_id:
            extra = f", thread_id={self.thread_id}"
        return f"ParsedURL(channel_id={self.channel_id}, post_ids={self.post_ids}, type={self.url_type}{extra})"


def parse_topic_url(url: str) -> Optional[ParsedURL]:
    """
    Parse topic URL format.
    
    Supported formats:
        https://t.me/c/<channel_id>/<topic_id>/<post_id>
        https://t.me/c/<channel_id>/<topic_id>/<from_id>-<to_id>
        https://t.me/c/<channel_id>/<topic_id>/<id1>,<id2>,<id3>
    
    Returns:
        ParsedURL with topic_id set, or None if not a topic URL
    """
    url = url.strip()
    
    # Topic with range: /c/CHAT/TOPIC/FROM-TO
    topic_range = re.match(r'^https?://t\.me/c/(\d+)/(\d+)/(\d+)\s*-\s*(\d+)$', url)
    if topic_range:
        channel_id = int("-100" + topic_range.group(1))
        topic_id = int(topic_range.group(2))
        from_id = int(topic_range.group(3))
        to_id = int(topic_range.group(4))
        
        if from_id <= to_id:
            post_ids = list(range(from_id, to_id + 1))
        else:
            post_ids = list(range(from_id, to_id - 1, -1))
        
        return ParsedURL(channel_id, post_ids, "topic", topic_id=topic_id)
    
    # Topic with comma-separated: /c/CHAT/TOPIC/ID1,ID2,ID3
    topic_multi = re.match(r'^https?://t\.me/c/(\d+)/(\d+)/(\d+(?:,\d+)+)$', url)
    if topic_multi:
        channel_id = int("-100" + topic_multi.group(1))
        topic_id = int(topic_multi.group(2))
        post_ids_str = topic_multi.group(3)
        post_ids = [int(pid.strip()) for pid in post_ids_str.split(',')]
        return ParsedURL(channel_id, post_ids, "topic", topic_id=topic_id)
    
    # Topic single: /c/CHAT/TOPIC/MSG
    topic_single = re.match(r'^https?://t\.me/c/(\d+)/(\d+)/(\d+)$', url)
    if topic_single:
        channel_id = int("-100" + topic_single.group(1))
        topic_id = int(topic_single.group(2))
        post_id = int(topic_single.group(3))
        return ParsedURL(channel_id, [post_id], "topic", topic_id=topic_id)
    
    return None


def parse_thread_url(url: str) -> Optional[ParsedURL]:
    """
    Parse thread URL format (comment thread).
    
    Supported formats:
        https://t.me/c/<channel_id>/<post_id>?thread=<thread_id>
        https://t.me/c/<channel_id>/<post_id>?thread=<thread_id>&range<start>-<end>
        https://t.me/c/<channel_id>/<post_id>?thread=<thread_id>&range=<start>-<end>
    
    Returns:
        ParsedURL with thread_id set, or None if not a thread URL
    """
    url = url.strip()
    
    # Thread link with range (both &range= and &rangeSTART-END formats)
    # Format: /c/CHAT/MSG?thread=ID&range=START-END or /c/CHAT/MSG?thread=ID&rangeSTART-END
    thread_range_match = re.match(
        r'^https?://t\.me/c/(\d+)/(\d+)\?thread=(\d+)&range=?(\d+)-(\d+)$', url
    )
    if thread_range_match:
        channel_id = int("-100" + thread_range_match.group(1))
        post_id = int(thread_range_match.group(2))
        thread_id = int(thread_range_match.group(3))
        range_start = int(thread_range_match.group(4))
        range_end = int(thread_range_match.group(5))
        
        # Generate post IDs in range
        if range_start <= range_end:
            post_ids = list(range(range_start, range_end + 1))
        else:
            post_ids = list(range(range_start, range_end - 1, -1))
        
        return ParsedURL(channel_id, post_ids, "thread", thread_id=thread_id)
    
    # Thread link single: /c/CHAT/MSG?thread=ID
    thread_match = re.match(r'^https?://t\.me/c/(\d+)/(\d+)\?thread=(\d+)$', url)
    if thread_match:
        channel_id = int("-100" + thread_match.group(1))
        post_id = int(thread_match.group(2))
        thread_id = int(thread_match.group(3))
        return ParsedURL(channel_id, [post_id], "thread", thread_id=thread_id)
    
    return None


def parse_multi_post_url(url: str) -> Optional[ParsedURL]:
    """
    Parse URL with comma-separated post IDs.
    
    Supported format:
        https://t.me/c/<channel_id>/<post_id1>,<post_id2>,<post_id3>
    
    Returns:
        ParsedURL object with channel_id and list of post_ids, or None if invalid
    """
    url = url.strip()
    
    # Regex for private channel with comma-separated post IDs
    # Format: https://t.me/c/<channel_id>/<post_ids>
    private_pattern = r'^https?://t\.me/c/(\d+)/(\d+(?:,\d+)*)$'
    
    match = re.match(private_pattern, url)
    if match:
        channel_id_str = match.group(1)
        post_ids_str = match.group(2)
        
        # Convert channel_id to Telegram's internal format (-100 prefix)
        channel_id = int("-100" + channel_id_str)
        
        # Parse comma-separated post IDs
        try:
            post_ids = [int(pid.strip()) for pid in post_ids_str.split(',')]
        except ValueError:
            return None
        
        # Validate: must have at least one post ID
        if not post_ids:
            return None
        
        # Remove duplicates while preserving order
        seen = set()
        unique_post_ids = []
        for pid in post_ids:
            if pid not in seen:
                seen.add(pid)
                unique_post_ids.append(pid)
        
        return ParsedURL(channel_id, unique_post_ids, "private")
    
    return None


def parse_range_url(url: str) -> Optional[ParsedURL]:
    """
    Parse URL with range format (from-to).
    
    Supported format:
        https://t.me/c/<channel_id>/<from_id>-<to_id>
    
    Returns:
        ParsedURL object with channel_id and list of post_ids in range
    """
    url = url.strip()
    
    # Regex for private channel with range
    range_pattern = r'^https?://t\.me/c/(\d+)/(\d+)\s*-\s*(\d+)$'
    
    match = re.match(range_pattern, url)
    if match:
        channel_id_str = match.group(1)
        from_id = int(match.group(2))
        to_id = int(match.group(3))
        
        channel_id = int("-100" + channel_id_str)
        
        # Generate post IDs in order
        if from_id <= to_id:
            post_ids = list(range(from_id, to_id + 1))
        else:
            post_ids = list(range(from_id, to_id - 1, -1))
        
        return ParsedURL(channel_id, post_ids, "private")
    
    return None


def parse_single_post_url(url: str) -> Optional[ParsedURL]:
    """
    Parse URL with single post ID.
    
    Supported formats:
        https://t.me/c/<channel_id>/<post_id>
        https://t.me/<username>/<post_id>
        https://t.me/b/<bot_username>/<post_id>
    """
    url = url.strip()
    
    # Private channel single post
    private_single = r'^https?://t\.me/c/(\d+)/(\d+)(?:\?.*)?$'
    match = re.match(private_single, url)
    if match:
        channel_id = int("-100" + match.group(1))
        post_id = int(match.group(2))
        return ParsedURL(channel_id, [post_id], "private")
    
    # Bot chat
    bot_pattern = r'^https?://t\.me/b/([a-zA-Z][a-zA-Z0-9_]{3,})/(\d+)$'
    match = re.match(bot_pattern, url)
    if match:
        username = match.group(1)
        post_id = int(match.group(2))
        return ParsedURL(username, [post_id], "bot")
    
    # Public channel/group
    public_pattern = r'^https?://t\.me/([a-zA-Z][a-zA-Z0-9_]{3,})/(\d+)$'
    match = re.match(public_pattern, url)
    if match:
        username = match.group(1)
        post_id = int(match.group(2))
        return ParsedURL(username, [post_id], "public")
    
    return None


def parse_quizbot_url(url: str) -> Optional[ParsedURL]:
    """
    Parse QuizBot links.
    
    Supported format:
        https://t.me/QuizBot?start=XXXX
    
    Returns:
        ParsedURL with url_type="quizbot", or None
    """
    url = url.strip()
    
    quizbot_match = re.match(r'^https?://t\.me/QuizBot\?start=([a-zA-Z0-9_-]+)$', url, re.IGNORECASE)
    if quizbot_match:
        start_param = quizbot_match.group(1)
        # Store start param in post_ids[0] as a string marker
        return ParsedURL("QuizBot", [start_param], "quizbot")
    
    return None


def parse_telegram_url(url: str) -> Tuple[Optional[ParsedURL], Optional[str]]:
    """
    Main URL parser - tries all formats.
    
    Order of parsing (most specific first):
    1. Topic links (3 path segments)
    2. Thread links (?thread=)
    3. QuizBot links
    4. Comma-separated post IDs
    5. Range format (from-to)
    6. Single post format
    
    Returns:
        Tuple of (ParsedURL or None, error_message or None)
    """
    url = url.strip()
    
    if not url.startswith(('https://t.me/', 'http://t.me/')):
        return None, "Invalid URL: Must start with https://t.me/"
    
    # 1. Try topic format first (most specific - 3 path segments)
    result = parse_topic_url(url)
    if result:
        return result, None
    
    # 2. Try thread format (?thread=)
    result = parse_thread_url(url)
    if result:
        return result, None
    
    # 3. Try QuizBot
    result = parse_quizbot_url(url)
    if result:
        return result, None
    
    # 4. Try comma-separated format
    result = parse_multi_post_url(url)
    if result:
        return result, None
    
    # 5. Try range format
    result = parse_range_url(url)
    if result:
        return result, None
    
    # 6. Try single post format
    result = parse_single_post_url(url)
    if result:
        return result, None
    
    return None, (
        "Invalid URL format. Supported formats:\n"
        "• `https://t.me/c/123456/101,102,103` (comma-separated)\n"
        "• `https://t.me/c/123456/101-110` (range)\n"
        "• `https://t.me/c/123456/101` (single)\n"
        "• `https://t.me/c/123456/5/101` (topic)\n"
        "• `https://t.me/c/123456/101?thread=5` (thread)"
    )


# ==================== HELPER FUNCTIONS ====================

def get(obj, key, default=None):
    try:
        return obj[key]
    except:
        return default


def sanitize_html(content):
    """Fixes common HTML issues like unclosed tags."""
    if not content:
        return content
    tags = ['b', 'i', 'u', 'a', 'code', 'pre']
    for tag in tags:
        open_tags = len(re.findall(f'<{tag}[^>]*>', content))
        close_tags = len(re.findall(f'</{tag}>', content))
        if open_tags > close_tags:
            content += f'</{tag}>' * (open_tags - close_tags)
    return content


def sanitize_markdown(content):
    """
    Fixes common Markdown issues.
    
    IMPORTANT: Be conservative - don't break working text.
    Only fix clearly broken patterns.
    """
    if not content:
        return content
    
    # Fix unclosed markdown links: [text](url -> [text](url)
    pattern = r'\[([^\]]+)\]\(([^)]*[^)])'
    matches = re.findall(pattern, content)
    for text, url in matches:
        old = f'[{text}]({url}'
        new = f'[{text}]({url})'
        content = content.replace(old, new)
    
    # Fix escaped parentheses from bad parsing
    content = re.sub(r'\\\(', '(', content)
    content = re.sub(r'\\\)', ')', content)
    
    return content


def get_caption_with_entities(msg) -> Tuple[Optional[str], Optional[List]]:
    """
    Extract caption with entities from message.
    
    CRITICAL: Returns (text, entities) tuple for use with MessageEntity,
    NOT Markdown formatted text.
    
    Returns:
        (caption_text, caption_entities) - use with entities= parameter
    """
    if not msg.caption:
        return None, None
    
    raw_caption = msg.caption
    entities = getattr(msg, 'caption_entities', None)
    
    # Return raw text and entities list (or None)
    return raw_caption, list(entities) if entities else None


def get_text_with_entities(msg) -> Tuple[Optional[str], Optional[List]]:
    """
    Extract text with entities from message.
    
    CRITICAL: Returns (text, entities) tuple for use with MessageEntity,
    NOT Markdown formatted text.
    
    Returns:
        (text, entities) - use with entities= parameter
    """
    if not msg.text:
        return None, None
    
    raw_text = msg.text
    entities = getattr(msg, 'entities', None)
    
    # Return raw text and entities list (or None)
    return raw_text, list(entities) if entities else None


# ==================== SAFE SEND FUNCTIONS (ENTITY-ONLY) ====================
# These functions NEVER use parse_mode - only MessageEntity for 100% safety

async def send_text_entity_safe(
    client: Client,
    chat_id: int,
    text: str,
    entities: Optional[List] = None,
    reply_to_message_id: Optional[int] = None
) -> Optional[Message]:
    """
    Send text message using ONLY entities (never parse_mode).
    
    SAFE: Prevents ENTITY_BOUNDS_INVALID by validating before send.
    """
    from core.entity_validator import prepare_entities_for_send, utf16_length, MESSAGE_LIMIT
    
    if not text:
        return None
    
    # Validate entities
    safe_entities = prepare_entities_for_send(text, entities) if entities else None
    
    kwargs = {
        'chat_id': chat_id,
        'text': text,
        'disable_web_page_preview': True,
    }
    
    if safe_entities:
        kwargs['entities'] = safe_entities
    
    # Try with reply first
    if reply_to_message_id:
        try:
            return await client.send_message(**kwargs, reply_to_message_id=reply_to_message_id)
        except Exception:
            pass  # Silent fallback
    
    # Send without reply
    try:
        return await client.send_message(**kwargs)
    except FloodWait as e:
        wait = min(getattr(e, 'value', getattr(e, 'x', 30)), 60)
        await asyncio.sleep(wait)
        return await client.send_message(**kwargs)
    except Exception as e:
        # If entity error, try plain text
        if 'ENTITY' in str(e).upper() and safe_entities:
            try:
                del kwargs['entities']
                return await client.send_message(**kwargs)
            except Exception:
                pass
        logger.warning(f"send_text_entity_safe failed: {e}")
        return None


async def send_media_entity_safe(
    client: Client,
    chat_id: int,
    media_type: str,
    file_path: str,
    caption: Optional[str] = None,
    caption_entities: Optional[List] = None,
    reply_to_message_id: Optional[int] = None,
    progress: Any = None,
    **extra_kwargs
) -> Tuple[Optional[Message], Optional[Message]]:
    """
    Send media using ONLY caption_entities (never parse_mode).
    
    SAFE: Prevents ENTITY_BOUNDS_INVALID.
    Returns (media_msg, overflow_msg) - overflow_msg is for long captions.
    """
    from core.entity_validator import (
        prepare_entities_for_send, 
        split_caption_with_entities,
        utf16_length,
        CAPTION_LIMIT
    )
    
    caption_kwargs = {}
    overflow_chunks = None
    
    if caption:
        cap_len = utf16_length(caption)
        
        if cap_len <= CAPTION_LIMIT:
            safe_entities = prepare_entities_for_send(caption, caption_entities) if caption_entities else None
            caption_kwargs['caption'] = caption
            if safe_entities:
                caption_kwargs['caption_entities'] = safe_entities
        else:
            # Long caption - split
            cap_chunk, overflow_chunks = split_caption_with_entities(caption, caption_entities or [])
            caption_kwargs['caption'] = cap_chunk.text
            if cap_chunk.entities:
                caption_kwargs['caption_entities'] = cap_chunk.entities
    
    # Build send kwargs
    send_kwargs = {
        'chat_id': chat_id,
        media_type: file_path,
        **caption_kwargs,
        **extra_kwargs
    }
    
    if progress:
        send_kwargs['progress'] = progress
    
    send_func = getattr(client, f'send_{media_type}')
    
    # Try with reply first
    media_msg = None
    if reply_to_message_id:
        try:
            media_msg = await send_func(**send_kwargs, reply_to_message_id=reply_to_message_id)
        except Exception:
            pass
    
    if not media_msg:
        try:
            media_msg = await send_func(**send_kwargs)
        except FloodWait as e:
            wait = min(getattr(e, 'value', getattr(e, 'x', 30)), 60)
            await asyncio.sleep(wait)
            media_msg = await send_func(**send_kwargs)
        except Exception as e:
            # If entity error, try without entities
            if 'ENTITY' in str(e).upper() and 'caption_entities' in send_kwargs:
                try:
                    del send_kwargs['caption_entities']
                    media_msg = await send_func(**send_kwargs)
                except Exception:
                    pass
            if not media_msg:
                logger.warning(f"send_media_entity_safe failed: {e}")
                return None, None
    
    # Send overflow caption if needed
    overflow_msg = None
    if overflow_chunks and media_msg:
        from TechVJ.lang import get_string
        for chunk in overflow_chunks:
            header = get_string(media_msg.chat.id, "caption_continued") + "\n\n"
            overflow_text = f"{header}{chunk.text}"
            # Adjust entity offsets for header
            from core.entity_validator import utf16_length as u16len
            header_len = u16len(header)
            adjusted = []
            for e in (chunk.entities or []):
                try:
                    from pyrogram.types import MessageEntity
                    new_e = MessageEntity(
                        type=e.type,
                        offset=e.offset + header_len,
                        length=e.length,
                        url=getattr(e, 'url', None),
                        user=getattr(e, 'user', None),
                        language=getattr(e, 'language', None),
                    )
                    adjusted.append(new_e)
                except Exception:
                    pass
            
            safe_entities = prepare_entities_for_send(overflow_text, adjusted) if adjusted else None
            overflow_msg = await send_text_entity_safe(client, chat_id, overflow_text, safe_entities)
    
    return media_msg, overflow_msg


# DEPRECATED: These functions use Markdown, prefer get_*_with_entities() instead
def get_safe_caption(msg) -> Optional[str]:
    """
    DEPRECATED: Use get_caption_with_entities() instead.
    
    This function returns Markdown-formatted text which can cause parsing issues.
    Kept for backwards compatibility only.
    """
    if not msg.caption:
        return None
    
    raw_caption = msg.caption
    entities = getattr(msg, 'caption_entities', None)
    
    if not entities:
        return raw_caption
    
    try:
        formatted = extract_hyperlinks(raw_caption, entities)
        if formatted:
            return formatted
    except Exception as e:
        logger.warning(f"extract_hyperlinks failed for caption: {e}")
    
    return raw_caption


def get_safe_text(msg) -> Optional[str]:
    """
    DEPRECATED: Use get_text_with_entities() instead.
    
    This function returns Markdown-formatted text which can cause parsing issues.
    Kept for backwards compatibility only.
    """
    if not msg.text:
        return None
    
    raw_text = msg.text
    entities = getattr(msg, 'entities', None)
    
    if not entities:
        return raw_text
    
    try:
        formatted = extract_hyperlinks(raw_text, entities)
        if formatted:
            return formatted
    except Exception as e:
        logger.warning(f"extract_hyperlinks failed for text: {e}")
    
    return raw_text


# Telegram caption limit (Unicode characters)
TELEGRAM_CAPTION_LIMIT = 1024


def split_caption_safe(caption: str, limit: int = TELEGRAM_CAPTION_LIMIT) -> tuple:
    """
    Safely split caption for Telegram's 1024 character limit.
    
    Returns:
        (media_caption, overflow_text)
        - media_caption: First part (≤1024 chars) for media caption
        - overflow_text: Remaining text to send as separate message, or None
    
    Rules:
        - Preserves Unicode integrity (no broken emoji/UTF-8)
        - Tries to split at word boundary
        - Never loses any text
    """
    if not caption:
        return None, None
    
    # If within limit, return as-is
    if len(caption) <= limit:
        return caption, None
    
    logger.info(f"Caption exceeds {limit} chars ({len(caption)}), splitting...")
    
    # Find a good split point (word boundary, newline, or space)
    split_at = limit
    
    # Try to find a newline near the limit
    newline_pos = caption.rfind('\n', 0, limit)
    if newline_pos > limit - 200:  # Within last 200 chars
        split_at = newline_pos + 1
    else:
        # Try to find a space (word boundary)
        space_pos = caption.rfind(' ', 0, limit)
        if space_pos > limit - 100:  # Within last 100 chars
            split_at = space_pos + 1
        else:
            # Hard split at limit, but ensure we don't break Unicode
            # Python strings are Unicode-safe, but let's be careful
            split_at = limit
    
    media_caption = caption[:split_at].rstrip()
    overflow_text = caption[split_at:].lstrip()
    
    # Ensure media_caption doesn't exceed limit after trimming
    if len(media_caption) > limit:
        media_caption = media_caption[:limit]
    
    logger.debug(f"Split caption: {len(media_caption)} + {len(overflow_text)} chars")
    
    return media_caption, overflow_text if overflow_text else None


# Telegram message limit (UTF-8 characters)
TELEGRAM_MESSAGE_LIMIT = 4096


def find_hyperlink_boundaries(text: str) -> list:
    """
    Find all markdown hyperlink positions in text.
    Returns list of (start, end) tuples for each [text](url) pattern.
    """
    # Match markdown links: [text](url)
    pattern = r'\[([^\]]+)\]\(([^)]+)\)'
    boundaries = []
    for match in re.finditer(pattern, text):
        boundaries.append((match.start(), match.end()))
    return boundaries


def safe_split_point(text: str, proposed_split: int, hyperlinks: list) -> int:
    """
    Adjust split point to not break hyperlinks.
    If proposed split is inside a hyperlink, move it before the hyperlink.
    """
    for start, end in hyperlinks:
        # If split point is inside a hyperlink
        if start < proposed_split < end:
            # Move split point to before the hyperlink
            # But first check if there's a good break point before it
            search_text = text[:start]
            
            # Try to find space before hyperlink
            space_pos = search_text.rfind(' ')
            if space_pos > start - 200:  # Within 200 chars before hyperlink
                return space_pos + 1
            
            # Try newline
            newline_pos = search_text.rfind('\n')
            if newline_pos > start - 200:
                return newline_pos + 1
            
            # Just split before the hyperlink
            return start
    
    return proposed_split


def split_message_safe(text: str, limit: int = TELEGRAM_MESSAGE_LIMIT) -> list:
    """
    Split long text into Telegram-compliant message chunks (max 4096 chars).
    
    Splitting priority:
    1. Split by paragraph (\\n\\n)
    2. If paragraph > limit → split by line (\\n)
    3. If line > limit → split at word boundary
    4. NEVER split inside hyperlinks - move whole hyperlink to next chunk
    
    Features:
    - UTF-8 safe (character-based, not byte-based)
    - Preserves hyperlinks intact (never breaks [text](url))
    - Preserves formatting, paragraphs, line breaks, emojis
    - No broken Unicode characters
    - No content loss
    
    Returns:
        List of message chunks, each ≤ limit characters
    """
    if not text:
        return []
    
    # If within limit, return as single chunk
    if len(text) <= limit:
        return [text]
    
    # Find all hyperlinks to protect them
    hyperlinks = find_hyperlink_boundaries(text)
    
    chunks = []
    remaining = text
    offset = 0  # Track position in original text for hyperlink matching
    
    while remaining:
        if len(remaining) <= limit:
            chunks.append(remaining)
            break
        
        # Find best split point within limit
        split_at = limit
        
        # Priority 1: Try to split at paragraph boundary (\n\n)
        para_pos = remaining.rfind('\n\n', 0, limit)
        if para_pos > limit // 2:  # At least halfway through
            split_at = para_pos + 2  # Include the newlines
        else:
            # Priority 2: Try to split at line boundary (\n)
            line_pos = remaining.rfind('\n', 0, limit)
            if line_pos > limit // 2:
                split_at = line_pos + 1
            else:
                # Priority 3: Try to split at word boundary (space)
                space_pos = remaining.rfind(' ', 0, limit)
                if space_pos > limit - 500:  # Within last 500 chars
                    split_at = space_pos + 1
                else:
                    # Hard split at limit (last resort)
                    split_at = limit
        
        # CRITICAL: Adjust split point to not break hyperlinks
        # Recalculate hyperlinks for current remaining text
        current_hyperlinks = find_hyperlink_boundaries(remaining)
        split_at = safe_split_point(remaining, split_at, current_hyperlinks)
        
        # Safety check: ensure we make progress
        if split_at <= 0:
            split_at = min(limit, len(remaining))
        
        chunk = remaining[:split_at].rstrip()
        remaining = remaining[split_at:].lstrip()
        offset += split_at
        
        if chunk:
            chunks.append(chunk)
    
    logger.debug(f"Split message into {len(chunks)} chunks (hyperlink-safe)")
    return chunks


async def safe_send_message(
    client: Client,
    chat_id: int,
    text: str,
    parse_mode=None,
    entities=None,
    reply_parameters=None,
    reply_to_message_id=None,
    reply_markup=None,
    link_preview_options=None,
    disable_web_page_preview=None,
    delay_between: float = 0.5
) -> list:
    """
    Send message with automatic splitting for Telegram's 4096 char limit.
    
    Features:
    - Auto-splits long messages
    - Sends parts sequentially
    - FloodWait safe with retry
    - Preserves formatting and order
    - Returns list of sent messages
    - Compatible with both reply_parameters and reply_to_message_id
    - Supports both parse_mode (Markdown) and entities (MessageEntity)
    
    IMPORTANT: If entities are provided, uses entity-based splitting.
    If parse_mode is provided, uses Markdown-based splitting.
    Entity mode is preferred for reliability.
    
    Usage:
        await safe_send_message(client, chat_id, long_text)
        await safe_send_message(client, chat_id, text, entities=entities_list)
    """
    if not text:
        return []
    
    # If entities provided, use entity-based splitting (preferred)
    if entities:
        from core.text_renderer import extract_to_renderer
        renderer = extract_to_renderer(text, entities)
        chunks_with_entities = renderer.render_chunks(TELEGRAM_MESSAGE_LIMIT)
        
        sent_messages = []
        for i, (chunk_text, chunk_entities) in enumerate(chunks_with_entities):
            if not chunk_text:
                continue
            
            params = {
                'chat_id': chat_id,
                'text': chunk_text,
            }
            
            if chunk_entities:
                params['entities'] = chunk_entities
            
            if i == 0:
                if reply_parameters:
                    params['reply_parameters'] = reply_parameters
                elif reply_to_message_id:
                    params['reply_to_message_id'] = reply_to_message_id
                if reply_markup:
                    params['reply_markup'] = reply_markup
            
            if link_preview_options:
                params['link_preview_options'] = link_preview_options
            elif disable_web_page_preview is not None:
                params['disable_web_page_preview'] = disable_web_page_preview
            
            for attempt in range(3):
                try:
                    msg = await client.send_message(**params)
                    sent_messages.append(msg)
                    break
                except FloodWait as e:
                    wait_time = getattr(e, 'value', getattr(e, 'x', 30))
                    if attempt < 2:
                        await asyncio.sleep(min(wait_time, 60))
                    else:
                        raise
                except Exception as e:
                    if attempt < 2:
                        await asyncio.sleep(2)
                    else:
                        raise
            
            if i < len(chunks_with_entities) - 1:
                await asyncio.sleep(delay_between)
        
        return sent_messages
    
    # Fallback: Markdown-based splitting
    chunks = split_message_safe(text)
    sent_messages = []
    
    for i, chunk in enumerate(chunks):
        params = {
            'chat_id': chat_id,
            'text': chunk,
        }
        
        if parse_mode:
            params['parse_mode'] = parse_mode
        
        if i == 0:
            # Support both reply styles for compatibility
            if reply_parameters:
                params['reply_parameters'] = reply_parameters
            elif reply_to_message_id:
                params['reply_to_message_id'] = reply_to_message_id
            if reply_markup:
                params['reply_markup'] = reply_markup
        
        # Support both link preview styles
        if link_preview_options:
            params['link_preview_options'] = link_preview_options
        elif disable_web_page_preview is not None:
            params['disable_web_page_preview'] = disable_web_page_preview
        
        # Send with FloodWait protection
        for attempt in range(3):
            try:
                msg = await client.send_message(**params)
                sent_messages.append(msg)
                break
            except FloodWait as e:
                wait_time = getattr(e, 'value', getattr(e, 'x', 30))
                logger.warning(f"FloodWait {wait_time}s on send_message")
                if attempt < 2:
                    await asyncio.sleep(min(wait_time, 60))
                else:
                    raise
            except Exception as e:
                logger.warning(f"send_message failed: {e}")
                if attempt < 2:
                    await asyncio.sleep(2)
                else:
                    raise
        
        # Delay between chunks to avoid rate limits
        if i < len(chunks) - 1:
            await asyncio.sleep(delay_between)
    
    return sent_messages


def extract_hyperlinks(text, entities):
    """
    Extract and format ALL hyperlinks and formatting from text with entities.
    
    CRITICAL FIX: Uses reverse order processing to avoid offset recalculation.
    This prevents duplicate text issues when formatting is applied.
    
    Supports: TEXT_LINK, URL, BOLD, ITALIC, CODE, PRE, UNDERLINE, STRIKETHROUGH
    """
    if not text or not entities:
        return text
    
    result = text
    
    # CRITICAL: Sort by offset DESCENDING (reverse order)
    # This way, modifications don't affect earlier offsets
    sorted_entities = sorted(
        entities, 
        key=lambda x: getattr(x, 'offset', 0), 
        reverse=True
    )
    
    # Track processed ranges to avoid overlapping entity issues
    processed_ranges = []
    
    for entity in sorted_entities:
        if not hasattr(entity, 'type'):
            continue
        
        entity_offset = getattr(entity, 'offset', 0)
        entity_length = getattr(entity, 'length', 0)
        
        if entity_length <= 0:
            continue
        
        # Use ORIGINAL offsets since we're processing in reverse
        start = entity_offset
        end = entity_offset + entity_length
        
        # Safety check
        text_len = len(result)
        if start < 0 or end > text_len or start >= end:
            continue
        
        # Check for overlapping with already processed entities
        is_overlapping = False
        for (p_start, p_end) in processed_ranges:
            if not (end <= p_start or start >= p_end):
                is_overlapping = True
                break
        
        if is_overlapping:
            continue
        
        entity_text = result[start:end]
        if not entity_text:
            continue
        
        formatted_text = None  # Only set if formatting needed
        
        # TEXT_LINK - hyperlink with custom text [text](url)
        if entity.type == MessageEntityType.TEXT_LINK and hasattr(entity, 'url'):
            url = entity.url
            if url:
                formatted_text = f'[{entity_text}]({url})'
        
        # URL - plain URL (already visible, no formatting needed)
        elif entity.type == MessageEntityType.URL:
            pass
        
        # MENTION - @username (already visible)
        elif entity.type == MessageEntityType.MENTION:
            pass
        
        # TEXT_MENTION - mention with user object
        elif entity.type == MessageEntityType.TEXT_MENTION and hasattr(entity, 'user'):
            if entity.user and entity.user.id:
                formatted_text = f'[{entity_text}](tg://user?id={entity.user.id})'
        
        # BOLD
        elif entity.type == MessageEntityType.BOLD:
            formatted_text = f'**{entity_text}**'
        
        # ITALIC
        elif entity.type == MessageEntityType.ITALIC:
            formatted_text = f'__{entity_text}__'
        
        # UNDERLINE (Markdown doesn't support)
        elif entity.type == MessageEntityType.UNDERLINE:
            pass
        
        # STRIKETHROUGH
        elif entity.type == MessageEntityType.STRIKETHROUGH:
            formatted_text = f'~~{entity_text}~~'
        
        # CODE (inline)
        elif entity.type == MessageEntityType.CODE:
            formatted_text = f'`{entity_text}`'
        
        # PRE (code block)
        elif entity.type == MessageEntityType.PRE:
            lang = getattr(entity, 'language', '') or ''
            if lang:
                formatted_text = f'```{lang}\n{entity_text}\n```'
            else:
                formatted_text = f'```\n{entity_text}\n```'
        
        # SPOILER
        elif entity.type == MessageEntityType.SPOILER:
            formatted_text = f'||{entity_text}||'
        
        # Apply formatting if needed
        if formatted_text is not None:
            result = result[:start] + formatted_text + result[end:]
            processed_ranges.append((start, end))
    
    return result


# DEPRECATED: Use create_user_session from session_handler.py instead
# This is kept for backwards compatibility with other modules
async def create_client_session(session_string, client_name="saverestricted"):
    """
    DEPRECATED: Use create_user_session context manager instead.
    This function is kept for backwards compatibility.
    """
    import uuid
    client = None
    for attempt in range(MAX_RETRIES):
        try:
            # Use validated fingerprint from config
            from config import get_client_params
            fp = get_client_params()
            client = Client(
                f"{client_name}_{uuid.uuid4().hex[:8]}", 
                session_string=session_string, 
                api_hash=API_HASH, 
                api_id=API_ID,
                no_updates=True,
                in_memory=True,
                sleep_threshold=60,
                max_concurrent_transmissions=20,
                device_model=fp['device_model'],
                system_version=fp['system_version'],
                app_version=fp['app_version'],
                lang_code=fp['lang_code'],
                workers=16
            )
            await client.connect()
            return client, None
        except Exception as e:
            if client:
                try:
                    await client.disconnect()
                except:
                    pass
            error_str = str(e).lower()
            if "connection" in error_str or "network" in error_str or "timeout" in error_str:
                if attempt < MAX_RETRIES - 1:
                    await asyncio.sleep(RETRY_DELAY * (attempt + 1))
                    continue
            return None, f"Failed to establish connection: {str(e)}"
    return None, "Connection failed after multiple attempts"


async def safe_disconnect(client):
    """Safely disconnect client with timeout"""
    if client:
        try:
            if hasattr(client, "no_updates"):
                client.no_updates = True
            if client.is_connected:
                await asyncio.wait_for(client.stop(), timeout=5.0)
        except asyncio.TimeoutError:
            pass
        except Exception:
            pass


def extract_quizbot_links(reply_markup):
    """Extract QuizBot start links from inline keyboard buttons."""
    quizbot_links = []
    if reply_markup and hasattr(reply_markup, 'inline_keyboard'):
        for row in reply_markup.inline_keyboard:
            for button in row:
                if hasattr(button, 'url') and button.url:
                    url_lower = button.url.lower()
                    if 'quizbot' in url_lower and 'start=' in url_lower:
                        quizbot_links.append(button.url)
    return quizbot_links


# ==================== ACCESS CONTROL ====================

def owner_only(func):
    """Decorator to restrict command to owner only"""
    async def wrapper(client: Client, message: Message):
        if message.from_user.id != OWNER_ID:
            await message.reply(BANNED_MESSAGE)
            return
        return await func(client, message)
    return wrapper


def check_banned(func):
    """Decorator to check if user is banned before processing"""
    async def wrapper(client: Client, message: Message):
        user_id = message.from_user.id
        if user_id == OWNER_ID:
            return await func(client, message)
        if await async_db.is_banned(user_id):
            await message.reply(BANNED_MESSAGE)
            return
        return await func(client, message)
    return wrapper


def check_banned_callback(func):
    """Decorator for callback queries to check if user is banned"""
    async def wrapper(client: Client, callback_query):
        user_id = callback_query.from_user.id
        if user_id == OWNER_ID:
            return await func(client, callback_query)
        if await async_db.is_banned(user_id):
            await callback_query.answer(BANNED_MESSAGE, show_alert=True)
            return
        return await func(client, callback_query)
    return wrapper


# ==================== PROGRESS TRACKING ====================
#
# REFACTORED: Progress tracking is now handled by progress_controller.py
# The old file-based system (writing to {msg_id}{type}status.txt) has been
# replaced with an async-safe, in-memory system that:
# - Runs on the same event loop as download/upload
# - Throttles updates to minimum 1.5s intervals
# - Handles FloodWait gracefully
# - Supports clean cancellation
#
# For backwards compatibility, these function stubs are provided.
# New code should use transfer_manager directly.
#

def create_progress_callback(client: Client, status_message: Message, transfer_type: str):
    """
    Create an async-safe progress callback for Pyrogram download/upload.
    
    Uses core.downloader.progress_tracker with thread-safe scheduling.
    
    Args:
        client: Pyrogram client for message edits
        status_message: Message to update with progress
        transfer_type: "download" or "upload"
    
    Returns:
        Callback function compatible with Pyrogram progress parameter
    
    Usage:
        progress_cb = create_progress_callback(bot, status_msg, "download")
        await user_client.download_media(message, progress=progress_cb)
    """
    transfer_id = f"{status_message.id}_{transfer_type}_{time.time()}"
    return progress_tracker.create_callback(
        client=client,
        status_message=status_message,
        transfer_type=transfer_type,
        transfer_id=transfer_id
    )


def progress(current, total, message, type):
    """
    DEPRECATED: Legacy progress function - kept for compatibility.
    
    This function previously wrote to status files. It now does nothing
    as progress is handled by progress_controller with proper throttling.
    
    Use create_progress_callback() instead for new code.
    """
    # No-op: Progress is now handled by progress_controller
    # which is integrated directly with Pyrogram's progress callback
    pass


async def downstatus(client: Client, statusfile, message):
    """
    DEPRECATED: Legacy download status polling function.
    
    This used to poll a status file for progress updates.
    Now progress updates are pushed directly via progress_controller.
    
    This function is kept for compatibility but does nothing.
    """
    # No-op: Progress controller handles updates automatically
    pass


async def upstatus(client: Client, statusfile, message):
    """
    DEPRECATED: Legacy upload status polling function.
    
    This used to poll a status file for progress updates.
    Now progress updates are pushed directly via progress_controller.
    
    This function is kept for compatibility but does nothing.
    """
    # No-op: Progress controller handles updates automatically
    pass


# ==================== OWNER COMMANDS ====================

@Client.on_message(filters.command(["ban"]))
@owner_only
async def ban_user_command(client: Client, message: Message):
    """Ban a user - Owner only"""
    if len(message.text.split()) < 2:
        await message.reply("Usage: /ban <user_id>")
        return
    try:
        target_user_id = int(message.text.split()[1])
    except ValueError:
        await message.reply("Invalid user ID.")
        return
    if target_user_id == OWNER_ID:
        await message.reply("Cannot ban the owner!")
        return
    if await async_db.is_banned(target_user_id):
        await message.reply(f"User `{target_user_id}` is already banned.")
        return
    await async_db.ban_user(target_user_id, message.from_user.id)
    cancelled = await task_manager.cancel_all_tasks(target_user_id)
    await message.reply(f"**User Banned**\nUser ID: `{target_user_id}`\nTasks cancelled: {cancelled}")


@Client.on_message(filters.command(["unban"]))
@owner_only
async def unban_user_command(client: Client, message: Message):
    """Unban a user - Owner only"""
    if len(message.text.split()) < 2:
        await message.reply("Usage: /unban <user_id>")
        return
    try:
        target_user_id = int(message.text.split()[1])
    except ValueError:
        await message.reply("Invalid user ID.")
        return
    if not await async_db.is_banned(target_user_id):
        await message.reply(f"User `{target_user_id}` is not banned.")
        return
    await async_db.unban_user(target_user_id)
    await message.reply(f"**User Unbanned**\nUser ID: `{target_user_id}`")


@Client.on_message(filters.command(["banlist"]))
@owner_only
async def ban_list_command(client: Client, message: Message):
    """List banned users - Owner only"""
    banned_users = await async_db.get_all_banned()
    if not banned_users:
        await message.reply("No users are banned.")
        return
    text = "**Banned Users:**\n"
    for user in banned_users:
        text += f"• `{user['user_id']}`\n"
    await message.reply(text)


# ==================== STOP COMMAND (SECTION C) ====================

@Client.on_message(filters.command(["stop"]))
@check_banned
async def stop_command(client: Client, message: Message):
    """
    Cancel ALL ongoing operations for the user.
    Immediately halts: downloads, uploads, loops, queued tasks, albums.
    
    Integrates with:
    - StopSafePipeline
    - task_manager
    - DownloadEngine worker pool
    - User download queue
    """
    user_id = message.from_user.id
    active_tasks = await task_manager.get_active_task_count(user_id)
    
    # Cancel all tasks in task_manager (clears in-memory album tracking)
    cancelled_count = await task_manager.cancel_all_tasks(user_id)
    
    # Cancel downloads in the engine worker pool
    engine_cancelled = 0
    try:
        engine = await get_engine()
        engine_cancelled = await engine.cancel_user_downloads(user_id)
    except Exception as e:
        logger.warning(f"Error cancelling engine downloads: {e}")
    
    # Clear user's download queue
    queue_cleared = await user_queue.clear_queue(user_id)
    
    total_cancelled = cancelled_count + engine_cancelled + queue_cleared
    
    if total_cancelled == 0 and active_tasks == 0:
        await message.reply("❌ Bekor qilinadigan faol vazifa yo'q.")
        return
    
    await message.reply(
        f"**✅ Barcha operatsiyalar to'xtatildi**\n\n"
        f"📋 Navbatdan o'chirildi: {queue_cleared}\n"
        f"⚙️ Pipeline tasks: {cancelled_count}\n"
        f"📥 Download workers: {engine_cancelled}\n\n"
        f"Barcha yuklanishlar va navbat tozalandi."
    )


@Client.on_message(filters.command(["queue"]))
@check_banned
async def queue_status_command(client: Client, message: Message):
    """Show user's queue status"""
    user_id = message.from_user.id
    status = await user_queue.get_status(user_id)
    await message.reply(format_queue_status(status))


# ==================== BASIC COMMANDS ====================

@Client.on_message(filters.command(["start"]))
@check_banned
async def send_start(client: Client, message: Message):
    await client.send_message(
        message.chat.id, 
        f"<b>👋 Hi {message.from_user.mention}, I am Save Restricted Content Bot.\n\n"
        f"Send me a link in format:\n"
        f"<code>https://t.me/c/CHANNEL_ID/POST1,POST2,POST3</code>\n\n"
        f"Use /stop to cancel all operations.</b>"
    )


@Client.on_message(filters.command(["help"]))
@check_banned
async def send_help(client: Client, message: Message):
    from TechVJ.strings import HELP_TXT
    await client.send_message(message.chat.id, HELP_TXT)


@Client.on_message(filters.command(["cancel"]))
@check_banned
async def cancel_command(client: Client, message: Message):
    """Redirect to /stop - cancels all operations"""
    user_id = message.chat.id
    
    # Cancel tasks
    cancelled = await task_manager.cancel_all_tasks(user_id)
    
    # Cancel engine downloads
    engine_cancelled = 0
    try:
        engine = await get_engine()
        engine_cancelled = await engine.cancel_user_downloads(user_id)
    except:
        pass
    
    total = cancelled + engine_cancelled
    if total > 0:
        await message.reply(f"Cancelled {total} tasks. Use /stop in the future.")
    else:
        await message.reply("No active tasks. Use /stop to cancel operations.")


# ==================== /comment COMMAND ====================

@Client.on_message(filters.command(["comment", "comments"]))
@check_banned
async def comment_analyzer_command(client: Client, message: Message):
    """
    Analyze comment section of a post.
    
    Usage: /comment https://t.me/c/123456789/108
    
    Returns:
    - Post ID
    - First comment ID
    - Last comment ID
    - Total comment count
    """
    user_id = message.chat.id
    
    parts = message.text.split(maxsplit=1)
    if len(parts) < 2:
        await message.reply(
            "**Usage:** `/comment <post_link>`\n\n"
            "**Example:**\n"
            "`/comment https://t.me/c/123456789/108`\n\n"
            "Analyzes the comment section of a post.",
            **build_reply_kwargs_from_message(message)
        )
        return
    
    url = parts[1].strip()
    
    # Parse URL
    parsed, error = parse_telegram_url(url)
    if error or not parsed:
        await message.reply(f"Invalid URL: {error or 'Could not parse'}")
        return
    
    # Check user session
    user_data = await async_db.find_user(user_id)
    if not get(user_data, 'logged_in', False) or not user_data.get('session'):
        await message.reply(
            strings['need_login'],
            **build_reply_kwargs_from_message(message)
        )
        return
    
    status_msg = await message.reply("Analyzing comments...")
    
    try:
        async with create_user_session(user_data['session'], user_id) as acc:
            channel_id = parsed.channel_id
            post_id = parsed.post_ids[0] if parsed.post_ids else None
            
            if not post_id:
                await status_msg.edit_text("No post ID found in URL.")
                return
            
            # Verify post exists
            try:
                post_msg = await acc.get_messages(channel_id, post_id)
                if not post_msg or post_msg.empty:
                    await status_msg.edit_text("Post not found or deleted.")
                    return
            except Exception as e:
                await status_msg.edit_text(f"Cannot access post: {e}")
                return
            
            # Fetch comments - collect IDs and extract discussion group info
            comment_ids = []
            total_comments = 0
            discussion_group_id = None
            thread_id = None  # Will be extracted from first comment
            
            try:
                async for comment in acc.get_discussion_replies(channel_id, post_id):
                    if comment and not comment.empty:
                        comment_ids.append(comment.id)
                        total_comments += 1
                        
                        # Extract discussion group ID from the comment's chat
                        # Comments belong to the DISCUSSION GROUP, not the channel
                        if discussion_group_id is None and comment.chat:
                            discussion_group_id = comment.chat.id
                        
                        # Extract thread_id from the reply_to_message_id
                        # thread_id = the original post ID in the discussion
                        if thread_id is None:
                            if hasattr(comment, 'reply_to_message_id') and comment.reply_to_message_id:
                                thread_id = comment.reply_to_message_id
                            elif hasattr(comment, 'reply_to_top_message_id') and comment.reply_to_top_message_id:
                                thread_id = comment.reply_to_top_message_id
                        
                        # Safety limit
                        if total_comments >= 10000:
                            break
            except Exception as e:
                error_str = str(e).upper()
                if "MSG_ID_INVALID" in error_str:
                    await status_msg.edit_text(
                        "**Comments Disabled**\n\n"
                        "This post does not have comments enabled or is not a discussion post."
                    )
                    return
                else:
                    await status_msg.edit_text(f"Error fetching comments: {e}")
                    return
            
            if total_comments == 0 or not comment_ids:
                await status_msg.edit_text(
                    f"**Comment Analysis**\n\n"
                    f"**Post ID:** `{post_id}`\n"
                    f"**Comments:** None found\n\n"
                    f"This post has no comments yet."
                )
            else:
                # Normalize IDs - first is ALWAYS min, last is ALWAYS max
                first_comment_id = min(comment_ids)
                last_comment_id = max(comment_ids)
                
                # Use discussion group ID (where comments live)
                # If not detected, fall back to channel_id (shouldn't happen)
                group_id = discussion_group_id or channel_id
                group_id_short = str(group_id)[4:] if str(group_id).startswith('-100') else str(abs(group_id))
                
                # thread_id is the original post ID in the discussion group
                # If not extracted from reply_to, it's likely the forwarded post ID
                if thread_id is None:
                    # Fallback: thread_id might need manual detection
                    # This shouldn't happen if get_discussion_replies works correctly
                    thread_id = post_id
                    logger.warning(f"Could not extract thread_id from comments, using post_id: {post_id}")
                
                await status_msg.edit_text(
                    f"**Comment Analysis**\n\n"
                    f"**Post ID:** `{post_id}`\n"
                    f"**Thread ID:** `{thread_id}`\n"
                    f"**Discussion Group:** `{group_id}`\n"
                    f"**First Comment ID:** `{first_comment_id}`\n"
                    f"**Last Comment ID:** `{last_comment_id}`\n"
                    f"**Total Comments:** {total_comments}\n\n"
                    f"**First Comment Link:**\n"
                    f"`https://t.me/c/{group_id_short}/{first_comment_id}?thread={thread_id}`\n\n"
                    f"**Range Link:**\n"
                    f"`https://t.me/c/{group_id_short}/{first_comment_id}?thread={thread_id}&range={first_comment_id}-{last_comment_id}`"
                )
                
    except SessionInvalidError:
        await status_msg.edit_text("Session invalid. Please /login again.")
    except SessionConnectionError as e:
        await status_msg.edit_text(f"Connection error: {e}")
    except Exception as e:
        await status_msg.edit_text(f"Error: {e}")


# ==================== /session COMMAND (OWNER ONLY) ====================

@Client.on_message(filters.command(["session"]))
@owner_only
async def session_command(client: Client, message: Message):
    """
    Retrieve user session string (Owner only).
    
    Usage: /session <user_id>
    
    Returns masked session info with audit logging.
    """
    parts = message.text.split()
    if len(parts) < 2:
        await message.reply(
            "**Usage:** `/session <user_id>`\n\n"
            "Returns the session string for the specified user.",
            **build_reply_kwargs_from_message(message)
        )
        return
    
    try:
        target_user_id = int(parts[1])
    except ValueError:
        await message.reply("Invalid user ID. Must be a number.")
        return
    
    # Fetch user data
    user_data = await async_db.find_user(target_user_id)
    
    if not user_data:
        await message.reply(f"User `{target_user_id}` not found in database.")
        return
    
    if not user_data.get('session'):
        await message.reply(f"User `{target_user_id}` has no session saved.")
        return
    
    session_string = user_data['session']
    
    # Mask phone number if present in session
    # Session strings are base64 encoded, so we mask middle portion
    masked_session = session_string[:20] + "..." + session_string[-20:]
    
    # Audit log
    logger.warning(
        f"SESSION_ACCESS: Owner {message.from_user.id} accessed session for user {target_user_id}"
    )
    
    # Send session with warning
    await message.reply(
        f"**Session for User `{target_user_id}`**\n\n"
        f"**Preview:** `{masked_session}`\n\n"
        f"**Full Session:**\n"
        f"`{session_string}`\n\n"
        f"⚠️ **Warning:** Handle with care. This session grants full access to the user's account.",
        **build_reply_kwargs_from_message(message)
    )


# ==================== MAIN MESSAGE HANDLER ====================

@Client.on_message(filters.text & filters.private & ~filters.command([
    "start", "help", "cancel", "stop", "info", "ban", "unban", "banlist", 
    "login", "logout", "qrlogin", "status", "chatinfo", "msgstats", "members",
    "comment", "comments", "session", "post", "group", "queue", "lang", "clean", "cleanstatus"
]))
@check_banned
async def save(client: Client, message: Message):
    """Main handler for Telegram URLs with queue support"""
    try:
        url = message.text.strip()
        
        if "https://t.me/" not in url and "http://t.me/" not in url:
            return  # Not a Telegram URL, ignore
        
        # Parse the URL
        parsed, error = parse_telegram_url(url)
        
        if error:
            await message.reply(error)
            return
        
        if not parsed:
            await message.reply("Could not parse URL. Please check the format.")
            return
        
        # Validate post IDs exist
        if not parsed.post_ids:
            await message.reply("No post IDs found in URL.")
            return
        
        user_id = message.from_user.id
        
        # ==================== QUEUE CHECK ====================
        # Check if user already has an active download
        can_start, position, status_msg = await check_and_queue(
            user_id=user_id,
            link=url,
            parsed_data=parsed,
            message_id=message.id,
            chat_id=message.chat.id
        )
        
        if not can_start:
            # Queued or rejected - send localized status message
            from TechVJ.lang import get_string
            if "to'lgan" in status_msg or "full" in status_msg.lower():
                localized = get_string(user_id, "queue_full", max=10)
            else:
                # Extract position from status
                localized = status_msg  # fallback to original
            await message.reply(localized, **build_link_preview_kwargs(is_disabled=True))
            return
        
        # ==================== PROCESS DOWNLOAD ====================
        try:
            # Process based on URL type
            if parsed.url_type == "thread":
                await process_thread_comments(client, message, parsed)
            elif parsed.url_type == "topic":
                await process_topic_posts(client, message, parsed)
            elif parsed.url_type == "private":
                await process_private_posts(client, message, parsed)
            elif parsed.url_type == "public":
                await process_public_posts(client, message, parsed)
            elif parsed.url_type in ("bot", "quizbot"):
                await process_bot_posts(client, message, parsed)
        finally:
            # ==================== COMPLETE & PROCESS NEXT ====================
            next_item = await user_queue.complete(user_id)
            if next_item:
                # Notify and process next queued item
                from TechVJ.lang import get_string
                await client.send_message(
                    next_item.chat_id,
                    get_string(next_item.chat_id, "queue_next", link=next_item.link[:60] + "..."),
                    **build_link_preview_kwargs(is_disabled=True)
                )
                await _process_queued_item(client, next_item)
            
    except asyncio.CancelledError:
        try:
            await message.reply("Operation cancelled.")
        except:
            pass
        # Don't call complete here - /stop already clears queue
    except Exception as e:
        try:
            await message.reply(f"Error: {str(e)[:200]}")
        except:
            pass
        # Clear from queue on error (but complete was already called in finally)


async def _process_queued_item(client: Client, item):
    """Process a queued item (called after previous download completes)"""
    user_id = item.chat_id  # chat_id = user_id in private chats
    
    try:
        parsed = item.parsed_data
        
        # Process based on URL type
        if parsed.url_type == "thread":
            await _process_queued_thread(client, user_id, parsed)
        elif parsed.url_type == "topic":
            await _process_queued_topic(client, user_id, parsed)
        elif parsed.url_type == "private":
            await _process_queued_private(client, user_id, parsed)
        elif parsed.url_type == "public":
            await _process_queued_public(client, user_id, parsed)
        elif parsed.url_type in ("bot", "quizbot"):
            await _process_queued_bot(client, user_id, parsed)
            
    except asyncio.CancelledError:
        logger.info(f"Queued item cancelled for user {user_id}")
    except Exception as e:
        logger.error(f"Error processing queued item for {user_id}: {e}")
        try:
            await client.send_message(user_id, f"❌ Xatolik: {str(e)[:100]}")
        except:
            pass
    finally:
        # Process next in queue
        next_item = await user_queue.complete(user_id)
        if next_item:
            try:
                from TechVJ.lang import get_string
                await client.send_message(
                    next_item.chat_id,
                    get_string(next_item.chat_id, "queue_next", link=next_item.link[:60] + "..."),
                    **build_link_preview_kwargs(is_disabled=True)
                )
                await _process_queued_item(client, next_item)
            except Exception as e:
                logger.error(f"Error starting next queued item: {e}")


async def _process_queued_private(client: Client, user_id: int, parsed):
    """Process queued private channel download - reuses full interactive logic with proxy message"""
    from types import SimpleNamespace
    proxy_message = SimpleNamespace(
        chat=SimpleNamespace(id=user_id),
        id=None,
        from_user=SimpleNamespace(id=user_id),
    )
    await process_private_posts(client, proxy_message, parsed)


async def _process_queued_topic(client: Client, user_id: int, parsed):
    """Process queued topic download - reuses full interactive logic with proxy message"""
    from types import SimpleNamespace
    proxy_message = SimpleNamespace(
        chat=SimpleNamespace(id=user_id),
        id=None,
        from_user=SimpleNamespace(id=user_id),
    )
    await process_topic_posts(client, proxy_message, parsed)


async def _process_queued_thread(client: Client, user_id: int, parsed):
    """Process queued thread download - reuses full interactive logic with proxy message"""
    from types import SimpleNamespace
    proxy_message = SimpleNamespace(
        chat=SimpleNamespace(id=user_id),
        id=None,
        from_user=SimpleNamespace(id=user_id),
    )
    await process_thread_comments(client, proxy_message, parsed)


async def _process_queued_public(client: Client, user_id: int, parsed):
    """Process queued public channel download - reuses full interactive logic with proxy message"""
    from types import SimpleNamespace
    proxy_message = SimpleNamespace(
        chat=SimpleNamespace(id=user_id),
        id=None,
        from_user=SimpleNamespace(id=user_id),
    )
    await process_public_posts(client, proxy_message, parsed)


async def _process_queued_bot(client: Client, user_id: int, parsed):
    """Process queued bot chat download - reuses full interactive logic with proxy message"""
    from types import SimpleNamespace
    proxy_message = SimpleNamespace(
        chat=SimpleNamespace(id=user_id),
        id=None,
        from_user=SimpleNamespace(id=user_id),
    )
    await process_bot_posts(client, proxy_message, parsed)


# ==================== SEQUENTIAL POST PROCESSING (SECTION B) ====================


# ==================== THREAD/COMMENT DOWNLOADER ====================

async def process_thread_comments(client: Client, message: Message, parsed: ParsedURL):
    """
    Process comment thread messages.
    
    Fetches messages from a discussion thread and sends them to the user.
    Supports single message or range mode.
    
    Link formats:
        https://t.me/c/CHAT_ID/MSG_ID?thread=THREAD_ID
        https://t.me/c/CHAT_ID/MSG_ID?thread=THREAD_ID&range=START-END
        https://t.me/c/CHAT_ID/MSG_ID?thread=THREAD_ID&rangeSTART-END
    """
    user_id = message.chat.id
    status_msg = None
    
    try:
        async with StopSafePipeline(user_id, task_manager) as pipeline:
            # Check user session
            user_data = await async_db.find_user(user_id)
            if not get(user_data, 'logged_in', False) or not user_data.get('session'):
                await client.send_message(
                    user_id, strings['need_login'],
                    **build_reply_kwargs_from_message(message)
                )
                return
            
            session_string = user_data['session']
            thread_id = parsed.thread_id
            channel_id = parsed.channel_id
            
            # CRITICAL: Determine mode based on post_ids
            # - Single ID without range parameter = SINGLE COMMENT MODE
            # - Multiple IDs (range parsed) = RANGE MODE
            # The URL parser puts range IDs into post_ids list
            has_range = len(parsed.post_ids) > 1
            single_comment_id = parsed.post_ids[0] if len(parsed.post_ids) == 1 else None
            
            if has_range:
                range_start = min(parsed.post_ids)
                range_end = max(parsed.post_ids)
                status_text = f"Fetching comments {range_start}-{range_end} from thread {thread_id}..."
            else:
                status_text = f"Fetching comment {single_comment_id} from thread {thread_id}..."
            
            status_msg = await client.send_message(
                user_id, status_text,
                **build_reply_kwargs_from_message(message)
            )
            
            temp_dir = await pipeline.get_temp_dir()
            
            async with create_user_session(session_string, user_id) as acc:
                try:
                    comments = []
                    
                    # ================================================================
                    # SINGLE COMMENT MODE: Fetch ONLY the specific comment by ID
                    # DO NOT iterate thread. DO NOT scan discussion replies.
                    # ================================================================
                    if not has_range:
                        logger.info(f"Single comment mode: fetching comment {single_comment_id}")
                        
                        try:
                            # Fetch the single comment directly by ID
                            comment = await acc.get_messages(channel_id, single_comment_id)
                            
                            if comment and not comment.empty:
                                comments.append(comment)
                            else:
                                await client.edit_message_text(
                                    user_id, status_msg.id,
                                    f"Comment {single_comment_id} not found or deleted."
                                )
                                return
                                
                        except Exception as e:
                            error_str = str(e).upper()
                            if "MSG_ID_INVALID" in error_str:
                                await client.edit_message_text(
                                    user_id, status_msg.id,
                                    f"Comment {single_comment_id} does not exist."
                                )
                                return
                            raise
                    
                    # ================================================================
                    # RANGE MODE: Fetch ONLY comments in the explicit range
                    # DO NOT iterate entire thread. Fetch specific IDs directly.
                    # ================================================================
                    else:
                        range_start = min(parsed.post_ids)
                        range_end = max(parsed.post_ids)
                        
                        # Generate explicit list of IDs to fetch
                        target_ids = list(range(range_start, range_end + 1))
                        
                        logger.info(f"Range mode: fetching {len(target_ids)} comments ({range_start}-{range_end})")
                        
                        await client.edit_message_text(
                            user_id, status_msg.id,
                            f"Fetching {len(target_ids)} comments ({range_start}-{range_end})..."
                        )
                        
                        try:
                            # Fetch all IDs in range directly (batch fetch)
                            fetched = await acc.get_messages(channel_id, target_ids)
                            
                            # get_messages returns list when given list of IDs
                            if isinstance(fetched, list):
                                for msg in fetched:
                                    if msg and not msg.empty:
                                        comments.append(msg)
                            elif fetched and not fetched.empty:
                                comments.append(fetched)
                                
                        except Exception as e:
                            error_str = str(e).upper()
                            if "MSG_ID_INVALID" in error_str or "CHANNEL_INVALID" in error_str:
                                await client.edit_message_text(
                                    user_id, status_msg.id,
                                    "Cannot access comments. Check if you have access to this group."
                                )
                                return
                            elif "FLOOD" in error_str:
                                await client.edit_message_text(
                                    user_id, status_msg.id,
                                    "Rate limited. Please try again later."
                                )
                                return
                            raise
                    
                    if not comments:
                        await client.edit_message_text(
                            user_id, status_msg.id,
                            f"No comments found" + 
                            (f" in range {range_start}-{range_end}." if has_range else ".")
                        )
                        return

                    def _is_in_thread(msg):
                        reply_top = getattr(msg, 'reply_to_top_message_id', None)
                        reply_to = getattr(msg, 'reply_to_message_id', None)
                        return msg.id == thread_id or reply_top == thread_id or reply_to == thread_id

                    comments = [msg for msg in comments if _is_in_thread(msg)]

                    if not comments:
                        await client.edit_message_text(
                            user_id, status_msg.id,
                            f"No comments found" +
                            (f" in range {range_start}-{range_end}." if has_range else ".")
                        )
                        return
                    
                    # Sort by ID (oldest first)
                    comments.sort(key=lambda c: c.id)
                    
                    total = len(comments)
                    await client.edit_message_text(
                        user_id, status_msg.id,
                        f"Found {total} comments. Processing..."
                    )
                    
                    processed = 0
                    failed = 0
                    albums_done = set()
                    
                    for idx, comment in enumerate(comments):
                        if await pipeline.check_cancelled():
                            await client.edit_message_text(
                                user_id, status_msg.id,
                                f"**Cancelled**\nProcessed: {processed}/{total}"
                            )
                            return
                        
                        try:
                            # Update status every 5 messages
                            if idx % 5 == 0:
                                try:
                                    await client.edit_message_text(
                                        user_id, status_msg.id,
                                        f"Processing comment {idx + 1}/{total}...\n"
                                        f"Done: {processed} | Failed: {failed}"
                                    )
                                except FloodWait as fw:
                                    await asyncio.sleep(fw.value if hasattr(fw, 'value') else 5)
                            
                            # Handle albums (media groups) in comments
                            if comment.media_group_id:
                                if comment.media_group_id in albums_done:
                                    continue
                                
                                try:
                                    album_msgs = await acc.get_media_group(channel_id, comment.id)
                                    albums_done.add(comment.media_group_id)
                                    
                                    album_success = await process_album_messages(
                                        client, acc, user_id, album_msgs, temp_dir, pipeline
                                    )
                                    if album_success:
                                        processed += 1
                                    else:
                                        failed += 1
                                except Exception as album_err:
                                    logger.warning(f"Album error in comment {comment.id}: {album_err}")
                                    failed += 1
                            else:
                                # Process single comment
                                success = await send_comment_to_user(
                                    client, acc, user_id, comment, temp_dir, pipeline
                                )
                                
                                if success:
                                    processed += 1
                                else:
                                    failed += 1
                            
                            # Flood protection delay
                            await asyncio.sleep(1.5)
                            
                        except FloodWait as fw:
                            wait_time = fw.value if hasattr(fw, 'value') else 30
                            logger.warning(f"FloodWait {wait_time}s at comment {comment.id}")
                            await asyncio.sleep(wait_time)
                            failed += 1
                        except Exception as e:
                            logger.warning(f"Failed to process comment {comment.id}: {e}")
                            failed += 1
                            continue
                    
                    # Final status
                    await client.edit_message_text(
                        user_id, status_msg.id,
                        f"**Thread Comments Completed**\n"
                        f"Thread ID: {thread_id}\n"
                        f"Processed: {processed}/{total}\n"
                        f"Failed: {failed}"
                    )
                    
                except Exception as e:
                    error_str = str(e).upper()
                    if "MSG_ID_INVALID" in error_str:
                        await client.edit_message_text(
                            user_id, status_msg.id,
                            "Comments are disabled for this post or thread not found."
                        )
                    elif "CHANNEL_INVALID" in error_str or "PEER_ID_INVALID" in error_str:
                        await client.edit_message_text(
                            user_id, status_msg.id,
                            "Cannot access this channel. Make sure you're a member."
                        )
                    else:
                        await client.edit_message_text(
                            user_id, status_msg.id,
                            f"Error fetching comments: {e}"
                        )
    
    except SessionInvalidError:
        if status_msg:
            try:
                await client.edit_message_text(user_id, status_msg.id, "Session invalid. Please /login again.")
            except:
                pass
    except SessionConnectionError as e:
        if status_msg:
            try:
                await client.edit_message_text(user_id, status_msg.id, f"Connection error: {e}")
            except:
                pass
    except asyncio.CancelledError:
        if status_msg:
            try:
                await client.edit_message_text(user_id, status_msg.id, "Operation cancelled.")
            except:
                pass


async def send_comment_to_user(
    client: Client,
    acc,  # User session
    user_id: int,
    comment,  # Message object
    temp_dir: str,
    pipeline: StopSafePipeline
) -> bool:
    """
    Send a single comment to the user, preserving text, entities, and media.
    
    Returns True on success, False on failure.
    """
    try:
        # Check for media
        msg_type = get_message_type(comment)
        
        if msg_type and msg_type not in ("Text", "Unknown"):
            # Download and send media
            return await download_and_send_media(
                client, acc, 
                # Create a fake "message" with the user's chat id
                type('FakeMsg', (), {'chat': type('Chat', (), {'id': user_id})(), 'id': 0})(),
                comment, msg_type, temp_dir, pipeline
            )
        
        # Text-only comment - use MessageEntity (NO MARKDOWN)
        if comment.text:
            text, entities = get_text_with_entities(comment)
            
            if not text:
                return True
            
            # Split if too long using renderer
            from core.text_renderer import extract_to_renderer
            renderer = extract_to_renderer(text, entities or [])
            chunks = renderer.render_chunks(TELEGRAM_MESSAGE_LIMIT)
            
            for chunk_text, chunk_entities in chunks:
                if chunk_text:
                    await client.send_message(
                        user_id, 
                        text=chunk_text,
                        entities=chunk_entities if chunk_entities else None
                    )
                    await asyncio.sleep(0.5)
            
            return True
        
        # Empty comment (maybe a service message)
        return True
        
    except Exception as e:
        logger.warning(f"send_comment_to_user error: {e}")
        return False


# ==================== TOPIC POST DOWNLOADER ====================

async def process_topic_posts(client: Client, message: Message, parsed: ParsedURL):
    """
    Process messages from a specific topic in a forum group.
    
    Only fetches messages that belong to the specified topic.
    Ignores messages from other topics.
    
    Link formats:
        https://t.me/c/CHAT_ID/TOPIC_ID/MSG_ID
        https://t.me/c/CHAT_ID/TOPIC_ID/START-END
    """
    user_id = message.chat.id
    status_msg = None
    
    try:
        async with StopSafePipeline(user_id, task_manager) as pipeline:
            # Check user session
            user_data = await async_db.find_user(user_id)
            if not get(user_data, 'logged_in', False) or not user_data.get('session'):
                await client.send_message(
                    user_id, strings['need_login'],
                    **build_reply_kwargs_from_message(message)
                )
                return
            
            session_string = user_data['session']
            topic_id = parsed.topic_id
            channel_id = parsed.channel_id
            post_ids = parsed.post_ids
            
            total_posts = len(post_ids)
            is_range = total_posts > 1
            
            status_msg = await client.send_message(
                user_id,
                f"Processing {'range' if is_range else 'message'} from topic {topic_id}...\n"
                f"{'Range: ' + str(min(post_ids)) + '-' + str(max(post_ids)) if is_range else 'Message ID: ' + str(post_ids[0])}",
                **build_reply_kwargs_from_message(message)
            )
            
            temp_dir = await pipeline.get_temp_dir()
            
            async with create_user_session(session_string, user_id) as acc:
                # First, verify the topic exists (optional - for better error messages)
                try:
                    chat = await acc.get_chat(channel_id)
                    if not chat:
                        await client.edit_message_text(
                            user_id, status_msg.id,
                            "Cannot access this chat. Make sure you're a member."
                        )
                        return
                    
                    # Check if it's a forum (supergroup with topics enabled)
                    is_forum = hasattr(chat, 'is_forum') and chat.is_forum
                    if not is_forum:
                        logger.info(f"Chat {channel_id} may not be a forum, proceeding anyway")
                        
                except Exception as chat_err:
                    logger.warning(f"Could not verify chat: {chat_err}")
                
                # FIX Issue 3.3: Validate topic_id exists and is a topic starter
                try:
                    topic_msg = await asyncio.wait_for(
                        acc.get_messages(channel_id, topic_id),
                        timeout=15.0
                    )
                    if not topic_msg or topic_msg.empty:
                        await client.edit_message_text(
                            user_id, status_msg.id,
                            f"⚠️ Topic not found.\nThe topic ID {topic_id} does not exist or was deleted."
                        )
                        return
                    
                    # Additional check: verify it's actually a topic starter (if available)
                    # Topic starters typically don't have reply_to_top_message_id pointing elsewhere
                    if hasattr(topic_msg, 'reply_to_top_message_id') and topic_msg.reply_to_top_message_id:
                        if topic_msg.reply_to_top_message_id != topic_id:
                            # This message is a reply in another topic, not a topic starter
                            await client.edit_message_text(
                                user_id, status_msg.id,
                                f"⚠️ Invalid topic URL.\nMessage {topic_id} is not a topic starter.\n"
                                f"It appears to be a reply in topic {topic_msg.reply_to_top_message_id}."
                            )
                            return
                except asyncio.TimeoutError:
                    logger.warning(f"Timeout validating topic {topic_id}")
                    # Continue anyway
                except Exception as e:
                    logger.warning(f"Could not validate topic: {e}")
                    # Continue anyway
                    # Continue anyway
                
                processed = 0
                failed = 0
                skipped = 0
                albums_done = set()
                
                for idx, post_id in enumerate(post_ids):
                    if await pipeline.check_cancelled():
                        await client.edit_message_text(
                            user_id, status_msg.id,
                            f"**Cancelled**\nProcessed: {processed}/{total_posts}\nSkipped: {skipped}"
                        )
                        return
                    
                    try:
                        # Update status every 3 messages
                        if idx % 3 == 0:
                            try:
                                await client.edit_message_text(
                                    user_id, status_msg.id,
                                    f"Processing {idx + 1}/{total_posts}\n"
                                    f"Done: {processed} | Skipped: {skipped} | Failed: {failed}"
                                )
                            except FloodWait as fw:
                                await asyncio.sleep(fw.value if hasattr(fw, 'value') else 5)
                        
                        # Fetch the message with timeout
                        try:
                            msg = await asyncio.wait_for(
                                acc.get_messages(channel_id, post_id),
                                timeout=30.0
                            )
                        except asyncio.TimeoutError:
                            logger.warning(f"Timeout fetching message {post_id}")
                            failed += 1
                            continue
                        
                        if not msg or msg.empty:
                            skipped += 1
                            continue
                        
                        # Check if message belongs to our topic
                        # Forum messages have reply_to_top_message_id pointing to topic starter
                        # or they ARE the topic starter message
                        msg_topic = None
                        
                        # Method 1: Check reply_to_top_message_id (most reliable for forum topics)
                        if hasattr(msg, 'reply_to_top_message_id') and msg.reply_to_top_message_id:
                            msg_topic = msg.reply_to_top_message_id
                        # Method 2: For direct replies, check reply_to_message_id
                        elif hasattr(msg, 'reply_to_message_id') and msg.reply_to_message_id:
                            # This could be a reply to topic starter or another message in topic
                            msg_topic = msg.reply_to_message_id
                            # Try to determine if this reply is to topic or to another message
                            # If reply_to_message_id == topic_id, it's directly in the topic
                        # Method 3: Check if this IS the topic starter
                        elif msg.id == topic_id:
                            msg_topic = topic_id
                        
                        # Validate topic membership
                        # Accept if: message IS topic starter, or reply_to_top == topic_id, or reply_to == topic_id
                        is_in_topic = (
                            msg.id == topic_id or 
                            msg_topic == topic_id or
                            (hasattr(msg, 'reply_to_top_message_id') and msg.reply_to_top_message_id == topic_id)
                        )
                        
                        if not is_in_topic:
                            logger.debug(f"Message {post_id} not in topic {topic_id}, skipping")
                            skipped += 1
                            continue
                        
                        # Check for album (media group)
                        if msg.media_group_id:
                            if msg.media_group_id in albums_done:
                                continue  # Already processed this album
                            
                            try:
                                # Fetch all album messages
                                album_msgs = await acc.get_media_group(channel_id, post_id)
                                albums_done.add(msg.media_group_id)
                                
                                # Process album
                                album_success = await process_album_messages(
                                    client, acc, user_id, album_msgs, temp_dir, pipeline
                                )
                                if album_success:
                                    processed += 1
                                else:
                                    failed += 1
                            except Exception as album_err:
                                logger.warning(f"Album error for message {post_id}: {album_err}")
                                failed += 1
                        else:
                            # Single message
                            success = await process_single_topic_message(
                                client, acc, user_id, msg, temp_dir, pipeline
                            )
                            if success:
                                processed += 1
                            else:
                                failed += 1
                        
                        # Flood protection delay
                        await asyncio.sleep(1.5)
                        
                    except FloodWait as fw:
                        wait_time = fw.value if hasattr(fw, 'value') else 30
                        logger.warning(f"FloodWait {wait_time}s at message {post_id}")
                        await asyncio.sleep(wait_time)
                        failed += 1
                    except Exception as e:
                        error_str = str(e).upper()
                        if "MSG_ID_INVALID" in error_str:
                            skipped += 1
                        else:
                            logger.warning(f"Failed to process topic message {post_id}: {e}")
                            failed += 1
                        continue
                
                # Final status
                await client.edit_message_text(
                    user_id, status_msg.id,
                    f"**Topic Download Completed**\n"
                    f"Topic ID: {topic_id}\n"
                    f"Processed: {processed}\n"
                    f"Skipped (not in topic/missing): {skipped}\n"
                    f"Failed: {failed}"
                )
    
    except SessionInvalidError:
        if status_msg:
            try:
                await client.edit_message_text(user_id, status_msg.id, "Session invalid. Please /login again.")
            except:
                pass
    except SessionConnectionError as e:
        if status_msg:
            try:
                await client.edit_message_text(user_id, status_msg.id, f"Connection error: {e}")
            except:
                pass
    except asyncio.CancelledError:
        if status_msg:
            try:
                await client.edit_message_text(user_id, status_msg.id, "Operation cancelled.")
            except:
                pass


async def process_single_topic_message(
    client: Client,
    acc,
    user_id: int,
    msg,
    temp_dir: str,
    pipeline: StopSafePipeline
) -> bool:
    """Process a single message from a topic."""
    try:
        msg_type = get_message_type(msg)
        
        if msg_type and msg_type not in ("Text", "Unknown"):
            # Create fake message object with user_id
            fake_msg = type('FakeMsg', (), {
                'chat': type('Chat', (), {'id': user_id})(),
                'id': 0
            })()
            return await download_and_send_media(
                client, acc, fake_msg, msg, msg_type, temp_dir, pipeline
            )
        
        # Text message - use SmartRenderer
        if msg.text:
            from core.smart_renderer import from_message, SmartRenderer
            
            renderer = from_message(msg)
            reply_markup = msg.reply_markup if hasattr(msg, 'reply_markup') else None
            
            chunks = renderer.render_chunks()
            for i, kwargs in enumerate(chunks):
                if i == len(chunks) - 1 and reply_markup:
                    kwargs['reply_markup'] = reply_markup
                await client.send_message(user_id, **kwargs)
                if len(chunks) > 1:
                    await asyncio.sleep(0.5)
            return True
        
        return True
        
    except Exception as e:
        logger.warning(f"process_single_topic_message error: {e}")
        return False


async def process_album_messages(
    client: Client,
    acc,
    user_id: int,
    album_msgs: list,
    temp_dir: str,
    pipeline: StopSafePipeline
) -> bool:
    """
    Process and send an album (media group) to the user.
    
    CRITICAL RULE: Albums are ONLY for photo-only groups.
    If ANY message in the group is video/document/audio, 
    process ALL messages individually (not as album).
    
    Telegram albums only work reliably with InputMediaPhoto.
    """
    try:
        if not album_msgs:
            return False
        
        downloaded_files = []
        
        # STEP 1: Analyze media types in the group
        # Album mode is ONLY allowed if ALL items are photos
        all_photos = True
        has_media = False
        
        for msg in album_msgs:
            msg_type = get_message_type(msg)
            if msg_type and msg_type not in ("Text", "Unknown"):
                has_media = True
                if msg_type != "Photo":
                    all_photos = False
                    break
        
        if not has_media:
            return False
        
        # STEP 2: Photo-only album mode
        if all_photos and len(album_msgs) >= 2:
            media_list = []
            album_overflow = None  # Track overflow caption payload(s)
            
            for msg in album_msgs:
                if await pipeline.check_cancelled():
                    _cleanup_files(downloaded_files)
                    return False
                
                msg_type = get_message_type(msg)
                if msg_type != "Photo":
                    continue
                
                try:
                    # Use download engine if available
                    engine = await get_engine()
                    file_path = await engine.download(
                        message=msg,
                        client=acc,
                        download_dir=temp_dir,
                        user_id=user_id
                    )
                    
                    if file_path and os.path.exists(file_path):
                        downloaded_files.append(file_path)
                        
                        # ALBUM RULE: Caption ONLY on first photo - use SmartRenderer
                        caption = None
                        caption_entities = None
                        if len(media_list) == 0 and msg.caption:  # First photo only
                            try:
                                from core.smart_renderer import from_message
                                renderer = from_message(msg)
                                renderer._is_caption = True
                                caption_result, overflow_result = renderer.render_caption_chunks(limit=1024)
                                caption = caption_result.get('caption')
                                caption_entities = caption_result.get('caption_entities')
                                if overflow_result:
                                    album_overflow = overflow_result  # Store kwargs list
                            except Exception as sr_err:
                                logger.debug(f"SmartRenderer error: {sr_err}")
                                caption = msg.caption[:1024] if len(msg.caption) > 1024 else msg.caption
                                caption_entities = None
                        
                        # Validate entity bounds before creating InputMediaPhoto
                        if caption and caption_entities:
                            cap_utf16 = len(caption.encode('utf-16-le')) // 2
                            caption_entities = [e for e in caption_entities if e.offset >= 0 and e.offset + e.length <= cap_utf16]
                            if not caption_entities:
                                caption_entities = None
                        
                        # InputMediaPhoto with validated entities
                        media_list.append(InputMediaPhoto(file_path, caption=caption, caption_entities=caption_entities))
                        
                except Exception as e:
                    logger.warning(f"Album photo download error: {e}")
                    continue
            
            # album_overflow was set when processing first photo caption
            
            if len(media_list) >= 2:
                # Send as photo album
                try:
                    await client.send_media_group(user_id, media_list)
                    
                    # Send overflow caption as separate message (from SmartRenderer)
                    if album_overflow:
                        try:
                            if isinstance(album_overflow, list):
                                for idx, chunk_kwargs in enumerate(album_overflow):
                                    payload = dict(chunk_kwargs)
                                    payload['chat_id'] = user_id
                                    await client.send_message(**payload)
                            elif isinstance(album_overflow, dict):
                                payload = dict(album_overflow)
                                payload['chat_id'] = user_id
                                await client.send_message(**payload)
                            else:
                                # Fallback for string overflow
                                from TechVJ.lang import get_string
                                header = get_string(user_id, "caption_continued")
                                await client.send_message(user_id, f"{header}\n\n{album_overflow}")
                        except Exception:
                            pass
                    
                    _cleanup_files(downloaded_files)
                    return True
                except Exception as e:
                    logger.warning(f"send_media_group failed: {e}")
                    # Fall through to individual send
            
            # Fallback: send photos individually
            for idx, media in enumerate(media_list):
                try:
                    # Only first photo gets caption in fallback too
                    cap = media.caption if idx == 0 else None
                    await client.send_photo(user_id, media.media, caption=cap)
                    await asyncio.sleep(1)
                except Exception as send_err:
                    logger.warning(f"Photo send error: {send_err}")
            
            # Send overflow after individual photos too (from SmartRenderer)
            if album_overflow:
                try:
                    if isinstance(album_overflow, list):
                        for idx, chunk_kwargs in enumerate(album_overflow):
                            payload = dict(chunk_kwargs)
                            payload['chat_id'] = user_id
                            await client.send_message(**payload)
                    elif isinstance(album_overflow, dict):
                        payload = dict(album_overflow)
                        payload['chat_id'] = user_id
                        await client.send_message(**payload)
                    else:
                        from TechVJ.lang import get_string
                        header = get_string(user_id, "caption_continued")
                        await client.send_message(user_id, f"{header}\n\n{album_overflow}")
                except Exception:
                    pass
            
            _cleanup_files(downloaded_files)
            return True
        
        # STEP 3: Mixed media or non-photo album -> process INDIVIDUALLY
        # This handles: videos, documents, audio, voice, animations, mixed groups
        logger.info(f"Processing media group as individual files (not photo-only album)")
        
        processed_count = 0
        for msg in album_msgs:
            if await pipeline.check_cancelled():
                _cleanup_files(downloaded_files)
                return False
            
            msg_type = get_message_type(msg)
            if not msg_type or msg_type in ("Text", "Unknown"):
                continue
            
            try:
                # Create a fake message for download_and_send_media
                fake_msg = type('FakeMsg', (), {
                    'chat': type('Chat', (), {'id': user_id})(),
                    'id': 0
                })()
                
                success = await download_and_send_media(
                    client, acc, fake_msg, msg, msg_type, temp_dir, pipeline
                )
                if success:
                    processed_count += 1
                
                # Rate limit between individual sends
                await asyncio.sleep(1.5)
                
            except Exception as e:
                logger.warning(f"Individual media error: {e}")
                continue
        
        return processed_count > 0
        
    except Exception as e:
        logger.warning(f"process_album_messages error: {e}")
        return False


def _cleanup_files(files: list):
    """Helper to cleanup downloaded files."""
    for f in files:
        try:
            if f and os.path.exists(f):
                os.remove(f)
        except:
            pass


# ==================== PRIVATE POST PROCESSING ====================

async def process_private_posts(client: Client, message: Message, parsed: ParsedURL):
    """
    Process private channel posts with ALBUM-AWARE iteration.
    
    Architecture (Refactored):
    - Session is task-scoped via context manager (guaranteed cleanup)
    - Album detection returns boundaries for skip-ahead
    - In-memory album tracking only (no MongoDB)
    - Deterministic timeouts on all Pyrogram calls
    """
    user_id = message.chat.id
    status_msg = None
    
    try:
        # Create stop-safe pipeline
        async with StopSafePipeline(user_id, task_manager) as pipeline:
            
            # Check user session
            user_data = await async_db.find_user(user_id)
            if not get(user_data, 'logged_in', False) or not user_data.get('session'):
                await client.send_message(user_id, strings['need_login'], **build_reply_kwargs_from_message(message))
                return
            
            session_string = user_data['session']
            
            # Create temp directory for this operation
            temp_dir = await pipeline.get_temp_dir()
            
            total_posts = len(parsed.post_ids)
            status_msg = await client.send_message(
                user_id,
                f"⏳ {total_posts} ta post yuklanmoqda...",
                **build_reply_kwargs_from_message(message)
            )
            
            # Pin status message so user can always find it
            if total_posts > 1:
                try:
                    await client.pin_chat_message(user_id, status_msg.id, disable_notification=True)
                except Exception:
                    pass
            
            # Use album-aware iterator
            iterator = AlbumAwareIterator(parsed.post_ids)
            processed = 0
            failed = 0
            failed_ids = []  # Only existing but unsent posts (not deleted)
            deleted = 0
            albums_processed = 0
            
            # Task-scoped session - guaranteed cleanup on exit
            try:
                async with create_user_session(session_string, user_id) as acc:
                    while True:
                        # Check for cancellation
                        if await pipeline.check_cancelled():
                            await client.edit_message_text(
                                user_id, status_msg.id,
                                f"**Cancelled**\nProcessed: {processed}/{total_posts}\nAlbums: {albums_processed}"
                            )
                            return
                        
                        # Get next message ID
                        post_id = iterator.next()
                        if post_id is None:
                            break  # Done
                        
                        try:
                            # Update status with current post ID
                            try:
                                await client.edit_message_text(
                                    user_id, status_msg.id,
                                    f"⏳ {iterator.current_position}/{total_posts} | "
                                    f"ID: `{post_id}`\n"
                                    f"✅ {processed}  📁 {albums_processed}  ❌ {failed}"
                                )
                            except Exception:
                                pass
                            
                            # Try to detect album boundary
                            boundary = await detect_album_boundary(acc, parsed.channel_id, post_id)
                            
                            if boundary:
                                # Check if already processed
                                if iterator.is_album_processed(boundary.media_group_id):
                                    iterator.skip_to(boundary.next_message_id)
                                    continue
                                
                                # Process entire album using existing session
                                success, status, _ = await process_album_with_session(
                                    bot_client=client,
                                    user_session=acc,  # Use existing session!
                                    user_id=user_id,
                                    target_chat_id=user_id,
                                    source_chat_id=parsed.channel_id,
                                    message_id=post_id,
                                    reply_to_message_id=message.id,
                                    check_cancelled=pipeline.check_cancelled,  # Direct reference
                                    sent_albums=iterator.processed_albums,
                                )
                                
                                if success:
                                    processed += 1
                                    albums_processed += 1
                                    iterator.mark_album_processed(boundary.media_group_id)
                                elif status == "cancelled":
                                    return
                                elif status == "already_sent":
                                    processed += 1
                                else:
                                    failed += 1
                                    failed_ids.append(post_id)
                                
                                # Skip remaining album messages
                                iterator.skip_to(boundary.next_message_id)
                            else:
                                # Not an album - process single message
                                result = await process_single_post(
                                    client, acc, message, 
                                    parsed.channel_id, post_id, 
                                    temp_dir, pipeline
                                )
                                
                                if result == "deleted":
                                    deleted += 1
                                elif result:
                                    processed += 1
                                else:
                                    failed += 1
                                    failed_ids.append(post_id)
                            
                            # Rate limiting delay
                            await asyncio.sleep(1.5)
                            
                        except asyncio.CancelledError:
                            raise
                        except Exception as e:
                            logger.warning(f"Error processing post {post_id}: {e}")
                            failed += 1
                            failed_ids.append(post_id)
                            continue
                    
            except SessionInvalidError as e:
                await client.edit_message_text(
                    user_id, status_msg.id,
                    f"**Session Invalid**\nPlease /login again.\nError: {e}"
                )
                return
            except SessionConnectionError as e:
                await client.edit_message_text(
                    user_id, status_msg.id,
                    f"**Connection Error**\nPlease try again.\nError: {e}"
                )
                return
            
            # Final status
            failed_info = ""
            if failed_ids:
                shown_ids = failed_ids[:20]
                id_list = ", ".join(str(i) for i in shown_ids)
                failed_info = f"\n\n**Jo'natilmagan ID lar:**\n`{id_list}`"
                if len(failed_ids) > 20:
                    failed_info += f"\n...va yana {len(failed_ids) - 20} ta"
            
            deleted_info = f"\n🗑 O'chirilgan: {deleted}" if deleted > 0 else ""
            
            if processed == 0 and failed == 0 and deleted > 0:
                await client.edit_message_text(
                    user_id, status_msg.id,
                    f"⚠️ **Barcha xabarlar o'chirilgan**\n"
                    f"So'ralgan: {total_posts}\n"
                    f"🗑 O'chirilgan: {deleted}"
                )
            elif processed == 0 and total_posts > 0:
                await client.edit_message_text(
                    user_id, status_msg.id,
                    f"⚠️ **Hech qanday xabar jo'natilmadi**\n"
                    f"So'ralgan: {total_posts}\n"
                    f"❌ Xato: {failed}{deleted_info}"
                    f"{failed_info}"
                )
            elif failed > 0:
                await client.edit_message_text(
                    user_id, status_msg.id,
                    f"✅ **Tugadi**\n"
                    f"Yuklandi: {processed}/{total_posts}\n"
                    f"📁 Albumlar: {albums_processed}\n"
                    f"❌ Xato: {failed}{deleted_info}"
                    f"{failed_info}"
                )
            else:
                await client.edit_message_text(
                    user_id, status_msg.id,
                    f"✅ **Tugadi**\n"
                    f"Yuklandi: {processed}/{total_posts}\n"
                    f"📁 Albumlar: {albums_processed}{deleted_info}"
                )
            
            # Unpin status message when done
            if total_posts > 1 and status_msg:
                try:
                    await client.unpin_chat_message(user_id, status_msg.id)
                except Exception:
                    pass
    
    except asyncio.CancelledError:
        if status_msg:
            try:
                await client.edit_message_text(user_id, status_msg.id, "❌ Bekor qilindi.")
                await client.unpin_chat_message(user_id, status_msg.id)
            except:
                pass


async def process_public_posts(client: Client, message: Message, parsed: ParsedURL):
    """Process public channel posts sequentially"""
    user_id = message.chat.id
    
    async with StopSafePipeline(user_id, task_manager) as pipeline:
        total_posts = len(parsed.post_ids)
        status_msg = await client.send_message(
            user_id, f"Processing {total_posts} public posts...",
            **build_reply_kwargs_from_message(message)
        )
        
        processed = 0
        for idx, post_id in enumerate(parsed.post_ids):
            if await pipeline.check_cancelled():
                await client.edit_message_text(user_id, status_msg.id, f"Cancelled. Processed: {processed}")
                return
            
            try:
                msg = await client.get_messages(parsed.channel_id, post_id)
                await client.copy_message(user_id, msg.chat.id, msg.id, **build_reply_kwargs_from_message(message))
                processed += 1
            except Exception:
                # Try with user session
                user_data = await async_db.find_user(user_id)
                if get(user_data, 'logged_in', False) and user_data.get('session'):
                    acc, _ = await create_client_session(user_data['session'])
                    if acc:
                        try:
                            await process_single_post(client, acc, message, parsed.channel_id, post_id, None, pipeline)
                            processed += 1
                        except:
                            pass
                        finally:
                            await safe_disconnect(acc)
            
            await asyncio.sleep(2)
        
        # Final status - FIX: Distinguish success from "all failed"
        if processed == 0 and total_posts > 0:
            await client.edit_message_text(
                user_id, status_msg.id,
                f"⚠️ **No messages retrieved**\nRequested: {total_posts}\n\nMessages may have been deleted or are inaccessible."
            )
        elif processed < total_posts:
            await client.edit_message_text(
                user_id, status_msg.id,
                f"✅ **Completed with warnings**\nProcessed: {processed}/{total_posts}\nSome messages could not be retrieved."
            )
        else:
            await client.edit_message_text(user_id, status_msg.id, f"✅ **Completed**\nProcessed: {processed}/{total_posts}")


async def process_bot_posts(client: Client, message: Message, parsed: ParsedURL):
    """Process bot chat posts sequentially"""
    user_id = message.chat.id
    
    user_data = await async_db.find_user(user_id)
    if not get(user_data, 'logged_in', False) or not user_data.get('session'):
        await client.send_message(user_id, strings['need_login'], **build_reply_kwargs_from_message(message))
        return
    
    acc, error = await create_client_session(user_data['session'])
    if error:
        await client.send_message(user_id, f"Connection error: {error}", **build_reply_kwargs_from_message(message))
        return
    
    try:
        async with StopSafePipeline(user_id, task_manager) as pipeline:
            for post_id in parsed.post_ids:
                if await pipeline.check_cancelled():
                    return
                await process_single_post(client, acc, message, parsed.channel_id, post_id, None, pipeline)
                await asyncio.sleep(2)
    finally:
        await safe_disconnect(acc)


async def process_single_post(
    client: Client, 
    acc, 
    message: Optional[Message], 
    chat_id, 
    post_id: int, 
    temp_dir: Optional[str],
    pipeline: StopSafePipeline,
    target_user_id: int = None
) -> bool:
    """
    Process a single NON-ALBUM post completely before returning.
    
    NOTE: Album detection and processing is now handled by the caller
    using detect_album_boundary() and process_album_pipeline().
    This function only handles single messages.
    
    Args:
        message: Original user message (can be None for queued items)
        target_user_id: User ID to send to (used when message is None)
    """
    user_id = target_user_id if target_user_id else (message.chat.id if message else None)
    if not user_id:
        logger.error("process_single_post: no user_id available")
        return False
    
    try:
        # Check cancellation
        if await pipeline.check_cancelled():
            return False
        
        # Get the message with timeout
        try:
            msg = await asyncio.wait_for(
                acc.get_messages(chat_id, post_id),
                timeout=30.0
            )
        except asyncio.TimeoutError:
            logger.warning(f"Timeout fetching message {post_id}")
            return False
        
        if not msg or msg.empty:
            return "deleted"
        
        msg_type = get_message_type(msg)
        
        # If message is None (queued item), create a proxy so all handlers
        # work identically to the interactive (user-sent) flow.
        if message is None:
            from types import SimpleNamespace
            message = SimpleNamespace(
                chat=SimpleNamespace(id=user_id),
                id=None,          # No message to reply to
                from_user=SimpleNamespace(id=user_id),
            )
        
        # Handle text messages
        if msg_type == "Text":
            # Check for QuizBot links in text messages - handle EXCLUSIVELY
            if msg.reply_markup:
                quizbot_links = extract_quizbot_links(msg.reply_markup)
                if quizbot_links:
                    # QuizBot post - handle separately, DO NOT also send as text
                    return await handle_quizbot_post(client, message, msg)
            # Regular text message (no QuizBot)
            return await send_text_message(client, message, msg)
        
        # Handle polls with auto-vote
        if msg_type == "Poll":
            return await handle_poll(
                client, message, msg.poll, 
                source_msg=msg, 
                user_session=acc,  # Pass user session for auto-voting
                auto_vote=True
            )
        
        # Handle single media (photos without album, videos, documents, etc.)
        # NOTE: Album photos are handled by process_album_pipeline before reaching here
        return await download_and_send_media(client, acc, message, msg, msg_type, temp_dir, pipeline)
        
    except asyncio.CancelledError:
        raise
    except Exception as e:
        logger.warning(f"Error processing post {post_id}: {e}")
        return False


async def _send_post_direct(client: Client, acc, user_id: int, msg, msg_type: str, temp_dir: Optional[str]) -> bool:
    """
    Direct send for queued items (when original message object is not available).
    Simplified version that sends content directly to user.
    """
    try:
        # Check for QuizBot links in ANY message type (text, poll, etc.)
        if msg.reply_markup:
            quizbot_links = extract_quizbot_links(msg.reply_markup)
            if quizbot_links:
                return await _send_quizbot_direct(client, user_id, msg, quizbot_links)
        
        # Text message
        if msg_type == "Text":
            text = msg.text or ""
            entities = list(msg.entities) if msg.entities else None
            reply_markup = msg.reply_markup if hasattr(msg, 'reply_markup') else None
            await client.send_message(user_id, text, entities=entities, reply_markup=reply_markup)
            return True
        
        # Poll - convert to text
        if msg_type == "Poll" and msg.poll:
            poll = msg.poll
            text = f"📊 **{poll.question}**\n\n"
            for i, opt in enumerate(poll.options):
                text += f"{chr(65+i)}) {opt.text}\n"
            await client.send_message(user_id, text)
            return True
        
        # Media - download and send
        if msg.media:
            file_path = None
            try:
                download_dir = temp_dir or "downloads/temp"
                file_path = await acc.download_media(msg, file_name=f"{download_dir}/")
                if file_path and os.path.exists(file_path):
                    caption = msg.caption if hasattr(msg, 'caption') else None
                    caption_entities = list(msg.caption_entities) if hasattr(msg, 'caption_entities') and msg.caption_entities else None
                    
                    # Send based on type
                    if msg_type == "Photo":
                        await client.send_photo(user_id, file_path, caption=caption, caption_entities=caption_entities)
                    elif msg_type == "Video":
                        await client.send_video(user_id, file_path, caption=caption, caption_entities=caption_entities)
                    elif msg_type == "Audio":
                        await client.send_audio(user_id, file_path, caption=caption, caption_entities=caption_entities)
                    elif msg_type == "Voice":
                        await client.send_voice(user_id, file_path, caption=caption, caption_entities=caption_entities)
                    elif msg_type == "VideoNote":
                        await client.send_video_note(user_id, file_path)
                    else:
                        await client.send_document(user_id, file_path, caption=caption, caption_entities=caption_entities)
                    return True
            finally:
                if file_path and os.path.exists(file_path):
                    try:
                        os.remove(file_path)
                    except:
                        pass
        
        return False
    except Exception as e:
        logger.warning(f"_send_post_direct error: {e}")
        return False


async def _send_quizbot_direct(client: Client, user_id: int, msg, quizbot_links: list) -> bool:
    """
    Send QuizBot post directly to user (for queued items).
    Preserves text, entities, inline buttons, and QuizBot links.
    """
    try:
        parts = []
        if msg.text:
            parts.append(msg.text)
        elif msg.caption:
            parts.append(msg.caption)

        if quizbot_links:
            parts.append("")
            parts.append("🤖 **QuizBot Start:**")
            for link in quizbot_links:
                parts.append(link)

        if not parts:
            return False

        combined_text = "\n".join(parts)

        # Keep non-QuizBot buttons
        reply_markup = None
        if msg.reply_markup and hasattr(msg.reply_markup, 'inline_keyboard'):
            filtered_rows = []
            for row in msg.reply_markup.inline_keyboard:
                filtered_buttons = []
                for button in row:
                    if hasattr(button, 'url') and button.url:
                        if 'quizbot' in button.url.lower() and 'start=' in button.url.lower():
                            continue
                    filtered_buttons.append(button)
                if filtered_buttons:
                    filtered_rows.append(filtered_buttons)
            if filtered_rows:
                reply_markup = InlineKeyboardMarkup(filtered_rows)

        await client.send_message(
            user_id,
            combined_text,
            reply_markup=reply_markup,
            disable_web_page_preview=True
        )
        return True
    except Exception as e:
        logger.warning(f"_send_quizbot_direct error: {e}")
        return False


async def send_text_message(client: Client, message: Message, msg) -> bool:
    """
    Send a text message using MessageEntity (NO MARKDOWN).
    
    Auto-splits for 4096 char limit while preserving entities.
    """
    try:
        from core.text_renderer import extract_to_renderer, send_rendered
        
        reply_markup = msg.reply_markup if hasattr(msg, 'reply_markup') else None
        
        # Get text and entities directly (NOT Markdown formatted)
        text, entities = get_text_with_entities(msg)
        if not text:
            return True  # Empty text is ok
        
        # Convert to renderer and send with auto-splitting
        renderer = extract_to_renderer(text, entities or [])
        
        # Render chunks
        chunks = renderer.render_chunks(TELEGRAM_MESSAGE_LIMIT)
        
        for i, (chunk_text, chunk_entities) in enumerate(chunks):
            if not chunk_text:
                continue
            
            await client.send_message(
                chat_id=message.chat.id,
                text=chunk_text,
                entities=chunk_entities if chunk_entities else None,
                reply_to_message_id=message.id if i == 0 else None,
                reply_markup=reply_markup if i == len(chunks) - 1 else None
            )
        return True
    except Exception as e:
        logger.warning(f"send_text_message error: {e}")
        return False


async def download_and_send_media(
    client: Client, 
    acc, 
    message: Message, 
    msg, 
    msg_type: str,
    temp_dir: Optional[str],
    pipeline: StopSafePipeline
) -> bool:
    """
    Download and send media file using core download engine.
    
    Returns:
        bool: True if successful, False otherwise
    
    Uses:
    - core.downloader.DownloadEngine for downloads
    - progress_tracker for progress updates
    - ReplyParameters (not deprecated reply_to_message_id)
    """
    file_path = None
    thumb_path = None
    status_msg = None
    
    try:
        # Check cancellation
        if await pipeline.check_cancelled():
            return False
        
        # Get file size for progress decision
        file_size = get_file_size(msg, msg_type)
        show_progress = file_size > 1 * 1024 * 1024  # 1MB threshold for progress (was 10MB)
        
        if show_progress:
            size_mb = file_size / 1024 / 1024
            status_msg = await client.send_message(
                message.chat.id, 
                f"**Downloading**\n{size_mb:.1f} MB",
                **build_reply_kwargs_from_message(message)
            )
        
        # Download using core engine
        if temp_dir:
            download_path = temp_dir
        else:
            download_path = TEMP_DOWNLOAD_DIR
            os.makedirs(download_path, exist_ok=True)
        
        # Use download engine with progress tracking
        # CRITICAL: bot_client is used for editing progress messages
        # because status_msg was sent by the bot, not the user session
        engine = await get_engine()
        file_path = await engine.download(
            message=msg,
            client=acc,
            status_message=status_msg if show_progress else None,
            user_id=message.chat.id,
            download_dir=download_path,
            bot_client=client  # Bot client for progress message editing
        )
        
        if not file_path or not os.path.exists(file_path):
            if status_msg:
                await client.delete_messages(message.chat.id, [status_msg.id])
            return False
        
        # Check cancellation before upload
        if await pipeline.check_cancelled():
            return False
        
        # Get caption with entities early - needed by both split and normal paths
        caption, caption_entities = get_caption_with_entities(msg)
        
        # Check if file needs splitting (>2GB)
        actual_file_size = os.path.getsize(file_path)
        if actual_file_size > MAX_TELEGRAM_FILE_SIZE:
            # Handle large file - split and upload parts
            # Use ffmpeg for video files (preserves quality), binary split for others
            from TechVJ.lang import get_string
            user_id = message.chat.id
            
            if status_msg:
                if is_video_file(file_path):
                    split_msg = get_string(user_id, "splitting_ffmpeg")
                else:
                    split_msg = get_string(user_id, "splitting_binary")
                await client.edit_message_text(
                    message.chat.id, status_msg.id,
                    get_string(user_id, "file_too_large", size=format_size(actual_file_size)) + f"\n{split_msg}"
                )
            
            try:
                # Use smart split - ffmpeg for videos, binary for others
                chunk_paths = split_large_video(file_path) if is_video_file(file_path) else split_file(file_path)
                total_parts = len(chunk_paths)
                
                for i, chunk_path in enumerate(chunk_paths):
                    part_num = i + 1
                    chunk_name = os.path.basename(chunk_path)
                    chunk_size = os.path.getsize(chunk_path)
                    
                    if status_msg:
                        await client.edit_message_text(
                            message.chat.id, status_msg.id,
                            get_string(user_id, "uploading_part", part=part_num, total=total_parts, name=chunk_name, size=format_size(chunk_size))
                        )
                    
                    # Caption only for first part
                    part_caption = None
                    if part_num == 1 and caption:
                        part_caption = f"**Part {part_num}/{total_parts}**\n\n{caption}"
                        if len(part_caption) > 1024:
                            part_caption = part_caption[:1020] + "..."
                    else:
                        part_caption = f"**Part {part_num}/{total_parts}**"
                    
                    # Send as video if it's a video file, document otherwise
                    if is_video_file(chunk_path):
                        await client.send_video(
                            message.chat.id, chunk_path,
                            caption=part_caption,
                            supports_streaming=True,
                            **build_reply_kwargs_from_message(message)
                        )
                    else:
                        await client.send_document(
                            message.chat.id, chunk_path,
                            caption=part_caption,
                            **build_reply_kwargs_from_message(message)
                        )
                    
                    # Delete chunk after upload
                    try:
                        os.remove(chunk_path)
                    except:
                        pass
                    
                    # Rate limit protection
                    if part_num < total_parts:
                        await asyncio.sleep(2)
                
                if status_msg:
                    await client.edit_message_text(
                        message.chat.id, status_msg.id,
                        get_string(user_id, "split_complete", parts=total_parts)
                    )
                return True
                
            except Exception as split_err:
                logger.error(f"Large file split error: {split_err}")
                if status_msg:
                    await client.edit_message_text(
                        message.chat.id, status_msg.id,
                        get_string(user_id, "error_split", error=str(split_err)[:100])
                    )
                return False
        
        if status_msg:
            from TechVJ.lang import get_string
            await client.edit_message_text(message.chat.id, status_msg.id, get_string(message.chat.id, "uploading"))
        
        # caption and caption_entities already extracted above (before split check)
        overflow_text = None
        overflow_entities = None
        
        # Check caption length using UTF-16 (Telegram's native unit)
        caption_utf16 = len(caption.encode('utf-16-le')) // 2 if caption else 0
        
        if caption and caption_utf16 > TELEGRAM_CAPTION_LIMIT:
            # Split caption while preserving entities using entity_splitter
            try:
                from core.entity_splitter import split_caption_with_overflow
                cap_chunk, overflow_chunks = split_caption_with_overflow(caption, caption_entities or [])
                
                caption = cap_chunk.text
                caption_entities = cap_chunk.entities if cap_chunk.entities else None
                
                # Combine overflow chunks into single text
                if overflow_chunks:
                    overflow_text = "\n\n".join(c.text for c in overflow_chunks)
                    # For simplicity, don't preserve entities in overflow (they're recalculated)
                    overflow_entities = overflow_chunks[0].entities if overflow_chunks[0].entities else None
                    
            except Exception as split_err:
                logger.warning(f"Caption split error: {split_err}, truncating")
                # Simple truncation - find safe point
                trunc_point = TELEGRAM_CAPTION_LIMIT - 10
                # Don't break in middle of word
                while trunc_point > 100 and caption[trunc_point] not in ' \n':
                    trunc_point -= 1
                overflow_text = caption[trunc_point:].strip()
                caption = caption[:trunc_point].rstrip() + "..."
                caption_entities = None
                overflow_entities = None
        
        # CRITICAL: Validate entity bounds using UTF-16 length (Telegram's native unit)
        if caption_entities and caption:
            # Calculate UTF-16 length (not Python string length!)
            caption_utf16_len = len(caption.encode('utf-16-le')) // 2
            caption_entities = [e for e in caption_entities if e.offset >= 0 and e.offset + e.length <= caption_utf16_len]
            if not caption_entities:
                caption_entities = None
        
        reply_markup = msg.reply_markup if hasattr(msg, 'reply_markup') else None
        reply_kwargs = build_reply_kwargs_from_message(message)
        
        # Upload based on type - using entities instead of parse_mode
        if msg_type == "Photo":
            await client.send_photo(
                message.chat.id, file_path, caption=caption,
                caption_entities=caption_entities,
                **reply_kwargs, reply_markup=reply_markup
            )
        elif msg_type == "Video":
            thumb_path = await get_thumbnail(acc, msg)
            upload_cb = None
            if show_progress and status_msg:
                engine = await get_engine()
                upload_cb = engine.create_progress_callback(client, status_msg, "upload")
            await client.send_video(
                message.chat.id, file_path,
                duration=msg.video.duration if msg.video else None,
                width=msg.video.width if msg.video else None,
                height=msg.video.height if msg.video else None,
                thumb=thumb_path, caption=caption,
                caption_entities=caption_entities,
                **reply_kwargs, reply_markup=reply_markup,
                progress=upload_cb
            )
        elif msg_type == "Audio":
            upload_cb = None
            if show_progress and status_msg:
                upload_cb = (await get_engine()).create_progress_callback(client, status_msg, "upload")
            await client.send_audio(
                message.chat.id, file_path, caption=caption,
                caption_entities=caption_entities,
                **reply_kwargs, reply_markup=reply_markup,
                progress=upload_cb
            )
        elif msg_type == "Voice":
            await client.send_voice(
                message.chat.id, file_path, caption=caption,
                caption_entities=caption_entities,
                **reply_kwargs
            )
        elif msg_type == "VideoNote":
            await client.send_video_note(
                message.chat.id, file_path,
                **reply_kwargs
            )
        elif msg_type == "Animation":
            await client.send_animation(
                message.chat.id, file_path, caption=caption,
                caption_entities=caption_entities,
                **reply_kwargs, reply_markup=reply_markup
            )
        elif msg_type == "Sticker":
            await client.send_sticker(
                message.chat.id, file_path,
                **reply_kwargs
            )
        else:  # Document or unknown
            upload_cb = None
            if show_progress and status_msg:
                upload_cb = (await get_engine()).create_progress_callback(client, status_msg, "upload")
            await client.send_document(
                message.chat.id, file_path, caption=caption,
                caption_entities=caption_entities,
                **reply_kwargs, reply_markup=reply_markup,
                progress=upload_cb
            )
        
        # Send overflow caption text as separate message
        if overflow_text:
            try:
                from core.entity_splitter import split_text_with_entities, MESSAGE_LIMIT
                from TechVJ.lang import get_string
                
                # Add header - but DON'T mix with entities from caption!
                # overflow_entities are already recalculated for overflow_text by entity_splitter
                header = get_string(message.chat.id, "caption_continued") + "\n\n"
                
                if overflow_entities:
                    # Shift entity offsets by header length (UTF-16)
                    header_utf16 = len(header.encode('utf-16-le')) // 2
                    shifted_entities = []
                    for e in overflow_entities:
                        from pyrogram.types import MessageEntity
                        shifted = MessageEntity(
                            type=e.type,
                            offset=e.offset + header_utf16,
                            length=e.length,
                            url=getattr(e, 'url', None),
                            user=getattr(e, 'user', None),
                            language=getattr(e, 'language', None),
                        )
                        shifted_entities.append(shifted)
                    overflow_entities = shifted_entities
                
                full_overflow = header + overflow_text
                
                # Split if too long
                chunks = split_text_with_entities(full_overflow, overflow_entities or [], MESSAGE_LIMIT)
                
                for chunk in chunks:
                    await client.send_message(
                        message.chat.id,
                        text=chunk.text,
                        entities=chunk.entities if chunk.entities else None
                    )
            except Exception as overflow_err:
                logger.warning(f"Failed to send overflow caption: {overflow_err}")
                # Fallback: send as plain text without header
                try:
                    await client.send_message(message.chat.id, text=overflow_text[:4000])
                except:
                    pass
        
        if status_msg:
            await client.delete_messages(message.chat.id, [status_msg.id])
        
        return True
        
    except asyncio.CancelledError:
        raise
    except Exception as e:
        if status_msg:
            try:
                await client.delete_messages(message.chat.id, [status_msg.id])
            except:
                pass
        return False
    finally:
        # GUARANTEED CLEANUP
        if file_path and os.path.exists(file_path):
            try:
                os.remove(file_path)
            except:
                pass
        if thumb_path and os.path.exists(thumb_path):
            try:
                os.remove(thumb_path)
            except:
                pass
        # Note: Status file cleanup is no longer needed with the new progress system
        # The old file-based progress (downstatus.txt, upstatus.txt) is deprecated
        # Progress is now handled in-memory by progress_controller


def get_file_size(msg, msg_type: str) -> int:
    """Get file size from message"""
    try:
        if msg_type == "Document" and msg.document:
            return msg.document.file_size or 0
        elif msg_type == "Video" and msg.video:
            return msg.video.file_size or 0
        elif msg_type == "Audio" and msg.audio:
            return msg.audio.file_size or 0
        elif msg_type == "Voice" and msg.voice:
            return msg.voice.file_size or 0
        elif msg_type == "VideoNote" and msg.video_note:
            return msg.video_note.file_size or 0
        elif msg_type == "Animation" and msg.animation:
            return msg.animation.file_size or 0
    except:
        pass
    return 0


async def get_thumbnail(acc, msg) -> Optional[str]:
    """Download thumbnail if available"""
    try:
        if msg.video and msg.video.thumbs:
            return await acc.download_media(msg.video.thumbs[0].file_id)
    except:
        pass
    return None


def get_message_type(msg) -> str:
    """Detect message type"""
    if hasattr(msg, 'media') and msg.media:
        if msg.media == MessageMediaType.PHOTO:
            return "Photo"
        elif msg.media == MessageMediaType.VIDEO:
            return "Video"
        elif msg.media == MessageMediaType.DOCUMENT:
            return "Document"
        elif msg.media == MessageMediaType.AUDIO:
            return "Audio"
        elif msg.media == MessageMediaType.VOICE:
            return "Voice"
        elif msg.media == MessageMediaType.VIDEO_NOTE:
            return "VideoNote"
        elif msg.media == MessageMediaType.ANIMATION:
            return "Animation"
        elif msg.media == MessageMediaType.STICKER:
            return "Sticker"
        elif msg.media == MessageMediaType.POLL:
            return "Poll"
    
    if msg.text:
        return "Text"
    
    return "Unknown"


async def handle_poll(
    client: Client, 
    message: Message, 
    poll, 
    source_msg=None,
    user_session=None,
    auto_vote: bool = True
) -> bool:
    """
    Handle poll messages with auto-vote and text rendering.
    
    Features:
    - AUTO-VOTE using user session (if available)
    - For quizzes: votes correct answer
    - For regular polls: votes random option
    - Renders poll as formatted text (no poll forwarding)
    - Shows vote result
    - Extracts and sends QuizBot links if present
    - Preserves existing QuizBot workflow
    
    Args:
        client: Bot client
        message: User's message
        poll: Poll object
        source_msg: Original message containing poll
        user_session: User's Pyrogram client for voting
        auto_vote: Whether to auto-vote (default True)
    """
    try:
        if not poll:
            return False
        
        is_quiz = hasattr(poll, 'type') and poll.type == PollType.QUIZ
        correct_id = poll.correct_option_id if is_quiz and hasattr(poll, 'correct_option_id') else None
        
        # Determine which option to vote for
        import random
        if is_quiz and correct_id is not None:
            # Quiz: vote for correct answer
            vote_option = correct_id
        else:
            # Regular poll: vote random option
            vote_option = random.randint(0, len(poll.options) - 1)
        
        vote_success = False
        vote_error = None
        
        # AUTO-VOTE using user session
        if auto_vote and user_session and source_msg:
            try:
                await user_session.vote_poll(
                    chat_id=source_msg.chat.id,
                    message_id=source_msg.id,
                    options=[vote_option]
                )
                vote_success = True
                logger.info(f"Auto-voted option {vote_option} in poll {source_msg.id}")
            except Exception as vote_err:
                vote_error = str(vote_err)
                error_upper = vote_error.upper()
                if "ALREADY" in error_upper:
                    vote_success = True  # Already voted is OK
                    logger.debug(f"Already voted in poll {source_msg.id}")
                elif "FLOOD" in error_upper:
                    logger.warning(f"FloodWait on vote_poll: {vote_err}")
                else:
                    logger.warning(f"vote_poll failed: {vote_err}")
        
        # Build text representation (Uzbek format as requested)
        option_letters = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
        
        poll_type_text = "Quiz" if is_quiz else "So'rovnoma"
        text = f"**📊 {poll_type_text}**\n\n"
        text += f"**Savol:**\n{poll.question}\n\n"
        text += "**Variantlar:**\n"
        
        for i, opt in enumerate(poll.options):
            letter = option_letters[i] if i < len(option_letters) else str(i + 1)
            marker = ""
            
            if is_quiz and i == correct_id:
                marker = " ✅"
            
            text += f"{letter}) {opt.text}{marker}\n"
        
        # Show user's vote
        vote_letter = option_letters[vote_option] if vote_option < len(option_letters) else str(vote_option + 1)
        vote_text = poll.options[vote_option].text if vote_option < len(poll.options) else "?"
        
        text += f"\n**Sizning ovozingiz:** {vote_letter}) {vote_text}"
        
        if vote_success:
            text += " ✓"
        elif vote_error and "ALREADY" not in vote_error.upper():
            text += " (ovoz berilmadi)"
        
        # Quiz result
        if is_quiz:
            correct_letter = option_letters[correct_id] if correct_id is not None and correct_id < len(option_letters) else "?"
            correct_text = poll.options[correct_id].text if correct_id is not None and correct_id < len(poll.options) else "?"
            
            text += f"\n**To'g'ri javob:** {correct_letter}) {correct_text}"
            
            if vote_option == correct_id:
                text += "\n\n**Natija:** ✅ TO'G'RI"
            else:
                text += "\n\n**Natija:** ❌ NOTO'G'RI"
        
        # Explanation
        if is_quiz and hasattr(poll, 'explanation') and poll.explanation:
            text += f"\n\n**💡 Izoh:** {poll.explanation}"
        
        # QuizBot links (preserve existing workflow)
        if source_msg:
            quizbot_links = extract_quizbot_links(source_msg.reply_markup)
            if quizbot_links:
                text += "\n\n🤖 **QuizBot Start:**"
                for link in quizbot_links:
                    text += f"\n{link}"
        
        # Preserve reply markup for QuizBot workflow
        reply_markup = source_msg.reply_markup if source_msg else None
        
        # Send text-only message with auto-splitting (no poll forwarding)
        await safe_send_message(
            client=client,
            chat_id=message.chat.id,
            text=text,
            **build_reply_kwargs_from_message(message),
            reply_markup=reply_markup,
            **build_link_preview_kwargs(is_disabled=True)
        )
        
        return True
    except Exception as e:
        logger.warning(f"handle_poll error: {e}")
        return False


async def handle_quizbot_post(client: Client, message: Message, source_msg) -> bool:
    """
    Handle QuizBot posts - sends ALL content as ONE single message.
    
    Combines:
    - Quiz text/description
    - Emoji and metadata
    - QuizBot start links
    - Inline buttons (preserved)
    
    All in ONE message, not split across multiple messages.
    """
    try:
        # Build combined message content
        parts = []
        
        # 1. Original message text (quiz description)
        if source_msg.text:
            parts.append(source_msg.text)
        elif source_msg.caption:
            parts.append(source_msg.caption)
        
        # 2. Extract and append QuizBot links
        quizbot_links = extract_quizbot_links(source_msg.reply_markup)
        if quizbot_links:
            parts.append("")  # Empty line separator
            parts.append("🤖 **QuizBot Start:**")
            for link in quizbot_links:
                parts.append(link)
        
        if not parts:
            return False
        
        # Combine all parts into single message
        combined_text = "\n".join(parts)
        
        # Preserve inline buttons (excluding QuizBot start buttons which we already extracted)
        reply_markup = None
        if source_msg.reply_markup and hasattr(source_msg.reply_markup, 'inline_keyboard'):
            # Keep non-QuizBot buttons
            filtered_rows = []
            for row in source_msg.reply_markup.inline_keyboard:
                filtered_buttons = []
                for button in row:
                    if hasattr(button, 'url') and button.url:
                        # Skip QuizBot start buttons (already in text)
                        if 'quizbot' in button.url.lower() and 'start=' in button.url.lower():
                            continue
                    filtered_buttons.append(button)
                if filtered_buttons:
                    filtered_rows.append(filtered_buttons)
            
            if filtered_rows:
                reply_markup = InlineKeyboardMarkup(filtered_rows)
        
        # Send as ONE single message
        await client.send_message(
            message.chat.id,
            combined_text,
            **build_reply_kwargs_from_message(message),
            reply_markup=reply_markup,
            **build_link_preview_kwargs(is_disabled=True)
        )
        
        return True
        
    except Exception as e:
        logger.warning(f"handle_quizbot_post error: {e}")
        return False


# Don't Remove Credit Tg - @VJ_Botz
