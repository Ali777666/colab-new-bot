"""
core/entity_builder.py - MessageEntity-based text formatting (NOT Markdown/HTML).

CRITICAL: This module uses Telegram's MessageEntity objects directly,
avoiding all Markdown/HTML parsing issues.

Features:
- Entity-aware text splitting (respects 4096 char limit)
- Automatic entity offset recalculation per chunk
- Hyperlinks via MessageEntity.TEXT_LINK
- All formatting preserved across message splits
- No broken links or formatting
"""

import logging
from typing import List, Tuple, Optional
from dataclasses import dataclass
from pyrogram.types import MessageEntity
from pyrogram.enums import MessageEntityType

logger = logging.getLogger(__name__)

# Telegram limits
MAX_MESSAGE_LENGTH = 4096
MAX_CAPTION_LENGTH = 1024


@dataclass
class TextWithEntities:
    """Text with associated MessageEntity objects."""
    text: str
    entities: List[MessageEntity]
    
    def __bool__(self):
        return bool(self.text)
    
    def __len__(self):
        return len(self.text)


class EntityBuilder:
    """
    Builder for creating text with MessageEntity objects.
    
    Usage:
        builder = EntityBuilder()
        builder.add_text("Click ")
        builder.add_link("here", "https://example.com")
        builder.add_text(" to continue")
        
        result = builder.build()
        # result.text = "Click here to continue"
        # result.entities = [MessageEntity(TEXT_LINK, offset=6, length=4, url="...")]
    """
    
    def __init__(self):
        self._text = ""
        self._entities: List[MessageEntity] = []
    
    def add_text(self, text: str) -> "EntityBuilder":
        """Add plain text."""
        self._text += text
        return self
    
    def add_link(self, text: str, url: str) -> "EntityBuilder":
        """Add hyperlink text."""
        if not text or not url:
            return self
        
        offset = len(self._text)
        self._text += text
        
        self._entities.append(MessageEntity(
            type=MessageEntityType.TEXT_LINK,
            offset=offset,
            length=len(text),
            url=url
        ))
        return self
    
    def add_bold(self, text: str) -> "EntityBuilder":
        """Add bold text."""
        if not text:
            return self
        
        offset = len(self._text)
        self._text += text
        
        self._entities.append(MessageEntity(
            type=MessageEntityType.BOLD,
            offset=offset,
            length=len(text)
        ))
        return self
    
    def add_italic(self, text: str) -> "EntityBuilder":
        """Add italic text."""
        if not text:
            return self
        
        offset = len(self._text)
        self._text += text
        
        self._entities.append(MessageEntity(
            type=MessageEntityType.ITALIC,
            offset=offset,
            length=len(text)
        ))
        return self
    
    def add_code(self, text: str) -> "EntityBuilder":
        """Add inline code."""
        if not text:
            return self
        
        offset = len(self._text)
        self._text += text
        
        self._entities.append(MessageEntity(
            type=MessageEntityType.CODE,
            offset=offset,
            length=len(text)
        ))
        return self
    
    def add_newline(self, count: int = 1) -> "EntityBuilder":
        """Add newline(s)."""
        self._text += "\n" * count
        return self
    
    def build(self) -> TextWithEntities:
        """Build final text with entities."""
        return TextWithEntities(
            text=self._text,
            entities=self._entities.copy()
        )
    
    def clear(self) -> "EntityBuilder":
        """Clear builder for reuse."""
        self._text = ""
        self._entities.clear()
        return self


def split_text_with_entities(
    text: str,
    entities: List[MessageEntity],
    max_length: int = MAX_MESSAGE_LENGTH
) -> List[TextWithEntities]:
    """
    Split long text into chunks while preserving entities.
    
    CRITICAL: Entity offsets are recalculated for each chunk.
    Entities are NEVER split - if an entity doesn't fit, the entire
    entity moves to the next chunk.
    
    Args:
        text: Original text
        entities: List of MessageEntity objects
        max_length: Maximum length per chunk (default 4096)
    
    Returns:
        List of TextWithEntities chunks
    """
    if not text:
        return []
    
    if len(text) <= max_length:
        return [TextWithEntities(text=text, entities=entities or [])]
    
    # Sort entities by offset
    sorted_entities = sorted(entities or [], key=lambda e: e.offset)
    
    chunks = []
    current_pos = 0
    
    while current_pos < len(text):
        # Calculate chunk end position
        chunk_end = min(current_pos + max_length, len(text))
        
        # Check if any entity would be split
        for entity in sorted_entities:
            entity_start = entity.offset
            entity_end = entity.offset + entity.length
            
            # Skip entities before current position
            if entity_end <= current_pos:
                continue
            
            # If entity would be split, move chunk_end before entity
            if entity_start < chunk_end <= entity_end:
                # Find safe split point before this entity
                safe_end = entity_start
                
                # Try to find a word boundary
                for i in range(safe_end - 1, max(current_pos, safe_end - 200), -1):
                    if text[i] in ' \n':
                        safe_end = i + 1
                        break
                
                if safe_end > current_pos:
                    chunk_end = safe_end
                else:
                    # Entity is too close to start, include it in next chunk
                    if entity_start > current_pos:
                        chunk_end = entity_start
                break
        
        # Ensure we make progress
        if chunk_end <= current_pos:
            chunk_end = min(current_pos + max_length, len(text))
        
        # Extract chunk text
        chunk_text = text[current_pos:chunk_end]
        
        # Calculate entities for this chunk
        chunk_entities = []
        for entity in sorted_entities:
            entity_start = entity.offset
            entity_end = entity.offset + entity.length
            
            # Check if entity is fully within this chunk
            if entity_start >= current_pos and entity_end <= chunk_end:
                # Recalculate offset relative to chunk start
                new_offset = entity_start - current_pos
                
                # Create new entity with adjusted offset
                new_entity = MessageEntity(
                    type=entity.type,
                    offset=new_offset,
                    length=entity.length,
                    url=getattr(entity, 'url', None),
                    user=getattr(entity, 'user', None),
                    language=getattr(entity, 'language', None),
                    custom_emoji_id=getattr(entity, 'custom_emoji_id', None)
                )
                chunk_entities.append(new_entity)
        
        chunks.append(TextWithEntities(
            text=chunk_text.strip(),
            entities=chunk_entities
        ))
        
        current_pos = chunk_end
        
        # Skip whitespace at chunk boundaries
        while current_pos < len(text) and text[current_pos] in ' \n':
            current_pos += 1
    
    return [c for c in chunks if c.text]


def extract_entities_from_message(msg) -> Tuple[str, List[MessageEntity]]:
    """
    Extract text and entities from a Pyrogram Message.
    
    Handles both text and caption, preserving all entity types.
    
    Args:
        msg: Pyrogram Message object
    
    Returns:
        (text, entities) tuple
    """
    # Try text first, then caption
    if msg.text:
        text = msg.text
        entities = msg.entities or []
    elif msg.caption:
        text = msg.caption
        entities = msg.caption_entities or []
    else:
        return "", []
    
    # Convert entities to list (might be a generator)
    entities = list(entities) if entities else []
    
    return text, entities


def copy_entities(entities: List[MessageEntity]) -> List[MessageEntity]:
    """Create deep copies of entities."""
    if not entities:
        return []
    
    copies = []
    for e in entities:
        copies.append(MessageEntity(
            type=e.type,
            offset=e.offset,
            length=e.length,
            url=getattr(e, 'url', None),
            user=getattr(e, 'user', None),
            language=getattr(e, 'language', None),
            custom_emoji_id=getattr(e, 'custom_emoji_id', None)
        ))
    return copies


async def send_text_with_entities(
    client,
    chat_id: int,
    text: str,
    entities: List[MessageEntity] = None,
    reply_to_message_id: int = None,
    disable_web_page_preview: bool = True
) -> List:
    """
    Send text message with entities, auto-splitting if needed.
    
    Args:
        client: Pyrogram Client
        chat_id: Target chat ID
        text: Message text
        entities: List of MessageEntity
        reply_to_message_id: Optional reply
        disable_web_page_preview: Disable link previews
    
    Returns:
        List of sent messages
    """
    from pyrogram.types import LinkPreviewOptions
    
    if not text:
        return []
    
    # Split if needed
    chunks = split_text_with_entities(text, entities or [])
    
    sent_messages = []
    for i, chunk in enumerate(chunks):
        try:
            # Only reply to first message
            reply_id = reply_to_message_id if i == 0 else None
            
            msg = await client.send_message(
                chat_id=chat_id,
                text=chunk.text,
                entities=chunk.entities if chunk.entities else None,
                reply_to_message_id=reply_id,
                link_preview_options=LinkPreviewOptions(is_disabled=disable_web_page_preview)
            )
            sent_messages.append(msg)
            
        except Exception as e:
            logger.warning(f"Error sending chunk {i}: {e}")
    
    return sent_messages
