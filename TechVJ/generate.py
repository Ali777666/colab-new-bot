# Don't Remove Credit Tg - @VJ_Botz
# Subscribe YouTube Channel For Amazing Bot https://youtube.com/@Tech_VJ
# Ask Doubt on telegram @KingVJ01

import traceback
import os
import asyncio
import logging
from pyrogram.types import Message
from pyrogram import Client, filters
from pyrogram.enums import ParseMode
from pyrogram.raw import functions, types as raw_types
from asyncio.exceptions import TimeoutError
from pyrogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from pyrogram.errors import (
    ApiIdInvalid,
    PhoneNumberInvalid,
    PhoneCodeInvalid,
    PhoneCodeExpired,
    SessionPasswordNeeded,
    PasswordHashInvalid
)
from TechVJ.strings import strings
from config import API_ID, API_HASH, BANNED_MESSAGE
from database.async_db import async_db
from TechVJ.qr_login import perform_qr_login, is_qr_available, cleanup_qr_files, get_device_info
from TechVJ.login_rate_limiter import login_limiter, rate_limited_login

logger = logging.getLogger(__name__)


_waiting_users: dict = {}
_handler_installed: bool = False


async def ask_message(
    client: Client,
    chat_id: int,
    text: str,
    timeout: int = 300,
    filters_arg=None,
) -> Message:
    """
    Send a Markdown-formatted message and wait for user reply.
    
    Uses a global dict approach - no dynamic handler add/remove.
    """
    global _handler_installed
    
    # Install handler once (lazy initialization)
    if not _handler_installed:
        _install_handler(client)
        _handler_installed = True
    
    # Send prompt
    try:
        await client.send_message(chat_id, text, parse_mode=ParseMode.MARKDOWN)
    except Exception as e:
        logger.warning(f"Markdown send failed: {e}")
        await client.send_message(chat_id, text)
    
    # Try listen() if available (pyromod/kurigram extension)
    if hasattr(client, 'listen'):
        try:
            return await client.listen(chat_id=chat_id, filters=filters_arg, timeout=timeout)
        except Exception:
            pass
    
    # Fallback: event-based waiting
    loop = asyncio.get_running_loop()
    future = loop.create_future()
    _waiting_users[chat_id] = future
    
    try:
        return await asyncio.wait_for(future, timeout=timeout)
    except asyncio.TimeoutError:
        raise TimeoutError("Response timeout")
    finally:
        _waiting_users.pop(chat_id, None)


def _install_handler(client: Client):
    """Install the response handler once."""
    from pyrogram.handlers import MessageHandler
    
    async def _response_handler(_, msg: Message):
        chat_id = msg.chat.id
        if chat_id in _waiting_users:
            future = _waiting_users[chat_id]
            if not future.done():
                future.set_result(msg)
    
    handler = MessageHandler(
        _response_handler,
        filters.private & ~filters.me & ~filters.bot
    )
    client.add_handler(handler, group=-999)
    logger.info("ask_message response handler installed")


def setup_ask_handler(app: Client):
    """Pre-register the handler at startup (optional, for eager init)."""
    global _handler_installed
    if not _handler_installed:
        _install_handler(app)
        _handler_installed = True

SESSION_STRING_SIZE = 351


async def send_code_raw(client, phone_number):
    """
    Send code using raw API to handle all response types.
    
    CodeSettings explained:
    - allow_flashcall=False: Don't use flash call (unreliable)
    - current_number=False: IMPORTANT! False = send to OTHER devices first
                            True = prioritize current device
    - allow_app_hash=True: Allow app-based verification
    - allow_missed_call=False: Don't use missed call method
    - allow_firebase=False: Don't use Firebase (Android only)
    
    By setting current_number=False, Telegram will prefer sending
    the code to the user's OTHER logged-in Telegram apps first,
    rather than SMS. This is more reliable and faster.
    """
    try:
        r = await client.invoke(
            functions.auth.SendCode(
                phone_number=phone_number,
                api_id=API_ID,
                api_hash=API_HASH,
                settings=raw_types.CodeSettings(
                    allow_flashcall=False,
                    current_number=False,  # Send to OTHER devices first (Telegram app)
                    allow_app_hash=True,   # Allow in-app verification
                    allow_missed_call=False,
                    allow_firebase=False,
                )
            )
        )
        
        sent_code_type = type(r.type).__name__
        email_required = False
        
        if hasattr(raw_types.auth, 'SentCodeTypeSetUpEmailRequired'):
            if isinstance(r.type, raw_types.auth.SentCodeTypeSetUpEmailRequired):
                email_required = True
        
        if 'EmailRequired' in sent_code_type or 'SetUpEmail' in sent_code_type:
            email_required = True
        
        return r.phone_code_hash, sent_code_type, email_required
        
    except Exception as e:
        error_str = str(e)
        if 'EMAIL' in error_str.upper() or 'SetUpEmail' in error_str:
            raise Exception("EMAIL_SETUP_REQUIRED")
        raise


def get(obj, key, default=None):
    try:
        return obj[key]
    except:
        return default


def check_banned_sync(func):
    """Decorator to check if user is banned before processing"""
    async def wrapper(client: Client, message: Message):
        user_id = message.from_user.id
        if await async_db.is_banned(user_id):
            await message.reply(BANNED_MESSAGE)
            return
        return await func(client, message)
    return wrapper


@Client.on_message(filters.private & ~filters.forwarded & filters.command(["logout"]))
@check_banned_sync
async def logout(_, msg):
    user_data = await async_db.find_user(msg.chat.id)
    if user_data is None or not user_data.get('session'):
        await msg.reply("You are not logged in.")
        return 
    await async_db.update_user(msg.chat.id, {'session': None, 'logged_in': False})
    await msg.reply("**Logout Successfully** ♦")


def save_session_to_file(user_id, session_string):
    """Save session to file as fallback when MongoDB is unavailable"""
    os.makedirs("sessions", exist_ok=True)
    session_file = f"sessions/user_{user_id}.session"
    with open(session_file, 'w') as f:
        f.write(session_string)
    return session_file


def load_session_from_file(user_id):
    """Load session from file"""
    session_file = f"sessions/user_{user_id}.session"
    if os.path.exists(session_file):
        with open(session_file, 'r') as f:
            return f.read().strip()
    return None


@Client.on_message(filters.private & ~filters.forwarded & filters.command(["login"]))
@check_banned_sync
async def main(bot: Client, message: Message):
    # Insert user if not exists
    await async_db.insert_user(message.from_user.id)
    user_data = await async_db.find_user(message.from_user.id)
    
    if get(user_data, 'logged_in', False):
        await message.reply(strings['already_logged_in'])
        return 
    
    user_id = int(message.from_user.id)
    
    # ===== LOGIN RATE LIMITING =====
    # Check if user can login (prevents IP blocks from Telegram)
    can_login, wait_time, reason = await login_limiter.can_login(user_id)
    if not can_login:
        if wait_time > 60:
            wait_str = f"{wait_time / 60:.0f} daqiqa"
        else:
            wait_str = f"{wait_time:.0f} soniya"
        
        await message.reply(
            f"⏳ **Login cheklangan**\n\n"
            f"{reason}\n\n"
            f"Iltimos **{wait_str}** kuting.\n\n"
            f"_Bu cheklov serverdan ko'p login qilinmasligi uchun._"
        )
        return
    # ===== END RATE LIMITING =====
    
    try:
        phone_number_msg = await ask_message(
            client=bot,
            chat_id=user_id, 
            text=(
                "*Please send your phone number with country code*\n"
                "*Example:* `+998901234567`\n\n"
                "_You have 5 minutes to respond._\n"
                "_Send /cancel to abort._"
            ),
            timeout=300,
        )
    except TimeoutError:
        return await message.reply("*Login cancelled due to timeout.*", parse_mode=ParseMode.MARKDOWN)
    
    if phone_number_msg.text == '/cancel':
        return await phone_number_msg.reply("*Process cancelled!*", parse_mode=ParseMode.MARKDOWN)
    phone_number = phone_number_msg.text
    
    os.makedirs("sessions", exist_ok=True)
    session_path = f"sessions/temp_user_{user_id}"
    
    # SECURITY: Always use 'desktop' - consistent fingerprint prevents security resets
    device_info = get_device_info(user_id, device_type='desktop')
    
    # ===== ACQUIRE LOGIN SLOT =====
    # This ensures only MAX_CONCURRENT_LOGINS happen at once
    async with login_limiter.acquire(user_id) as slot_acquired:
        if not slot_acquired:
            await message.reply(
                "⏳ **Server band**\n\n"
                "Boshqa userlar login qilmoqda. Iltimos 1-2 daqiqa kuting."
            )
            return
        
        client = Client(
            session_path,
            api_id=API_ID,
            api_hash=API_HASH,
            device_model=device_info['device_model'],
            system_version=device_info['system_version'],
            app_version=device_info['app_version'],
            lang_code=device_info['lang_code']
        )
        
        try:
            await client.connect()
            await phone_number_msg.reply("Sending OTP...")
            
            try:
                phone_code_hash, code_type, email_required = await send_code_raw(client, phone_number)
                
                if email_required:
                    await phone_number_msg.reply(
                        "*OTP could not be sent to this account.*\n\n"
                        "This may be due to Telegram's security requirements.\n\n"
                        "*Alternative: Use QR Code Login*\n"
                        "Send /qrlogin to login by scanning a QR code with your phone.\n\n"
                        "_This method is more reliable and doesn't require OTP._",
                        parse_mode=ParseMode.MARKDOWN
                    )
                    return
            except PhoneNumberInvalid:
                await phone_number_msg.reply("`PHONE_NUMBER` *is invalid.*", parse_mode=ParseMode.MARKDOWN)
                return
            except Exception as e:
                error_str = str(e).upper()
                
                # FIX Issue 6.4: Better FloodWait handling during login
                if "FLOOD" in error_str or hasattr(e, 'value') or hasattr(e, 'x'):
                    # Extract wait time
                    wait_time = getattr(e, 'value', getattr(e, 'x', 60))
                    if wait_time > 3600:
                        wait_str = f"{wait_time // 3600} soat {(wait_time % 3600) // 60} daqiqa"
                    elif wait_time > 60:
                        wait_str = f"{wait_time // 60} daqiqa"
                    else:
                        wait_str = f"{wait_time} soniya"
                    
                    await phone_number_msg.reply(
                        f"⏳ **Telegram tomonidan cheklangan**\n\n"
                        f"Bu telefon raqamiga juda ko'p so'rov yuborilgan.\n\n"
                        f"Iltimos **{wait_str}** kutib, qayta urinib ko'ring.\n\n"
                        f"_Yoki /qrlogin orqali kirish usulini sinab ko'ring._",
                        parse_mode=ParseMode.MARKDOWN
                    )
                    return
                
                if "EMAIL_SETUP_REQUIRED" in str(e) or "EMAIL" in error_str or "SENTCODETYPE" in error_str:
                    await phone_number_msg.reply(
                        "*OTP could not be sent to this account.*\n\n"
                        "This may be due to Telegram's security requirements.\n\n"
                        "*Alternative: Use QR Code Login*\n"
                        "Send /qrlogin to login by scanning a QR code with your phone.\n\n"
                        "_This method is more reliable and doesn't require OTP._",
                        parse_mode=ParseMode.MARKDOWN
                    )
                    return
                raise
            
            max_otp_attempts = 3
            for otp_attempt in range(max_otp_attempts):
                try:
                    remaining_attempts = max_otp_attempts - otp_attempt
                    phone_code_msg = await ask_message(
                        client=bot,
                        chat_id=user_id, 
                        text=(
                            "Please check for an OTP in official Telegram account.\n"
                            "If you got it, send OTP here after reading the format below.\n\n"
                            "If OTP is `12345`, *please send it as* `1 2 3 4 5`\n\n"
                            f"*Attempts remaining:* {remaining_attempts}\n"
                            "_Send /cancel to abort._"
                        ),
                        filters_arg=filters.text, 
                        timeout=300
                    )
                except TimeoutError:
                    return await message.reply("*Login cancelled due to timeout.*", parse_mode=ParseMode.MARKDOWN)
                
                if phone_code_msg.text == '/cancel':
                    return await phone_code_msg.reply("*Process cancelled!*", parse_mode=ParseMode.MARKDOWN)
                
                try:
                    phone_code = phone_code_msg.text.replace(" ", "")
                    try:
                        await client.invoke(
                            functions.auth.SignIn(
                                phone_number=phone_number,
                                phone_code_hash=phone_code_hash,
                                phone_code=phone_code
                            )
                        )
                        break
                    except Exception as sign_in_error:
                        if "SESSION_PASSWORD_NEEDED" in str(sign_in_error).upper() or isinstance(sign_in_error, SessionPasswordNeeded):
                            raise SessionPasswordNeeded()
                        raise
                except PhoneCodeInvalid:
                    if otp_attempt < max_otp_attempts - 1:
                        await phone_code_msg.reply("*OTP is invalid. Please try again.*", parse_mode=ParseMode.MARKDOWN)
                        continue
                    else:
                        await phone_code_msg.reply("*OTP is invalid. Maximum attempts reached.*", parse_mode=ParseMode.MARKDOWN)
                        return
                except PhoneCodeExpired:
                    await phone_code_msg.reply("*OTP is expired. Please use /login to start again.*", parse_mode=ParseMode.MARKDOWN)
                    return
                except SessionPasswordNeeded:
                    max_2fa_attempts = 3
                    for fa_attempt in range(max_2fa_attempts):
                        try:
                            remaining_2fa = max_2fa_attempts - fa_attempt
                            two_step_msg = await ask_message(
                                client=bot,
                                chat_id=user_id, 
                                text=(
                                    "*Your account has enabled two-step verification.*\n"
                                    "Please provide the password.\n\n"
                                    f"*Attempts remaining:* {remaining_2fa}\n"
                                    "_Send /cancel to abort._"
                                ),
                                filters_arg=filters.text, 
                                timeout=300
                            )
                        except TimeoutError:
                            return await message.reply("*Login cancelled due to timeout.*", parse_mode=ParseMode.MARKDOWN)
                        
                        if two_step_msg.text == '/cancel':
                            return await two_step_msg.reply("*Process cancelled!*", parse_mode=ParseMode.MARKDOWN)
                        
                        try:
                            password = two_step_msg.text
                            await client.check_password(password=password)
                            break
                        except PasswordHashInvalid:
                            if fa_attempt < max_2fa_attempts - 1:
                                await two_step_msg.reply("*Invalid password. Please try again.*", parse_mode=ParseMode.MARKDOWN)
                                continue
                            else:
                                await two_step_msg.reply("*Invalid password. Maximum attempts reached.*", parse_mode=ParseMode.MARKDOWN)
                                return
                    break
            
            string_session = await client.export_session_string()
            await client.disconnect()
            
            if len(string_session) < SESSION_STRING_SIZE:
                return await message.reply("*Invalid session string*", parse_mode=ParseMode.MARKDOWN)
            
            session_saved = False
            try:
                await async_db.update_user(message.from_user.id, {
                    'session': string_session,
                    'logged_in': True
                })
                session_saved = True
            except Exception as e:
                try:
                    save_session_to_file(user_id, string_session)
                    session_saved = True
                    await message.reply_text("*Warning: Database unavailable. Session saved to file.*", parse_mode=ParseMode.MARKDOWN)
                except Exception as file_error:
                    return await message.reply_text(f"*ERROR IN LOGIN:* `{e}`", parse_mode=ParseMode.MARKDOWN)
            
            if session_saved:
                await bot.send_message(
                    message.from_user.id,
                    "*Account Login Successfully.*\n\n"
                    "_If you get any error related to AUTH KEY, use /logout and /login again._",
                    parse_mode=ParseMode.MARKDOWN
                )
        finally:
            try:
                await client.disconnect()
            except:
                pass
            
            try:
                if os.path.exists(f"{session_path}.session"):
                    os.remove(f"{session_path}.session")
            except:
                pass


@Client.on_message(filters.private & ~filters.forwarded & filters.command(["qrlogin"]))
@check_banned_sync
async def qr_login_command(bot: Client, message: Message):
    """Login using QR code - alternative to phone number login"""
    
    if not is_qr_available():
        await message.reply(
            "*QR Code login is not available.*\n\n"
            "Please use /login with your phone number instead.",
            parse_mode=ParseMode.MARKDOWN
        )
        return
    
    await async_db.insert_user(message.from_user.id)
    user_data = await async_db.find_user(message.from_user.id)
    
    if get(user_data, 'logged_in', False):
        await message.reply(strings['already_logged_in'])
        return
    
    user_id = int(message.from_user.id)
    
    # ===== LOGIN RATE LIMITING =====
    can_login, wait_time, reason = await login_limiter.can_login(user_id)
    if not can_login:
        if wait_time > 60:
            wait_str = f"{wait_time / 60:.0f} daqiqa"
        else:
            wait_str = f"{wait_time:.0f} soniya"
        
        await message.reply(
            f"⏳ **Login cheklangan**\n\n"
            f"{reason}\n\n"
            f"Iltimos **{wait_str}** kuting."
        )
        return
    # ===== END RATE LIMITING =====
    
    status_msg = await message.reply(
        "*QR Code Login*\n\n"
        "Generating QR code...\n"
        "Please wait a moment.",
        parse_mode=ParseMode.MARKDOWN
    )
    
    try:
        success, session_string, error = await perform_qr_login(bot, message, user_id)
        
        if not success:
            await status_msg.edit_text(
                f"*Login Failed*\n\n"
                f"{error or 'Unknown error occurred.'}\n\n"
                "You can try:\n"
                "- /qrlogin - Try QR login again\n"
                "- /login - Use phone number login",
                parse_mode=ParseMode.MARKDOWN
            )
            return
        
        try:
            await async_db.update_user(message.from_user.id, {
                'session': session_string,
                'logged_in': True
            })
            
            await status_msg.edit_text(
                "*Login Successful!*\n\n"
                "Your account has been connected.\n"
                "You can now use the bot to save restricted content.\n\n"
                "_If you encounter AUTH KEY errors, use /logout and login again._",
                parse_mode=ParseMode.MARKDOWN
            )
            
        except Exception as e:
            await status_msg.edit_text(f"*Error saving session:* `{e}`", parse_mode=ParseMode.MARKDOWN)
            
    except Exception as e:
        cleanup_qr_files(user_id)
        await status_msg.edit_text(
            f"*Login Error*\n\n"
            f"`{str(e)}`\n\n"
            "Please try again with /qrlogin or /login",
            parse_mode=ParseMode.MARKDOWN
        )


# Don't Remove Credit Tg - @VJ_Botz
# Subscribe YouTube Channel For Amazing Bot https://youtube.com/@Tech_VJ
# Ask Doubt on telegram @KingVJ01
