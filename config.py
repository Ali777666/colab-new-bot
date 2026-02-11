import os
import hashlib
from typing import Dict, Optional

# Bot token @Botfather
BOT_TOKEN = os.environ.get("BOT_TOKEN", "")

# Your API ID from my.telegram.org
API_ID = int(os.environ.get("API_ID", ""))

# Your API Hash from my.telegram.org
API_HASH = os.environ.get("API_HASH", "")

# Database 
DB_URI = os.environ.get("DB_URI", "")

# Owner Configuration
OWNER_ID = int(os.environ.get("OWNER_ID", "7256358665"))  # Replace with actual owner ID
OWNER_USERNAME = os.environ.get("OWNER_USERNAME", "secretfishka")  # Replace with actual username (without @)

# Message for banned/unauthorized users
BANNED_MESSAGE = f"To use this bot, please contact the owner: @{OWNER_USERNAME}"

# ============================================================================
# Force Subscription Configuration (Majburiy Obuna)
# ============================================================================
# Foydalanuvchilar botdan foydalanish uchun quyidagi kanallarga obuna bo'lishi kerak.
# Bo'sh ro'yxat = tekshirilmaydi
#
# Format:
#   - Ommaviy kanal: "@username" yoki "username"
#   - Shaxsiy kanal: chat_id (manfiy son) + invite_link
#
# Misol:
#   FORCE_SUB_CHANNELS = [
#       {"chat_id": "@mychannel", "title": "Asosiy kanal"},
#       {"chat_id": -1001234567890, "title": "VIP Kanal", "invite_link": "https://t.me/+abc123"},
#   ]
# ============================================================================

FORCE_SUB_ENABLED = os.environ.get("FORCE_SUB_ENABLED", "false").lower() == "true"

# Kanallar ro'yxati (bo'sh = tekshirilmaydi)
FORCE_SUB_CHANNELS = [
    # {"chat_id": "@your_channel", "title": "Asosiy Kanal"},
    # {"chat_id": -1001234567890, "title": "Shaxsiy Kanal", "invite_link": "https://t.me/+xxx"},
]

# Admin ID'lar - ular tekshirilmaydi
FORCE_SUB_ADMIN_IDS = [OWNER_ID]

# Temp directory base path for downloads
TEMP_DOWNLOAD_DIR = os.environ.get("TEMP_DOWNLOAD_DIR", "downloads/temp")

# Session Cleanup Configuration
CLEANUP_INTERVAL_HOURS = float(os.environ.get("CLEANUP_INTERVAL_HOURS", "1"))  # Default: 1 hour
MAX_CONCURRENT_VALIDATIONS = int(os.environ.get("MAX_CONCURRENT_VALIDATIONS", "5"))  # For free-tier MongoDB
VALIDATION_DELAY_SECONDS = float(os.environ.get("VALIDATION_DELAY_SECONDS", "1.0"))  # Telegram rate limit protection
MONGODB_OPERATION_DELAY = float(os.environ.get("MONGODB_OPERATION_DELAY", "0.2"))  # Free-tier rate limiting

# ============================================================================
# Login Rate Limiting Configuration
# ============================================================================
# CRITICAL: Prevents Telegram from blocking SMS codes due to too many login
# attempts from the same IP address. Production servers MUST use these limits.
#
# Telegram typically blocks after:
# - 5+ simultaneous login attempts from same IP
# - Rapid consecutive logins (< 30 sec apart)
# ============================================================================

# Maximum concurrent login attempts (across all users)
MAX_CONCURRENT_LOGINS = int(os.environ.get("MAX_CONCURRENT_LOGINS", "2"))

# Minimum delay between login attempts (seconds)
LOGIN_COOLDOWN_SECONDS = float(os.environ.get("LOGIN_COOLDOWN_SECONDS", "60"))

# Per-user login cooldown (seconds) - prevents same user spamming
USER_LOGIN_COOLDOWN = float(os.environ.get("USER_LOGIN_COOLDOWN", "300"))

# Maximum login attempts per user per hour
MAX_LOGIN_ATTEMPTS_PER_HOUR = int(os.environ.get("MAX_LOGIN_ATTEMPTS_PER_HOUR", "3"))

# ============================================================================
# MTProto Client Fingerprint Configuration
# ============================================================================
# SECURITY CRITICAL: Platform parameters MUST be consistent within each family.
# Mixing platforms (e.g., Android device_model with Desktop app_version) triggers
# Telegram security resets, forced logouts, and session invalidation.
#
# Rules:
# - Desktop: device_model="Desktop", system_version=OS, app_version=Desktop version
# - Android: device_model=Android device, system_version=Android ver, app_version=Android app ver
# - iOS: device_model=iPhone/iPad, system_version=iOS ver, app_version=iOS app ver
# ============================================================================

class ClientFingerprint:
    """
    Immutable, platform-consistent MTProto client fingerprint.
    
    SECURITY: All parameters are validated to belong to the same platform family.
    Once created for a user, the fingerprint MUST remain constant for the session lifetime.
    """
    
    # Platform-consistent fingerprint presets (verified safe combinations)
    # Updated: January 2026 - Telegram Desktop 5.x/6.x series
    # WHY DESKTOP? Desktop has no hardware IDs (IMEI, UDID), making it
    # safest for multi-user bots. Corporate use of multiple accounts is normal.
    PLATFORM_PRESETS = {
        'desktop_windows': {
            'device_model': 'Desktop',
            'system_version': 'Windows 11',
            'app_version': '5.9.0 x64',  # Stable version format
            'lang_code': 'en',
        },
        'desktop_macos': {
            'device_model': 'Desktop',
            'system_version': 'macOS 14.5',
            'app_version': '5.9.0',
            'lang_code': 'en',
        },
        'desktop_linux': {
            'device_model': 'Desktop',
            'system_version': 'Linux x86_64',
            'app_version': '5.9.0 x64',
            'lang_code': 'en',
        },
        'android_samsung': {
            'device_model': 'Samsung Galaxy S24',
            'system_version': 'SDK 34',
            'app_version': '11.4.2',
            'lang_code': 'en',
        },
        'android_pixel': {
            'device_model': 'Google Pixel 8',
            'system_version': 'SDK 34',
            'app_version': '11.4.2',
            'lang_code': 'en',
        },
        'ios_iphone': {
            'device_model': 'iPhone 15 Pro',
            'system_version': 'iOS 17.4',
            'app_version': '10.8.3',
            'lang_code': 'en',
        },
    }
    
    # Default platform for all sessions (ensures consistency)
    DEFAULT_PLATFORM = 'desktop_windows'
    
    def __init__(self, device_model: str, system_version: str, app_version: str, lang_code: str = 'en'):
        self._device_model = device_model
        self._system_version = system_version
        self._app_version = app_version
        self._lang_code = lang_code
        self._validate()
    
    def _validate(self) -> None:
        """Validate platform consistency. Raises ValueError on mismatch."""
        platform = self._detect_platform()
        if platform == 'unknown':
            raise ValueError(
                f"Invalid fingerprint: device_model='{self._device_model}', "
                f"system_version='{self._system_version}', app_version='{self._app_version}' "
                "do not form a consistent platform identity"
            )
    
    def _detect_platform(self) -> str:
        """Detect platform family from parameters."""
        dm_lower = self._device_model.lower()
        sv_lower = self._system_version.lower()
        
        # Desktop detection
        if 'desktop' in dm_lower or dm_lower in ('pc', 'telegram desktop'):
            if any(x in sv_lower for x in ('windows', 'macos', 'linux', 'ubuntu')):
                return 'desktop'
        
        # Android detection
        if any(x in dm_lower for x in ('samsung', 'pixel', 'oneplus', 'xiaomi', 'huawei', 'sony', 'oppo', 'vivo')):
            if 'android' in sv_lower or 'sdk' in sv_lower:
                return 'android'
        
        # iOS detection
        if any(x in dm_lower for x in ('iphone', 'ipad')):
            if 'ios' in sv_lower or 'ipados' in sv_lower:
                return 'ios'
        
        return 'unknown'
    
    @property
    def device_model(self) -> str:
        return self._device_model
    
    @property
    def system_version(self) -> str:
        return self._system_version
    
    @property
    def app_version(self) -> str:
        return self._app_version
    
    @property
    def lang_code(self) -> str:
        return self._lang_code
    
    def to_dict(self) -> Dict[str, str]:
        """Return fingerprint as dict for Pyrogram Client kwargs."""
        return {
            'device_model': self._device_model,
            'system_version': self._system_version,
            'app_version': self._app_version,
            'lang_code': self._lang_code,
        }
    
    @classmethod
    def from_preset(cls, preset_name: str = DEFAULT_PLATFORM) -> 'ClientFingerprint':
        """Create fingerprint from a verified preset."""
        if preset_name not in cls.PLATFORM_PRESETS:
            raise ValueError(f"Unknown preset: {preset_name}. Valid: {list(cls.PLATFORM_PRESETS.keys())}")
        params = cls.PLATFORM_PRESETS[preset_name]
        return cls(**params)
    
    @classmethod
    def get_default(cls) -> 'ClientFingerprint':
        """Get the default fingerprint (Desktop Windows)."""
        return cls.from_preset(cls.DEFAULT_PLATFORM)
    
    @classmethod
    def for_user(cls, user_id: int, platform: Optional[str] = None) -> 'ClientFingerprint':
        """
        Get a deterministic, consistent fingerprint for a user.
        
        SECURITY: The same user_id ALWAYS returns the same fingerprint,
        ensuring session fingerprint immutability across restarts.
        
        Args:
            user_id: Telegram user ID
            platform: Optional platform preference ('desktop', 'android', 'ios')
                      If None, uses DEFAULT_PLATFORM for consistency
        
        Returns:
            ClientFingerprint that is deterministic for this user_id
        """
        if platform is None:
            return cls.get_default()
        
        # SECURITY FIX: Always use desktop_windows for consistency
        # Multiple desktop platforms caused confusion in Telegram devices list
        if platform == 'desktop' or platform is None:
            return cls.from_preset('desktop_windows')
        
        platform_presets = {
            'android': ['android_samsung', 'android_pixel'],
            'ios': ['ios_iphone'],
        }
        
        if platform not in platform_presets:
            return cls.from_preset('desktop_windows')
        
        presets = platform_presets[platform]
        idx = int(hashlib.md5(str(user_id).encode()).hexdigest(), 16) % len(presets)
        return cls.from_preset(presets[idx])
    
    def __repr__(self) -> str:
        return f"ClientFingerprint(device={self._device_model}, os={self._system_version}, app={self._app_version})"


# Global default fingerprint - use this for ALL client creations
DEFAULT_CLIENT_FINGERPRINT = ClientFingerprint.get_default()


def get_client_params(user_id: Optional[int] = None) -> Dict[str, str]:
    """
    Get validated client parameters for MTProto sessions.
    
    SECURITY: Always returns platform-consistent parameters.
    
    Args:
        user_id: Optional user ID for deterministic fingerprint selection
    
    Returns:
        Dict with device_model, system_version, app_version, lang_code
    """
    if user_id is not None:
        return ClientFingerprint.for_user(user_id).to_dict()
    return DEFAULT_CLIENT_FINGERPRINT.to_dict()
