"""
Event Loop Configuration

Ensures proper asyncio event loop policy BEFORE any async code runs.
Must be imported FIRST in main entry point.

Windows: WindowsSelectorEventLoopPolicy (avoids Proactor issues)
Linux: Default policy (optionally uvloop for performance)
"""

import asyncio
import platform
import logging

logger = logging.getLogger(__name__)

_loop_configured = False


def configure_event_loop() -> None:
    """
    Configure event loop policy.
    
    MUST be called before ANY asyncio operations or imports
    that might create event loops.
    
    This function is idempotent - safe to call multiple times.
    """
    global _loop_configured
    
    if _loop_configured:
        return
    
    system = platform.system()
    
    if system == "Windows":
        # WindowsSelectorEventLoopPolicy is more stable for network I/O on Windows
        # Avoids issues with Proactor event loop and subprocess/socket operations
        if hasattr(asyncio, 'WindowsSelectorEventLoopPolicy'):
            current_policy = asyncio.get_event_loop_policy()
            if not isinstance(current_policy, asyncio.WindowsSelectorEventLoopPolicy):
                asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
                logger.info("Configured WindowsSelectorEventLoopPolicy for Windows")
    else:
        # On Linux/Unix, optionally use uvloop for better performance
        # uvloop is NOT in requirements.txt to avoid Windows issues
        try:
            import uvloop
            uvloop.install()
            logger.info("Installed uvloop for improved async performance")
        except ImportError:
            logger.debug("uvloop not available, using default event loop")
    
    _loop_configured = True


def get_running_loop_safe() -> asyncio.AbstractEventLoop:
    """
    Get the running event loop, or create a new one if none exists.
    
    This is safer than asyncio.get_event_loop() which is deprecated
    in Python 3.10+ for cases where no loop is running.
    """
    try:
        return asyncio.get_running_loop()
    except RuntimeError:
        # No running loop - this should only happen in synchronous contexts
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        return loop


def is_loop_running() -> bool:
    """Check if an event loop is currently running."""
    try:
        asyncio.get_running_loop()
        return True
    except RuntimeError:
        return False
