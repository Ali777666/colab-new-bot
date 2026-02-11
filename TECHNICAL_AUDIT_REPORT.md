# Technical Audit Report - Telegram MTProto Bot (Pyrofork)
**Date:** 2026-01-25
**Auditor:** Technical Analysis

---

## Executive Summary

This audit covers the full architecture, integration, and runtime analysis of a Telegram MTProto bot built on Pyrofork. The project is functional with several identified issues requiring attention.

---

## 1. ARCHITECTURE & INTEGRATION AUDIT

### 1.1 Project Structure Analysis

| Module | Responsibility | Status |
|--------|----------------|--------|
| `main.py` | Bot class, lifecycle management | OK |
| `bot.py` | BotRunner with restart logic | OK |
| `app.py` | Flask web interface | **FIXED** |
| `config.py` | Configuration management | **SECURITY RISK** |
| `TechVJ/save.py` | Main download/upload handlers | OK |
| `TechVJ/generate.py` | Login/session generation | OK |
| `core/downloader/` | Download engine (workers, progress) | OK |
| `core/handlers.py` | Alternative command handlers | PARTIALLY USED |
| `database/async_db.py` | MongoDB async operations | OK |

### 1.2 Integration Issues Found

#### ISSUE #1: Duplicate Handler Systems (LOW)
- `TechVJ/save.py` and `core/handlers.py` both define `/start`, `/help`, `/stop` handlers
- `core/handlers.py` is NOT actively registered (safe)
- **Status:** No action needed - core/handlers.py appears to be legacy/alternative

#### ISSUE #2: Dead Code Paths
- `progress()`, `downstatus()`, `upstatus()` in `save.py` (lines 807-830) are deprecated stubs
- Legacy file-based progress system has been replaced with in-memory `progress_controller`
- **Status:** Safe - kept for backward compatibility

### 1.3 Module Integration Map

```
main.py (Bot)
  ├── TechVJ/save.py (main handlers - registered via plugins)
  │     ├── core/downloader/engine.py (DownloadEngine)
  │     ├── TechVJ/progress_controller.py (progress tracking)
  │     ├── TechVJ/session_handler.py (user session management)
  │     └── database/async_db.py (MongoDB operations)
  │
  ├── TechVJ/generate.py (login flow)
  │     ├── TechVJ/qr_login.py
  │     └── TechVJ/login_rate_limiter.py
  │
  └── core/downloader/ (download engine)
        ├── engine.py (main API)
        ├── worker.py (adaptive worker pool 32-150)
        └── progress.py (Pyrogram callback handling)
```

---

## 2. FUNCTIONAL VALIDATION

### 2.1 Commands Verified

| Command | Handler Location | Status |
|---------|------------------|--------|
| `/start` | TechVJ/save.py | OK |
| `/help` | TechVJ/save.py | OK |
| `/login` | TechVJ/generate.py | OK |
| `/logout` | TechVJ/generate.py | OK |
| `/stop` | TechVJ/save.py | OK |
| `/cancel` | TechVJ/save.py | OK (redirects to /stop) |
| `/ban` | TechVJ/save.py | OK (owner only) |
| `/unban` | TechVJ/save.py | OK (owner only) |
| `/comment` | TechVJ/save.py | OK |
| `/info` | TechVJ/info_command.py | OK |

### 2.2 Handler Lifecycle

- Handlers registered via `plugins=dict(root="TechVJ")` in Bot class
- `ask_message` handler installed lazily (one-time) - **CORRECT PATTERN**
- No handler add/remove conflicts detected

### 2.3 Background Tasks

| Task | Location | Status |
|------|----------|--------|
| Connection monitor | main.py | OK (safe_reconnect module) |
| Inactivity manager | TechVJ/inactivity_manager.py | OK |
| Worker pool scaler | core/downloader/worker.py | OK |

---

## 3. DOWNLOAD PIPELINE ANALYSIS

### 3.1 Progress Display Chain

```
User sends link
    ↓
save.py → download_and_send_media()
    ↓
engine.download() with status_message
    ↓
progress_tracker.create_callback()
    ↓
Pyrogram download_media(progress=callback)
    ↓
callback schedules async update via call_soon_threadsafe
    ↓
_update_message() edits Telegram message
```

### 3.2 Progress Issues Identified

#### ISSUE #3: Progress Not Displayed for Small Files (BY DESIGN)
- `download_and_send_media()` line 2428: `show_progress = file_size > 10 * 1024 * 1024`
- Files under 10MB don't show progress
- **Status:** Intentional design decision

#### ISSUE #4: Missing Download Progress Callback
- In `engine.download()`, progress callback is only created if `status_message` is provided
- When `status_message=None`, no progress tracking occurs
- **Root Cause:** `show_progress=False` for small files → no status_msg → no callback

**Recommendation:** For large files, progress IS shown. For small files (<10MB), no progress is intentional.

### 3.3 Upload Progress

- Upload progress callback created via `engine.create_progress_callback()`
- Correctly passed to `send_video()` and `send_document()`
- **Status:** Working correctly

---

## 4. PERFORMANCE & MEMORY OPTIMIZATION

### 4.1 Current Storage Strategy

| File Size | Strategy | Location |
|-----------|----------|----------|
| All files | Disk-based | `downloads/temp/` |

### 4.2 Recommended Smart Storage (NOT IMPLEMENTED)

For future optimization:
```python
# Suggested threshold
RAM_THRESHOLD = 50 * 1024 * 1024  # 50MB

# Small files: BytesIO buffer
# Large files: Disk streaming (current behavior)
```

**Status:** Current implementation uses disk for all files - safe and predictable.

### 4.3 Memory Leak Analysis

- Progress states cleaned up via `progress_tracker.cleanup(task_id)`
- Temp files cleaned in `finally` blocks
- User task tracking cleaned on completion
- **No memory leaks detected in happy path**

#### Potential Leak: Orphaned progress states
- If download crashes without cleanup, `_transfers` dict retains state
- **Mitigation:** `cleanup_all()` exists but should be called on bot stop

---

## 5. CONCURRENCY & WORKERS

### 5.1 Worker Configuration

| Component | Count | Scaling |
|-----------|-------|---------|
| core/downloader WorkerPool | 32-150 | Adaptive |
| core/download_manager | 150 | Fixed |
| Pyrogram Client workers | 16 | Fixed |

### 5.2 Concurrency Limits

```python
PER_USER_LIMIT = 5  # Max concurrent tasks per user
MAX_CONCURRENT_LOGINS = 2  # Global login limit
LOGIN_COOLDOWN_SECONDS = 60  # Between login attempts
```

### 5.3 Task Cancellation

- Cancel flags tracked per task_id
- `cancel_event` in DownloadTask for real cancellation
- Background tasks check `self._running` flag
- **Status:** Properly implemented

---

## 6. ERROR HANDLING & STABILITY

### 6.1 Critical Error Handlers

| Error | Handler | Status |
|-------|---------|--------|
| FloodWait | Exponential backoff | OK |
| AUTH_KEY_DUPLICATED | 10s+ delay before reconnect | OK |
| FileReferenceExpired | Message refresh + retry | OK |
| SessionInvalidError | User prompted to /login | OK |

### 6.2 Silent Failures Found

#### ISSUE #5: Bare `except:` Clauses
- Multiple instances of `except:` or `except Exception:` without logging
- Files affected: save.py, session_handler.py, album_collector.py

**Examples:**
```python
# save.py line 1033
except:
    pass  # Silently ignores error
```

**Recommendation:** Add `logger.debug()` or `logger.warning()` to track failures.

### 6.3 Timeout Handling

- Download timeout: 3600s (1 hour) - adequate for large files
- Connection timeout: 30s for reconnect attempts
- Session start timeout: varies by retry attempt
- **Status:** Reasonable defaults

---

## 7. ISSUES FIXED IN THIS AUDIT

### FIX #1: app.py - Incorrect asyncio.sleep() Usage

**Before (BROKEN):**
```python
def run_bot():
    try:
        bot = Bot()
        bot.run()
    except Exception as e:
        asyncio.sleep(5)  # WRONG: Called in sync context
        run_bot()  # WRONG: Recursive, can cause stack overflow
```

**After (FIXED):**
```python
def run_bot():
    while True:
        try:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            bot = Bot()
            bot.run()
            return
        except Exception as e:
            print(f"Bot crashed with error: {e}")
            time.sleep(5)  # CORRECT: sync sleep
```

---

## 8. SECURITY ISSUES

### CRITICAL: Hardcoded Secrets in config.py

```python
BOT_TOKEN = os.environ.get("BOT_TOKEN", "8340426105:AAH...")  # EXPOSED
API_HASH = os.environ.get("API_HASH", "62b630c71e...")  # EXPOSED
DB_URI = os.environ.get("DB_URI", "mongodb+srv://...")  # EXPOSED
```

**Risk:** If this code is pushed to public repository, all credentials are compromised.

**Recommendation:**
1. Remove default values with actual secrets
2. Use `.env` file with python-decouple (already in requirements)
3. Never commit real credentials

---

## 9. TEST RESULTS

```
$ python run_tests.py

Ran 11 tests in 0.235s
OK

Tests passed:
- TestDownloadTask (3 tests)
- TestDownloadStatus (1 test)
- TestProgressEvent (2 tests)
- TestSpeedTracker (2 tests)
- TestConfig (3 tests)
```

---

## 10. REMAINING RISKS & RECOMMENDATIONS

### HIGH Priority

1. **Rotate all exposed credentials immediately**
   - BOT_TOKEN, API_HASH, DB_URI are visible in config.py

2. **Add logging to bare except clauses**
   - Helps debug production issues

### MEDIUM Priority

3. **Implement periodic progress state cleanup**
   - Call `progress_tracker.cleanup_all()` on bot shutdown

4. **Consider RAM buffering for small files**
   - Files < 50MB could use BytesIO for faster processing

### LOW Priority

5. **Remove dead code in core/handlers.py**
   - Or document why it exists

6. **Add integration tests**
   - Current tests only cover models and config

---

## 11. CONCLUSION

The project is architecturally sound with proper separation of concerns. The download pipeline correctly uses thread-safe progress callbacks with throttling. One critical bug in `app.py` has been fixed.

**Primary concerns:**
1. Security: Hardcoded credentials must be rotated and removed
2. Observability: Silent exception handling reduces debuggability

**Overall assessment:** Production-ready with the fixes applied, pending credential rotation.

---

*Report generated: 2026-01-25*
