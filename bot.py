#!/usr/bin/env python3
"""
Synthesis Robot - Telegram Poll Solving Bot
Multi-Key Gemini Fallback: Gemini × up to 10 keys
"""

# ══════════════════════════════════════════════════════════════════
#  HEALTH SERVER
# ══════════════════════════════════════════════════════════════════
from http.server import HTTPServer, BaseHTTPRequestHandler
import threading, os

class HealthHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.send_header("Content-type", "text/plain")
        self.end_headers()
        self.wfile.write(b"Synthesis Robot is running!")
    def log_message(self, *args): pass

def start_health_server():
    port = int(os.getenv("PORT", 10000))
    HTTPServer(("0.0.0.0", port), HealthHandler).serve_forever()

# ══════════════════════════════════════════════════════════════════
#  IMPORTS
# ══════════════════════════════════════════════════════════════════
import json, asyncio, logging, re, time, itertools, uuid, random
import sqlite3, hashlib
import urllib.request as _req
import urllib.error as _url_err
from datetime import datetime
import datetime as _dt
import pytz
from telegram import (Update, InlineKeyboardButton, InlineKeyboardMarkup,
                      ReplyKeyboardMarkup, KeyboardButton, InputFile)
from pathlib import Path
from telegram.ext import (Application, CommandHandler, MessageHandler,
                           ChatMemberHandler, ContextTypes, filters,
                           ApplicationHandlerStop)
from telegram.constants import ParseMode
from telegram.error import BadRequest
from telegram.request import HTTPXRequest

logging.basicConfig(format='%(asctime)s - %(levelname)s - %(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)

# ══════════════════════════════════════════════════════════════════
#  TURSO DATABASE LAYER
#  Render restart হলেও সব data থাকবে।
#  Env vars: TURSO_URL, TURSO_AUTH_TOKEN
# ══════════════════════════════════════════════════════════════════
try:
    import libsql_client
    _TURSO_AVAILABLE = True
except ImportError:
    _TURSO_AVAILABLE = False
    logger.warning("⚠️ libsql-client not installed — Turso disabled. Run: pip install libsql-client")

TURSO_URL        = os.environ.get("TURSO_URL", "")
TURSO_AUTH_TOKEN = os.environ.get("TURSO_AUTH_TOKEN", "")

_turso_client = None

async def _get_turso():
    """Turso client singleton — lazy init."""
    global _turso_client
    if _turso_client is not None:
        return _turso_client
    if not _TURSO_AVAILABLE or not TURSO_URL:
        return None
    try:
        # libsql-client এর websocket mode (libsql:// / wss://) মাঝে মাঝে
        # "Invalid response status 400" error দেয় (known bug)। HTTP mode
        # (https://) ব্যবহার করলে এই সমস্যা হয় না, তাই scheme টা force করে
        # দেওয়া হচ্ছে যাতে যেভাবেই TURSO_URL দেওয়া থাকুক না কেন HTTP দিয়েই connect হয়।
        http_url = TURSO_URL
        if http_url.startswith("libsql://"):
            http_url = "https://" + http_url[len("libsql://"):]
        elif http_url.startswith("wss://"):
            http_url = "https://" + http_url[len("wss://"):]
        elif http_url.startswith("ws://"):
            http_url = "http://" + http_url[len("ws://"):]

        _turso_client = libsql_client.create_client(
            url=http_url,
            auth_token=TURSO_AUTH_TOKEN or None,
        )
        logger.info(f"✅ Turso client connected (http mode: {http_url})")
    except Exception as e:
        logger.error(f"Turso connect error: {e}")
        _turso_client = None
    return _turso_client


async def turso_exec(sql: str, args: tuple = ()):
    """Single statement execute — fire and forget style."""
    client = await _get_turso()
    if client is None:
        return None
    try:
        result = await client.execute(sql, list(args))
        return result
    except Exception as e:
        logger.error(f"Turso exec error: {e}\nSQL: {sql[:120]}")
        return None


async def turso_exec_many(stmts: list):
    """Batch execute list of (sql, args) tuples in one transaction."""
    client = await _get_turso()
    if client is None:
        return
    try:
        await client.batch([
            libsql_client.Statement(sql, list(args)) for sql, args in stmts
        ])
    except Exception as e:
        logger.error(f"Turso batch error: {e}")


async def init_turso_schema():
    """সব table create করো (idempotent)."""
    client = await _get_turso()
    if client is None:
        logger.warning("Turso not available — skipping schema init")
        return

    schema_stmts = [
        # ── Users ──
        ("""CREATE TABLE IF NOT EXISTS users (
            user_id     INTEGER PRIMARY KEY,
            name        TEXT,
            username    TEXT,
            joined      TEXT,
            poll_count  INTEGER DEFAULT 0,
            verified    INTEGER DEFAULT 0,
            last_active REAL DEFAULT 0
        )""", ()),

        # ── Rate limits (daily) ──
        ("""CREATE TABLE IF NOT EXISTS rate_limits (
            user_id   INTEGER PRIMARY KEY,
            date      TEXT,
            count     INTEGER DEFAULT 0,
            last_time REAL DEFAULT 0
        )""", ()),

        # ── Text Q&A daily limits (poll limit থেকে সম্পূর্ণ আলাদা) ──
        ("""CREATE TABLE IF NOT EXISTS qa_rate_limits (
            user_id INTEGER PRIMARY KEY,
            date    TEXT,
            count   INTEGER DEFAULT 0
        )""", ()),

        # ── Image (OCR) Q&A daily limits — text Q&A limit থেকে সম্পূর্ণ আলাদা ──
        ("""CREATE TABLE IF NOT EXISTS ocr_rate_limits (
            user_id INTEGER PRIMARY KEY,
            date    TEXT,
            count   INTEGER DEFAULT 0
        )""", ()),


        # ── Daily stats ──
        ("""CREATE TABLE IF NOT EXISTS daily_stats (
            date         TEXT PRIMARY KEY,
            polls_solved INTEGER DEFAULT 0,
            active_users TEXT DEFAULT '[]'
        )""", ()),

        # ── Referral bonus ──
        ("""CREATE TABLE IF NOT EXISTS referral_bonus (
            user_id     INTEGER PRIMARY KEY,
            date        TEXT,
            extra       INTEGER DEFAULT 0,
            count_today INTEGER DEFAULT 0
        )""", ()),

        # ── Admin-granted extra daily poll limit (resets daily, set by admin) ──
        ("""CREATE TABLE IF NOT EXISTS admin_extra_limit (
            user_id INTEGER PRIMARY KEY,
            date    TEXT,
            extra   INTEGER DEFAULT 0
        )""", ()),

        # ── Admin-granted extra daily TEXT Q&A limit (resets daily, set by admin) ──
        ("""CREATE TABLE IF NOT EXISTS admin_extra_text_limit (
            user_id INTEGER PRIMARY KEY,
            date    TEXT,
            extra   INTEGER DEFAULT 0
        )""", ()),

        # ── Admin-granted extra daily OCR (image) Q&A limit (resets daily, set by admin) ──
        ("""CREATE TABLE IF NOT EXISTS admin_extra_ocr_limit (
            user_id INTEGER PRIMARY KEY,
            date    TEXT,
            extra   INTEGER DEFAULT 0
        )""", ()),

        # ── Pending referrals (join করেছে কিন্তু এখনও verify করেনি) ──
        # কোনো TTL/expiry নেই — referral link কখনো expire হবে না, friend দেরিতে
        # verify করলেও referrer বোনাস পাবে। Turso-তে persist করার একমাত্র কারণ:
        # bot restart/redeploy হলে RAM-এ থাকা pending_referrals হারিয়ে না যায়।
        ("""CREATE TABLE IF NOT EXISTS pending_referrals (
            user_id     INTEGER PRIMARY KEY,
            referrer_id INTEGER NOT NULL,
            created_at  REAL DEFAULT 0
        )""", ()),

        # ── Poll cache ──
        ("""CREATE TABLE IF NOT EXISTS poll_cache (
            cache_key    TEXT PRIMARY KEY,
            question     TEXT NOT NULL,
            options_json TEXT NOT NULL,
            answer       TEXT NOT NULL,
            hit_count    INTEGER DEFAULT 0,
            created_at   REAL NOT NULL,
            last_hit_at  REAL NOT NULL
        )""", ()),

        ("CREATE INDEX IF NOT EXISTS idx_cache_created ON poll_cache(created_at)", ()),

        # ── Synthesis Library (whole tree stored as one JSON blob) ──
        ("""CREATE TABLE IF NOT EXISTS library_store (
            store_key  TEXT PRIMARY KEY,
            data_json  TEXT NOT NULL,
            updated_at REAL DEFAULT 0
        )""", ()),

        # ── Generic bot settings (report group, future toggles, etc.) ──
        ("""CREATE TABLE IF NOT EXISTS bot_settings (
            setting_key TEXT PRIMARY KEY,
            value       TEXT
        )""", ()),

        # ── Broadcast Poll Library — student-দের solve করা poll গুলো (clean tag +
        #    আমাদের join-link সহ) জমা হয়, পরে সবাইকে periodically পাঠানোর জন্য ──
        ("""CREATE TABLE IF NOT EXISTS broadcast_polls (
            poll_key     TEXT PRIMARY KEY,
            question     TEXT NOT NULL,
            options_json TEXT NOT NULL,
            correct_idx  INTEGER NOT NULL,
            explanation  TEXT NOT NULL,
            created_at   REAL NOT NULL
        )""", ()),

        # ── কোন user কে কোন broadcast poll আগে পাঠানো হয়েছে (uniqueness track) ──
        ("""CREATE TABLE IF NOT EXISTS broadcast_sent (
            user_id  INTEGER NOT NULL,
            poll_key TEXT NOT NULL,
            sent_at  REAL NOT NULL,
            PRIMARY KEY (user_id, poll_key)
        )""", ()),
    ]

    try:
        await turso_exec_many(schema_stmts)
        logger.info("✅ Turso schema initialized")
    except Exception as e:
        logger.error(f"Turso schema init error: {e}")

    # ── Migration: users টেবিলে lifetime qa_count/ocr_count কলাম যোগ করা ──
    # (CREATE TABLE IF NOT EXISTS আগে থেকে থাকা টেবিলে নতুন কলাম যোগ করে না,
    #  তাই PRAGMA দিয়ে চেক করে না থাকলে তবেই ALTER — বারবার restart-এ log
    #  spam/duplicate-column error এড়ানোর জন্য)
    try:
        pragma_rs = await client.execute("PRAGMA table_info(users)")
        existing_cols = {row[1] for row in pragma_rs.rows}
        if "qa_count" not in existing_cols:
            await turso_exec("ALTER TABLE users ADD COLUMN qa_count INTEGER DEFAULT 0")
        if "ocr_count" not in existing_cols:
            await turso_exec("ALTER TABLE users ADD COLUMN ocr_count INTEGER DEFAULT 0")
    except Exception as e:
        logger.error(f"users table qa_count/ocr_count migration error: {e}")

    # ── Migration: Streak System-এর জন্য নতুন কলাম যোগ করা ──
    # streak            = টানা কয়দিন (consecutive days) user bot ব্যবহার করছে
    # longest_streak    = এখন পর্যন্ত সর্বোচ্চ streak
    # active_days       = মোট কতদিন (distinct days) user bot ব্যবহার করেছে
    # last_active_date  = শেষ কোন তারিখে (Dhaka date, YYYY-MM-DD) activity হয়েছে
    try:
        pragma_rs2 = await client.execute("PRAGMA table_info(users)")
        existing_cols2 = {row[1] for row in pragma_rs2.rows}
        if "streak" not in existing_cols2:
            await turso_exec("ALTER TABLE users ADD COLUMN streak INTEGER DEFAULT 0")
        if "longest_streak" not in existing_cols2:
            await turso_exec("ALTER TABLE users ADD COLUMN longest_streak INTEGER DEFAULT 0")
        if "active_days" not in existing_cols2:
            await turso_exec("ALTER TABLE users ADD COLUMN active_days INTEGER DEFAULT 0")
        if "last_active_date" not in existing_cols2:
            await turso_exec("ALTER TABLE users ADD COLUMN last_active_date TEXT DEFAULT ''")
    except Exception as e:
        logger.error(f"users table streak migration error: {e}")

    # ── Migration: Engagement Notification System-এর জন্য নতুন কলাম যোগ করা ──
    # last_notify_date = শেষ কোন Dhaka date-এ engagement notification পাঠানো হয়েছে
    #                     (দিনে সর্বোচ্চ ১টা notification নিশ্চিত করতে ব্যবহার হয়)
    # last_notify_ts    = শেষ notification পাঠানোর timestamp (gap-based category-র
    #                     জন্য, যেমন ৪-৭ দিন বা ৭+ দিন inactive user)
    try:
        pragma_rs3 = await client.execute("PRAGMA table_info(users)")
        existing_cols3 = {row[1] for row in pragma_rs3.rows}
        if "last_notify_date" not in existing_cols3:
            await turso_exec("ALTER TABLE users ADD COLUMN last_notify_date TEXT DEFAULT ''")
        if "last_notify_ts" not in existing_cols3:
            await turso_exec("ALTER TABLE users ADD COLUMN last_notify_ts REAL DEFAULT 0")
    except Exception as e:
        logger.error(f"users table engagement-notify migration error: {e}")


# ── Load all data from Turso into in-memory dicts on startup ──

async def load_from_turso():
    """Bot start হলে Turso থেকে সব data in-memory-তে load করো।"""
    client = await _get_turso()
    if client is None:
        return

    global registered_users, rate_data, daily_stats, referral_bonus

    # Users
    try:
        rs = await client.execute(
            "SELECT user_id, name, username, joined, poll_count, verified, last_active, qa_count, ocr_count, "
            "streak, longest_streak, active_days, last_active_date, last_notify_date, last_notify_ts FROM users"
        )
        for row in rs.rows:
            uid = int(row[0])
            registered_users[uid] = {
                "name":        row[1] or "Unknown",
                "username":    row[2] or "N/A",
                "joined":      row[3] or "",
                "poll_count":  int(row[4] or 0),
                "verified":    bool(int(row[5] or 0)),
                "last_active": float(row[6] or 0),
                "qa_count":    int(row[7] or 0),
                "ocr_count":   int(row[8] or 0),
                "streak":            int(row[9] or 0) if len(row) > 9 else 0,
                "longest_streak":    int(row[10] or 0) if len(row) > 10 else 0,
                "active_days":       int(row[11] or 0) if len(row) > 11 else 0,
                "last_active_date":  (row[12] or "") if len(row) > 12 else "",
                "last_notify_date":  (row[13] or "") if len(row) > 13 else "",
                "last_notify_ts":    float(row[14] or 0) if len(row) > 14 else 0,
            }
            if registered_users[uid]["verified"]:
                verified_users.add(uid)
        logger.info(f"✅ Loaded {len(registered_users)} users from Turso")
    except Exception as e:
        logger.error(f"Turso load users error: {e}")

    # Rate limits
    try:
        rs = await client.execute("SELECT user_id, date, count, last_time FROM rate_limits")
        for row in rs.rows:
            rate_data[int(row[0])] = {
                "date":      row[1] or "",
                "count":     int(row[2] or 0),
                "last_time": float(row[3] or 0),
            }
        logger.info(f"✅ Loaded {len(rate_data)} rate limit entries from Turso")
    except Exception as e:
        logger.error(f"Turso load rate_limits error: {e}")

    # Text Q&A daily limits
    try:
        rs = await client.execute("SELECT user_id, date, count FROM qa_rate_limits")
        for row in rs.rows:
            qa_daily_data[int(row[0])] = {
                "date":  row[1] or "",
                "count": int(row[2] or 0),
            }
        logger.info(f"✅ Loaded {len(qa_daily_data)} Q&A daily limit entries from Turso")
    except Exception as e:
        logger.error(f"Turso load qa_rate_limits error: {e}")

    # Image (OCR) Q&A daily limits
    try:
        rs = await client.execute("SELECT user_id, date, count FROM ocr_rate_limits")
        for row in rs.rows:
            ocr_daily_data[int(row[0])] = {
                "date":  row[1] or "",
                "count": int(row[2] or 0),
            }
        logger.info(f"✅ Loaded {len(ocr_daily_data)} OCR daily limit entries from Turso")
    except Exception as e:
        logger.error(f"Turso load ocr_rate_limits error: {e}")


    # Daily stats
    try:
        rs = await client.execute("SELECT date, polls_solved, active_users FROM daily_stats")
        for row in rs.rows:
            d = row[0]
            try:
                active_set = set(json.loads(row[2] or "[]"))
            except Exception:
                active_set = set()
            daily_stats[d] = {
                "polls_solved": int(row[1] or 0),
                "active_users": active_set,
            }
        logger.info(f"✅ Loaded {len(daily_stats)} daily_stats days from Turso")
    except Exception as e:
        logger.error(f"Turso load daily_stats error: {e}")

    # Referral bonus
    try:
        rs = await client.execute("SELECT user_id, date, extra, count_today FROM referral_bonus")
        for row in rs.rows:
            referral_bonus[int(row[0])] = {
                "date":        row[1] or "",
                "extra":       int(row[2] or 0),
                "count_today": int(row[3] or 0),
            }
        logger.info(f"✅ Loaded {len(referral_bonus)} referral entries from Turso")
    except Exception as e:
        logger.error(f"Turso load referral_bonus error: {e}")

    # Pending referrals (join করেছে, এখনও verify করেনি — কখনো expire হয় না)
    try:
        rs = await client.execute("SELECT user_id, referrer_id FROM pending_referrals")
        for row in rs.rows:
            pending_referrals[int(row[0])] = int(row[1])
        logger.info(f"✅ Loaded {len(pending_referrals)} pending referrals from Turso")
    except Exception as e:
        logger.error(f"Turso load pending_referrals error: {e}")

    # Admin-granted extra daily limit (resets daily)
    try:
        rs = await client.execute("SELECT user_id, date, extra FROM admin_extra_limit")
        for row in rs.rows:
            admin_extra_limit[int(row[0])] = {
                "date":  row[1] or "",
                "extra": int(row[2] or 0),
            }
        logger.info(f"✅ Loaded {len(admin_extra_limit)} admin extra-limit entries from Turso")
    except Exception as e:
        logger.error(f"Turso load admin_extra_limit error: {e}")

    # Admin-granted extra daily TEXT Q&A limit (resets daily)
    try:
        rs = await client.execute("SELECT user_id, date, extra FROM admin_extra_text_limit")
        for row in rs.rows:
            admin_extra_text_limit[int(row[0])] = {
                "date":  row[1] or "",
                "extra": int(row[2] or 0),
            }
        logger.info(f"✅ Loaded {len(admin_extra_text_limit)} admin extra-text-limit entries from Turso")
    except Exception as e:
        logger.error(f"Turso load admin_extra_text_limit error: {e}")

    # Admin-granted extra daily OCR (image) Q&A limit (resets daily)
    try:
        rs = await client.execute("SELECT user_id, date, extra FROM admin_extra_ocr_limit")
        for row in rs.rows:
            admin_extra_ocr_limit[int(row[0])] = {
                "date":  row[1] or "",
                "extra": int(row[2] or 0),
            }
        logger.info(f"✅ Loaded {len(admin_extra_ocr_limit)} admin extra-ocr-limit entries from Turso")
    except Exception as e:
        logger.error(f"Turso load admin_extra_ocr_limit error: {e}")


# ── Save helpers (call these after every mutation) ──

_turso_bg_tasks: set = set()

# ── Retry queue: Turso সাময়িকভাবে unavailable থাকলে failed write গুলো এখানে জমা হয়,
#    আর _turso_retry_loop() পর্যায়ক্রমে সেগুলো আবার চেষ্টা করে। ──
_turso_retry_queue: list = []   # each item: {"factory": callable, "label": str, "attempts": int, "next_try": float}
MAX_TURSO_RETRY_ATTEMPTS = 8    # এর বেশি বার fail করলে queue থেকে বাদ (log করে)

def _turso_bg(factory, label: str = ""):
    """Fire-and-forget: asyncio task হিসেবে Turso write করো।

    `factory` অবশ্যই argument-less callable হতে হবে যেটা call করলে coroutine রিটার্ন করে
    (যেমন: lambda: _save_user(uid)) — coroutine সরাসরি না দিয়ে factory দেওয়া হয় কারণ
    ব্যর্থ হলে retry-র জন্য coroutine-টা আবার fresh ভাবে তৈরি করতে হয় (একটা coroutine
    object একবার await হয়ে গেলে দ্বিতীয়বার চালানো যায় না)।

    Task-এর reference সেটে রাখা হয় — না রাখলে asyncio মাঝপথে task garbage-collect
    করে ফেলতে পারে আর write টা silently হারিয়ে যায়। কোনো write fail করলে সেটা
    retry queue-তে যোগ হয়, পুরোপুরি হারিয়ে যায় না।"""

    async def _wrapped():
        try:
            await factory()
        except Exception as e:
            logger.warning(f"Turso bg write failed ({label or 'unknown'}): {e} — retry queue-তে যোগ করা হলো")
            _turso_retry_queue.append({
                "factory": factory,
                "label": label,
                "attempts": 0,
                "next_try": time.time() + 10,  # প্রথম retry ১০ সেকেন্ড পর
            })

    try:
        loop = asyncio.get_running_loop()
        task = loop.create_task(_wrapped())
        _turso_bg_tasks.add(task)
        task.add_done_callback(_turso_bg_tasks.discard)
    except RuntimeError:
        # কোনো running loop নেই (খুবই rare) — সরাসরি sync-ভাবে চালিয়ে দাও
        try:
            asyncio.run(_wrapped())
        except Exception as e:
            logger.warning(f"Turso bg task error: {e}")


async def _turso_retry_loop():
    """প্রতি ৩০ সেকেন্ডে retry queue চেক করে, exponential backoff সহ ব্যর্থ Turso write গুলো
    আবার চেষ্টা করে। Turso সাময়িকভাবে down থাকলেও data eventually sync হয়ে যাবে।"""
    while True:
        await asyncio.sleep(30)
        if not _turso_retry_queue:
            continue

        now = time.time()
        still_pending = []
        for item in _turso_retry_queue:
            if now < item["next_try"]:
                still_pending.append(item)
                continue
            try:
                await item["factory"]()
                logger.info(f"✅ Turso retry succeeded ({item['label'] or 'unknown'}) after {item['attempts'] + 1} attempt(s)")
            except Exception as e:
                item["attempts"] += 1
                if item["attempts"] >= MAX_TURSO_RETRY_ATTEMPTS:
                    logger.error(
                        f"❌ Turso retry giving up ({item['label'] or 'unknown'}) after "
                        f"{item['attempts']} attempts: {e}"
                    )
                    continue  # queue থেকে বাদ
                # Exponential backoff: 10s, 20s, 40s, ... সর্বোচ্চ 10 মিনিট
                backoff = min(10 * (2 ** item["attempts"]), 600)
                item["next_try"] = now + backoff
                still_pending.append(item)
                logger.warning(
                    f"Turso retry failed again ({item['label'] or 'unknown'}), "
                    f"attempt {item['attempts']}, next try in {backoff}s: {e}"
                )

        _turso_retry_queue[:] = still_pending
        if _turso_retry_queue:
            logger.info(f"🔁 Turso retry queue: {len(_turso_retry_queue)} pending write(s)")


async def _save_user(user_id: int):
    info = registered_users.get(user_id)
    if info is None:
        return
    await turso_exec(
        """INSERT OR REPLACE INTO users
           (user_id, name, username, joined, poll_count, verified, last_active, qa_count, ocr_count,
            streak, longest_streak, active_days, last_active_date, last_notify_date, last_notify_ts)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (user_id, info.get("name"), info.get("username"),
         info.get("joined"), info.get("poll_count", 0),
         1 if info.get("verified") else 0,
         info.get("last_active", 0),
         info.get("qa_count", 0),
         info.get("ocr_count", 0),
         info.get("streak", 0),
         info.get("longest_streak", 0),
         info.get("active_days", 0),
         info.get("last_active_date", ""),
         info.get("last_notify_date", ""),
         info.get("last_notify_ts", 0))
    )

async def _save_qa_rate(user_id: int):
    entry = qa_daily_data.get(user_id)
    if entry is None:
        return
    await turso_exec(
        """INSERT OR REPLACE INTO qa_rate_limits (user_id, date, count)
           VALUES (?, ?, ?)""",
        (user_id, entry["date"], entry["count"])
    )

async def _save_ocr_rate(user_id: int):
    entry = ocr_daily_data.get(user_id)
    if entry is None:
        return
    await turso_exec(
        """INSERT OR REPLACE INTO ocr_rate_limits (user_id, date, count)
           VALUES (?, ?, ?)""",
        (user_id, entry["date"], entry["count"])
    )

async def _save_rate(user_id: int):

    entry = rate_data.get(user_id)
    if entry is None:
        return
    await turso_exec(
        """INSERT OR REPLACE INTO rate_limits (user_id, date, count, last_time)
           VALUES (?, ?, ?, ?)""",
        (user_id, entry["date"], entry["count"], entry["last_time"])
    )

async def _save_daily_stats(date: str):
    data = daily_stats.get(date)
    if data is None:
        return
    active_json = json.dumps(list(data.get("active_users", set())))
    await turso_exec(
        """INSERT OR REPLACE INTO daily_stats (date, polls_solved, active_users)
           VALUES (?, ?, ?)""",
        (date, data.get("polls_solved", 0), active_json)
    )

async def _save_referral(user_id: int):
    entry = referral_bonus.get(user_id)
    if entry is None:
        return
    await turso_exec(
        """INSERT OR REPLACE INTO referral_bonus (user_id, date, extra, count_today)
           VALUES (?, ?, ?, ?)""",
        (user_id, entry["date"], entry["extra"], entry["count_today"])
    )

async def _save_pending_referral(user_id: int):
    """pending_referrals-এ নতুন entry persist করো (TTL নেই, কখনো নিজে থেকে expire হবে না)।"""
    referrer_id = pending_referrals.get(user_id)
    if referrer_id is None:
        return
    await turso_exec(
        """INSERT OR REPLACE INTO pending_referrals (user_id, referrer_id, created_at)
           VALUES (?, ?, ?)""",
        (user_id, referrer_id, time.time())
    )

async def _delete_pending_referral(user_id: int):
    """Verify হয়ে গেলে (বা invalid হলে) pending_referrals থেকে Turso-তেও মুছে দাও।"""
    await turso_exec("DELETE FROM pending_referrals WHERE user_id = ?", (user_id,))

async def _save_admin_extra_limit(user_id: int):
    """আজকের জন্য admin-এর দেওয়া extra daily poll limit Turso-তে persist করো (date-scoped)।"""
    entry = admin_extra_limit.get(user_id)
    if entry is None or entry.get("extra", 0) <= 0:
        await turso_exec("DELETE FROM admin_extra_limit WHERE user_id = ?", (user_id,))
        return
    await turso_exec(
        """INSERT OR REPLACE INTO admin_extra_limit (user_id, date, extra)
           VALUES (?, ?, ?)""",
        (user_id, entry["date"], entry["extra"])
    )

async def _save_admin_extra_text_limit(user_id: int):
    """আজকের জন্য admin-এর দেওয়া extra daily TEXT Q&A limit Turso-তে persist করো (date-scoped)।"""
    entry = admin_extra_text_limit.get(user_id)
    if entry is None or entry.get("extra", 0) <= 0:
        await turso_exec("DELETE FROM admin_extra_text_limit WHERE user_id = ?", (user_id,))
        return
    await turso_exec(
        """INSERT OR REPLACE INTO admin_extra_text_limit (user_id, date, extra)
           VALUES (?, ?, ?)""",
        (user_id, entry["date"], entry["extra"])
    )

async def _save_admin_extra_ocr_limit(user_id: int):
    """আজকের জন্য admin-এর দেওয়া extra daily OCR (image) Q&A limit Turso-তে persist করো (date-scoped)।"""
    entry = admin_extra_ocr_limit.get(user_id)
    if entry is None or entry.get("extra", 0) <= 0:
        await turso_exec("DELETE FROM admin_extra_ocr_limit WHERE user_id = ?", (user_id,))
        return
    await turso_exec(
        """INSERT OR REPLACE INTO admin_extra_ocr_limit (user_id, date, extra)
           VALUES (?, ?, ?)""",
        (user_id, entry["date"], entry["extra"])
    )

async def _save_library_to_turso():
    """পুরো Synthesis Library tree (JSON) Turso-তে save করো, যাতে redeploy-তে
    local disk মুছে গেলেও library data হারিয়ে না যায়।"""
    await turso_exec(
        """INSERT OR REPLACE INTO library_store (store_key, data_json, updated_at)
           VALUES (?, ?, ?)""",
        ("library", json.dumps(library_data, ensure_ascii=False), time.time())
    )

async def load_library_from_turso():
    """Bot start হলে Turso থেকে library tree load করো (local file fallback হিসেবে থাকবে)।"""
    global library_data
    client = await _get_turso()
    if client is None:
        return
    try:
        rs = await client.execute("SELECT data_json FROM library_store WHERE store_key = ?", ["library"])
        if rs.rows:
            data = json.loads(rs.rows[0][0])
            if "root" in data:
                library_data = data
                logger.info(f"✅ Loaded library ({len(library_data)} nodes) from Turso")
                return
        logger.info("ℹ️ No library data in Turso yet — using local/default")
    except Exception as e:
        logger.error(f"Turso load library error: {e}")


async def _save_setting(key: str, value):
    """যেকোনো single bot setting (যেমন REPORT_GROUP_ID) Turso-তে save করো,
    যাতে redeploy-তে admin-এর সেট করা সব config টিকে থাকে।"""
    await turso_exec(
        "INSERT OR REPLACE INTO bot_settings (setting_key, value) VALUES (?, ?)",
        (key, json.dumps(value))
    )

async def load_settings_from_turso():
    """Bot start হলে Turso থেকে সব bot_settings load করো।"""
    global REPORT_GROUP_ID
    client = await _get_turso()
    if client is None:
        return
    try:
        rs = await client.execute("SELECT setting_key, value FROM bot_settings")
        for row in rs.rows:
            key, raw = row[0], row[1]
            try:
                value = json.loads(raw)
            except Exception:
                value = raw
            if key == "REPORT_GROUP_ID":
                REPORT_GROUP_ID = int(value)
        logger.info(f"✅ Loaded bot settings from Turso (REPORT_GROUP_ID={REPORT_GROUP_ID})")
    except Exception as e:
        logger.error(f"Turso load settings error: {e}")

# ══════════════════════════════════════════════════════════════════
#  CONFIG
# ══════════════════════════════════════════════════════════════════
BOT_TOKEN      = os.environ.get("BOT_TOKEN", "")
ADMIN_ID       = int(os.environ.get("ADMIN_ID", "0"))
BOT_NAME       = "Synthesis Robot"

# ── Report Group Config ──
REPORT_GROUP_ID: int = int(os.environ.get("REPORT_GROUP_ID", "0"))

# ══════════════════════════════════════════════════════════════════
#  MULTI-PROVIDER API KEY POOL
#  Render-এ এই env variables গুলো set করতে হবে।
#  Key না থাকলে সেই provider skip হবে — error হবে না।
# ══════════════════════════════════════════════════════════════════

def _split_env_values(value: str) -> list:
    """Comma/newline separated env value থেকে clean unique list বানায়."""
    if not value:
        return []
    parts = re.split(r"[,;|\n\r\t ]+", value)
    cleaned = []
    seen = set()
    for part in parts:
        item = part.strip().strip('"').strip("'")
        if item and item not in seen:
            seen.add(item)
            cleaned.append(item)
    return cleaned


def _env_list(name: str, defaults: list) -> list:
    values = _split_env_values(os.environ.get(name, ""))
    return values if values else list(defaults)


# Model list env দিয়ে override করা যাবে, যেমন:
# GEMINI_MODELS=gemini-2.5-flash,gemini-2.0-flash
GEMINI_MODELS = _env_list("GEMINI_MODELS", [
    "gemini-2.5-flash",
    "gemini-2.0-flash",
    "gemini-1.5-flash",
])


class ProviderError(Exception):
    """Provider/API error; status_code থাকলে fallback logic বুঝতে পারে।
    daily_quota=True মানে এটা আসল দৈনিক (RPD) quota শেষ — সারাদিনের জন্য key বন্ধ রাখা উচিত।
    daily_quota=False (default) মানে এটা সাময়িক burst/RPM (per-minute) limit — কিছুক্ষণ
    cooldown দিয়ে আবার এই key-ই ব্যবহার করা যাবে।"""
    def __init__(self, message: str, status_code: int | None = None, daily_quota: bool = False):
        super().__init__(message)
        self.status_code = status_code
        self.daily_quota = daily_quota


def _classify_429(raw_detail: str) -> bool:
    """
    Gemini free-tier 429 (RESOURCE_EXHAUSTED) দুই ধরনের কারণে আসতে পারে —
    per-minute (RPM) burst limit, বা per-day (RPD) quota একদমই শেষ। দুটোরই
    top-level message একই ("Resource has been exhausted..."), কিন্তু error
    body-র ভেতরের QuotaFailure/quotaId-এ "PerDay" বা "PerMinute" উল্লেখ থাকে।
    এই ফাংশন সেটা দেখে বুঝে নেয় real daily exhaustion কিনা।
    "PerDay" পেলে True (সত্যিকারের দৈনিক limit শেষ)।
    না পেলে (RPM burst বা অজানা) False ধরা হয়, যাতে ভুলে পুরো দিনের জন্য key বন্ধ না হয়ে যায়।
    """
    text = (raw_detail or "")
    if re.search(r"per\s*day", text, re.IGNORECASE):
        return True
    if re.search(r"per\s*minute", text, re.IGNORECASE):
        return False
    # কোনো hint না পেলে safe default: RPM burst ধরে নাও (short cooldown),
    # যাতে সাময়িক spike-এ পুরো key pool কেই সারাদিনের জন্য হারাতে না হয়।
    return False


def _mask_secret(value: str) -> str:
    if not value:
        return "empty"
    if len(value) <= 10:
        return value[:2] + "***"
    return value[:6] + "..." + value[-4:]


def _collect_keys(prefix: str, single_name: str) -> list:
    """
    Flexible env support:
    - GEMINI_API_KEY
    - GEMINI_API_KEY_1 ... GEMINI_API_KEY_20
    - GEMINI_API_KEYS (comma/newline separated)
    """
    keys = []
    keys.extend(_split_env_values(os.environ.get(f"{prefix}_API_KEYS", "")))
    keys.extend(_split_env_values(os.environ.get(single_name, "")))
    for i in range(1, 21):
        keys.extend(_split_env_values(os.environ.get(f"{prefix}_API_KEY_{i}", "")))

    unique = []
    seen = set()
    for key in keys:
        if key and key not in seen:
            seen.add(key)
            unique.append(key)
    return unique


def _build_api_pool() -> list:
    """
    সব available API keys দিয়ে provider list তৈরি করে।
    আগে কোড শুধু _1/_2 নামের env ধরত; এখন single, numbered এবং comma-list—সব support করে।

    NOTE: "label" (Gemini#1, Gemini#2...) পুরোপুরি positional — .env-এ কোনো key
    add/remove/reorder করলে সব label শিফট হয়ে যায় (যেমন key #1 delete করলে আগের
    #2 এখন #1 হয়ে যায়)। কিন্তু provider_stats (success/fail/limited/cycle) Turso-তে
    persist হয় এবং bot restart হলে ফিরে লোড হয় — তাই label-কে ID হিসেবে ব্যবহার করলে
    redeploy-এর পর ভুল key-র সাথে ভুল পুরনো history (stale success/fail/limited) জুড়ে
    যায়। এটা এড়াতে প্রতিটা key-র আসল মান থেকে একটা stable hash ("stat_id") বানানো
    হচ্ছে, যেটা key না বদলালে কখনো বদলাবে না (env-এ position যাই হোক না কেন) —
    provider_stats, cooldown streak ইত্যাদি সব এই stat_id দিয়েই ট্র্যাক হবে, শুধু
    display-এর জন্য label ব্যবহার হবে।
    """
    pool = []

    for idx, key in enumerate(_collect_keys("GEMINI", "GEMINI_API_KEY"), start=1):
        stat_id = hashlib.sha256(key.encode("utf-8")).hexdigest()[:12]
        pool.append({"type": "gemini", "key": key, "label": f"Gemini#{idx}", "stat_id": stat_id})

    return pool


API_POOL: list = _build_api_pool()

# ── Sequential active-key model ──
# আগে round-robin ছিল, ফলে সব key একসাথেই ব্যবহার হতো (Gemini#1,2,3... সবাই সমান হারে)।
# এখন: সব user/request একই "active" key ব্যবহার করবে, যতক্ষণ না সেই key
# rate-limited (429) হয় — তখনই bot পরের key-তে shift করবে, তার আগে না।
# _active_idx এবং _dead_keys global/shared, তাই একটা key 429 খেলে
# পরের সব call (অন্য user-এর হলেও) নতুন key ব্যবহার করবে।
_active_idx  = 0
_active_lock = asyncio.Lock()
_dead_keys: set = set()   # 401/403 (invalid) key — permanently skip

# ── RPM burst cooldown (NEW) ──
# 429 পেলেই যদি key-কে সাথে সাথে সারাদিনের জন্য "limited" মেরে দেওয়া হয়, তাহলে একসাথে
# অনেক poll (burst traffic) এলে প্রতিটা key-ই পর পর কয়েক সেকেন্ডে RPM (per-minute) limit
# খেয়ে পুরো ১৪টা key-ই একসাথে "শেষ" দেখায় — যদিও আসলে তাদের দৈনিক quota-র বেশিরভাগই বাকি
# থাকে। তাই real daily (RPD) exhaustion আর সাময়িক RPM burst আলাদা করে ট্র্যাক করা হচ্ছে:
# RPM burst হলে key শুধু অল্প সময় (COOLDOWN_SECS) এর জন্য skip হবে, সারাদিনের জন্য না।
_key_cooldowns: dict = {}   # {key: unix_ts যতক্ষণ পর্যন্ত এই key সাময়িকভাবে skip করা হবে}
RPM_COOLDOWN_SECS = int(os.environ.get("GEMINI_RPM_COOLDOWN_SECS", "30"))

# ── 429 misclassification fallback (NEW) ──
# Google মাঝে মাঝে real দৈনিক (RPD) quota exhaustion-এও সেই generic
# "Resource has been exhausted (e.g. check quota)." message দেয়, response-এ
# "PerDay"/"PerMinute" breakdown ছাড়াই — তখন _classify_429() ভুল করে সেটাকে
# সাময়িক RPM burst ধরে নেয় (safe default)। ফলে key repeatedly ৩০s cooldown
# শেষে আবার 429 খায়, "limited" কখনো mark হয় না (admin notify হয় না, সঠিক
# reset time দেখা যায় না), আর শেষে সব round শেষ হয়ে key শুধু fail/fail
# দেখিয়ে "💀 dead" এর মতো দেখায় — যদিও আসল কারণ daily quota।
# ফিক্স: একই key পরপর ২ বার 429 (RPM cooldown পার হয়েও) খেলে সেটাকে আর
# সাময়িক ধরা হয় না — real daily exhaustion হিসেবে মেনে নিয়ে সঠিকভাবে
# "limited" mark ও admin-notify করা হয়।
_key_429_streak: dict = {}   # {stat_id: consecutive 429 count since last success}

def _classify_daily_with_fallback(stat_id: str, daily_quota_hint: bool) -> bool:
    if daily_quota_hint:
        _key_429_streak[stat_id] = 0
        return True
    streak = _key_429_streak.get(stat_id, 0) + 1
    _key_429_streak[stat_id] = streak
    if streak >= 2:
        logger.warning(
            f"⚠️ key({stat_id}): consecutive 429 even after cooldown — সম্ভবত real daily quota "
            f"exhaustion (Google-এর response-এ PerDay/PerMinute hint ছিল না), তাই fallback "
            f"হিসেবে daily quota হিসেবেই ধরা হচ্ছে"
        )
        return True
    return False

_PACIFIC_TZ = pytz.timezone("US/Pacific")

def _gemini_daily_reset_ts(limited_at: float) -> float:
    """
    একটা key ঠিক কখন (real daily RPD quota শেষ হয়ে) 'limited' হয়েছিল (limited_at),
    সেটার ভিত্তিতে ওই নির্দিষ্ট key কবে reset হবে (next midnight Pacific Time) — তার
    epoch timestamp হিসেব করে দেয়। প্রতিটা key আলাদা সময়ে exhausted হয়েছে মানে
    প্রতিটার reset timestamp-ও আলাদা আলাদা হবে (কোনো shared/global reset time না)।
    """
    if not limited_at:
        return 0
    pac_now = datetime.fromtimestamp(limited_at, _PACIFIC_TZ)
    pac_next_midnight = (pac_now + _dt.timedelta(days=1)).replace(
        hour=0, minute=0, second=0, microsecond=0
    )
    return pac_next_midnight.timestamp()

# একই সময়ে active key-তে সর্বোচ্চ কতগুলো concurrent request যেতে পারবে। burst traffic-এ
# (অনেক poll একসাথে forward হলে) এই cap ছাড়া একটা key-র RPM limit কয়েক সেকেন্ডেই শেষ হয়ে
# যায়, যার ফলে bot দ্রুত পরের key-তে shift করে — সেটাও একইভাবে উড়ে যায়, cascade হয়ে পুরো
# pool শেষ হয়ে যায়। Semaphore দিয়ে throughput smooth রাখলে প্রতিটা key তার real RPM
# limit-এর মধ্যেই থাকে, ফলে key-গুলো আলাদা আলাদা সময়ে (একে একে, real usage অনুযায়ী) শেষ হবে।
_GEMINI_CONCURRENCY = int(os.environ.get("GEMINI_MAX_CONCURRENT_PER_KEY", "4"))
_key_semaphore = asyncio.Semaphore(_GEMINI_CONCURRENCY)

# ── Soft daily pacing cap (NEW) ──
# সমস্যা: sequential model-এ Gemini#1 real RPD limit না খাওয়া পর্যন্ত একাই সব
# traffic নেয় — busy সময়ে (exam hour ইত্যাদি) এটা কয়েক ঘণ্টার মধ্যেই পুরোপুরি শেষ
# হয়ে যেতে পারে, এবং একে একে বাকি key-গুলোও একই busy window-এ শেষ হয়ে যাওয়ার
# ঝুঁকি থাকে — ফলে দিনের বাকি অংশে (পরের key-গুলোর reset না আসা পর্যন্ত) bot পুরোপুরি
# ডাউন থাকতে পারে। GEMINI_DAILY_SOFT_CAP_PER_KEY সেট করা থাকলে, বাস্তব 429 আসার
# আগেই (জানা/অনুমিত real RPD-এর চেয়ে কমে) key-কে proactively "resting" করে দেওয়া
# হয়, যাতে ১৪টা key-ই কোনো এক busy সময়ে একসাথে শেষ না হয়ে সারাদিন জুড়ে ছড়িয়ে
# ব্যবহার হয় এবং সবসময় অন্তত একটা key সচল থাকার সম্ভাবনা বাড়ে।
# 0 (default) মানে disabled — শুধু real 429 না আসা পর্যন্ত key ব্যবহার হবে।
GEMINI_DAILY_SOFT_CAP_PER_KEY = int(os.environ.get("GEMINI_DAILY_SOFT_CAP_PER_KEY", "0"))
_key_daily_count: dict = {}   # {stat_id: {"date": "YYYY-MM-DD" (Pacific), "count": int}}


def _pacific_date_str(ts: float | None = None) -> str:
    dt = datetime.fromtimestamp(ts if ts else time.time(), _PACIFIC_TZ)
    return dt.strftime("%Y-%m-%d")


def _bump_soft_daily_count(stat_id: str) -> int:
    """এই key (stat_id দিয়ে চেনা)-র জন্য আজকের (Pacific date) successful-call counter ১ বাড়ায়,
    date বদলে গেলে counter reset করে দেয়। নতুন count ফেরত দেয়।"""
    today = _pacific_date_str()
    entry = _key_daily_count.get(stat_id)
    if entry is None or entry.get("date") != today:
        entry = {"date": today, "count": 0}
        _key_daily_count[stat_id] = entry
    entry["count"] += 1
    return entry["count"]


def _maybe_daily_stat_reset(entry: dict) -> bool:
    """
    Gemini free-tier RPD quota Google-এর দিক থেকে প্রতিদিন midnight Pacific Time
    (≈ দুপুর ১টা, Dhaka time — DST অনুযায়ী কিছুটা এদিক-ওদিক হতে পারে) reset হয়,
    key সেদিন quota শেষ করুক বা না করুক — এটা bot এই key দিয়ে কল করেছে কিনা তার
    উপর নির্ভর করে না।

    আগে provider_stats-এর success/fail (ও cycle count) শুধু key আসলে 429
    (limited) খেলেই reset হতো — ফলে যেসব key কখনো limit খায়নি (যেমন সবসময়
    active থাকা Gemini#1), তাদের success/fail সংখ্যা দিনের পর দিন জমতেই থাকতো
    (lifetime counter), যদিও Google-এর real RPD window ইতিমধ্যে reset হয়ে
    গিয়েছিল। এই ফাংশন সেটা ঠিক করে — প্রতিটা key-র entry-তে শেষ কবে reset হয়েছে
    (Pacific date হিসেবে) তা track করে, date বদলে গেলে সব counter শূন্য করে দেয়,
    যাতে /apistatus সবসময় "আজকের" (Google-এর real quota window অনুযায়ী) usage
    দেখায়, lifetime accumulated সংখ্যা না।

    Return করে: reset হয়েছিল কিনা (True/False) — caller দরকার হলে log/notify করতে পারে।
    """
    today = _pacific_date_str()
    if entry.get("reset_date") == today:
        return False
    was_first_time = "reset_date" not in entry
    entry["reset_date"] = today
    if was_first_time:
        # bot প্রথমবার এই key দেখছে (fresh entry) — reset date শুধু বসিয়ে দিলেই হলো,
        # counter গুলো এমনিতেই ০ থেকে শুরু, আলাদা করে reset log করার দরকার নেই।
        return False
    entry["success"] = 0
    entry["fail"] = 0
    entry["poll_success"] = 0
    entry["text_success"] = 0
    entry["ocr_success"] = 0
    entry["poll_fail"] = 0
    entry["text_fail"] = 0
    entry["ocr_fail"] = 0
    entry["solved_since_reset"] = 0
    entry["last_cycle_solved"] = 0
    entry["cooldowns"] = 0
    entry["limited"] = False
    entry["limited_at"] = 0.0
    entry["last_error"] = ""
    return True


def _is_still_limited(stat: dict) -> bool:
    """
    একটা key-র stat entry দেখে সত্যিকারের এই মুহূর্তের limited status ফেরত দেয়।
    Reset time (নিজস্ব limited_at থেকে হিসাব করা পরের Pacific midnight) পার হয়ে
    গেলে stat['limited'] নিজে থেকেই False করে দেয় — শুধু _get_active_provider()
    ব্যবহার করার সময় না, /apistatus-এর মতো read-only display command থেকেও কল
    করা যায় যাতে rotation না ঘুরলেও stale "LIMIT EXCEEDED" দেখা না যায়
    (আগে Gemini#1 কাজ করতে থাকলে rotation কখনো বাকি key পর্যন্ত পৌঁছাতোই না,
    ফলে তাদের reset time পার হয়ে গেলেও ফ্ল্যাগ আপডেট হতো না)।
    """
    if not stat:
        return False
    _maybe_daily_stat_reset(stat)  # calendar-date based reset (Google-এর real RPD window অনুযায়ী)
    if not stat.get("limited"):
        return False
    reset_ts = _gemini_daily_reset_ts(stat.get("limited_at", 0))
    if time.time() >= reset_ts:
        stat["limited"] = False
        return False
    return True


async def _get_active_provider():
    """
    এই মুহূর্তে যেই key "active" (ব্যবহারের জন্য নির্বাচিত) সেটা return করে।
    সব concurrent request একই provider পাবে (round-robin না)।
    """
    global _active_idx
    if not API_POOL:
        return None, -1
    async with _active_lock:
        n = len(API_POOL)
        now = time.time()
        for _ in range(n):
            idx = _active_idx % n
            provider = API_POOL[idx]
            key = provider["key"]
            label = provider["label"]
            stat_id = provider["stat_id"]
            if key in _dead_keys:
                _active_idx += 1
                continue
            if _key_cooldowns.get(key, 0) > now:
                # RPM burst cooldown চলছে — এখনো সারাদিনের জন্য dead না, তাই
                # _active_idx আগায় কিন্তু key pool-এ থেকেই যায়, cooldown শেষ হলে
                # আবার স্বাভাবিকভাবেই ব্যবহার হবে।
                _active_idx += 1
                continue
            stat = provider_stats.get(stat_id)
            if _is_still_limited(stat):
                # আসল দৈনিক (RPD) quota শেষ হয়েছিল — সেই key-র নিজের limited_at থেকে
                # হিসেব করা নিজস্ব reset time না আসা পর্যন্ত skip করো। প্রতিটা key
                # আলাদা সময়ে limited হয়েছে বলে প্রতিটার reset-ও আলাদা সময়ে হবে —
                # কোনো wasted API call ছাড়াই।
                _active_idx += 1
                continue
            # stat['limited'] false-ই ছিল, অথবা _is_still_limited এইমাত্র reset করে দিলো —
            # দুই ক্ষেত্রেই এখন ব্যবহারযোগ্য।
            return provider, idx
        return None, -1




async def _advance_active_provider(from_idx: int, label: str):
    """
    Current active key limit শেষ (429) বা invalid হওয়ায় পরের key-তে shift করে।
    from_idx মিলিয়ে check করা হয় যাতে একই সময়ে একাধিক request 429 পেলে
    একের পর এক অনেকবার আগায় না গিয়ে একবারই next key-তে move হয়।
    """
    global _active_idx
    async with _active_lock:
        if API_POOL and _active_idx % len(API_POOL) == from_idx:
            _active_idx += 1
            nxt = API_POOL[_active_idx % len(API_POOL)]["label"]
            logger.warning(f"🔀 {label} শেষ — এখন থেকে সব request {nxt}-এ যাবে")


# ══════════════════════════════════════════════════════════════════
#  PROVIDER-SPECIFIC API CALLERS
# ══════════════════════════════════════════════════════════════════

def _http_error_detail(e: _url_err.HTTPError) -> str:
    try:
        raw = e.read().decode("utf-8", "replace")
    except Exception:
        raw = ""
    raw = re.sub(r"AIza[0-9A-Za-z_\-]{20,}", "[REDACTED_GEMINI_KEY]", raw)
    raw = raw.strip().replace("\n", " ")
    return raw[:700] if raw else getattr(e, "reason", "")


def _post_json(url: str, body: dict, headers: dict, timeout: int = 60) -> dict:
    req = _req.Request(
        url,
        data=json.dumps(body).encode("utf-8"),
        headers={"Content-Type": "application/json", **headers},
        method="POST",
    )
    with _req.urlopen(req, timeout=timeout) as resp:
        raw = resp.read().decode("utf-8", "replace")
    if not raw.strip():
        raise ProviderError("Empty API response")
    return json.loads(raw)


def _extract_chat_text(data: dict, provider_name: str) -> str:
    if isinstance(data, dict) and data.get("error"):
        err = data.get("error") or {}
        if isinstance(err, dict):
            raise ProviderError(f"{provider_name} API error: {err.get('message') or err}", err.get("code"))
        raise ProviderError(f"{provider_name} API error: {err}")

    choices = data.get("choices") or []
    if not choices:
        raise ProviderError(f"{provider_name} returned no choices")

    message = choices[0].get("message") or {}
    content = message.get("content", "")
    if isinstance(content, list):
        content = "\n".join(
            str(part.get("text") or part.get("content") or "")
            for part in content
            if isinstance(part, dict)
        )
    text = str(content).strip()
    if not text:
        raise ProviderError(f"{provider_name} returned blank answer")
    return text


def _call_gemini_sync(key: str, prompt: str, image_b64: str = None,
                      image_mime: str = "image/jpeg") -> str:
    """Gemini call. image_b64 দিলে (raw base64, data: prefix ছাড়া) সেটি
    inlineData হিসেবে পাঠানো হয় — অর্থাৎ ছবি পড়ে (OCR/vision) উত্তর দেয়।"""
    errors = []
    for model in GEMINI_MODELS:
        model_name = model.replace("models/", "").strip()
        url = (
            "https://generativelanguage.googleapis.com/v1beta/"
            f"models/{model_name}:generateContent?key={key}"
        )
        parts = [{"text": prompt}]
        if image_b64:
            parts.append({"inlineData": {"mimeType": image_mime or "image/jpeg",
                                         "data": image_b64}})
        body = {
            "contents": [{"role": "user", "parts": parts}],
            "generationConfig": {
                "temperature": 0.2,
                "maxOutputTokens": 8192,
                # gemini-2.5-flash ডিফল্টে internal "thinking" tokens ব্যবহার করে, যা
                # maxOutputTokens বাজেট থেকেই কাটা হয়। math-heavy (LaTeX সহ) উত্তরে
                # thinking tokens-ই বেশিরভাগ বাজেট খেয়ে ফেলছিল, ফলে আসল উত্তর মাঝপথে
                # কেটে যাচ্ছিল (যেমন screenshot-এ দেখা গেছে)। thinkingBudget: 0 দিয়ে
                # সেটা বন্ধ করে পুরো বাজেট answer text-এর জন্য রাখা হচ্ছে। যে model
                # এই field সাপোর্ট করে না, সেখানে Google API এটা নিরাপদে ignore করে।
                "thinkingConfig": {"thinkingBudget": 0},
            },
        }
        try:
            data = _post_json(url, body, {})
            candidates = data.get("candidates") or []
            texts = []
            for cand in candidates:
                content = cand.get("content") or {}
                for part in content.get("parts") or []:
                    if isinstance(part, dict) and part.get("text"):
                        texts.append(str(part["text"]))
            text = "\n".join(texts).strip()
            finish = candidates[0].get("finishReason") if candidates else "NO_CANDIDATES"
            if text:
                if finish == "MAX_TOKENS":
                    # উত্তর মাঝপথে কেটে গেছে (token budget শেষ) — অসম্পূর্ণ উত্তর
                    # silently পাঠানোর বদলে এটাকে fail হিসেবে ধরে পরের key/round-এ
                    # আবার (thinking বন্ধ থাকা অবস্থায়) সম্পূর্ণ উত্তর আনার চেষ্টা করা হয়।
                    raise ProviderError(
                        f"Gemini {model_name} truncated (MAX_TOKENS) at {len(text)} chars"
                    )
                return text
            feedback = data.get("promptFeedback") or {}
            raise ProviderError(f"Gemini {model_name} returned no text; finish={finish}; feedback={feedback}")
        except _url_err.HTTPError as e:
            detail = _http_error_detail(e)
            err = f"Gemini {model_name} HTTP {e.code}: {detail}"
            errors.append(err)
            # Auth/quota/rate-limit একই key-এর সব model-এ fail করবে—সরাসরি next key/provider.
            if e.code == 429:
                raise ProviderError(err, e.code, daily_quota=_classify_429(detail))
            if e.code in (401, 403):
                raise ProviderError(err, e.code)
            # Server error হলে provider বদলানো ভালো; unsupported model হলে next model try হবে.
            if e.code in (500, 502, 503, 504):
                raise ProviderError(err, e.code)
        except ProviderError as e:
            errors.append(str(e))
        except Exception as e:
            errors.append(f"Gemini {model_name}: {e}")
    raise ProviderError("Gemini all models failed: " + " | ".join(errors[-4:]))


# ══════════════════════════════════════════════════════════════════
#  UNIFIED AI CALLER — Round-robin + Smart Fallback
# ══════════════════════════════════════════════════════════════════

# ══════════════════════════════════════════════════════════════════
#  PROVIDER STATS — কোন provider কতবার fail/success হচ্ছে track করে,
#  যাতে dead/unreliable API key বা provider সহজে চিহ্নিত করা যায়।
# ══════════════════════════════════════════════════════════════════
# { label: {"success": int, "fail": int, "last_error": str, "last_used": float,
#           "limited": bool, "limited_at": float,
#           "solved_since_reset": int,  # এই key যতগুলো poll solve করেছে current cycle-এ
#                                        # (limited হলেই ০-তে reset হয়, reset হওয়ার পর
#                                        # আবার নতুন করে ০ থেকে গোনা শুরু হয়)
#           "last_cycle_solved": int}   # আগের cycle-এ limit খাওয়ার আগ পর্যন্ত কতগুলো solve হয়েছিল
provider_stats: dict = {}

# ══════════════════════════════════════════════════════════════════
#  LIFETIME USAGE TOTALS — /apistatus-এর "Total Usage (all keys)" এর জন্য।
#  provider_stats-এর success/fail এখন প্রতিদিন (Pacific date reset অনুযায়ী)
#  reset হয়ে যায় (_maybe_daily_stat_reset), কিন্তু bot চালু হওয়ার পর থেকে মোট
#  কতগুলো poll/text/ocr solve হয়েছে সেই সংখ্যা কখনো reset হওয়া উচিত না — তাই
#  এটা সম্পূর্ণ আলাদা, দৈনিক reset-এর বাইরে থাকা lifetime counter।
# ══════════════════════════════════════════════════════════════════
lifetime_usage_totals: dict = {"poll": 0, "text": 0, "ocr": 0}


# ══════════════════════════════════════════════════════════════════
#  ADMIN NOTIFICATION — কোনো key-র limit শেষ হলে admin-কে জানানোর জন্য
# ══════════════════════════════════════════════════════════════════
_bot_app_ref = None   # main()/_run() থেকে set হয়, যাতে non-handler (background) context
                       # থেকেও (যেমন call_ai) admin-কে সরাসরি message পাঠানো যায়

def _set_bot_app_ref(app):
    global _bot_app_ref
    _bot_app_ref = app


def _notify_admin_bg(text: str, parse_mode: str = "HTML"):
    """Fire-and-forget: admin-কে message পাঠায়। ctx.bot না থাকা context (call_ai-এর
    ভেতর, background task ইত্যাদি) থেকেও কাজ করে কারণ এটা main()-এ set করা global
    bot app reference ব্যবহার করে।"""
    if not _bot_app_ref or not ADMIN_ID:
        return

    async def _send():
        try:
            await _bot_app_ref.bot.send_message(chat_id=ADMIN_ID, text=text, parse_mode=parse_mode)
        except Exception as e:
            logger.error(f"Admin notify failed: {e}")

    try:
        task = asyncio.create_task(_send())
        _turso_bg_tasks.add(task)   # GC থেকে বাঁচানোর জন্য reference রাখা (silent drop এড়াতে)
        task.add_done_callback(_turso_bg_tasks.discard)
    except RuntimeError:
        logger.warning("Admin notify skipped: no running event loop")


def _mark_key_limited(entry: dict, label: str, reason: str = "daily"):
    """
    একটা key দৈনিক (RPD) quota বা soft-cap-এ 'limited' হলে এখানে mark করা হয়:
      1. Admin-কে জানানো হয় — কোন key, এই cycle-এ কতগুলো poll solve করেছে, আর কবে reset হবে।
      2. solved_since_reset কাউন্টার ০-তে reset করা হয়, যাতে key আবার active হওয়ার
         (reset হওয়ার) পর থেকে সেই key-র solve count নতুন করে ০ থেকে শুরু হয়।
    একই limited period-এ বারবার duplicate notification পাঠানো হয় না — শুধু
    active → limited transition-এই একবার পাঠানো হয়।
    """
    already_limited = entry.get("limited", False)
    entry["limited"]    = True
    entry["limited_at"] = time.time()
    if already_limited:
        return  # আগে থেকেই limited ছিল — নতুন করে notify/reset দরকার নেই

    solved = entry.get("solved_since_reset", 0)
    entry["last_cycle_solved"]  = solved
    entry["solved_since_reset"] = 0  # ← পরের cycle এখান থেকেই (০) শুরু হবে

    reset_ts  = _gemini_daily_reset_ts(entry["limited_at"])
    dhaka_tz  = pytz.timezone("Asia/Dhaka")
    reset_str = (
        datetime.fromtimestamp(reset_ts, dhaka_tz).strftime("%I:%M %p, %d %b")
        if reset_ts else "অজানা"
    )
    reason_bn = "সাময়িক (Soft Cap)" if reason == "soft cap" else "দৈনিক (RPD Quota)"

    text = (
        f"🚫 <b>API Key Limit শেষ!</b>\n"
        f"▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬\n"
        f"🔑 <b>Key:</b> {label}\n"
        f"🗂 <b>ধরন:</b> {reason_bn} limit\n"
        f"📊 <b>এই cycle-এ Solve করেছে:</b> {solved} টি poll\n"
        f"🕐 <b>Reset হবে:</b> {reset_str} (Dhaka time)\n"
        f"▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬\n"
        f"🔀 পরের key-তে shift করা হয়েছে।\n"
        f"♻️ Reset হওয়ার পর এই key-র solve count আবার <b>০ থেকে</b> শুরু হবে।"
    )
    logger.warning(f"🚫 {label} limited ({reason}) — solved {solved} this cycle, resets ~{reset_str}")
    _notify_admin_bg(text)


def _record_provider_result(stat_id: str, label: str, success: bool, error: str = "", limited: bool = False,
                            cooldown: bool = False, task: str = "poll"):
    entry = provider_stats.get(stat_id)
    if entry is None:
        entry = {"success": 0, "fail": 0, "last_error": "", "last_used": 0.0,
                  "limited": False, "limited_at": 0.0, "cooldowns": 0,
                  "solved_since_reset": 0, "last_cycle_solved": 0,
                  "poll_success": 0, "text_success": 0, "ocr_success": 0,
                  "poll_fail": 0, "text_fail": 0, "ocr_fail": 0,
                  "reset_date": _pacific_date_str()}
        provider_stats[stat_id] = entry
    else:
        _maybe_daily_stat_reset(entry)  # calendar-date based reset (Google-এর real RPD window অনুযায়ী)
    # poll / text / ocr (image vision) — তিনটা আলাদা করে গোনা হয়, যাতে /apistatus-এ
    # কোন key দিয়ে কয়টা poll, কয়টা text Q&A আর কয়টা OCR solve হলো আলাদা দেখা যায়।
    if task == "text":
        task_key = "text"
    elif task in ("image", "ocr", "vision"):
        task_key = "ocr"
    else:
        task_key = "poll"
    if success:
        entry["success"] += 1
        entry[f"{task_key}_success"] = entry.get(f"{task_key}_success", 0) + 1
        entry["solved_since_reset"] = entry.get("solved_since_reset", 0) + 1
        lifetime_usage_totals[task_key] = lifetime_usage_totals.get(task_key, 0) + 1
        _turso_bg(lambda: _save_lifetime_usage_totals(), "save_lifetime_usage_totals")

        entry["limited"] = False
        _key_429_streak[stat_id] = 0
        if GEMINI_DAILY_SOFT_CAP_PER_KEY > 0:
            count = _bump_soft_daily_count(stat_id)
            if count >= GEMINI_DAILY_SOFT_CAP_PER_KEY:
                # আজকের জন্য নির্ধারিত soft budget শেষ — real 429 আসার আগেই এই key-কে
                # rest-এ পাঠানো হচ্ছে, যাতে busy সময়েও সব key একসাথে ফুরিয়ে না যায়।
                _mark_key_limited(entry, label, reason="soft cap")
    else:
        entry["fail"] += 1
        entry[f"{task_key}_fail"] = entry.get(f"{task_key}_fail", 0) + 1
        entry["last_error"] = error[:200]

        if limited:
            _mark_key_limited(entry, label, reason="daily")
        elif cooldown:
            # সাময়িক RPM burst — সারাদিনের জন্য "limited" মার্ক করা হচ্ছে না,
            # শুধু গণনার জন্য রাখা হলো যাতে /apistatus এ পার্থক্য বোঝা যায়।
            entry["cooldowns"] = entry.get("cooldowns", 0) + 1
    entry["last_used"] = time.time()
    # Turso-তে save করো, যাতে bot restart/redeploy হলেও call count হারিয়ে না যায়
    _turso_bg(lambda: _save_provider_stats(), "save_provider_stats")


async def _save_provider_stats():
    """provider_stats dict টা bot_settings table-এ JSON আকারে save করো (bot_settings
    generic key-value store পুনরায় ব্যবহার করা হচ্ছে — নতুন table লাগেনি)।"""
    await turso_exec(
        "INSERT OR REPLACE INTO bot_settings (setting_key, value) VALUES (?, ?)",
        ("provider_stats", json.dumps(provider_stats))
    )


async def load_provider_stats_from_turso():
    """Bot start হলে Turso থেকে আগের provider_stats (Gemini call count ইত্যাদি) ফিরিয়ে আনো।"""
    global provider_stats
    client = await _get_turso()
    if client is None:
        return
    try:
        rs = await client.execute("SELECT value FROM bot_settings WHERE setting_key = ?", ["provider_stats"])
        if rs.rows:
            loaded = json.loads(rs.rows[0][0])
            if isinstance(loaded, dict):
                provider_stats.update(loaded)
                logger.info(f"✅ Loaded provider_stats for {len(provider_stats)} provider(s) from Turso")
    except Exception as e:
        logger.error(f"Turso load provider_stats error: {e}")

async def _save_lifetime_usage_totals():
    """lifetime_usage_totals (কখনো reset না হওয়া poll/text/ocr মোট সংখ্যা) Turso-তে save করো।"""
    await turso_exec(
        "INSERT OR REPLACE INTO bot_settings (setting_key, value) VALUES (?, ?)",
        ("lifetime_usage_totals", json.dumps(lifetime_usage_totals))
    )


async def load_lifetime_usage_totals_from_turso():
    """Bot start হলে Turso থেকে আগের lifetime_usage_totals ফিরিয়ে আনো।"""
    global lifetime_usage_totals
    client = await _get_turso()
    if client is None:
        return
    try:
        rs = await client.execute("SELECT value FROM bot_settings WHERE setting_key = ?", ["lifetime_usage_totals"])
        if rs.rows:
            loaded = json.loads(rs.rows[0][0])
            if isinstance(loaded, dict):
                lifetime_usage_totals.update(loaded)
                logger.info(f"✅ Loaded lifetime_usage_totals from Turso: {lifetime_usage_totals}")
    except Exception as e:
        logger.error(f"Turso load lifetime_usage_totals error: {e}")


def get_provider_stats_text() -> str:
    """Admin /stats এর মতো command থেকে call করার জন্য — readable summary।
    provider_stats-এর key এখন stable stat_id (label না), তাই display-এর জন্য
    বর্তমান API_POOL থেকে stat_id → label ম্যাপ করে নেওয়া হচ্ছে।"""
    if not provider_stats:
        return "এখনও কোনো provider call হয়নি।"
    id_to_label = {p["stat_id"]: p["label"] for p in API_POOL}
    lines = []
    for stat_id, s in sorted(provider_stats.items(), key=lambda kv: -kv[1]["fail"]):
        _maybe_daily_stat_reset(s)  # calendar-date based reset (Google-এর real RPD window অনুযায়ী)
        label = id_to_label.get(stat_id, f"(removed key {stat_id})")
        total = s["success"] + s["fail"]
        rate = (s["success"] / total * 100) if total else 0
        if s.get("limited"):
            flag = "🚫"
        elif s["fail"] > 0 and s["success"] == 0:
            flag = "💀"
        elif rate < 50:
            flag = "⚠️"
        else:
            flag = "✅"
        lines.append(
            f"{flag} {label}: {s['success']}✅ / {s['fail']}❌ ({rate:.0f}% success)"
        )
    return "\n".join(lines)



async def call_ai(prompt: str, task: str = "poll",
                  image_b64: str = None, image_mime: str = "image/jpeg") -> str:
    """
    সব user-এর সব request একটাই "active" key ব্যবহার করে (parallel/round-robin না)।
    Gemini#1 যতক্ষণ কাজ করে, সবাই #1 ব্যবহার করবে। #1 rate-limited (429) হলেই
    bot shift করে Gemini#2-তে যাবে, তারপর #3 — এভাবে ক্রমান্বয়ে।
    401/403 (invalid key) পেলে সেই key permanently skip হয়।
    Round 1: active key দিয়ে (no wait); Round 2: 15s wait; Round 3: 30s wait —
    সবগুলো round-ই একই shared active-key state ব্যবহার করে।
    """
    if not API_POOL:
        logger.error(
            "❌ No API keys found. Set GEMINI_API_KEY "
            "or *_API_KEY_1..15 or *_API_KEYS comma-list env variables."
        )
        return "AI_FAILED"

    loop = asyncio.get_event_loop()
    max_rounds = int(os.environ.get("AI_MAX_ROUNDS", "3"))
    # Round 1: no wait, Round 2: 15s, Round 3: 30s
    round_waits = [0, 15, 30]
    last_errors = []

    for round_num in range(max_rounds):
        if round_num > 0:
            wait = round_waits[min(round_num, len(round_waits) - 1)]
            logger.warning(f"🔄 Round {round_num+1}/{max_rounds}: সব provider fail, {wait}s পর retry...")
            await asyncio.sleep(wait)

        any_tried = False
        attempts = 0
        # একটা round-এ সর্বোচ্চ len(API_POOL) বার key shift করে try করা হয়
        # (429 পেলেই পরের key, তাই এক round-এ পুরো pool একবার ঘুরে আসতে পারে)
        while attempts < len(API_POOL):
            provider, idx = await _get_active_provider()
            if not provider:
                break  # সব key dead (401/403)

            ptype = provider["type"]
            key   = provider["key"]
            label = provider["label"]
            stat_id = provider["stat_id"]

            any_tried = True
            attempts += 1
            try:
                logger.info(f"[Round {round_num+1}] Trying: {label} ({_mask_secret(key)})")
                if ptype == "gemini":
                    # Semaphore দিয়ে একই key-তে concurrent call সীমিত রাখা হচ্ছে, যাতে
                    # sudden burst-এ এক ঝটকায় RPM limit শেষ হয়ে cascade না হয়।
                    async with _key_semaphore:
                        result = await loop.run_in_executor(
                            None, _call_gemini_sync, key, prompt, image_b64, image_mime
                        )
                else:
                    continue

                result = (result or "").strip()
                if not result:
                    raise ProviderError(f"{label} returned empty text")

                logger.info(f"✅ Success via {label} (Round {round_num+1})")
                _record_provider_result(stat_id, label, success=True, task=task)
                return result

            except ProviderError as e:
                code = e.status_code
                msg = str(e)
                last_errors.append(f"{label}: {msg}")
                if code == 429:
                    is_daily = _classify_daily_with_fallback(stat_id, e.daily_quota)
                    if is_daily:
                        # আসল দৈনিক (RPD) quota শেষ — এই key সারাদিনের জন্যই বন্ধ থাকুক।
                        _record_provider_result(stat_id, label, success=False, error=msg, limited=True, task=task)
                        logger.warning(f"⚠️ {label} DAILY quota exhausted — পরের key-তে shift করা হচ্ছে")
                    else:
                        # সাময়িক RPM/burst limit — অল্প সময়ের জন্য cooldown, সারাদিনের জন্য না।
                        _key_cooldowns[key] = time.time() + RPM_COOLDOWN_SECS
                        _record_provider_result(stat_id, label, success=False, error=msg, cooldown=True, task=task)
                        logger.warning(f"⚠️ {label} temporary RPM burst limit — {RPM_COOLDOWN_SECS}s cooldown, তারপর আবার ব্যবহার হবে")
                    await _advance_active_provider(idx, label)
                    await asyncio.sleep(1)
                    continue  # নতুন active key দিয়ে সাথে সাথে আবার try
                elif code in (401, 403):
                    logger.error(f"🔑 {label} invalid/revoked key ({code}) — permanently skipping this key")
                    _dead_keys.add(key)
                    await _advance_active_provider(idx, label)
                    continue
                elif code in (500, 502, 503, 504):
                    logger.warning(f"⚠️ {label} server error ({code}) — সাথে সাথে পরের key-তে shift করা হচ্ছে")
                    await _advance_active_provider(idx, label)
                    continue  # পুরো round wait না করে সাথে সাথে পরের key try করো (10 key pool থাকায় সাধারণত কোনো না কোনোটা কাজ করবে)
                else:
                    logger.error(f"❌ {label} failed: {msg} — পরের key-তে shift করা হচ্ছে")
                    await _advance_active_provider(idx, label)
                    continue
            except _url_err.HTTPError as e:
                detail = _http_error_detail(e)
                msg = f"HTTP {e.code}: {detail}"
                last_errors.append(f"{label}: {msg}")
                logger.error(f"❌ {label} {msg}")
                if e.code == 429:
                    # BUGFIX: আগে এখানে যেকোনো 429-কেই blindly limited=True (সারাদিনের জন্য বন্ধ)
                    # মার্ক করা হতো — ProviderError branch-এর মতো RPM-burst বনাম real daily (RPD)
                    # classify করা হতো না। ফলে সাময়িক per-minute burst-এও key ভুলভাবে পুরো দিনের
                    # জন্য হারিয়ে যেত। এখন একই _classify_daily_with_fallback() logic ব্যবহার করা
                    # হচ্ছে, যাতে দুই error path (ProviderError vs raw HTTPError) সবসময় একই রকম
                    # আচরণ করে।
                    is_daily = _classify_daily_with_fallback(stat_id, _classify_429(detail))
                    if is_daily:
                        _record_provider_result(stat_id, label, success=False, error=msg, limited=True, task=task)
                        logger.warning(f"⚠️ {label} DAILY quota exhausted — পরের key-তে shift করা হচ্ছে")
                    else:
                        _key_cooldowns[key] = time.time() + RPM_COOLDOWN_SECS
                        _record_provider_result(stat_id, label, success=False, error=msg, cooldown=True, task=task)
                        logger.warning(f"⚠️ {label} temporary RPM burst limit — {RPM_COOLDOWN_SECS}s cooldown")
                    await _advance_active_provider(idx, label)
                    await asyncio.sleep(1)
                    continue
                if e.code in (401, 403):
                    _dead_keys.add(key)
                _record_provider_result(stat_id, label, success=False, error=msg, task=task)
                await _advance_active_provider(idx, label)
                continue  # যেকোনো HTTP error-এই সাথে সাথে পরের key try করো, পুরো round wait না করে
            except Exception as e:
                last_errors.append(f"{label}: {e}")
                logger.error(f"❌ {label} error: {e} — পরের key-তে shift করা হচ্ছে")
                _record_provider_result(stat_id, label, success=False, error=str(e), task=task)
                await _advance_active_provider(idx, label)
                continue  # network glitch বা transient error-এও অন্য key দিয়ে সাথে সাথে আবার try

        # সব key-ই dead হয়ে গেলে আর retry করে লাভ নেই
        if not any_tried or len(_dead_keys) >= len(API_POOL):
            logger.error("💀 All keys are dead (401/403) — stopping early")
            break

    logger.error("💀 All providers failed after all rounds — returning AI_FAILED")
    if last_errors:
        logger.error("Last errors:\n" + "\n".join(last_errors[-12:]))
    return "AI_FAILED"


# ══════════════════════════════════════════════════════════════════
#  RICH MESSAGE HELPER  (Telegram Bot API 10.1 — June 11, 2026)
#  sendRichMessage headings, bold, list, table ইত্যাদি সহ raw Markdown
#  render করে দেয়। python-telegram-bot library-এ এখনো native wrapper
#  নেই, তাই সরাসরি HTTPS POST দিয়ে call করা হচ্ছে (Gemini call-এর মতোই
#  urllib ব্যবহার করে, এই file-এ যেভাবে অন্য সব raw HTTP call হয়)।
# ══════════════════════════════════════════════════════════════════
async def send_rich_message(chat_id, markdown_text: str, reply_markup=None, reply_to_message_id: int = None):
    """
    rich_message.markdown হিসেবে raw Markdown পাঠায় — Telegram নিজেই
    heading (#), bold (**), table (| | |), list ইত্যাদি parse করে সুন্দর
    ফরম্যাটে render করে। ব্যর্থ হলে (পুরনো client / API সমস্যা) None রিটার্ন
    করে, caller তখন plain sendMessage-এ fallback করতে পারবে।
    """
    if not BOT_TOKEN:
        return None

    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendRichMessage"
    payload = {
        "chat_id": chat_id,
        "rich_message": {"markdown": markdown_text},
    }
    if reply_markup is not None:
        payload["reply_markup"] = (
            reply_markup.to_dict() if hasattr(reply_markup, "to_dict") else reply_markup
        )
    if reply_to_message_id:
        payload["reply_parameters"] = {"message_id": reply_to_message_id}

    data = json.dumps(payload).encode("utf-8")

    def _do_request():
        req = _req.Request(url, data=data, headers={"Content-Type": "application/json"}, method="POST")
        with _req.urlopen(req, timeout=25) as resp:
            return json.loads(resp.read().decode("utf-8"))

    loop = asyncio.get_event_loop()
    try:
        result = await loop.run_in_executor(None, _do_request)
        if result.get("ok"):
            return result.get("result")
        logger.warning(f"sendRichMessage failed: {result}")
        return None
    except _url_err.HTTPError as e:
        detail = _http_error_detail(e)
        logger.warning(f"sendRichMessage HTTP {e.code}: {detail}")
        return None
    except Exception as e:
        logger.warning(f"sendRichMessage error: {e}")
        return None


# ══════════════════════════════════════════════════════════════════
#  REPORT GROUP HELPER
# ══════════════════════════════════════════════════════════════════
async def send_poll_report(ctx, user, poll_msg, source_link: str, source_name: str):
    if not REPORT_GROUP_ID:
        return

    uname = f"@{user.username}" if user.username else "username নেই"
    name  = user.full_name or "Unknown"

    import html as _html
    safe_name     = _html.escape(name)
    safe_uname    = _html.escape(uname)
    safe_src_name = _html.escape(source_name)
    if source_link.startswith("http"):
        safe_src_link = f'<a href="{source_link}">{source_link}</a>'
    else:
        safe_src_link = _html.escape(source_link)

    info_text = (
        f"📨 <b>নতুন Poll Solve হয়েছে!</b>\n"
        f"▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬\n\n"
        f"👤 <b>Name:</b> {safe_name}\n"
        f"🔖 <b>Username:</b> {safe_uname}\n"
        f"🆔 <b>User ID:</b> <code>{user.id}</code>\n\n"
        f"📌 <b>Poll Source:</b>\n"
        f"{safe_src_name}\n"
        f"{safe_src_link}\n\n"
        f"▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬\n"
        f"🕐 {get_dhaka_time()}"
    )

    try:
        await ctx.bot.send_message(
            chat_id=REPORT_GROUP_ID,
            text=info_text,
            parse_mode="HTML",
            disable_web_page_preview=True
        )
    except Exception as e:
        logger.error(f"Report info send error: {e}")

    try:
        await ctx.bot.forward_message(
            chat_id=REPORT_GROUP_ID,
            from_chat_id=poll_msg.chat_id,
            message_id=poll_msg.message_id
        )
    except Exception as e:
        logger.error(f"Report poll forward error: {e}")


async def send_poll_fail_report(ctx, user, poll_msg, question: str = ""):
    """
    AI সব provider দিয়ে চেষ্টা করার পরও poll solve করতে না পারলে (AI_FAILED),
    সেই attempt-এর details (কার poll, কখন) report group-এ পাঠায়, যাতে admin
    বুঝতে পারে কারা fail experience করছে।
    """
    if not REPORT_GROUP_ID or not user:
        return

    import html as _html
    uname = f"@{user.username}" if getattr(user, "username", None) else "username নেই"
    name  = getattr(user, "full_name", None) or "Unknown"
    safe_name  = _html.escape(str(name))
    safe_uname = _html.escape(str(uname))
    safe_q     = _html.escape((question or "")[:250])

    fail_text = (
        f"\u274c <b>Poll Solve FAILED!</b>\n"
        f"\u25ac\u25ac\u25ac\u25ac\u25ac\u25ac\u25ac\u25ac\u25ac\u25ac\u25ac\u25ac\u25ac\u25ac\u25ac\u25ac\u25ac\u25ac\u25ac\u25ac\u25ac\n\n"
        f"\U0001F464 <b>Name:</b> {safe_name}\n"
        f"\U0001F516 <b>Username:</b> {safe_uname}\n"
        f"\U0001F194 <b>User ID:</b> <code>{user.id}</code>\n\n"
        + (f"\u2753 <b>Question:</b> {safe_q}\n\n" if safe_q else "")
        + f"\u26A0\uFE0F AI সব provider দিয়ে চেষ্টা করেও উত্তর দিতে ব্যর্থ হয়েছে।\n"
        f"\u25ac\u25ac\u25ac\u25ac\u25ac\u25ac\u25ac\u25ac\u25ac\u25ac\u25ac\u25ac\u25ac\u25ac\u25ac\u25ac\u25ac\u25ac\u25ac\u25ac\u25ac\n"
        f"\U0001F550 {get_dhaka_time()}"
    )

    try:
        await ctx.bot.send_message(
            chat_id=REPORT_GROUP_ID,
            text=fail_text,
            parse_mode="HTML",
            disable_web_page_preview=True
        )
    except Exception as e:
        logger.error(f"Fail report send error: {e}")

    if poll_msg is not None and getattr(poll_msg, "chat_id", None) and getattr(poll_msg, "message_id", None):
        try:
            await ctx.bot.forward_message(
                chat_id=REPORT_GROUP_ID,
                from_chat_id=poll_msg.chat_id,
                message_id=poll_msg.message_id
            )
        except Exception as e:
            logger.error(f"Fail report poll forward error: {e}")


async def send_text_qa_report(ctx, user, question: str):
    """
    Student text দিয়ে সরাসরি প্রশ্ন করলে তার name, username, user id,
    কী প্রশ্ন করেছে এবং কখন করেছে — এই full detail REPORT_GROUP_ID-তে পাঠায়।
    """
    if not REPORT_GROUP_ID or not user:
        return

    import html as _html
    uname = f"@{user.username}" if getattr(user, "username", None) else "username নেই"
    name  = getattr(user, "full_name", None) or "Unknown"
    safe_name  = _html.escape(str(name))
    safe_uname = _html.escape(str(uname))
    safe_q     = _html.escape((question or "").strip()[:1000])

    info_text = (
        f"❓ <b>নতুন Text Question এসেছে!</b>\n"
        f"▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬\n\n"
        f"👤 <b>Name:</b> {safe_name}\n"
        f"🔖 <b>Username:</b> {safe_uname}\n"
        f"🆔 <b>User ID:</b> <code>{user.id}</code>\n\n"
        f"📝 <b>Question:</b>\n{safe_q}\n\n"
        f"▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬\n"
        f"🕐 {get_dhaka_time()}"
    )

    try:
        await ctx.bot.send_message(
            chat_id=REPORT_GROUP_ID,
            text=info_text,
            parse_mode="HTML",
            disable_web_page_preview=True
        )
    except Exception as e:
        logger.error(f"Text QA report send error: {e}")


# ══════════════════════════════════════════════════════════════════
#  CHANNEL LEAVE DETECTOR
# ══════════════════════════════════════════════════════════════════
async def channel_member_update(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    result = update.chat_member
    if not result:
        return

    chat = result.chat
    # Check if this event is from our target group/channel
    username_match = (chat.username and
                      chat.username.lower() == GROUP_CHAT_ID.lstrip("@").lower())
    id_match = (isinstance(GROUP_CHAT_ID, int) and chat.id == GROUP_CHAT_ID)
    if not username_match and not id_match:
        return

    old_status = result.old_chat_member.status
    new_status = result.new_chat_member.status
    left_user  = result.new_chat_member.user

    if old_status in ("member", "administrator", "creator") and new_status in ("left", "kicked", "restricted"):
        if not REPORT_GROUP_ID:
            return

        import html as _html
        name  = _html.escape(left_user.full_name or "Unknown")
        uname = f"@{_html.escape(left_user.username)}" if left_user.username else "username নেই"
        uid   = left_user.id
        action = "🚪 Leave নিয়েছে" if new_status == "left" else "🚫 Kicked হয়েছে"

        alert_text = (
            f"⚠️ <b>Channel Member Update!</b>\n"
            f"▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬\n\n"
            f"{action}\n\n"
            f"👤 <b>Name:</b> {name}\n"
            f"🔖 <b>Username:</b> {uname}\n"
            f"🆔 <b>User ID:</b> <code>{uid}</code>\n\n"
            f"🔒 Bot access <b>বন্ধ</b> হয়ে গেছে।\n"
            f"▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬\n"
            f"🕐 {get_dhaka_time()}"
        )

        try:
            await ctx.bot.send_message(
                chat_id=REPORT_GROUP_ID,
                text=alert_text,
                parse_mode="HTML"
            )
        except Exception as e:
            logger.error(f"Leave alert error: {e}")

        verified_users.discard(uid)
        if uid in registered_users:
            registered_users[uid]["verified"] = False
            _turso_bg(lambda: _save_user(uid), "save_user")

        try:
            keyboard = [
                [InlineKeyboardButton("📢 Live Exam TPC", url=GROUP_LINK)],
                [InlineKeyboardButton("✅ Joined — Verify করো", callback_data="verify_check")],
            ]
            await ctx.bot.send_message(
                chat_id=uid,
                text=(
                    "⛔ *Bot Access বন্ধ হয়ে গেছে!*\n\n"
                    "তুমি *Live Exam TPC* channel ছেড়ে দিয়েছো,\n"
                    "তাই bot আর ব্যবহার করতে পারবে না।\n\n"
                    "আবার ব্যবহার করতে হলে:\n"
                    "1️⃣ channel-এ আবার join করো\n"
                    "2️⃣ তারপর *Verify* চাপো"
                ),
                parse_mode=ParseMode.MARKDOWN,
                reply_markup=InlineKeyboardMarkup(keyboard)
            )
        except Exception as e:
            logger.error(f"Leave DM error: {e}")


# ══════════════════════════════════════════════════════════════════
#  /setreportgroup COMMAND (Admin only)
# ══════════════════════════════════════════════════════════════════
async def setreportgroup_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    global REPORT_GROUP_ID
    user = update.effective_user

    if user.id != ADMIN_ID:
        return

    msg  = update.message
    args = ctx.args

    if args:
        target = args[0].strip()
        if target.lstrip("-").isdigit():
            chat_identifier = int(target)
        else:
            chat_identifier = target if target.startswith("@") else f"@{target}"

        try:
            chat = await ctx.bot.get_chat(chat_identifier)
        except Exception as e:
            await msg.reply_text(
                f"❌ *Group খুঁজে পাওয়া যায়নি!*\n\nকারণ: `{e}`",
                parse_mode=ParseMode.MARKDOWN
            )
            return

        if chat.type not in ("group", "supergroup"):
            await msg.reply_text("❌ এটা group/supergroup না।")
            return

        REPORT_GROUP_ID = chat.id
        group_name = chat.title or str(chat.id)
        _turso_bg(lambda: _save_setting("REPORT_GROUP_ID", REPORT_GROUP_ID), "save_setting")

        await msg.reply_text(
            f"✅ *Report Group Set হয়েছে!*\n\n"
            f"🏠 *Group:* {group_name}\n"
            f"🆔 *ID:* `{REPORT_GROUP_ID}`",
            parse_mode=ParseMode.MARKDOWN
        )
        return

    if msg.chat.type not in ("group", "supergroup"):
        await msg.reply_text(
            "ℹ️ *ব্যবহার:*\n\n"
            "1️⃣ `/setreportgroup @groupusername`\n"
            "2️⃣ `/setreportgroup -1001234567890`\n"
            "3️⃣ Group-এ গিয়ে `/setreportgroup`",
            parse_mode=ParseMode.MARKDOWN
        )
        return

    REPORT_GROUP_ID = msg.chat_id
    group_name = msg.chat.title or "Unknown Group"
    _turso_bg(lambda: _save_setting("REPORT_GROUP_ID", REPORT_GROUP_ID), "save_setting")

    await msg.reply_text(
        f"✅ *Report Group Set হয়েছে!*\n\n"
        f"🏠 *Group:* {group_name}\n"
        f"🆔 *ID:* `{REPORT_GROUP_ID}`",
        parse_mode=ParseMode.MARKDOWN
    )


# ══════════════════════════════════════════════════════════════════
#  USER STORAGE
# ══════════════════════════════════════════════════════════════════
registered_users: dict = {}

# Retry storage: { "retry_<id>": {"question":..,"options":..,"correct_idx":..,"chat_id":..,"user_id":..,"_created_at":..} }
retry_poll_data: dict = {}
RETRY_DATA_TTL_SECS = 20 * 60  # ২০ মিনিট পরে stale retry entry expire হবে (memory leak fix)


def _upsert_user(user_id: int, data: dict):
    """registered_users update করো + Turso-তে async save করো."""
    registered_users[user_id] = data
    _turso_bg(lambda: _save_user(user_id), "save_user")

# ══════════════════════════════════════════════════════════════════
#  GROUP VERIFICATION CONFIG
# ══════════════════════════════════════════════════════════════════
GROUP_LINK    = "https://t.me/LiveExamTPC"
GROUP_CHAT_ID = "@LiveExamTPC"

verified_users: set = set()

# ══════════════════════════════════════════════════════════════════
#  VERIFICATION GROUP-এ BOT সম্পূর্ণ চুপ থাকবে
#  (bot শুধু membership check-এর জন্য admin হিসেবে থাকবে —
#   এই group-এর কোনো message/command/poll/quiz-এ response দিবে না)
# ══════════════════════════════════════════════════════════════════
async def block_verification_group(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    chat = update.effective_chat
    if chat and chat.username and f"@{chat.username}".lower() == GROUP_CHAT_ID.lower():
        raise ApplicationHandlerStop

async def is_active_member(bot, user_id: int) -> bool:
    try:
        member = await bot.get_chat_member(chat_id=GROUP_CHAT_ID, user_id=user_id)
        result = member.status in ("member", "administrator", "creator")
    except Exception as e:
        logger.error(f"Membership check error: {e}")
        return False

    if not result:
        verified_users.discard(user_id)

    return result

# ══════════════════════════════════════════════════════════════════
#  RATE LIMIT CONFIG
# ══════════════════════════════════════════════════════════════════
DAILY_LIMIT   = 25        # প্রতিদিন সর্বোচ্চ poll solve
COOLDOWN_SECS = 120       # ২ মিনিট = 120 seconds gap

# { user_id: {"date": "YYYY-MM-DD", "count": int, "last_time": float} }
rate_data: dict = {}

# ══════════════════════════════════════════════════════════════════
#  REFERRAL SYSTEM
# ══════════════════════════════════════════════════════════════════
REFERRAL_BONUS_PER_INVITE = 1   # প্রতি successful referral-এ আজকের জন্য +1 poll

# ── Referral link obfuscation ──────────────────────────────────────────
# Raw Telegram user_id সরাসরি referral link-এ (?start=123456789) না দেখিয়ে,
# একটা ছোট, random-দেখতে alphanumeric code দেখানো হয় (?start=xyz123abcd)।
# XOR + base62 reversible encoding — কোনো আলাদা DB/mapping table লাগে না।
_REF_XOR_KEY = 0x9F3C7A15E824
_B62_ALPHABET = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"


def _b62_encode(n: int) -> str:
    if n == 0:
        return _B62_ALPHABET[0]
    digits = []
    while n:
        n, r = divmod(n, 62)
        digits.append(_B62_ALPHABET[r])
    return "".join(reversed(digits))


def _b62_decode(s: str) -> int:
    n = 0
    for ch in s:
        idx = _B62_ALPHABET.find(ch)
        if idx < 0:
            raise ValueError(f"Invalid base62 char: {ch}")
        n = n * 62 + idx
    return n


_REF_CODE_PREFIX = "ref2id"

def make_referral_code(user_id: int) -> str:
    """user_id থেকে obfuscated referral code বানায় (raw ID লুকানোর জন্য)।"""
    return _REF_CODE_PREFIX + _b62_encode(user_id ^ _REF_XOR_KEY)


def decode_referral_code(code: str):
    """Referral code থেকে ফিরে user_id বের করে; invalid হলে None রিটার্ন করে।"""
    try:
        if code.startswith(_REF_CODE_PREFIX):
            code = code[len(_REF_CODE_PREFIX):]
        return _b62_decode(code) ^ _REF_XOR_KEY
    except (ValueError, IndexError):
        return None



# { new_user_id: referrer_user_id }  — /start?start=<referrer_id> দিয়ে আসা pending referral,
# verify হওয়ার আগ পর্যন্ত এখানে থাকবে।
# NOTE: referral link কখনো expire হবে না — কেউ ৫ দিন পরে link-এ click করে join করলেও
# referrer ঠিকই বোনাস পাবে। তাই এখানে কোনো TTL/expiry নেই, ইচ্ছাকৃতভাবে।
pending_referrals: dict = {}

# { referrer_id: {"date": "YYYY-MM-DD", "extra": int, "count_today": int} }
referral_bonus: dict = {}

# { user_id: {"date": "YYYY-MM-DD", "extra": int} }  — admin কর্তৃক manually দেওয়া
# extra daily poll limit। শুধু সেই দিনের জন্যই valid, পরদিন reset হয়ে যায়।
admin_extra_limit: dict = {}

# একই কাঠামো, কিন্তু Text Q&A আর OCR (image) Q&A limit-এর জন্য — poll limit থেকে
# সম্পূর্ণ আলাদা, /addlimit wizard থেকে admin type বেছে সেট করে।
admin_extra_text_limit: dict = {}
admin_extra_ocr_limit: dict = {}

def get_referral_bonus(user_id: int) -> int:
    today = get_dhaka_date()
    entry = referral_bonus.get(user_id)
    if entry is None or entry.get("date") != today:
        return 0
    return entry.get("extra", 0)

def add_referral_bonus(referrer_id: int):
    today = get_dhaka_date()
    entry = referral_bonus.get(referrer_id)
    if entry is None or entry.get("date") != today:
        entry = {"date": today, "extra": 0, "count_today": 0}
        referral_bonus[referrer_id] = entry
    entry["extra"]       += REFERRAL_BONUS_PER_INVITE
    entry["count_today"] += 1
    _turso_bg(lambda: _save_referral(referrer_id), "save_referral")

def get_admin_extra_limit(user_id: int) -> int:
    """আজকের জন্য admin-এর দেওয়া extra limit — অন্য দিনের হলে 0।"""
    today = get_dhaka_date()
    entry = admin_extra_limit.get(user_id)
    if entry is None or entry.get("date") != today:
        return 0
    return entry.get("extra", 0)

def set_admin_extra_limit(user_id: int, extra: int):
    """Admin manually আজকের জন্য এই user-এর extra daily poll limit set করে।
    পরদিন এটা আবার 0 হয়ে যাবে — স্থায়ী নয়।"""
    extra = max(0, extra)
    today = get_dhaka_date()
    if extra == 0:
        admin_extra_limit.pop(user_id, None)
    else:
        admin_extra_limit[user_id] = {"date": today, "extra": extra}
    _turso_bg(lambda: _save_admin_extra_limit(user_id), "save_admin_extra_limit")

def get_effective_daily_limit(user_id: int) -> int:
    return DAILY_LIMIT + get_referral_bonus(user_id) + get_admin_extra_limit(user_id)

def get_admin_extra_text_limit(user_id: int) -> int:
    """আজকের জন্য admin-এর দেওয়া extra TEXT Q&A limit — অন্য দিনের হলে 0।"""
    today = get_dhaka_date()
    entry = admin_extra_text_limit.get(user_id)
    if entry is None or entry.get("date") != today:
        return 0
    return entry.get("extra", 0)

def set_admin_extra_text_limit(user_id: int, extra: int):
    """Admin manually আজকের জন্য এই user-এর extra daily TEXT Q&A limit set করে।
    পরদিন এটা আবার 0 হয়ে যাবে — স্থায়ী নয়।"""
    extra = max(0, extra)
    today = get_dhaka_date()
    if extra == 0:
        admin_extra_text_limit.pop(user_id, None)
    else:
        admin_extra_text_limit[user_id] = {"date": today, "extra": extra}
    _turso_bg(lambda: _save_admin_extra_text_limit(user_id), "save_admin_extra_text_limit")

def get_admin_extra_ocr_limit(user_id: int) -> int:
    """আজকের জন্য admin-এর দেওয়া extra OCR (image) Q&A limit — অন্য দিনের হলে 0।"""
    today = get_dhaka_date()
    entry = admin_extra_ocr_limit.get(user_id)
    if entry is None or entry.get("date") != today:
        return 0
    return entry.get("extra", 0)

def set_admin_extra_ocr_limit(user_id: int, extra: int):
    """Admin manually আজকের জন্য এই user-এর extra daily OCR (image) Q&A limit set করে।
    পরদিন এটা আবার 0 হয়ে যাবে — স্থায়ী নয়।"""
    extra = max(0, extra)
    today = get_dhaka_date()
    if extra == 0:
        admin_extra_ocr_limit.pop(user_id, None)
    else:
        admin_extra_ocr_limit[user_id] = {"date": today, "extra": extra}
    _turso_bg(lambda: _save_admin_extra_ocr_limit(user_id), "save_admin_extra_ocr_limit")

# ══════════════════════════════════════════════════════════════════
#  SYNTHESIS LIBRARY  (admin-managed nested menu, JSON-persisted)
# ══════════════════════════════════════════════════════════════════
LIBRARY_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "library_data.json")

def _default_library() -> dict:
    return {
        "root": {
            "id": "root",
            "title": "📚 Synthesis Library",
            "type": "menu",
            "parent": None,
            "children": []
        }
    }

def load_library() -> dict:
    try:
        with open(LIBRARY_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
        if "root" not in data:
            data = _default_library()
        return data
    except Exception:
        return _default_library()

def save_library(data: dict):
    try:
        with open(LIBRARY_FILE, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
    except Exception as e:
        logger.error(f"Library save error: {e}")
    # Render-এর মতো ephemeral disk-এ local file redeploy-তে মুছে যায়,
    # তাই Turso-তেও একসাথে save করে রাখা হচ্ছে (background task)।
    _turso_bg(lambda: _save_library_to_turso(), "save_library")

library_data: dict = load_library()

def library_new_id() -> str:
    return f"node_{uuid.uuid4().hex[:10]}"

def library_get_children(node_id: str) -> list:
    node = library_data.get(node_id)
    if not node:
        return []
    return [library_data[c] for c in node.get("children", []) if c in library_data]

def library_get_row_sizes(node_id: str) -> list:
    """
    Returns a valid row_sizes list for node_id's children — a list of ints,
    each one the number of buttons in that row, where sum(row_sizes) == len(children).

    Self-healing: if row_sizes is missing or stale (out of sync with the actual
    children count, e.g. after Add/Delete/Cut/Paste touched children directly),
    falls back to a default 2-per-row layout for any "leftover" children.
    """
    node = library_data.get(node_id)
    if not node:
        return []
    children = node.get("children", [])
    total = len(children)
    if total == 0:
        return []
    row_sizes = node.get("row_sizes", [])
    covered = sum(row_sizes)
    if covered > total:
        # stale: trim rows from the end until it fits
        fixed = []
        running = 0
        for sz in row_sizes:
            if running + sz > total:
                break
            fixed.append(sz)
            running += sz
        row_sizes = fixed
        covered = running
    if covered < total:
        # new/unaccounted children → default them into 2-per-row rows
        leftover = total - covered
        row_sizes = list(row_sizes)
        while leftover > 0:
            take = min(2, leftover)
            row_sizes.append(take)
            leftover -= take
    return row_sizes

def library_chunk_children(node_id: str) -> list:
    """Returns children grouped into rows, e.g. [[node, node, node], [node]]."""
    children = library_get_children(node_id)
    row_sizes = library_get_row_sizes(node_id)
    rows = []
    i = 0
    for sz in row_sizes:
        rows.append(children[i:i + sz])
        i += sz
    return rows

def library_add_node(parent_id: str, title: str, node_type: str, text: str = None) -> str:
    new_id = library_new_id()
    library_data[new_id] = {
        "id": new_id,
        "title": title,
        "type": node_type,        # "menu" or "message"
        "parent": parent_id,
        "children": [],
        "text": text
    }
    if parent_id in library_data:
        library_data[parent_id].setdefault("children", []).append(new_id)
    save_library(library_data)
    return new_id

def library_delete_node(node_id: str):
    if node_id == "root" or node_id not in library_data:
        return
    node = library_data[node_id]
    for child_id in list(node.get("children", [])):
        library_delete_node(child_id)
    parent_id = node.get("parent")
    if parent_id and parent_id in library_data:
        children = library_data[parent_id].get("children", [])
        if node_id in children:
            children.remove(node_id)
    library_data.pop(node_id, None)
    save_library(library_data)

LIB_DEFAULT_ROW_SIZE = 2  # used only when auto-placing brand-new buttons into rows

def library_move_node(node_id: str, direction: str) -> bool:
    """
    Move a node within its parent's children, using variable-length rows
    (parent's "row_sizes" list — e.g. row_sizes=[3,1] means:

        Chem  Bio  Phy
        Math

    - left/right → swap with the immediate neighbor in the SAME row.
      No-op at the row's start/end.

    - up   → remove the button from its current row and append it to the
             END of the PREVIOUS row. If its old row becomes empty, that
             row is deleted (rows merge). No-op if there's no previous row.

    - down → remove the button from its current row and append it to the
             END of the NEXT row. If there's no next row, a brand-new row
             containing just this button is created at the end. If its old
             row becomes empty, that row is deleted.

    Only the moved button's position changes — every other button keeps its
    relative order. Returns True if moved successfully.
    """
    node = library_data.get(node_id)
    if not node:
        return False
    parent_id = node.get("parent")
    if not parent_id or parent_id not in library_data:
        return False
    parent = library_data[parent_id]
    children = parent.get("children", [])
    if node_id not in children:
        return False

    row_sizes = library_get_row_sizes(parent_id)
    # locate which row + position-in-row this node is in
    idx = children.index(node_id)
    row_i = 0
    row_start = 0
    for sz in row_sizes:
        if idx < row_start + sz:
            break
        row_start += sz
        row_i += 1
    pos_in_row = idx - row_start
    row_len = row_sizes[row_i]

    if direction == "left":
        if pos_in_row == 0:
            return False
        children[idx - 1], children[idx] = children[idx], children[idx - 1]

    elif direction == "right":
        if pos_in_row >= row_len - 1:
            return False
        children[idx], children[idx + 1] = children[idx + 1], children[idx]

    elif direction == "up":
        if row_i == 0:
            return False
        children.pop(idx)
        row_sizes[row_i] -= 1
        prev_end = row_start  # row_start is exactly the END index of the previous row pre-pop
        children.insert(prev_end, node_id)
        row_sizes[row_i - 1] += 1
        if row_sizes[row_i] == 0:
            row_sizes.pop(row_i)

    elif direction == "down":
        children.pop(idx)
        row_sizes[row_i] -= 1
        if row_i + 1 < len(row_sizes):
            # append to end of next row (next row shifts left by 1 after the pop)
            next_row_start = row_start + row_sizes[row_i]
            next_row_end = next_row_start + row_sizes[row_i + 1]
            children.insert(next_row_end, node_id)
            row_sizes[row_i + 1] += 1
            if row_sizes[row_i] == 0:
                row_sizes.pop(row_i)
        else:
            # no next row → create a brand-new row at the very end
            children.append(node_id)
            if row_sizes[row_i] == 0:
                row_sizes[row_i] = 1  # reuse the now-empty row slot for the new row
            else:
                row_sizes.append(1)
    else:
        return False

    parent["row_sizes"] = row_sizes
    save_library(library_data)
    return True


def library_paste_node(node_id: str, new_parent_id: str) -> bool:
    """
    Clipboard থেকে node_id কে new_parent_id তে move করে।
    Returns True on success.
    """
    node = library_data.get(node_id)
    if not node or node_id == "root" or node_id == new_parent_id:
        return False

    # Prevent pasting into own descendant
    check_id = new_parent_id
    while check_id:
        if check_id == node_id:
            return False  # circular
        check_id = library_data.get(check_id, {}).get("parent")

    old_parent_id = node.get("parent")
    if old_parent_id == new_parent_id:
        return False  # same place, nothing to do

    # Remove from old parent
    if old_parent_id and old_parent_id in library_data:
        old_children = library_data[old_parent_id].get("children", [])
        if node_id in old_children:
            old_children.remove(node_id)

    # Add to new parent
    library_data[new_parent_id].setdefault("children", []).append(node_id)
    node["parent"] = new_parent_id

    save_library(library_data)
    return True


def library_rename_node(node_id: str, new_title: str) -> bool:
    """Rename a node title. Returns True on success."""
    node = library_data.get(node_id)
    if not node or node_id == "root":
        return False
    node["title"] = new_title
    save_library(library_data)
    return True
library_nav: dict = {}

# { admin_id: {"step": ..., "parent_id": ..., ...} }  — admin wizard state
pending_library_build: dict = {}

# { admin_id: "btn" | "post" }  — tracks which editor mode admin is in
library_editor_active: dict = {}

# { admin_id: node_id }  — 1st tap selected node (quick action state)
library_selected_node: dict = {}

# { admin_id: node_id }  — clipboard for Cut/Paste
library_clipboard: dict = {}

# ── Label constants ──
LIB_BACK_LABEL        = "⬅️ Back"
LIB_MAIN_MENU_LABEL   = "🏠 Main Menu"
LIB_BTN_EDITOR_LABEL  = "🗂 Button Editor"
LIB_POST_EDITOR_LABEL = "📝 Posts Editor"
LIB_ADD_BTN_LABEL     = "➕ Add Button"
LIB_ADD_MSG_LABEL     = "📝 Add Msg"
LIB_DELETE_BTN_LABEL  = "🗑 Delete Button"
LIB_DELETE_MSG_LABEL  = "🗑 Delete Msg"

# ── Quick Action bar labels (pic-এর মতো) ──
LIB_QA_EDIT_LABEL     = "✏️ Edit"
LIB_QA_DELETE_LABEL   = "🗑 Delete"
LIB_QA_CUT_LABEL      = "✂️ Cut"
LIB_QA_PASTE_LABEL    = "📋 Paste"
LIB_QA_ENTER_LABEL    = "📂 Enter"
LIB_QA_CANCEL_LABEL   = "❌ Cancel"


def _lib_keyboard_admin_entry() -> ReplyKeyboardMarkup:
    """
    Admin এর জন্য Synthesis Library এর প্রথম screen:
    Button Editor | Posts Editor
    Back          | Main Menu
    """
    return ReplyKeyboardMarkup(
        [
            [KeyboardButton(LIB_BTN_EDITOR_LABEL), KeyboardButton(LIB_POST_EDITOR_LABEL)],
            [KeyboardButton(LIB_BACK_LABEL),        KeyboardButton(LIB_MAIN_MENU_LABEL)],
        ],
        resize_keyboard=True
    )


def _lib_keyboard_btn_editor_root(mode: str = "btn") -> ReplyKeyboardMarkup:
    """
    mode="btn": Button Editor — Add Button + Delete Button only
    mode="post": Posts Editor — Add Msg + Delete Msg only
    """
    root_node = library_data.get("root", {})
    rows = [[KeyboardButton(c["title"]) for c in row] for row in library_chunk_children("root")]
    children = library_get_children("root")

    if mode == "btn":
        rows.append([KeyboardButton(LIB_ADD_BTN_LABEL)])
        if children:
            rows.append([KeyboardButton(LIB_DELETE_BTN_LABEL)])
    else:  # post
        rows.append([KeyboardButton(LIB_ADD_MSG_LABEL)])
        if root_node.get("text"):
            rows.append([KeyboardButton(LIB_DELETE_MSG_LABEL)])

    rows.append([KeyboardButton(LIB_BACK_LABEL), KeyboardButton(LIB_MAIN_MENU_LABEL)])
    return ReplyKeyboardMarkup(rows, resize_keyboard=True)


def _lib_keyboard_btn_editor_node(node_id: str, mode: str = "btn") -> ReplyKeyboardMarkup:
    """
    mode="btn": Button Editor — Add Button + Delete Button only
    mode="post": Posts Editor — Add Msg + Delete Msg only
    """
    node = library_data.get(node_id, {})
    rows = [[KeyboardButton(c["title"]) for c in row] for row in library_chunk_children(node_id)]
    children = library_get_children(node_id)

    if mode == "btn":
        rows.append([KeyboardButton(LIB_ADD_BTN_LABEL)])
        if children:
            rows.append([KeyboardButton(LIB_DELETE_BTN_LABEL)])
    else:  # post
        rows.append([KeyboardButton(LIB_ADD_MSG_LABEL)])
        if node.get("text"):
            rows.append([KeyboardButton(LIB_DELETE_MSG_LABEL)])

    rows.append([KeyboardButton(LIB_BACK_LABEL), KeyboardButton(LIB_MAIN_MENU_LABEL)])
    return ReplyKeyboardMarkup(rows, resize_keyboard=True)


def _lib_keyboard_user(node_id: str) -> ReplyKeyboardMarkup:
    """Clean keyboard for regular users — only content buttons + navigation."""
    rows = [[KeyboardButton(c["title"]) for c in row] for row in library_chunk_children(node_id)]

    nav_row = []
    if node_id != "root":
        nav_row.append(KeyboardButton(LIB_BACK_LABEL))
    nav_row.append(KeyboardButton(LIB_MAIN_MENU_LABEL))
    rows.append(nav_row)

    return ReplyKeyboardMarkup(rows, resize_keyboard=True)


def _lib_keyboard_btn_editor_with_paste(node_id: str, has_clipboard: bool = False) -> ReplyKeyboardMarkup:
    """
    Button Editor keyboard — node-এর ভেতরে।
    Clipboard-এ কিছু থাকলে Paste button দেখায়।
    """
    rows = [[KeyboardButton(c["title"]) for c in row] for row in library_chunk_children(node_id)]

    rows.append([KeyboardButton(LIB_ADD_BTN_LABEL)])
    if has_clipboard:
        rows.append([KeyboardButton(LIB_QA_PASTE_LABEL)])
    rows.append([KeyboardButton(LIB_BACK_LABEL), KeyboardButton(LIB_MAIN_MENU_LABEL)])
    return ReplyKeyboardMarkup(rows, resize_keyboard=True)



def _lib_quick_action_inline(node_id: str) -> InlineKeyboardMarkup:
    """
    Quick Action inline keyboard — pic-এর মতো msg-এর নিচে দেখাবে।
    Row 1: arrows (move, variable row-size aware)
    Row 2: Edit | Delete | Cut
    Row 3: ❌ Cancel
    (Enter বাটন নেই — same button-এ ২য়বার tap করলে enter হবে)
    """
    parent_id = library_data.get(node_id, {}).get("parent", "root")
    siblings  = library_data.get(parent_id, {}).get("children", [])
    total     = len(siblings)
    idx       = siblings.index(node_id) if node_id in siblings else -1

    row_sizes = library_get_row_sizes(parent_id)
    row_i, row_start = 0, 0
    for sz in row_sizes:
        if idx < row_start + sz:
            break
        row_start += sz
        row_i += 1
    pos_in_row = idx - row_start if idx >= 0 else 0
    row_len = row_sizes[row_i] if row_sizes else 1

    # Row 1: arrows — variable row-size aware boundary checks
    arrow_row = []
    if row_i > 0:
        arrow_row.append(InlineKeyboardButton("⬆️", callback_data=f"lqa:{node_id}:up"))
    if total > 1:
        # down is available unless this is the lone button on its own already-last row
        is_last_row = row_i == len(row_sizes) - 1
        if not (is_last_row and row_len == 1):
            arrow_row.append(InlineKeyboardButton("⬇️", callback_data=f"lqa:{node_id}:down"))
    if pos_in_row > 0:
        arrow_row.append(InlineKeyboardButton("⬅️", callback_data=f"lqa:{node_id}:left"))
    if pos_in_row < row_len - 1:
        arrow_row.append(InlineKeyboardButton("➡️", callback_data=f"lqa:{node_id}:right"))

    rows = []
    if arrow_row:
        rows.append(arrow_row)

    # Row 2: Edit | Delete | Cut
    rows.append([
        InlineKeyboardButton("✏️ Edit", callback_data=f"lqa:{node_id}:edit"),
        InlineKeyboardButton("🗑 Delete", callback_data=f"lqa:{node_id}:delete"),
        InlineKeyboardButton("✂️ Cut", callback_data=f"lqa:{node_id}:cut"),
    ])

    # Row 3: Cancel
    rows.append([
        InlineKeyboardButton("❌ Cancel", callback_data=f"lqa:{node_id}:cancel"),
    ])

    return InlineKeyboardMarkup(rows)


daily_stats: dict = {}

def record_poll_solved(user_id: int):
    today = get_dhaka_date()
    if today not in daily_stats:
        daily_stats[today] = {"polls_solved": 0, "active_users": set()}
    daily_stats[today]["polls_solved"] += 1
    daily_stats[today]["active_users"].add(user_id)
    _turso_bg(lambda: _save_daily_stats(today), "save_daily_stats")

def get_dhaka_date() -> str:
    dhaka_tz = pytz.timezone("Asia/Dhaka")
    return datetime.now(dhaka_tz).strftime("%Y-%m-%d")

def get_dhaka_time() -> str:
    dhaka_tz = pytz.timezone("Asia/Dhaka")
    now = datetime.now(dhaka_tz)
    return now.strftime("%d %b %Y, %I:%M %p")


# ══════════════════════════════════════════════════════════════════
#  STREAK SYSTEM — টানা কয়দিন (consecutive days) user bot ব্যবহার করছে
#  তার হিসাব রাখে (poll solve / text Q&A / OCR — যেকোনো activity-তেই count হয়)।
#  registered_users[uid] এ রাখা হয়:
#    streak            → এই মুহূর্তের চলমান streak (consecutive active days)
#    longest_streak    → সর্বোচ্চ streak যা কখনো হয়েছে
#    active_days       → মোট কতদিন (distinct days) bot ব্যবহার করেছে
#    last_active_date  → শেষ কোন Dhaka date-এ activity হয়েছে (YYYY-MM-DD)
# ══════════════════════════════════════════════════════════════════
STREAK_BADGE_TIERS = [
    (30, "🏆"),
    (14, "💎"),
    (7,  "⚡"),
    (3,  "🔥"),
    (1,  "✨"),
]


def get_streak_badge(streak: int) -> str:
    """Streak count অনুযায়ী badge emoji ফেরত দেয় (বড় streak → বড় badge)।"""
    for threshold, badge in STREAK_BADGE_TIERS:
        if streak >= threshold:
            return badge
    return ""


def update_user_streak(user_id: int) -> None:
    """
    User poll solve / text Q&A / OCR — যেকোনো activity করলে একবার call করো।
    দিনে প্রথমবার call হলেই streak update হয় (একই দিনে বারবার call করলে কিছু বদলায় না)।

    নিয়ম:
      • গতকাল active ছিল, আজও active হলো → streak += 1 (ধারাবাহিকতা বজায়)
      • গতকাল active ছিল না (gap ≥ 2 দিন) বা এই প্রথম activity → streak = 1 (নতুন করে শুরু)
      • longest_streak এবং active_days (মোট কতদিন ব্যবহার করেছে) ও সাথে সাথে আপডেট হয়
    """
    info = registered_users.get(user_id)
    if info is None:
        return

    today = get_dhaka_date()
    last_date = info.get("last_active_date", "")

    if last_date == today:
        return  # আজকের জন্য ইতিমধ্যে count হয়ে গেছে

    gap_days = None
    if last_date:
        try:
            last_dt  = datetime.strptime(last_date, "%Y-%m-%d").date()
            today_dt = datetime.strptime(today, "%Y-%m-%d").date()
            gap_days = (today_dt - last_dt).days
        except Exception:
            gap_days = None

    if gap_days == 1:
        info["streak"] = info.get("streak", 0) + 1
    else:
        info["streak"] = 1  # gap ছিল (বা প্রথমবার) — নতুন করে শুরু

    info["longest_streak"]   = max(info.get("longest_streak", 0), info["streak"])
    info["active_days"]      = info.get("active_days", 0) + 1
    info["last_active_date"] = today

    _turso_bg(lambda: _save_user(user_id), "save_user_streak")


# ══════════════════════════════════════════════════════════════════
#  ENGAGEMENT NOTIFICATION SYSTEM
#  ─────────────────────────────────────────────────────────────────
#  User-এর activity pattern (আজ active / আজ এখনো আসেনি / ১ দিন / ২-৩ দিন /
#  ৪-৭ দিন / ৭+ দিন inactive / streak-achievement) অনুযায়ী উপযুক্ত category
#  থেকে randomly একটা message বেছে পাঠানো হয়, সাথে সবসময় Admission
#  Discussion Group-এর button থাকে। নিয়মগুলো:
#    • প্রতিটা user দিনে সর্বোচ্চ ১টা engagement notification পাবে।
#    • সব user-কে একসাথে না পাঠিয়ে, প্রত্যেক user-এর জন্য দিনের একটা
#      ভিন্ন ভিন্ন (deterministic কিন্তু ছড়িয়ে থাকা) সময় বরাদ্দ করা হয়,
#      যাতে সবাই একই মুহূর্তে notification না পায়।
#    • 4–7 দিন inactive হলে প্রতি ২ দিনে ১বার, 7+ দিন inactive হলে প্রতি
#      ৫-৭ দিনে ১বার পাঠানো হয় (প্রতিদিন নয়) — বেশি বার পাঠালে বিরক্ত লাগবে।
#    • Streak চলমান থাকলে মাঝেমধ্যে streak/achievement message, আর
#      নিয়মিত ভালো ব্যবহারকারীদের জন্যও মাঝে মাঝে appreciation + study tip
#      মেশানো থাকে, যাতে একঘেয়ে না লাগে।
# ══════════════════════════════════════════════════════════════════

ADMISSION_GROUP_LINK = "https://t.me/TPCadmission"

def _notify_admission_keyboard() -> InlineKeyboardMarkup:
    """প্রতিটা engagement notification-এর সাথে যুক্ত থাকা common button।"""
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("💬 Admission Discussion Group", url=ADMISSION_GROUP_LINK)],
    ])


NOTIFY_DAILY_ACTIVE = [
    "🔥 আজও দেখা হয়ে গেল! Streakটা কিন্তু দারুণ চলছে।",
    "📚 আরেকটা দিন, আরেকটা ছোট্ট progress. Keep going! 💪",
    "🧠 আজকের brain workout শুরু হবে?",
    "✨ আপনার consistency দেখে ভালো লাগছে! আজও একটু practice হয়ে যাক।",
    "🔥 Streak চলছে মানেই আপনি ঠিক পথে আছেন।",
    "🎯 আজকের ছোট target: কয়েকটা প্রশ্ন solve করে ফেলুন।",
    "☀️ নতুন দিন, নতুন questions! Ready?",
    "📖 প্রতিদিন একটু একটু—এভাবেই preparation strong হয়।",
    "🫶 আপনি আসছেন, practice করছেন—এটাই আসল ব্যাপার।",
    "🚀 আজকের study session-এর জন্য Synthesis Robot ready!",
    "🧩 একটা MCQ দিয়ে শুরু করবেন?",
    "😌 কোনো চাপ নেই—আজ শুধু একটু practice।",
    "🌱 Yesterday-এর চেয়ে আজকে 1% better হওয়ার চেষ্টা করি।",
    "🏆 আপনার streak কিন্তু আপনাকে ছাড়ছে না!",
    "💙 আজও পড়াশোনার সঙ্গী হিসেবে আমাকে ব্যবহার করতে পারেন।",
    "🌤️ আজকের দিনটাও productive হোক।",
    "📌 ছোট ছোট প্রশ্ন, বড় বড় result—চালিয়ে যান।",
    "🧭 আপনার preparation সঠিক দিকেই এগোচ্ছে।",
    "🎉 আজও চেষ্টা চালিয়ে যাচ্ছেন—এটাই আসল জয়।",
    "🔋 প্রতিদিনের practice আপনার confidence-এর battery চার্জ করে।",
    "🌟 প্রতিদিনের এই অভ্যাসটাই একদিন বড় difference তৈরি করবে।",
    "📈 আজও একধাপ এগিয়ে গেলেন।",
    "🧩 আজকের ছোট্ট quiz-টা মিস করবেন না।",
    "🏃 প্রস্তুতির দৌড়ে আজও আপনি এগিয়ে।",
    "🕊️ শান্তমনে আজকের পড়াটা শুরু করুন।",
    "🌈 প্রতিদিনের এই ছোট্ট commitment-ই বড় স্বপ্ন পূরণ করে।",
    "🎓 আজকের effort আগামীর সাফল্যের ভিত্তি।",
    "🧗 প্রতিদিন একটু একটু করে উপরে উঠছেন।",
    "🕰️ আজকের ৫ মিনিট, আগামীকালের জন্য বিনিয়োগ।",
]

NOTIFY_MISSED_TODAY = [
    "👀 আজ আপনাকে এখনো দেখা গেল না!",
    "📚 আজকের study sessionটা কি এখনও বাকি?",
    "🔥 আপনার streak আজকের check-in-এর অপেক্ষায় আছে।",
    "🧠 আজ brain-টা একটু exercise করাবেন?",
    "😌 Busy day? সমস্যা নেই—একটা question দিয়েই শুরু করুন।",
    "⏳ দিন তো চলে যাচ্ছে… আজ একটু practice হবে?",
    "📖 আজকের পড়াটা এখনও শুরু হয়নি? চলুন ছোট করে শুরু করি।",
    "👋 Synthesis Robot আজও এখানে—আপনার অপেক্ষায়।",
    "🎯 আজ শুধু ১টা question solve করুন। তারপর ইচ্ছা হলে আরও।",
    "🔔 ছোট্ট reminder: আজকের study session এখনও বাকি।",
    "🥺 আপনার streak-টা আজ একটু attention চাইছে।",
    "💭 \"পরে করব\" বলার আগে একটা question করে ফেলি?",
    "🔥 প্রতিদিনের habit আজও continue করা যাক?",
    "🌙 দিন শেষ হওয়ার আগে একটু practice করে যাবেন?",
    "✨ আজকের progress এখনও 0 থেকে শুরু করা যায়।",
    "⏰ দিনটা শেষ হওয়ার আগে একটা question দিয়ে দিন।",
    "🎯 আজকের জন্য target ছিল ছোট—এখনো সময় আছে সেটা পূরণ করার।",
    "🌙 রাত হওয়ার আগে ২ মিনিট practice করে ফেলুন।",
    "🧠 Brain-টা আজ একদম idle বসে আছে!",
    "📱 একটা poll forward করলেই আজকের কাজ শুরু হয়ে যাবে।",
    "🥱 আজ একটু busy? তাও একটা question তো করাই যায়।",
    "🔔 আজকের reminder: এখনও কিছু বাকি আছে।",
    "🌤️ দিনটা এখনও শেষ হয়নি—একটু সময় বের করুন।",
    "📖 বইটা আজ এখনো খোলা হয়নি মনে হচ্ছে।",
    "🚀 এখনই শুরু করলে আজকের দিনটা 0 থেকে বাঁচবে।",
    "🎯 ছোট্ট একটা step—আজকের জন্য যথেষ্ট।",
    "💭 আজকের কাজটা কালকে ফেলে রাখবেন না।",
    "🧩 একটা MCQ-ই যথেষ্ট আজকের জন্য।",
    "👋 আজকের চেক-ইন এখনো বাকি।",
]

NOTIFY_1_DAY_INACTIVE = [
    "👀 গতকাল আপনাকে মিস করেছি! আজ আবার শুরু করবেন?",
    "🌱 একদিন বাদ গেছে—কোনো সমস্যা নেই। আজ থেকেই আবার শুরু।",
    "💙 Break হয়ে গেছে? No worries. Let's continue.",
    "🔥 Comeback-এর জন্য perfect day হলো আজ।",
    "📚 গতকালের gap নিয়ে ভাববেন না—আজকের question দিয়ে শুরু করুন।",
    "🙂 একদিন practice না করলেই journey শেষ হয়ে যায় না।",
    "🚀 Ready for a small comeback?",
    "🧠 আপনার brain কি আজ আবার একটু question চায়?",
    "👋 অনেকদিন না, শুধু একটা দিন! আজ আবার দেখা যাক।",
    "🎯 আজকের target খুব ছোট: ১টা question।",
    "📖 গতকাল বাদ গেছে, আজটা যেন না যায়।",
    "✨ Restart button দরকার নেই—শুধু একটা question পাঠান।",
    "🔔 ছোট্ট reminder: আপনার study routine-এ আজ আবার ফিরতে পারেন।",
    "🫶 No guilt. No pressure. Just come back.",
    "🔥 আপনার আগের consistency মনে আছে? আবার শুরু করা যাক।",
    "🌅 নতুন দিনে নতুন শুরু।",
    "🧠 একদিনের বিরতি, brain রিফ্রেশ হয়ে গেছে হয়তো—এবার test করে দেখুন।",
    "📚 গতকাল ছুটি ছিল, আজ থেকে আবার routine-এ ফিরুন।",
    "💫 ছোট্ট gap, ছোট্ট comeback—শুরু করে দিন।",
    "🎯 একদিনের miss কোনো বড় ব্যাপার না।",
    "🙌 আজ থেকে আবার শুরু করলেই হবে।",
    "🔄 Reset হয়ে গেছে? তাহলে আজ থেকে নতুন করে শুরু।",
    "🌱 একদিনের বিরতি, journey থামেনি।",
    "📖 চলুন, আজকের প্রশ্ন দিয়ে আবার শুরু করি।",
    "🧩 একটা ছোট্ট MCQ দিয়ে আজকের কামব্যাক হোক।",
    "😊 কোনো tension নেই, শুধু ফিরে আসুন।",
    "🚦 Green signal আছে—এখনই শুরু করুন।",
    "🕊️ চাপ না নিয়ে, শুধু একটা প্রশ্ন করুন।",
    "🔥 কালকের miss আজকের comeback দিয়ে পুষিয়ে দিন।",
]

NOTIFY_2_3_DAYS_INACTIVE = [
    "👀 Hey! বেশ কিছুদিন হয়ে গেল—সব ঠিকঠাক তো?",
    "📚 আপনার study cornerটা একটু quiet হয়ে গেছে। 😅",
    "🧠 কয়েকদিন gap হয়েছে—আজ ১টা question দিয়ে comeback?",
    "🌱 কয়েকদিন না পড়লেও সমস্যা নেই। আবার শুরু করাটাই important।",
    "💙 Preparation থেমে থাকলেও আপনার journey থামেনি।",
    "🔥 Comeback করার সময় কিন্তু এখনই।",
    "📖 আজ শুধু ২ মিনিট দিন। বেশি কিছু চাইছি না।",
    "👋 Synthesis Robot এখনও এখানেই আছে।",
    "🎯 কয়েকদিনের gap-এর পর first step: একটা সহজ question।",
    "😌 কোনো pressure নেই—যখন ready, তখনই শুরু করুন।",
    "🧩 একটা Poll forward করুন, দেখি কতটা মনে আছে!",
    "🖼️ চাইলে আজ একটা question-এর ছবি পাঠিয়েও শুরু করতে পারেন।",
    "💬 কোনো question নিয়ে আটকে আছেন? পাঠিয়ে দিন।",
    "🌤️ নতুন দিন মানেই নতুন chance।",
    "🚀 3-day gap → 1-question comeback. Deal?",
    "🌤️ কয়েকদিন বিরতির পর, ফিরে আসার এখনই সময়।",
    "🧠 কয়েকদিন gap হলেও brain কিছু ভোলেনি—চেক করে দেখুন।",
    "📚 কয়েক দিনের ছুটি শেষ, এবার একটু পড়ার পালা।",
    "💪 ছোট্ট break শেষ, এবার আবার সেই পুরনো momentum-এ ফিরুন।",
    "🎯 আজ শুধু একটা প্রশ্ন—তাতেই আবার শুরু হয়ে যাবে।",
    "🌱 কয়েকদিন দূরে থেকেছেন, তাতে কিছু যায় আসে না—আজ ফিরুন।",
    "👋 আপনার জন্য কিছু নতুন প্রশ্ন অপেক্ষা করছে।",
    "🔥 এই ছোট্ট gap-টাকেই বড় comeback-এ পরিণত করুন।",
    "📖 কয়েক দিনের বিরতির পর বইটা আবার খুলুন।",
    "🧩 একটা সহজ প্রশ্ন দিয়ে শুরু করুন, তারপর ধীরে ধীরে গতি বাড়ান।",
    "😌 কোনো চাপ নেই—নিজের সময়ে ফিরুন।",
    "🚀 আজকের একটা ছোট্ট পদক্ষেপ বড় পরিবর্তন আনতে পারে।",
    "💙 আপনার প্রস্তুতি এখনো শেষ হয়ে যায়নি—শুধু একটু বিরতি নিয়েছে।",
    "🌟 ছোট্ট বিরতির পর বড় comeback করে দেখান।",
]

NOTIFY_4_7_DAYS_INACTIVE = [
    "👋 অনেকদিন দেখা নেই! আপনার study journey কেমন চলছে?",
    "📚 এক সপ্তাহের কাছাকাছি gap হয়ে গেছে—আবার একটু শুরু করবেন?",
    "🥺 আপনার questions-গুলো কিন্তু আপনাকে মিস করছে!",
    "🔥 Streak চলে গেলেও progress আবার তৈরি করা যায়।",
    "🌱 কয়েকদিনের break কোনো failure নয়। Comeback-টাই আসল।",
    "💙 পড়াশোনা থেকে দূরে ছিলেন? আজ শুধু ১টা question দিয়ে ফিরুন।",
    "🧠 দেখি তো কয়েকদিন পর আপনার brain কতটা sharp আছে!",
    "🎯 আজকের mission: শুধু ১টি question।",
    "👀 আপনার chatটা বেশ quiet হয়ে গেছে।",
    "📖 বই খুলতে ইচ্ছা করছে না? একটা question দিয়ে শুরু করুন।",
    "😌 No lecture. No pressure. Just one small step.",
    "🚀 Preparation-এ comeback-এর জন্য কোনো special day লাগে না।",
    "🧩 একটা MCQ পাঠান—আজকের comeback শুরু হোক।",
    "🫶 আপনি যতদিনই দূরে থাকুন, আবার শুরু করার সুযোগ সবসময় আছে।",
    "🔔 Comeback reminder: আজ ২ মিনিট পড়াশোনা করে দেখবেন?",
    # ── personalized variants (নাম / longest streak / lifetime usage callback) ──
    "👋 {name}, কয়েকদিন ধরে আপনাকে দেখছি না। সব ঠিক আছে তো?",
    "🏅 {name}, আপনার সেরা streak ছিল {longest_streak} দিন! সেটা আবার ছুঁয়ে দেখবেন?",
    "🧩 {name}, এখন পর্যন্ত {poll_count}+ question solve করেছেন—এই momentum হারাবেন না।",
    "💙 {name}, আপনার progress এতদিনের effort-এ তৈরি। কয়েকদিনের gap সেটা মুছে দেয় না।",
    "🔥 {name}, {longest_streak}-দিনের streak গড়েছিলেন একসময়। আজ থেকে আবার সেই পথে?",
    # ── exact-day-count variants (কতদিন ধরে inactive সেটা সরাসরি বলা, প্রতিদিন একই না) ──
    "😳 তুমি গত {days} দিন ধরে কোনো poll solve করোনি! একটা poll forward করো, আবার শুরু করো 🚀",
    "⏳ {name}, {days} দিন হয়ে গেছে শেষ poll solve করার পর। আজ একটা দিয়ে gapটা ভাঙবে?",
    "👀 {days} দিন ধরে chat-টা quiet—একটা MCQ পাঠিয়ে আবার শুরু করো।",
    "🌱 এক সপ্তাহের কাছাকাছি বিরতি—কোনো ব্যাপার না, আজই শুরু করা যায়।",
    "📚 আপনার বইটা কয়েকদিন ধরে বন্ধ পড়ে আছে হয়তো।",
    "🔥 এই gap-টা শেষ করার সবচেয়ে ভালো সময় এখনই।",
    "🧠 কয়েকদিন পর brain-এর একটা ছোট্ট test দিন।",
    "💙 কয়েকদিনের দূরত্ব সম্পর্ক নষ্ট করে না—আপনার আর প্রস্তুতির সম্পর্কও না।",
    "🎯 আজ শুধু ১টা প্রশ্ন—এতটুকুই যথেষ্ট শুরু করার জন্য।",
    "👋 {name}, আপনার জন্য আমরা এখনো এখানে অপেক্ষা করছি।",
    "🚀 ছোট্ট একটা পদক্ষেপ, কিন্তু সেটাই বড় পরিবর্তনের শুরু।",
    "📖 বই খোলার আগে শুধু একবার ভাবুন—কেন শুরু করেছিলেন।",
    "🌤️ সপ্তাহের মাঝপথে একটা ছোট্ট restart নেওয়া যায়।",
    "🧩 একটা প্রশ্ন forward করুন, বাকিটা আমরা দেখছি।",
    "😌 কোনো judgment নেই এখানে—শুধু ফিরে আসুন।",
    "🔔 Reminder: আপনার preparation-এর জন্য আজকের দিনটাও গুরুত্বপূর্ণ।",
    "💫 ছোট্ট বিরতি শেষে বড় momentum তৈরি করা যায়।",
]

NOTIFY_7_PLUS_DAYS_INACTIVE = [
    "👋 অনেকদিন হয়ে গেল! আশা করি আপনার preparation ভালোই চলছে।",
    "📚 অনেকদিন Synthesis Robot ব্যবহার করেননি—আবার দরকার হলে আমি এখানেই আছি।",
    "🌱 কিছুদিন gap হয়েছে? আজ থেকে নতুনভাবে শুরু করা যায়।",
    "💙 কোনো guilt নেই। আপনার নিজের pace-এ আবার শুরু করুন।",
    "🧠 অনেকদিন পর একটা question solve করে memoryটা refresh করবেন?",
    "📖 Preparation যদি আবার শুরু করতে চান, প্রথম step খুব ছোট হতে পারে।",
    "🎯 আজ শুধু একটা question। ব্যস।",
    "👀 Long time no see! 😄 আবার study mode-এ ফিরবেন?",
    "🚀 আপনার next study session-এর জন্য Synthesis Robot ready।",
    "🌸 পড়াশোনায় pause হতে পারে, কিন্তু comeback-এর দরজা সবসময় খোলা।",
    # ── personalized variants ──
    "👋 {name}, আপনাকে অনেকদিন মিস করছি! Preparation কেমন চলছে?",
    "🏅 {name}, আপনার {longest_streak}-দিনের best streak কিন্তু এখনও রেকর্ড হয়ে আছে—ভাঙতে চান?",
    "🧩 {name}, {poll_count}+ question ইতিমধ্যে solve করে ফেলেছেন। এই journey অসম্পূর্ণ রাখবেন না।",
    "💙 {name}, যত দূরেই থাকুন, আপনার জন্য জায়গাটা এখনও আছে। এক লাইনে হলেও ফিরে আসুন।",
    # ── exact-day-count variants ──
    "😳 তুমি গত {days} দিন ধরে বট-টা একদমই ব্যবহার করোনি! একটা poll forward করো, আবার শুরু করো 🚀",
    "⏳ {name}, {days} দিন হয়ে গেছে—কিন্তু ফিরে আসার জন্য কখনোই দেরি হয় না।",
    "👀 {days} দিন ধরে খবর নেই! একটা question দিয়ে আবার শুরু করো তো দেখি।",
    "🌱 অনেকদিন পর হলেও, ফিরে আসাটাই সবচেয়ে গুরুত্বপূর্ণ।",
    "📚 আপনার preparation-এর journey এখনো শেষ হয়নি, শুধু একটু থেমে আছে।",
    "🔥 যত দিনই দূরে থাকুন, শুরু করার জন্য আজকের চেয়ে ভালো দিন নেই।",
    "🧠 অনেকদিন পর brain-কে একটু warm-up করান।",
    "💙 কোনো guilt লাগবে না—শুধু একটা ছোট্ট step নিন।",
    "🎯 আজ শুধু একটা প্রশ্ন। বাকিটা ধীরে ধীরে হবে।",
    "👋 {name}, আপনার এই জায়গাটা এখনো খালি পড়ে নেই—আপনার জন্যই রাখা আছে।",
    "🚀 Comeback story-গুলো সবচেয়ে inspiring হয়—আপনারটাও হোক।",
    "📖 বই বন্ধ থাকলেও স্বপ্নটা তো বন্ধ হয়নি।",
    "🌤️ অনেকদিন পর একটা নতুন শুরু করার সুযোগ এসেছে।",
    "🧩 একটা প্রশ্ন solve করেই দেখুন, কেমন লাগে।",
    "😌 দেরি হয়ে গেছে ভেবে থেমে থাকবেন না—শুরু করাই আসল।",
    "🔔 আপনার progress এখনো record-এ আছে, শুধু আজকের অংশটা বাকি।",
    "💫 অনেকদিন পর একটা ছোট্ট শুরুই বড় কিছুর ভিত্তি হতে পারে।",
]

NOTIFY_STREAK_ACHIEVEMENT = [
    "🔥 {streak} দিনের streak! আজও একটা question করে streakটা এগিয়ে নিন।",
    "🏆 আর মাত্র {remaining} দিন—নতুন badge unlock হবে!",
    "🎉 আজকের activity complete! আপনার consistency সত্যিই impressive।",
    "🔥 Personal best-এর কাছাকাছি চলে এসেছেন!",
    "📈 আপনার progress ধীরে ধীরে কিন্তু সুন্দরভাবে বাড়ছে।",
    "🏅 {streak} days strong! Keep it going.",
    "🎯 আজকের ছোট effort-ই আপনার long-term preparation তৈরি করছে।",
    "🧠 আজকের questions শেষ? নিজের progressটা একবার দেখে নিন।",
    "📊 কতটা এগিয়েছেন দেখতে /myreport খুলে দেখতে পারেন।",
    "🏆 আপনার longest streak: {longest_streak} দিন—এবার কি সেটা beat করবেন?",
    "🔥 নতুন record করার আরেকটা সুযোগ আজও আছে।",
    "🎉 100 questions solved! আপনার effort কিন্তু কম নয়।",
    "💪 আজকের কয়েকটা question = আগামীকালের confidence।",
    "🌟 আপনি শুধু question solve করছেন না—একটা habit তৈরি করছেন।",
    "❤️ Keep learning. Keep coming back. ছোট ছোট effort-ই বড় result তৈরি করে।",
    "🔥 {streak} দিন ধরে থেমে নেই আপনার journey!",
    "🏆 প্রতিদিনের ছোট্ট জয়গুলোই বড় সাফল্যের ভিত্তি।",
    "📈 আপনার consistency অন্যদের চেয়ে আপনাকে এগিয়ে রাখছে।",
    "🎯 আজকের কাজটা শেষ—নিজেকে একটু credit দিন।",
    "🌟 {streak} দিনের streak মানে {streak} দিনের discipline।",
    "🧠 প্রতিদিনের এই habit আপনার exam-এর দিন কাজে লাগবে।",
    "🏅 আপনার effort চোখে না পড়লেও, result-এ ঠিকই দেখা যাবে।",
    "🔥 এভাবেই চালিয়ে যান—momentum ভাঙবেন না।",
    "💪 আজকের consistency আগামীকালের confidence তৈরি করছে।",
    "🎉 আরেকটা দিন শেষ, আরেকটা ধাপ এগিয়ে।",
    "🚀 Streak বাড়ছে মানে habit শক্ত হচ্ছে।",
    "🌱 প্রতিদিনের ছোট্ট effort, বড় ফলাফলের বীজ।",
    "🏆 {longest_streak} দিনের record-এর দিকে এগোচ্ছেন।",
    "📊 নিজের progress /myreport-এ একবার দেখে নিন—ভালো লাগবে।",
    "❤️ আজকের এই কাজটা ভবিষ্যতের আপনাকে গর্বিত করবে।",
]

TIP_MOTIVATION = [
    "🎯 Admission chance শুধু বেশি পড়লেই আসে না—সঠিক জিনিস বারবার পড়লেই আসে।",
    "🏆 অন্যরা কত ঘণ্টা পড়ছে সেটা না দেখে, আপনি আজ কতটা effective পড়লেন সেটা দেখুন।",
    "🔥 আপনার প্রতিদিনের ৩ ঘণ্টা focused study, অনেক সময় ৮ ঘণ্টার distracted study-এর চেয়েও valuable।",
    "📚 একটা chapter ভালোভাবে শেষ করা, পাঁচটা chapter অর্ধেক পড়ার চেয়ে বেশি useful।",
    "💭 আজকের পড়া হয়তো ছোট মনে হচ্ছে, কিন্তু admission-এর দিন প্রতিটি ছোট effort-এর মূল্য বুঝবেন।",
    "🎯 Admission preparation-এর লক্ষ্য শুধু syllabus শেষ করা নয়—question দেখলে চিনতে পারা।",
    "🚀 আজকের একটা ভালো study session আপনাকে হাজার হাজার candidate-এর সামনে এগিয়ে দিতে পারে।",
    "🧠 পড়ার সময় মনে রাখুন: আমি শুধু পড়ছি না, exam-এর জন্য নিজেকে train করছি।",
    "🔥 Competition বড়? তাহলে preparation-টাও smart হতে হবে।",
    "🌱 প্রতিদিন 1% improvement দীর্ঘ সময়ে বিশাল difference তৈরি করে।",
    "🏆 আপনি আজ যে chapterটা পড়ছেন, হয়তো সেখান থেকেই আপনার admission-এর গুরুত্বপূর্ণ একটা question আসবে।",
    "💙 Result নিয়ে এখনই চিন্তা করবেন না। আজকের কাজটা ঠিকভাবে করুন।",
    "🎯 Target university নয়, target করুন প্রতিদিনের improvement।",
    "🔥 Motivation-এর অপেক্ষা করবেন না। Routine follow করুন—motivation পরে আসবে।",
    "📖 আজকে মন বসছে না? শুধু 10 মিনিট শুরু করুন। অনেক সময় শুরু করাটাই সবচেয়ে কঠিন।",
    "🚀 আপনার competition অন্য student না—গতকালের আপনি।",
    "🧠 ভুল question মানেই failure না। ভুল question মানে কোথায় দুর্বল সেটা জানা।",
    "🏅 Admission preparation-এ consistency অনেক সময় intelligence-এর চেয়েও বড় advantage।",
    "🌟 আজকের পড়া boring হতে পারে, কিন্তু ভবিষ্যতের সুযোগটা boring হবে না।",
    "❤️ একদিন আপনি চাইবেন, আজকের দিনটা আরও একটু বেশি কাজে লাগিয়েছিলেন।",
]

TIP_PRODUCTIVITY = [
    "⏱️ কম সময়ে বেশি পড়তে চাইলে প্রথমে distraction কমান, study time বাড়ানো নয়।",
    "📵 ১ ঘণ্টা পড়বেন? ফোনটা ১ ঘণ্টার জন্য দূরে রাখুন।",
    "🎯 পড়তে বসে \"অনেক পড়ব\" বলবেন না। বলুন—এই ৪৫ মিনিটে এই chapter-এর এই অংশ শেষ করব।",
    "🧠 Active recall ব্যবহার করুন: বই বন্ধ করে নিজেকে প্রশ্ন করুন—আমি কী মনে রাখতে পেরেছি?",
    "📚 ৫০ মিনিট focused study + ১০ মিনিট break—একটানা distracted study-এর চেয়ে অনেক effective হতে পারে।",
    "🚫 পড়ার সময় notification off করুন। আপনার brain-কে বারবার context switch করতে দেবেন না।",
    "✍️ পড়ার সময় সবকিছু highlight করার দরকার নেই। যেটা ভুলে যাওয়ার সম্ভাবনা বেশি, সেটাই mark করুন।",
    "🎯 একটা study session = একটা clear target।",
    "⏰ সময় কম? তাহলে easy chapter দিয়ে সময় নষ্ট না করে high-yield topic আগে ধরুন।",
    "🧠 পড়ার পর নিজেকে জিজ্ঞেস করুন: \"আমি এটা কাউকে বুঝিয়ে বলতে পারব?\" না পারলে আবার দেখুন।",
    "📖 ৩০ মিনিট focused study > ২ ঘণ্টা বই সামনে রেখে phone ব্যবহার।",
    "🔥 পড়ার সময় শুধু পড়বেন না—প্রশ্ন করুন, recall করুন, solve করুন।",
    "📝 Study session শুরু করার আগে ৩টা task লিখে নিন। তারপর একটার পর একটা শেষ করুন।",
    "🧠 Brain-এর attention সীমিত। তাই একই সময়ে বই + Facebook + Telegram + YouTube করবেন না।",
    "⏱️ সময় কম থাকলে perfection বাদ দিন—priority ঠিক করুন।",
    "🎯 আজ ৫ ঘণ্টা পড়তে না পারলে ২ ঘণ্টা focused পড়ুন। Zero করার চেয়ে অনেক ভালো।",
    "🔄 একটা chapter পড়া শেষ → MCQ solve → ভুলগুলো mark → পরে revision। এই cycle ব্যবহার করুন।",
    "📵 \"শুধু ৫ মিনিট ফোন দেখব\"—এই ৫ মিনিটই অনেক সময় ৫০ মিনিট হয়ে যায়। 😅",
    "🔥 পড়ার আগে ঠিক করুন কখন শেষ করবেন, শুধু কখন শুরু করবেন তা নয়।",
    "🌙 ঘুমানোর আগে পরের দিনের ৩টা study target লিখে রাখুন। সকালে সিদ্ধান্ত নিতে সময় নষ্ট হবে না।",
]

TIP_MAIN_BOOK = [
    "📖 মূল বইকে বাদ দিয়ে শুধু shortcut পড়লে preparation-এর foundation দুর্বল হতে পারে।",
    "🎯 মূল বই পড়ার সময় line-by-line মুখস্থ করার চেষ্টা না করে concept বুঝুন।",
    "🧠 একটা topic পড়ুন → বই বন্ধ করুন → নিজের ভাষায় explain করুন।",
    "📚 মূল বইয়ের গুরুত্বপূর্ণ definition, exception, diagram ও table-এ বেশি attention দিন।",
    "🔍 MCQ করার আগে chapter-এর মূল concept একবার ভালোভাবে পড়ে নিন।",
    "📝 মূল বই পড়ার সময় পাশে ছোট করে নিজের ভাষায় keyword লিখতে পারেন।",
    "📖 শুধু guide-এর answer মুখস্থ করবেন না—answerটা মূল বইয়ের কোথা থেকে এসেছে সেটা খুঁজে দেখুন।",
    "🧠 একটা MCQ ভুল হয়েছে? শুধু correct answer দেখবেন না। মূল বই খুলে topicটা আবার দেখুন।",
    "🎯 Admission-এর জন্য মূল বই + question practice—দুটো একসাথে রাখুন।",
    "📚 Chapter শেষ করার পর আবার পুরো chapter পড়ার আগে নিজের ভুলগুলো review করুন।",
    "🔥 মূল বইয়ের ছোট্ট একটা line কখনো কখনো MCQ-এর answer হয়ে যায়।",
    "👀 Diagram, chart, comparison table—এসবকে ignore করবেন না।",
    "📖 প্রথমবার পড়ার লক্ষ্য: বোঝা। দ্বিতীয়বার: মনে করা। তৃতীয়বার: দ্রুত recall করা।",
    "🧠 পড়া শেষে বই বন্ধ করে ৫টি গুরুত্বপূর্ণ point লিখে ফেলুন।",
    "🎯 মূল বই পড়ার সময় নিজেকে জিজ্ঞেস করুন—\"এখান থেকে কী ধরনের MCQ হতে পারে?\"",
]

TIP_NOTE_MAKING = [
    "✍️ Note মানে পুরো বই আবার copy করা নয়।",
    "📝 ভালো note হলো এমন কিছু, যেটা exam-এর আগে দ্রুত revise করা যায়।",
    "🎯 Note-এ রাখুন: formula + exception + confusing point + নিজের ভুল।",
    "🧠 কোনো topic বুঝতে কষ্ট হলে নিজের ভাষায় ২–৩ লাইনে লিখে রাখুন।",
    "📚 বড় paragraph না লিখে keyword + arrow + ছোট explanation ব্যবহার করুন।",
    "🔥 আপনার সবচেয়ে valuable note হতে পারে আপনার নিজের ভুলের list।",
    "📝 একই ভুল বারবার হলে সেটা আলাদা করে mark করুন। 🔴",
    "🎯 Revision-এর জন্য এমন note বানান যাতে ১০ মিনিটে পুরো topic recall করা যায়।",
    "📖 বইয়ের সবকিছু note করার দরকার নেই। যেটা বই খুলে খুঁজতে সময় লাগে, সেটা note করুন।",
    "🧠 Note সুন্দর হওয়ার চেয়ে useful হওয়া বেশি গুরুত্বপূর্ণ।",
]

TIP_REVISION_MCQ = [
    "🔄 পড়া শেষ মানেই chapter শেষ নয়। Revision না করলে অনেক কিছু ভুলে যাবেন।",
    "🧠 আজ পড়েছেন? কাল ৫ মিনিট recall করুন।",
    "🎯 একটা chapter পড়ার পর সাথে সাথে MCQ করুন। কোথায় gap আছে বুঝতে পারবেন।",
    "❌ ভুল MCQ skip করবেন না। আপনার ভুলগুলোই আপনার next revision-এর roadmap।",
    "📊 বারবার ভুল হওয়া topicগুলোকে Weak Topic হিসেবে ধরে extra practice করুন।",
    "🔥 ১০০টা নতুন question করার চেয়ে নিজের ভুল ২০টা question আবার solve করা বেশি valuable হতে পারে।",
    "🧠 Answer দেখে \"হ্যাঁ, এটা তো জানতাম!\" বলবেন না। বই বন্ধ করে নিজে answer করতে পারছেন কি না দেখুন।",
    "🔄 Read → Recall → Solve → Review → Repeat.",
    "📚 Revision-এর সময় পুরো chapter আবার পড়ার আগে নিজের notes + mistakes দেখুন।",
    "🎯 সপ্তাহে অন্তত একবার পুরোনো topics-এর mixed MCQ দিন।",
    "🧩 শুধু chapter-wise question নয়—মাঝে মাঝে mixed questions করুন। Exam-এর environment-এর জন্য useful।",
    "⏱️ Timer দিয়ে MCQ solve করুন। শুধু accuracy নয়, speed-ও train করুন।",
    "🔥 যে question আজ ভুল হয়েছে, সেটাই কয়েকদিন পর আবার নিজে solve করার চেষ্টা করুন।",
    "🧠 Revision-এর সেরা test হলো—বই বন্ধ করে কতটা মনে আছে?",
    "🏆 আপনার ভুলের সংখ্যা কমতে থাকাই আসল progress।",
]

TIP_EXAM_STRATEGY = [
    "🎯 সব topic সমান গুরুত্বপূর্ণ ধরে পড়বেন না। আপনার syllabus, previous questions ও weak areas দেখে priority ঠিক করুন।",
    "📊 যেসব topic থেকে বারবার question আসে, সেগুলোকে extra attention দিন।",
    "⏱️ Exam preparation-এ speed + accuracy—দুটোই train করতে হবে।",
    "🧠 Easy question আগে করার অভ্যাস আপনার confidence এবং time management দুটোতেই সাহায্য করতে পারে।",
    "📚 শুধু নতুন নতুন chapter পড়বেন না। পুরোনোগুলোও ধরে রাখুন।",
    "🔥 Admission preparation হলো coverage + revision + question practice—তিনটার balance।",
    "🎯 সপ্তাহের শুরুতে target করুন, সপ্তাহের শেষে দেখুন কতটা complete হয়েছে।",
    "📈 /myreport দেখে নিজের activity এবং streak track করুন—progress চোখে দেখলে consistency ধরে রাখা সহজ হয়।",
    "🧠 কোনো topic বারবার ভুল হলে সেটাকে avoid নয়—attack করুন।",
    "🏆 অন্যের routine copy করার দরকার নেই। নিজের available time অনুযায়ী sustainable routine বানান।",
]

TIP_EMOTIONAL = [
    "🌱 আজ হয়তো আপনি পিছিয়ে আছেন। কিন্তু আজকের ২ ঘণ্টা আগামীকালের gap কমিয়ে দিতে পারে।",
    "🔥 Admission-এর result এখন আপনার হাতে নেই। কিন্তু আজ কতটা পড়বেন—সেটা আপনার হাতে।",
    "📚 আজ একটা chapter। কাল আরেকটা। এভাবেই বড় syllabus ছোট হয়ে যায়।",
    "🧠 আপনার future নিজে নিজে তৈরি হবে না—আজকের ছোট ছোট decisions-এ তৈরি হবে।",
    "🎯 যখন পড়তে ইচ্ছা করবে না, তখন motivation খুঁজবেন না। শুধু ১০ মিনিট বসুন।",
    "💙 অন্য কেউ আপনার journey বুঝুক বা না বুঝুক, আপনার effort-এর মূল্য আছে।",
    "🔥 একদিন result বের হবে। তখন wish করবেন না যে আরও motivation পেলে পড়তেন—বরং আজই শুরু করুন।",
    "🌟 Slow progress is still progress. আজ একটু এগিয়ে যান।",
    "🏆 আপনার goal যদি বড় হয়, তাহলে প্রতিদিনের ছোট কাজগুলোকে ছোট ভাববেন না।",
    "❤️ আজকের আপনি যদি কালকের আপনাকে একটু এগিয়ে দিতে পারেন—তাহলেই আজকের দিনটা সফল।",
]


# সব category-র study tip pool একসাথে — মাঝে মাঝে (৪-৫ দিনে একবার এর মতো)
# regular/daily-active user-দের কাছে motivation বা study-tip পাঠানোর জন্য ব্যবহার হয়।
ALL_STUDY_TIPS = (
    TIP_MOTIVATION + TIP_PRODUCTIVITY + TIP_MAIN_BOOK + TIP_NOTE_MAKING +
    TIP_REVISION_MCQ + TIP_EXAM_STRATEGY + TIP_EMOTIONAL
)


def _notify_stagger_minute(user_id: int, date_str: str, window_start_min: int = 9 * 60,
                            window_end_min: int = 22 * 60) -> int:
    """
    প্রতিটা user-এর জন্য দিনের একটা fixed কিন্তু আলাদা আলাদা সময় (minute-of-day,
    সকাল ৯টা থেকে রাত ১০টার মধ্যে) বের করে, যেটা uid + date-এর উপর ভিত্তি করে
    deterministic (তাই একই দিনে বারবার recompute করলেও একই সময় আসবে, কিন্তু
    ভিন্ন ভিন্ন user-এর সময় ভিন্ন ভিন্ন হবে — একসাথে সবাইকে notify করা এড়ানো যায়)।
    """
    h = hashlib.sha256(f"{user_id}:{date_str}".encode()).hexdigest()
    span = max(1, window_end_min - window_start_min)
    offset = int(h[:8], 16) % span
    return window_start_min + offset


def _days_between(date_a: str, date_b: str) -> "int | None":
    """YYYY-MM-DD দুটো date-এর মধ্যে দিনের পার্থক্য (date_b - date_a). Invalid হলে None."""
    try:
        da = datetime.strptime(date_a, "%Y-%m-%d").date()
        db = datetime.strptime(date_b, "%Y-%m-%d").date()
        return (db - da).days
    except Exception:
        return None


def _build_engagement_notification(user_id: int, info: dict, today: str) -> "tuple[str, bool] | None":
    """
    User-এর activity/streak অনুযায়ী উপযুক্ত notification category বেছে
    (text, is_gap_category) রিটার্ন করে। is_gap_category=True মানে এই category-র
    জন্য "প্রতিদিন নয়, কয়েকদিন পরপর" নিয়ম প্রযোজ্য (৪-৭ দিন / ৭+ দিন)।
    কোনো কারণে পাঠানো উচিত না হলে None রিটার্ন করে।
    """
    last_active_date = info.get("last_active_date", "")
    streak           = info.get("streak", 0)
    longest_streak   = info.get("longest_streak", 0)
    poll_count       = info.get("poll_count", 0) + info.get("qa_count", 0) + info.get("ocr_count", 0)

    days_gap = _days_between(last_active_date, today) if last_active_date else None

    # ── আজই active হয়ে গেছে ──
    if days_gap == 0:
        # Streak চলমান (৩+ দিন) হলে মাঝেমধ্যে streak/achievement message,
        # বাকি সময় সাধারণ "daily active" appreciation বা মাঝেমধ্যে একটা study tip।
        seed = int(hashlib.sha256(f"active:{user_id}:{today}".encode()).hexdigest()[:8], 16)
        if streak >= 3 and seed % 3 != 0:
            remaining_map = [(7, "⚡"), (14, "💎"), (30, "🏆")]
            remaining = next((t - streak for t, _ in remaining_map if t > streak), 0)
            msg = random.choice(NOTIFY_STREAK_ACHIEVEMENT)
            try:
                msg = msg.format(streak=streak, remaining=max(remaining, 1), longest_streak=longest_streak)
            except Exception:
                pass  # format key মিসিং হলেও crash না করে raw text পাঠানো হবে
            return msg, False
        if seed % 5 == 0:  # প্রতি ~৫ দিনে একবার study tip মিশিয়ে দেওয়া হয়
            return random.choice(ALL_STUDY_TIPS), False
        return random.choice(NOTIFY_DAILY_ACTIVE), False

    # ── আজ এখনো আসেনি, কিন্তু গতকাল active ছিল (নিয়মিত user) ──
    if days_gap == 1 and streak >= 1:
        # শুধু সন্ধ্যার দিকে পাঠানো ভালো — কিন্তু এখানে category ঠিক করাই যথেষ্ট,
        # সময় stagger window (সকাল ৯টা - রাত ১০টা)-এই এটা handle হয়ে যায়।
        return random.choice(NOTIFY_MISSED_TODAY), False

    if days_gap is None:
        return None  # কখনো active হয়নি এমন user-কে engagement notification পাঠানো হয় না

    if days_gap == 1:
        return random.choice(NOTIFY_1_DAY_INACTIVE), False
    if 2 <= days_gap <= 3:
        return random.choice(NOTIFY_2_3_DAYS_INACTIVE), False
    if 4 <= days_gap <= 7 or days_gap > 7:
        name = info.get("name") or "Student"
        pool = NOTIFY_4_7_DAYS_INACTIVE if days_gap <= 7 else NOTIFY_7_PLUS_DAYS_INACTIVE
        msg = random.choice(pool)
        try:
            msg = msg.format(name=name, longest_streak=longest_streak, poll_count=poll_count, days=days_gap)
        except Exception:
            pass  # কোনো কারণে format key মিসিং হলে raw text-ই পাঠানো হবে
        return msg, True

    return None


async def _engagement_notification_loop(app):
    """
    প্রতি ১৫ মিনিট পরপর check করে — কোন user-দের আজকের বরাদ্দকৃত (staggered)
    সময় হয়ে গেছে এবং আজ এখনো notification পায়নি, তাদের উপযুক্ত category-র
    message পাঠায়। ৪-৭ দিন / ৭+ দিন inactive user-দের জন্য প্রতিদিন নয়,
    গ্যাপ নিয়ম মেনে পাঠানো হয়।
    """
    CHECK_INTERVAL = 15 * 60
    while True:
        await asyncio.sleep(CHECK_INTERVAL)
        try:
            dhaka_tz = pytz.timezone("Asia/Dhaka")
            now      = datetime.now(dhaka_tz)
            today    = now.strftime("%Y-%m-%d")
            now_min  = now.hour * 60 + now.minute

            for uid, info in list(registered_users.items()):
                if uid == ADMIN_ID:
                    continue

                # প্রতিটা user আলাদাভাবে try/except-এ wrap করা — কোনো একজনের
                # জন্য error হলেও বাকি সবার notification miss হওয়া উচিত না
                # (আগে এই wrap না থাকায় একজনের exception পুরো batch থামিয়ে দিত)।
                try:
                    # আজ ইতিমধ্যে notification পেয়ে থাকলে skip — দিনে সর্বোচ্চ ১টা
                    if info.get("last_notify_date") == today:
                        continue

                    # এই user-এর জন্য আজকের বরাদ্দকৃত সময় এখনো না হলে skip
                    assigned_min = _notify_stagger_minute(uid, today)
                    if now_min < assigned_min:
                        continue

                    result = _build_engagement_notification(uid, info, today)
                    if result is None:
                        continue
                    message, is_gap_category = result

                    # Gap-based category (৪-৭ দিন / ৭+ দিন) হলে সবসময় পাঠানো হয় না —
                    # শেষবার notify হওয়ার পর পর্যাপ্ত দিন না গেলে এবার skip।
                    if is_gap_category:
                        last_notify_date = info.get("last_notify_date", "")
                        gap = _days_between(last_notify_date, today) if last_notify_date else None
                        days_gap = _days_between(info.get("last_active_date", ""), today) or 0
                        min_gap_days = 2 if days_gap <= 7 else 5
                        if gap is not None and gap < min_gap_days:
                            continue

                    await app.bot.send_message(
                        chat_id=uid,
                        text=message,
                        reply_markup=_notify_admission_keyboard(),
                    )
                    info["last_notify_date"] = today
                    info["last_notify_ts"]   = time.time()
                    _turso_bg(lambda u=uid: _save_user(u), "save_user_notify")
                except Exception as e:
                    logger.warning(f"Engagement notify failed for {uid}: {e}")

                await asyncio.sleep(0.05)  # rate-limit avoid করতে ছোট delay
        except Exception as e:
            logger.error(f"Engagement notification loop error: {e}")

# { user_id: asyncio.Lock } — একই user থেকে দ্রুত একাধিক poll forward এলে
# check_rate_limit + consume_rate_limit এর মাঝে AI call (কয়েক সেকেন্ড) চলাকালীন
# race condition হতে পারতো (দুটো poll-ই limit/cooldown check pass করে ফেলত)।
# এই lock per-user atomically reserve করে সেটা ঠেকায়।
_rate_limit_locks: dict = {}

def _get_rate_limit_lock(user_id: int) -> "asyncio.Lock":
    lock = _rate_limit_locks.get(user_id)
    if lock is None:
        lock = asyncio.Lock()
        _rate_limit_locks[user_id] = lock
    return lock

async def try_reserve_rate_limit(user_id: int) -> tuple[bool, str, int]:
    """
    check_rate_limit + consume_rate_limit কে একটা lock-এর ভেতরে atomically করে,
    যাতে একই user-এর দ্রুত পরপর ২টা poll forward একসাথে race করে দুটোই pass করে
    না যায়। Slot পেলে সাথে সাথেই consume (reserve) করে ফেলে।
    """
    lock = _get_rate_limit_lock(user_id)
    async with lock:
        allowed, reason, remaining_secs = check_rate_limit(user_id)
        if allowed:
            consume_rate_limit(user_id)
        return allowed, reason, remaining_secs


def check_rate_limit(user_id: int) -> tuple[bool, str, int]:
    """
    User এখন poll solve করতে পারবে কিনা চেক করে (cooldown + daily limit)।
    কোনো state mutate করে না — শুধু read-only check। (Actual reserve
    consume_rate_limit() দিয়ে হয়, try_reserve_rate_limit() থেকে call হওয়ার পর।)
    """
    now   = time.time()
    today = get_dhaka_date()

    entry = rate_data.get(user_id)
    if entry is None:
        entry = {"date": today, "count": 0, "last_time": 0.0}
        rate_data[user_id] = entry

    if entry["date"] != today:
        entry["date"]  = today
        entry["count"] = 0

    elapsed = now - entry["last_time"]
    if elapsed < COOLDOWN_SECS:
        remaining_secs = int(COOLDOWN_SECS - elapsed) + 1
        return False, "cooldown", remaining_secs

    if entry["count"] >= get_effective_daily_limit(user_id):
        return False, "daily_limit", 0

    return True, "ok", 0

def refund_rate_limit(user_id: int):
    """
    AI call fail হলে reserve করা slot ফেরত দাও (attempt 'not counted' রাখার জন্য)।
    count কমিয়ে দেয়, কিন্তু last_time অপরিবর্তিত রাখে (cooldown বাইপাস ঠেকাতে)।
    """
    entry = rate_data.get(user_id)
    if entry and entry["count"] > 0:
        entry["count"] -= 1
        _turso_bg(lambda: _save_rate(user_id), "save_rate")


def consume_rate_limit(user_id: int):
    today = get_dhaka_date()
    entry = rate_data.get(user_id)
    if entry is None:
        entry = {"date": today, "count": 0, "last_time": 0.0}
        rate_data[user_id] = entry
    if entry["date"] != today:
        entry["date"]  = today
        entry["count"] = 0
    entry["count"]     += 1
    entry["last_time"]  = time.time()
    _turso_bg(lambda: _save_rate(user_id), "save_rate")

# ══════════════════════════════════════════════════════════════════
#  COUNTDOWN ANIMATION (2 min cooldown message)
# ══════════════════════════════════════════════════════════════════
async def send_cooldown_countdown(msg, remaining_secs: int):
    """
    User কে countdown দেখায়। প্রতি 15 সেকেন্ডে update।
    """
    left = remaining_secs

    def _make_bar(seconds_left: int) -> str:
        total = COOLDOWN_SECS
        pct   = max(0, seconds_left / total)
        filled = round(pct * 10)
        bar    = "🟥" * filled + "⬜" * (10 - filled)
        mins   = seconds_left // 60
        secs   = seconds_left % 60
        time_str = f"{mins}:{secs:02d}"
        return (
            f"⏳ *একটু অপেক্ষা করো!*\n\n"
            f"পরের poll solve করতে আরো:\n\n"
            f"```\n{bar}\n```\n"
            f"⏱ *{time_str}* বাকি\n\n"
            f"_প্রতিটি poll-এর মাঝে ২ মিনিট গ্যাপ রাখো।_"
        )

    try:
        status = await msg.reply_text(_make_bar(left), parse_mode=ParseMode.MARKDOWN)
    except Exception:
        return

    # প্রতি 15 সেকেন্ডে update (Telegram edit limit মাথায় রেখে)
    while left > 0:
        await asyncio.sleep(15)
        left = max(0, left - 15)
        try:
            if left == 0:
                await status.edit_text(
                    "✅ *এখন poll solve করতে পারো!*\n\nPoll forward করো 👇",
                    parse_mode=ParseMode.MARKDOWN
                )
            else:
                await status.edit_text(_make_bar(left), parse_mode=ParseMode.MARKDOWN)
        except Exception:
            break


# ══════════════════════════════════════════════════════════════════
#  LOADING ANIMATION FRAMES
# ══════════════════════════════════════════════════════════════════
STAGES = [
    "🔍 Searching",
    "🚀 Processing",
    "🧠 Thinking",
    "⏳ Preparing solution",
]

def _render_frame(dot_i: int, stage_i: int) -> str:
    label = STAGES[min(stage_i, len(STAGES) - 1)]
    dots = "." * ((dot_i % 3) + 1)  # three-state: . / .. / ...
    return f"*{label}{dots}*"

async def animate(msg, stop_event: asyncio.Event):
    dot_i = 0
    stage_i = 0
    ticks_per_stage = 6  # each stage stays visible briefly before moving on
    tick = 0
    last_text = None
    while not stop_event.is_set():
        text = _render_frame(dot_i, stage_i)
        if text != last_text:
            try:
                await msg.edit_text(text, parse_mode=ParseMode.MARKDOWN)
                last_text = text
            except Exception:
                pass
        await asyncio.sleep(0.5)
        dot_i += 1
        tick += 1
        if tick >= ticks_per_stage and stage_i < len(STAGES) - 1:
            stage_i += 1
            tick = 0

async def run_with_animation(status_msg, coro):
    stop   = asyncio.Event()
    anim   = asyncio.create_task(animate(status_msg, stop))
    try:
        result = await coro
    finally:
        stop.set()
        anim.cancel()
        try:
            await anim
        except asyncio.CancelledError:
            pass
    return result


# ══════════════════════════════════════════════════════════════════
#  GENERAL Q&A  (student সরাসরি text দিয়ে প্রশ্ন করলে)
#  Poll solving-এর মতোই প্রতিটি প্রশ্নের মাঝে ২ মিনিট gap রাখা হয়, আলাদা
#  cooldown dict দিয়ে — poll-এর দৈনিক limit-কে এটা প্রভাবিত করে না।
# ══════════════════════════════════════════════════════════════════
QA_COOLDOWN_SECS = COOLDOWN_SECS  # ২ মিনিট = poll-এর সাথে একই gap
TEXT_DAILY_LIMIT = 10             # প্রতিদিন সর্বোচ্চ কতটি text প্রশ্ন করা যাবে

qa_rate_data: dict = {}          # { user_id: last_time (float) }
_qa_rate_locks: dict = {}        # { user_id: asyncio.Lock }
retry_qa_data: dict = {}         # { retry_id: {"question": str, "chat_id": int, "user_id": int, "_created_at": float} }
# { user_id: {"date": "YYYY-MM-DD", "count": int} } — text প্রশ্নের দৈনিক হিসাব
qa_daily_data: dict = {}


def get_qa_daily_entry(user_id: int) -> dict:
    """আজকের তারিখে reset করা text-প্রশ্নের entry ফেরত দেয়।"""
    today = get_dhaka_date()
    entry = qa_daily_data.get(user_id)
    if entry is None:
        entry = {"date": today, "count": 0}
        qa_daily_data[user_id] = entry
    if entry.get("date") != today:
        entry["date"]  = today
        entry["count"] = 0
    return entry


def get_effective_text_daily_limit(user_id: int) -> int:
    """base TEXT_DAILY_LIMIT + admin-এর দেওয়া আজকের extra text bonus।"""
    return TEXT_DAILY_LIMIT + get_admin_extra_text_limit(user_id)


def check_qa_daily_limit(user_id: int) -> bool:
    """আজ আরো প্রশ্ন করা যাবে কিনা (state mutate করে না, শুধু date reset করে)।"""
    return get_qa_daily_entry(user_id)["count"] < get_effective_text_daily_limit(user_id)


def consume_qa_daily(user_id: int) -> dict:
    """সফল উত্তরের পর আজকের count বাড়ায় এবং entry ফেরত দেয়। সাথে lifetime
    qa_count (registered_users-এ, /userdata admin command-এর জন্য) বাড়ায়।"""
    entry = get_qa_daily_entry(user_id)
    entry["count"] += 1
    _turso_bg(lambda: _save_qa_rate(user_id), "save_qa_rate")

    if user_id in registered_users:
        registered_users[user_id]["qa_count"] = registered_users[user_id].get("qa_count", 0) + 1
        _turso_bg(lambda: _save_user(user_id), "save_user_qa_count")

    return entry


def qa_daily_footer(user_id: int) -> str:
    """উত্তরের নিচে দেখানোর জন্য দৈনিক ব্যবহারের progress bar (HTML)।"""
    limit = get_effective_text_daily_limit(user_id)
    used  = get_qa_daily_entry(user_id)["count"]
    left  = max(0, limit - used)
    fill  = round((min(used, limit) / limit) * 10) if limit else 0
    bar   = "🟩" * fill + "⬜" * (10 - fill)
    text  = f"\n\n📊 <b>আজকের প্রশ্ন:</b> {bar} {used}/{limit}"
    if 0 < left <= 3:
        text += f"\n⚠️ <b>আজ আর মাত্র {left}টি প্রশ্ন বাকি!</b>"
    elif left == 0:
        text += "\n🚫 <b>আজকের limit শেষ! কাল আবার এসো।</b>"
    return text


async def send_qa_limit_message(msg, user_id: int = None):
    """দৈনিক text-প্রশ্নের limit শেষ হলে user-কে জানায়।"""
    limit = get_effective_text_daily_limit(user_id) if user_id is not None else TEXT_DAILY_LIMIT
    try:
        await msg.reply_text(
            f"🚫 <b>আজকের প্রশ্নের limit শেষ!</b>\n"
            f"▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬\n"
            f"📊 আজ তুমি <b>{limit}/{limit}</b> টি প্রশ্ন করে ফেলেছ।\n"
            f"🕛 রাত ১২টার পর (Dhaka time) আবার নতুন করে <b>{limit}</b> টি প্রশ্ন করতে পারবে।\n"
            f"▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬",
            parse_mode="HTML"
        )
    except Exception:
        pass


def _get_qa_rate_lock(user_id: int) -> "asyncio.Lock":
    lock = _qa_rate_locks.get(user_id)
    if lock is None:
        lock = asyncio.Lock()
        _qa_rate_locks[user_id] = lock
    return lock

def check_qa_cooldown(user_id: int) -> int:
    """0 রিটার্ন করলে allowed, নইলে remaining seconds রিটার্ন করে।"""
    last_time = qa_rate_data.get(user_id, 0.0)
    elapsed = time.time() - last_time
    if elapsed < QA_COOLDOWN_SECS:
        return int(QA_COOLDOWN_SECS - elapsed) + 1
    return 0

async def try_reserve_qa_cooldown(user_id: int) -> int:
    """Atomically check + reserve (race-condition safe, poll rate-limit-এর প্যাটার্ন অনুসরণ করে)।"""
    lock = _get_qa_rate_lock(user_id)
    async with lock:
        remaining = check_qa_cooldown(user_id)
        if remaining == 0:
            qa_rate_data[user_id] = time.time()
        return remaining

async def send_qa_cooldown_countdown(msg, remaining_secs: int):
    """Poll cooldown countdown-এর মতোই progress bar, কিন্তু প্রশ্নোত্তরের জন্য।"""
    left = remaining_secs

    def _make_bar(seconds_left: int) -> str:
        total = QA_COOLDOWN_SECS
        pct   = max(0, seconds_left / total)
        filled = round(pct * 10)
        bar    = "🟥" * filled + "⬜" * (10 - filled)
        mins   = seconds_left // 60
        secs   = seconds_left % 60
        time_str = f"{mins}:{secs:02d}"
        return (
            f"⏳ *একটু অপেক্ষা করো!*\n\n"
            f"পরের প্রশ্ন করতে আরো:\n\n"
            f"```\n{bar}\n```\n"
            f"⏱ *{time_str}* বাকি\n\n"
            f"_প্রতিটি প্রশ্নের মাঝে ২ মিনিট গ্যাপ রাখো।_"
        )

    try:
        status = await msg.reply_text(_make_bar(left), parse_mode=ParseMode.MARKDOWN)
    except Exception:
        return

    while left > 0:
        await asyncio.sleep(15)
        left = max(0, left - 15)
        try:
            if left == 0:
                await status.edit_text(
                    "✅ *এখন আবার প্রশ্ন করতে পারো!*", parse_mode=ParseMode.MARKDOWN
                )
            else:
                await status.edit_text(_make_bar(left), parse_mode=ParseMode.MARKDOWN)
        except Exception:
            break


# ══════════════════════════════════════════════════════════════════
#  OFF-TOPIC / SMALL-TALK FILTER — পড়ালেখা-সম্পর্কিত না এমন greeting,
#  বট সম্পর্কে মেটা-প্রশ্ন (তোমার নাম কি, তোমাকে কে বানিয়েছে), বা নিছক
#  খোঁচা/গালি টাইপ মেসেজ পেলে Gemini API একদমই call না করেই সরাসরি
#  canned reply পাঠানো হয় — এতে অহেতুক API quota, daily limit, বা
#  cooldown কোনোটাই খরচ হয় না। এটা সম্পূর্ণ local (regex-based) check,
#  কোনো network call নেই।
#
#  সতর্কতা: এটা heuristic — খুব ছোট, প্রশ্ন-সূচক শব্দ ছাড়া মেসেজকে
#  casual ধরে নেয়। নিচের প্যাটার্ন/word-list-এ false positive দেখা গেলে
#  এখানে যোগ/বাদ দিয়ে টিউন করা যাবে।
# ══════════════════════════════════════════════════════════════════
_GREETING_PATTERNS = [
    r"^h+i+$", r"^h+e+y+$", r"^h+e+l+l*o+$",
    r"^হ্যা?লো$", r"^হাই$", r"^ওহে$",
    r"^assalamu ?alaikum$", r"^সালাম$", r"^আসসালামু ?আলাইকুম$",
    r"^(তুমি )?কেমন আছ", r"^(tumi )?kemon ach", r"^ki obostha", r"^কি অবস্থা",
    r"^valo asen", r"^ভালো আছ",
]

_META_ABOUT_BOT_PATTERNS = [
    r"তোমার নাম", r"tomar naa?m", r"\byour name\b",
    r"তোমাকে কে বানিয়েছে", r"tomake ke baniy[ae]c?[eh]e?", r"who (made|created|built) you",
    r"^তুমি কে[?।]?$", r"^tumi ke\b", r"^who are you\??$",
    r"তুমি কি ?কি পার", r"tumi ki ?ki paro", r"what can you do",
    r"তুই কেন(ো)? এখানে", r"tui keno ekhane", r"tumi keno ekhane",
    r"তোর কপালে", r"tor kopale",
    r"তুই খুব বাজে", r"tui khub baje", r"তুমি (খুব )?বাজে", r"tumi (khub )?baje",
]

_QUESTION_MARKERS = [
    "?", "কি ", "কী ", "কেন", "কীভাবে", "কিভাবে", "ব্যাখ্যা", "সমাধান",
    "অর্থ", "মানে", "কত ", "সংজ্ঞা", "কাকে বলে", "সূত্র", "কোনটি", "কোনটা",
    "সমীকরণ", "নিয়ম", "পার্থক্য", "উদাহরণ",
    "what", "why", "how", "explain", "solve", "define", "calculate", "meaning",
    "difference", "formula", "example",
]

# ── লিংক/URL — student ভুল করে কোনো লিংক (drive/YouTube/website/t.me ইত্যাদি)
#    পাঠালে সেটাকে কোনোভাবেই "প্রশ্ন" ধরে AI-কে দেওয়া হবে না (AI তখন লিংকের
#    কনটেন্ট আসলে দেখতেই পায় না, অথচ হ্যালুসিনেট করে ভুলভাল উত্তর বানিয়ে দেয়)।
_LINK_PATTERN = re.compile(
    r"https?://\S+|(?<!\w)(?:www\.)?\S+\.(?:com|net|org|io|me|co|in|xyz|info|"
    r"app|dev|bd|link|gg|ly)(?:/\S*)?|t\.me/\S+|drive\.google\.com/\S+",
    re.IGNORECASE
)

# ── গালি/অপমানসূচক শব্দ — এমন মেসেজ পেলে AI-কে না দিয়ে সরাসরি canned reply।
#    এটা একটা সাধারণ moderation list (সবচেয়ে প্রচলিত কিছু বাংলা/ইংরেজি গালি/
#    অপশব্দ, transliteration সহ) — সম্পূর্ণ local check, কোনো network call নেই।
_PROFANITY_WORDS = [
    "chuda", "chudi", "choda", "chodon", "magi", "magir", "khanki", "khankir",
    "kuttar baccha", "harami", "haramzada",
    "gu kha", "boka chuda",
    "মাগি", "খানকি", "খানকির", "চুদা", "চুদি", "চুদন", "হারামি", "হারামজাদা",
    "কুত্তার বাচ্চা", "শালার পো", "গু খা", "বোকাচোদা",
    "fuck", "f*ck", "fck", "bitch", "asshole", "motherfucker", "bastard",
]
_PROFANITY_PATTERN = re.compile(
    "(" + "|".join(re.escape(w) for w in _PROFANITY_WORDS) + ")",
    re.IGNORECASE
)


def _contains_link(text: str) -> bool:
    return bool(_LINK_PATTERN.search(text))


def _contains_profanity(text: str) -> bool:
    return bool(_PROFANITY_PATTERN.search(text))


def _looks_like_junk(text: str) -> bool:
    """
    ভুলবশত পাঠানো random keystroke/junk মেসেজ (যেমন "asdkjaksjd", "......",
    "১২৩৪৫৬৭৮৯", emoji-spam) ধরার জন্য একটা হালকা heuristic — real Bangla/
    English শব্দ/অক্ষর খুব কম থাকলে (বাকিটা সংখ্যা/symbol/repeated char)
    junk হিসেবে ধরা হয়।
    """
    letters = re.findall(r"[A-Za-zঀ-৿]", text)
    if len(text) >= 6 and len(letters) < max(3, len(text) * 0.25):
        return True
    # একই character বারবার (৫+ বার পরপর) — যেমন "aaaaaa", "......", "😂😂😂😂😂😂"
    if re.search(r"(.)\1{4,}", text):
        return True
    return False


# link/gali/junk/off-topic মেসেজ পেলে এই দুটো canned reply থেকে randomly
# একটা বেছে পাঠানো হয় (দুটো একসাথে না, প্রতিবার একটাই) — যাতে বারবার একই
# ফরম্যাটের মেসেজ না লাগে।
_ASK_QUESTION_CANNED_REPLIES = [
    "দয়া করে আপনার প্রশ্ন পাঠান।",
    "Please send your question.",
]


def is_academic_question(text: str) -> bool:
    """
    True ফেরত দেয় যদি এটা সত্যিকারের পড়ালেখা/জ্ঞানমূলক প্রশ্ন মনে হয় — তখনই শুধু
    Gemini call হবে। False ফেরত দেয় যদি এটা নিছক greeting, বট সম্পর্কে
    meta-question, গালি/অপমানসূচক মেসেজ, লিংক/URL, বা random junk হয় —
    তখন কোনো API call ছাড়াই canned "Please send your question." reply যাবে।
    """
    q = text.strip().lower()
    if not q:
        return False

    # লিংক/URL — কোনোভাবেই AI-কে "প্রশ্ন" হিসেবে দেওয়া হবে না (drive/YouTube/
    # website/t.me লিংক পাঠালে AI হ্যালুসিনেট করে ভুল উত্তর বানিয়ে দেয়)
    if _contains_link(text):
        return False

    # গালি/অপমানসূচক মেসেজ
    if _contains_profanity(text):
        return False

    for pat in _GREETING_PATTERNS:
        if re.search(pat, q):
            return False

    for pat in _META_ABOUT_BOT_PATTERNS:
        if re.search(pat, q):
            return False

    # খুব ছোট (≤৩ শব্দ) মেসেজে কোনো প্রশ্ন-সূচক শব্দ/চিহ্ন না থাকলে casual/small-talk
    # হিসেবে ধরা হয় (যেমন "Hi", "কেমন আছো", "তুই বাজে" — কিন্তু "পানির সংকেত কি" না,
    # কারণ ওখানে "কি" marker আছে)।
    word_count = len(q.split())
    if word_count <= 3 and not any(m in q for m in _QUESTION_MARKERS):
        return False

    # random keystroke/junk মেসেজ (ভুল করে চাপ পড়ে যাওয়া, emoji-spam ইত্যাদি)
    if _looks_like_junk(text):
        return False

    return True


# ══════════════════════════════════════════════════════════════════
#  MCQ-STYLE TEXT DETECTION — student যখন poll/exam question copy-paste করে
#  text হিসেবে পাঠায় (মূল প্রশ্নের নিচে একগুচ্ছ ছোট option লাইন, যেমন poll
#  forward করলে যেমন আসে), তখন সাধারণ বিস্তারিত রচনার মতো prompt না দিয়ে
#  Gemini/ChatGPT app-এর মতো সংক্ষিপ্ত "সঠিক উত্তর + ছোট ব্যাখ্যা" prompt
#  ব্যবহার করা হয় — অহেতুক লম্বা heading/bullet/table এড়াতে।
# ══════════════════════════════════════════════════════════════════
_EXAM_TAG_RE = re.compile(r"^\(.*\)$")


def looks_like_mcq_text(question: str) -> bool:
    """
    True যদি প্রশ্নের সাথে poll-এর মতো একগুচ্ছ ছোট option লাইন থাকে
    (প্রতি লাইনে একটা করে সম্ভাব্য উত্তর, exam collection থেকে copy-paste
    করলে প্রায়ই এভাবে আসে)। এমন হলে concise MCQ prompt ব্যবহার হবে।
    """
    lines = [l.strip() for l in question.split("\n") if l.strip()]
    if len(lines) < 3:
        return False
    option_like = [
        l for l in lines
        if len(l) <= 30 and not _EXAM_TAG_RE.match(l) and not l.endswith(("?", "।"))
    ]
    return len(option_like) >= 3


def build_qa_mcq_prompt(question: str) -> str:
    """
    MCQ-এর মতো option-লাইন থাকা প্রশ্নের জন্য সংক্ষিপ্ত prompt — Gemini app/
    ChatGPT-এ যেভাবে সংক্ষেপে উত্তর দেয় (সঠিক উত্তর + ছোট এক-প্যারা ব্যাখ্যা),
    ঠিক সেভাবেই। কোনো heading/bullet/table/"অন্য অপশন কেন ভুল" section নেই —
    এসবই লম্বা রচনার মতো লাগার মূল কারণ ছিল।
    """
    return (
        "তুমি একজন অভিজ্ঞ বাংলাদেশি শিক্ষক। শিক্ষার্থী নিচে একটা MCQ (বহুনির্বাচনী) "
        "প্রশ্ন পাঠিয়েছে — প্রথম লাইন(গুলো)-এ প্রশ্ন, তারপর প্রতিটা আলাদা লাইনে একেকটা "
        "সম্ভাব্য অপশন।\n\n"
        f"{question}\n\n"
        "উত্তর দেওয়ার নিয়ম (কঠোরভাবে মানবে — Gemini app বা ChatGPT-এ যেভাবে ছোট করে "
        "দেয় ঠিক সেভাবে, কোনো অতিরিক্ত section/heading/bullet/numbered-list/table "
        "বসাবে না):\n\n"
        "১. প্রথম লাইনেই শুধু এই ফরম্যাটে: **সঠিক উত্তর: [অপশনের নাম]**\n"
        "২. এরপর একটা ফাঁকা লাইন দিয়ে \"ব্যাখ্যা:\" লিখে মাত্র ৩-৫ বাক্যের একটামাত্র "
        "সংক্ষিপ্ত প্যারাগ্রাফে কারণ বুঝিয়ে দাও — আলাদা section, sub-heading, bullet "
        "point, table, বা \"অন্যান্য অপশন কেন ভুল\" অংশ বসাবে না।\n"
        "৩. পুরো উত্তর সর্বোচ্চ ~৬০০ অক্ষরের মধ্যে রাখবে — সংক্ষিপ্ত, টু-দ্য-পয়েন্ট, "
        "exam-এর মতো।\n\n"
        "গাণিতিক রাশি/সূত্র থাকলে সঠিক LaTeX ব্যবহার করবে — inline-এর জন্য $...$ এবং "
        "আলাদা লাইনে বড় সমীকরণের জন্য $$...$$ (\\frac{}{}, \\sqrt{}, ^{}, _{}, "
        "\\Delta, \\alpha, \\theta ইত্যাদি সব standard LaTeX command ব্যবহার করা যাবে — "
        "এই চ্যাট এখন rich message-এর মাধ্যমে LaTeX সরাসরি render করে)। নিজের নাম/মডেল/"
        "কোম্পানি নিয়ে কিছু বলবে না, শুরুতে কোনো branding বসাবে না — সরাসরি উত্তর দিয়েই "
        "শুরু করবে।"
    )


def build_qa_prompt(question: str) -> str:
    """
    Student যে প্রশ্নই করুক (পড়ালেখা, সাধারণ জ্ঞান, ব্যাখ্যা, কোড, পরামর্শ — যা-ই হোক),

    Gemini নিজে যেভাবে উত্তর দিত ঠিক সেই মান ও কনটেন্টের উত্তরই দিতে বলা হচ্ছে —
    শুধু একটাই মেসেজে, গুছিয়ে, সহজ করে, আর যেখানে দরকার সেখানে সুন্দর table দিয়ে।
    """
    return (
        "তুমি একজন অত্যন্ত জ্ঞানী, বন্ধুত্বপূর্ণ বাংলাদেশি শিক্ষক ও সহকারী।\n"
        "শিক্ষার্থী যে প্রশ্নই করুক না কেন — পড়ালেখা, বিজ্ঞান, গণিত, সাধারণ জ্ঞান, "
        "ইতিহাস, প্রযুক্তি, পরামর্শ, বা সাধারণ আলাপ — কোনো প্রশ্ন এড়িয়ে যাবে না, "
        "'আমি পারি না' বলবে না। যেকোনো প্রশ্নের সম্পূর্ণ, নির্ভুল ও সহায়ক উত্তর দেবে "
        "ঠিক যেভাবে একটি শীর্ষমানের AI assistant দিত।\n\n"
        f"শিক্ষার্থীর প্রশ্ন: {question}\n\n"
        "উত্তর দেওয়ার নিয়ম (কঠোরভাবে মানবে):\n\n"
        "১. একটাই মেসেজ: পুরো উত্তরটা একটামাত্র মেসেজে শেষ করবে। তাই গুছিয়ে, "
        "টু-দ্য-পয়েন্ট লিখবে — সর্বোচ্চ ~২৫০০ অক্ষরের মধ্যে রাখার চেষ্টা করবে, কিন্তু "
        "কোনো অবস্থাতেই মাঝপথে/মাঝ-বাক্যে থেমে যাবে না। প্রশ্ন ছোট হলে উত্তরও ছোট "
        "রাখবে; অপ্রয়োজনীয় ভূমিকা, filler বা একই কথা বারবার লিখবে না।\n\n"
        "২. সহজ ভাষা: সহজ, স্পষ্ট বাংলায় এমনভাবে বোঝাবে যেন একজন ছাত্র প্রথমবারেই "
        "বুঝে ফেলে (প্রয়োজনে ইংরেজি টার্ম রাখতে পারো)। কঠিন বিষয় হলে ছোট উদাহরণ বা "
        "বাস্তব তুলনা দিয়ে বোঝাবে।\n\n"
        "৩. STRUCTURE (rich formatting — সুন্দর দেখাতে হবে):\n"
        "   - মূল টপিকের title-এর জন্য `## ` (H2), সাব-সেকশনের জন্য `### ` (H3) "
        "ব্যবহার করবে। section title শুধু **bold** করে রাখবে না।\n"
        "   - গুরুত্বপূর্ণ শব্দ/টার্ম/চূড়ান্ত উত্তর **bold** করবে।\n"
        "   - ধাপে ধাপে ব্যাখ্যায় numbered list (1. 2. 3.), আলাদা পয়েন্টে bullet (- )।\n"
        "   - উত্তর যদি খুব ছোট বা একটামাত্র বাক্যের হয়, তাহলে heading ছাড়াই সরাসরি "
        "উত্তর দেবে — অকারণে বড় কাঠামো বানাবে না।\n\n"
        "৪. TABLE (যেখানেই মানানসই, সেখানেই দেবে — এতে দেখতে সুন্দর ও বুঝতে সহজ হয়):\n"
        "   - তুলনা, সূত্রের তালিকা, সংজ্ঞা, শ্রেণিবিভাগ, ধাপ/বৈশিষ্ট্যের তালিকা — "
        "এগুলো bullet-এর বদলে GFM markdown table-এ দেখাবে। দরকার হলে একাধিক table দেবে।\n"
        "   - Table syntax (header row + separator row বাধ্যতামূলক):\n"
        "     | কলাম ১ | কলাম ২ |\n"
        "     |---|---|\n"
        "     | মান ১ | মান ২ |\n"
        "   - Table-এর ঘর ছোট রাখবে (এক-দুই শব্দ/ছোট সূত্র), যাতে ফোনে সুন্দর দেখায়।\n\n"
        "৫. MATH/PHYSICS/CHEMISTRY লেখার নিয়ম (অত্যন্ত গুরুত্বপূর্ণ):\n"
        "   - এই চ্যাট এখন Telegram-এর rich message ফিচার ব্যবহার করে, যেখানে LaTeX "
        "সরাসরি সুন্দরভাবে render হয়। তাই সব সূত্র/সমীকরণ/রাসায়নিক বিক্রিয়ায় সঠিক "
        "LaTeX লিখবে — ছোট inline রাশির জন্য $...$ (যেমন $x^2 + y^2$), আর আলাদা লাইনে "
        "দেখানোর মতো বড় সমীকরণের জন্য $$...$$ (যেমন $$F = \\frac{Gm_1m_2}{r^2}$$)।\n"
        "   - ভগ্নাংশে \\frac{লব}{হর}, বর্গমূলে \\sqrt{...}, সূচক/subscript-এ ^{} ও _{}, "
        "গ্রিক অক্ষরে \\Delta \\alpha \\beta \\theta \\pi \\eta \\mu \\lambda \\omega \\Omega "
        "\\Sigma \\infty, রাসায়নিক বিক্রিয়ায় \\rightarrow ও \\rightleftharpoons, একক "
        "\\text{...} দিয়ে লিখবে (যেমন $500\\ \\text{J}$)।\n"
        "   - প্রতিটা সূত্র ও সমাধানের প্রতিটা ধাপ আলাদা লাইনে লিখবে, শেষে চূড়ান্ত উত্তর "
        "**bold** করে দেখাবে।\n"
        "   - ⚠️ লম্বা সমীকরণ (একাধিক bracket-group পাশাপাশি গুণ/ভাগ, যেমন "
        "$$2\\sin\\left(\\frac{C+D}{2}\\right)\\cos\\left(\\frac{C-D}{2}\\right)$$, বা "
        "রাসায়নিক বিক্রিয়ার মতো লম্বা reaction) কখনো দুই টুকরায় ভেঙে দুই লাইনে বসাবে "
        "না — বরং ঠিক রাসায়নিক বিক্রিয়া লেখার মতোই, পুরো সমীকরণটা একটামাত্র $$...$$ "
        "ব্লকে অক্ষত রাখবে (যেমন $$CH_3CH_2OH \\xrightarrow{Conc.\\ H_2SO_4/\\Delta} "
        "CH_2=CH_2 + H_2O$$)। rich message এই ধরনের লম্বা $$...$$ ব্লককে নিজে থেকেই "
        "সুন্দর ডানে-বামে scroll করা যায় এমনভাবে দেখায় — তাই ভেঙে দিলে বরং এই scroll "
        "সুবিধা নষ্ট হয়ে সূত্র অসম্পূর্ণ দেখাবে। শুধু একগুচ্ছ আলাদা identity/সূত্রের "
        "তালিকা (formula-sheet ধরনের) হলে সেগুলোকে bullet-এর বদলে GFM markdown table-এ "
        "বসাবে (এক কলামে নাম/শর্ত, অন্য কলামে সূত্র) — কিন্তু কোনো একটামাত্র লম্বা "
        "সমীকরণকে কখনো টুকরো করবে না।\n\n"
        "৬. তুমি কোন AI বা কোন কোম্পানির তৈরি — সে প্রসঙ্গ কখনো তুলবে না, নিজের নাম/মডেল "
        "নিয়ে কিছু বলবে না, শুধু বিষয়বস্তুতে ফোকাস করবে। উত্তরের শুরুতে নিজে থেকে কোনো "
        "branding বসাবে না — সরাসরি উত্তর দিয়েই শুরু করবে।"
    )


async def answer_question(question: str) -> str:
    """সাধারণ প্রশ্নের জন্য AI call করে raw Markdown ফেরত দেয় (rich message-এ পাঠানোর জন্য)।
    MCQ-এর মতো option-লাইন থাকলে সংক্ষিপ্ত prompt (Gemini/ChatGPT app স্টাইল),
    নাহলে সাধারণ বিস্তারিত prompt।"""
    prompt = build_qa_mcq_prompt(question) if looks_like_mcq_text(question) else build_qa_prompt(question)
    result = await call_ai(prompt, task="text")
    return (result or "").strip()


# ══════════════════════════════════════════════════════════════════
#  IMAGE (OCR) Q&A — student ছবি পাঠিয়ে সেই ছবির reply-তে প্রশ্ন করলে
#  Gemini Vision দিয়ে ছবি পড়ে rich-formatted উত্তর দেওয়া হয়।
#    • দৈনিক limit: ১০টি (text প্রশ্নের limit থেকে আলাদা)
#    • প্রতি প্রশ্নের মাঝে ২ মিনিট gap (poll/text-এর মতোই — একই cooldown)
#    • প্রতিটি প্রশ্ন ছবি সহ report group-এ চলে যায়
# ══════════════════════════════════════════════════════════════════
import base64 as _b64

OCR_DAILY_LIMIT = 10
ocr_daily_data: dict = {}   # { user_id: {"date": "YYYY-MM-DD", "count": int} }


def get_ocr_daily_entry(user_id: int) -> dict:
    today = get_dhaka_date()
    entry = ocr_daily_data.get(user_id)
    if entry is None:
        entry = {"date": today, "count": 0}
        ocr_daily_data[user_id] = entry
    if entry.get("date") != today:
        entry["date"]  = today
        entry["count"] = 0
    return entry


def get_effective_ocr_daily_limit(user_id: int) -> int:
    """base OCR_DAILY_LIMIT + admin-এর দেওয়া আজকের extra OCR bonus।"""
    return OCR_DAILY_LIMIT + get_admin_extra_ocr_limit(user_id)


def check_ocr_daily_limit(user_id: int) -> bool:
    return get_ocr_daily_entry(user_id)["count"] < get_effective_ocr_daily_limit(user_id)


def consume_ocr_daily(user_id: int) -> dict:
    """সফল উত্তরের পর আজকের count বাড়ায়, সাথে lifetime ocr_count
    (registered_users-এ, /userdata admin command-এর জন্য) বাড়ায়।"""
    entry = get_ocr_daily_entry(user_id)
    entry["count"] += 1
    _turso_bg(lambda: _save_ocr_rate(user_id), "save_ocr_rate")

    if user_id in registered_users:
        registered_users[user_id]["ocr_count"] = registered_users[user_id].get("ocr_count", 0) + 1
        _turso_bg(lambda: _save_user(user_id), "save_user_ocr_count")

    return entry


def ocr_daily_footer(user_id: int) -> str:
    limit = get_effective_ocr_daily_limit(user_id)
    used = get_ocr_daily_entry(user_id)["count"]
    left = max(0, limit - used)
    fill = round((min(used, limit) / limit) * 10) if limit else 0
    bar  = "🟩" * fill + "⬜" * (10 - fill)
    text = f"\n\n🖼 <b>আজকের ছবি-প্রশ্ন:</b> {bar} {used}/{limit}"
    if 0 < left <= 3:
        text += f"\n⚠️ <b>আজ আর মাত্র {left}টি ছবি-প্রশ্ন বাকি!</b>"
    elif left == 0:
        text += "\n🚫 <b>আজকের ছবি-প্রশ্নের limit শেষ! কাল আবার এসো।</b>"
    return text


async def send_ocr_limit_message(msg, user_id: int = None):
    limit = get_effective_ocr_daily_limit(user_id) if user_id is not None else OCR_DAILY_LIMIT
    try:
        await msg.reply_text(
            f"🚫 <b>আজকের ছবি-প্রশ্নের limit শেষ!</b>\n"
            f"▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬\n"
            f"🖼 আজ তুমি <b>{limit}/{limit}</b> টি ছবি থেকে উত্তর নিয়েছ।\n"
            f"🕛 রাত ১২টার পর (Dhaka time) আবার নতুন করে <b>{limit}</b> টি ছবি-প্রশ্ন করতে পারবে।\n"
            f"▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬",
            parse_mode="HTML"
        )
    except Exception:
        pass


async def send_photo_hint(msg, user_id: int = None):
    """ছবি পেলে student-কে বুঝিয়ে দেয় যে প্রশ্নটা ঐ ছবির reply-তে লিখতে হবে।"""
    used_line = ""
    if user_id is not None and user_id != ADMIN_ID:
        ocr_cap = get_effective_ocr_daily_limit(user_id)
        used = get_ocr_daily_entry(user_id)["count"]
        used_line = f"\n🖼 <b>আজকের ছবি-প্রশ্ন বাকি:</b> {max(0, ocr_cap - used)}/{ocr_cap}"
    try:
        await msg.reply_text(
            "🖼 <b>ছবি পেয়েছি!</b>\n"
            "▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬\n\n"
            "এখন <b>এই ছবিটার reply</b>-তে তোমার প্রশ্নটা লিখে পাঠাও 👇\n\n"
            "📌 <b>কীভাবে করবে:</b>\n"
            "১️⃣ ছবিটার উপর চাপ ধরে রাখো (long press)\n"
            "২️⃣ <b>Reply</b> চাপো\n"
            "৩️⃣ প্রশ্ন লিখে Send করো\n\n"
            "💡 <b>উদাহরণ:</b> <code>64 no. Ans dao</code>\n"
            "💡 <code>45 নম্বর প্রশ্নের ব্যাখ্যা দাও</code>"
            f"{used_line}\n\n"
            "⚠️ ছবির reply ছাড়া শুধু প্রশ্ন লিখলে bot ছবিটা দেখতে পাবে না।",
            parse_mode="HTML"
        )
    except Exception as e:
        logger.error(f"send_photo_hint error: {e}")


def build_image_qa_prompt(question: str) -> str:
    """ছবি (প্রশ্নপত্র/বইয়ের পাতা/MCQ) + student-এর প্রশ্ন — দুটো মিলিয়ে prompt।"""
    return (
        "তুমি একজন অত্যন্ত জ্ঞানী, বন্ধুত্বপূর্ণ বাংলাদেশি শিক্ষক ও সহকারী।\n"
        "সাথের ছবিটি একজন শিক্ষার্থীর পাঠানো (বইয়ের পাতা, প্রশ্নপত্র, MCQ, নোট, "
        "অঙ্ক, ডায়াগ্রাম — যা-ই হোক)। ছবিটি খুব মনোযোগ দিয়ে পড়ো (বাংলা ও ইংরেজি "
        "দুই লেখাই), তারপর শিক্ষার্থীর প্রশ্নের উত্তর দাও।\n\n"
        f"শিক্ষার্থীর প্রশ্ন: {question}\n\n"
        "গুরুত্বপূর্ণ নির্দেশনা:\n"
        "• শিক্ষার্থী যদি নির্দিষ্ট প্রশ্ন নম্বর বলে (যেমন: '64 no. Ans dao'), তাহলে ছবিতে "
        "ঠিক ওই নম্বরের প্রশ্নটি খুঁজে বের করো, প্রশ্ন ও অপশনগুলো হুবহু পড়ো, তারপর উত্তর দাও।\n"
        "• একাধিক নম্বর চাইলে (যেমন: '45, 46') প্রতিটির উত্তর আলাদা করে দাও।\n"
        "• কোনো নম্বর না বললে ছবির মূল বিষয়/প্রশ্ন ধরে নিয়ে উত্তর দাও।\n"
        "• ছবিতে ঐ নম্বরের প্রশ্ন না থাকলে ভান করবে না — কোন কোন নম্বর ছবিতে আছে সেটা "
        "সংক্ষেপে জানিয়ে দাও।\n\n"
        "উত্তরের ফরম্যাট (কঠোরভাবে মানবে):\n"
        "১. প্রথম লাইনেই: **সঠিক উত্তর: (অপশন) উত্তরের নাম** — bold করে।\n"
        "২. তারপর **ব্যাখ্যা (ধাপে ধাপে)** heading দিয়ে bullet list:\n"
        "   • **প্রশ্ন:** (ছবির প্রশ্নটা নিজের ভাষায় ছোট করে)\n"
        "   • **মূল তথ্য:** (কোন concept/তথ্য থেকে উত্তরটা আসছে)\n"
        "   • **ক্রিয়াকৌশল/কারণ:** (কীভাবে কাজ করে বা কেন এটাই ঠিক)\n"
        "   • **সিদ্ধান্ত:** (তাই সঠিক উত্তর এটাই)\n"
        "৩. MCQ হলে শেষে **অন্য অপশনগুলো কেন ভুল?** heading দিয়ে একটা GFM markdown table:\n"
        "   | অপশন | বিষয় | কারণ (কেন ভুল) |\n"
        "   |---|---|---|\n"
        "   টেবিলের প্রতিটা row অবশ্যই আলাদা নতুন লাইনে লিখবে (কখনোই এক লাইনে সব row "
        "জোড়া লাগাবে না), heading আর table-এর মাঝে একটা ফাঁকা লাইন রাখবে, এবং "
        "প্রতিটা row `|` দিয়ে শুরু ও শেষ করবে।\n"
        "   ঘরগুলো ছোট রাখবে যেন মোবাইলে সুন্দর দেখায়।\n"
        "৪. সবশেষে এক লাইনে: **সুতরাং সঠিক উত্তর — ...**\n\n"
        "লেখার নিয়ম:\n"
        "• সহজ, স্পষ্ট বাংলায় লিখবে (দরকারে ইংরেজি টার্ম রাখবে)। পুরো উত্তর একটামাত্র "
        "মেসেজে শেষ করবে — সর্বোচ্চ ~২৫০০ অক্ষর, কিন্তু মাঝপথে থামবে না।\n"
        "• এই চ্যাট rich message ব্যবহার করে, তাই LaTeX সরাসরি render হয় — সব সূত্র/"
        "সমীকরণে সঠিক LaTeX লিখবে: inline রাশির জন্য $...$, বড় সমীকরণের জন্য $$...$$, "
        "ভগ্নাংশে \\frac{}{}, বর্গমূলে \\sqrt{}, সূচক/subscript-এ ^{} ও _{}, গ্রিক অক্ষরে "
        "\\Delta \\alpha \\beta \\theta \\pi ইত্যাদি।\n"
        "• ⚠️ লম্বা সমীকরণ (একাধিক bracket-group পাশাপাশি গুণ/ভাগ, যেমন "
        "$$2\\sin\\left(\\frac{C+D}{2}\\right)\\cos\\left(\\frac{C-D}{2}\\right)$$, বা "
        "রাসায়নিক বিক্রিয়ার মতো লম্বা reaction) কখনো ভেঙে দুই লাইনে বসাবে না — ঠিক "
        "রাসায়নিক বিক্রিয়া লেখার মতোই পুরো সমীকরণটা একটামাত্র $$...$$ ব্লকে অক্ষত "
        "রাখবে (যেমন $$CH_3CH_2OH \\xrightarrow{Conc.\\ H_2SO_4/\\Delta} CH_2=CH_2 + "
        "H_2O$$)। rich message লম্বা $$...$$ ব্লককে নিজে থেকেই ডানে-বামে scroll করা "
        "যায় এমনভাবে দেখায় — ভেঙে দিলে সেই scroll সুবিধা নষ্ট হয়ে সূত্র অসম্পূর্ণ "
        "দেখাবে। শুধু আলাদা identity/সূত্রের তালিকা (formula-sheet) থাকলে সেগুলো GFM "
        "markdown table-এ (এক কলামে নাম/শর্ত, অন্য কলামে সূত্র) বসাবে — কিন্তু কোনো "
        "একটামাত্র লম্বা সমীকরণকে কখনো টুকরো করবে না।\n"
        "• নিজের নাম/মডেল/কোম্পানি নিয়ে কিছু বলবে না, শুরুতে কোনো branding বসাবে না — "
        "সরাসরি উত্তর দিয়েই শুরু করবে।"
    )


async def fetch_photo_b64(ctx, file_id: str):
    """Telegram থেকে ছবি নামিয়ে (base64, mime) ফেরত দেয়। ব্যর্থ হলে (None, None)।"""
    try:
        tg_file = await ctx.bot.get_file(file_id)
        data    = await tg_file.download_as_bytearray()
        if not data:
            return None, None
        return _b64.b64encode(bytes(data)).decode("ascii"), "image/jpeg"
    except Exception as e:
        logger.error(f"fetch_photo_b64 error: {e}")
        return None, None


async def answer_image_question(question: str, image_b64: str,
                                image_mime: str = "image/jpeg") -> str:
    """ছবি + প্রশ্ন দিয়ে AI call করে raw Markdown উত্তর ফেরত দেয়।"""
    prompt = build_image_qa_prompt(question)
    result = await call_ai(prompt, task="image",
                           image_b64=image_b64, image_mime=image_mime)
    return (result or "").strip()


async def send_image_qa_report(ctx, user, question: str, file_id: str):
    """ছবি-প্রশ্নের full detail (user info + ছবি + প্রশ্ন) report group-এ পাঠায়।"""
    if not REPORT_GROUP_ID or not user:
        return

    import html as _html
    uname = f"@{user.username}" if getattr(user, "username", None) else "username নেই"
    name  = getattr(user, "full_name", None) or "Unknown"
    safe_name  = _html.escape(str(name))
    safe_uname = _html.escape(str(uname))
    safe_q     = _html.escape((question or "").strip()[:800])

    caption = (
        f"🖼 <b>নতুন Image (OCR) Question এসেছে!</b>\n"
        f"▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬\n\n"
        f"👤 <b>Name:</b> {safe_name}\n"
        f"🔖 <b>Username:</b> {safe_uname}\n"
        f"🆔 <b>User ID:</b> <code>{user.id}</code>\n\n"
        f"📝 <b>Question:</b>\n{safe_q}\n\n"
        f"▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬\n"
        f"🕐 {get_dhaka_time()}"
    )

    try:
        await ctx.bot.send_photo(
            chat_id=REPORT_GROUP_ID, photo=file_id,
            caption=caption, parse_mode="HTML"
        )
    except Exception as e:
        logger.error(f"Image QA report photo send error: {e}")
        try:
            await ctx.bot.send_message(
                chat_id=REPORT_GROUP_ID, text=caption,
                parse_mode="HTML", disable_web_page_preview=True
            )
        except Exception as e2:
            logger.error(f"Image QA report text fallback error: {e2}")


async def process_image_question(ctx, msg, user, question: str, file_id: str):
    """
    ছবি-প্রশ্নের পুরো flow: daily limit → ২ মিনিট cooldown → report group →
    processing animation → rich answer → usage footer। AI fail করলে Retry বাটন
    দেওয়া হয় এবং সেই attempt গণনা করা হয় না।
    """
    if user.id != ADMIN_ID:
        if not check_ocr_daily_limit(user.id):
            await send_ocr_limit_message(msg, user.id)
            return
        # text প্রশ্নের মতোই একই ২ মিনিট gap (poll/text/image সব মিলিয়ে)
        remaining = await try_reserve_qa_cooldown(user.id)
        if remaining > 0:
            asyncio.create_task(send_qa_cooldown_countdown(msg, remaining))
            return

    asyncio.create_task(send_image_qa_report(ctx, user, question, file_id))

    status = await msg.reply_text("🔍 *Reading image*", parse_mode=ParseMode.MARKDOWN)

    async def _work():
        image_b64, mime = await fetch_photo_b64(ctx, file_id)
        if not image_b64:
            return "AI_FAILED"
        return await answer_image_question(question, image_b64, mime)

    answer = await run_with_animation(status, _work())

    if not answer or "AI_FAILED" in answer:
        try:
            await status.delete()
        except Exception:
            pass
        retry_id = f"qaretry_{uuid.uuid4().hex[:8]}"
        retry_qa_data[retry_id] = {
            "question":       question,
            "chat_id":        msg.chat_id,
            "user_id":        user.id,
            "image_file_id":  file_id,
            "_created_at":    time.time(),
        }
        keyboard = [[InlineKeyboardButton("🔄 Retry", callback_data=retry_id)]]
        await msg.reply_text(
            f"🤖 *{BOT_NAME}*\n\n"
            "🚧 *System Update in Progress*\n\n"
            "⚙️ AI service সাময়িকভাবে unavailable। এই attempt গণনা করা হয়নি — আবার চেষ্টা করো!",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
        return

    try:
        await status.delete()
    except Exception:
        pass

    await send_long_qa_answer(ctx.bot, msg.chat_id, answer,
                              reply_to_message_id=msg.message_id, user_id=user.id)

    if user.id != ADMIN_ID:
        consume_ocr_daily(user.id)
        update_user_streak(user.id)
        try:
            await ctx.bot.send_message(
                msg.chat_id, ocr_daily_footer(user.id).strip(), parse_mode="HTML"
            )
        except Exception:
            pass

    if user.id in registered_users:
        registered_users[user.id]["last_active"] = time.time()
        _turso_bg(lambda: _save_user(user.id), "save_user")


def format_qa_rich_answer(answer_text: str) -> str:
    """Bot-এর নিজের নাম দিয়ে ছোট branded tag বসিয়ে দেয় (AI কখনো 'Gemini' লিখলেও সেটা বাদ দিয়ে)।
    এটা H1 heading না করে ছোট bold tag রাখা হচ্ছে, যাতে AI-এর নিজের `## টপিক` heading-ই
    উত্তরের মূল, সবচেয়ে বড় title হিসেবে দেখা যায় (reference bot-গুলোর মতোই)।
    'সঠিক উত্তর' লাইনটা inline-code করে দেওয়া হয় যাতে সরাসরি ট্যাপ করেই কপি করা যায়
    (কোনো আলাদা বাটন ছাড়াই) — student এক ট্যাপে friend/group-এ paste করে দিতে পারে।"""
    cleaned = re.sub(r'(?i)\bgemini\b', BOT_NAME, answer_text)
    cleaned = repair_markdown_tables(cleaned)
    cleaned = _make_answer_line_copyable(cleaned)
    return f"✨ **{BOT_NAME}**\n\n{cleaned}"


_SUPERSCRIPT_MAP = str.maketrans({
    "0": "⁰", "1": "¹", "2": "²", "3": "³", "4": "⁴", "5": "⁵", "6": "⁶",
    "7": "⁷", "8": "⁸", "9": "⁹", "+": "⁺", "-": "⁻", "=": "⁼", "(": "⁽", ")": "⁾",
    "n": "ⁿ", "i": "ⁱ",
})
_SUBSCRIPT_MAP = str.maketrans({
    "0": "₀", "1": "₁", "2": "₂", "3": "₃", "4": "₄", "5": "₅", "6": "₆",
    "7": "₇", "8": "₈", "9": "₉", "+": "₊", "-": "₋", "=": "₌", "(": "₍", ")": "₎",
    "a": "ₐ", "e": "ₑ", "h": "ₕ", "i": "ᵢ", "j": "ⱼ", "k": "ₖ", "l": "ₗ", "m": "ₘ",
    "n": "ₙ", "o": "ₒ", "p": "ₚ", "r": "ᵣ", "s": "ₛ", "t": "ₜ", "u": "ᵤ", "v": "ᵥ", "x": "ₓ",
})

_GREEK_MAP = {
    "alpha": "α", "beta": "β", "gamma": "γ", "Gamma": "Γ", "delta": "δ", "Delta": "Δ",
    "epsilon": "ε", "zeta": "ζ", "eta": "η", "theta": "θ", "Theta": "Θ", "iota": "ι",
    "kappa": "κ", "lambda": "λ", "Lambda": "Λ", "mu": "μ", "nu": "ν", "xi": "ξ",
    "pi": "π", "Pi": "Π", "rho": "ρ", "sigma": "σ", "Sigma": "Σ", "tau": "τ",
    "upsilon": "υ", "phi": "φ", "Phi": "Φ", "chi": "χ", "psi": "ψ", "omega": "ω",
    "Omega": "Ω", "infty": "∞", "hbar": "ℏ", "nabla": "∇", "partial": "∂",
}


def delatex(text: str) -> str:
    """
    এই চ্যাট-এ আসল LaTeX render হয় না, তাই AI যদি (prompt-এ নিষেধ করার পরও) ভুলে
    কিছু LaTeX syntax লিখে ফেলে, সেটা safety-net হিসেবে readable Unicode
    গাণিতিক নোটেশনে কনভার্ট করে দেয় — যাতে কখনোই ভাঙা \\frac{}{} বা \\Delta-এর মতো
    raw code চোখে না পড়ে।
    """
    if not text or not re.search(r'\\|\$|\^|_[0-9a-zA-Z{]', text):
        return text

    t = text

    # $$...$$ এবং $...$ delimiter সরিয়ে ভেতরের content রাখো
    t = re.sub(r'\${1,2}([^$]+)\${1,2}', r'\1', t)

    # \text{...}, \mathrm{...} ইত্যাদি wrapper সরিয়ে শুধু ভেতরের টেক্সট রাখো
    t = re.sub(r'\\(?:text|mathrm|mathbf|mathit|operatorname)\{([^{}]*)\}', r'\1', t)

    # \frac{a}{b} -> (a)/(b)  (nested braces ছাড়া সাধারণ ক্ষেত্রে কাজ করে)
    t = re.sub(r'\\frac\{([^{}]*)\}\{([^{}]*)\}', r'(\1)/(\2)', t)
    # \dfrac, \tfrac একই আচরণ
    t = re.sub(r'\\[dt]?frac\{([^{}]*)\}\{([^{}]*)\}', r'(\1)/(\2)', t)

    # \sqrt{a} -> √(a)
    t = re.sub(r'\\sqrt\{([^{}]*)\}', r'√(\1)', t)
    t = re.sub(r'\\sqrt', '√', t)

    # গ্রিক অক্ষর
    for name, sym in _GREEK_MAP.items():
        t = re.sub(rf'\\{name}\b', sym, t)

    # সাধারণ operator/relation
    replacements = {
        r'\\cdot': '×', r'\\times': '×', r'\\div': '÷',
        r'\\pm': '±', r'\\mp': '∓',
        r'\\leq': '≤', r'\\le\b': '≤', r'\\geq': '≥', r'\\ge\b': '≥',
        r'\\neq': '≠', r'\\ne\b': '≠', r'\\approx': '≈', r'\\propto': '∝',
        r'\\to': '→', r'\\rightarrow': '→', r'\\leftarrow': '←',
        r'\\int': '∫', r'\\sum': 'Σ', r'\\prod': '∏',
        r'\\%': '%', r'\\ ': ' ', r'\\,': ' ', r'\\;': ' ', r'\\!': '',
    }
    for pattern, repl in replacements.items():
        t = re.sub(pattern, repl, t)

    # ^{xyz} / _{xyz} — mappable হলে Unicode super/subscript, নাহলে সাধারণ (xyz) ফর্মে
    def _sup(m):
        content = m.group(1)
        if all(ch in "0123456789+-=()ni" for ch in content):
            return content.translate(_SUPERSCRIPT_MAP)
        return f"^({content})"

    def _sub(m):
        content = m.group(1)
        if all(ch in "0123456789+-=()aehijklmnoprstuvx" for ch in content):
            return content.translate(_SUBSCRIPT_MAP)
        return f"_({content})"

    t = re.sub(r'\^\{([^{}]*)\}', _sup, t)
    t = re.sub(r'_\{([^{}]*)\}', _sub, t)
    # ^2, ^n (brace ছাড়া single char)
    t = re.sub(r'\^([0-9a-zA-Z+\-])', lambda m: m.group(1).translate(_SUPERSCRIPT_MAP) if m.group(1) in "0123456789+-ni" else m.group(0), t)
    t = re.sub(r'_([0-9a-zA-Z+\-])', lambda m: m.group(1).translate(_SUBSCRIPT_MAP) if m.group(1) in "0123456789+-aehijklmnoprstuvx" else m.group(0), t)

    # অবশিষ্ট \command (আর কিছু না মিললে) — শুধু backslash-টা সরাও, নাম রাখো
    t = re.sub(r'\\([a-zA-Z]+)', r'\1', t)
    t = t.replace('\\', '')

    return t


# ══════════════════════════════════════════════════════════════════
#  MARKDOWN TABLE REPAIR
#  AI (বিশেষ করে OCR/image উত্তরে) মাঝে মাঝে পুরো table-টা এক লাইনে
#  ("| ক | খ ||---|---|| ...") পাঠিয়ে দেয়, তখন Telegram সেটাকে table
#  হিসেবে render করতে পারে না — raw pipe দেখা যায়। নিচের helper গুলো
#  সেই squashed table কে ঠিক করে আলাদা লাইনে ভাগ করে দেয়।
# ══════════════════════════════════════════════════════════════════
_SEP_CELL_RE = re.compile(r'^\s*:?-{2,}:?\s*$')


def _is_separator_row(row: str) -> bool:
    cells = [c for c in row.strip().strip('|').split('|')]
    return bool(cells) and all(_SEP_CELL_RE.match(c) for c in cells)


def _split_squashed_table_line(line: str) -> list:
    """এক লাইনে ঠেসে দেওয়া table কে আলাদা row-তে ভাগ করে।"""
    # row boundary: "| |" বা "||" — নতুন row-এর শুরু
    parts = re.sub(r'\|\s*\|', '|\n|', line).split('\n')
    out = []
    for i, part in enumerate(parts):
        part = part.rstrip()
        if not part.strip():
            continue
        if i == 0:
            # header row-এর আগে যদি সাধারণ লেখা থাকে, সেটা আলাদা লাইনে নাও
            m = re.match(r'^(.*?[^|\s])\s*(\|.*)$', part)
            if m and '|' not in m.group(1):
                out.append(m.group(1).strip())
                part = m.group(2)
        if not part.strip().startswith('|'):
            part = '|' + part.strip()
        if not part.strip().endswith('|'):
            part = part.rstrip() + '|'
        out.append(part.strip())
    return out


def repair_markdown_tables(text: str) -> str:
    """
    markdown table গুলোকে Telegram-এ ঠিকভাবে render হওয়ার মতো করে সাজায়:
    • এক লাইনে ঠেসে দেওয়া table কে আলাদা row-তে ভাগ করে
    • header row-এর পরে separator row না থাকলে বসিয়ে দেয়
    • table block-এর আগে/পরে একটা করে blank line রাখে
    """
    if not text or '|' not in text:
        return text

    lines = []
    for raw in text.split('\n'):
        if raw.count('|') >= 3 and re.search(r'\|\s*:?-{2,}', raw) and re.search(r'\|\s*\|', raw):
            lines.extend(_split_squashed_table_line(raw))
        else:
            lines.append(raw)

    out = []
    i = 0
    n = len(lines)
    while i < n:
        line = lines[i]
        if line.strip().startswith('|') and line.count('|') >= 2:
            # পুরো table block সংগ্রহ করো
            block = []
            while i < n and lines[i].strip().startswith('|'):
                block.append(lines[i].strip())
                i += 1
            if len(block) >= 2:
                if not _is_separator_row(block[1]):
                    cols = max(1, len([c for c in block[0].strip().strip('|').split('|')]))
                    block.insert(1, '|' + '|'.join(['---'] * cols) + '|')
                if out and out[-1].strip():
                    out.append('')
                out.extend(block)
                out.append('')
            else:
                out.extend(block)
            continue
        out.append(line)
        i += 1

    # একাধিক পরপর blank line একটাতে নামাও
    result = '\n'.join(out)
    result = re.sub(r'\n{3,}', '\n\n', result)
    return result.strip()


def tables_to_plain_text(text: str) -> str:
    """rich render fail করলে (fallback) table কে পড়ার উপযোগী লাইনে বদলায়।"""
    if not text or '|' not in text:
        return text
    out = []
    header = None
    for line in text.split('\n'):
        s = line.strip()
        if s.startswith('|') and s.count('|') >= 2:
            cells = [c.strip() for c in s.strip('|').split('|')]
            if _is_separator_row(s):
                continue
            if header is None:
                header = cells
                continue
            pairs = []
            for idx, c in enumerate(cells):
                label = header[idx] if idx < len(header) else ''
                pairs.append(f"{label}: {c}" if label else c)
            out.append("• " + " — ".join(pairs))
        else:
            header = None
            out.append(line)
    return '\n'.join(out)


_TABLE_ROW_RE = re.compile(r'^\s*\|')


def _is_table_block(block: str) -> bool:
    """block-টা markdown table কিনা চেক করে (header row + separator row থাকলে)।"""
    lines = [l for l in block.split("\n") if l.strip()]
    return len(lines) >= 2 and bool(_TABLE_ROW_RE.match(lines[0])) and bool(_TABLE_ROW_RE.match(lines[1]))


def split_markdown_for_telegram(text: str, max_len: int = 3500) -> list:
    """
    বড় markdown answer-কে Telegram-friendly কয়েকটা chunk-এ ভাগ করে — কিন্তু
    মাঝ-বাক্যে, মাঝ-সূত্রে (LaTeX/math) বা মাঝ-table-এ কখনো কাটে না। আগে
    blank-line দিয়ে block (paragraph/heading/table/list-item) আলাদা করে,
    তারপর সেই block-গুলো greedily জোড়া লাগিয়ে max_len-এর মধ্যে রাখা হয়। কোনো
    একটা block একাই max_len-এর চেয়ে বড় হলে (যেমন: বিশাল table), সেটাকে
    ভাগ করা হয় — টেক্সট কখনো hard-truncate করা হয় না, পুরোটাই কোনো না কোনো
    chunk-এ থাকবে। Table-এর ক্ষেত্রে header+separator row প্রতিটা continuation
    chunk-এ আবার বসিয়ে দেওয়া হয়, নাহলে পরের অংশে খালি data row Telegram-এ
    ভাঙাচোরা/বর্ডার-অলি দেখায়।
    """
    text = (text or "").strip()
    if not text:
        return [""]
    if len(text) <= max_len:
        return [text]

    blocks = re.split(r'\n{2,}', text)
    chunks: list = []
    current = ""

    def _flush():
        nonlocal current
        if current.strip():
            chunks.append(current.strip())
        current = ""

    for block in blocks:
        block = block.strip("\n")
        if not block:
            continue
        candidate = f"{current}\n\n{block}" if current else block

        if len(candidate) <= max_len:
            current = candidate
            continue

        _flush()

        if len(block) <= max_len:
            current = block
            continue

        # এই একটা block-ই max_len-এর চেয়ে বড়
        if _is_table_block(block):
            lines = block.split("\n")
            header = lines[0].strip("\n")
            separator = lines[1].strip("\n")
            head_prefix = f"{header}\n{separator}"
            sub = head_prefix
            for row in lines[2:]:
                if not row.strip():
                    continue
                sub_candidate = f"{sub}\n{row}"
                if len(sub_candidate) <= max_len:
                    sub = sub_candidate
                else:
                    chunks.append(sub.strip())
                    # নতুন continuation chunk header দিয়েই শুরু করো
                    sub = f"{head_prefix}\n{row}" if len(f"{head_prefix}\n{row}") <= max_len else row
            current = sub
        else:
            # লাইন ধরে ধরে ভাগ করো (normal paragraph/list)
            sub = ""
            for line in block.split("\n"):
                sub_candidate = f"{sub}\n{line}" if sub else line
                if len(sub_candidate) <= max_len:
                    sub = sub_candidate
                    continue
                if sub.strip():
                    chunks.append(sub.strip())
                if len(line) > max_len:
                    # একটা লাইনই max_len-এর চেয়ে বড় (খুবই বিরল) — hard-split, কিন্তু
                    # তাও কোনো content হারায় না
                    for i in range(0, len(line), max_len):
                        chunks.append(line[i:i + max_len])
                    sub = ""
                else:
                    sub = line
            current = sub

    _flush()
    return chunks or [text[:max_len]]


async def send_long_qa_answer(bot, chat_id: int, answer_text: str, reply_to_message_id: int = None, user_id: int = None):
    """
    General Q&A-এর উত্তর **একটামাত্র মেসেজে** পাঠায় — rich formatting (heading,
    bold, list, table) সহ, ঠিক যেভাবে Gemini-তে দেখায়।

    prompt-এ AI-কে একটামাত্র মেসেজের মধ্যে উত্তর শেষ করতে বলা আছে, তাই প্রায়
    সব ক্ষেত্রেই পুরো উত্তরটা একটা bubble-এই যায়। খুব বিরল ক্ষেত্রে উত্তরটা
    Telegram-এর সীমার (৪০৯৬ অক্ষর) চেয়ে বড় হয়ে গেলে কোনো লেখা হারিয়ে না
    ফেলে বাকিটুকু পরের মেসেজে continuation হিসেবে পাঠানো হয়।

    user_id দেওয়া থাকলে শেষ chunk-এর সাথে একটা "🎁 Get Extra Limits" বাটন
    জুড়ে দেওয়া হয় (poll-answer share keyboard-এর referral বাটনের মতোই)।
    """
    import urllib.parse

    rich_text = format_qa_rich_answer(answer_text)
    chunks = split_markdown_for_telegram(rich_text, max_len=3900)

    ref_keyboard = None
    if user_id is not None and bot.username:
        bot_link = f"https://t.me/{bot.username}"
        ref_link = f"{bot_link}?start={make_referral_code(user_id)}"
        ref_share_text = urllib.parse.quote(
            f"🚀 *MCQ Poll Solve করতে আর সময় নষ্ট নয়!*\n\n"
            f"🤖 *Synthesis Robot* দিয়ে সেকেন্ডেই Telegram MCQ Poll Solve করুন।\n"
            f"⚡ Fast  •  🎯 Accurate  •  📚 Study Friendly\n\n"
            f"👇 *আমার Referral Link দিয়ে Join করুন:*\n"
            f"{ref_link}\n\n"
            f"💙 *একবার ব্যবহার করলেই পার্থক্য বুঝতে পারবেন!*"
        )
        ref_keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton("🎁 Get Extra Limits", url=f"https://t.me/share/url?text={ref_share_text}")]
        ])

    for i, piece in enumerate(chunks, start=1):
        reply_id = reply_to_message_id if i == 1 else None
        markup = ref_keyboard if i == len(chunks) else None

        sent = await send_rich_message(chat_id, piece, reply_markup=markup, reply_to_message_id=reply_id)
        if sent is None:
            fallback_piece = clean_text(tables_to_plain_text(piece))
            header = f"🤖 *{BOT_NAME}*\n\n" if i == 1 else ""
            try:
                await bot.send_message(
                    chat_id, f"{header}{fallback_piece}",
                    parse_mode=ParseMode.MARKDOWN, reply_to_message_id=reply_id,
                    reply_markup=markup
                )
            except Exception:
                try:
                    await bot.send_message(chat_id, f"{header}{fallback_piece}",
                                           reply_to_message_id=reply_id, reply_markup=markup)
                except Exception as e:
                    logger.error(f"send_long_qa_answer fallback failed (part {i}): {e}")

        if i < len(chunks):
            await asyncio.sleep(0.4)


# ══════════════════════════════════════════════════════════════════
#  TEXT HELPERS
# ══════════════════════════════════════════════════════════════════
def clean_text(text: str) -> str:
    """Plain-text fallback (rich message পাঠানো ব্যর্থ হলে ব্যবহার হয়) — LaTeX
    মুছে ফেলার বদলে delatex() দিয়ে readable Unicode-এ কনভার্ট করে, তারপর
    markdown bold/italic marker সরিয়ে দেয়।"""
    text = delatex(text)
    text = re.sub(r'\*\*(.*?)\*\*', r'\1', text)
    text = re.sub(r'\*(.*?)\*', r'\1', text)
    return text.strip()

def format_section_headers(text: str) -> str:
    """
    AI-এর দেওয়া section header গুলোকে bold করে।
    Header patterns: 📌 মূল সূত্র, 📖 ধাপে ধাপে সমাধান, ❌ অন্য অপশন কেন ভুল
    """
    import re
    header_patterns = [
        r'📌\s*মূল সূত্র',
        r'📖\s*ধাপে ধাপে সমাধান',
        r'❌\s*অন্য অপশন কেন.? ভুল',
    ]
    lines = text.split("\n")
    result_lines = []
    for line in lines:
        stripped = line.strip()
        matched = False
        for pattern in header_patterns:
            if re.match(pattern, stripped):
                bold_line = f"**{stripped}**"
                result_lines.append(bold_line)
                matched = True
                break
        if not matched:
            result_lines.append(line)
    return "\n".join(result_lines)


async def send_retrying(reply_target, *args, retries: int = 2, delay: float = 1.5, **kwargs):
    """
    `reply_target.reply_text(...)` (বা যেকোনো send/reply method)-কে transient
    network glitch (TimedOut/NetworkError — Render-এ মাঝেমধ্যে হয়) হলে অল্প
    delay দিয়ে আবার চেষ্টা করে। এটা মূলত /start-এর মতো critical প্রথম-touch
    reply-গুলোর জন্য — একটা network hiccup-এ user যেন পুরোপুরি কোনো response
    ছাড়াই আটকে না থাকে।

    ব্যবহার: await send_retrying(update.message, text, parse_mode=..., reply_markup=...)
    (update.message.reply_text(text, ...) এর সমতুল্য, শুধু retry সহ)
    """
    from telegram.error import TimedOut, NetworkError
    last_exc = None
    for attempt in range(retries + 1):
        try:
            return await reply_target.reply_text(*args, **kwargs)
        except (TimedOut, NetworkError) as e:
            last_exc = e
            if attempt < retries:
                await asyncio.sleep(delay)
            else:
                logger.warning(f"send_retrying: {retries + 1} attempts-এও পাঠানো গেলো না: {e}")
    raise last_exc


async def safe_edit(msg, text: str, reply_markup=None):
    """Edit a message. If edit fails for any reason, delete and send fresh."""
    # Telegram message edit limit: 4096 chars
    MAX = 4000
    if len(text) > MAX:
        text = text[:MAX] + "\n\n_(বাকি অংশ কাটা হয়েছে)_"
    try:
        await msg.edit_text(text, parse_mode=ParseMode.MARKDOWN,
                            disable_web_page_preview=True, reply_markup=reply_markup)
        return
    except BadRequest:
        pass
    except Exception as e:
        logger.warning(f"safe_edit markdown failed: {e}")
    # Try plain text edit
    try:
        await msg.edit_text(clean_text(text), disable_web_page_preview=True,
                            reply_markup=reply_markup)
        return
    except Exception:
        pass
    # Last resort: delete loading msg, send as new message
    try:
        await msg.delete()
    except Exception:
        pass
    try:
        await msg.chat.send_message(text, parse_mode=ParseMode.MARKDOWN,
                                    disable_web_page_preview=True, reply_markup=reply_markup)
    except Exception:
        try:
            await msg.chat.send_message(clean_text(text), disable_web_page_preview=True,
                                        reply_markup=reply_markup)
        except Exception as e:
            logger.error(f"safe_edit total failure: {e}")


# ══════════════════════════════════════════════════════════════════
#  TAP-TO-COPY ANSWER LINE
#  আলাদা কোনো বাটন ছাড়াই — উত্তরের "সঠিক উত্তর: ..." লাইনটা monospace/
#  inline-code হিসেবে ফরম্যাট করা হয়, যাতে Telegram-এ ওই লাইনে সরাসরি
#  ট্যাপ করলেই টেক্সট clipboard-এ কপি হয়ে যায় (ঠিক যেভাবে ```code block```
#  বা `inline code`-এ ট্যাপ করলে Telegram নিজে থেকেই কপি করে দেয়)।
#  Student তখন কপি হওয়া উত্তরটা সরাসরি কোনো friend/group chat-এ paste
#  করে দিতে পারবে।
# ══════════════════════════════════════════════════════════════════
def _make_line_copyable(line: str) -> str:
    """একটা লাইনকে GFM inline-code (`...`) দিয়ে wrap করে — Telegram-এ
    ট্যাপ করলে কপি হয়ে যাবে। লাইনে ব্যাকটিক থাকলে সেটা সরিয়ে দেয়
    (নইলে code span ভেঙে যায়)।"""
    safe = line.replace("`", "'")
    return f"`{safe}`"


def _make_answer_line_copyable(text: str) -> str:
    """
    AI-এর rich উত্তরে যে লাইনে 'সঠিক উত্তর' থাকে, সেই লাইনটাকে (bold marker
    সহ) inline-code দিয়ে wrap করে দেয়, যাতে সরাসরি ট্যাপ করে কপি করা যায়।
    অন্য কোনো লাইন স্পর্শ করা হয় না।
    """
    if not text or "সঠিক উত্তর" not in text:
        return text
    lines = text.split("\n")
    for idx, line in enumerate(lines):
        stripped = line.strip()
        if "সঠিক উত্তর" in stripped and not stripped.startswith("`"):
            leading_ws = line[:len(line) - len(line.lstrip())]
            # bold marker (**...**) সরিয়ে ফেলি — code span-এর ভেতরে markdown parse হয় না
            plain = re.sub(r'\*\*(.*?)\*\*', r'\1', stripped)
            lines[idx] = f"{leading_ws}{_make_line_copyable(plain)}"
            break
    return "\n".join(lines)


def _make_share_keyboard(bot_username: str, question: str, options: list, correct_idx, user_id: int = None) -> InlineKeyboardMarkup:
    """
    Share button তৈরি করে।
    Format: Question + Options + সঠিক উত্তর + bot link
    """
    import urllib.parse
    bot_link = f"https://t.me/{bot_username}"

    # Options text
    opts_lines = "\n".join([
        f"{'✅' if i == correct_idx else '▪️'} {chr(65+i)}) {opt}"
        for i, opt in enumerate(options)
    ])
    correct_letter = chr(65 + correct_idx) if correct_idx is not None else "?"
    correct_text   = options[correct_idx] if correct_idx is not None else "?"

    share_text = (
        f"🤖 Solve by Synthesis Robot\n\n"
        f"❓ {question}\n\n"
        f"{opts_lines}\n\n"
        f"✅ সঠিক উত্তর: {correct_letter}) {correct_text}\n\n"
        f"━━━━━━━━━━━━━━\n"
        f"Use the Poll Solver Bot:\n{bot_link}"
    )

    # Telegram URL max ~4096 chars — truncate if needed
    if len(share_text) > 3800:
        share_text = share_text[:3800] + "..."

    share_url = (
        f"https://t.me/share/url?"
        f"url={urllib.parse.quote(bot_link, safe='')}"
        f"&text={urllib.parse.quote(share_text, safe='')}"
    )

    buttons = []

    # Referral link share button — same button/behavior as "Get Extra Poll Limits" menu
    if user_id is not None:
        ref_link = f"{bot_link}?start={make_referral_code(user_id)}"
        ref_share_text = urllib.parse.quote(
            f"🚀 *MCQ Poll Solve করতে আর সময় নষ্ট নয়!*\n\n"
            f"🤖 *Synthesis Robot* দিয়ে সেকেন্ডেই Telegram MCQ Poll Solve করুন।\n"
            f"⚡ Fast  •  🎯 Accurate  •  📚 Study Friendly\n\n"
            f"👇 *আমার Referral Link দিয়ে Join করুন:*\n"
            f"{ref_link}\n\n"
            f"💙 *একবার ব্যবহার করলেই পার্থক্য বুঝতে পারবেন!*"
        )
        buttons.append(
            [InlineKeyboardButton("🎁 Get Extra Limits", url=f"https://t.me/share/url?text={ref_share_text}")]
        )

    return InlineKeyboardMarkup(buttons)

def _lib_move_keyboard(node_id: str, parent_id: str) -> InlineKeyboardMarkup:
    """Inline keyboard for reordering a button within its parent."""
    children = library_data.get(parent_id, {}).get("children", [])
    idx = children.index(node_id) if node_id in children else -1
    total = len(children)
    buttons = []
    # Row 1: Up arrow (only if not first)
    if idx > 0:
        buttons.append([InlineKeyboardButton("⬆️ Move Up", callback_data=f"lib_move:{node_id}:up")])
    # Row 2: Left | Right (position within row pair)
    lr = []
    if idx > 0:
        lr.append(InlineKeyboardButton("⬅️ Move Left", callback_data=f"lib_move:{node_id}:left"))
    if idx < total - 1:
        lr.append(InlineKeyboardButton("➡️ Move Right", callback_data=f"lib_move:{node_id}:right"))
    if lr:
        buttons.append(lr)
    # Row 3: Down arrow (only if not last)
    if idx < total - 1:
        buttons.append([InlineKeyboardButton("⬇️ Move Down", callback_data=f"lib_move:{node_id}:down")])
    # Always show Done
    buttons.append([InlineKeyboardButton("✅ Done", callback_data=f"lib_move:{node_id}:done")])
    return InlineKeyboardMarkup(buttons)


import html as _html_lib

_TAG_BRACKET_PAIRS = [('[', ']'), ('【', '】'), ('《', '》'), ('「', '」'), ('『', '』')]
_TAG_ARROW_CHARS = '➡➔→⇒▶►\u27A1\uFE0F'
_TAG_DECORATIVE_CHARS = '◊⬥⬦✦✧★☆⚡➤➔→⇒▶►«»「」『』【】━─▪▫◆◇🔷🔶🔰📌📍❇❄'

# যেকোনো emoji (☀️, 🤖, ➡️, ✨ ইত্যাদি — source bot/channel প্রায়ই এগুলো দিয়ে
# নিজের tag/branding line সাজায়) ধরার জন্য ব্যাপক Unicode range — decorative
# char list-এ প্রতিটা emoji আলাদা করে বসানোর বদলে single pattern দিয়ে কভার করা হয়।
_EMOJI_PATTERN = re.compile(
    "["
    "\U0001F000-\U0001FFFF"   # emoticons, symbols, pictographs, transport ইত্যাদি
    "\U00002190-\U000021FF"   # arrows (→ ➡ ইত্যাদি ব্লকের একটা অংশ)
    "\U00002300-\U000027BF"   # misc technical + dingbats (☀ ✅ ✨ ➡ ➤ ইত্যাদি)
    "\U00002B00-\U00002BFF"   # misc symbols and arrows
    "\uFE0F"                  # variation selector (emoji রঙিন রেন্ডার করার marker)
    "]"
)


def _has_emoji(text: str) -> bool:
    return bool(_EMOJI_PATTERN.search(text or ''))


# source bot/channel প্রায়ই নিজের নাম একটা bare single-word tag হিসেবে বসায়
# (যেমন "Qubix") — কোনো decoration/emoji/link ছাড়াই। এমন লাইন যদি একদম আলাদা
# (নিজের লাইনে) এবং তার ঠিক পরের লাইনেই আসল প্রশ্ন (যেটা '?'/'।' দিয়ে শেষ হয়)
# থাকে, সেটাকেও bare tag হিসেবে ধরা হয়।
_BARE_WORD_TAG_RE = re.compile(r'^[A-Za-z][A-Za-z0-9_]{1,24}$')


def _looks_like_tag_line(ln: str) -> bool:
    """
    একটা লাইন (সাধারণত explanation-এর ভেতরের কোনো একটা লাইন) source bot/channel-এর
    ছোট branding/tag-লাইন কিনা তা বলে — যেমন "☀️ Midday Challenge", "➡️ Qubix"।
    শর্ত: লাইনটা ছোট (≤60 char), প্রশ্ন/বাক্য-শেষের চিহ্ন দিয়ে শেষ হয় না, তাতে
    emoji/decorative symbol আছে, আর emoji/decorative char বাদ দিলে যা থাকে সেটা
    বড়জোর কয়েকটা শব্দ (আসল ব্যাখ্যা/বাক্য নয়, শুধু নাম/হেডলাইন)।
    """
    if not ln or len(ln) > 60:
        return False
    if ln.endswith(('?', '؟', '।', '.', '!', ':')):
        return False
    has_decor = any(ch in _TAG_DECORATIVE_CHARS for ch in ln) or _has_emoji(ln)
    if not has_decor:
        return False
    core = _EMOJI_PATTERN.sub('', ln)
    for ch in _TAG_DECORATIVE_CHARS:
        core = core.replace(ch, '')
    core = core.strip(" -–—:|,")
    return len(core.split()) <= 4


def _strip_question_tag(question: str) -> str:
    """
    Question-এর শুরুতে থাকা (এক বা একাধিক, stacked) source-bot/channel-এর
    নিজস্ব tag/branding/emoji-headline সরিয়ে শুধু আসল প্রশ্নের টেক্সট রাখে।
    কোনো bot/channel যেভাবেই tag বসাক না কেন, নিচের সব ধরনের pattern handle করে —
    আর একাধিক tag stacked (একটার পর একটা লাইনে বা একই লাইনে dash দিয়ে জোড়া
    লাগানো) থাকলে loop করে সবগুলো সরায়:
      • "[Tag] ➡", "(Tag)", "【Tag】", "《Tag》", "「Tag」", "『Tag』" — যেকোনো bracket
        style + ঐচ্ছিক arrow
      • "◊ Name ◊", "⬥ Name ⬥" — decorative-symbol/emoji দিয়ে wrap করা tag line
      • "☀️ Midday Challenge — আসল প্রশ্ন...?" — emoji/headline + em-dash/hyphen
        দিয়ে একই লাইনে জোড়া লাগানো tag+question
      • কোনো ছোট আলাদা লাইন যেখানে শুধু channel/bot-এর নাম, @username,
        t.me/ লিংক, https:// লিংক, emoji/decorative symbol, অথবা একটামাত্র
        bare word (যেমন "Qubix") আছে (প্রশ্ন-চিহ্ন দিয়ে শেষ হয় না, ছোট)
    সর্বোচ্চ ৫ বার loop করে, যাতে ভুলবশত আসল প্রশ্নের অংশ কেটে না যায়।
    """
    if not question:
        return question
    q = question.strip()

    for _ in range(5):
        changed = False

        # ── Style A: bracket-wrapped tag (যেকোনো bracket ধরন) + ঐচ্ছিক arrow ──
        for open_ch, close_ch in _TAG_BRACKET_PAIRS:
            pattern = (
                '^' + re.escape(open_ch) + r'.{0,80}?' + re.escape(close_ch)
                + r'\s*[' + re.escape(_TAG_ARROW_CHARS) + r']*\s*\n*'
            )
            m = re.match(pattern, q)
            if m and m.end() > 0:
                q = q[m.end():].strip()
                changed = True
                break
        if changed:
            continue

        # ── Style B: decorative-symbol/emoji wrap বা ছোট bare tag-line
        #    (channel/bot নাম, @handle, link, বা একটামাত্র bare word) ──
        lines = q.split('\n', 1)
        if len(lines) == 2:
            first_line, rest = lines[0].strip(), lines[1]
            has_decoration = (
                any(ch in _TAG_DECORATIVE_CHARS for ch in first_line)
                or _has_emoji(first_line)
            )
            has_link_or_handle = bool(re.search(r'https?://|t\.me/|@\w{4,}', first_line, re.IGNORECASE))
            is_bare_word_tag = bool(_BARE_WORD_TAG_RE.match(first_line))
            looks_like_tag = (
                first_line
                and len(first_line) <= 60
                and not first_line.endswith(('?', '؟', '।'))
                and (has_decoration or has_link_or_handle or is_bare_word_tag or '_' in first_line)
            )
            if looks_like_tag and rest.strip():
                q = rest.strip()
                changed = True
        if changed:
            continue

        # ── Style C: একই লাইনে emoji/headline + em-dash/hyphen দিয়ে জোড়া লাগানো
        #    tag+question (যেমন "☀️ Midday Challenge — কোনটি পাইরিমিডিন নয়?") ──
        first_line_full = q.split('\n', 1)[0]
        m = re.match(r'^(?P<prefix>\S.{0,58}?)\s*[—–]\s+(?P<rest>\S.*)$', first_line_full)
        if m:
            prefix = m.group('prefix').strip()
            rest_of_line = m.group('rest').strip()
            prefix_has_decor = _has_emoji(prefix) or any(ch in _TAG_DECORATIVE_CHARS for ch in prefix)
            prefix_not_question = not prefix.endswith(('?', '؟', '।'))
            rest_looks_like_question = bool(re.search(r'[\u0980-\u09FF]|[A-Za-z]', rest_of_line))
            if prefix_has_decor and prefix_not_question and rest_looks_like_question:
                q = (rest_of_line + q[len(first_line_full):]).strip()
                changed = True

        if not changed:
            break

    return q


def _md_line_to_html(line: str) -> str:
    """
    একটা লাইন — যদি পুরোটা *...* দিয়ে wrap করা থাকে (format_section_headers
    যেভাবে section header বোল্ড করে), সেটাকে <b>...</b>-এ কনভার্ট করে।
    আর যদি পুরোটা `...` (inline-code) দিয়ে wrap করা থাকে (_make_answer_line_copyable
    যেভাবে 'সঠিক উত্তর' লাইন wrap করে), সেটাকে <code>...</code>-এ কনভার্ট করে —
    HTML fallback-এও লাইনটা tap-to-copy monospace থাকে।
    বাকি সব লাইন শুধু HTML-escape করা হয় (AI-এর টেক্সটে থাকা যেকোনো stray
    *, _, <, > থাকলেও তাতে HTML parsing ভাঙবে না)।
    """
    stripped = line.strip()
    m = re.match(r'^\*\*(.+)\*\*$', stripped) or re.match(r'^\*(.+)\*$', stripped)
    if m:
        leading_ws = line[:len(line) - len(line.lstrip())]
        return f"{leading_ws}<b>{_html_lib.escape(m.group(1))}</b>"
    mc = re.match(r'^`(.+)`$', stripped)
    if mc:
        leading_ws = line[:len(line) - len(line.lstrip())]
        return f"{leading_ws}<code>{_html_lib.escape(mc.group(1))}</code>"
    return _html_lib.escape(line)


def _result_to_html(result_text: str) -> str:
    return "\n".join(_md_line_to_html(l) for l in result_text.split("\n"))


async def send_solved_answer(bot, chat_id: int, status_msg, question: str, options: list,
                              result_text: str, footer_html: str = "", reply_markup=None):
    """
    Status ("🔍 Analyzing...") মেসেজ ডিলিট করে rich message (Telegram Bot API 10.1)
    হিসেবে পাঠায়: copyable question+options (GFM fenced code block) + AI-এর
    GFM markdown উত্তর (table, LaTeX সহ) — সব একসাথে, একটামাত্র bubble-এ।
    rich message পাঠানো ব্যর্থ হলে (পুরনো client ইত্যাদি) আগের HTML পদ্ধতিতে
    fallback করে, যেখানে LaTeX delatex() দিয়ে readable Unicode-এ কনভার্ট হয়।
    footer_html-এ থাকা HTML tag (<b>, <code> ইত্যাদি) Rich Markdown-এর ভেতর
    সরাসরি বসানো যায় — Rich Markdown GFM-এর পাশাপাশি supported HTML tag-ও
    inline-এ render করে, তাই আলাদা কনভার্সনের দরকার নেই।
    """
    try:
        await status_msg.delete()
    except Exception:
        pass

    question_clean = _strip_question_tag(question)
    opts_lines = "\n".join([f"{chr(65+i)}) {opt}" for i, opt in enumerate(options)])
    block_text = f"◊ The Paraffin Classroom -TPC ◊\n{question_clean}\n\n{opts_lines}"
    safe_block = block_text.replace("```", "'''")  # fenced code block ভাঙা এড়াতে

    result_md = repair_markdown_tables(result_text)
    result_md = _make_answer_line_copyable(result_md)

    header_md = f"🤖 **{BOT_NAME}**\n\n```text\n{safe_block}\n```\n\n"
    MAX = 3900

    room_for_first_chunk = max(500, MAX - len(header_md) - len(footer_html) - 50)
    text_chunks = split_markdown_for_telegram(result_md, max_len=room_for_first_chunk)
    total = len(text_chunks)

    last_sent = None
    for i, chunk in enumerate(text_chunks, start=1):
        part_tag = f"\n\n_(অংশ {i}/{total})_" if total > 1 else ""
        if i == 1:
            piece = f"{header_md}{chunk}{footer_html if total == 1 else ''}{part_tag}"
        else:
            piece = f"{chunk}{footer_html if i == total else ''}{part_tag}"
        piece_markup = reply_markup if i == total else None

        sent = await send_rich_message(chat_id, piece, reply_markup=piece_markup)
        if sent is not None:
            last_sent = sent
        else:
            # ── Fallback: rich message ব্যর্থ হলে HTML দিয়ে পাঠাও ──
            logger.warning(f"send_solved_answer rich-message failed (part {i}/{total}), falling back to HTML")
            html_header = f"🤖 <b>{_html_lib.escape(BOT_NAME)}</b>\n\n<pre><code class=\"language-text\">{_html_lib.escape(block_text)}</code></pre>\n\n" if i == 1 else ""
            fallback_chunk = _result_to_html(delatex(chunk))
            piece_html = f"{html_header}{fallback_chunk}{footer_html if i == total else ''}{part_tag}"
            try:
                last_sent = await bot.send_message(chat_id, piece_html, parse_mode=ParseMode.HTML,
                                                    disable_web_page_preview=True, reply_markup=piece_markup)
            except Exception as e:
                logger.warning(f"send_solved_answer HTML failed (part {i}/{total}): {e}")
                try:
                    last_sent = await bot.send_message(chat_id, clean_text(chunk),
                                                        disable_web_page_preview=True, reply_markup=piece_markup)
                except Exception as e2:
                    logger.error(f"send_solved_answer total failure (part {i}/{total}): {e2}")

        if i < total:
            await asyncio.sleep(0.4)

    return last_sent


def fix_answer_letter(text: str, options: list) -> str:
    """
    AI-এর দেওয়া উত্তর থেকে সঠিক option text বের করে,
    তারপর সেই text-টি forwarded poll-এর options-এ কোন position-এ আছে
    সেই অনুযায়ী letter (A/B/C/D) ঠিক করে।
    """
    lines = text.split("\n")
    for idx, line in enumerate(lines):
        if "সঠিক উত্তর" not in line:
            continue

        # Step 1: AI-এর line থেকে option text বের করো
        # Format সাধারণত: "✅ সঠিক উত্তর: X) SomeText"
        # আমরা X) এর পরের text নিব
        ai_answer_text = None
        letter_match = re.search(r'[A-Da-d]\)\s*(.+)', line)
        if letter_match:
            ai_answer_text = letter_match.group(1).strip()

        # Step 2: সেই text forwarded poll-এর options-এ কোথায় আছে খুঁজো
        matched_i = None
        best_len  = 0

        if ai_answer_text:
            # Exact match বা substring match (case-insensitive)
            for i, opt in enumerate(options):
                opt_clean = opt.strip()
                if not opt_clean:
                    continue
                # Exact match
                if opt_clean.lower() == ai_answer_text.lower():
                    matched_i = i
                    best_len  = len(opt_clean)
                    break
                # Substring match (opt inside ai_answer_text or vice versa)
                if (opt_clean.lower() in ai_answer_text.lower() or
                        ai_answer_text.lower() in opt_clean.lower()):
                    if len(opt_clean) > best_len:
                        matched_i = i
                        best_len  = len(opt_clean)

        # Fallback: পুরো line-এ option text খোঁজো (আগের পদ্ধতি)
        if matched_i is None:
            for i, opt in enumerate(options):
                opt_clean = opt.strip()
                if opt_clean and opt_clean in line and len(opt_clean) > best_len:
                    matched_i = i
                    best_len  = len(opt_clean)

        if matched_i is None:
            continue

        # Step 3: Correct letter এবং text দিয়ে line replace করো
        correct_letter = chr(65 + matched_i)
        correct_text   = options[matched_i].strip()
        prefix = "✅ সঠিক উত্তর:" if line.lstrip().startswith("✅") else "সঠিক উত্তর:"
        lines[idx] = f"{prefix} {correct_letter}) {correct_text}"
        break
    return "\n".join(lines)


# ══════════════════════════════════════════════════════════════════
#  POLL SOLVER
# ══════════════════════════════════════════════════════════════════

# ══════════════════════════════════════════════════════════════════
#  POLL ANSWER CACHE  (SQLite — AI call বাঁচায় ৭০-৯০%)
# ══════════════════════════════════════════════════════════════════

CACHE_DB = os.path.join(os.path.dirname(os.path.abspath(__file__)), "poll_cache.db")
_cache_conn: sqlite3.Connection | None = None
POLL_CACHE_TTL_SECS = 60 * 24 * 3600  # ৬০ দিন ব্যবহার না হলে cache entry expire (DB ছোট রাখার জন্য)

def _get_cache_conn() -> sqlite3.Connection:
    global _cache_conn
    if _cache_conn is None:
        _cache_conn = sqlite3.connect(CACHE_DB, check_same_thread=False)
        _cache_conn.execute("""
            CREATE TABLE IF NOT EXISTS poll_cache (
                cache_key   TEXT PRIMARY KEY,
                question    TEXT NOT NULL,
                options_json TEXT NOT NULL,
                answer      TEXT NOT NULL,
                hit_count   INTEGER DEFAULT 0,
                created_at  REAL NOT NULL,
                last_hit_at REAL NOT NULL
            )
        """)
        _cache_conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_created ON poll_cache(created_at)"
        )
        _cache_conn.commit()
        logger.info(f"✅ Poll cache DB initialized: {CACHE_DB}")
    return _cache_conn


def _make_cache_key(question: str, options: list) -> str:
    """প্রশ্ন + অপশন normalize করে SHA-256 key বানায়।"""
    norm_q = re.sub(r"\s+", " ", question.strip().lower())
    norm_opts = "|".join(re.sub(r"\s+", " ", o.strip().lower()) for o in options)
    raw = f"{norm_q}::{norm_opts}"
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def cache_get(question: str, options: list) -> str | None:
    """Cache hit হলে answer return করে, না হলে None।"""
    try:
        key = _make_cache_key(question, options)
        conn = _get_cache_conn()
        row = conn.execute(
            "SELECT answer FROM poll_cache WHERE cache_key = ?", (key,)
        ).fetchone()
        if row:
            conn.execute(
                "UPDATE poll_cache SET hit_count = hit_count + 1, last_hit_at = ? WHERE cache_key = ?",
                (time.time(), key)
            )
            conn.commit()
            logger.info(f"🎯 Cache HIT: {question[:60]!r}")
            return row[0]
        return None
    except Exception as e:
        logger.error(f"Cache get error: {e}")
        return None


def cache_set(question: str, options: list, answer: str) -> None:
    """AI উত্তর cache-এ save করে (local SQLite + Turso)."""
    try:
        key = _make_cache_key(question, options)
        now = time.time()
        opts_json = json.dumps(options, ensure_ascii=False)
        conn = _get_cache_conn()
        conn.execute(
            "INSERT OR REPLACE INTO poll_cache "
            "(cache_key, question, options_json, answer, hit_count, created_at, last_hit_at) "
            "VALUES (?, ?, ?, ?, COALESCE((SELECT hit_count FROM poll_cache WHERE cache_key = ?), 0), ?, ?)",
            (key, question, opts_json, answer, key, now, now)
        )
        conn.commit()
        logger.info(f"Cache SAVE: {question[:60]!r}")
        # Turso-তেও async save করো
        _turso_bg(lambda: turso_exec(
            "INSERT OR REPLACE INTO poll_cache "
            "(cache_key, question, options_json, answer, hit_count, created_at, last_hit_at) "
            "VALUES (?, ?, ?, ?, COALESCE((SELECT hit_count FROM poll_cache WHERE cache_key = ?), 0), ?, ?)",
            (key, question, opts_json, answer, key, now, now)
        ), "save_poll_cache")
    except Exception as e:
        logger.error(f"Cache set error: {e}")


def cache_stats() -> dict:
    """Cache statistics — /stats command-এ দেখানোর জন্য।"""
    try:
        conn = _get_cache_conn()
        total = conn.execute("SELECT COUNT(*) FROM poll_cache").fetchone()[0]
        hits  = conn.execute("SELECT SUM(hit_count) FROM poll_cache").fetchone()[0] or 0
        return {"total_entries": total, "total_hits": hits}
    except Exception:
        return {"total_entries": 0, "total_hits": 0}


async def load_poll_cache_from_turso():
    """
    BUG FIX: cache_set() সবসময় local SQLite + Turso দুটোতেই write করতো, কিন্তু
    startup-এ কোনো function poll_cache Turso থেকে ফিরিয়ে আনতো না — শুধু
    users/rate_limits/daily_stats/referral_bonus load হতো (load_from_turso দেখো)।
    Render-এ local SQLite file ephemeral, তাই restart/redeploy হলেই cache খালি
    হয়ে যেত আর Turso-তে থাকা সব আগের cached answer অকেজো পড়ে থাকতো —
    AI call বাঁচানোর পুরো উদ্দেশ্যটাই নষ্ট হয়ে যেত। এই function সেই gap পূরণ করে।
    """
    client = await _get_turso()
    if client is None:
        return
    try:
        rs = await client.execute(
            "SELECT cache_key, question, options_json, answer, hit_count, created_at, last_hit_at FROM poll_cache"
        )
        conn = _get_cache_conn()
        n = 0
        for row in rs.rows:
            conn.execute(
                """INSERT INTO poll_cache
                   (cache_key, question, options_json, answer, hit_count, created_at, last_hit_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?)
                   ON CONFLICT(cache_key) DO UPDATE SET
                       hit_count   = MAX(poll_cache.hit_count, excluded.hit_count),
                       last_hit_at = MAX(poll_cache.last_hit_at, excluded.last_hit_at)""",
                (row[0], row[1], row[2], row[3],
                 int(row[4] or 0), float(row[5] or 0), float(row[6] or 0))
            )
            n += 1
        conn.commit()
        logger.info(f"✅ Loaded {n} poll-cache entries from Turso into local cache")
    except Exception as e:
        logger.error(f"Turso load poll_cache error: {e}")


# Cache শুরুতেই initialize করো
try:
    _get_cache_conn()
except Exception as e:
    logger.error(f"Cache init failed: {e}")


# ══════════════════════════════════════════════════════════════════
#  BROADCAST POLL LIBRARY
#  Student-রা bot দিয়ে যেই poll গুলো solve করে, সেগুলো (source-এর নিজস্ব
#  tag/branding সরিয়ে আমাদের নিজস্ব tag বসিয়ে, explanation-এর link/tag
#  সরিয়ে আমাদের ৩টা join-link বসিয়ে) এখানে জমা হয় — পরে প্রতিদিন ৪ বার
#  (সকাল ৭টা, দুপুর ১টা, বিকেল ৫টা, রাত ৯টা — Dhaka time) প্রতিটা user-কে
#  randomly একটা "সে আগে যেটা পায়নি" এমন poll পাঠানো হয়।
# ══════════════════════════════════════════════════════════════════

BROADCAST_DB = os.path.join(os.path.dirname(os.path.abspath(__file__)), "broadcast_polls.db")
_broadcast_conn: sqlite3.Connection | None = None

# ── আমাদের bot-এর নিজস্ব join-link — explanation-এ যেকোনো পুরনো link/tag
#    সরিয়ে সবসময় এই ৩টাই বসবে ──
BROADCAST_JOIN_LINKS = [
    "Join : https://t.me/TPCadmission",
    "Join : https://t.me/ArektaQuizBot",
    "Join: https://t.me/SynthesisAIRobot",
]
_BROADCAST_JOIN_FOOTER = "\n".join(BROADCAST_JOIN_LINKS)

TELEGRAM_POLL_QUESTION_LIMIT    = 300  # Telegram Bot API hard limit
TELEGRAM_POLL_EXPLANATION_LIMIT = 200  # Telegram Bot API hard limit


def _get_broadcast_conn() -> sqlite3.Connection:
    global _broadcast_conn
    if _broadcast_conn is None:
        _broadcast_conn = sqlite3.connect(BROADCAST_DB, check_same_thread=False)
        _broadcast_conn.execute("""
            CREATE TABLE IF NOT EXISTS broadcast_polls (
                poll_key     TEXT PRIMARY KEY,
                question     TEXT NOT NULL,
                options_json TEXT NOT NULL,
                correct_idx  INTEGER NOT NULL,
                explanation  TEXT NOT NULL,
                created_at   REAL NOT NULL
            )
        """)
        _broadcast_conn.execute("""
            CREATE TABLE IF NOT EXISTS broadcast_sent (
                user_id  INTEGER NOT NULL,
                poll_key TEXT NOT NULL,
                sent_at  REAL NOT NULL,
                PRIMARY KEY (user_id, poll_key)
            )
        """)
        _broadcast_conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_broadcast_sent_user ON broadcast_sent(user_id)"
        )
        _broadcast_conn.commit()
        logger.info(f"✅ Broadcast poll DB initialized: {BROADCAST_DB}")
    return _broadcast_conn


def _make_broadcast_poll_key(question_clean: str, options: list) -> str:
    """প্রশ্ন (tag বাদে) + option মিলিয়ে unique key — একই poll দুইবার library-তে ঢুকবে না।"""
    norm_q = re.sub(r"\s+", " ", question_clean.strip().lower())
    norm_opts = "|".join(re.sub(r"\s+", " ", o.strip().lower()) for o in options)
    raw = f"{norm_q}::{norm_opts}"
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def _prepare_broadcast_question(question_raw: str) -> str:
    """Source poll-এর নিজস্ব tag সরিয়ে আমাদের bot-tag বসায় (Telegram 300-char limit মেনে)।"""
    q_clean = _strip_question_tag(question_raw)
    tag_line = f"◊ {BOT_NAME} ◊"
    full = f"{tag_line}\n{q_clean}"
    if len(full) > TELEGRAM_POLL_QUESTION_LIMIT:
        max_q_len = TELEGRAM_POLL_QUESTION_LIMIT - len(tag_line) - 2  # \n + ellipsis room
        if max_q_len < 10:
            max_q_len = 10
        q_clean = q_clean[:max_q_len].rstrip() + "…"
        full = f"{tag_line}\n{q_clean}"
    return full


def _clean_explanation_for_broadcast(raw_explanation: str) -> str:
    """
    Original poll-এর explanation-এ থাকা যেকোনো link (https://, t.me/, @username),
    bracket-wrapped source/channel tag ("[Arekta Quiz Bot]", "【Tag】" ইত্যাদি), এবং
    'Join/Channel/Subscribe' জাতীয় ছোট promotional লাইন সরিয়ে ফেলে, তারপর আমাদের
    নিজস্ব ৩টা join-link যোগ করে — সব মিলিয়ে Telegram-এর 200-char explanation
    limit-এর মধ্যে রাখে। Idempotent — আগে থেকেই আমাদের নিজস্ব footer যুক্ত থাকা
    explanation আবার re-clean করলেও (resanitize_broadcast_library-এর মতো) footer
    ডুপ্লিকেট হয় না।
    """
    text = (raw_explanation or "").strip()

    # ইতিমধ্যে আমাদের footer যুক্ত থাকলে (re-sanitize করার সময়) সেটা আগে সরাও, নইলে ডুপ্লিকেট হবে
    if text.endswith(_BROADCAST_JOIN_FOOTER):
        text = text[:-len(_BROADCAST_JOIN_FOOTER)].rstrip()

    # সব ধরনের link/handle সরাও
    text = re.sub(r'https?://\S+', '', text)
    text = re.sub(r'(?<!\w)t\.me/\S+', '', text, flags=re.IGNORECASE)
    text = re.sub(r'(?<!\w)@\w{4,}', '', text)

    # bracket-wrapped source/channel tag সরাও ("[Arekta Quiz Bot]", "【Tag】" ইত্যাদি —
    # যেকোনো জায়গায়, শুধু শুরুতে না, কারণ explanation-এর মাঝেও এমন tag বসানো থাকতে পারে)
    for open_ch, close_ch in _TAG_BRACKET_PAIRS:
        text = re.sub(re.escape(open_ch) + r'.{0,60}?' + re.escape(close_ch), '', text)

    # ছোট promotional/tag-জাতীয় লাইন বাদ দাও (join/channel/subscribe ইত্যাদি),
    # আর emoji/decorative-symbol দিয়ে সাজানো ছোট branding/tag লাইনও (যেমন
    # "☀️ Midday Challenge", "➡️ Qubix") বাদ দাও — আসল বাক্য/সমাধান নয় এমন
    # ছোট, emoji-heavy লাইন বাদ দেওয়া হয়, কোনো পূর্ণ বাক্য/ব্যাখ্যা স্পর্শ করা হয় না।
    kept_lines = []
    for ln in text.split('\n'):
        ln = ln.strip(" -•—\t")
        if not ln:
            continue
        low = ln.lower()
        if len(ln) <= 60 and any(k in low for k in ('join', 'channel', 'subscribe', 'group', 'follow', 'link')):
            continue
        if _looks_like_tag_line(ln):
            continue
        kept_lines.append(ln)

    body = " ".join(kept_lines).strip()
    body = re.sub(r'\s+', ' ', body)

    room = TELEGRAM_POLL_EXPLANATION_LIMIT - len(_BROADCAST_JOIN_FOOTER) - 1  # -1 = newline
    if room < 0:
        return _BROADCAST_JOIN_FOOTER[:TELEGRAM_POLL_EXPLANATION_LIMIT]

    if body:
        if len(body) > room:
            body = body[:max(0, room - 1)].rstrip() + "…"
        return f"{body}\n{_BROADCAST_JOIN_FOOTER}"
    return _BROADCAST_JOIN_FOOTER


def _extract_correct_index(result_text: str, options: list) -> "int | None":
    """AI-এর টেক্সট উত্তর থেকে সঠিক option-এর index বের করে (fix_answer_letter-এর matching logic পুনঃব্যবহার করে)।"""
    if not result_text:
        return None
    for line in result_text.split("\n"):
        if "সঠিক উত্তর" not in line:
            continue
        letter_match = re.search(r'[A-Da-d]\)\s*(.+)', line)
        ai_answer_text = letter_match.group(1).strip() if letter_match else None
        matched_i, best_len = None, 0
        if ai_answer_text:
            for i, opt in enumerate(options):
                opt_clean = opt.strip()
                if not opt_clean:
                    continue
                if opt_clean.lower() == ai_answer_text.lower():
                    return i
                if opt_clean.lower() in ai_answer_text.lower() or ai_answer_text.lower() in opt_clean.lower():
                    if len(opt_clean) > best_len:
                        matched_i, best_len = i, len(opt_clean)
        if matched_i is None:
            bare_letter = re.search(r'[A-Da-d]\)', line)
            if bare_letter:
                idx = ord(bare_letter.group(0)[0].upper()) - 65
                if 0 <= idx < len(options):
                    matched_i = idx
        if matched_i is not None:
            return matched_i
    return None


def save_broadcast_poll(question_raw: str, options: list, correct_idx: "int | None", explanation_raw: str) -> None:
    """সফলভাবে solve হওয়া poll — clean tag + clean explanation সহ — broadcast library-তে save করে।"""
    try:
        if correct_idx is None or not (0 <= correct_idx < len(options)):
            return  # সঠিক answer নিশ্চিত না হলে broadcast library-তে রাখা হবে না
        q_bare      = _strip_question_tag(question_raw)
        poll_key    = _make_broadcast_poll_key(q_bare, options)
        q_display   = _prepare_broadcast_question(question_raw)
        explanation = _clean_explanation_for_broadcast(explanation_raw)
        now         = time.time()
        opts_json   = json.dumps(options, ensure_ascii=False)

        conn = _get_broadcast_conn()
        conn.execute(
            "INSERT OR IGNORE INTO broadcast_polls "
            "(poll_key, question, options_json, correct_idx, explanation, created_at) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            (poll_key, q_display, opts_json, correct_idx, explanation, now)
        )
        conn.commit()

        _turso_bg(lambda: turso_exec(
            "INSERT OR IGNORE INTO broadcast_polls "
            "(poll_key, question, options_json, correct_idx, explanation, created_at) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            (poll_key, q_display, opts_json, correct_idx, explanation, now)
        ), "save_broadcast_poll")
    except Exception as e:
        logger.error(f"save_broadcast_poll error: {e}")


async def load_broadcast_data_from_turso():
    """Startup-এ Turso থেকে broadcast_polls + broadcast_sent local SQLite-এ mirror করে
    (Render-এর মতো ephemeral disk-এ redeploy হলেও library আর history হারায় না)।"""
    client = await _get_turso()
    if client is None:
        return
    try:
        conn = _get_broadcast_conn()
        rs = await client.execute(
            "SELECT poll_key, question, options_json, correct_idx, explanation, created_at FROM broadcast_polls"
        )
        n = 0
        for row in rs.rows:
            conn.execute(
                "INSERT OR IGNORE INTO broadcast_polls "
                "(poll_key, question, options_json, correct_idx, explanation, created_at) "
                "VALUES (?, ?, ?, ?, ?, ?)",
                (row[0], row[1], row[2], int(row[3] or 0), row[4], float(row[5] or 0))
            )
            n += 1

        rs2 = await client.execute("SELECT user_id, poll_key, sent_at FROM broadcast_sent")
        m = 0
        for row in rs2.rows:
            conn.execute(
                "INSERT OR IGNORE INTO broadcast_sent (user_id, poll_key, sent_at) VALUES (?, ?, ?)",
                (int(row[0]), row[1], float(row[2] or 0))
            )
            m += 1
        conn.commit()
        logger.info(f"✅ Loaded {n} broadcast_polls + {m} broadcast_sent rows from Turso")
    except Exception as e:
        logger.error(f"Turso load broadcast data error: {e}")


def resanitize_broadcast_library() -> int:
    """
    Broadcast library-তে আগে থেকেই জমা থাকা poll-গুলোর question/explanation
    আবার নতুন (আরও শক্তিশালী) tag/link-cleaning logic দিয়ে re-process করে।
    কোনো পুরনো bug বা কোনো source bot/channel-এর অচেনা tag-format-এর কারণে
    যদি আগে কোনো branding/link miss হয়ে থেকে যায় (যেমন "[Arekta Quiz Bot]"
    টাইপ tag), সেটা এখন সরিয়ে দেওয়া হয় — bot restart হলেই startup-এ একবার
    চলে, idempotent (আগে থেকেই clean থাকা entry-তে কিছু পরিবর্তন হয় না)।
    """
    try:
        conn = _get_broadcast_conn()
        rows = conn.execute("SELECT poll_key, question, explanation FROM broadcast_polls").fetchall()
        tag_prefix = f"◊ {BOT_NAME} ◊\n"
        updated = 0
        for poll_key, question, explanation in rows:
            bare_q = question[len(tag_prefix):] if question.startswith(tag_prefix) else question
            new_question = _prepare_broadcast_question(bare_q)
            new_explanation = _clean_explanation_for_broadcast(explanation)
            if new_question != question or new_explanation != explanation:
                conn.execute(
                    "UPDATE broadcast_polls SET question = ?, explanation = ? WHERE poll_key = ?",
                    (new_question, new_explanation, poll_key)
                )
                _turso_bg(lambda pk=poll_key, q=new_question, e=new_explanation: turso_exec(
                    "UPDATE broadcast_polls SET question = ?, explanation = ? WHERE poll_key = ?",
                    (q, e, pk)
                ), "resanitize_broadcast")
                updated += 1
        conn.commit()
        if updated:
            logger.info(f"🧹 Re-sanitized {updated} broadcast_polls entries (পুরনো tag/link cleanup)")
        return updated
    except Exception as e:
        logger.error(f"resanitize_broadcast_library error: {e}")
        return 0


def record_broadcast_sent(user_id: int, poll_key: str) -> None:
    try:
        now = time.time()
        conn = _get_broadcast_conn()
        conn.execute(
            "INSERT OR IGNORE INTO broadcast_sent (user_id, poll_key, sent_at) VALUES (?, ?, ?)",
            (user_id, poll_key, now)
        )
        conn.commit()
        _turso_bg(lambda: turso_exec(
            "INSERT OR IGNORE INTO broadcast_sent (user_id, poll_key, sent_at) VALUES (?, ?, ?)",
            (user_id, poll_key, now)
        ), "save_broadcast_sent")
    except Exception as e:
        logger.error(f"record_broadcast_sent error: {e}")


def broadcast_library_size() -> int:
    try:
        conn = _get_broadcast_conn()
        return conn.execute("SELECT COUNT(*) FROM broadcast_polls").fetchone()[0]
    except Exception:
        return 0


def pick_unsent_poll_for_user(user_id: int) -> "dict | None":
    """user_id এখনো যেসব poll পায়নি, তার মধ্য থেকে randomly একটা বেছে দেয়। সব পেয়ে থাকলে None।"""
    try:
        conn = _get_broadcast_conn()
        sent_keys = {r[0] for r in conn.execute(
            "SELECT poll_key FROM broadcast_sent WHERE user_id = ?", (user_id,)
        ).fetchall()}
        rows = conn.execute(
            "SELECT poll_key, question, options_json, correct_idx, explanation FROM broadcast_polls"
        ).fetchall()
        candidates = [r for r in rows if r[0] not in sent_keys]
        if not candidates:
            return None
        row = random.choice(candidates)
        return {
            "poll_key":    row[0],
            "question":    row[1],
            "options":     json.loads(row[2]),
            "correct_idx": int(row[3]),
            "explanation": row[4],
        }
    except Exception as e:
        logger.error(f"pick_unsent_poll_for_user error: {e}")
        return None


# Broadcast poll DB শুরুতেই initialize করো
try:
    _get_broadcast_conn()
except Exception as e:
    logger.error(f"Broadcast poll DB init failed: {e}")


async def solve_poll(question: str, options: list, correct_idx: int = None) -> str:
    # ── Cache check first ──
    cached = cache_get(question, options)
    if cached:
        # Cache থেকে পেলে সরাসরি return — AI call নেই
        return cached

    opts_text = "\n".join([f"{chr(65+i)}) {o}" for i, o in enumerate(options)])

    poll_hint = ""
    if correct_idx is not None:
        hint_letter = chr(65 + correct_idx)
        hint_text   = options[correct_idx]
        poll_hint = (
            f"\n\n⚠️ নোট: এই পোলে quiz creator চিহ্নিত করেছেন যে সঠিক উত্তর হলো "
            f"{hint_letter}) {hint_text}। কিন্তু এই তথ্য সবসময় সঠিক নয় — ভুলও হতে পারে। "
            "তোমার নিজের বিষয়জ্ঞান দিয়ে যাচাই করো। যদি quiz creator-এর চিহ্নিত উত্তর ভুল হয়, "
            "তাহলে প্রকৃত সঠিক উত্তরটিই দাও (creator-এর ভুল উত্তর অনুসরণ করবে না)।"
        )

    prompt = (
        "তুমি একজন অভিজ্ঞ বাংলাদেশি শিক্ষক। নিচের MCQ প্রশ্নটি সমাধান করো।\n\n"
        f"প্রশ্ন: {question}\n\nঅপশন:\n{opts_text}{poll_hint}\n\n"
        "ফরম্যাটিং নিয়ম: এই চ্যাট এখন Telegram-এর rich message ফিচার ব্যবহার করে, তাই "
        "GFM markdown (**bold**, heading, list) ও LaTeX সরাসরি সুন্দরভাবে render হয়। "
        "সূত্র/গাণিতিক রাশি/রাসায়নিক বিক্রিয়া লিখতে সঠিক LaTeX ব্যবহার করবে — ছোট "
        "inline রাশির জন্য $...$ (যেমন $v = u + at$), বড় আলাদা সমীকরণের জন্য $$...$$, "
        "ভগ্নাংশে \\frac{লব}{হর}, বর্গমূলে \\sqrt{...}, সূচক/subscript-এ ^{} ও _{}, গ্রিক "
        "অক্ষরে \\Delta \\theta \\pi \\eta \\mu ইত্যাদি। যেখানে একাধিক মান/সূত্র/ধাপ "
        "তুলনা করা দরকার (যেমন \"❌ অন্য অপশন কেন ভুল\"), সেখানে GFM markdown table "
        "(| কলাম | কলাম |\\n|---|---|) ব্যবহার করবে — header row-এর ঠিক নিচে separator "
        "row বাধ্যতামূলক, প্রতিটা row আলাদা লাইনে। সহজ বাংলায় লিখবে।\n\n"
        "⚠️ সূত্র লেখার নিয়ম: লম্বা সমীকরণ (একাধিক bracket-group পাশাপাশি গুণ/ভাগ, যেমন "
        "$$\\sin(X)+\\sin(Y)=2\\sin\\left(\\frac{X+Y}{2}\\right)\\cos\\left(\\frac{X-Y}{2}\\right)$$"
        ", বা রাসায়নিক বিক্রিয়ার মতো লম্বা reaction) কখনো ভেঙে দুই লাইনে বসাবে না — ঠিক "
        "রাসায়নিক বিক্রিয়া লেখার মতোই পুরো সমীকরণটা একটামাত্র $$...$$ ব্লকে অক্ষত "
        "রাখবে (যেমন $$CH_3CH_2OH \\xrightarrow{Conc.\\ H_2SO_4/\\Delta} CH_2=CH_2 + "
        "H_2O$$)। rich message লম্বা $$...$$ ব্লককে নিজে থেকেই ডানে-বামে scroll করা "
        "যায় এমনভাবে দেখায় — ভেঙে দিলে সেই scroll সুবিধা নষ্ট হয়ে সূত্র অসম্পূর্ণ "
        "দেখাবে। শুধু \"📌 মূল সূত্র\"-এ একাধিক আলাদা identity/সূত্র থাকলে সেগুলোকে "
        "bullet-এর বদলে GFM markdown table-এ বসাবে (এক কলামে নাম/শর্ত, অন্য কলামে "
        "সূত্র) — কিন্তু কোনো একটামাত্র লম্বা সমীকরণকে কখনো টুকরো করবে না।\n\n"
        "⚠️ কখনোই নিজের পরিচয়/ভূমিকা নিয়ে কোনো বাক্য লিখবে না — যেমন \"আমি একজন "
        "অভিজ্ঞ বাংলাদেশি শিক্ষক হিসেবে...\" বা এই জাতীয় কোনো intro/branding লাইন। "
        "কোনো ভূমিকা ছাড়াই সরাসরি নিচের format দিয়ে উত্তর শুরু করবে।\n\n"
        "অত্যন্ত গুরুত্বপূর্ণ — Letter নির্ধারণের নিয়ম:\n"
        "উপরের অপশন তালিকায় যে ক্রমে অপশনগুলো দেওয়া আছে, সেই ক্রম অনুযায়ী letter:\n"
        f"{chr(10).join([f'  → {chr(65+i)}) = {o}' for i, o in enumerate(options)])}\n"
        "প্রকৃত সঠিক উত্তরের option text-টি উপরের তালিকায় যে letter-এ আছে, সেই letter-ই দাও। "
        "নিজে থেকে letter পরিবর্তন করবে না।\n\n"
        "নিচের format-এ উত্তর দাও:\n\n"
        "✅ সঠিক উত্তর: [লেটার]) [হুবহু অপশন টেক্সট]\n\n"
        "📌 মূল সূত্র:\n[প্রযোজ্য হলে সূত্র, নইলে এই অংশ বাদ দাও]\n\n"
        "📖 ধাপে ধাপে সমাধান:\n[step-by-step ব্যাখ্যা]\n\n"
        "❌ অন্য অপশন কেন ভুল:\n[প্রতিটি আলাদা লাইনে]"
    )

    result = await call_ai(prompt)
    result = re.sub(r'(?i)\bgemini\b', BOT_NAME, result)
    result = repair_markdown_tables(result)
    result = fix_answer_letter(result, options)
    result = format_section_headers(result)

    # ── Cache save (শুধু successful result) ──
    if result and "AI_FAILED" not in result:
        cache_set(question, options, result)

    return result


# ══════════════════════════════════════════════════════════════════
#  GLOBAL ERROR HANDLER
# ══════════════════════════════════════════════════════════════════
async def global_error_handler(update: object, ctx: ContextTypes.DEFAULT_TYPE):
    """
    সব uncaught exception ধরে log করে + admin-কে detailed report পাঠায়
    (কোন user, কী error, কখন — যাতে debug করে fix করা যায়)।
    শুধুমাত্র user-triggered (message/poll) events এ user-কে error message দেখায়।
    BadRequest, Forbidden, network errors এ user কে message দেওয়া হয় না —
    কারণ সেগুলো user-এর কারণে হয় না, আর message পাঠাতে গেলে loop হতে পারে।
    """
    import traceback, html as _html
    from telegram.error import BadRequest, Forbidden, NetworkError, TimedOut, RetryAfter

    error  = ctx.error
    tb_str = "".join(traceback.format_exception(type(error), error, error.__traceback__))

    is_api_error = isinstance(error, (BadRequest, Forbidden, NetworkError, TimedOut, RetryAfter))

    if is_api_error:
        logger.warning(f"Telegram API error (not user-triggered): {type(error).__name__}: {error}")
    else:
        logger.error("Unhandled exception while processing update:\n%s", tb_str)

    # ── শুধু real user message/poll update-এ user-কে reply করো (API error বাদে) ──
    user_notified = False
    if not is_api_error:
        try:
            if (
                isinstance(update, Update)
                and update.effective_message
                and (update.message or update.edited_message)  # actual user message
            ):
                await update.effective_message.reply_text(
                    "⚠️ *একটা সমস্যা হয়েছে।*\n\n"
                    "Poll টা আবার forward করো। সমস্যা থাকলে /feedback দিয়ে জানাও।",
                    parse_mode=ParseMode.MARKDOWN
                )
                user_notified = True
        except Exception as e:
            logger.warning(f"Error handler reply failed: {e}")

    # ── Admin-কে detailed error report পাঠানো ──
    try:
        user = chat = None
        if isinstance(update, Update):
            user = update.effective_user
            chat = update.effective_chat

        dhaka_tz = pytz.timezone("Asia/Dhaka")
        now_str  = datetime.now(dhaka_tz).strftime("%d %b %Y, %I:%M:%S %p")

        name   = _html.escape(user.full_name) if user else "Unknown"
        uname  = f"@{user.username}" if (user and user.username) else "N/A"
        uid    = user.id if user else "N/A"
        chat_t = chat.type if chat else "N/A"

        err_type = type(error).__name__
        err_msg  = str(error) or "(no message)"
        tb_tail  = tb_str[-1500:]   # সবচেয়ে relevant অংশ (শেষের দিকের lines)

        report = (
            f"🐞 <b>Bot Error Report</b>\n"
            f"▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬\n"
            f"👤 <b>User:</b> {name}\n"
            f"🔖 <b>Username:</b> {uname}\n"
            f"🆔 <b>User ID:</b> <code>{uid}</code>\n"
            f"💬 <b>Chat Type:</b> {chat_t}\n"
            f"🕐 <b>Time:</b> {now_str}\n"
            f"👀 <b>User Notified:</b> {'হ্যাঁ' if user_notified else 'না'}\n"
            f"▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬\n"
            f"⚠️ <b>Error:</b> {_html.escape(err_type)}: {_html.escape(err_msg)}\n\n"
            f"<b>Traceback (শেষ অংশ):</b>\n<code>{_html.escape(tb_tail)}</code>"
        )

        if len(report) > 4000:
            report = report[:3900] + "\n... (truncated)</code>"

        # ── Retry কারণ: এই send নিজেও TimedOut খেতে পারে (network flakiness) —
        # আগে একবার fail হলেই silently log হয়ে report টা হারিয়ে যেত।
        for attempt in range(3):
            try:
                await ctx.bot.send_message(chat_id=ADMIN_ID, text=report, parse_mode="HTML")
                break
            except (TimedOut, NetworkError) as e:
                if attempt == 2:
                    raise
                await asyncio.sleep(2 * (attempt + 1))
    except Exception as e:
        logger.error(f"Failed to send error report to admin: {e}")


# ══════════════════════════════════════════════════════════════════
#  POLL HANDLER
# ══════════════════════════════════════════════════════════════════
async def handle_poll(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    msg = update.message
    if not msg or not msg.poll:
        return

    if msg.chat.type == "channel":
        return

    if update.effective_user:
        await maybe_send_first_use_tip(msg, update.effective_user.id)

    poll     = msg.poll
    question = poll.question
    options  = [opt.text for opt in poll.options]

    # ── Debug log: forwarded vs self-sent poll-এর তথ্য Render logs-এ দেখার জন্য ──
    logger.info(
        "Poll received | forward_origin=%s | "
        "is_anonymous=%s | correct_option_id=%s | type=%s | user_id=%s",
        bool(msg.forward_origin),
        poll.is_anonymous, poll.correct_option_id, poll.type,
        update.effective_user.id if update.effective_user else None,
    )

    if not question or not options:
        return

    user = update.effective_user

    # ── If user is None (anonymous forward / hidden sender), try to get from message ──
    if user is None:
        # Cannot verify membership for anonymous user, treat as unverified
        await msg.reply_text(
            "⚠️ *Sender identity unknown.*\n\n"
            "Poll solve করতে হলে তোমার Telegram account দিয়ে directly পাঠাও।\n"
            "Anonymous বা hidden sender থেকে poll accept করা সম্ভব না।",
            parse_mode=ParseMode.MARKDOWN
        )
        return

    # ── Forward source detect ──
    # Default: user নিজে poll পাঠিয়েছে (forwarded না)
    source_link = "📩 User-sent (forwarded নয়)"
    source_name = "User-sent Poll"
    if msg.forward_origin:
        origin = msg.forward_origin
        if hasattr(origin, "chat") and origin.chat:
            chat = origin.chat
            if chat.username:
                source_link = f"https://t.me/{chat.username}"
                source_name = f"📢 {chat.title or chat.username}"
            else:
                source_name = f"🔒 {chat.title or 'Private Channel'}"
                source_link = "🔒 Private"
        elif hasattr(origin, "sender_chat") and origin.sender_chat:
            chat = origin.sender_chat
            if chat.username:
                source_link = f"https://t.me/{chat.username}"
                source_name = f"📢 {chat.title or chat.username}"
            else:
                source_name = f"🔒 {chat.title or 'Private Channel'}"
                source_link = "🔒 Private"

    # ── Real-time membership check ──
    if user and user.id != ADMIN_ID:
        still_member = await is_active_member(ctx.bot, user.id)
        if not still_member:
            keyboard = [
                [InlineKeyboardButton("📢 Live Exam TPC", url=GROUP_LINK)],
                [InlineKeyboardButton("✅ Joined — Verify করো", callback_data="verify_check")],
            ]
            await msg.reply_text(
                "⛔ *Access Denied!*\n\n"
                "Bot ব্যবহার করতে হলে আমাদের channel-এ *সক্রিয় সদস্য* থাকতে হবে।\n\n"
                "Channel ছেড়ে দিলে bot access বন্ধ হয়ে যায়।\n\n"
                "1️⃣ নিচের বাটন দিয়ে channel-এ join করো\n"
                "2️⃣ তারপর *Verify* চাপো",
                parse_mode=ParseMode.MARKDOWN,
                reply_markup=InlineKeyboardMarkup(keyboard)
            )
            return

    # ── Rate limit check (atomic reserve — race condition fix) ──
    if user and user.id != ADMIN_ID:
        allowed, reason, remaining_secs = await try_reserve_rate_limit(user.id)
        if not allowed:
            if reason == "cooldown":
                # Countdown animation দেখাও
                asyncio.create_task(send_cooldown_countdown(msg, remaining_secs))
            else:
                # Daily limit — referral bonus / admin extra সহ actual effective limit দেখাও
                effective_limit = get_effective_daily_limit(user.id)
                await msg.reply_text(
                    f"🚫 *আজকের limit শেষ!*\n\n"
                    f"তুমি আজ সর্বোচ্চ *{effective_limit}টি poll* solve করে ফেলেছো।\n\n"
                    f"🌙 মধ্যরাতে (Dhaka time) আবার reset হবে।\n"
                    f"কাল আবার এসো! 🌅",
                    parse_mode=ParseMode.MARKDOWN
                )
            return

    correct_idx = poll.correct_option_id

    status = await msg.reply_text("🔍 *Analyzing...*", parse_mode=ParseMode.MARKDOWN)
    result = await run_with_animation(status, solve_poll(question, options, correct_idx))

    # AI fail check
    ai_failed = (result == "AI_FAILED" or "AI_FAILED" in result)

    # ── AI completely failed — show retry message with button ──
    if ai_failed:
        if user and user.id != ADMIN_ID:
            refund_rate_limit(user.id)  # reserve করা slot ফেরত, "not counted" রাখার জন্য
        if user and REPORT_GROUP_ID:
            try:
                await send_poll_fail_report(ctx, user, msg, question)
            except Exception as e:
                logger.error(f"Poll fail report error: {e}")
        try:
            await status.delete()
        except Exception:
            pass
        retry_id = f"retry_{uuid.uuid4().hex[:8]}"
        retry_poll_data[retry_id] = {
            "question":    question,
            "options":     options,
            "correct_idx": correct_idx,
            "chat_id":     msg.chat_id,
            "user_id":     user.id if user else None,
            "source_link": source_link,
            "source_name": source_name,
            "_created_at": time.time(),
        }
        fail_text = (
            "🤖 *Synthesis Robot*\n\n"
            "🚧 *System Update in Progress*\n\n"
            "⚙️ We\'re currently improving the AI service to provide a better experience.\n\n"
            "⚠️ AI is temporarily unavailable. This attempt was *not counted* — please try again!"
        )
        keyboard = [[InlineKeyboardButton("🔄 Retry", callback_data=retry_id)]]
        await msg.reply_text(
            fail_text,
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
        return

    # ── Success ──
    # ── এই solve হওয়া poll টা (tag/link পরিষ্কার করে) broadcast library-তে জমা রাখো —
    #    non-blocking background task, user-এর উত্তর পাওয়া এতে দেরি হবে না ──
    try:
        _bc_correct_idx = correct_idx if (correct_idx is not None and 0 <= correct_idx < len(options)) \
            else _extract_correct_index(result, options)
        _bc_explanation = getattr(poll, "explanation", "") or ""
        asyncio.create_task(asyncio.to_thread(
            save_broadcast_poll, question, options, _bc_correct_idx, _bc_explanation
        ))
    except Exception as e:
        logger.error(f"broadcast library queue error: {e}")

    if user and user.id != ADMIN_ID:
        if user.id in registered_users:
            registered_users[user.id]["poll_count"] = registered_users[user.id].get("poll_count", 0) + 1
            registered_users[user.id]["last_active"] = time.time()
            _turso_bg(lambda: _save_user(user.id), "save_user")
        # NOTE: rate limit already reserved atomically before the AI call (try_reserve_rate_limit)
        record_poll_solved(user.id)
        update_user_streak(user.id)
        asyncio.create_task(notify_ready_after_cooldown(ctx.bot, msg.chat_id, user.id))
        entry    = rate_data.get(user.id, {})
        used     = entry.get("count", 1)
        eff_limit = get_effective_daily_limit(user.id)
        left     = eff_limit - used
        bar_fill = round((used / eff_limit) * 10) if eff_limit else 0
        bar      = "🟩" * bar_fill + "⬜" * (10 - bar_fill)
        footer   = f"\n\n📊 <b>Daily:</b> {bar} {used}/{eff_limit}"
        if left <= 3 and left > 0:
            footer += f"\n⚠️ <b>মাত্র {left}টি solve বাকি আজ!</b>"
        elif left == 0:
            footer += "\n🚫 <b>আজকের limit শেষ! কাল আবার এসো।</b>"
    else:
        if user and user.id in registered_users:
            registered_users[user.id]["poll_count"] = registered_users[user.id].get("poll_count", 0) + 1
            registered_users[user.id]["last_active"] = time.time()
            _turso_bg(lambda: _save_user(user.id), "save_user")
            record_poll_solved(user.id)
            update_user_streak(user.id)
        footer = ""

    await send_solved_answer(
        ctx.bot, msg.chat_id, status, question, options,
        result, footer_html=footer,
        reply_markup=_make_share_keyboard(ctx.bot.username, question, options, correct_idx, user.id if user else None)
    )

    if user and REPORT_GROUP_ID:
        try:
            await send_poll_report(ctx, user, msg, source_link, source_name)
        except Exception as e:
            logger.error(f"Poll report error: {e}")


async def notify_ready_after_cooldown(bot, chat_id: int, user_id: int):
    """
    Poll solve হওয়ার ঠিক COOLDOWN_SECS পর user কে একটা notification পাঠায়
    যে সে আবার poll solve করতে পারে। ততক্ষণে user যদি নিজেই আগে poll
    forward করে (early), তাহলে rate-limit cooldown path-এর countdown
    সেটা handle করবে, তাই এখানে শুধু rate_data চেক করে নিশ্চিত হই যে
    এই সময়ের মধ্যে user নতুন কোনো poll solve করেনি — করলে notification
    স্কিপ করি, যাতে duplicate/ভুল notification না যায়।
    """
    await asyncio.sleep(COOLDOWN_SECS)
    try:
        entry = rate_data.get(user_id)
        if entry:
            elapsed = time.time() - entry.get("last_time", 0.0)
            # যদি ইতিমধ্যে নতুন poll solve হয়ে যায় (elapsed কমে গেছে), skip করো
            if elapsed < COOLDOWN_SECS - 2:
                return
        await bot.send_message(
            chat_id=chat_id,
            text="✅ *এখন তুমি আবার Poll Solve করতে পারো!*\n\nPoll forward করো 👇",
            parse_mode=ParseMode.MARKDOWN
        )
    except Exception as e:
        logger.error(f"Ready-notify error for {user_id}: {e}")


# ══════════════════════════════════════════════════════════════════
#  START / HELP
# ══════════════════════════════════════════════════════════════════
START_TEXT = r"""👋 *Welcome to Synthesis Robot\!*

🤖 I'm an AI\-powered Quiz Solver\.
Forward any Telegram poll or quiz here —
I'll instantly give you the correct answer with a full explanation\!

▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬
📋 *Available Commands:*

▸ /start — বট শুরু করো
▸ /pollsolver — Poll solve করো
▸ /dailyusage — আজকের usage দেখো
▸ /myreport — তোমার streak ও usage report
▸ /getextralimit — Extra poll limit পাও
▸ /help — সব command দেখো
▸ /feedback — মতামত পাঠাও
▸ /qbank — Question Bank ও Library
▸ /unlimited\_exam — Unlimited Exam Practice করো

▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬
🚀 *How to get started:*
Just forward a poll to this chat\!
📸 Question\-এর ছবি পাঠাও অথবা টাইপ করে জিজ্ঞাসা করো অথবা poll forward করো\!
▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬
_Powered by Synthesis Robot 🧠_"""


# ══════════════════════════════════════════════════════════════════
#  HOW TO USE — animated HTML guide sent as a file
# ══════════════════════════════════════════════════════════════════
# পুরো guide এখন bot.py-এর ভিতরেই embed করা — আলাদা কোনো ফাইল লাগবে না।
HOW_TO_USE_HTML = r"""<!DOCTYPE html>
<html lang="bn">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Synthesis Robot — How to Use</title>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
<link href="https://fonts.googleapis.com/css2?family=Fraunces:opsz,wght@9..144,500;9..144,600;9..144,700&family=Inter:wght@400;500;600;700&family=Noto+Sans+Bengali:wght@400;500;600;700&display=swap" rel="stylesheet">
<style>
  :root{
    --ink:#0F1729;
    --panel:#172037;
    --panel-2:#1D2842;
    --hair:rgba(241,243,248,0.10);
    --text:#F1F3F8;
    --text-dim:#A7B0C9;
    --amber:#FFC145;
    --amber-soft:rgba(255,193,69,0.14);
    --mint:#4ADE80;
    --mint-soft:rgba(74,222,128,0.14);
    --coral:#FF7A6E;
    --radius:18px;
    --shadow: 0 20px 60px rgba(0,0,0,0.35);
  }
  [data-theme="light"]{
    --ink:#FAF7F0;
    --panel:#FFFFFF;
    --panel-2:#F3EFE4;
    --hair:rgba(27,34,51,0.10);
    --text:#1B2233;
    --text-dim:#5B6478;
    --amber:#E08E00;
    --amber-soft:rgba(224,142,0,0.12);
    --mint:#1C9A5B;
    --mint-soft:rgba(28,154,91,0.12);
    --coral:#D9483C;
    --shadow: 0 20px 50px rgba(27,34,51,0.10);
  }
  *{box-sizing:border-box; margin:0; padding:0;}
  html{scroll-behavior:smooth;}
  body{
    background:var(--ink);
    color:var(--text);
    font-family:'Inter','Noto Sans Bengali',sans-serif;
    transition:background .4s ease, color .4s ease;
    overflow-x:hidden;
    padding-bottom:40px;
  }
  .bn{font-family:'Noto Sans Bengali','Inter',sans-serif;}
  .display{font-family:'Fraunces','Noto Sans Bengali',serif; font-weight:600;}

  /* Ambient background texture */
  .noise{
    position:fixed; inset:0; pointer-events:none; z-index:0; opacity:.5;
    background-image: radial-gradient(circle at 15% 20%, var(--amber-soft), transparent 40%),
                       radial-gradient(circle at 85% 75%, var(--mint-soft), transparent 45%);
  }

  header{
    position:sticky; top:0; z-index:50;
    display:flex; align-items:center; justify-content:space-between;
    padding:16px 22px;
    backdrop-filter: blur(14px);
    background:color-mix(in srgb, var(--ink) 78%, transparent);
    border-bottom:1px solid var(--hair);
  }
  .brand{display:flex; align-items:center; gap:10px; font-weight:700; letter-spacing:.2px;}
  .brand .dot{width:9px; height:9px; border-radius:50%; background:var(--amber); box-shadow:0 0 0 5px var(--amber-soft);}
  .theme-toggle{
    width:42px; height:42px; border-radius:50%;
    border:1px solid var(--hair); background:var(--panel);
    display:flex; align-items:center; justify-content:center;
    cursor:pointer; font-size:18px; color:var(--text);
    transition: transform .3s ease;
  }
  .theme-toggle:active{transform:scale(.9) rotate(20deg);}

  main{position:relative; z-index:1; max-width:640px; margin:0 auto; padding:0 20px;}

  /* HERO */
  .hero{padding:56px 0 30px; text-align:center;}
  .eyebrow{
    display:inline-flex; align-items:center; gap:8px;
    font-size:12.5px; letter-spacing:.06em; text-transform:uppercase;
    color:var(--amber); background:var(--amber-soft);
    padding:6px 14px; border-radius:999px; font-weight:600;
    margin-bottom:22px;
  }
  h1.headline{
    font-size:clamp(30px, 8vw, 42px);
    line-height:1.28;
    letter-spacing:-0.01em;
  }
  .mark{
    position:relative; white-space:nowrap;
  }
  .mark::before{
    content:"";
    position:absolute; left:-4px; right:-4px; bottom:2px; top:38%;
    background:var(--amber);
    border-radius:3px;
    transform-origin:left;
    animation: sweep 1.1s .5s cubic-bezier(.65,0,.35,1) both;
    z-index:-1;
    opacity:.55;
  }
  [data-theme="light"] .mark::before{opacity:.45;}
  @keyframes sweep{from{transform:scaleX(0);} to{transform:scaleX(1);}}
  .sub{
    margin-top:18px; color:var(--text-dim); font-size:15.5px; line-height:1.7;
    max-width:480px; margin-left:auto; margin-right:auto;
  }

  .input-row{
    display:flex; justify-content:center; gap:12px; margin-top:34px; flex-wrap:wrap;
  }
  .chip{
    display:flex; align-items:center; gap:8px;
    background:var(--panel); border:1px solid var(--hair);
    padding:10px 16px; border-radius:999px; font-size:13.5px; font-weight:600;
    animation: floaty 4.5s ease-in-out infinite;
  }
  .chip:nth-child(2){animation-delay:.4s;}
  .chip:nth-child(3){animation-delay:.8s;}
  @keyframes floaty{0%,100%{transform:translateY(0);} 50%{transform:translateY(-6px);}}

  /* SECTION HEADERS */
  .section{padding:52px 0 8px;}
  .section-head{margin-bottom:26px;}
  .kicker{
    font-size:12px; text-transform:uppercase; letter-spacing:.08em;
    color:var(--text-dim); font-weight:700; margin-bottom:8px;
  }
  h2.section-title{font-size:26px; letter-spacing:-.01em;}

  /* STEPS */
  .steps{display:flex; flex-direction:column; gap:14px;}
  .step{
    background:var(--panel);
    border:1px solid var(--hair);
    border-radius:var(--radius);
    padding:20px;
    display:flex; gap:16px; align-items:flex-start;
    opacity:0; transform:translateY(18px);
    transition:opacity .6s ease, transform .6s ease;
  }
  .step.in{opacity:1; transform:translateY(0);}
  .step-icon{
    flex:0 0 auto; width:46px; height:46px; border-radius:13px;
    display:flex; align-items:center; justify-content:center; font-size:21px;
    background:var(--amber-soft);
  }
  .step:nth-child(2) .step-icon{background:var(--mint-soft);}
  .step:nth-child(3) .step-icon{background:color-mix(in srgb, var(--coral) 16%, transparent);}
  .step-body h3{font-size:16.5px; margin-bottom:5px; font-weight:700;}
  .step-body p{font-size:14px; color:var(--text-dim); line-height:1.65;}

  /* DEMO STRIP - animated bubble sequence */
  .demo{
    margin-top:16px; border-radius:16px; overflow:hidden;
    border:1px solid var(--hair); background:var(--panel-2);
    padding:18px; position:relative; min-height:96px;
  }
  .bubble{
    display:inline-flex; align-items:center; gap:8px;
    font-size:13px; padding:9px 13px; border-radius:12px;
    max-width:78%;
  }
  .bubble.user{background:var(--amber); color:#241a00; margin-left:auto; border-bottom-right-radius:3px;}
  [data-theme="light"] .bubble.user{color:#3a2900;}
  .bubble.bot{background:var(--mint-soft); border:1px solid var(--hair); border-bottom-left-radius:3px; color:var(--text);}
  .demo-row{display:flex; margin-bottom:9px; opacity:0; animation: pop .5s ease forwards;}
  .demo-row:nth-child(1){animation-delay: .1s;}
  .demo-row:nth-child(2){animation-delay: .9s;}
  @keyframes pop{from{opacity:0; transform:translateY(6px) scale(.97);} to{opacity:1; transform:translateY(0) scale(1);}}

  /* BUTTON GRID */
  .btn-grid{
    display:grid; grid-template-columns:1fr 1fr; gap:12px;
  }
  .btn-card{
    background:var(--panel); border:1px solid var(--hair); border-radius:14px;
    padding:16px 14px;
    opacity:0; transform:translateY(14px);
    transition:opacity .5s ease, transform .5s ease;
  }
  .btn-card.in{opacity:1; transform:translateY(0);}
  .btn-card .emoji{font-size:20px; margin-bottom:8px; display:block;}
  .btn-card b{font-size:13.5px; display:block; margin-bottom:4px;}
  .btn-card span{font-size:12.5px; color:var(--text-dim); line-height:1.5; display:block;}

  /* CHANNELS */
  .channel-list{display:flex; flex-direction:column; gap:10px;}
  .channel{
    display:flex; align-items:center; justify-content:space-between; gap:12px;
    background:var(--panel); border:1px solid var(--hair);
    padding:15px 16px; border-radius:14px;
    text-decoration:none; color:var(--text);
    transition: border-color .25s ease, transform .2s ease;
  }
  .channel:active{transform:scale(.98);}
  .channel:hover{border-color:var(--amber);}
  .channel .info{display:flex; align-items:center; gap:12px;}
  .channel .ico{
    width:38px; height:38px; border-radius:10px; background:var(--amber-soft);
    display:flex; align-items:center; justify-content:center; font-size:16px;
  }
  .channel .name{font-size:14px; font-weight:700;}
  .channel .tag{font-size:11.5px; color:var(--text-dim); margin-top:2px;}
  .join-pill{
    font-size:12px; font-weight:700; padding:7px 14px; border-radius:999px;
    background:var(--amber); color:#241a00; white-space:nowrap;
  }
  [data-theme="light"] .join-pill{color:#3a2900;}

  footer{
    margin-top:60px; text-align:center; padding:26px 20px 10px;
    color:var(--text-dim); font-size:12.5px; border-top:1px solid var(--hair);
  }
  footer .heart{color:var(--coral);}

  @media (prefers-reduced-motion: reduce){
    *{animation:none !important; transition:none !important;}
  }
</style>
</head>
<body data-theme="dark">
<div class="noise"></div>

<header>
  <div class="brand"><span class="dot"></span> Synthesis Robot</div>
  <button class="theme-toggle" id="themeBtn" aria-label="Toggle theme">🌙</button>
</header>

<main>

  <section class="hero">
    <div class="eyebrow bn">💡 ব্যবহারের গাইড</div>
    <h1 class="headline bn">প্রশ্ন পাঠাও, <span class="mark">উত্তর</span> পেয়ে যাও</h1>
    <p class="sub bn">Poll forward করো, ছবি তোলো, বা টাইপ করো — Synthesis Robot প্রতিটা প্রশ্নের সঠিক উত্তর আর ব্যাখ্যা সহ দিয়ে দেবে, সেকেন্ডের মধ্যেই।</p>
    <div class="input-row">
      <div class="chip">🧩 Poll Forward</div>
      <div class="chip">🖼 Image Question</div>
      <div class="chip">💬 Text Question</div>
    </div>
  </section>

  <section class="section" id="how">
    <div class="section-head">
      <div class="kicker bn">কীভাবে সমাধান করবে</div>
      <h2 class="section-title bn">তিনটা রাস্তা, একই বট</h2>
    </div>
    <div class="steps">
      <div class="step">
        <div class="step-icon">🧩</div>
        <div class="step-body">
          <h3 class="bn">যেকোনো Poll ফরওয়ার্ড করো</h3>
          <p class="bn">টেলিগ্রামের যেকোনো গ্রুপ বা চ্যানেল থেকে quiz poll পেলে সরাসরি এই বটে forward করে দাও — সঠিক অপশন আর ব্যাখ্যা মুহূর্তেই চলে আসবে।</p>
        </div>
      </div>
      <div class="step">
        <div class="step-icon">🖼</div>
        <div class="step-body">
          <h3 class="bn">প্রশ্নের ছবি তুলে পাঠাও</h3>
          <p class="bn">বই বা প্রশ্নপত্রের একটা ছবি তুলে পাঠিয়ে দাও। বট ছবি পড়ে প্রশ্ন বুঝে নিয়ে সমাধান করে দেবে।</p>
        </div>
      </div>
      <div class="step">
        <div class="step-icon">💬</div>
        <div class="step-body">
          <h3 class="bn">টাইপ করেও জিজ্ঞাসা করা যায়</h3>
          <p class="bn">প্রশ্নটা লিখে সরাসরি পাঠিয়ে দাও, চ্যাটে সাধারণভাবে মেসেজ করার মতোই। বট সাথে সাথে উত্তর দিয়ে দেবে।</p>
        </div>
      </div>
    </div>

    <div class="demo">
      <div class="demo-row"><div class="bubble user bn">📷 এই ছবিটার উত্তর কী?</div></div>
      <div class="demo-row"><div class="bubble bot bn">✅ সঠিক উত্তর: গ — সম্পূর্ণ ব্যাখ্যা সহ প্রস্তুত।</div></div>
    </div>
  </section>

  <section class="section" id="buttons">
    <div class="section-head">
      <div class="kicker bn">মেনু</div>
      <h2 class="section-title bn">বটের বাটনগুলো চেনো</h2>
    </div>
    <div class="btn-grid">
      <div class="btn-card"><span class="emoji">🛠</span><b class="bn">Solve Tools</b><span class="bn">Poll, Text, Image — সব সমাধান টুল একসাথে।</span></div>
      <div class="btn-card"><span class="emoji">📊</span><b class="bn">Daily Usage</b><span class="bn">আজকে কতগুলো ব্যবহার করেছ, কতগুলো বাকি — দেখে নাও।</span></div>
      <div class="btn-card"><span class="emoji">🎁</span><b class="bn">Get Extra Limits</b><span class="bn">বন্ধুদের রেফার করে বাড়তি লিমিট পেয়ে যাও।</span></div>
      <div class="btn-card"><span class="emoji">📝</span><b class="bn">Unlimited Exam</b><span class="bn">সীমাহীন প্র্যাকটিস মোডে পরীক্ষা দাও।</span></div>
      <div class="btn-card"><span class="emoji">📋</span><b class="bn">Help</b><span class="bn">সব কমান্ডের তালিকা এক জায়গায়।</span></div>
      <div class="btn-card"><span class="emoji">💬</span><b class="bn">Feedback</b><span class="bn">মতামত বা সমস্যা সরাসরি জানাও।</span></div>
    </div>
  </section>

  <section class="section" id="channels">
    <div class="section-head">
      <div class="kicker bn">কমিউনিটি</div>
      <h2 class="section-title bn">এই চ্যানেলগুলোতে জয়েন করো</h2>
    </div>
    <div class="channel-list">
      <a class="channel" href="https://t.me/PDFStudyBD" target="_blank" rel="noopener">
        <div class="info">
          <div class="ico">📚</div>
          <div>
            <div class="name">PDF Study BD</div>
            <div class="tag bn">ফ্রি স্টাডি ম্যাটেরিয়াল ও PDF</div>
          </div>
        </div>
        <span class="join-pill bn">Join</span>
      </a>
      <a class="channel" href="https://t.me/TheParaffinClassroom" target="_blank" rel="noopener">
        <div class="info">
          <div class="ico">🏫</div>
          <div>
            <div class="name">The Paraffin Classroom</div>
            <div class="tag bn">লাইভ ক্লাস ও আপডেট</div>
          </div>
        </div>
        <span class="join-pill bn">Join</span>
      </a>
      <a class="channel" href="https://t.me/AdmissionCampusNews" target="_blank" rel="noopener">
        <div class="info">
          <div class="ico">🎓</div>
          <div>
            <div class="name">Admission Campus News</div>
            <div class="tag bn">ভর্তি সংক্রান্ত সব খবর</div>
          </div>
        </div>
        <span class="join-pill bn">Join</span>
      </a>
      <a class="channel" href="https://t.me/TPCadmission" target="_blank" rel="noopener">
        <div class="info">
          <div class="ico">📢</div>
          <div>
            <div class="name">TPC Admission</div>
            <div class="tag bn">ভর্তি প্রস্তুতি ও নোটিশ</div>
          </div>
        </div>
        <span class="join-pill bn">Join</span>
      </a>
    </div>
  </section>

  <footer class="bn">
    Powered by <b>Synthesis Robot</b> 🧠 — তৈরি হয়েছে শিক্ষার্থীদের জন্য, <span class="heart">❤</span> দিয়ে।
  </footer>

</main>

<script>
  const body = document.body;
  const btn = document.getElementById('themeBtn');
  const prefersLight = window.matchMedia('(prefers-color-scheme: light)').matches;
  if(prefersLight){ body.setAttribute('data-theme','light'); btn.textContent='☀️'; }

  btn.addEventListener('click', () => {
    const isDark = body.getAttribute('data-theme') !== 'light';
    body.setAttribute('data-theme', isDark ? 'light' : 'dark');
    btn.textContent = isDark ? '☀️' : '🌙';
  });

  const io = new IntersectionObserver((entries) => {
    entries.forEach(e => { if(e.isIntersecting) e.target.classList.add('in'); });
  }, { threshold: 0.2 });
  document.querySelectorAll('.step, .btn-card').forEach(el => io.observe(el));
</script>

</body>
</html>
"""

HOW_TO_USE_BUTTON = InlineKeyboardButton("💡 How to Use", callback_data="how_to_use")
HOW_TO_USE_KEYBOARD = InlineKeyboardMarkup([[HOW_TO_USE_BUTTON]])


async def how_to_use_callback(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    try:
        await query.answer()
    except Exception as e:
        logger.error(f"how_to_use_callback answer() error: {e}")

    try:
        from io import BytesIO
        doc = BytesIO(HOW_TO_USE_HTML.encode("utf-8"))
        doc.name = "how_to_use.html"
        await ctx.bot.send_document(
            chat_id=query.message.chat_id,
            document=InputFile(doc, filename="how_to_use.html"),
            caption=(
                "💡 *Synthesis Robot — How to Use*\n\n"
                "ফাইলটা ডাউনলোড করে ব্রাউজারে খোলো — Poll, Text ও Image দিয়ে "
                "কিভাবে সমাধান করবে সব ধাপে ধাপে দেখানো আছে (Dark/Light mode সহ)।"
            ),
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=MAIN_MENU_KEYBOARD,
        )
    except Exception as e:
        logger.error(f"how_to_use_callback send_document error: {e}")
        try:
            await query.message.reply_text(
                "⚠️ Guide পাঠাতে সমস্যা হয়েছে। একটু পর আবার চেষ্টা করো।"
            )
        except Exception as e2:
            logger.error(f"how_to_use_callback error-notice failed: {e2}")


# ══════════════════════════════════════════════════════════════════
#  PERSISTENT KEYBOARD MENU
# ══════════════════════════════════════════════════════════════════
SOLVE_TOOLS_LABEL   = "🛠 Solve Tools"
SOLVE_POLL_LABEL    = "🧩 Poll Solve"
SOLVE_TEXT_LABEL    = "💬 Text Q&A"
SOLVE_IMAGE_LABEL   = "🖼 Image Q&A"
SOLVE_BACK_LABEL    = "🔙 Back"

MAIN_MENU_KEYBOARD = ReplyKeyboardMarkup(
    [
        [KeyboardButton(SOLVE_TOOLS_LABEL),     KeyboardButton("📊 Daily Usage")],
        [KeyboardButton("🎁 Get Extra Limits"), KeyboardButton("📝 Unlimited Exam")],
        [KeyboardButton("📋 Help"),             KeyboardButton("💬 Feedback")],
    ],
    resize_keyboard=True
)

# Solve Tools submenu — শুধু guide দেখানোর জন্য; button না চেপেও
# direct poll/text/image পাঠালে আগের মতোই solve হবে।
SOLVE_TOOLS_KEYBOARD = ReplyKeyboardMarkup(
    [
        [KeyboardButton(SOLVE_POLL_LABEL),  KeyboardButton(SOLVE_TEXT_LABEL)],
        [KeyboardButton(SOLVE_IMAGE_LABEL), KeyboardButton(SOLVE_BACK_LABEL)],
    ],
    resize_keyboard=True
)

# Button label → command name (used to route taps to the same handlers as the / commands)
MENU_BUTTON_COMMANDS = {
    "🧩 Poll Solver":  "pollsolver",
    "📊 Daily Usage":  "dailyusage",
    "📋 Help":          "help",
    "💬 Feedback":      "feedback",
}

# ── One-time tip (প্রথমবার poll forward / প্রথম text প্রশ্ন করলে) ──
first_tip_sent = set()

async def maybe_send_first_use_tip(msg, user_id: int):
    """নতুন user-এর প্রথম poll/text প্রশ্নে একবারই tip পাঠায়।"""
    try:
        if not msg or user_id in first_tip_sent:
            return
        first_tip_sent.add(user_id)
        await msg.reply_text(
            "💡 *Tip:* এখন থেকে সরাসরি ছবি তুলে পাঠিয়েও প্রশ্ন করতে পারবে 📷",
            parse_mode=ParseMode.MARKDOWN
        )
    except Exception as e:
        logger.error(f"first-use tip error: {e}")


async def _notify_admin_start(ctx, user, user_id, is_new):
    total      = len(registered_users)
    uname      = f"@{user.username}" if user.username else "N/A"
    status_lbl = "🟢 NEW USER" if is_new else "🔄 RETURNING USER"
    poll_count = registered_users.get(user_id, {}).get("poll_count", 0)
    report = (
        f"🔔 {status_lbl}\n"
        f"▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬\n\n"
        f"👤 Name: {user.full_name or 'Unknown'}\n"
        f"🔖 Username: {uname}\n"
        f"🆔 ID: {user_id}\n"
        f"🧩 Polls Solved: {poll_count}\n"
        f"🕐 Time: {get_dhaka_time()}\n\n"
        f"▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬\n"
        f"👥 Total Users: {total}"
    )
    try:
        await ctx.bot.send_message(chat_id=ADMIN_ID, text=report)
    except Exception as e:
        logger.error(f"Admin notify error: {e}")


async def _is_group_member(bot, user_id: int) -> bool:
    try:
        member = await bot.get_chat_member(chat_id=GROUP_CHAT_ID, user_id=user_id)
        return member.status in ("member", "administrator", "creator")
    except Exception as e:
        logger.error(f"getChatMember error: {e}")
        return False


async def _send_verify_prompt(target, edit=False):
    keyboard = [
        [InlineKeyboardButton("📢 Live Exam TPC", url=GROUP_LINK)],
        [InlineKeyboardButton("✅ I've Joined — Verify", callback_data="verify_check")],
    ]
    text = (
        "⛔ *Access Restricted!*\n\n"
        "Bot ব্যবহার করতে হলে আমাদের channel-এ join করতে হবে।\n\n"
        "1️⃣ *Live Exam TPC*-এ join করো\n"
        "2️⃣ তারপর *✅ Joined — Verify করো* চাপো"
    )
    markup = InlineKeyboardMarkup(keyboard)
    if edit:
        await target.edit_message_text(text, parse_mode=ParseMode.MARKDOWN,
                                       reply_markup=markup, disable_web_page_preview=True)
    else:
        await send_retrying(target, text, parse_mode=ParseMode.MARKDOWN,
                            reply_markup=markup, disable_web_page_preview=True)


async def start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    user    = update.effective_user
    user_id = user.id
    library_nav.pop(user_id, None)
    pending_library_build.pop(user_id, None)
    library_editor_active.pop(user_id, None)
    library_selected_node.pop(user_id, None)
    library_clipboard.pop(user_id, None)

    if user_id == ADMIN_ID:
        is_new = user_id not in registered_users
        existing_count = registered_users.get(user_id, {}).get("poll_count", 0)
        _upsert_user(user_id, {
            "name": user.full_name or "Unknown",
            "username": f"@{user.username}" if user.username else "N/A",
            "joined": registered_users.get(user_id, {}).get("joined", get_dhaka_time()),
            "poll_count": existing_count,
            "verified": True,
            "last_active": registered_users.get(user_id, {}).get("last_active", time.time()),
        })
        verified_users.add(user_id)
        await send_retrying(update.message, START_TEXT, parse_mode=ParseMode.MARKDOWN_V2,
                            reply_markup=HOW_TO_USE_KEYBOARD)
        return

    if user_id in verified_users:
        # Re-verify in real-time — channel থেকে leave করলে cache হলেও block হবে
        still_member = await is_active_member(ctx.bot, user_id)
        if not still_member:
            verified_users.discard(user_id)
            if user_id in registered_users:
                registered_users[user_id]["verified"] = False
                _turso_bg(lambda: _save_user(user_id), "save_user")
            await _send_verify_prompt(update.message)
            return
        is_new = user_id not in registered_users
        existing_count = registered_users.get(user_id, {}).get("poll_count", 0)
        _upsert_user(user_id, {
            "name": user.full_name or "Unknown",
            "username": f"@{user.username}" if user.username else "N/A",
            "joined": registered_users.get(user_id, {}).get("joined", get_dhaka_time()),
            "poll_count": existing_count,
            "verified": True,
            "last_active": registered_users.get(user_id, {}).get("last_active", time.time()),
        })
        await send_retrying(update.message, START_TEXT, parse_mode=ParseMode.MARKDOWN_V2,
                            reply_markup=HOW_TO_USE_KEYBOARD)
        if ADMIN_ID:
            await _notify_admin_start(ctx, user, user_id, is_new)
        return

    # Referral capture: t.me/BotUsername?start=<referral_code>  (নতুন: obfuscated code)
    # পুরনো shared link গুলোতে raw numeric user_id থাকতে পারে — backward-compat রাখা হলো।
    if ctx.args:
        try:
            param = ctx.args[0]
            referrer_id = int(param) if param.isdigit() else decode_referral_code(param)
            if referrer_id and referrer_id != user_id and referrer_id in registered_users:
                pending_referrals[user_id] = referrer_id
                _turso_bg(lambda: _save_pending_referral(user_id), "save_pending_referral")
        except (ValueError, IndexError):
            pass

    keyboard = [
        [InlineKeyboardButton("📢 Live Exam TPC", url=GROUP_LINK)],
        [InlineKeyboardButton("✅ I've Joined — Verify", callback_data="verify_check")],
    ]
    await send_retrying(
        update.message,
        "👋 *Welcome to Synthesis Robot!*\n\n"
        "To use this bot, you must join our channel first:\n\n"
        "📢 *Live Exam TPC*\n"
        f"`{GROUP_LINK}`\n\n"
        "1️⃣ Click the button below to join the group\n"
        "2️⃣ After joining, click *✅ I've Joined — Verify*\n\n"
        "▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬\n"
        "_Powered by Synthesis Robot 🧠_",
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=InlineKeyboardMarkup(keyboard),
        disable_web_page_preview=True
    )


async def _send_start_text(ctx: ContextTypes.DEFAULT_TYPE, user_id: int):
    """
    Welcome/start text পাঠানোর জন্য centralized helper —
    transient Telegram API failure হলে যাতে পুরো verify_callback crash না করে।
    """
    try:
        await ctx.bot.send_message(
            chat_id=user_id, text=START_TEXT,
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=HOW_TO_USE_KEYBOARD,
        )
    except Exception as e:
        logger.error(f"_send_start_text error (user {user_id}): {e}")


async def verify_callback(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    query   = update.callback_query
    try:
        await query.answer()
    except Exception as e:
        logger.error(f"verify_callback answer() error: {e}")
    user    = query.from_user
    user_id = user.id

    try:
        is_member = await _is_group_member(ctx.bot, user_id)
    except Exception as e:
        logger.error(f"verify_callback membership check error (user {user_id}): {e}")
        try:
            await query.edit_message_text(
                "⚠️ যাচাই করতে সমস্যা হচ্ছে। একটু পর আবার চেষ্টা করো।"
            )
        except Exception as e2:
            logger.error(f"verify_callback error-notice edit failed: {e2}")
        return

    if not is_member:
        keyboard = [
            [InlineKeyboardButton("📢 Live Exam TPC", url=GROUP_LINK)],
            [InlineKeyboardButton("✅ I've Joined — Verify", callback_data="verify_check")],
        ]
        try:
            await query.edit_message_text(
                "❌ *You haven't joined the channel yet!*\n\n"
                "Please join *Live Exam TPC* first,\n"
                "then click Verify again.\n\n"
                f"👉 {GROUP_LINK}",
                parse_mode=ParseMode.MARKDOWN,
                reply_markup=InlineKeyboardMarkup(keyboard),
                disable_web_page_preview=True
            )
        except Exception as e:
            logger.error(f"verify_callback not-member edit error (user {user_id}): {e}")
        return

    verified_users.add(user_id)
    is_new = user_id not in registered_users
    existing_count = registered_users.get(user_id, {}).get("poll_count", 0)
    _upsert_user(user_id, {
        "name": user.full_name or "Unknown",
        "username": f"@{user.username}" if user.username else "N/A",
        "joined": registered_users.get(user_id, {}).get("joined", get_dhaka_time()),
        "poll_count": existing_count,
        "verified": True,
        "last_active": registered_users.get(user_id, {}).get("last_active", time.time()),
    })

    try:
        await query.edit_message_text(
            "✅ *Verified successfully!*\n\nWelcome to Synthesis Robot! 🎉",
            parse_mode=ParseMode.MARKDOWN
        )
    except Exception as e:
        logger.error(f"verify_callback success-edit error (user {user_id}): {e}")

    await _send_start_text(ctx, user_id)

    if ADMIN_ID:
        try:
            await _notify_admin_start(ctx, user, user_id, is_new)
        except Exception as e:
            logger.error(f"verify_callback admin-notify error (user {user_id}): {e}")

    # Referral credit: শুধু প্রথমবার verify হলেই count হবে (re-verify এ duplicate credit না)।
    # এটা কখনো expire হয় না — referrer ৫ দিন পরে friend join করলেও বোনাস পাবে,
    # যতদিন পর্যন্ত না user verify হয়ে যাচ্ছে ততদিন pending_referrals-এ থাকবে।
    if is_new and user_id in pending_referrals:
        referrer_id = pending_referrals.pop(user_id)
        _turso_bg(lambda: _delete_pending_referral(user_id), "delete_pending_referral")
        add_referral_bonus(referrer_id)
        new_total = get_effective_daily_limit(referrer_id)
        try:
            await ctx.bot.send_message(
                chat_id=referrer_id,
                text=(
                    "🎉 *Referral সফল হয়েছে!*\n\n"
                    f"তোমার আমন্ত্রণে *{user.full_name or 'একজন user'}* bot-এ join করেছে!\n\n"
                    f"🎁 আজকের জন্য *+{REFERRAL_BONUS_PER_INVITE} poll* বোনাস পেয়েছো।\n"
                    f"📦 আজকের total limit: *{new_total}টি*\n\n"
                    "_/dailyusage লিখে চেক করো।_"
                ),
                parse_mode=ParseMode.MARKDOWN
            )
        except Exception as e:
            logger.error(f"Referral notify error: {e}")
    elif user_id in pending_referrals:
        pending_referrals.pop(user_id, None)
        _turso_bg(lambda: _delete_pending_referral(user_id), "delete_pending_referral")


# ══════════════════════════════════════════════════════════════════
#  /help COMMAND
# ══════════════════════════════════════════════════════════════════
async def help_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    user     = update.effective_user
    is_admin = user.id == ADMIN_ID
    if not is_admin and not await is_active_member(ctx.bot, user.id):
        await _send_verify_prompt(update.message)
        return

    user_cmds = (
        "🤖 *Synthesis Robot — Commands*\n"
        "▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬\n\n"
        "👤 *User Commands:*\n\n"
        "▸ /start — Start the bot\n"
        "▸ /pollsolver — Poll solve করো\n"
        "▸ /help — View all commands\n"
        "▸ /dailyusage — আজকের usage দেখো\n"
        "▸ /myreport — তোমার streak ও usage report\n"
        "▸ /getextralimit — Extra poll limit পাও\n"
        "▸ /feedback — মতামত পাঠাও\n"
        "▸ /contact — Contact the developer\n"
        "▸ /qbank — Question Bank ও Library\n"
        "▸ /unlimited\\_exam — Unlimited Exam Practice করো\n\n"
        "🧠 *কীভাবে প্রশ্ন করবে:*\n\n"
        "🧩 Poll → forward করো\n"
        "💬 Text প্রশ্ন → সরাসরি লিখো\n"
        "🖼 Image প্রশ্ন → ছবি পাঠাও\n\n"
        "📊 *Poll Solving:*\n\n"
        "▸ Forward any Telegram poll or quiz\n"
        "   to this chat —\n"
        "   AI will solve it instantly\\!\n\n"
        "🖼 *Image \\(ছবি\\) Question:*\n\n"
        "▸ প্রশ্নের ছবি পাঠাও, তারপর\n"
        "   *ঐ ছবির reply*\\-তে প্রশ্ন লিখো\n"
        "   \\(যেমন: 64 no\\. Ans dao\\)\n"
        "▸ দৈনিক ১০টি ছবি\\-প্রশ্ন, মাঝে ২ মিনিট গ্যাপ\n\n"
        "▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬\n"
        "_Powered by Synthesis Robot 🧠_"
    )

    admin_cmds = (
        "🤖 *Synthesis Robot — Commands*\n"
        "▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬\n\n"
        "👤 *User Commands:*\n\n"
        "▸ /start — Start the bot\n"
        "▸ /pollsolver — Poll solve করো\n"
        "▸ /help — View all commands\n"
        "▸ /dailyusage — আজকের usage দেখো\n"
        "▸ /myreport — তোমার streak ও usage report\n"
        "▸ /getextralimit — Extra poll limit পাও\n"
        "▸ /feedback — মতামত পাঠাও\n"
        "▸ /contact — Contact the developer\n"
        "▸ /qbank — Question Bank ও Library\n"
        "▸ /unlimited\\_exam — Unlimited Exam Practice করো\n\n"
        "🧠 *কীভাবে প্রশ্ন করবে:*\n\n"
        "🧩 Poll → forward করো\n"
        "💬 Text প্রশ্ন → সরাসরি লিখো\n"
        "🖼 Image প্রশ্ন → ছবি পাঠাও\n\n"
        "📊 *Poll Solving:*\n\n"
        "▸ Forward any Telegram poll or quiz\n"
        "   to this chat —\n"
        "   AI will solve it instantly\\!\n\n"
        "🖼 *Image \\(ছবি\\) Question:*\n\n"
        "▸ প্রশ্নের ছবি পাঠাও, তারপর\n"
        "   *ঐ ছবির reply*\\-তে প্রশ্ন লিখো\n"
        "   \\(যেমন: 64 no\\. Ans dao\\)\n"
        "▸ দৈনিক ১০টি ছবি\\-প্রশ্ন, মাঝে ২ মিনিট গ্যাপ\n\n"
        "▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬\n"
        "🔐 *Admin Commands:*\n\n"
        "▸ /stats — View bot analytics\n"
        "▸ /apistatus — AI provider/API key status\n"
        "▸ /users — View detailed user list\n"
        "▸ /userdata — Top 10 active users \\(poll/text/OCR\\)\n"
        "▸ /broadcast — Send a message to all users\n"
        "▸ /setreportgroup — Report group set করো\n"
        "▸ /addlimit — user কে আজকের জন্য extra limit দাও \\(Poll/Text/OCR\\)\n"
        "▸ /cancel — Cancel current operation\n\n"
        "▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬\n"
        "_Powered by Synthesis Robot 🧠_"
    )

    text = admin_cmds if is_admin else user_cmds
    try:
        await update.message.reply_text(text, parse_mode=ParseMode.MARKDOWN_V2,
                                        reply_markup=MAIN_MENU_KEYBOARD)
    except BadRequest as e:
        # MarkdownV2 parsing কোনো কারণে fail করলেও (যেমন কোনো character escape
        # করতে ভুলে গেলে) যাতে /help পুরোপুরি ভেঙে না পড়ে — plain text fallback।
        logger.error(f"help_cmd MarkdownV2 parse error: {e}")
        plain_text = text.replace("\\", "").replace("*", "").replace("_", "")
        await update.message.reply_text(plain_text, reply_markup=MAIN_MENU_KEYBOARD)

    # ── Contact Admin button — persistent bottom keyboard-এর পাশাপাশি এক message-এ
    #    দুটো ভিন্ন ধরনের reply_markup পাঠানো যায় না, তাই ছোট একটা follow-up
    #    message-এ inline button হিসেবে পাঠানো হচ্ছে ──
    contact_keyboard = InlineKeyboardMarkup([
        [HOW_TO_USE_BUTTON],
        [InlineKeyboardButton("📩 Contact Admin", url="https://t.me/RanaSynth")],
    ])
    await update.message.reply_text(
        "কোনো সমস্যা হলে বা সরাসরি কথা বলতে চাইলে নিচের বাটনে চাপো 👇",
        reply_markup=contact_keyboard
    )


# ══════════════════════════════════════════════════════════════════
#  /stats COMMAND (Admin only)
# ══════════════════════════════════════════════════════════════════
async def stats_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    if user.id != ADMIN_ID:
        await update.message.reply_text("❌ This command is for admins only.")
        return

    today       = get_dhaka_date()
    total_users = len(registered_users)

    today_data   = daily_stats.get(today, {})
    today_polls  = today_data.get("polls_solved", 0)
    today_active = len(today_data.get("active_users", set()))

    all_time_polls = sum(info.get("poll_count", 0) for info in registered_users.values())

    top_users = sorted(
        registered_users.items(),
        key=lambda x: x[1].get("poll_count", 0),
        reverse=True
    )[:5]

    import html as _html
    top_text = ""
    for i, (uid, info) in enumerate(top_users, 1):
        medals = ["🥇", "🥈", "🥉", "4️⃣", "5️⃣"]
        name   = _html.escape(info.get("name", "Unknown"))
        uname  = _html.escape(info.get("username", "N/A"))
        pc     = info.get("poll_count", 0)
        top_text += f"{medals[i-1]} {name} ({uname}) — {pc} polls\n"

    if not top_text:
        top_text = "No data yet."


    import datetime as _dt
    dhaka_tz     = pytz.timezone("Asia/Dhaka")
    history_lines = ""
    MAX_BAR = 15
    for i in range(6, -1, -1):
        day_dt    = datetime.now(dhaka_tz) - _dt.timedelta(days=i)
        day       = day_dt.strftime("%Y-%m-%d")
        day_label = day_dt.strftime("%d %b")
        polls     = daily_stats.get(day, {}).get("polls_solved", 0)
        filled    = min(polls, MAX_BAR)
        bar       = "█" * filled + "░" * (MAX_BAR - filled)
        history_lines += f"<code>{day_label}  {bar}  {polls}</code>\n"

    # Cache stats
    cs = cache_stats()
    cache_entries = cs["total_entries"]
    cache_hits    = cs["total_hits"]
    saved_calls   = cache_hits
    cache_line = (
        f"💾 <b>Cache:</b> {cache_entries} প্রশ্ন সংরক্ষিত | "
        f"🎯 {cache_hits} বার AI বাঁচানো হয়েছে\n"
    )

    report = (
        f"📊 <b>Bot Analytics</b>\n"
        f"▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬\n\n"
        f"👥 <b>Total Users:</b> {total_users}\n"
        f"🧩 <b>All-time Polls Solved:</b> {all_time_polls}\n\n"
        f"📅 <b>Today ({today}):</b>\n"
        f"┣ Polls Solved: {today_polls}\n"
        f"┗ Active Users: {today_active}\n\n"
        f"▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬\n"
        f"{cache_line}"
        f"▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬\n"
        f"🏆 <b>Top Users:</b>\n{top_text}\n"
        f"▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬\n"
        f"📈 <b>Last 7 Days:</b>\n{history_lines}"
        f"▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬\n"
        f"🕐 {get_dhaka_time()}\n\n"
        f"<i>AI provider/API key status দেখতে /apistatus ব্যবহার করো</i>"
    )
    await update.message.reply_text(report, parse_mode="HTML")


# ════════════════════════════════════════════════════════════════
#  /apistatus COMMAND (Admin only) — গুলো সব AI provider/API key এর live status (এোটা সরাসরি /stats থেকে সরিয়ে নেওয়া হয়েছে)
# ════════════════════════════════════════════════════════════════
async def apistatus_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    if user.id != ADMIN_ID:
        await update.message.reply_text("\u274c This command is for admins only.")
        return

    limited_labels = []
    error_labels = []

    # টেবিলের প্রতিটা row: (status_emoji, label, s_f_rate, cycle_or_note, poll, text, ocr)
    table_rows = []

    dhaka_tz_stats = pytz.timezone("Asia/Dhaka")
    pacific_tz_stats = pytz.timezone("US/Pacific")
    for p in API_POOL:
        label = p["label"]
        s = provider_stats.get(p["stat_id"])
        if s:
            # rotation loop প্রতিটা key পর্যন্ত না পৌঁছালে stale "limited" ফ্ল্যাগ থেকে
            # যেতে পারে (যেমন Gemini#1 কাজ করতে থাকলে rotation কখনো #2-14 পর্যন্ত
            # যায়ই না) — তাই display করার আগে নিজে থেকেই reset time চেক করে
            # real-time status বের করা হচ্ছে। এটা calendar-date ভিত্তিক daily
            # counter reset ও করে দেয় (Google-এর real RPD window অনুযায়ী), তাই
            # total/rate অবশ্যই এর *পরে* বের করতে হবে, নাহলে stale (reset-হওয়ার-
            # আগের) সংখ্যা থেকে ভুল rate দেখানো হবে।
            still_limited = _is_still_limited(s)
            total = s["success"] + s["fail"]
            rate = (s["success"] / total * 100) if total else 0

            if still_limited:
                limited_labels.append(label)
                limited_at = s.get("limited_at", 0)
                solved_this_cycle = s.get("last_cycle_solved", 0)
                if limited_at:
                    pac_now = datetime.fromtimestamp(limited_at, pacific_tz_stats)
                    pac_next_midnight = (pac_now + _dt.timedelta(days=1)).replace(
                        hour=0, minute=0, second=0, microsecond=0
                    )
                    reset_dhaka = pac_next_midnight.astimezone(dhaka_tz_stats)
                    now_dhaka = datetime.now(dhaka_tz_stats)
                    reset_str = (
                        "shortly" if reset_dhaka <= now_dhaka
                        else reset_dhaka.strftime("%I:%M %p, %d %b")
                    )
                else:
                    reset_str = "unknown"
                status = "🚫 LIMIT"
                sf_note = f"resets ~{reset_str} · {solved_this_cycle} solved"
            elif s["fail"] > 0 and s["success"] == 0:
                error_labels.append(label)
                status = "💀 DOWN"
                sf_note = (s.get("last_error") or "")[:60]
            elif rate < 50:
                error_labels.append(label)
                status = "⚠️ UNSTABLE"
                sf_note = (s.get("last_error") or "")[:60]
            else:
                status = "✅ OK"
                cd_until = _key_cooldowns.get(p["key"], 0)
                sf_note = f"⏳ cooling {int(cd_until - time.time())}s" if cd_until > time.time() else ""

            txt_ok  = int(s.get("text_success", 0))
            ocr_ok  = int(s.get("ocr_success", 0))
            poll_ok = int(s.get("poll_success", max(0, s["success"] - txt_ok - ocr_ok)))
            txt_bad  = int(s.get("text_fail", 0))
            ocr_bad  = int(s.get("ocr_fail", 0))
            poll_bad = int(s.get("poll_fail", max(0, s["fail"] - txt_bad - ocr_bad)))

            table_rows.append({
                "label": label, "status": status,
                "sf": f"{s['success']}/{s['fail']} ({rate:.0f}%)",
                "cycle": s.get("solved_since_reset", 0),
                "poll": f"{poll_ok}✅/{poll_bad}❌",
                "text": f"{txt_ok}✅/{txt_bad}❌",
                "ocr": f"{ocr_ok}✅/{ocr_bad}❌",
                "note": sf_note,
            })
        else:
            table_rows.append({
                "label": label, "status": "➖ unused", "sf": "—", "cycle": "—",
                "poll": "—", "text": "—", "ocr": "—", "note": "no calls yet",
            })

    summary_parts = []
    if limited_labels:
        summary_parts.append(f"🚫 **Limit Exceeded ({len(limited_labels)}):** {', '.join(limited_labels)}")
    if error_labels:
        summary_parts.append(f"💀 **Error/Down ({len(error_labels)}):** {', '.join(error_labels)}")
    if not summary_parts:
        summary_parts.append("✅ এখন কোনো key-এ সমস্যা নেই — সব ঠিক আছে!")
    limit_summary = "\n\n".join(summary_parts)

    # ── Markdown table (Telegram Bot API 10.1 rich message এ native render হবে) ──
    table_lines = [
        "| Key | Status | Success/Fail | Cycle | Poll | Text | OCR |",
        "| --- | --- | --- | --- | --- | --- | --- |",
    ]
    for r in table_rows:
        note = f" _{r['note']}_" if r["note"] else ""
        table_lines.append(
            f"| {r['label']} | {r['status']} | {r['sf']} | {r['cycle']} | {r['poll']} | {r['text']} | {r['ocr']}{note} |"
        )
    table_md = "\n".join(table_lines)

    # "Total Usage" — lifetime_usage_totals থেকে (প্রতিদিনের per-key reset-এর
    # বাইরে থাকা persistent counter, তাই key reset হলেও এই সংখ্যা কমে না)
    report_md = (
        f"# 🤖 AI Provider Status ({len(API_POOL)} active)\n\n"
        f"{table_md}\n\n"
        f"## 📊 Total Usage (all keys, lifetime)\n"
        f"- 📝 Poll solved: **{lifetime_usage_totals.get('poll', 0)}**\n"
        f"- 💬 Text Q&A answered: **{lifetime_usage_totals.get('text', 0)}**\n"
        f"- 🖼 OCR (image) solved: **{lifetime_usage_totals.get('ocr', 0)}**\n\n"
        f"{limit_summary}\n\n"
        f"🕐 {get_dhaka_time()}"
    )

    chat_id = update.effective_chat.id
    sent = await send_rich_message(chat_id, report_md, reply_to_message_id=update.message.message_id)
    if sent is None:
        # পুরনো client / rich message ব্যর্থ হলে plain HTML-এ fallback
        fallback_lines = [f"🤖 <b>AI Provider Status ({len(API_POOL)} active)</b>\n"]
        for r in table_rows:
            fallback_lines.append(
                f"{r['status']} <b>{r['label']}</b>: {r['sf']} | cycle:{r['cycle']}\n"
                f"   ↳ Poll:{r['poll']} Text:{r['text']} OCR:{r['ocr']}"
                + (f" — {r['note']}" if r["note"] else "")
            )
        fallback_lines.append(
            f"\n📊 <b>Total Usage (lifetime)</b>\n"
            f"Poll: <b>{lifetime_usage_totals.get('poll', 0)}</b> | "
            f"Text: <b>{lifetime_usage_totals.get('text', 0)}</b> | "
            f"OCR: <b>{lifetime_usage_totals.get('ocr', 0)}</b>\n"
        )
        fallback_lines.append(clean_text(tables_to_plain_text(limit_summary)))
        fallback_lines.append(f"\n🕐 {get_dhaka_time()}")
        await update.message.reply_text("\n".join(fallback_lines), parse_mode="HTML")



# ══════════════════════════════════════════════════════════════════
#  /users COMMAND (Admin only)
# ══════════════════════════════════════════════════════════════════
def _bn_num(n) -> str:
    """ASCII সংখ্যাকে বাংলা সংখ্যায় রূপান্তর করে (শুধু ডিসপ্লে টেক্সটের জন্য, CSV-তে না)।"""
    bn_digits = "০১২৩৪৫৬৭৮৯"
    return "".join(bn_digits[int(ch)] if ch.isdigit() else ch for ch in str(n))


async def users_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    if user.id != ADMIN_ID:
        keyboard = [[InlineKeyboardButton("📩 Contact Developer", url="https://t.me/RanaSynth")]]
        await update.message.reply_text(
            "❌ You are not authorized to use this bot.\n\n"
            "💬 Please contact the developer.\n"
            "📩 Contact: @RanaSynth",
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
        return

    if not registered_users:
        await update.message.reply_text("📭 No users registered yet.")
        return

    import csv, io
    from io import BytesIO
    from datetime import timedelta

    dhaka_tz   = pytz.timezone("Asia/Dhaka")
    now_dhaka  = datetime.now(dhaka_tz)
    today_date = now_dhaka.date()
    week_start = today_date - timedelta(days=6)

    total = len(registered_users)

    # ── প্রতিটি user-এর joined date parse করে today/this-week count বের করা ──
    joined_today = 0
    joined_week  = 0
    parsed_rows  = []   # (uid, info, parsed_datetime_or_None)

    for uid, info in registered_users.items():
        joined_str = str(info.get("joined", "")).strip()
        dt = None
        if joined_str:
            try:
                dt = datetime.strptime(joined_str, "%d %b %Y, %I:%M %p")
            except ValueError:
                dt = None
        if dt:
            d = dt.date()
            if d == today_date:
                joined_today += 1
            if d >= week_start:
                joined_week += 1
        parsed_rows.append((uid, info, dt))

    # ── সব user নিয়ে পূর্ণ CSV তৈরি (5000/10000+ যত-ই হোক, সবাই থাকবে) ──
    csv_buf = io.StringIO()
    writer  = csv.writer(csv_buf)
    writer.writerow(["#", "Name", "Username", "User ID", "First Seen (Dhaka)"])

    # সবচেয়ে নতুন user আগে দেখানোর জন্য sort (parse না হলে শেষে থাকবে)
    ordered = sorted(
        parsed_rows,
        key=lambda r: r[2] if r[2] else datetime.min,
        reverse=True,
    )

    for i, (uid, info, _dt) in enumerate(ordered, 1):
        writer.writerow([
            i,
            str(info.get("name", "Unknown")),
            str(info.get("username", "N/A")),
            uid,
            str(info.get("joined", "")),
        ])

    csv_bytes  = csv_buf.getvalue().encode("utf-8-sig")   # BOM, Excel-এ বাংলা ঠিকমতো দেখাবে
    file_stamp = now_dhaka.strftime("%Y%m%d_%H%M")
    filename   = f"bot_users_{total}_{file_stamp}.csv"

    doc      = BytesIO(csv_bytes)
    doc.name = filename

    # ── সংক্ষিপ্ত সামারি (caption) — সর্বশেষ 5 জন দেখানো হবে ──
    latest_lines = []
    for uid, info, _dt in ordered[:5]:
        name  = str(info.get("name", "Unknown"))
        uname = str(info.get("username", "N/A"))
        latest_lines.append(f"• {name} (@{uname.lstrip('@')}) — {uid}")

    caption = (
        "👥 <b>সকল Bot Users</b>\n"
        "▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬\n\n"
        f"📊 <b>Total:</b> {_bn_num(total)} জন\n"
        f"🆕 <b>আজকে যোগ হয়েছে:</b> {_bn_num(joined_today)} জন\n"
        f"📅 <b>এই সপ্তাহে যোগ হয়েছে:</b> {_bn_num(joined_week)} জন\n\n"
        "▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬\n\n"
        f"🕐 <b>সর্বশেষ {_bn_num(len(latest_lines))} জন:</b>\n"
        + "\n".join(latest_lines) + "\n\n"
        "▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬\n\n"
        f"📎 সম্পূর্ণ {_bn_num(total)} জনের list CSV file এ আছে — Excel/Sheets এ খুলে দেখো।"
    )

    if len(caption) > 1024:
        caption = caption[:1000] + "…\n(পুরো list CSV ফাইলে আছে)"

    await update.message.reply_document(
        document=doc,
        filename=filename,
        caption=caption,
        parse_mode="HTML",
    )


# ══════════════════════════════════════════════════════════════════
#  Full User-Data Dashboard (HTML) — /userdata command এর সাথে যুক্ত।
#  সব user-এর name/username/user_id/joined/poll/text/OCR/verified/
#  last_active সহ একটা সুন্দর dark⇄light dashboard তৈরি করে।
# ══════════════════════════════════════════════════════════════════
_USERDATA_HTML_TEMPLATE = """<!DOCTYPE html>
<html lang="bn">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>__BOT_NAME__ — User Data Dashboard</title>
<style>
  :root{
    --bg:#f4f5fb; --bg-soft:#ffffff; --card:#ffffff; --text:#161522; --muted:#6b7086;
    --border:#e6e7f0; --accent:#6c5ce7; --accent2:#00cec9; --accent-grad:linear-gradient(135deg,#6c5ce7,#00cec9);
    --row-hover:#f1f1fb; --shadow:0 4px 18px rgba(30,30,60,.06); --danger:#e74c3c; --ok:#00b894;
    --gold:#f5b301; --silver:#b0b3bd; --bronze:#c97b3d;
  }
  [data-theme="dark"]{
    --bg:#0f1017; --bg-soft:#151622; --card:#181a26; --text:#eef0fb; --muted:#9295ac;
    --border:#262838; --accent:#8b7bff; --accent2:#28e0d9; --accent-grad:linear-gradient(135deg,#8b7bff,#28e0d9);
    --row-hover:#1f2130; --shadow:0 4px 18px rgba(0,0,0,.35); --danger:#ff6b6b; --ok:#2ecc94;
  }
  *{box-sizing:border-box;}
  body{
    margin:0; font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,"Noto Sans Bengali",Arial,sans-serif;
    background:var(--bg); color:var(--text); transition:background .25s ease,color .25s ease;
  }
  .topbar{
    position:sticky; top:0; z-index:20; background:var(--bg-soft); border-bottom:1px solid var(--border);
    padding:14px 22px; display:flex; align-items:center; justify-content:space-between; gap:16px; flex-wrap:wrap;
    box-shadow:var(--shadow);
  }
  .brand{display:flex; align-items:center; gap:10px;}
  .brand .logo{
    width:38px; height:38px; border-radius:11px; background:var(--accent-grad);
    display:flex; align-items:center; justify-content:center; font-size:19px;
  }
  .brand h1{font-size:16px; margin:0; font-weight:700;}
  .brand span{font-size:11.5px; color:var(--muted);}
  .controls{display:flex; align-items:center; gap:10px; flex-wrap:wrap;}
  .search-box{
    display:flex; align-items:center; gap:8px; background:var(--card); border:1px solid var(--border);
    border-radius:10px; padding:8px 12px; min-width:230px;
  }
  .search-box input{
    border:none; outline:none; background:transparent; color:var(--text); font-size:13.5px; width:100%;
  }
  .theme-btn{
    border:1px solid var(--border); background:var(--card); color:var(--text); border-radius:10px;
    padding:8px 13px; cursor:pointer; font-size:13.5px; display:flex; align-items:center; gap:6px;
    transition:.15s ease;
  }
  .theme-btn:hover{border-color:var(--accent);}
  .container{padding:22px; max-width:1400px; margin:0 auto;}
  .stats-grid{
    display:grid; grid-template-columns:repeat(auto-fit,minmax(150px,1fr)); gap:14px; margin-bottom:22px;
  }
  .stat-card{
    background:var(--card); border:1px solid var(--border); border-radius:14px; padding:16px 18px;
    box-shadow:var(--shadow);
  }
  .stat-card .val{font-size:22px; font-weight:800; background:var(--accent-grad); -webkit-background-clip:text;
    background-clip:text; color:transparent;}
  .stat-card .lbl{font-size:12px; color:var(--muted); margin-top:4px;}
  .panel{
    background:var(--card); border:1px solid var(--border); border-radius:16px; box-shadow:var(--shadow);
    overflow:hidden;
  }
  .panel-head{
    padding:14px 18px; display:flex; align-items:center; justify-content:space-between; flex-wrap:wrap; gap:10px;
    border-bottom:1px solid var(--border);
  }
  .panel-head h2{font-size:14.5px; margin:0;}
  .panel-head .count{font-size:12px; color:var(--muted);}
  .table-wrap{overflow-x:auto;}
  table{width:100%; border-collapse:collapse; min-width:920px;}
  thead th{
    position:sticky; top:0; background:var(--bg-soft); text-align:left; padding:11px 14px; font-size:12px;
    color:var(--muted); text-transform:uppercase; letter-spacing:.03em; cursor:pointer; user-select:none;
    border-bottom:1px solid var(--border); white-space:nowrap;
  }
  thead th:hover{color:var(--accent);}
  thead th.sorted{color:var(--accent);}
  tbody td{padding:11px 14px; font-size:13.3px; border-bottom:1px solid var(--border); white-space:nowrap;}
  tbody tr:hover{background:var(--row-hover);}
  .rank{font-weight:700; width:34px; display:inline-flex; align-items:center; justify-content:center;
    height:26px; border-radius:8px;}
  .rank.gold{background:var(--gold); color:#3a2c00;}
  .rank.silver{background:var(--silver); color:#22232b;}
  .rank.bronze{background:var(--bronze); color:#2c1804;}
  .name-cell{display:flex; flex-direction:column;}
  .name-cell .uname{font-size:11.5px; color:var(--muted);}
  .badge{
    display:inline-block; padding:2px 9px; border-radius:20px; font-size:11px; font-weight:600;
  }
  .badge.ok{background:rgba(0,184,148,.15); color:var(--ok);}
  .badge.no{background:rgba(231,76,60,.13); color:var(--danger);}
  .pill{padding:2px 8px; border-radius:7px; background:var(--row-hover); font-size:12px; font-weight:600;}
  .total-pill{background:var(--accent-grad); color:#fff; font-weight:700;}
  .footer-bar{
    display:flex; align-items:center; justify-content:space-between; padding:12px 18px; flex-wrap:wrap; gap:10px;
    border-top:1px solid var(--border);
  }
  .page-btn{
    border:1px solid var(--border); background:var(--card); color:var(--text); border-radius:8px;
    padding:6px 12px; cursor:pointer; font-size:12.5px;
  }
  .page-btn:disabled{opacity:.4; cursor:not-allowed;}
  .page-info{font-size:12.5px; color:var(--muted);}
  .empty-state{padding:40px; text-align:center; color:var(--muted); font-size:13.5px;}
  .gen-time{font-size:11px; color:var(--muted); margin-top:10px; text-align:center;}
  select.page-size{
    background:var(--card); color:var(--text); border:1px solid var(--border); border-radius:8px;
    padding:5px 8px; font-size:12.5px;
  }
  @media (max-width:640px){
    .topbar{padding:12px 14px;} .container{padding:14px;}
    .search-box{min-width:0; flex:1;}
  }
</style>
</head>
<body data-theme="dark">
  <div class="topbar">
    <div class="brand">
      <div class="logo">🤖</div>
      <div>
        <h1>__BOT_NAME__ — User Data</h1>
        <span>সকল user এর poll / text Q&amp;A / OCR ব্যবহার — একনজরে</span>
      </div>
    </div>
    <div class="controls">
      <div class="search-box">
        <span>🔍</span>
        <input id="searchInput" type="text" placeholder="নাম, username বা user id দিয়ে খুঁজুন...">
      </div>
      <button class="theme-btn" id="themeToggle">🌙 <span id="themeLabel">Dark</span></button>
    </div>
  </div>

  <div class="container">
    <div class="stats-grid" id="statsGrid"></div>

    <div class="panel">
      <div class="panel-head">
        <h2>📋 সকল Users (বিস্তারিত)</h2>
        <span class="count" id="rowCount"></span>
      </div>
      <div class="table-wrap">
        <table id="dataTable">
          <thead>
            <tr>
              <th data-key="rank">#</th>
              <th data-key="name">User</th>
              <th data-key="uid">User ID</th>
              <th data-key="joined">Joined</th>
              <th data-key="poll">Poll</th>
              <th data-key="text">Text</th>
              <th data-key="ocr">OCR</th>
              <th data-key="total">Total</th>
              <th data-key="verified">Verified</th>
              <th data-key="last_active">Last Active</th>
            </tr>
          </thead>
          <tbody id="tableBody"></tbody>
        </table>
      </div>
      <div class="footer-bar">
        <div class="page-info" id="pageInfo"></div>
        <div style="display:flex; align-items:center; gap:8px;">
          <select class="page-size" id="pageSize">
            <option value="25">25 / page</option>
            <option value="50" selected>50 / page</option>
            <option value="100">100 / page</option>
            <option value="99999">সব দেখাও</option>
          </select>
          <button class="page-btn" id="prevBtn">◀ Prev</button>
          <button class="page-btn" id="nextBtn">Next ▶</button>
        </div>
      </div>
    </div>
    <div class="gen-time">Generated: __GEN_TIME__ (Asia/Dhaka) · __TOTAL_USERS__ users total</div>
  </div>

<script id="userDataJson" type="application/json">__DATA_JSON__</script>
<script>
(function(){
  var raw = document.getElementById('userDataJson').textContent;
  var DATA = JSON.parse(raw);

  // ── theme ──
  var root = document.body;
  var themeBtn = document.getElementById('themeToggle');
  var themeLabel = document.getElementById('themeLabel');
  function applyTheme(t){
    root.setAttribute('data-theme', t);
    themeLabel.textContent = t === 'dark' ? 'Dark' : 'Light';
    themeBtn.firstChild.textContent = (t === 'dark' ? '🌙 ' : '☀️ ');
    try{ localStorage.setItem('userdata_theme', t); }catch(e){}
  }
  var savedTheme = 'dark';
  try{ savedTheme = localStorage.getItem('userdata_theme') || 'dark'; }catch(e){}
  applyTheme(savedTheme);
  themeBtn.addEventListener('click', function(){
    applyTheme(root.getAttribute('data-theme') === 'dark' ? 'light' : 'dark');
  });

  // ── stats cards ──
  var totalUsers = DATA.length;
  var totalPoll = 0, totalText = 0, totalOcr = 0, totalVerified = 0;
  DATA.forEach(function(u){
    totalPoll += u.poll; totalText += u.text; totalOcr += u.ocr;
    if(u.verified) totalVerified++;
  });
  var totalUsage = totalPoll + totalText + totalOcr;
  var stats = [
    ['👥', totalUsers, 'মোট User'],
    ['✅', totalVerified, 'Verified User'],
    ['🧩', totalPoll, 'মোট Poll Solve'],
    ['💬', totalText, 'মোট Text Q&A'],
    ['🖼️', totalOcr, 'মোট OCR Q&A'],
    ['🔢', totalUsage, 'সর্বমোট ব্যবহার'],
  ];
  var statsGrid = document.getElementById('statsGrid');
  stats.forEach(function(s){
    var d = document.createElement('div');
    d.className = 'stat-card';
    d.innerHTML = '<div class="val">' + s[0] + ' ' + s[1].toLocaleString() + '</div><div class="lbl">' + s[2] + '</div>';
    statsGrid.appendChild(d);
  });

  // ── table state ──
  var sortKey = 'total', sortDir = -1;
  var page = 1, pageSize = 50;
  var filtered = DATA.slice();

  function escapeHtml(s){
    return String(s).replace(/[&<>"']/g, function(c){
      return {'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c];
    });
  }

  function rankClass(i){
    if(i === 1) return 'gold'; if(i === 2) return 'silver'; if(i === 3) return 'bronze'; return '';
  }

  function render(){
    var q = document.getElementById('searchInput').value.trim().toLowerCase();
    filtered = DATA.filter(function(u){
      if(!q) return true;
      return (u.name && u.name.toLowerCase().indexOf(q) !== -1) ||
             (u.username && u.username.toLowerCase().indexOf(q) !== -1) ||
             String(u.uid).indexOf(q) !== -1;
    });
    filtered.sort(function(a,b){
      var va = a[sortKey], vb = b[sortKey];
      if(typeof va === 'string'){ va = va.toLowerCase(); vb = vb.toLowerCase(); }
      if(va < vb) return -1 * sortDir;
      if(va > vb) return 1 * sortDir;
      return 0;
    });

    document.getElementById('rowCount').textContent = filtered.length.toLocaleString() + ' জন';

    var totalPages = Math.max(1, Math.ceil(filtered.length / pageSize));
    if(page > totalPages) page = totalPages;
    var startIdx = (page - 1) * pageSize;
    var pageRows = filtered.slice(startIdx, startIdx + pageSize);

    var tbody = document.getElementById('tableBody');
    if(pageRows.length === 0){
      tbody.innerHTML = '<tr><td colspan="10"><div class="empty-state">😕 কোনো user পাওয়া যায়নি।</div></td></tr>';
    } else {
      var html = '';
      pageRows.forEach(function(u, idx){
        var globalRank = startIdx + idx + 1;
        html += '<tr>';
        html += '<td><span class="rank ' + rankClass(globalRank) + '">' + globalRank + '</span></td>';
        html += '<td><div class="name-cell"><b>' + escapeHtml(u.name) + '</b><span class="uname">@' + escapeHtml(u.username) + '</span></div></td>';
        html += '<td><code>' + u.uid + '</code></td>';
        html += '<td>' + escapeHtml(u.joined || 'N/A') + '</td>';
        html += '<td><span class="pill">' + u.poll + '</span></td>';
        html += '<td><span class="pill">' + u.text + '</span></td>';
        html += '<td><span class="pill">' + u.ocr + '</span></td>';
        html += '<td><span class="pill total-pill">' + u.total + '</span></td>';
        html += '<td>' + (u.verified ? '<span class="badge ok">✅ Yes</span>' : '<span class="badge no">— No</span>') + '</td>';
        html += '<td>' + escapeHtml(u.last_active || 'N/A') + '</td>';
        html += '</tr>';
      });
      tbody.innerHTML = html;
    }

    document.getElementById('pageInfo').textContent = 'Page ' + page + ' / ' + totalPages + ' (' + filtered.length.toLocaleString() + ' জন)';
    document.getElementById('prevBtn').disabled = page <= 1;
    document.getElementById('nextBtn').disabled = page >= totalPages;

    var ths = document.querySelectorAll('thead th');
    ths.forEach(function(th){
      th.classList.toggle('sorted', th.getAttribute('data-key') === sortKey);
      var base = th.textContent.replace(/ ▲| ▼/g,'');
    });
  }

  document.querySelectorAll('thead th').forEach(function(th){
    th.addEventListener('click', function(){
      var key = th.getAttribute('data-key');
      if(key === 'rank') return;
      if(sortKey === key){ sortDir *= -1; } else { sortKey = key; sortDir = -1; }
      page = 1;
      render();
    });
  });

  document.getElementById('searchInput').addEventListener('input', function(){ page = 1; render(); });
  document.getElementById('pageSize').addEventListener('change', function(e){
    pageSize = parseInt(e.target.value, 10); page = 1; render();
  });
  document.getElementById('prevBtn').addEventListener('click', function(){ if(page > 1){ page--; render(); } });
  document.getElementById('nextBtn').addEventListener('click', function(){ page++; render(); });

  render();
})();
</script>
</body>
</html>
"""


def _build_full_userdata_html() -> "tuple[str, int]":
    """registered_users থেকে সব user এর পূর্ণ data নিয়ে একটা সুন্দর,
    dark/light toggle করা যায় এমন standalone HTML dashboard বানায়।
    Return: (html_string, total_user_count)
    """
    dhaka_tz = pytz.timezone("Asia/Dhaka")

    def _fmt_last_active(ts):
        try:
            ts = float(ts or 0)
        except (TypeError, ValueError):
            ts = 0
        if not ts:
            return "N/A"
        try:
            dt = datetime.fromtimestamp(ts, dhaka_tz)
            return dt.strftime("%d %b %Y, %I:%M %p")
        except Exception:
            return "N/A"

    users_out = []
    for uid, info in registered_users.items():
        pc  = int(info.get("poll_count", 0) or 0)
        qac = int(info.get("qa_count", 0) or 0)
        ocr = int(info.get("ocr_count", 0) or 0)
        name  = str(info.get("name", "Unknown")).strip() or "Unknown"
        uname = str(info.get("username", "N/A")).strip().lstrip("@") or "N/A"
        users_out.append({
            "uid":         uid,
            "name":        name,
            "username":    uname,
            "joined":      str(info.get("joined", "")).strip(),
            "poll":        pc,
            "text":        qac,
            "ocr":         ocr,
            "total":       pc + qac + ocr,
            "verified":    bool(info.get("verified", False)),
            "last_active": _fmt_last_active(info.get("last_active", 0)),
        })

    # সবচেয়ে বেশি ব্যবহারকারী আগে থাকুক (default view) — client-side sort এ পরে বদলানো যাবে
    users_out.sort(key=lambda u: u["total"], reverse=True)

    data_json = json.dumps(users_out, ensure_ascii=False).replace("</", "<\\/")
    gen_time  = datetime.now(dhaka_tz).strftime("%d %b %Y, %I:%M %p")

    html_out = (
        _USERDATA_HTML_TEMPLATE
        .replace("__BOT_NAME__", BOT_NAME)
        .replace("__DATA_JSON__", data_json)
        .replace("__GEN_TIME__", gen_time)
        .replace("__TOTAL_USERS__", str(len(users_out)))
    )
    return html_out, len(users_out)


# ══════════════════════════════════════════════════════════════════
#  /userdata COMMAND (Admin only) — bot সবচেয়ে বেশি কারা ব্যবহার করছে,
#  Top 10 user + poll/text-QA/OCR-এর আলাদা আলাদা count, table আকারে।
#  এর সাথে সব user-এর পূর্ণ data সহ একটা HTML dashboard ফাইলও পাঠায়।
# ══════════════════════════════════════════════════════════════════
async def userdata_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    if user.id != ADMIN_ID:
        keyboard = [[InlineKeyboardButton("📩 Contact Developer", url="https://t.me/RanaSynth")]]
        await update.message.reply_text(
            "❌ You are not authorized to use this bot.\n\n"
            "💬 Please contact the developer.\n"
            "📩 Contact: @RanaSynth",
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
        return

    if not registered_users:
        await update.message.reply_text("📭 No users registered yet.")
        return

    # ── প্রতিটা user-এর poll/text-QA/OCR/total usage বের করা ──
    rows = []
    for uid, info in registered_users.items():
        pc  = int(info.get("poll_count", 0) or 0)
        qac = int(info.get("qa_count", 0) or 0)
        ocr = int(info.get("ocr_count", 0) or 0)
        total = pc + qac + ocr
        if total <= 0:
            continue  # কোনো ব্যবহার নেই এমন user বাদ
        name  = str(info.get("name", "Unknown")).strip() or "Unknown"
        rows.append((uid, name, pc, qac, ocr, total))

    if not rows:
        await update.message.reply_text("📭 এখনও কোনো user poll/Q&A/OCR ব্যবহার করেনি।")
        return

    rows.sort(key=lambda r: r[5], reverse=True)
    top10 = rows[:10]

    grand_poll  = sum(r[2] for r in rows)
    grand_qa    = sum(r[3] for r in rows)
    grand_ocr   = sum(r[4] for r in rows)
    grand_total = sum(r[5] for r in rows)

    def _short_name(n: str, limit: int = 14) -> str:
        n = n.replace("|", "¦").strip()
        return n if len(n) <= limit else n[:limit - 1] + "…"

    lines = [
        "## 📊 Top 10 Most Active Users",
        "",
        f"👥 **মোট সক্রিয় user:** {len(rows)} জন",
        f"🧩 **সর্বমোট Poll Solve:** {grand_poll}",
        f"💬 **সর্বমোট Text Q&A:** {grand_qa}",
        f"🖼️ **সর্বমোট OCR Q&A:** {grand_ocr}",
        f"🔢 **সর্বমোট ব্যবহার:** {grand_total}",
        "",
        "| # | User | Poll | Text | OCR | Total |",
        "|---|---|---|---|---|---|",
    ]
    for i, (uid, name, pc, qac, ocr, total) in enumerate(top10, 1):
        lines.append(f"| {i} | {_short_name(name)} | {pc} | {qac} | {ocr} | **{total}** |")

    lines.append("")
    lines.append("_Poll = Poll solve, Text = Text Q&A, OCR = ছবি থেকে Q&A_")

    chat_id = update.effective_chat.id
    sent = await send_rich_message(chat_id, "\n".join(lines), reply_to_message_id=update.message.message_id)
    if sent is None:
        # rich message ব্যর্থ হলে plain HTML table-এ fallback
        fallback = [
            "📊 <b>Top 10 Most Active Users</b>\n",
            f"👥 মোট সক্রিয় user: <b>{len(rows)}</b> জন",
            f"🧩 সর্বমোট Poll Solve: <b>{grand_poll}</b>",
            f"💬 সর্বমোট Text Q&A: <b>{grand_qa}</b>",
            f"🖼 সর্বমোট OCR Q&A: <b>{grand_ocr}</b>",
            f"🔢 সর্বমোট ব্যবহার: <b>{grand_total}</b>\n",
        ]
        for i, (uid, name, pc, qac, ocr, total) in enumerate(top10, 1):
            fallback.append(
                f"{i}. <b>{name}</b> — Poll:{pc} Text:{qac} OCR:{ocr} → <b>মোট {total}</b>"
            )
        await update.message.reply_text("\n".join(fallback), parse_mode="HTML")

    # ── এর সাথে সব user-এর পূর্ণ data সহ একটা সুন্দর HTML dashboard ফাইল পাঠাও ──
    try:
        from io import BytesIO
        html_str, total_cnt = _build_full_userdata_html()
        html_bytes = html_str.encode("utf-8")
        dhaka_tz   = pytz.timezone("Asia/Dhaka")
        stamp      = datetime.now(dhaka_tz).strftime("%Y%m%d_%H%M")
        filename   = f"userdata_dashboard_{total_cnt}_{stamp}.html"

        doc = BytesIO(html_bytes)
        doc.name = filename

        await update.message.reply_document(
            document=doc,
            filename=filename,
            caption=(
                "🖥️ <b>Full User Data Dashboard</b>\n\n"
                f"👥 <b>{total_cnt}</b> জন user-এর সম্পূর্ণ data (name, username, user id, "
                "joined date, poll/text/OCR count, verified, last active) সহ।\n\n"
                "📎 ফাইলটা download করে যেকোনো browser-এ খুলো — search, sort, "
                "pagination আর 🌙/☀️ dark-light toggle সবই আছে।"
            ),
            parse_mode="HTML",
        )
    except Exception as e:
        logger.error(f"userdata HTML dashboard build/send error: {e}")
        try:
            await update.message.reply_text(
                "⚠️ HTML dashboard ফাইল তৈরি করতে সমস্যা হয়েছে। Log চেক করো।"
            )
        except Exception:
            pass


# ══════════════════════════════════════════════════════════════════
#  /broadcast COMMAND (Admin only)
# ══════════════════════════════════════════════════════════════════
pending_broadcast: dict = {}

# ── /addlimit wizard state ── { admin_id: {"type": "poll"/"text"/"ocr"} }
# বাটনে টাইপ বেছে নেওয়ার পর পরের text মেসেজে "user_id extra" আশা করা হয়।
pending_admin_limit: dict = {}

async def broadcast_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    if user.id != ADMIN_ID:
        await update.message.reply_text("❌ This command is for admins only.")
        return

    pending_broadcast[user.id] = {"step": "waiting_message"}
    await update.message.reply_text(
        "📢 *Broadcast Message*\n\n"
        "Type the message you want to send to all users.\n\n"
        "_Type /cancel to abort._",
        parse_mode=ParseMode.MARKDOWN
    )

async def broadcast_msg_handler(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    user  = update.effective_user
    msg   = update.message
    if not msg or user.id != ADMIN_ID:
        return

    state = pending_broadcast.get(user.id)
    if not state:
        return

    step = state.get("step")

    if step == "waiting_message":
        photo_id     = msg.photo[-1].file_id if msg.photo else None
        video_id     = msg.video.file_id if msg.video else None
        document_id  = msg.document.file_id if msg.document else None
        animation_id = msg.animation.file_id if msg.animation else None

        pending_broadcast[user.id] = {
            "step": "waiting_choice",
            "text": msg.text or msg.caption or "",
            "entities": msg.entities or msg.caption_entities or [],
            "photo_id": photo_id,
            "video_id": video_id,
            "document_id": document_id,
            "animation_id": animation_id,
        }
        keyboard = [[
            InlineKeyboardButton("📤 Send Without Button", callback_data="bc_no_button"),
            InlineKeyboardButton("🔘 Send With Button",    callback_data="bc_with_button"),
        ]]
        await msg.reply_text(
            "✅ *Message received.*\n\nHow would you like to send it?",
            reply_markup=InlineKeyboardMarkup(keyboard)
        )

    elif step == "waiting_button":
        raw = msg.text.strip()
        if " - " not in raw:
            await msg.reply_text(
                "❌ *Invalid format.*\n\nFormat:\n`Button Name - https://link`",
                parse_mode=ParseMode.MARKDOWN
            )
            return

        parts     = raw.split(" - ", 1)
        btn_label = parts[0].strip()
        btn_url   = parts[1].strip()

        if not btn_url.startswith("http"):
            await msg.reply_text("❌ URL must start with `https://`", parse_mode=ParseMode.MARKDOWN)
            return

        pending_broadcast[user.id]["step"]      = "confirming_button"
        pending_broadcast[user.id]["btn_label"] = btn_label
        pending_broadcast[user.id]["btn_url"]   = btn_url

        keyboard = [[
            InlineKeyboardButton("✅ Confirm & Send", callback_data="bc_confirm_send"),
            InlineKeyboardButton("❌ বাতিল",          callback_data="bc_cancel"),
        ]]
        await msg.reply_text(
            f"👇 *Button Preview:*\n\n[ {btn_label} ]({btn_url})\n\nSend?",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=InlineKeyboardMarkup(keyboard)
        )

async def broadcast_callback(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    user  = query.from_user

    if user.id != ADMIN_ID:
        await query.answer("❌ Admin only", show_alert=True)
        return

    data  = query.data
    state = pending_broadcast.get(user.id, {})

    if data == "bc_no_button":
        await query.edit_message_text("⏳ Sending...")
        sent, failed = await _do_broadcast(ctx, state["text"], state["entities"], None, None,
                             state.get("photo_id"), state.get("video_id"),
                             state.get("document_id"), state.get("animation_id"))
        await query.edit_message_text("✅ Broadcast sent! Report below 👇")
        await _send_broadcast_report(ctx, user.id, sent, failed, sent + failed)
        pending_broadcast.pop(user.id, None)

    elif data == "bc_with_button":
        pending_broadcast[user.id]["step"] = "waiting_button"
        await query.edit_message_text(
            "🔘 *Enter button details:*\n\nFormat:\n`Button Name - https://link`",
            parse_mode=ParseMode.MARKDOWN
        )

    elif data == "bc_confirm_send":
        await query.edit_message_text("⏳ Sending...")
        sent, failed = await _do_broadcast(ctx, state["text"], state["entities"],
                             state.get("btn_label"), state.get("btn_url"),
                             state.get("photo_id"), state.get("video_id"),
                             state.get("document_id"), state.get("animation_id"))
        await query.edit_message_text("✅ Broadcast sent! Report below 👇")
        await _send_broadcast_report(ctx, user.id, sent, failed, sent + failed)
        pending_broadcast.pop(user.id, None)

    elif data == "bc_cancel":
        pending_broadcast.pop(user.id, None)
        await query.edit_message_text("❌ Broadcast cancelled.")

async def _do_broadcast(ctx, text: str, entities: list, btn_label, btn_url,
                         photo_id=None, video_id=None, document_id=None, animation_id=None):
    from telegram import LinkPreviewOptions
    reply_markup  = None
    if btn_label and btn_url:
        reply_markup = InlineKeyboardMarkup([[InlineKeyboardButton(btn_label, url=btn_url)]])
    link_preview  = LinkPreviewOptions(is_disabled=True)

    success, fail = 0, 0
    for uid in list(registered_users.keys()):
        try:
            if photo_id:
                await ctx.bot.send_photo(
                    chat_id=uid,
                    photo=photo_id,
                    caption=text or None,
                    caption_entities=entities if entities else None,
                    reply_markup=reply_markup,
                )
            elif video_id:
                await ctx.bot.send_video(
                    chat_id=uid,
                    video=video_id,
                    caption=text or None,
                    caption_entities=entities if entities else None,
                    reply_markup=reply_markup,
                )
            elif animation_id:
                await ctx.bot.send_animation(
                    chat_id=uid,
                    animation=animation_id,
                    caption=text or None,
                    caption_entities=entities if entities else None,
                    reply_markup=reply_markup,
                )
            elif document_id:
                await ctx.bot.send_document(
                    chat_id=uid,
                    document=document_id,
                    caption=text or None,
                    caption_entities=entities if entities else None,
                    reply_markup=reply_markup,
                )
            else:
                await ctx.bot.send_message(
                    chat_id=uid,
                    text=text,
                    entities=entities if entities else None,
                    reply_markup=reply_markup,
                    link_preview_options=link_preview
                )
            success += 1
        except Exception as e:
            logger.warning(f"Broadcast failed for {uid}: {e}")
            fail += 1
        await asyncio.sleep(0.05)

    logger.info(f"Broadcast done: {success} success, {fail} failed")
    return success, fail


async def _send_broadcast_report(ctx, admin_id: int, sent: int, failed: int, total: int):
    """
    Broadcast শেষে admin-কে rich table format-এ report পাঠায় (weekly report-এর
    মতো একই style — markdown table, rich_message ব্যর্থ হলে HTML fallback)।
    """
    rich_text = (
        f"# ✅ Broadcast Complete!\n\n"
        f"| Info | Value |\n"
        f"| --- | --- |\n"
        f"| 🎯 Target | All Users |\n"
        f"| 📤 Sent | {sent} |\n"
        f"| ❌ Failed | {failed} |\n"
        f"| 📊 Total | {total} |\n\n"
        f"🕐 {get_dhaka_time()}"
    )

    result = await send_rich_message(admin_id, rich_text)
    if result is None:
        fallback = (
            f"✅ <b>Broadcast Complete!</b>\n"
            f"▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬\n\n"
            f"🎯 <b>Target:</b> All Users\n"
            f"📤 <b>Sent:</b> {sent}\n"
            f"❌ <b>Failed:</b> {failed}\n"
            f"📊 <b>Total:</b> {total}\n\n"
            f"▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬\n"
            f"🕐 {get_dhaka_time()}"
        )
        try:
            await ctx.bot.send_message(chat_id=admin_id, text=fallback, parse_mode="HTML")
        except Exception as e:
            logger.warning(f"Broadcast report fallback send failed: {e}")


# ══════════════════════════════════════════════════════════════════
#  /cancel COMMAND
# ══════════════════════════════════════════════════════════════════
async def cancel_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    user      = update.effective_user
    cancelled = False
    if user.id in pending_broadcast:
        pending_broadcast.pop(user.id)
        await update.message.reply_text("❌ Broadcast বাতিল হয়েছে।")
        cancelled = True
    if user.id in pending_admin_limit:
        pending_admin_limit.pop(user.id)
        await update.message.reply_text("❌ Extra limit দেওয়া বাতিল হয়েছে।")
        cancelled = True
    if user.id in pending_feedback:
        pending_feedback.pop(user.id)
        await update.message.reply_text("❌ Feedback বাতিল হয়েছে।")
        cancelled = True
    if not cancelled:
        await update.message.reply_text("ℹ️ বাতিল করার কিছু নেই।")


# ══════════════════════════════════════════════════════════════════
#  LIBRARY INTERACTION HANDLER
# ══════════════════════════════════════════════════════════════════
async def handle_library_interaction(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> bool:
    """
    Returns True if this message was consumed as a library interaction.

    Admin Button Editor Flow (নতুন):
      1st tap on button → Quick Action screen (msg preview + arrows + Edit/Delete/Cut)
      2nd tap on same button → Navigate inside (enter the button)
      ✱ → Rename, ✏️ Edit → edit msg, 🗑 Delete → delete button+children,
      ✂️ Cut → clipboard, 📋 Paste → paste in current folder

    Posts Editor Flow:
      Tap on button → show msg preview + Add Msg / Delete Msg

    User Flow:
      Tap → show msg + sub-buttons
    """
    msg  = update.message
    user = update.effective_user
    if not user or not msg or not msg.text:
        return False

    text     = msg.text.strip()
    is_admin = (user.id == ADMIN_ID)
    state    = pending_library_build.get(user.id)
    in_btn_editor = is_admin and (user.id in library_editor_active)
    editor_mode   = library_editor_active.get(user.id, "btn")

    # ══════════════════════════════════════════════
    # WIZARD STEPS — awaiting text input from admin
    # ══════════════════════════════════════════════

    # Step: awaiting new button title
    if state and state.get("step") == "awaiting_title":
        if text == "🚫 Cancel":
            pending_library_build.pop(user.id, None)
            parent_id = state["parent_id"]
            has_cb = user.id in library_clipboard
            kbd = _lib_keyboard_btn_editor_with_paste(parent_id, has_cb) if parent_id != "root" \
                  else _lib_keyboard_btn_editor_root("btn")
            await msg.reply_text("❌ Cancelled.", reply_markup=kbd)
            return True
        title = text.strip()
        if not title:
            await msg.reply_text("⚠️ Name cannot be empty. Please enter a name:")
            return True
        parent_id = state["parent_id"]
        library_add_node(parent_id, title, "menu")
        pending_library_build.pop(user.id, None)
        has_cb = user.id in library_clipboard
        if parent_id == "root":
            kbd = _lib_keyboard_btn_editor_root("btn")
        else:
            kbd = _lib_keyboard_btn_editor_with_paste(parent_id, has_cb)
        await msg.reply_text(
            f"✅ Button *\"{title}\"* created!",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=kbd
        )
        return True

    # Step: awaiting rename text
    if state and state.get("step") == "awaiting_rename":
        if text == "🚫 Cancel":
            pending_library_build.pop(user.id, None)
            node_id   = state["node_id"]
            parent_id = library_data.get(node_id, {}).get("parent", "root")
            stack     = library_nav.get(user.id, ["root"])
            current_id = stack[-1] if stack else "root"
            has_cb = user.id in library_clipboard
            kbd = _lib_keyboard_btn_editor_root("btn") if current_id == "root" \
                  else _lib_keyboard_btn_editor_with_paste(current_id, has_cb)
            await msg.reply_text("❌ Cancelled.", reply_markup=kbd)
            return True
        new_title = text.strip()
        if not new_title:
            await msg.reply_text("⚠️ Name cannot be empty:")
            return True
        node_id = state["node_id"]
        old_title = library_data.get(node_id, {}).get("title", "")
        library_rename_node(node_id, new_title)
        pending_library_build.pop(user.id, None)
        # After rename, go back to parent level
        stack = library_nav.get(user.id, ["root"])
        current_id = stack[-1] if stack else "root"
        has_cb = user.id in library_clipboard
        kbd = _lib_keyboard_btn_editor_root("btn") if current_id == "root" \
              else _lib_keyboard_btn_editor_with_paste(current_id, has_cb)
        await msg.reply_text(
            f"✅ *\"{old_title}\"* → *\"{new_title}\"* rename হয়েছে!",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=kbd
        )
        return True

    # Step: awaiting post/msg text
    if state and state.get("step") == "awaiting_post_text":
        parent_id = state["parent_id"]
        if text == "🚫 Cancel":
            pending_library_build.pop(user.id, None)
            stack = library_nav.get(user.id, ["root"])
            current_id = stack[-1] if stack else "root"
            if editor_mode == "post":
                kbd = _lib_keyboard_btn_editor_root("post") if current_id == "root" \
                      else _lib_keyboard_btn_editor_node(current_id, "post")
            else:
                has_cb = user.id in library_clipboard
                kbd = _lib_keyboard_btn_editor_root("btn") if current_id == "root" \
                      else _lib_keyboard_btn_editor_with_paste(current_id, has_cb)
            await msg.reply_text("❌ Cancelled.", reply_markup=kbd)
            return True
        node = library_data.get(parent_id)
        if node:
            node["text"] = text
            entities = msg.entities or []
            node["entities"] = [e.to_dict() for e in entities] if entities else []
            save_library(library_data)
        pending_library_build.pop(user.id, None)
        stack = library_nav.get(user.id, ["root"])
        current_id = stack[-1] if stack else "root"
        if editor_mode == "post":
            kbd = _lib_keyboard_btn_editor_root("post") if current_id == "root" \
                  else _lib_keyboard_btn_editor_node(current_id, "post")
        else:
            has_cb = user.id in library_clipboard
            kbd = _lib_keyboard_btn_editor_root("btn") if current_id == "root" \
                  else _lib_keyboard_btn_editor_with_paste(current_id, has_cb)
        await msg.reply_text("✅ *Message saved!*", parse_mode=ParseMode.MARKDOWN, reply_markup=kbd)
        return True

    # Only proceed if user is inside library nav
    if user.id not in library_nav:
        return False

    stack      = library_nav[user.id]
    current_id = stack[-1] if stack else "root"

    # ══════════════════════════════════════════════
    # UNIVERSAL: Main Menu
    # ══════════════════════════════════════════════
    if text == LIB_MAIN_MENU_LABEL:
        library_nav.pop(user.id, None)
        library_editor_active.pop(user.id, None)
        library_selected_node.pop(user.id, None)
        pending_library_build.pop(user.id, None)
        await msg.reply_text("🏠 Main Menu", reply_markup=MAIN_MENU_KEYBOARD)
        return True

    # ══════════════════════════════════════════════
    # UNIVERSAL: Back
    # ══════════════════════════════════════════════
    if text == LIB_BACK_LABEL:
        pending_library_build.pop(user.id, None)
        library_selected_node.pop(user.id, None)

        if is_admin and in_btn_editor:
            if len(stack) > 1:
                stack.pop()
                current_id = stack[-1]
                node = library_data.get(current_id, library_data["root"])
                has_cb = user.id in library_clipboard
                if current_id == "root":
                    kbd = _lib_keyboard_btn_editor_root(editor_mode)
                elif editor_mode == "post":
                    kbd = _lib_keyboard_btn_editor_node(current_id, "post")
                else:
                    kbd = _lib_keyboard_btn_editor_with_paste(current_id, has_cb)
                await msg.reply_text(
                    f"📂 *{node['title']}*",
                    parse_mode=ParseMode.MARKDOWN,
                    reply_markup=kbd
                )
                return True
            # At root → back to admin entry screen
            library_editor_active.pop(user.id, None)
            library_clipboard.pop(user.id, None)
            await msg.reply_text(
                "📚 *Synthesis Library*\n\nকী করতে চাও? 👇",
                parse_mode=ParseMode.MARKDOWN,
                reply_markup=_lib_keyboard_admin_entry()
            )
            return True

        if is_admin and not in_btn_editor:
            # Admin entry screen → main menu
            library_nav.pop(user.id, None)
            await msg.reply_text("🏠 Main Menu", reply_markup=MAIN_MENU_KEYBOARD)
            return True

        # Regular user back
        if len(stack) <= 1:
            library_nav.pop(user.id, None)
            await msg.reply_text("🏠 Main Menu", reply_markup=MAIN_MENU_KEYBOARD)
            return True
        stack.pop()
        current_id = stack[-1]
        node = library_data.get(current_id, library_data["root"])
        await msg.reply_text(
            f"📚 *{node['title']}*",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=_lib_keyboard_user(current_id)
        )
        return True

    # ══════════════════════════════════════════════
    # ADMIN ENTRY SCREEN
    # ══════════════════════════════════════════════
    if is_admin and not in_btn_editor:
        if text == LIB_BTN_EDITOR_LABEL:
            library_editor_active[user.id] = "btn"
            library_nav[user.id] = ["root"]
            await msg.reply_text(
                "🗂 *Button Editor*\n\nButton tap করো 👇",
                parse_mode=ParseMode.MARKDOWN,
                reply_markup=_lib_keyboard_btn_editor_root("btn")
            )
            return True

        if text == LIB_POST_EDITOR_LABEL:
            library_editor_active[user.id] = "post"
            library_nav[user.id] = ["root"]
            children = library_get_children("root")
            if not children:
                await msg.reply_text(
                    "⚠️ এখনো কোনো button নেই।\n\nআগে *Button Editor* দিয়ে button তৈরি করো।",
                    parse_mode=ParseMode.MARKDOWN,
                    reply_markup=_lib_keyboard_admin_entry()
                )
                library_editor_active.pop(user.id, None)
            else:
                await msg.reply_text(
                    "📝 *Posts Editor*\n\nকোন button-এ ঢুকে msg লিখবে? 👇",
                    parse_mode=ParseMode.MARKDOWN,
                    reply_markup=_lib_keyboard_btn_editor_root("post")
                )
            return True

        await msg.reply_text(
            "📚 *Synthesis Library*\n\nকী করতে চাও? 👇",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=_lib_keyboard_admin_entry()
        )
        return True

    # ══════════════════════════════════════════════
    # POSTS EDITOR MODE
    # ══════════════════════════════════════════════
    if in_btn_editor and editor_mode == "post":

        def _post_kbd():
            if current_id == "root":
                return _lib_keyboard_btn_editor_root("post")
            return _lib_keyboard_btn_editor_node(current_id, "post")

        if text == "🚫 Cancel":
            pending_library_build.pop(user.id, None)
            await msg.reply_text("❌ Cancelled.", reply_markup=_post_kbd())
            return True

        if text == LIB_ADD_MSG_LABEL:
            node = library_data.get(current_id, {})
            existing = node.get("text", "")
            pending_library_build[user.id] = {"step": "awaiting_post_text", "parent_id": current_id}
            cancel_kbd = ReplyKeyboardMarkup([[KeyboardButton("🚫 Cancel")]], resize_keyboard=True)
            prompt = (
                "✏️ *Type your message now.*\n"
                "Bold, italic, links — all Telegram formatting works.\n\n"
                "_(Type directly or paste your content)_"
            )
            if existing:
                prompt += f"\n\n📌 *Current message:*\n{existing[:300]}{'...' if len(existing) > 300 else ''}"
            await msg.reply_text(prompt, parse_mode=ParseMode.MARKDOWN, reply_markup=cancel_kbd)
            return True

        if text == LIB_DELETE_MSG_LABEL:
            node = library_data.get(current_id, {})
            if node.get("text"):
                node["text"] = ""
                node["entities"] = []
                save_library(library_data)
                await msg.reply_text("🗑 *Msg ডিলিট হয়ে গেছে!*", parse_mode=ParseMode.MARKDOWN,
                                     reply_markup=_post_kbd())
            else:
                await msg.reply_text("⚠️ এখানে কোনো msg নেই।", reply_markup=_post_kbd())
            return True

        # Tap on child button in Posts Editor → navigate in
        children = library_get_children(current_id)
        matched = next((c for c in children if c["title"] == text), None)
        if matched:
            stack.append(matched["id"])
            node_text = matched.get("text", "")
            info = f"📝 *{matched['title']}*"
            if node_text:
                preview = node_text[:300] + ("..." if len(node_text) > 300 else "")
                info += f"\n\n📄 *Current msg:*\n{preview}"
            else:
                info += "\n\n_(No msg yet — tap '📝 Add Msg' to add)_"
            await msg.reply_text(info, parse_mode=ParseMode.MARKDOWN,
                                 reply_markup=_lib_keyboard_btn_editor_node(matched["id"], "post"))
            return True

        return False

    # ══════════════════════════════════════════════
    # BUTTON EDITOR MODE — NEW 1ST-TAP / 2ND-TAP FLOW
    # ══════════════════════════════════════════════
    if in_btn_editor and editor_mode == "btn":
        has_cb = user.id in library_clipboard

        def _btn_kbd():
            if current_id == "root":
                return _lib_keyboard_btn_editor_root("btn")
            return _lib_keyboard_btn_editor_with_paste(current_id, has_cb)

        # ── Cancel ──
        if text == "🚫 Cancel":
            pending_library_build.pop(user.id, None)
            library_selected_node.pop(user.id, None)
            await msg.reply_text("❌ Cancelled.", reply_markup=_btn_kbd())
            return True

        # ── Quick Action selection state ──
        # selection থাকা মানে এই node-এর quick-action menu আগেই দেখানো হয়েছে।
        # সেই same button-এ আবার (2nd বার) tap করলে → Enter (ভিতরে ঢোকা)।
        # অন্য কোনো button/action চাপলে selection clear হয়ে normal flow চলবে।
        selection = library_selected_node.get(user.id)
        selected_node = library_data.get(selection["node_id"]) if selection else None
        if selection and selected_node and selected_node.get("title") == text and selected_node.get("parent") == current_id:
            # ── 2nd tap on the same button → Enter (navigate inside) ──
            library_selected_node.pop(user.id, None)
            # 1st-tap quick-action message থেকে inline keyboard সরিয়ে দাও
            try:
                await ctx.bot.edit_message_reply_markup(
                    chat_id=selection["chat_id"],
                    message_id=selection["message_id"],
                    reply_markup=None
                )
            except Exception:
                pass
            node_id = selection["node_id"]
            stack.append(node_id)
            library_nav[user.id] = stack
            has_cb_enter = user.id in library_clipboard
            body = selected_node.get("text", "")
            if body:
                preview = body[:200] + ("..." if len(body) > 200 else "")
                info = f"📂 *{text}*\n\n📄 *Msg:*\n{preview}"
            else:
                info = f"📂 *{text}*\n\n_(No msg yet)_"
            await msg.reply_text(
                info,
                parse_mode=ParseMode.MARKDOWN,
                reply_markup=_lib_keyboard_btn_editor_with_paste(node_id, has_cb_enter)
            )
            return True
        if selection:
            # ভিন্ন button/action চাপা হয়েছে → পুরনো quick-action keyboard সরিয়ে selection clear করো
            library_selected_node.pop(user.id, None)
            try:
                await ctx.bot.edit_message_reply_markup(
                    chat_id=selection["chat_id"],
                    message_id=selection["message_id"],
                    reply_markup=None
                )
            except Exception:
                pass


        # ── Add Button ──
        if text == LIB_ADD_BTN_LABEL:
            pending_library_build[user.id] = {"step": "awaiting_title", "parent_id": current_id}
            cancel_kbd = ReplyKeyboardMarkup([[KeyboardButton("🚫 Cancel")]], resize_keyboard=True)
            await msg.reply_text(
                "📝 *Button এর নাম লিখো:*\n_(emoji সহ লিখতে পারো, যেমন: 🧬 Biology)_",
                parse_mode=ParseMode.MARKDOWN,
                reply_markup=cancel_kbd
            )
            return True

        # ── Paste ──
        if text == LIB_QA_PASTE_LABEL and has_cb:
            clip_id = library_clipboard.get(user.id)
            if not clip_id or clip_id not in library_data:
                library_clipboard.pop(user.id, None)
                await msg.reply_text("⚠️ Clipboard খালি।", reply_markup=_btn_kbd())
                return True
            clip_title = library_data[clip_id].get("title", "?")
            ok = library_paste_node(clip_id, current_id)
            library_clipboard.pop(user.id, None)
            has_cb = False
            if ok:
                await msg.reply_text(
                    f"📋 *\"{clip_title}\"* এখানে paste হয়েছে!",
                    parse_mode=ParseMode.MARKDOWN,
                    reply_markup=_lib_keyboard_btn_editor_with_paste(current_id, False)
                )
            else:
                await msg.reply_text(
                    "⚠️ Paste করা সম্ভব হয়নি (circular বা same folder)।",
                    reply_markup=_btn_kbd()
                )
            return True

        # ── 1st tap on a child button → Quick Action screen (inline buttons) ──
        children = library_get_children(current_id)
        matched = next((c for c in children if c["title"] == text), None)
        if matched:
            node_text = matched.get("text", "")
            # Show msg preview + quick action inline keyboard
            if node_text:
                preview = node_text[:300] + ("..." if len(node_text) > 300 else "")
                info = f"📌 *{matched['title']}*\n\n{preview}"
            else:
                info = f"📌 *{matched['title']}*\n\n_(No msg yet)_"
            info += f"\n\n_📂 *\"{matched['title']}\"* এ আবার ট্যাপ করলে ভেতরে ঢুকবে।_"
            sent = await msg.reply_text(
                info,
                parse_mode=ParseMode.MARKDOWN,
                reply_markup=_lib_quick_action_inline(matched["id"])
            )
            library_selected_node[user.id] = {
                "node_id": matched["id"],
                "message_id": sent.message_id,
                "chat_id": sent.chat_id,
            }
            return True

        return False

    # ══════════════════════════════════════════════
    # USER NAVIGATION
    # ══════════════════════════════════════════════
    children = library_get_children(current_id)
    matched = next((c for c in children if c["title"] == text), None)
    if matched:
        stack.append(matched["id"])
        child_children = library_get_children(matched["id"])
        body = matched.get("text", "")
        entities_data = matched.get("entities", [])

        async def _send_msg_body(reply_markup):
            if not body:
                return
            try:
                from telegram import MessageEntity
                if entities_data:
                    ents = [MessageEntity.de_json(e, ctx.bot) for e in entities_data]
                    await msg.reply_text(body, entities=ents, reply_markup=reply_markup)
                else:
                    await msg.reply_text(body, parse_mode=ParseMode.MARKDOWN, reply_markup=reply_markup)
            except Exception:
                try:
                    await msg.reply_text(body, parse_mode=ParseMode.MARKDOWN, reply_markup=reply_markup)
                except Exception:
                    await msg.reply_text(body, reply_markup=reply_markup)

        kbd = _lib_keyboard_user(matched["id"])
        if child_children:
            if body:
                await _send_msg_body(kbd)
            else:
                await msg.reply_text(
                    f"📚 *{matched['title']}*\n\nনিচের topic বেছে নাও 👇",
                    parse_mode=ParseMode.MARKDOWN,
                    reply_markup=kbd
                )
        else:
            if body:
                await _send_msg_body(kbd)
            else:
                await msg.reply_text(
                    "_(এখনো কোনো content নেই)_",
                    parse_mode=ParseMode.MARKDOWN,
                    reply_markup=kbd
                )
        return True

    return False


async def library_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    if user.id != ADMIN_ID and not await is_active_member(ctx.bot, user.id):
        await _send_verify_prompt(update.message)
        return

    # Reset any previous state
    library_nav.pop(user.id, None)
    library_editor_active.pop(user.id, None)
    pending_library_build.pop(user.id, None)

    is_admin = (user.id == ADMIN_ID)

    if is_admin:
        # Admin sees entry screen: Button Editor | Posts Editor | Back | Main Menu
        library_nav[user.id] = ["root"]
        await update.message.reply_text(
            "📚 *Synthesis Library*\n\nকী করতে চাও? 👇",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=_lib_keyboard_admin_entry()
        )
    else:
        # User sees the content directly
        children = library_get_children("root")
        root_node = library_data.get("root", {})
        root_msg = root_node.get("text", "")
        root_entities = root_node.get("entities", [])
        library_nav[user.id] = ["root"]

        # Show root msg if set
        if root_msg:
            try:
                from telegram import MessageEntity
                if root_entities:
                    ents = [MessageEntity.de_json(e, ctx.bot) for e in root_entities]
                    await update.message.reply_text(root_msg, entities=ents,
                                                    reply_markup=_lib_keyboard_user("root"))
                else:
                    await update.message.reply_text(root_msg, parse_mode=ParseMode.MARKDOWN,
                                                    reply_markup=_lib_keyboard_user("root"))
            except Exception:
                try:
                    await update.message.reply_text(root_msg, parse_mode=ParseMode.MARKDOWN,
                                                    reply_markup=_lib_keyboard_user("root"))
                except Exception:
                    await update.message.reply_text(root_msg, reply_markup=_lib_keyboard_user("root"))
        elif not children:
            await update.message.reply_text(
                "📚 *Synthesis Library*\n\n"
                "এখনো কোনো content add করা হয়নি।\n"
                "শীঘ্রই আসছে! 🚧",
                parse_mode=ParseMode.MARKDOWN,
                reply_markup=_lib_keyboard_user("root")
            )
        else:
            await update.message.reply_text(
                "📚 *Synthesis Library*\n\nনিচের বাটন থেকে topic বেছে নাও 👇",
                parse_mode=ParseMode.MARKDOWN,
                reply_markup=_lib_keyboard_user("root")
            )


async def addlimit_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """
    Admin-only: কোনো user-এর আজকের জন্য daily limit বাড়িয়ে দাও — Poll Solve,
    Text Q&A, বা Image OCR Q&A — তিনটার মধ্যে যেকোনো একটা বেছে নিয়ে।
    বাটনে চাপ দিয়ে টাইপ বেছে নিলে পরের মেসেজে `user_id extra` পাঠাতে হবে।
    এই bonus শুধু আজকের জন্যই প্রযোজ্য — রাত ১২টার পর (Dhaka time) সব আবার reset হয়ে যাবে।
    """
    user = update.effective_user
    if user.id != ADMIN_ID:
        return

    pending_admin_limit.pop(user.id, None)

    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("🧩 Poll Solve Limit", callback_data="addlim:poll")],
        [InlineKeyboardButton("💬 Text Q&A Limit", callback_data="addlim:text")],
        [InlineKeyboardButton("🖼 OCR Q&A Limit", callback_data="addlim:ocr")],
        [InlineKeyboardButton("❌ Cancel", callback_data="addlim:cancel")],
    ])
    await update.message.reply_text(
        "🎁 *Extra Limit দাও (আজকের জন্য)*\n\n"
        "নিচের বাটন থেকে কোন ধরনের limit বাড়াতে চাও বেছে নাও 👇\n"
        "_(এটা শুধু আজকের জন্যই — রাত ১২টার পর আবার reset হয়ে যাবে)_",
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=keyboard
    )


# টাইপ অনুযায়ী label/icon/set-function/effective-limit-function — নতুন টাইপ
# যোগ করতে হলে শুধু এই dict-এ একটা entry বাড়ালেই যথেষ্ট।
ADDLIMIT_TYPE_META = {
    "poll": {
        "label":        "🧩 Poll Solve",
        "set_fn":       set_admin_extra_limit,
        "eff_fn":       get_effective_daily_limit,
        "unit":         "poll",
        "student_text": "Daily Poll Limit",
    },
    "text": {
        "label":        "💬 Text Q&A",
        "set_fn":       set_admin_extra_text_limit,
        "eff_fn":       get_effective_text_daily_limit,
        "unit":         "প্রশ্ন",
        "student_text": "Daily Text Q&A Limit",
    },
    "ocr": {
        "label":        "🖼 OCR (ছবি) Q&A",
        "set_fn":       set_admin_extra_ocr_limit,
        "eff_fn":       get_effective_ocr_daily_limit,
        "unit":         "ছবি-প্রশ্ন",
        "student_text": "Daily Image OCR Q&A Limit",
    },
}


async def addlimit_type_callback(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """/addlimit wizard-এর টাইপ-selection বাটন হ্যান্ডেল করে।"""
    query = update.callback_query
    user  = update.effective_user
    if user.id != ADMIN_ID:
        await query.answer("❌ Admin only.", show_alert=True)
        return

    data = query.data  # "addlim:poll" | "addlim:text" | "addlim:ocr" | "addlim:cancel"
    kind = data.split(":", 1)[1]

    if kind == "cancel":
        pending_admin_limit.pop(user.id, None)
        await query.answer()
        try:
            await query.edit_message_text("❌ বাতিল করা হয়েছে।")
        except Exception:
            pass
        return

    meta = ADDLIMIT_TYPE_META.get(kind)
    if meta is None:
        await query.answer("❌ Invalid type.", show_alert=True)
        return

    pending_admin_limit[user.id] = {"type": kind}
    await query.answer()
    try:
        await query.edit_message_text(
            f"🎯 *{meta['label']} Limit* বেছে নিয়েছ।\n\n"
            f"এখন এই ফরম্যাটে পাঠাও 👇\n"
            f"`<user_id> <extra_{meta['unit']}>`\n\n"
            f"উদাহরণ: `123456789 10`\n"
            f"_(এটা ওই user-এর আজকের {meta['label']} limit-এ +10 যোগ করবে)_\n\n"
            f"মুছে দিতে extra = 0 পাঠাও।\n"
            f"বাতিল করতে /cancel লেখো।",
            parse_mode=ParseMode.MARKDOWN
        )
    except Exception:
        pass


async def handle_addlimit_input(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Admin টাইপ বেছে নেওয়ার পর `user_id extra` মেসেজ পাঠালে এখানে process হয়।"""
    user  = update.effective_user
    msg   = update.message
    state = pending_admin_limit.get(user.id)
    if not state:
        return

    kind = state.get("type")
    meta = ADDLIMIT_TYPE_META.get(kind)
    if meta is None:
        pending_admin_limit.pop(user.id, None)
        return

    parts = (msg.text or "").split()
    if len(parts) != 2:
        await msg.reply_text(
            f"ℹ️ *Usage:* `<user_id> <extra_{meta['unit']}>`\n\n"
            f"উদাহরণ: `123456789 10`\n"
            f"বাতিল করতে /cancel লেখো।",
            parse_mode=ParseMode.MARKDOWN
        )
        return

    try:
        target_id = int(parts[0])
        extra     = int(parts[1])
    except ValueError:
        await msg.reply_text("❌ user_id এবং extra অবশ্যই সংখ্যা হতে হবে।")
        return

    if extra < 0:
        await msg.reply_text("❌ extra negative হতে পারবে না।")
        return

    meta["set_fn"](target_id, extra)
    new_total = meta["eff_fn"](target_id)
    pending_admin_limit.pop(user.id, None)

    await msg.reply_text(
        f"✅ *{meta['label']} Limit Updated! (আজকের জন্য)*\n\n"
        f"👤 User ID: `{target_id}`\n"
        f"➕ Admin Bonus: *{extra}* {meta['unit']} (শুধু আজ)\n"
        f"📊 আজকের মোট {meta['label']} limit: *{new_total}*",
        parse_mode=ParseMode.MARKDOWN
    )

    try:
        await ctx.bot.send_message(
            chat_id=target_id,
            text=(
                f"🎁 *আজকের জন্য তোমার {meta['student_text']} বাড়ানো হয়েছে!*\n\n"
                f"আজ তুমি মোট *{new_total}টি* {meta['unit']} করতে পারবে।\n"
                "_কাল আবার আগের নিয়মে limit reset হয়ে যাবে।_"
            ),
            parse_mode=ParseMode.MARKDOWN
        )
    except Exception:
        pass


async def get_extra_limits_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    if user.id != ADMIN_ID and not await is_active_member(ctx.bot, user.id):
        await _send_verify_prompt(update.message)
        return

    bot_username = ctx.bot.username
    ref_link = f"https://t.me/{bot_username}?start={make_referral_code(user.id)}"

    today = get_dhaka_date()
    entry = referral_bonus.get(user.id)
    if entry and entry.get("date") == today:
        invited_today = entry.get("count_today", 0)
        bonus_today   = entry.get("extra", 0)
    else:
        invited_today = 0
        bonus_today   = 0

    import urllib.parse as _urlparse
    share_text = _urlparse.quote(
        f"🚀 *MCQ Poll Solve করতে আর সময় নষ্ট নয়!*\n\n"
        f"🤖 *Synthesis Robot* দিয়ে সেকেন্ডেই Telegram MCQ Poll Solve করুন।\n"
        f"⚡ Fast  •  🎯 Accurate  •  📚 Study Friendly\n\n"
        f"👇 *আমার Referral Link দিয়ে Join করুন:*\n"
        f"{ref_link}\n\n"
        f"💙 *একবার ব্যবহার করলেই পার্থক্য বুঝতে পারবেন!*"
    )
    keyboard = [
        [InlineKeyboardButton("🎁 Get Extra Limits", url=f"https://t.me/share/url?text={share_text}")]
    ]
    text = (
        "🎁 *Get Extra Poll Limits!*\n\n"
        "তোমার বন্ধুদের bot এ আমন্ত্রণ করো —\n"
        f"প্রতিজন successfully join করলে *আজকের জন্য +{REFERRAL_BONUS_PER_INVITE} poll* বোনাস পাবে!\n\n"
        "▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬\n"
        f"🔗 *তোমার Referral Link:*\n`{ref_link}`\n\n"
        "▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬\n"
        f"👥 আজ Invite করেছো: *{invited_today} জন*\n"
        f"🎯 আজকের বোনাস: *+{bonus_today} poll*\n\n"
        "_বন্ধু channel join করে Verify করলেই বোনাস পাবে।_"
    )
    await update.message.reply_text(
        text, parse_mode=ParseMode.MARKDOWN,
        reply_markup=InlineKeyboardMarkup(keyboard),
        disable_web_page_preview=True
    )


# ══════════════════════════════════════════════════════════════════
#  /pollsolver COMMAND
# ══════════════════════════════════════════════════════════════════
async def pollsolver_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    if user.id != ADMIN_ID and not await is_active_member(ctx.bot, user.id):
        await _send_verify_prompt(update.message)
        return
    await update.message.reply_text(
        "🧩 *Poll Solver*\n\n"
        "যে poll/quiz টা solve করাতে চাও সেটা এখানে *forward* করো বা সরাসরি *send* করো 👇",
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=MAIN_MENU_KEYBOARD
    )


# ══════════════════════════════════════════════════════════════════
#  /dailyusage COMMAND
# ══════════════════════════════════════════════════════════════════
async def dailyusage_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    if user.id != ADMIN_ID and not await is_active_member(ctx.bot, user.id):
        await _send_verify_prompt(update.message)
        return

    if user.id == ADMIN_ID:
        await update.message.reply_text(
            "👑 *Admin Account*\n\n"
            "You have no daily limit\\!\n"
            "Solve unlimited polls\\. 🚀",
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=MAIN_MENU_KEYBOARD
        )
        return

    today = get_dhaka_date()
    entry = rate_data.get(user.id)

    if entry is None or entry.get("date") != today:
        used = 0
    else:
        used = entry.get("count", 0)

    bonus         = get_referral_bonus(user.id)
    admin_extra   = get_admin_extra_limit(user.id)
    effective_cap = get_effective_daily_limit(user.id)
    remaining     = effective_cap - used
    bar_fill      = round((used / effective_cap) * 10) if effective_cap else 0
    bar           = "🟩" * bar_fill + "⬜" * (10 - bar_fill)

    # Cooldown status
    last_time = entry.get("last_time", 0.0) if entry else 0.0
    elapsed   = time.time() - last_time
    if elapsed < COOLDOWN_SECS:
        cooldown_left = int(COOLDOWN_SECS - elapsed) + 1
        mins = cooldown_left // 60
        secs = cooldown_left % 60
        cooldown_text = f"⏳ Next poll in: **{mins}:{secs:02d}**"
    else:
        cooldown_text = "✅ Ready to solve!"

    # Reset ETA
    dhaka_tz_r = pytz.timezone("Asia/Dhaka")
    now_dhaka  = datetime.now(dhaka_tz_r)
    import datetime as _dt
    midnight   = (now_dhaka + _dt.timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
    secs_left  = int((midnight - now_dhaka).total_seconds())
    reset_eta  = f"{secs_left // 3600}h {(secs_left % 3600) // 60}m"
    reset_time_str = now_dhaka.strftime("%d %b at 12:00 AM")

    # ── Text Q&A এবং OCR (ছবি) usage — poll limit থেকে সম্পূর্ণ আলাদা ──
    qa_cap    = get_effective_text_daily_limit(user.id)
    qa_used   = get_qa_daily_entry(user.id)["count"]
    qa_left   = max(0, qa_cap - qa_used)
    ocr_cap   = get_effective_ocr_daily_limit(user.id)
    ocr_used  = get_ocr_daily_entry(user.id)["count"]
    ocr_left  = max(0, ocr_cap - ocr_used)

    qa_bar_fill  = round((qa_used / qa_cap) * 10) if qa_cap else 0
    qa_bar       = "🟩" * qa_bar_fill + "⬜" * (10 - qa_bar_fill)
    ocr_bar_fill = round((ocr_used / ocr_cap) * 10) if ocr_cap else 0
    ocr_bar      = "🟩" * ocr_bar_fill + "⬜" * (10 - ocr_bar_fill)

    bonus_line = f"- 🎁 Referral Bonus: **+{bonus}**\n" if bonus > 0 else ""
    admin_extra_line = f"- 🛠️ Admin Extra: **+{admin_extra}**\n" if admin_extra > 0 else ""
    qa_admin_extra   = get_admin_extra_text_limit(user.id)
    ocr_admin_extra  = get_admin_extra_ocr_limit(user.id)
    qa_extra_line    = f" (base {TEXT_DAILY_LIMIT} + admin bonus {qa_admin_extra})" if qa_admin_extra > 0 else ""
    ocr_extra_line   = f" (base {OCR_DAILY_LIMIT} + admin bonus {ocr_admin_extra})" if ocr_admin_extra > 0 else ""

    # ── Telegram Bot API 10.1 Rich Message: real markdown table ──
    rich_text = (
        f"# 📊 Daily Usage\n\n"
        f"| Type | Used | Remaining | Limit |\n"
        f"| --- | --- | --- | --- |\n"
        f"| 🧩 Poll Solve | {used} | {remaining} | {effective_cap} |\n"
        f"| 💬 Text Q&A | {qa_used} | {qa_left} | {qa_cap} |\n"
        f"| 🖼 Image OCR Q&A | {ocr_used} | {ocr_left} | {ocr_cap} |\n\n"
        f"- 📦 Poll Base Limit: **{DAILY_LIMIT}**\n"
        f"{bonus_line}"
        f"{admin_extra_line}"
        f"- {cooldown_text}\n\n"
        f"---\n\n"
        f"🔄 Resets in **{reset_eta}** ({reset_time_str}, Dhaka)"
    )

    bot_username = (await ctx.bot.get_me()).username
    ref_link = f"https://t.me/{bot_username}?start={make_referral_code(user.id)}"
    import urllib.parse as _urlparse
    share_text = _urlparse.quote(
        f"🚀 *MCQ Poll Solve করতে আর সময় নষ্ট নয়!*\n\n"
        f"🤖 *Synthesis Robot* দিয়ে সেকেন্ডেই Telegram MCQ Poll Solve করুন।\n"
        f"⚡ Fast  •  🎯 Accurate  •  📚 Study Friendly\n\n"
        f"👇 *আমার Referral Link দিয়ে Join করুন:*\n"
        f"{ref_link}\n\n"
        f"💙 *একবার ব্যবহার করলেই পার্থক্য বুঝতে পারবেন!*"
    )
    share_keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("🎁 Get Extra Limits", url=f"https://t.me/share/url?text={share_text}")]
    ])

    # সবার আগে rich message (real table) দিয়ে চেষ্টা করো; পুরনো client/API
    # সমস্যায় ব্যর্থ হলে plain MarkdownV2 ফলব্যাকে চলে যায়, যাতে কেউ কখনো
    # খালি হাতে ফিরে না যায়।
    rich_result = await send_rich_message(
        update.effective_chat.id, rich_text, reply_markup=share_keyboard
    )
    if rich_result is None:
        import re as _re_escape
        def _esc(s: str) -> str:
            return _re_escape.sub(r'([_*\[\]()~`>#+\-=|{}.!])', r'\\\1', s)
        fallback_text = (
            f"📊 *Daily Usage*\n"
            f"▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬\n\n"
            f"🧩 *Poll:* {used}/{effective_cap} used, {_esc(str(remaining))} left\n"
            f"💬 *Text Q&A:* {qa_used}/{qa_cap} used, {qa_left} left\n"
            f"🖼 *Image OCR:* {ocr_used}/{ocr_cap} used, {ocr_left} left\n\n"
            f"{bar}\n"
            f"{qa_bar}\n"
            f"{ocr_bar}\n\n"
            f"{_esc(cooldown_text)}\n\n"
            f"▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬\n"
            f"🔄 Resets in: *{reset_eta}*\n"
            f"🕛 \\({reset_time_str}, Dhaka\\)"
        )
        await update.message.reply_text(fallback_text, parse_mode=ParseMode.MARKDOWN_V2,
                                        reply_markup=share_keyboard)


# ══════════════════════════════════════════════════════════════════
#  /myreport COMMAND — student নিজের ব্যবহার-ইতিহাস দেখতে পারবে:
#  কতদিন ধরে bot ব্যবহার করছে (streak + badge), মোট কতদিন active ছিল,
#  কয়টা poll/text/OCR solve করেছে — সব rich message (table) formatting-এ।
# ══════════════════════════════════════════════════════════════════
async def myreport_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    if user.id != ADMIN_ID and not await is_active_member(ctx.bot, user.id):
        await _send_verify_prompt(update.message)
        return

    info = registered_users.get(user.id, {})

    streak         = info.get("streak", 0)
    longest_streak = info.get("longest_streak", 0)
    active_days    = info.get("active_days", 0)
    badge          = get_streak_badge(streak)
    badge_line     = f"{badge} " if badge else ""

    poll_count = info.get("poll_count", 0)
    qa_count   = info.get("qa_count", 0)
    ocr_count  = info.get("ocr_count", 0)
    total_use  = poll_count + qa_count + ocr_count

    today     = get_dhaka_date()
    qa_used   = get_qa_daily_entry(user.id)["count"]
    ocr_used  = get_ocr_daily_entry(user.id)["count"]
    entry     = rate_data.get(user.id)
    poll_used_today = entry.get("count", 0) if (entry and entry.get("date") == today) else 0

    next_badge_line = ""
    for threshold, nxt_badge in reversed(STREAK_BADGE_TIERS):
        if streak < threshold:
            remaining = threshold - streak
            next_badge_line = f"\n🎯 আরো **{remaining} দিন** টানা ব্যবহার করলে পাবে {nxt_badge} badge!"
            break

    rich_text = (
        f"# 📖 তোমার Report — {info.get('name', user.full_name or 'Student')}\n\n"
        f"## {badge_line}Streak\n\n"
        f"| | |\n"
        f"| --- | --- |\n"
        f"| 🔥 বর্তমান Streak | **{streak} দিন** |\n"
        f"| 🏅 সর্বোচ্চ Streak | **{longest_streak} দিন** |\n"
        f"| 📅 মোট Active Days | **{active_days} দিন** |\n\n"
        f"{next_badge_line}\n\n"
        f"## 📊 মোট ব্যবহার (Lifetime)\n\n"
        f"| Type | Count |\n"
        f"| --- | --- |\n"
        f"| 🧩 Poll Solve | {poll_count} |\n"
        f"| 💬 Text Q&A | {qa_count} |\n"
        f"| 🖼 Image (OCR) Q&A | {ocr_count} |\n"
        f"| 🔢 **সর্বমোট** | **{total_use}** |\n\n"
        f"## 📆 আজকের ব্যবহার\n\n"
        f"| Type | Used Today |\n"
        f"| --- | --- |\n"
        f"| 🧩 Poll | {poll_used_today} |\n"
        f"| 💬 Text Q&A | {qa_used} |\n"
        f"| 🖼 OCR | {ocr_used} |\n\n"
        f"---\n\n"
        f"🕐 {get_dhaka_time()}"
    )

    sent = await send_rich_message(update.effective_chat.id, rich_text,
                                   reply_to_message_id=update.message.message_id)
    if sent is None:
        fallback = (
            f"📖 <b>তোমার Report</b>\n"
            f"▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬\n\n"
            f"{badge_line}<b>Streak:</b> {streak} দিন (সর্বোচ্চ: {longest_streak} দিন)\n"
            f"📅 <b>মোট Active Days:</b> {active_days} দিন\n\n"
            f"📊 <b>মোট ব্যবহার:</b>\n"
            f"🧩 Poll: {poll_count} | 💬 Text: {qa_count} | 🖼 OCR: {ocr_count}\n"
            f"🔢 সর্বমোট: <b>{total_use}</b>\n\n"
            f"📆 <b>আজকের ব্যবহার:</b>\n"
            f"🧩 Poll: {poll_used_today} | 💬 Text: {qa_used} | 🖼 OCR: {ocr_used}\n\n"
            f"▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬\n"
            f"🕐 {get_dhaka_time()}"
        )
        await update.message.reply_text(fallback, parse_mode="HTML")


# ══════════════════════════════════════════════════════════════════
#  /contact COMMAND
# ══════════════════════════════════════════════════════════════════
async def contact_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    text = (
        "📩 *Contact the Developer*\n\n"
        "━━━━━━━━━━━━━━━━━━━\n"
        "🤖 Bot: *Synthesis Robot*\n"
        "👨‍💻 Developer: @RanaSynth\n"
        "━━━━━━━━━━━━━━━━━━━\n\n"
        "💬 For any issues, questions, or feedback\n"
        "feel free to message the developer directly."
    )
    keyboard = [[InlineKeyboardButton("📩 Contact Developer", url="https://t.me/RanaSynth")]]
    await update.message.reply_text(text, parse_mode=ParseMode.MARKDOWN,
                                    reply_markup=InlineKeyboardMarkup(keyboard))


# ══════════════════════════════════════════════════════════════════
#  /feedback COMMAND
# ══════════════════════════════════════════════════════════════════
pending_feedback: dict = {}

async def feedback_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    if user.id != ADMIN_ID and not await is_active_member(ctx.bot, user.id):
        await _send_verify_prompt(update.message)
        return
    pending_feedback[user.id] = True
    await update.message.reply_text(
        "💬 *Feedback পাঠাও*\n\n"
        "তোমার মতামত, সমস্যা বা পরামর্শ লিখে পাঠাও —\n"
        "সরাসরি developer এর কাছে যাবে!\n\n"
        "_/cancel লিখলে বাতিল হবে।_",
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=MAIN_MENU_KEYBOARD
    )


# ══════════════════════════════════════════════════════════════════
#  GENERAL MESSAGE HANDLER
# ══════════════════════════════════════════════════════════════════
async def msg_handler(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    msg = update.message
    if not msg:
        return

    if msg.chat.type == "channel":
        return

    user = update.effective_user

    # Broadcast flow (admin only, no membership check needed)
    if user and user.id == ADMIN_ID and user.id in pending_broadcast:
        step = pending_broadcast[user.id].get("step")
        if step in ("waiting_message", "waiting_button"):
            await broadcast_msg_handler(update, ctx)
            return

    # /addlimit wizard flow (admin only, no membership check needed) —
    # টাইপ বেছে নেওয়ার পর এই মেসেজে "user_id extra" আশা করা হয়।
    if user and user.id == ADMIN_ID and user.id in pending_admin_limit:
        await handle_addlimit_input(update, ctx)
        return

    # ── Central membership guard ──
    # Admin ছাড়া সব non-admin user-কে এখানে check করা হয়।
    # /start ও /cancel ছাড়া সব কিছুতেই channel membership লাগবে।
    if user and user.id != ADMIN_ID:
        still_member = await is_active_member(ctx.bot, user.id)
        if not still_member:
            verified_users.discard(user.id)
            if user.id in registered_users:
                registered_users[user.id]["verified"] = False
                _turso_bg(lambda: _save_user(user.id), "save_user")
            await _send_verify_prompt(msg)
            return

    # Library interactions (navigation taps + admin builder wizard) take priority
    # over feedback-capture so admin/users can navigate freely mid-flow.
    # ── Poll message? Skip all text-routing and go directly to poll handler ──
    if msg.poll:
        await handle_poll(update, ctx)
        return

    if user:
        consumed = await handle_library_interaction(update, ctx)
        if consumed:
            return

    # Persistent menu button taps → route to the matching command handler
    if user and msg.text in MENU_BUTTON_COMMANDS:
        if user.id in pending_feedback:
            pending_feedback.pop(user.id)  # button tap cancels an in-progress feedback capture
        library_nav.pop(user.id, None)     # leaving library mode
        library_editor_active.pop(user.id, None)
        command = MENU_BUTTON_COMMANDS[msg.text]
        if command == "pollsolver":
            await pollsolver_cmd(update, ctx)
        elif command == "dailyusage":
            await dailyusage_cmd(update, ctx)
        elif command == "help":
            await help_cmd(update, ctx)
        elif command == "feedback":
            await feedback_cmd(update, ctx)
        return

    # ── Solve Tools submenu ──
    if user and msg.text == SOLVE_TOOLS_LABEL:
        pending_feedback.pop(user.id, None)
        library_nav.pop(user.id, None)
        library_editor_active.pop(user.id, None)
        await msg.reply_text(
            "🛠 *Solve Tools*\n\n"
            "🧩 *Poll Solve* — poll/quiz forward করো\n"
            "💬 *Text Q&A* — প্রশ্ন সরাসরি লিখে পাঠাও\n"
            "🖼 *Image Q&A* — প্রশ্নের ছবি পাঠাও\n\n"
            "_এই button গুলো না চেপেও সরাসরি poll / text / ছবি পাঠালেই solve হবে।_",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=SOLVE_TOOLS_KEYBOARD
        )
        return

    if user and msg.text == SOLVE_POLL_LABEL:
        pending_feedback.pop(user.id, None)
        if user.id != ADMIN_ID and not await is_active_member(ctx.bot, user.id):
            await _send_verify_prompt(msg)
            return
        await msg.reply_text(
            "🧩 *Poll Solve*\n\n"
            "যে poll/quiz solve করাতে চাও সেটা এখানে *forward* করো বা সরাসরি *send* করো 👇",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=SOLVE_TOOLS_KEYBOARD
        )
        return

    if user and msg.text == SOLVE_TEXT_LABEL:
        pending_feedback.pop(user.id, None)
        if user.id != ADMIN_ID and not await is_active_member(ctx.bot, user.id):
            await _send_verify_prompt(msg)
            return
        await msg.reply_text(
            "💬 *Text Q&A*\n\n"
            "প্রশ্নটা সরাসরি এখানে *লিখে* পাঠাও — AI সাথে সাথেই উত্তর দিবে 👇",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=SOLVE_TOOLS_KEYBOARD
        )
        return

    if user and msg.text == SOLVE_IMAGE_LABEL:
        pending_feedback.pop(user.id, None)
        if user.id != ADMIN_ID and not await is_active_member(ctx.bot, user.id):
            await _send_verify_prompt(msg)
            return
        await msg.reply_text(
            "🖼 *Image Q&A*\n\n"
            "প্রশ্নের *ছবি* পাঠাও 📷\n"
            "caption-এ প্রশ্ন লিখলে (যেমন: 64 no. ans dao) সাথে সাথেই solve হবে,\n"
            "না লিখলে ছবির *reply*-তে প্রশ্ন লিখো।",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=SOLVE_TOOLS_KEYBOARD
        )
        return

    if user and msg.text == SOLVE_BACK_LABEL:
        pending_feedback.pop(user.id, None)
        await msg.reply_text("🏠 Main Menu", reply_markup=MAIN_MENU_KEYBOARD)
        return

    if user and msg.text == "🎁 Get Extra Limits":
        if user.id in pending_feedback:
            pending_feedback.pop(user.id)
        library_nav.pop(user.id, None)
        library_editor_active.pop(user.id, None)
        await get_extra_limits_cmd(update, ctx)
        return

    if user and msg.text == "📝 Unlimited Exam":
        if user.id in pending_feedback:
            pending_feedback.pop(user.id)
        library_editor_active.pop(user.id, None)
        await library_cmd(update, ctx)
        return

    # 🏠 Main Menu button — from anywhere (library, etc.) → show start keyboard
    if user and msg.text == LIB_MAIN_MENU_LABEL:
        library_nav.pop(user.id, None)
        library_editor_active.pop(user.id, None)
        pending_library_build.pop(user.id, None)
        pending_feedback.pop(user.id, None)
        await msg.reply_text("🏠 Main Menu", reply_markup=MAIN_MENU_KEYBOARD)
        return

    # Feedback flow
    if user and user.id in pending_feedback and msg.text:
        pending_feedback.pop(user.id)
        name     = user.full_name or "Unknown"
        username = f"@{user.username}" if user.username else "no username"
        uid      = user.id
        report   = (
            f"📩 NEW FEEDBACK\n"
            f"━━━━━━━━━━━━━━━━━━━\n"
            f"👤 {name} ({username})\n"
            f"🆔 {uid}\n"
            f"🕐 {get_dhaka_time()}\n"
            f"━━━━━━━━━━━━━━━━━━━\n\n"
            f"{msg.text}"
        )
        try:
            await ctx.bot.send_message(chat_id=ADMIN_ID, text=report)
        except Exception as e:
            logger.error(f"Feedback forward error: {e}")
        await msg.reply_text(
            "✅ *Feedback পাঠানো হয়েছে!*\n\nধন্যবাদ, developer শীঘ্রই দেখবেন 🙏",
            parse_mode=ParseMode.MARKDOWN
        )
        return

    # (poll messages are handled earlier, before text routing)

    # ── ছবি পাঠালে: প্রশ্নটা ছবির reply-তে লিখতে বলা হয় ──
    #    (caption-এ প্রশ্ন লেখা থাকলে সেটাকেই প্রশ্ন ধরে সরাসরি সমাধান করা হয়)
    if user and msg.photo and msg.chat.type == "private":
        caption = (msg.caption or "").strip()
        if caption:
            await process_image_question(ctx, msg, user, caption,
                                         msg.photo[-1].file_id)
        else:
            await send_photo_hint(msg, user.id)
        return

    # ── ছবির reply-তে প্রশ্ন করলে: ছবি পড়ে (OCR) উত্তর ──
    if (user and msg.text and msg.chat.type == "private"
            and msg.reply_to_message and msg.reply_to_message.photo):
        img_question = msg.text.strip()
        if img_question:
            await process_image_question(
                ctx, msg, user, img_question,
                msg.reply_to_message.photo[-1].file_id
            )
            return

    # ── General Q&A — student সরাসরি text দিয়ে প্রশ্ন করলে rich-formatted AI answer ──
    if user and msg.text and msg.chat.type == "private":
        question = msg.text.strip()
        if question:
            # ── Off-topic/small-talk/meta/link/gali/junk হলে Gemini call না করেই সরাসরি reply ──
            if not is_academic_question(question):
                try:
                    await msg.reply_text(random.choice(_ASK_QUESTION_CANNED_REPLIES))
                except Exception as e:
                    logger.error(f"off-topic canned reply error: {e}")
                return

            await maybe_send_first_use_tip(msg, user.id)

            if user.id != ADMIN_ID:
                # দৈনিক text-প্রশ্নের limit (poll limit থেকে আলাদা)
                if not check_qa_daily_limit(user.id):
                    await send_qa_limit_message(msg, user.id)
                    return
                remaining = await try_reserve_qa_cooldown(user.id)
                if remaining > 0:
                    asyncio.create_task(send_qa_cooldown_countdown(msg, remaining))
                    return

            # ── Report group-এ full detail পাঠানো (name, username, id, question, time) ──
            asyncio.create_task(send_text_qa_report(ctx, user, question))

            status = await msg.reply_text("🔍 *Searching*", parse_mode=ParseMode.MARKDOWN)
            answer = await run_with_animation(status, answer_question(question))

            ai_failed = (not answer or "AI_FAILED" in answer)
            if ai_failed:
                try:
                    await status.delete()
                except Exception:
                    pass
                retry_id = f"qaretry_{uuid.uuid4().hex[:8]}"
                retry_qa_data[retry_id] = {
                    "question":    question,
                    "chat_id":     msg.chat_id,
                    "user_id":     user.id,
                    "_created_at": time.time(),
                }
                keyboard = [[InlineKeyboardButton("🔄 Retry", callback_data=retry_id)]]
                await msg.reply_text(
                    f"🤖 *{BOT_NAME}*\n\n"
                    "🚧 *System Update in Progress*\n\n"
                    "⚙️ AI service সাময়িকভাবে unavailable। এই attempt গণনা করা হয়নি — আবার চেষ্টা করো!",
                    parse_mode=ParseMode.MARKDOWN,
                    reply_markup=InlineKeyboardMarkup(keyboard)
                )
                return

            try:
                await status.delete()
            except Exception:
                pass

            await send_long_qa_answer(ctx.bot, msg.chat_id, answer, reply_to_message_id=msg.message_id, user_id=user.id)

            # সফল উত্তরের পরেই দৈনিক count বাড়ে (fail হলে গণনা হয় না)
            if user.id != ADMIN_ID:
                consume_qa_daily(user.id)
                update_user_streak(user.id)
                try:
                    await ctx.bot.send_message(
                        msg.chat_id, qa_daily_footer(user.id).strip(), parse_mode="HTML"
                    )
                except Exception:
                    pass

            if user.id in registered_users:
                registered_users[user.id]["last_active"] = time.time()
                _turso_bg(lambda: _save_user(user.id), "save_user")
            return



# ══════════════════════════════════════════════════════════════════
#  MAIN
# ══════════════════════════════════════════════════════════════════
from telegram.ext import CallbackQueryHandler


# ══════════════════════════════════════════════════════════════════
#  RETRY CALLBACK — user taps Retry after AI failure
# ══════════════════════════════════════════════════════════════════
async def retry_callback(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer("🔄 Retrying...")
    user  = query.from_user
    data  = query.data  # retry_<id>

    # Membership check — channel leave করলে retry করতে পারবে না
    if user and user.id != ADMIN_ID:
        still_member = await is_active_member(ctx.bot, user.id)
        if not still_member:
            verified_users.discard(user.id)
            if user.id in registered_users:
                registered_users[user.id]["verified"] = False
                _turso_bg(lambda: _save_user(user.id), "save_user")
            keyboard = [
                [InlineKeyboardButton("📢 Live Exam TPC", url=GROUP_LINK)],
                [InlineKeyboardButton("✅ Joined — Verify করো", callback_data="verify_check")],
            ]
            try:
                await query.edit_message_text(
                    "⛔ *Access Denied!*\n\n"
                    "Channel থেকে leave করলে bot ব্যবহার করা যাবে না।\n\n"
                    "আবার join করো এবং Verify চাপো।",
                    parse_mode=ParseMode.MARKDOWN,
                    reply_markup=InlineKeyboardMarkup(keyboard)
                )
            except Exception:
                pass
            return

    poll_info = retry_poll_data.pop(data, None)
    if not poll_info:
        await query.edit_message_text(
            "⚠️ এই retry টা আর valid নেই। Poll টা আবার forward করো।"
        )
        return

    # TTL check — অনেকক্ষণ আগের retry button হলে আর valid রাখব না (memory leak/stale-data fix)
    if time.time() - poll_info.get("_created_at", 0) > RETRY_DATA_TTL_SECS:
        await query.edit_message_text(
            "⚠️ এই retry button-টার মেয়াদ শেষ হয়ে গেছে। Poll টা আবার forward করো।"
        )
        return

    # Update button to show retrying
    try:
        await query.edit_message_text(
            "🤖 *Synthesis Robot*\n\n🔄 *Retrying...*\n\n⏳ একটু অপেক্ষা করো...",
            parse_mode=ParseMode.MARKDOWN
        )
    except Exception:
        pass

    question    = poll_info["question"]
    options     = poll_info["options"]
    correct_idx = poll_info["correct_idx"]
    chat_id     = poll_info["chat_id"]
    source_link = poll_info.get("source_link", "")
    source_name = poll_info.get("source_name", "")
    # Use the actual callback query sender as uid (poll_info user_id may be None)
    uid = user.id if user else poll_info.get("user_id")

    # Send a fresh status message to show animation
    status = await ctx.bot.send_message(chat_id, "🔍 *Analyzing...*", parse_mode=ParseMode.MARKDOWN)
    result = await run_with_animation(status, solve_poll(question, options, correct_idx))

    ai_failed = ("AI_FAILED" in result)

    if ai_failed:
        if REPORT_GROUP_ID and uid:
            try:
                fake_user = type("U", (), {"id": uid, "full_name": registered_users.get(uid, {}).get("name", "User"),
                                            "username": registered_users.get(uid, {}).get("username", "").lstrip("@") or None})()
                class _FakeMsg:
                    chat_id = chat_id
                    message_id = None
                await send_poll_fail_report(ctx, fake_user, _FakeMsg(), question)
            except Exception as e:
                logger.error(f"Retry fail report error: {e}")
        try:
            await status.delete()
        except Exception:
            pass
        # Store again for another retry
        retry_id = f"retry_{uuid.uuid4().hex[:8]}"
        poll_info["_created_at"] = time.time()
        retry_poll_data[retry_id] = poll_info
        fail_text = (
            "🤖 *Synthesis Robot*\n\n"
            "🚧 *System Update in Progress*\n\n"
            "⚙️ We\'re currently improving the AI service to provide a better experience.\n\n"
            "⚠️ AI is temporarily unavailable. This attempt was *not counted* — please try again!"
        )
        keyboard = [[InlineKeyboardButton("🔄 Retry", callback_data=retry_id)]]
        await ctx.bot.send_message(
            chat_id, fail_text,
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
        return

    # Success — count it (uid already set above from callback query sender)
    if uid and uid != ADMIN_ID:
        if uid in registered_users:
            registered_users[uid]["poll_count"] = registered_users[uid].get("poll_count", 0) + 1
            registered_users[uid]["last_active"] = time.time()
            _turso_bg(lambda: _save_user(uid), "save_user")
        consume_rate_limit(uid)
        record_poll_solved(uid)
        asyncio.create_task(notify_ready_after_cooldown(ctx.bot, chat_id, uid))
        entry    = rate_data.get(uid, {})
        used     = entry.get("count", 1)
        eff_limit = get_effective_daily_limit(uid)
        left     = eff_limit - used
        bar_fill = round((used / eff_limit) * 10) if eff_limit else 0
        bar      = "🟩" * bar_fill + "⬜" * (10 - bar_fill)
        footer   = f"\n\n📊 <b>Daily:</b> {bar} {used}/{eff_limit}"
        if left <= 3 and left > 0:
            footer += f"\n⚠️ <b>মাত্র {left}টি solve বাকি আজ!</b>"
        elif left == 0:
            footer += "\n🚫 <b>আজকের limit শেষ! কাল আবার এসো।</b>"
    else:
        if uid and uid in registered_users:
            registered_users[uid]["poll_count"] = registered_users[uid].get("poll_count", 0) + 1
            registered_users[uid]["last_active"] = time.time()
            _turso_bg(lambda: _save_user(uid), "save_user")
            record_poll_solved(uid)
        footer = ""

    await send_solved_answer(
        ctx.bot, chat_id, status, question, options,
        result, footer_html=footer,
        reply_markup=_make_share_keyboard(ctx.bot.username, question, options, correct_idx, uid)
    )

    if REPORT_GROUP_ID and uid:
        try:
            fake_user = type("U", (), {"id": uid, "full_name": registered_users.get(uid, {}).get("name", "User"),
                                        "username": registered_users.get(uid, {}).get("username", "").lstrip("@") or None})()
            class _FakeMsg:
                chat_id = chat_id
                message_id = status.message_id
            await send_poll_report(ctx, fake_user, _FakeMsg(), source_link, source_name)
        except Exception as e:
            logger.error(f"Retry report error: {e}")


async def qa_retry_callback(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """General Q&A answer-এ AI fail হলে 'Retry' বাটনে চাপ দিলে এই callback চলে।"""
    query = update.callback_query
    await query.answer("🔄 Retrying...")
    user = query.from_user
    data = query.data  # qaretry_<id>

    if user and user.id != ADMIN_ID:
        still_member = await is_active_member(ctx.bot, user.id)
        if not still_member:
            verified_users.discard(user.id)
            if user.id in registered_users:
                registered_users[user.id]["verified"] = False
                _turso_bg(lambda: _save_user(user.id), "save_user")
            keyboard = [
                [InlineKeyboardButton("📢 Live Exam TPC", url=GROUP_LINK)],
                [InlineKeyboardButton("✅ Joined — Verify করো", callback_data="verify_check")],
            ]
            try:
                await query.edit_message_text(
                    "⛔ *Access Denied!*\n\nChannel থেকে leave করলে bot ব্যবহার করা যাবে না।\n\n"
                    "আবার join করো এবং Verify চাপো।",
                    parse_mode=ParseMode.MARKDOWN,
                    reply_markup=InlineKeyboardMarkup(keyboard)
                )
            except Exception:
                pass
            return

    qa_info = retry_qa_data.pop(data, None)
    if not qa_info:
        await query.edit_message_text("⚠️ এই retry টা আর valid নেই। প্রশ্নটা আবার লিখো।")
        return

    if time.time() - qa_info.get("_created_at", 0) > RETRY_DATA_TTL_SECS:
        await query.edit_message_text("⚠️ এই retry button-টার মেয়াদ শেষ হয়ে গেছে। প্রশ্নটা আবার লিখো।")
        return

    try:
        await query.edit_message_text(
            f"🤖 *{BOT_NAME}*\n\n🔄 *Retrying...*\n\n⏳ একটু অপেক্ষা করো...",
            parse_mode=ParseMode.MARKDOWN
        )
    except Exception:
        pass

    question = qa_info["question"]
    chat_id  = qa_info["chat_id"]
    uid      = user.id if user else qa_info.get("user_id")

    img_file_id = qa_info.get("image_file_id")

    # Retry করার আগেও দৈনিক limit চেক করা হয় — নাহলে limit শেষ হয়ে যাওয়ার পরেও
    # পুরোনো Retry বাটন চেপে অতিরিক্ত উত্তর নেওয়া যেত (bug fix)।
    if uid and uid != ADMIN_ID:
        if img_file_id and not check_ocr_daily_limit(uid):
            try:
                _ocr_lim = get_effective_ocr_daily_limit(uid)
                await ctx.bot.send_message(
                    chat_id,
                    f"🚫 <b>আজকের ছবি-প্রশ্নের limit শেষ!</b>\n"
                    f"🕛 রাত ১২টার পর আবার <b>{_ocr_lim}</b> টি ছবি-প্রশ্ন করতে পারবে।",
                    parse_mode="HTML"
                )
            except Exception:
                pass
            return
        if (not img_file_id) and not check_qa_daily_limit(uid):
            try:
                _txt_lim = get_effective_text_daily_limit(uid)
                await ctx.bot.send_message(
                    chat_id,
                    f"🚫 <b>আজকের প্রশ্নের limit শেষ!</b>\n"
                    f"🕛 রাত ১২টার পর আবার <b>{_txt_lim}</b> টি প্রশ্ন করতে পারবে।",
                    parse_mode="HTML"
                )
            except Exception:
                pass
            return


    if img_file_id:
        status = await ctx.bot.send_message(chat_id, "🔍 *Reading image*",
                                            parse_mode=ParseMode.MARKDOWN)

        async def _retry_image_work():
            image_b64, mime = await fetch_photo_b64(ctx, img_file_id)
            if not image_b64:
                return "AI_FAILED"
            return await answer_image_question(question, image_b64, mime)

        answer = await run_with_animation(status, _retry_image_work())
    else:
        status = await ctx.bot.send_message(chat_id, "🔍 *Searching*", parse_mode=ParseMode.MARKDOWN)
        answer = await run_with_animation(status, answer_question(question))

    ai_failed = (not answer or "AI_FAILED" in answer)
    if ai_failed:
        try:
            await status.delete()
        except Exception:
            pass
        retry_id = f"qaretry_{uuid.uuid4().hex[:8]}"
        qa_info["_created_at"] = time.time()
        retry_qa_data[retry_id] = qa_info
        keyboard = [[InlineKeyboardButton("🔄 Retry", callback_data=retry_id)]]
        await ctx.bot.send_message(
            chat_id,
            f"🤖 *{BOT_NAME}*\n\n"
            "🚧 *System Update in Progress*\n\n"
            "⚙️ AI service সাময়িকভাবে unavailable। এই attempt গণনা করা হয়নি — আবার চেষ্টা করো!",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
        return

    try:
        await status.delete()
    except Exception:
        pass

    if uid and uid in registered_users:
        registered_users[uid]["last_active"] = time.time()
        _turso_bg(lambda: _save_user(uid), "save_user")

    await send_long_qa_answer(ctx.bot, chat_id, answer, user_id=uid)

    if uid and uid != ADMIN_ID:
        if img_file_id:
            consume_ocr_daily(uid)
            footer = ocr_daily_footer(uid).strip()
        else:
            consume_qa_daily(uid)
            footer = qa_daily_footer(uid).strip()
        update_user_streak(uid)
        try:
            await ctx.bot.send_message(chat_id, footer, parse_mode="HTML")
        except Exception:
            pass



async def lqa_callback(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Handle inline quick-action button presses for library button editor."""
    query = update.callback_query
    await query.answer()
    user = query.from_user

    if user.id != ADMIN_ID:
        await query.answer("❌ Admin only", show_alert=True)
        return

    # data format: "lqa:<node_id>:<action>"
    parts = query.data.split(":", 2)
    if len(parts) != 3:
        return
    _, node_id, action = parts

    node = library_data.get(node_id)
    if not node:
        await query.edit_message_text("⚠️ Button খুঁজে পাওয়া যায়নি।")
        return

    sel_title = node.get("title", "?")
    parent_id = node.get("parent", "root")

    # Helper: get current editor keyboard for parent level
    stack = library_nav.get(user.id, ["root"])
    current_id = stack[-1] if stack else "root"
    has_cb = user.id in library_clipboard

    def _parent_kbd():
        if parent_id == "root":
            return _lib_keyboard_btn_editor_root("btn")
        return _lib_keyboard_btn_editor_with_paste(parent_id, user.id in library_clipboard)

    # ── Cancel ──
    if action == "cancel":
        library_selected_node.pop(user.id, None)
        try:
            await query.edit_message_reply_markup(reply_markup=None)
        except Exception:
            pass
        return

    # ── Move arrows ──
    if action in ("up", "down", "left", "right"):
        moved = library_move_node(node_id, action)
        dir_labels = {"up": "⬆️ উপরে", "down": "⬇️ নিচে", "left": "⬅️ বামে", "right": "➡️ ডানে"}
        if moved:
            try:
                await query.edit_message_reply_markup(
                    reply_markup=_lib_quick_action_inline(node_id)
                )
            except Exception:
                pass
            await ctx.bot.send_message(
                chat_id=query.message.chat_id,
                text=f"{dir_labels[action]} *\"{sel_title}\"* সরানো হয়েছে।",
                parse_mode=ParseMode.MARKDOWN,
                reply_markup=_parent_kbd()
            )
        else:
            await query.answer("⚠️ আর সরানো যাচ্ছে না।", show_alert=True)
        library_selected_node.pop(user.id, None)
        return

    # ── Rename ──
    if action == "rename":
        library_selected_node.pop(user.id, None)
        pending_library_build[user.id] = {"step": "awaiting_rename", "node_id": node_id}
        cancel_kbd = ReplyKeyboardMarkup([[KeyboardButton("🚫 Cancel")]], resize_keyboard=True)
        try:
            await query.edit_message_reply_markup(reply_markup=None)
        except Exception:
            pass
        await ctx.bot.send_message(
            chat_id=query.message.chat_id,
            text=f"✏️ *\"{sel_title}\"* এর নতুন নাম লিখো:",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=cancel_kbd
        )
        return

    # ── Edit msg ──
    if action == "edit":
        existing = node.get("text", "")
        library_selected_node.pop(user.id, None)
        pending_library_build[user.id] = {"step": "awaiting_post_text", "parent_id": node_id}
        cancel_kbd = ReplyKeyboardMarkup([[KeyboardButton("🚫 Cancel")]], resize_keyboard=True)
        prompt = (
            f"✏️ *\"{sel_title}\"* এর message লিখো:\n\n"
            "_(Bold, italic, links — সব Telegram formatting কাজ করে)_"
        )
        if existing:
            prompt += f"\n\n📌 *Current message:*\n{existing[:300]}{'...' if len(existing) > 300 else ''}"
        try:
            await query.edit_message_reply_markup(reply_markup=None)
        except Exception:
            pass
        await ctx.bot.send_message(
            chat_id=query.message.chat_id,
            text=prompt,
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=cancel_kbd
        )
        return

    # ── Delete ──
    if action == "delete":
        library_delete_node(node_id)
        library_selected_node.pop(user.id, None)
        try:
            await query.edit_message_reply_markup(reply_markup=None)
        except Exception:
            pass
        await ctx.bot.send_message(
            chat_id=query.message.chat_id,
            text=f"🗑 *\"{sel_title}\"* ডিলিট হয়ে গেছে!",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=_parent_kbd()
        )
        return

    # ── Cut ──
    if action == "cut":
        library_clipboard[user.id] = node_id
        library_selected_node.pop(user.id, None)
        try:
            await query.edit_message_reply_markup(reply_markup=None)
        except Exception:
            pass
        await ctx.bot.send_message(
            chat_id=query.message.chat_id,
            text=(
                f"✂️ *\"{sel_title}\"* clipboard-এ আছে।\n\n"
                "যেখানে paste করবে সেই folder-এ ঢুকে *📋 Paste* চাপো।"
            ),
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=_lib_keyboard_btn_editor_with_paste(current_id, True)
                if current_id != "root" else _lib_keyboard_btn_editor_root("btn")
        )
        return


async def lib_move_callback(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Handle inline button presses for reordering library buttons."""
    query = update.callback_query
    await query.answer()
    user = query.from_user

    if user.id != ADMIN_ID:
        await query.answer("❌ Admin only", show_alert=True)
        return

    data = query.data  # "lib_move:<node_id>:<direction>"
    parts = data.split(":")
    if len(parts) != 3:
        return
    _, node_id, direction = parts

    node = library_data.get(node_id)
    if not node:
        await query.edit_message_text("⚠️ Button not found.")
        return

    parent_id = node.get("parent")
    title = node.get("title", "?")

    if direction == "done":
        await query.edit_message_text(f"✅ Done rearranging *{title}*.", parse_mode=ParseMode.MARKDOWN)
        return

    moved = library_move_node(node_id, direction)
    if not moved:
        await query.answer("⚠️ Cannot move further in that direction.", show_alert=True)
        return

    # Refresh the inline keyboard to reflect new position
    new_move_kbd = _lib_move_keyboard(node_id, parent_id)
    dir_labels = {"up": "⬆️ Up", "down": "⬇️ Down", "left": "⬅️ Left", "right": "➡️ Right"}
    dir_label = dir_labels.get(direction, direction)

    node_text = node.get("text", "")
    info = f"📚 *{title}*"
    if node_text:
        preview = node_text[:150] + ("..." if len(node_text) > 150 else "")
        info += f"\n\n📄 *Current msg:*\n{preview}"
    else:
        info += "\n\n_(No msg yet)_"

    try:
        await query.edit_message_text(
            info + f"\n\n🔀 *Moved {dir_label}! Reorder this button:*",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=new_move_kbd
        )
    except Exception:
        await query.answer(f"✅ Moved {dir_label}!", show_alert=False)


def main():
    # Startup: API pool status দেখাও
    print("=" * 55)
    print(f"🤖  {BOT_NAME} is ONLINE!")
    print(f"🔑  API Pool: {len(API_POOL)} provider(s) active")
    for p in API_POOL:
        print(f"    ✅ {p['label']}")
    if not API_POOL:
        print("    ❌ No API keys found! Set env variables.")
    print("=" * 55)

    from telegram import LinkPreviewOptions
    from telegram.ext import Defaults
    _defaults = Defaults(link_preview_options=LinkPreviewOptions(is_disabled=True))

    # ── Custom HTTP request settings ──
    # Render free instance + Telegram API-র মধ্যে network jitter এর কারণে
    # আগে ছোট default timeout (connect/read ~5s) এ প্রায়ই TimedOut error হচ্ছিলো —
    # এমনকি admin-কে error report পাঠানোর সময়ও একই কারণে সেটা fail করে যাচ্ছিলো।
    # timeout গুলো বাড়িয়ে দেওয়া হলো যাতে সাময়িক network slowness এ request fail না করে।
    _request = HTTPXRequest(
        connect_timeout=15.0,
        read_timeout=30.0,
        write_timeout=30.0,
        pool_timeout=15.0,
    )
    app = (
        Application.builder()
        .token(BOT_TOKEN)
        .defaults(_defaults)
        .request(_request)
        .get_updates_request(HTTPXRequest(
            connect_timeout=15.0,
            read_timeout=40.0,   # long-poll getUpdates এর জন্য বেশি সময় দরকার
            write_timeout=15.0,
            pool_timeout=15.0,
        ))
        .build()
    )
    app.add_handler(MessageHandler(filters.ALL, block_verification_group), group=-1)

    app.add_handler(CommandHandler("start",          start))
    app.add_handler(CommandHandler("help",           help_cmd))
    app.add_handler(CommandHandler("stats",          stats_cmd))
    app.add_handler(CommandHandler("apistatus",      apistatus_cmd))
    app.add_handler(CommandHandler("users",          users_cmd))
    app.add_handler(CommandHandler("userdata",       userdata_cmd))
    app.add_handler(CommandHandler("dailyusage",     dailyusage_cmd))
    app.add_handler(CommandHandler("myreport",       myreport_cmd))
    app.add_handler(CommandHandler("pollsolver",     pollsolver_cmd))
    app.add_handler(CommandHandler("feedback",       feedback_cmd))
    app.add_handler(CommandHandler("contact",        contact_cmd))
    app.add_handler(CommandHandler("broadcast",      broadcast_cmd))
    app.add_handler(CommandHandler("cancel",         cancel_cmd))
    app.add_handler(CommandHandler("setreportgroup", setreportgroup_cmd))
    app.add_handler(CommandHandler("library",        library_cmd))
    app.add_handler(CommandHandler("qbank",           library_cmd))
    app.add_handler(CommandHandler("unlimited_exam",  library_cmd))
    app.add_handler(CommandHandler("getextralimits", get_extra_limits_cmd))
    app.add_handler(CommandHandler("getextralimit",  get_extra_limits_cmd))
    app.add_handler(CommandHandler("addlimit",       addlimit_cmd))
    app.add_handler(CallbackQueryHandler(verify_callback,    pattern="^verify_check$"))
    app.add_handler(CallbackQueryHandler(how_to_use_callback, pattern="^how_to_use$"))
    app.add_handler(CallbackQueryHandler(broadcast_callback, pattern="^bc_"))
    app.add_handler(CallbackQueryHandler(retry_callback,     pattern="^retry_"))
    app.add_handler(CallbackQueryHandler(qa_retry_callback,  pattern="^qaretry_"))
    app.add_handler(CallbackQueryHandler(lqa_callback,       pattern="^lqa:"))
    app.add_handler(CallbackQueryHandler(lib_move_callback,  pattern="^lib_move:"))
    app.add_handler(CallbackQueryHandler(addlimit_type_callback, pattern="^addlim:"))
    # Dedicated poll handler — ensures BOTH forwarded polls AND user-sent polls are solved.
    app.add_handler(MessageHandler(filters.POLL, handle_poll))
    app.add_handler(MessageHandler(filters.ALL & ~filters.COMMAND, msg_handler))
    app.add_handler(ChatMemberHandler(channel_member_update, ChatMemberHandler.CHAT_MEMBER))

    # ── Global error handler ──
    # এর আগে কোনো error handler ছিল না, ফলে handle_poll বা অন্য কোনো হ্যান্ডলারে
    # exception হলে PTB শুধু stderr-এ log করতো, user কিছুই দেখতে পেতো না (silent fail)।
    # এই handler সব uncaught exception ধরে log করবে এবং সম্ভব হলে user-কে জানাবে।
    app.add_error_handler(global_error_handler)

    threading.Thread(target=start_health_server, daemon=True).start()

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    async def _midnight_summary(app):
        import datetime as _dt
        while True:
            dhaka_tz = pytz.timezone("Asia/Dhaka")
            now      = datetime.now(dhaka_tz)
            midnight = (now + _dt.timedelta(days=1)).replace(
                hour=0, minute=0, second=5, microsecond=0
            )
            secs = (midnight - now).total_seconds()
            await asyncio.sleep(secs)

            yesterday       = (datetime.now(dhaka_tz) - _dt.timedelta(days=1)).strftime("%Y-%m-%d")
            yesterday_label = (datetime.now(dhaka_tz) - _dt.timedelta(days=1)).strftime("%d %b %Y")
            data       = daily_stats.get(yesterday, {})
            polls      = data.get("polls_solved", 0)
            active     = len(data.get("active_users", set()))
            total_u    = len(registered_users)
            all_time   = sum(i.get("poll_count", 0) for i in registered_users.values())

            summary = (
                f"🌙 Daily Summary — {yesterday_label}\n"
                f"▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬\n\n"
                f"🧩 Polls Solved: {polls}\n"
                f"👥 Active Users: {active}\n"
                f"📦 Total Users: {total_u}\n"
                f"🏆 All-time Polls: {all_time}\n\n"
                f"▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬\n"
                f"Good morning! New day started. 🌅"
            )
            try:
                await app.bot.send_message(chat_id=ADMIN_ID, text=summary)
            except Exception as e:
                logger.error(f"Midnight summary error: {e}")

    # নোট: পুরনো সাধারণ `_inactivity_reminder` loop-টা (২৪h পর পর, মাত্র ২ ধরনের
    # message) এখন সরিয়ে ফেলা হলো — এর জায়গায় module-level `_engagement_notification_loop`
    # ব্যবহার হচ্ছে, যেটা activity-pattern অনুযায়ী ৭টা category + streak/achievement +
    # study-tip মিশিয়ে, প্রতি user-কে দিনে সর্বোচ্চ ১বার, ভিন্ন ভিন্ন সময়ে পাঠায়
    # (দেখুন: ENGAGEMENT NOTIFICATION SYSTEM সেকশন)।

    async def _morning_notification(app):
        """প্রতিদিন সকাল ৭টায় (Dhaka time) সব registered user-কে morning notification পাঠায়।"""
        import datetime as _dt
        while True:
            dhaka_tz = pytz.timezone("Asia/Dhaka")
            now      = datetime.now(dhaka_tz)
            # আজকে ৭টা হয়ে গেলে পরদিনের ৭টার জন্য অপেক্ষা করো
            today_7am = now.replace(hour=7, minute=0, second=0, microsecond=0)
            if now >= today_7am:
                target = today_7am + _dt.timedelta(days=1)
            else:
                target = today_7am
            secs = (target - now).total_seconds()
            logger.info(f"Morning notification scheduled in {int(secs)}s")
            await asyncio.sleep(secs)

            today = datetime.now(dhaka_tz).strftime("%d %b %Y")
            success, fail = 0, 0
            for uid, info in list(registered_users.items()):
                if uid == ADMIN_ID:
                    continue
                try:
                    eff_limit = get_effective_daily_limit(uid)
                    bot_username = app.bot.username
                    ref_link = f"https://t.me/{bot_username}?start={make_referral_code(uid)}"

                    import urllib.parse as _urlparse
                    share_text = _urlparse.quote(
                        f"🤖 Synthesis Robot দিয়ে সেকেন্ডেই MCQ Poll Solve করো!\n"
                        f"👇 আমার link দিয়ে join করো:\n{ref_link}"
                    )

                    text = (
                        f"🌅 *শুভ সকাল! — {today}*\n"
                        f"▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬\n\n"
                        f"🤖 *Synthesis Robot* তোমার সাথে আছে!\n\n"
                        f"📦 আজকের limit: *{eff_limit}টি poll*\n\n"
                        f"যেকোনো channel বা group-এর poll solve করাতে চাইলে —\n"
                        f"সেটা এখানে *forward* করো, আমি সঙ্গে সঙ্গে solve করে দেবো! ⚡\n\n"
                        f"▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬\n"
                        f"🎁 বেশি limit পেতে বন্ধুকে invite করো!\n"
                        f"প্রতিজন join করলে +1 extra poll পাবে।"
                    )
                    keyboard = [
                        [InlineKeyboardButton("🎁 Extra Limits পাও", url=f"https://t.me/share/url?text={share_text}")],
                        [InlineKeyboardButton("💬 Admission Discussion Group", url=ADMISSION_GROUP_LINK)],
                    ]
                    await app.bot.send_message(
                        chat_id=uid,
                        text=text,
                        parse_mode=ParseMode.MARKDOWN,
                        reply_markup=InlineKeyboardMarkup(keyboard)
                    )
                    success += 1
                except Exception as e:
                    logger.warning(f"Morning notify failed for {uid}: {e}")
                    fail += 1
                await asyncio.sleep(0.05)  # rate limit avoid করতে ছোট delay

            logger.info(f"Morning notification done: {success} sent, {fail} failed")

    async def _poll_broadcast_loop(app):
        """
        প্রতিদিন ৪ বার (সকাল ৭টা, দুপুর ১টা, বিকেল ৫টা, রাত ৯টা — Asia/Dhaka) —
        প্রতিটা registered user-কে broadcast library থেকে randomly একটা poll
        পাঠায়, যেটা সেই user আগে কখনো পায়নি (uniqueness broadcast_sent টেবিল
        দিয়ে track করা হয়)। Library-তে কোনো নতুন (unsent) poll না থাকলে সেই
        user-কে এই round-এ skip করা হয়।
        """
        dhaka_tz     = pytz.timezone("Asia/Dhaka")
        target_hours = [7, 13, 17, 21]
        while True:
            now = datetime.now(dhaka_tz)
            candidates = []
            for h in target_hours:
                t = now.replace(hour=h, minute=0, second=0, microsecond=0)
                if t <= now:
                    t += _dt.timedelta(days=1)
                candidates.append(t)
            target = min(candidates)
            secs = (target - now).total_seconds()
            logger.info(f"📨 Poll broadcast scheduled in {int(secs)}s (at {target.strftime('%d %b %I:%M %p')} Dhaka)")
            await asyncio.sleep(secs)

            lib_size = broadcast_library_size()
            if lib_size == 0:
                logger.info("📨 Poll broadcast skipped — broadcast library এখনো খালি।")
                continue

            sent, skipped, failed = 0, 0, 0
            for uid in list(registered_users.keys()):
                if uid == ADMIN_ID:
                    continue
                poll_data = pick_unsent_poll_for_user(uid)
                if poll_data is None:
                    skipped += 1
                    continue
                try:
                    await app.bot.send_poll(
                        chat_id=uid,
                        question=poll_data["question"],
                        options=poll_data["options"],
                        type="quiz",
                        correct_option_id=poll_data["correct_idx"],
                        explanation=poll_data["explanation"],
                        is_anonymous=True,
                    )
                    record_broadcast_sent(uid, poll_data["poll_key"])
                    sent += 1
                except Exception as e:
                    failed += 1
                    logger.warning(f"Poll broadcast send failed for {uid}: {e}")
                await asyncio.sleep(0.06)  # flood-limit avoid করতে ছোট delay

            logger.info(f"📨 Poll broadcast round done: sent={sent} skipped(no-new-poll)={skipped} failed={failed} (library={lib_size})")

    async def _weekly_report_loop(app):
        """
        প্রতি সপ্তাহে একবার (শুক্রবার সকাল ৯টা, Asia/Dhaka) — প্রতিটা student-কে
        (admin বাদে) তার নিজের সেই সপ্তাহের/lifetime streak ও poll/text/OCR usage
        নিয়ে একটা personalized weekly report পাঠায় — rich message (markdown table)
        formatting-এ। rich message ব্যর্থ হলে plain HTML-এ fallback করে।
        """
        dhaka_tz = pytz.timezone("Asia/Dhaka")
        WEEKLY_REPORT_WEEKDAY = 4   # 0=Monday ... 4=Friday
        WEEKLY_REPORT_HOUR    = 9

        while True:
            now = datetime.now(dhaka_tz)
            days_ahead = (WEEKLY_REPORT_WEEKDAY - now.weekday()) % 7
            target = (now + _dt.timedelta(days=days_ahead)).replace(
                hour=WEEKLY_REPORT_HOUR, minute=0, second=0, microsecond=0
            )
            if target <= now:
                target += _dt.timedelta(days=7)
            secs = (target - now).total_seconds()
            logger.info(f"📈 Weekly report scheduled in {int(secs)}s (at {target.strftime('%d %b %I:%M %p')} Dhaka)")
            await asyncio.sleep(secs)

            sent, failed = 0, 0
            for uid, info in list(registered_users.items()):
                if uid == ADMIN_ID:
                    continue
                try:
                    streak         = info.get("streak", 0)
                    longest_streak = info.get("longest_streak", 0)
                    active_days    = info.get("active_days", 0)
                    badge          = get_streak_badge(streak)
                    badge_line     = f"{badge} " if badge else ""

                    poll_count = info.get("poll_count", 0)
                    qa_count   = info.get("qa_count", 0)
                    ocr_count  = info.get("ocr_count", 0)
                    total_use  = poll_count + qa_count + ocr_count
                    name       = info.get("name", "Student")

                    rich_text = (
                        f"# 📈 তোমার Weekly Report — {name}\n\n"
                        f"## {badge_line}Streak Status\n\n"
                        f"| | |\n"
                        f"| --- | --- |\n"
                        f"| 🔥 বর্তমান Streak | **{streak} দিন** |\n"
                        f"| 🏅 সর্বোচ্চ Streak | **{longest_streak} দিন** |\n"
                        f"| 📅 মোট Active Days | **{active_days} দিন** |\n\n"
                        f"## 📊 মোট ব্যবহার (Lifetime)\n\n"
                        f"| Type | Count |\n"
                        f"| --- | --- |\n"
                        f"| 🧩 Poll Solve | {poll_count} |\n"
                        f"| 💬 Text Q&A | {qa_count} |\n"
                        f"| 🖼 Image (OCR) Q&A | {ocr_count} |\n"
                        f"| 🔢 **সর্বমোট** | **{total_use}** |\n\n"
                        f"---\n\n"
                        f"বিস্তারিত দেখতে /myreport লিখো। প্রতিদিন ব্যবহার করে streak ধরে রাখো! 🚀\n\n"
                        f"🕐 {get_dhaka_time()}"
                    )

                    result = await send_rich_message(uid, rich_text)
                    if result is None:
                        fallback = (
                            f"📈 <b>তোমার Weekly Report — {name}</b>\n"
                            f"▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬\n\n"
                            f"{badge_line}<b>Streak:</b> {streak} দিন (সর্বোচ্চ: {longest_streak} দিন)\n"
                            f"📅 <b>মোট Active Days:</b> {active_days} দিন\n\n"
                            f"🧩 Poll: {poll_count} | 💬 Text: {qa_count} | 🖼 OCR: {ocr_count}\n"
                            f"🔢 সর্বমোট: <b>{total_use}</b>\n\n"
                            f"▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬\n"
                            f"বিস্তারিত দেখতে /myreport লিখো।\n"
                            f"🕐 {get_dhaka_time()}"
                        )
                        await app.bot.send_message(chat_id=uid, text=fallback, parse_mode="HTML")
                    sent += 1
                except Exception as e:
                    failed += 1
                    logger.warning(f"Weekly report send failed for {uid}: {e}")
                await asyncio.sleep(0.06)  # flood-limit avoid করতে ছোট delay

            logger.info(f"📈 Weekly report round done: sent={sent} failed={failed}")

    async def _memory_cleanup(app):
        """
        Periodic cleanup (প্রতি ১ ঘণ্টা পর পর):
          1. retry_poll_data থেকে RETRY_DATA_TTL_SECS-এর চেয়ে পুরোনো entry সরায় (memory leak fix)।
          2. poll_cache (local SQLite + Turso) থেকে POLL_CACHE_TTL_SECS-এর চেয়ে পুরোনো,
             কম-ব্যবহৃত cache entry মুছে ফেলে যাতে database/RAM বাড়তে না থাকে।

          NOTE: pending_referrals ইচ্ছাকৃতভাবে এখানে clean করা হয় না — referral link
          কখনো expire হয় না, friend ৫ দিন পরে join করলেও referrer বোনাস পাবে। এই dict
          এমনিতেই self-bounded (প্রতি user_id-এর জন্য একটা entry, verify হলে নিজে থেকেই
          মুছে যায়), তাই এটা leak তৈরি করে না।
        """
        CLEANUP_INTERVAL = 3600  # প্রতি ১ ঘণ্টা
        while True:
            await asyncio.sleep(CLEANUP_INTERVAL)
            now = time.time()

            # ── retry_poll_data cleanup ──
            try:
                stale_retry_ids = [
                    rid for rid, info in list(retry_poll_data.items())
                    if now - info.get("_created_at", 0) > RETRY_DATA_TTL_SECS
                ]
                for rid in stale_retry_ids:
                    retry_poll_data.pop(rid, None)
                if stale_retry_ids:
                    logger.info(f"🧹 Cleaned {len(stale_retry_ids)} stale retry_poll_data entries")
            except Exception as e:
                logger.error(f"retry_poll_data cleanup error: {e}")

            # ── poll_cache expiry (local + Turso) ──
            try:
                cutoff = now - POLL_CACHE_TTL_SECS
                conn = _get_cache_conn()
                cur = conn.execute(
                    "DELETE FROM poll_cache WHERE last_hit_at < ?", (cutoff,)
                )
                conn.commit()
                deleted = cur.rowcount if cur.rowcount and cur.rowcount > 0 else 0
                if deleted:
                    logger.info(f"🧹 Cleaned {deleted} expired poll_cache entries (local)")
                _turso_bg(lambda: turso_exec(
                    "DELETE FROM poll_cache WHERE last_hit_at < ?", (cutoff,)
                ), "cleanup_poll_cache")
            except Exception as e:
                logger.error(f"poll_cache cleanup error: {e}")

    async def _run():
        _set_bot_app_ref(app)  # background context (call_ai ইত্যাদি) থেকে admin notify পাঠানোর জন্য
        async with app:
            await app.initialize()
            await app.start()

            # Turso: schema init then load all persistent data
            await init_turso_schema()
            await load_from_turso()
            await load_poll_cache_from_turso()
            await load_broadcast_data_from_turso()
            resanitize_broadcast_library()  # পুরনো/miss হওয়া source-tag, link cleanup (idempotent)
            await load_library_from_turso()
            await load_settings_from_turso()
            await load_provider_stats_from_turso()
            await load_lifetime_usage_totals_from_turso()
            logger.info(f"Turso load done: {len(registered_users)} users, {len(daily_stats)} stat-days")

            # ── Admin-কে restart/redeploy notification পাঠানো ──
            try:
                dhaka_tz = pytz.timezone("Asia/Dhaka")
                now_str  = datetime.now(dhaka_tz).strftime("%d %b %Y, %I:%M:%S %p")
                startup_msg = (
                    f"✅ <b>Bot Restarted / Redeployed</b>\n"
                    f"▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬\n"
                    f"🤖 <b>{BOT_NAME}</b>\n"
                    f"🕐 <b>Time:</b> {now_str}\n"
                    f"👥 <b>Loaded Users:</b> {len(registered_users)}\n"
                    f"🔑 <b>API Pool:</b> {len(API_POOL)} active provider(s)"
                )
                await app.bot.send_message(chat_id=ADMIN_ID, text=startup_msg, parse_mode="HTML")
            except Exception as e:
                logger.error(f"Startup notification failed: {e}")

            asyncio.create_task(_midnight_summary(app))
            asyncio.create_task(_engagement_notification_loop(app))
            asyncio.create_task(_morning_notification(app))
            asyncio.create_task(_poll_broadcast_loop(app))
            asyncio.create_task(_weekly_report_loop(app))
            asyncio.create_task(_memory_cleanup(app))
            asyncio.create_task(_turso_retry_loop())
            await app.updater.start_polling(
                drop_pending_updates=True,
                allowed_updates=["message", "edited_message", "channel_post", "callback_query", "chat_member", "poll", "poll_answer"]
            )
            await asyncio.Event().wait()

    loop.run_until_complete(_run())

if __name__ == "__main__":
    main()
