from flask import jsonify, Flask, render_template, request, redirect, url_for, session, flash
import sqlite3
import json
import os
import re
import asyncio
import uuid
from datetime import datetime, timezone, timedelta
from threading import Lock

# ======================
# 基础配置
# ======================
CHINA_TZ = timezone(timedelta(hours=8))

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_FILE = os.path.join(BASE_DIR, "history.db")
REPORTS_FILE = os.path.join(BASE_DIR, "reports.json")
CHANNELS_FILE = os.path.join(BASE_DIR, "channels.json")

RISK_KEYWORDS = [k.strip() for k in os.environ.get("RISK_KEYWORDS", "曝光,骗子,反诈,诈骗,黑名单,避雷").split(",") if k.strip()]

LIVE_SCAN_MAX_CHATS = int(os.environ.get("LIVE_SCAN_MAX_CHATS", "20"))
LIVE_SCAN_LIMIT_PER_CHAT = int(os.environ.get("LIVE_SCAN_LIMIT_PER_CHAT", "3"))
LIVE_SCAN_TIMEOUT_SEC = int(os.environ.get("LIVE_SCAN_TIMEOUT_SEC", "12"))

# Telethon（优先环境变量；没有就自动从 telethon.json / tg.py / zpf_web.py / zpf.py 等文件里读取）
TELETHON_API_ID = os.environ.get("TELETHON_API_ID", "").strip()
TELETHON_API_HASH = os.environ.get("TELETHON_API_HASH", "").strip()
WEB_TELETHON_SESSION_FILE = os.environ.get("WEB_TELETHON_SESSION_FILE", "").strip()  # 例：/app/web_scan.session

# 功能开关 / 管理
CHATLOG_FEATURE_DEFAULT = os.environ.get("CHATLOG_FEATURE_DEFAULT", "1").strip() not in ("0", "false", "False", "no", "NO")
TOKEN_TTL_SEC = int(os.environ.get("CHATLOG_TOKEN_TTL_SEC", "900"))  # 机器人跳转链接有效期（秒）

ADMIN_PASSWORD = os.environ.get("ADMIN_PASSWORD", "").strip()  # 不设置则禁用管理后台
FLASK_SECRET_KEY = os.environ.get("FLASK_SECRET_KEY", "").strip()

# 网页版强制验证（加入频道 + 机器人验证）
WEB_VERIFY_ENABLED = os.environ.get("WEB_VERIFY_ENABLED", "1").strip() not in ("0", "false", "False", "no", "NO")
WEB_REQUIRED_CHANNEL = (os.environ.get("WEB_REQUIRED_CHANNEL", "your_channel") or "your_channel").strip().lstrip('@')
WEB_REQUIRED_CHANNEL_URL = os.environ.get("WEB_REQUIRED_CHANNEL_URL", "").strip() or f"https://t.me/{WEB_REQUIRED_CHANNEL}"
WEB_VERIFY_TTL_SEC = int(os.environ.get("WEB_VERIFY_TTL_SEC", str(30*24*3600)))  # 30 天
BOT_USERNAME = os.environ.get("BOT_USERNAME", "").strip().lstrip('@')
BOT_USERNAME_FILE = os.environ.get("BOT_USERNAME_FILE", os.path.join(BASE_DIR, "bot_username.txt")).strip()



db_lock = Lock()


def get_db_connection():
    conn = sqlite3.connect(DB_FILE, timeout=15)
    conn.row_factory = sqlite3.Row
    return conn


def db_exists():
    return os.path.exists(DB_FILE)


def ensure_extra_tables():
    """为“聊天记录查询/跳转/开关”补充表结构（不影响旧库）。"""
    if not db_exists():
        return
    with db_lock:
        conn = get_db_connection()
        c = conn.cursor()
        c.execute(
            """
            CREATE TABLE IF NOT EXISTS system_settings (
                key TEXT PRIMARY KEY,
                value TEXT,
                updated INTEGER NOT NULL
            )
            """
        )
        c.execute(
            """
            CREATE TABLE IF NOT EXISTS web_query_tokens (
                token TEXT PRIMARY KEY,
                target_user_id INTEGER NOT NULL,
                requester_user_id INTEGER,
                created INTEGER NOT NULL,
                expires INTEGER NOT NULL
            )
            """
        )
        c.execute(
            """
            CREATE TABLE IF NOT EXISTS web_verify_states (
                state TEXT PRIMARY KEY,
                tg_user_id INTEGER,
                next_path TEXT,
                created INTEGER NOT NULL,
                expires INTEGER NOT NULL,
                used INTEGER NOT NULL DEFAULT 0,
                approved INTEGER NOT NULL DEFAULT 0
            )
            """
        )
        c.execute(
            """
            CREATE TABLE IF NOT EXISTS web_verified_users (
                tg_user_id INTEGER PRIMARY KEY,
                verified_at INTEGER NOT NULL,
                expires INTEGER NOT NULL
            )
            """
        )

        now = int(datetime.now(timezone.utc).timestamp())
        c.execute(
            "INSERT OR IGNORE INTO system_settings (key, value, updated) VALUES (?, ?, ?)",
            ("chatlog_feature_enabled", "1" if CHATLOG_FEATURE_DEFAULT else "0", now),
        )
        
        # --- web verify schema upgrades (safe for existing DB) ---
        try:
            c.execute("ALTER TABLE web_verify_states ADD COLUMN approved INTEGER NOT NULL DEFAULT 0")
        except sqlite3.OperationalError:
            pass
        
        c.execute(
            """
            CREATE TABLE IF NOT EXISTS web_verify_pending (
                state TEXT PRIMARY KEY,
                tg_user_id INTEGER NOT NULL,
                created INTEGER NOT NULL,
                expires INTEGER NOT NULL,
                notified INTEGER NOT NULL DEFAULT 0
            )
            """
        )
        
        conn.commit()
        conn.close()


def load_json_safe(path, default):
    if not os.path.exists(path):
        return default
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return default


def load_reports():
    data = load_json_safe(REPORTS_FILE, {"pending": {}, "verified": {}})
    if "pending" not in data:
        data["pending"] = {}
    if "verified" not in data:
        data["verified"] = {}
    return data



def load_monitored_channels():
    """Load monitored channels/groups for live scan.

    Prefer the SQLite table `monitor_channels` (used by the bot) when available.
    Fall back to channels.json for backward compatibility.
    """
    # 1) Prefer DB table (created/managed by the bot)
    if db_exists():
        try:
            with db_lock:
                conn = get_db_connection()
                c = conn.cursor()
                c.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='monitor_channels'")
                has_tbl = c.fetchone() is not None
                if has_tbl:
                    c.execute("SELECT channel FROM monitor_channels ORDER BY added_at ASC")
                    rows = c.fetchall()
                    conn.close()

                    out = []
                    for r in rows:
                        try:
                            ch = r["channel"]
                        except Exception:
                            ch = r[0] if r else None
                        if ch is None:
                            continue
                        ch = str(ch).strip()
                        if not ch:
                            continue
                        if ch.startswith("@"):
                            out.append(ch)
                        elif ch.isdigit() or (ch.startswith("-") and ch[1:].isdigit()):
                            try:
                                out.append(int(ch))
                            except Exception:
                                pass
                        else:
                            out.append("@" + ch.lstrip("@"))
                    if out:
                        return out
                else:
                    conn.close()
        except Exception:
            try:
                conn.close()
            except Exception:
                pass

    # 2) Fallback: JSON file
    channels = load_json_safe(CHANNELS_FILE, [])
    return channels if isinstance(channels, list) else []

def is_exposure_chat(title: str) -> bool:
    t = (title or "").strip()
    if not t:
        return False
    return any(k in t for k in RISK_KEYWORDS)


def tme_link(chat_id: int, chat_username: str, msg_id: int) -> str:
    if chat_username:
        u = chat_username.lstrip("@")
        return f"https://t.me/{u}/{msg_id}"
    cid = abs(int(chat_id))
    s = str(cid)
    if s.startswith("100"):  # 兼容 -100xxxx
        s = s[3:]
    return f"https://t.me/c/{s}/{msg_id}"


def chat_open_link(chat_id: int, chat_username: str) -> str:
    """Return a best-effort Telegram link to open the chat/channel itself.
    - Public: https://t.me/<username>
    - Private supergroup/channel (no username): https://t.me/c/<internal_id>/1
    """
    if chat_username:
        u = str(chat_username).lstrip("@")
        return f"https://t.me/{u}"
    try:
        cid = abs(int(chat_id))
        s = str(cid)
        if s.startswith("100"):  # -100xxxx -> internal id
            s = s[3:]
            return f"https://t.me/c/{s}/1"
    except Exception:
        pass
    return ""


def get_bot_username() -> str:
    u = (BOT_USERNAME or "").strip().lstrip("@")
    if u:
        return u
    try:
        if BOT_USERNAME_FILE and os.path.exists(BOT_USERNAME_FILE):
            with open(BOT_USERNAME_FILE, "r", encoding="utf-8") as f:
                u2 = (f.read() or "").strip().lstrip("@")
                if u2:
                    return u2
    except Exception:
        pass
    return ""
def get_setting(key: str, default=None):
    if not db_exists():
        return default
    ensure_extra_tables()
    with db_lock:
        conn = get_db_connection()
        c = conn.cursor()
        try:
            c.execute("SELECT value FROM system_settings WHERE key = ?", (key,))
            row = c.fetchone()
        except sqlite3.OperationalError:
            row = None
        conn.close()
    if not row:
        return default
    return row["value"]


def set_setting(key: str, value: str):
    if not db_exists():
        return False
    ensure_extra_tables()
    with db_lock:
        conn = get_db_connection()
        c = conn.cursor()
        now = int(datetime.now(timezone.utc).timestamp())
        c.execute(
            "INSERT INTO system_settings (key, value, updated) VALUES (?, ?, ?) "
            "ON CONFLICT(key) DO UPDATE SET value=excluded.value, updated=excluded.updated",
            (key, str(value), now),
        )
        conn.commit()
        conn.close()
    return True


def is_chatlog_enabled() -> bool:
    v = get_setting("chatlog_feature_enabled", None)
    if v is None:
        return CHATLOG_FEATURE_DEFAULT
    return str(v).strip() not in ("0", "false", "False", "no", "NO")


def create_web_token(target_user_id: int, requester_user_id: int | None, ttl_sec: int = TOKEN_TTL_SEC) -> str:
    ensure_extra_tables()
    token = YOUR_TELEGRAM_BOT_TOKEN
    now = int(datetime.now(timezone.utc).timestamp())
    expires = now + max(60, int(ttl_sec))
    with db_lock:
        conn = get_db_connection()
        c = conn.cursor()
        c.execute(
            "INSERT INTO web_query_tokens (token, target_user_id, requester_user_id, created, expires) VALUES (?, ?, ?, ?, ?)",
            (token, int(target_user_id), int(requester_user_id) if requester_user_id is not None else None, now, expires),
        )
        conn.commit()
        conn.close()
    return token


def get_token_target(token: str):
    ensure_extra_tables()
    now = int(datetime.now(timezone.utc).timestamp())
    with db_lock:
        conn = get_db_connection()
        c = conn.cursor()
        c.execute("SELECT * FROM web_query_tokens WHERE token = ?", (token,))
        row = c.fetchone()
        if row and int(row["expires"]) < now:
            # 过期清理
            c.execute("DELETE FROM web_query_tokens WHERE token = ?", (token,))
            conn.commit()
            conn.close()
            return None
        conn.close()
    return dict(row) if row else None


def resolve_query_to_id(query: str):
    if not query:
        return None
    q_norm = query.strip().lower().lstrip("@")

    with db_lock:
        conn = get_db_connection()
        c = conn.cursor()

        if q_norm.isdigit():
            uid = int(q_norm)
            c.execute("SELECT 1 FROM users WHERE user_id = ?", (uid,))
            if c.fetchone():
                conn.close()
                return uid

        c.execute("SELECT user_id FROM users WHERE LOWER(username) = ?", (q_norm,))
        row = c.fetchone()
        if row:
            conn.close()
            return row["user_id"]

        pattern = f'%"{q_norm}"%'
        c.execute("SELECT user_id FROM users WHERE active_usernames_json LIKE ?", (pattern,))
        row = c.fetchone()
        if row:
            conn.close()
            return row["user_id"]

        c.execute(
            """
            SELECT user_id FROM username_history
            WHERE LOWER(new_username) = ?
            ORDER BY change_date DESC
            LIMIT 1
            """,
            (q_norm,),
        )
        row = c.fetchone()
        conn.close()
        if row:
            return row["user_id"]

    return None


def query_user_profile(user_id: int):
    with db_lock:
        conn = get_db_connection()
        c = conn.cursor()
        c.execute("SELECT * FROM users WHERE user_id = ?", (user_id,))
        profile = c.fetchone()
        conn.close()
    if not profile:
        return None
    return {"user_id": user_id, "current_profile": profile}


def search_history_mentions(user_id=None, usernames=None, limit_per_keyword=15):
    """旧逻辑：根据关键词（ID/用户名）在 message_history.text 中模糊匹配“提及记录”。"""
    if not user_id and not usernames:
        return []

    usernames = usernames or []
    keywords = set()

    if user_id:
        keywords.add(str(user_id))

    for u in usernames:
        if not u:
            continue
        name = u.strip()
        if not name:
            continue
        if name.startswith("@"):
            keywords.add(name)
            keywords.add(name[1:])
        else:
            keywords.add(name)
            keywords.add("@" + name)

    if not keywords:
        return []

    results = {}
    with db_lock:
        conn = get_db_connection()
        c = conn.cursor()
        for kw in keywords:
            like_kw = f"%{kw}%"
            c.execute(
                """
                SELECT m.chat_id, m.message_id, m.text, m.link, m.message_date,
                       ci.title, ci.username
                FROM message_history m
                LEFT JOIN chat_info ci ON ci.chat_id = m.chat_id
                WHERE m.text LIKE ?
                ORDER BY m.message_date DESC
                LIMIT ?
                """,
                (like_kw, limit_per_keyword),
            )
            for row in c.fetchall():
                key = (row["chat_id"], row["message_id"])
                if key in results:
                    continue
                results[key] = {
                    "chat_id": row["chat_id"],
                    "message_id": row["message_id"],
                    "text": (row["text"] or "").strip(),
                    "link": row["link"] or "",
                    "message_date": row["message_date"],
                    "chat_title": row["title"] or "频道/群",
                    "chat_username": row["username"] or "",
                }
        conn.close()

    out = sorted(results.values(), key=lambda r: r.get("message_date", 0), reverse=True)
    for m in out:
        if not m.get("link"):
            try:
                m["link"] = tme_link(m["chat_id"], m.get("chat_username", ""), m["message_id"])
            except Exception:
                pass
        ts = m.get("message_date") or 0
        try:
            dt = datetime.fromtimestamp(int(ts), tz=CHINA_TZ)
            m["dt_str"] = dt.strftime("%m-%d %H:%M")
        except Exception:
            m["dt_str"] = ""
    return out


def query_user_chat_summary(user_id: int, keyword: str | None = None, limit: int = 200):
    if not db_exists():
        return []

    kw = (keyword or "").strip()
    with db_lock:
        conn = get_db_connection()
        c = conn.cursor()
        if kw:
            like_kw = f"%{kw}%"
            c.execute(
                """
                SELECT m.chat_id,
                       COALESCE(ci.title, '频道/群') AS title,
                       COALESCE(ci.username, '') AS username,
                       COUNT(*) AS msg_count,
                       MAX(m.message_date) AS last_date
                FROM message_history m
                LEFT JOIN chat_info ci ON ci.chat_id = m.chat_id
                WHERE m.user_id = ? AND (m.text LIKE ?)
                GROUP BY m.chat_id
                ORDER BY last_date DESC
                LIMIT ?
                """,
                (int(user_id), like_kw, int(limit)),
            )
        else:
            c.execute(
                """
                SELECT m.chat_id,
                       COALESCE(ci.title, '频道/群') AS title,
                       COALESCE(ci.username, '') AS username,
                       COUNT(*) AS msg_count,
                       MAX(m.message_date) AS last_date
                FROM message_history m
                LEFT JOIN chat_info ci ON ci.chat_id = m.chat_id
                WHERE m.user_id = ?
                GROUP BY m.chat_id
                ORDER BY last_date DESC
                LIMIT ?
                """,
                (int(user_id), int(limit)),
            )
        rows = c.fetchall()
        conn.close()

    out = []
    for r in rows:
        last_ts = int(r["last_date"] or 0)
        last_str = ""
        if last_ts:
            try:
                last_str = datetime.fromtimestamp(last_ts, tz=CHINA_TZ).strftime("%Y-%m-%d %H:%M")
            except Exception:
                last_str = ""
        out.append(
            {
                "chat_id": int(r["chat_id"]),
                "title": r["title"] or "频道/群",
                "username": r["username"] or "",
                "chat_url": chat_open_link(int(r["chat_id"]), r["username"] or ""),
                "chat_title": r["title"] or "频道/群",
                "chat_username": r["username"] or "",
                "msg_count": int(r["msg_count"] or 0),
                "last_date": last_ts,
                "last_str": last_str,
            }
        )
    return out


def query_user_messages(user_id: int, chat_id: int | None = None, keyword: str | None = None, page: int = 1, per_page: int = 30):
    if not db_exists():
        return {"items": [], "total": 0}

    page = max(1, int(page or 1))
    per_page = max(10, min(100, int(per_page or 30)))
    offset = (page - 1) * per_page

    kw = (keyword or "").strip()

    with db_lock:
        conn = get_db_connection()
        c = conn.cursor()

        where = ["m.user_id = ?"]
        params = [int(user_id)]
        if chat_id is not None:
            where.append("m.chat_id = ?")
            params.append(int(chat_id))
        if kw:
            where.append("m.text LIKE ?")
            params.append(f"%{kw}%")

        where_sql = " AND ".join(where)

        c.execute(f"SELECT COUNT(*) AS cnt FROM message_history m WHERE {where_sql}", tuple(params))
        total = int(c.fetchone()["cnt"])

        c.execute(
            f"""
            SELECT m.chat_id, m.message_id, m.text, m.link, m.message_date,
                   COALESCE(ci.title, '频道/群') AS title,
                   COALESCE(ci.username, '') AS username
            FROM message_history m
            LEFT JOIN chat_info ci ON ci.chat_id = m.chat_id
            WHERE {where_sql}
            ORDER BY m.message_date DESC
            LIMIT ? OFFSET ?
            """,
            tuple(params + [per_page, offset]),
        )
        rows = c.fetchall()
        conn.close()

    items = []
    for row in rows:
        ts = int(row["message_date"] or 0)
        dt_str = ""
        if ts:
            try:
                dt_str = datetime.fromtimestamp(ts, tz=CHINA_TZ).strftime("%m-%d %H:%M")
            except Exception:
                dt_str = ""

        link = row["link"] or ""
        if not link:
            try:
                link = tme_link(int(row["chat_id"]), row["username"] or "", int(row["message_id"]))
            except Exception:
                link = ""

        items.append(
            {
                "chat_id": int(row["chat_id"]),
                "chat_title": row["title"] or "频道/群",
                "chat_username": row["username"] or "",
                "chat_url": chat_open_link(int(row["chat_id"]), row["username"] or ""),
                "message_id": int(row["message_id"]),
                "text": (row["text"] or "").strip(),
                "message_date": ts,
                "dt_str": dt_str,
                "link": link,
            }
        )

    return {"items": items, "total": total, "page": page, "per_page": per_page}


def get_verified_entry(user_id, usernames):
    reports = load_reports()
    verified = reports.get("verified", {})
    key = str(user_id) if user_id is not None else None

    if key and key in verified:
        return verified[key]

    lower_usernames = {u.lower().lstrip("@") for u in (usernames or []) if u}
    for entry in verified.values():
        entry_ids = {str(uid) for uid in entry.get("user_ids", []) if uid is not None}
        entry_names = {(u or "").lower().lstrip("@") for u in entry.get("usernames", [])}
        if (key and key in entry_ids) or (lower_usernames & entry_names):
            return entry

    return None


def query_basic_stats():
    if not db_exists():
        return {}
    with db_lock:
        conn = get_db_connection()
        c = conn.cursor()
        c.execute("SELECT COUNT(*) AS cnt FROM users")
        total_users = c.fetchone()["cnt"]
        try:
            c.execute("SELECT COUNT(*) AS cnt FROM bot_interactors")
            interacted_users = c.fetchone()["cnt"]
        except sqlite3.OperationalError:
            interacted_users = 0
        try:
            c.execute("SELECT COUNT(*) AS cnt FROM message_history")
            total_messages = c.fetchone()["cnt"]
        except sqlite3.OperationalError:
            total_messages = 0
        conn.close()
    reports = load_reports()
    verified_count = len(reports.get("verified", {}))

    # db size
    db_bytes = os.path.getsize(DB_FILE) if os.path.exists(DB_FILE) else 0

    return {
        "total_users": total_users,
        "interacted_users": interacted_users,
        "total_messages": total_messages,
        "verified_count": verified_count,
        "db_bytes": db_bytes,
    }


def query_leaderboard(limit=10):
    if not db_exists():
        return []
    with db_lock:
        conn = get_db_connection()
        c = conn.cursor()
        try:
            c.execute(
                """
                SELECT s.user_id, s.total_amount_usdt, u.first_name, u.last_name
                FROM sponsors s
                LEFT JOIN users u ON s.user_id = u.user_id
                ORDER BY s.total_amount_usdt DESC
                LIMIT ?
                """,
                (limit,),
            )
            rows = c.fetchall()
        except sqlite3.OperationalError:
            rows = []
        conn.close()
    return rows

def infer_possible_ids_from_text(text: str):
    if not text:
        return []
    candidates = set()
    for m in re.findall(r"\bID\s*[:：]\s*(\d{5,})\b", text, flags=re.IGNORECASE):
        candidates.add(m)
    for m in re.findall(r"\bid(\d{6,})\b", text, flags=re.IGNORECASE):
        candidates.add(m)
    out = []
    for c in candidates:
        if 5 <= len(c) <= 12:
            out.append(int(c))
    return sorted(set(out))


# ======================
# Telethon 凭据自动读取
# ======================

def _read_creds_from_json():
    for fn in ("telethon.json", "config.json", "settings.json"):
        p = os.path.join(BASE_DIR, fn)
        if not os.path.exists(p):
            continue
        try:
            with open(p, "r", encoding="utf-8") as f:
                data = json.load(f)
            api_id = str(data.get("api_id") or data.get("TELETHON_API_ID") or "").strip()
            api_hash = str(data.get("api_hash") or data.get("TELETHON_API_HASH") or "").strip()
            if api_id.isdigit() and api_hash:
                return int(api_id), api_hash, fn
        except Exception:
            continue
    return None


def _read_creds_from_config_dict(txt: str):
    """
    支持 zpf_web.py 这种：
      CONFIG = { "api_id": 12345678, "api_hash": "xxx", ... }
    """
    m = re.search(r"\bCONFIG\s*=\s*\{([\s\S]{0,2000})\}", txt)
    if not m:
        return None
    block = m.group(0)
    mid = re.search(r"['\"]api_id['\"]\s*:\s*(\d{3,})", block)
    mh = re.search(r"['\"]api_hash['\"]\s*:\s*['\"]([^'\"]{10,})['\"]", block)
    if mid and mh:
        return int(mid.group(1)), mh.group(1)
    return None


def _read_creds_from_pyfiles():
    candidates = ["zpf_web.py", "tg.py", "zpf.py"]
    for fn in candidates:
        p = os.path.join(BASE_DIR, fn)
        if not os.path.exists(p):
            continue
        try:
            txt = open(p, "r", encoding="utf-8", errors="ignore").read()
        except Exception:
            continue

        c = _read_creds_from_config_dict(txt)
        if c:
            return c[0], c[1], fn

        m_id = re.search(r"\b(api[_-]?id|API[_-]?ID)\s*=\s*(\d{3,})\b", txt)
        m_hash = re.search(r"\b(api[_-]?hash|API[_-]?HASH)\s*=\s*['\"]([^'\"]{10,})['\"]", txt)
        if m_id and m_hash:
            try:
                return int(m_id.group(2)), m_hash.group(2), fn
            except Exception:
                pass

        m_tc = re.search(r"TelegramClient\([^,]+,\s*(\d{3,})\s*,\s*['\"]([^'\"]{10,})['\"]\s*\)", txt)
        if m_tc:
            try:
                return int(m_tc.group(1)), m_tc.group(2), fn
            except Exception:
                pass

    return None


def load_telethon_credentials():
    if TELETHON_API_ID.isdigit() and TELETHON_API_HASH:
        return int(TELETHON_API_ID), TELETHON_API_HASH, "env"
    j = _read_creds_from_json()
    if j:
        return j
    p = _read_creds_from_pyfiles()
    if p:
        return p
    return None


def _pick_or_copy_session_file():
    if WEB_TELETHON_SESSION_FILE:
        return WEB_TELETHON_SESSION_FILE
    target = os.path.join(BASE_DIR, "web_scan.session")
    if os.path.exists(target):
        return target
    sess = None
    for fn in os.listdir(BASE_DIR):
        if fn.endswith(".session") and fn != "web_scan.session":
            sess = os.path.join(BASE_DIR, fn)
            break
    if sess:
        try:
            import shutil

            shutil.copy2(sess, target)
            return target
        except Exception:
            return sess
    return target


def _normalize_targets(channels):
    targets = []
    if not isinstance(channels, list):
        return targets

    for it in channels:
        if isinstance(it, dict):
            cid = it.get("chat_id") or it.get("id") or it.get("channel_id")
            uname = it.get("username") or it.get("chat_username") or ""
            title = it.get("title") or it.get("name") or ""

            if isinstance(cid, str) and cid.strip().startswith("@") and not uname:
                uname = cid.strip()
                cid = None

            targets.append({"chat_id": cid, "username": uname, "title": title})
        else:
            if isinstance(it, str) and it.strip().startswith("@"):
                targets.append({"chat_id": None, "username": it.strip(), "title": ""})
            else:
                targets.append({"chat_id": it, "username": "", "title": ""})
    return targets


async def _live_scan_async(keywords, max_chats, limit_per_chat, timeout_sec):
    creds = load_telethon_credentials()
    if not creds:
        return {"ok": False, "error": "未配置扫描账号"}

    api_id, api_hash, src = creds
    try:
        from telethon import TelegramClient
    except Exception:
        return {"ok": False, "error": "缺少 telethon"}

    session_file = _pick_or_copy_session_file()

    channels = load_monitored_channels()
    targets = _normalize_targets(channels)[:max_chats]

    results = {}
    async with TelegramClient(session_file, int(api_id), api_hash) as client:

        async def scan_one_target(t):
            try:
                entity = None
                uname = (t.get("username") or "").strip()
                cid = t.get("chat_id")

                if uname:
                    entity = await client.get_entity(uname.lstrip("@"))
                else:
                    if isinstance(cid, str) and cid.strip().startswith("@"):
                        entity = await client.get_entity(cid.strip().lstrip("@"))
                    else:
                        entity = await client.get_entity(int(cid))

                chat_title = getattr(entity, "title", None) or getattr(entity, "first_name", "") or t.get("title", "") or "频道/群"
                chat_username = getattr(entity, "username", "") or ""

                for kw in keywords:
                    async for msg in client.iter_messages(entity, search=kw, limit=limit_per_chat):
                        key = (int(getattr(entity, "id", 0)), int(msg.id))
                        if key in results:
                            continue
                        text = (msg.message or "").strip()
                        dt = msg.date.astimezone(CHINA_TZ) if msg.date else None
                        dt_str = dt.strftime("%m-%d %H:%M") if dt else ""
                        link = ""
                        try:
                            chat_id = int(getattr(entity, "id", 0))
                            link = tme_link(chat_id, chat_username, msg.id)
                        except Exception:
                            link = ""

                        results[key] = {
                            "chat_id": int(getattr(entity, "id", 0)),
                            "message_id": int(msg.id),
                            "text": text,
                            "link": link,
                            "dt_str": dt_str,
                            "chat_title": chat_title,
                            "chat_username": chat_username,
                            "exposure": is_exposure_chat(chat_title),
                        }
            except Exception:
                return

        tasks = [scan_one_target(t) for t in targets]
        try:
            await asyncio.wait_for(asyncio.gather(*tasks), timeout=timeout_sec)
        except asyncio.TimeoutError:
            pass

    out = list(results.values())
    out.sort(key=lambda x: (not x.get("exposure", False), x.get("dt_str", "")))
    return {"ok": True, "hits": out[:30], "src": src}


def live_scan(keywords):
    return asyncio.run(_live_scan_async(keywords, LIVE_SCAN_MAX_CHATS, LIVE_SCAN_LIMIT_PER_CHAT, LIVE_SCAN_TIMEOUT_SEC))


def build_keywords(query: str, resolved_id):
    q = (query or "").strip()
    kws = set()
    if not q:
        return []
    if q.isdigit():
        kws.add(q)
    else:
        name = q.lstrip("@")
        kws.add(name)
        kws.add("@" + name)
    if resolved_id is not None:
        kws.add(str(resolved_id))
    return [k for k in kws if len(k) >= 3]


# ======================
# Admin helpers
# ======================

def require_admin():
    if not ADMIN_PASSWORD:
        return False
    return bool(session.get("is_admin"))


def delete_oldest_messages(count: int) -> int:
    if not db_exists():
        return 0
    count = max(0, int(count))
    if count <= 0:
        return 0
    with db_lock:
        conn = get_db_connection()
        c = conn.cursor()
        try:
            c.execute(
                "DELETE FROM message_history WHERE rowid IN (SELECT rowid FROM message_history ORDER BY message_date ASC LIMIT ?)",
                (count,),
            )
            deleted = c.rowcount if c.rowcount is not None else 0
            conn.commit()
        except sqlite3.OperationalError:
            deleted = 0
        conn.close()
    return int(deleted)


def delete_messages_by_keywords(keywords: list[str]) -> int:
    if not db_exists():
        return 0
    kws = [k.strip() for k in (keywords or []) if k and k.strip()]
    if not kws:
        return 0

    deleted_total = 0
    with db_lock:
        conn = get_db_connection()
        c = conn.cursor()
        for kw in kws:
            like_kw = f"%{kw}%"
            try:
                c.execute("DELETE FROM message_history WHERE text LIKE ?", (like_kw,))
                if c.rowcount:
                    deleted_total += int(c.rowcount)
            except sqlite3.OperationalError:
                continue
        conn.commit()
        conn.close()
    return deleted_total


def vacuum_db():
    if not db_exists():
        return False
    with db_lock:
        conn = get_db_connection()
        try:
            conn.execute("VACUUM")
            conn.commit()
        except Exception:
            pass
        conn.close()
    return True


# ======================
# Flask
# ======================
app = Flask(__name__)
app.secret_key = FLASK_SECRET_KEY or os.urandom(24)


@app.context_processor
def inject_globals():
    return {
        "chatlog_enabled": is_chatlog_enabled(),
        "admin_enabled": bool(ADMIN_PASSWORD),
        "is_admin": require_admin(),
    }


@app.after_request
def add_no_cache_headers(resp):
    """避免浏览器缓存敏感页（尤其是手机浏览器的 BFCache/返回缓存）导致看起来像绕过验证。"""
    try:
        p = request.path or ""
        # 静态资源允许缓存，提升加载速度
        if p.startswith("/static/"):
            return resp

        resp.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
        resp.headers["Pragma"] = "no-cache"
        resp.headers["Expires"] = "0"
    except Exception:
        pass
    return resp



@app.before_request
def enforce_web_verify():
    """网页版强制验证：直接访问网页需先在 Telegram 加频道并通过机器人验证。
    但从机器人跳转的 /chatlog/token/<token> 会自动放行（视为已在 Telegram 内验证）。
    """
    if not WEB_VERIFY_ENABLED:
        return
    p = request.path or "/"
    # Exempt paths
    if p.startswith("/static/") or p.startswith("/verify") or p.startswith("/chatlog/token/") or p.startswith("/admin"):
        return
    # health or misc
    if p in ("/health", "/favicon.ico"):
        return
    now = int(datetime.now(timezone.utc).timestamp())
    exp = int(session.get("web_verified_expires", 0) or 0)
    if session.get("web_verified") and exp > now:
        return
    # clear any stale
    session.pop("web_verified", None)
    session.pop("web_verified_expires", None)
    # remember next
    nxt = request.full_path or p
    session["verify_next"] = nxt
    return redirect(url_for("verify", next=nxt))


@app.route("/verify")
def verify():
    """验证入口：提示加入频道并跳转机器人验证。"""
    nxt = request.args.get("next") or session.get("verify_next") or "/"
    # only allow relative paths
    if not nxt.startswith("/"):
        nxt = "/"
    ensure_extra_tables()
    # 复用当前浏览器的 state，避免刷新页面导致 state 变化（从而需要用户重复 /start）
    now = int(datetime.now(timezone.utc).timestamp())
    state = (session.get("verify_state") or "").strip()
    row = None
    if state and db_exists():
        with db_lock:
            conn = get_db_connection()
            c = conn.cursor()
            try:
                c.execute("SELECT state, expires, used FROM web_verify_states WHERE state = ?", (state,))
                row = c.fetchone()
            except sqlite3.OperationalError:
                row = None
            conn.close()
    if (not state) or (not row) or int(row["expires"] or 0) < now or int(row["used"] or 0) == 1:
        state = uuid.uuid4().hex
        session["verify_state"] = state
        exp = now + 15 * 60  # state 有效期 15 分钟
        if db_exists():
            with db_lock:
                conn = get_db_connection()
                c = conn.cursor()
                c.execute(
                    "INSERT OR REPLACE INTO web_verify_states (state, tg_user_id, next_path, created, expires, used, approved) VALUES (?, NULL, ?, ?, ?, 0, 0)",
                    (state, nxt, now, exp),
                )
                conn.commit()
                conn.close()
    
    bot_u = get_bot_username()
    start_param = f"webv_{state}"
    bot_link = f"https://t.me/{bot_u}?start={start_param}" if bot_u else ""
    return render_template(
        "verify.html",
        required_channel=WEB_REQUIRED_CHANNEL,
        required_channel_url=WEB_REQUIRED_CHANNEL_URL,
        bot_username=bot_u,
        bot_link=bot_link,
        state=state,
        next_path=nxt,
    )

@app.route("/verify/callback")
def verify_callback():
    """机器人验证通过后回跳页面。"""
    state = (request.args.get("state") or "").strip()
    if not state:
        # 回跳参数异常：清理任何可能残留的验证状态，避免浏览器返回显示旧页面造成误解。
        session.pop("web_verified", None)
        session.pop("web_verified_expires", None)
        session.pop("web_verified_tg_user_id", None)
        session.pop("web_verified_via", None)
        return render_template(
            "feature_disabled.html",
            title="验证参数缺失",
            detail="请从机器人验证页面重新进入。",
        )
    ensure_extra_tables()
    now = int(datetime.now(timezone.utc).timestamp())
    row = None
    if db_exists():
        with db_lock:
            conn = get_db_connection()
            c = conn.cursor()
            try:
                c.execute("SELECT state, tg_user_id, next_path, expires, used, approved FROM web_verify_states WHERE state = ?", (state,))
                row = c.fetchone()
            except sqlite3.OperationalError:
                row = None
            if row and int(row["expires"] or 0) >= now and int(row["used"] or 0) == 0 and int(row["approved"] or 0) == 1 and row["tg_user_id"]:
                # mark used and persist verified user
                c.execute("UPDATE web_verify_states SET used = 1 WHERE state = ?", (state,))
                uid = int(row["tg_user_id"])
                vexp = now + WEB_VERIFY_TTL_SEC
                c.execute(
                    "INSERT OR REPLACE INTO web_verified_users (tg_user_id, verified_at, expires) VALUES (?, ?, ?)",
                    (uid, now, vexp),
                )
                conn.commit()
            conn.close()

    if (not row) or int(row["expires"] or 0) < now:
        session.pop("web_verified", None)
        session.pop("web_verified_expires", None)
        session.pop("web_verified_tg_user_id", None)
        session.pop("web_verified_via", None)
        return render_template(
            "feature_disabled.html",
            title="验证已过期，请返回重新验证",
            detail="验证链接已超时。请返回机器人重新点击“验证”按钮获取新链接。",
        )
    if int(row["used"] or 0) != 0:
        # already used; still allow session
        pass
    if not row["tg_user_id"]:
        session.pop("web_verified", None)
        session.pop("web_verified_expires", None)
        session.pop("web_verified_tg_user_id", None)
        session.pop("web_verified_via", None)
        return render_template(
            "feature_disabled.html",
            title="尚未在机器人中完成验证，请回到机器人点击验证按钮",
            detail="请回到机器人点击“验证”按钮，完成后会自动跳回网页。",
        )

    uid = int(row["tg_user_id"])
    # set session
    session["web_verified"] = True
    session["web_verified_tg_user_id"] = uid
    session["web_verified_expires"] = int(datetime.now(timezone.utc).timestamp()) + WEB_VERIFY_TTL_SEC
    session["web_verified_via"] = "bot"
    nxt = (row["next_path"] or "/").strip()
    if not nxt.startswith("/"):
        nxt = "/"
    return redirect(nxt)


@app.route("/verify/status")
def verify_status():
    """验证页面轮询状态：实现加入频道后自动放行。"""
    state = (request.args.get("state") or "").strip()
    if not state:
        return jsonify({"status": "error"}), 400
    ensure_extra_tables()
    now = int(datetime.now(timezone.utc).timestamp())
    row = None
    if db_exists():
        with db_lock:
            conn = get_db_connection()
            c = conn.cursor()
            try:
                c.execute("SELECT state, tg_user_id, expires, used, approved FROM web_verify_states WHERE state = ?", (state,))
                row = c.fetchone()
            except sqlite3.OperationalError:
                row = None
            conn.close()
    if (not row) or int(row["expires"] or 0) < now:
        return jsonify({"status": "expired"})
    if int(row["approved"] or 0) == 1 and row["tg_user_id"]:
        return jsonify({"status": "approved", "callback_url": url_for("verify_callback", state=state)})
    if row["tg_user_id"]:
        return jsonify({"status": "waiting_join"})
    return jsonify({"status": "pending"})


@app.route("/")
def index():
    q = request.args.get("q", "").strip()
    if q:
        return redirect(url_for("query", q=q))
    return render_template("index.html", has_db=db_exists())



@app.route("/query")
def query():
    if not db_exists():
        return render_template("no_db.html")

    q = request.args.get("q", "").strip()
    if not q:
        return redirect(url_for("index"))

    live_requested = request.args.get("live", "0") == "1"

    live = live_requested

    resolved_id = resolve_query_to_id(q)

    user_id_for_search = None
    q_norm = q.strip()
    if resolved_id is not None:
        user_id_for_search = resolved_id
    elif q_norm.isdigit():
        user_id_for_search = int(q_norm)

    profile = None
    active_usernames = []
    verified_entry = None

    if resolved_id is not None:
        profile = query_user_profile(resolved_id)
        if profile and profile.get("current_profile"):
            row = profile["current_profile"]
            raw_active = None
            try:
                raw_active = row["active_usernames_json"]
            except Exception:
                raw_active = None
            if raw_active:
                try:
                    active_usernames = json.loads(raw_active)
                except Exception:
                    active_usernames = []
        verified_entry = get_verified_entry(resolved_id, active_usernames)

    usernames_for_search = list(active_usernames)
    if q_norm and not q_norm.isdigit():
        usernames_for_search.append(q_norm)

    history_mentions = search_history_mentions(user_id=user_id_for_search, usernames=usernames_for_search)

    inferred_ids = []
    if (resolved_id is None) and (not q_norm.isdigit()) and history_mentions:
        for m in history_mentions[:10]:
            inferred_ids.extend(infer_possible_ids_from_text(m.get("text", "")))
        inferred_ids = sorted(set(inferred_ids))[:5]

    exposure_history = [m for m in history_mentions if is_exposure_chat(m.get("chat_title", ""))]
    risk_level = "low"
    risk_source = None
    if verified_entry:
        risk_level = "high"
        risk_source = "官方核实"
    elif exposure_history:
        risk_level = "high"
        risk_source = "曝光频道"
    elif history_mentions:
        risk_level = "medium"
        risk_source = "历史记录"

    live_hits = []
    live_error = None

    # 如果本地库没有任何命中，默认自动触发一次实时扫描（避免“网页版查不到”的体验）
    auto_live = False
    if (not live_requested) and (not any([profile, verified_entry, history_mentions])):
        auto_live = True
        live = True

    if live:
        kws = build_keywords(q_norm, resolved_id)
        for pid in inferred_ids:
            kws.append(str(pid))
        kws = list(dict.fromkeys(kws))[:6]

        r = live_scan(kws)
        if r.get("ok"):
            live_hits = r.get("hits", [])
        else:
            live_error = r.get("error") or "扫描失败"

        if any(x.get("exposure") for x in live_hits):
            risk_level = "high"
            risk_source = "实时扫描"
        elif live_hits and risk_level == "low":
            risk_level = "medium"
            risk_source = "实时扫描"

    has_any_data = any([profile, verified_entry, history_mentions, live_hits]) or live
    if not has_any_data:
        return render_template("not_found.html", query=q)

    chatlog_url = None
    if resolved_id is not None:
        chatlog_url = url_for("chatlog_view", user_id=resolved_id)

    return render_template(
        "result.html",
        query=q,
        profile=profile,
        verified_entry=verified_entry,
        history_mentions=history_mentions,
        inferred_ids=inferred_ids,
        risk_level=risk_level,
        risk_source=risk_source,
        live=live,
        live_hits=live_hits,
        live_error=live_error,
        resolved_id=resolved_id,
        chatlog_url=chatlog_url,
    )


@app.route("/chatlog")
def chatlog_index():
    if not db_exists():
        return render_template("no_db.html")

    if not is_chatlog_enabled():
        return render_template("feature_disabled.html", title="聊天记录功能已关闭")

    q = request.args.get("q", "").strip()
    if not q:
        return render_template("chatlog_index.html")

    resolved_id = resolve_query_to_id(q)
    if resolved_id is None and q.isdigit():
        resolved_id = int(q)

    if resolved_id is None:
        return render_template("not_found.html", query=q)

    return redirect(url_for("chatlog_view", user_id=resolved_id))


@app.route("/chatlog/token/<token>")
def chatlog_token(token: str):
    if not db_exists():
        return render_template("no_db.html")

    if not is_chatlog_enabled():
        return render_template("feature_disabled.html", title="聊天记录功能已关闭")

    info = get_token_target(token)
    if not info:
        return render_template(
            "feature_disabled.html",
            title="链接已失效或不存在",
            detail="请回到机器人重新生成网页链接。",
        )


    # 从 Telegram 机器人跳转的 token 链接：视为已验证，不再要求网页版验证。
    # 但验证有效期不应超过 token 自身的有效期，避免 "返回" 看起来绕过验证。
    now = int(datetime.now(timezone.utc).timestamp())
    session["web_verified"] = True
    try:
        token_exp = int(info.get("expires") or 0)
    except Exception:
        token_exp = 0
    session["web_verified_expires"] = min(now + WEB_VERIFY_TTL_SEC, token_exp) if token_exp else (now + WEB_VERIFY_TTL_SEC)
    session["web_verified_via"] = "telegram"
    if info.get("requester_user_id"):
        try:
            session["web_verified_tg_user_id"] = int(info["requester_user_id"])
        except Exception:
            pass

    return redirect(url_for("chatlog_view", user_id=int(info["target_user_id"]), token=token))


@app.route("/chatlog/<int:user_id>")
def chatlog_view(user_id: int):
    if not db_exists():
        return render_template("no_db.html")

    if not is_chatlog_enabled():
        return render_template("feature_disabled.html", title="聊天记录功能已关闭")

    keyword = request.args.get("kw", "").strip()
    chat_id = request.args.get("chat_id", "").strip()
    page = int(request.args.get("page", "1") or 1)
    per_page = int(request.args.get("per_page", "30") or 30)

    chat_id_int = None
    if chat_id:
        try:
            chat_id_int = int(chat_id)
        except Exception:
            chat_id_int = None

    profile = query_user_profile(user_id)
    if not profile:
        return render_template("not_found.html", query=str(user_id))

    chats = query_user_chat_summary(user_id, keyword=keyword)
    msg_pack = query_user_messages(user_id, chat_id=chat_id_int, keyword=keyword, page=page, per_page=per_page)

    selected_chat = None
    if chat_id_int is not None:
        for c in chats:
            if int(c.get("chat_id")) == int(chat_id_int):
                selected_chat = c
                break

    total = int(msg_pack.get("total", 0))
    total_pages = max(1, (total + per_page - 1) // per_page)

    return render_template(
        "chatlog.html",
        user_id=user_id,
        profile=profile,
        chats=chats,
        selected_chat=selected_chat,
        messages=msg_pack.get("items", []),
        keyword=keyword,
        chat_id=chat_id_int,
        page=page,
        per_page=per_page,
        total=total,
        total_pages=total_pages,
        token=YOUR_TELEGRAM_BOT_TOKEN
    )


@app.route("/leaderboard")
def leaderboard():
    rows = query_leaderboard(limit=10)
    return render_template("leaderboard.html", rows=rows)


@app.route("/stats")
def stats():
    data = query_basic_stats()
    return render_template("stats.html", stats=data, has_db=db_exists())


# ======================
# Admin
# ======================
@app.route("/admin/login", methods=["GET", "POST"])
def admin_login():
    if not ADMIN_PASSWORD:
        return render_template("feature_disabled.html", title="未配置 ADMIN_PASSWORD，管理后台不可用")

    if request.method == "POST":
        pwd = (request.form.get("password") or "").strip()
        if pwd and pwd == ADMIN_PASSWORD:
            session["is_admin"] = True
            return redirect(url_for("admin_panel"))
        flash("密码错误")

    return render_template("admin_login.html")


@app.route("/admin/logout")
def admin_logout():
    session.pop("is_admin", None)
    return redirect(url_for("index"))


@app.route("/admin", methods=["GET", "POST"])
def admin_panel():
    if not require_admin():
        return redirect(url_for("admin_login"))

    ensure_extra_tables()

    action = (request.form.get("action") or "").strip()
    msg = None

    if action == "toggle_chatlog":
        enable = request.form.get("enable", "0") == "1"
        set_setting("chatlog_feature_enabled", "1" if enable else "0")
        msg = f"已{'开启' if enable else '关闭'}聊天记录功能"

    elif action == "delete_oldest":
        try:
            cnt = int(request.form.get("count", "0") or 0)
        except Exception:
            cnt = 0
        deleted = delete_oldest_messages(cnt)
        msg = f"已删除 {deleted} 条最旧聊天记录"

    elif action == "delete_keyword":
        raw = (request.form.get("keywords", "") or "").strip()
        kws = [k.strip() for k in raw.split(",") if k.strip()]
        deleted = delete_messages_by_keywords(kws)
        msg = f"已按关键词删除 {deleted} 条聊天记录"

    elif action == "vacuum":
        vacuum_db()
        msg = "已执行 VACUUM（若数据量大可能需要一点时间）"

    stats_data = query_basic_stats()
    db_bytes = stats_data.get("db_bytes", 0)

    return render_template(
        "admin.html",
        stats=stats_data,
        db_bytes=db_bytes,
        chatlog_enabled=is_chatlog_enabled(),
        message=msg,
    )


if __name__ == "__main__":
    ensure_extra_tables()
    port = int(os.environ.get("PORT", "8000"))
    app.run(host="0.0.0.0", port=port, debug=False)