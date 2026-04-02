import os
import time
import json
import re
import asyncio
import threading
import traceback
import functools
import copy
import requests
import sqlite3
import io
from datetime import datetime, timezone, timedelta
import uuid
from concurrent.futures import TimeoutError as FuturesTimeoutError

from telethon import TelegramClient, events, utils
from telethon.tl.types import (
    User, Channel, PeerChannel, PeerChat, Chat, Dialog, UserStatusOffline, UserStatusOnline,
    BusinessWorkHours, BusinessLocation, GeoPoint
)
from telethon.errors.rpcerrorlist import (
    FloodWaitError, ApiIdInvalidError, AuthKeyDuplicatedError, PeerIdInvalidError,
    ChannelPrivateError, TimeoutError as TelethonTimeoutError, UserNotParticipantError,
    UsernameInvalidError, InviteHashExpiredError, ChannelsTooMuchError, ChatAdminRequiredError,
    UserAlreadyParticipantError
)
from telethon.tl.functions.users import GetFullUserRequest
from telethon.tl.functions.channels import JoinChannelRequest, GetFullChannelRequest
from telethon.tl.functions.messages import GetFullChatRequest

import telebot
from telebot import types
from telebot.apihelper import ApiTelegramException
from telebot.types import ReplyParameters

# ---------------------- 新增：赞助功能所需的库 ----------------------
import hashlib
import urllib.parse
from flask import Flask, request, jsonify
import logging

# 初始化 logger（避免在模块加载阶段 logger 未定义导致异常）
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(name)s - %(message)s')
logger = logging.getLogger('ZPF-Bot')
# ---------------------- 新增结束 ----------------------

# ---------------------- 配置参数 ----------------------
CHINA_TZ = timezone(timedelta(hours=8))

# --- Web verify (force join channel for web) ---
WEB_REQUIRED_CHANNEL = os.environ.get('WEB_REQUIRED_CHANNEL', '@your_channel').strip() or '@your_channel'
if not WEB_REQUIRED_CHANNEL.startswith('@'):
    WEB_REQUIRED_CHANNEL = '@' + WEB_REQUIRED_CHANNEL
WEB_REQUIRED_CHANNEL_URL = os.environ.get('WEB_REQUIRED_CHANNEL_URL', 'https://t.me/your_channel').strip() or 'https://t.me/your_channel'

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))

CONFIG = {


    "api_id": 12345678,
    "api_hash": 'YOUR_TELEGRAM_API_HASH',
    "telegram_session": "telegram_session",
    "BOT_TOKEN": 'YOUR_TELEGRAM_BOT_TOKEN',
    "ADMIN_ID": 123456789,
    "LOG_CHANNEL_ID": -1001234567890,
    "REQUIRED_CHANNEL": WEB_REQUIRED_CHANNEL,
    "CHANNELS_FILE": "channels.json",
    "REPORTS_FILE": "reports.json",
    "DATABASE_FILE": os.path.join(SCRIPT_DIR, "history.db"),
    "ONLINE_THRESHOLD": 300,
    "TELETHON_TIMEOUT": 45,
    "TELEBOT_API_TIMEOUT": 40,
    "TELEBOT_POLLING_TIMEOUT": 30,
    "LOG_BATCH_INTERVAL": 15,
    "LOG_MAX_MESSAGE_LENGTH": 4000,
    "PROFILE_HISTORY_PAGE_SIZE": 15,
    "GROUP_HISTORY_PAGE_SIZE": 10,
    "SCAM_CHANNEL_SEARCH_LIMIT": 5,
    "SCAM_CHANNEL_SEARCH_TIMEOUT": 40,
    "SCAM_CHANNEL_SCAN_DEPTH": 300,
    "COMMON_GROUPS_TIMEOUT": 90,
    "BUSINESS_SCAN_COOLDOWN": 60,
    "OFFLINE_REPLY_COOLDOWN": 10,
    # ---------------------- 新增：OKPay 赞助功能配置 ----------------------
    "OKPAY_ID": 10000,  # 请替换为你的 OKPay 商户 APP ID
    "OKPAY_TOKEN": "YOUR_OKPAY_TOKEN",  # 请替换为你的 OKPay 商户 Token
    "SERVER_PUBLIC_IP": "YOUR_SERVER_IP",  # 请替换为你的服务器公网 IP
    "WEBHOOK_PORT": 1010,  # 用于接收支付回调的端口
    "WEBHOOK_PUBLIC_PORT": 2009,  # 外部映射端口（公网访问端口，比如 2009 -> 容器 1010）
    # ---------------------- 新增结束 ----------------------
}


# ---------------------- 新增：聊天记录查询/网站跳转配置 ----------------------
# 机器人内查询后可给出网站跳转链接：/chatlog/token/<token>
# - WEB_BASE_URL: 网站根地址，例如 https://your.domain 或 http://ip:8000
# - CHATLOG_TOKEN_TTL_SEC: 跳转 token 有效期（秒）
# - CHATLOG_FEATURE_DEFAULT: 默认是否开启（1/0）
CONFIG["WEB_BASE_URL"] = os.getenv("WEB_BASE_URL", "http://your-server-ip:PORT").strip()
CONFIG["CHATLOG_TOKEN_TTL_SEC"] = int(os.getenv("CHATLOG_TOKEN_TTL_SEC", "900") or 900)
CONFIG["CHATLOG_FEATURE_DEFAULT"] = os.getenv("CHATLOG_FEATURE_DEFAULT", "1").strip()
CONFIG["INVITE_REQUIRED_COUNT_DEFAULT"] = int(os.getenv("INVITE_REQUIRED_COUNT", "1") or 1)
# -----------------------------------------------------------

BOT_VERSION = "v24.8.17.5 | Sponsorship Update"
DONE_SUBMISSION_COMMAND = "/done"

# 动态生成回调 URL（重要：公网端口可能与容器监听端口不同）
# 1Panel 端口映射示例：应用端口 1010 -> 外部映射端口 2009
# - WEBHOOK_LISTEN_PORT: 容器内监听端口（默认取 WEBHOOK_PORT）
# - WEBHOOK_PUBLIC_PORT: 公网访问端口（外部映射端口）
# - CALLBACK_BASE_URL: 推荐（可填域名或 IP+端口），例如 http://your-server-ip:PORT 或 https://your.domain
# - USE_PUBLIC_BASE_URL: 兼容旧的 PUBLIC_BASE_URL（默认不启用，避免历史环境变量误覆盖）
CALLBACK_BASE_URL = os.getenv("CALLBACK_BASE_URL", "").strip()
PUBLIC_BASE_URL = os.getenv("PUBLIC_BASE_URL", "").strip()
USE_PUBLIC_BASE_URL = os.getenv("USE_PUBLIC_BASE_URL", "").strip().lower() in ("1", "true", "yes", "y", "on")

# 容器内监听端口（应用端口）
listen_port = int(os.getenv("WEBHOOK_LISTEN_PORT", os.getenv("WEBHOOK_PORT", str(CONFIG.get("WEBHOOK_PORT", 1010)))))

# 公网访问端口（外部映射端口）
public_port = int(os.getenv("WEBHOOK_PUBLIC_PORT", str(CONFIG.get("WEBHOOK_PUBLIC_PORT", listen_port))))

# 写回 CONFIG，保证后续 run_server 监听正确端口
CONFIG["WEBHOOK_PORT"] = listen_port
CONFIG["WEBHOOK_LISTEN_PORT"] = listen_port
CONFIG["WEBHOOK_PUBLIC_PORT"] = public_port

# 最终回调 URL：默认使用 IP+公网端口；只有在明确启用时才使用 PUBLIC_BASE_URL
base_url = ""
source = "IP+PORT"
if CALLBACK_BASE_URL:
    base_url = CALLBACK_BASE_URL
    source = "CALLBACK_BASE_URL"
elif USE_PUBLIC_BASE_URL and PUBLIC_BASE_URL:
    base_url = PUBLIC_BASE_URL
    source = "PUBLIC_BASE_URL"

if base_url:
    CONFIG["CALLBACK_URL"] = base_url.rstrip("/") + "/okpay"
else:
    public_ip = os.getenv("SERVER_PUBLIC_IP", "").strip() or CONFIG.get("SERVER_PUBLIC_IP", "")
    CONFIG["CALLBACK_URL"] = f"http://{public_ip}:{public_port}/okpay"

CONFIG["CALLBACK_URL_SOURCE"] = source
logger.info(f"🧩 OKPay CALLBACK_URL = {CONFIG['CALLBACK_URL']} (source={source}, LISTEN={listen_port}, PUBLIC={public_port})")



# Manually define all content types for compatibility with any py-telegram-bot-api version
ALL_CONTENT_TYPES = [
    'text', 'audio', 'document', 'animation', 'game', 'photo', 'sticker', 'video', 'video_note',
    'voice', 'contact', 'location', 'venue', 'dice', 'new_chat_members', 'left_chat_member',
    'new_chat_title', 'new_chat_photo', 'delete_chat_photo', 'group_chat_created',
    'supergroup_chat_created', 'channel_chat_created', 'migrate_to_chat_id',
    'migrate_from_chat_id', 'pinned_message', 'invoice', 'successful_payment', 'connected_website',
    'poll', 'passport_data', 'proximity_alert_triggered', 'video_chat_scheduled', 'video_chat_started',
    'video_chat_ended', 'video_chat_participants_invited', 'web_app_data', 'message_auto_delete_timer_changed',
    'forum_topic_created', 'forum_topic_edited', 'forum_topic_closed', 'forum_topic_reopened',
    'general_forum_topic_hidden', 'general_forum_topic_unhidden', 'write_access_allowed',
    'user_shared', 'chat_shared', 'story'
]


# ---------------------- 并发控制、状态与日志缓冲 ----------------------
channels_lock = threading.Lock()
reports_lock = threading.Lock()
db_lock = threading.Lock()
log_buffer_lock = threading.Lock()
log_buffer = []

# (user_id, contact_id): timestamp
offline_reply_cooldown_cache = {}
# user_id: {'flow': '...', 'data': {...}, 'messages': []}
user_settings_state = {}
# ---------------------- 新增：赞助流程状态 ----------------------
user_sponsorship_state = {}


# ---------------------- 新增：Flask 和 OKPay 支付逻辑 ----------------------
app = Flask(__name__)
logger = logging.getLogger('ZPF-Bot') # 使用已有的 logger

class OkayPay:
    def __init__(self, id, token, api_url_base='https://api.okaypay.me/shop/'):
        self.id = id
        self.token = token
        self.api_url_payLink = api_url_base + 'payLink'
        self.api_url_transfer = api_url_base + 'transfer'
        self.api_url_TransactionHistory = api_url_base + 'TransactionHistory'

    def pay_link(self, amount, return_url=None):
        """Create a payment link.

        Notes:
        - Some gateways use `return_url` only for browser redirect, and a separate `notify_url` / `callback_url`
          for server-to-server payment notification.
        - We try to send common notify fields first; if the API rejects/ignores them, we fall back to the
          original minimal payload to avoid breaking order creation.
        """
        callback = (return_url or CONFIG.get("CALLBACK_URL") or "").strip()

        base_data = {
            'name': '机器人赞助',
            'amount': amount,
            'coin': 'USDT',
            'return_url': callback
        }

        # Attempt 1: include common server-notify fields
        attempt1 = dict(base_data)
        attempt1.update({
            'notify_url': callback,
            'callback_url': callback,
        })

        resp = self._post(self.api_url_payLink, self._sign(attempt1))
        if isinstance(resp, dict) and resp.get('status') == 'success' and resp.get('data'):
            return resp

        # Attempt 2 (fallback): original minimal payload
        return self._post(self.api_url_payLink, self._sign(base_data))

    def _sign(self, data):
        data['id'] = self.id
        filtered_data = {k: v for k, v in data.items() if v is not None and v != ''}
        sorted_data = sorted(filtered_data.items())
        query_str = urllib.parse.urlencode(sorted_data, quote_via=urllib.parse.quote)
        decoded_str = urllib.parse.unquote(query_str)
        sign_str = decoded_str + '&token=' + self.token
        signature = hashlib.md5(sign_str.encode('utf-8')).hexdigest().upper()
        signed_data = dict(sorted_data)
        signed_data['sign'] = signature
        return signed_data

    def _post(self, url, data):
        try:
            response = requests.post(
                url,
                data=data,
                headers={'User-Agent': 'HTTP CLIENT'},
                timeout=10,
                verify=True # 在生产环境中建议开启 SSL 验证
            )
            logger.info(f"OKPay API请求: URL={url}, 数据={data}, 响应={response.text}")
            return response.json()
        except Exception as e:
            logger.error(f"OKPay API请求错误: {e}")
            return {'error': str(e), 'status': 'request_failed'}

# 实例化 OKPay 客户端
okpay_client = OkayPay(id=CONFIG["OKPAY_ID"], token=CONFIG["OKPAY_TOKEN"])

@app.route('/heal', methods=['GET'])
def heal():
    """健康检查：确认回调服务对外可达"""
    return jsonify({
        'status': 'ok',
        'callback_url': CONFIG.get('CALLBACK_URL', ''),
        'callback_url_source': CONFIG.get('CALLBACK_URL_SOURCE', ''),
        'server_public_ip': os.getenv("SERVER_PUBLIC_IP", "").strip() or CONFIG.get('SERVER_PUBLIC_IP', ''),
        'listen_port': CONFIG.get('WEBHOOK_LISTEN_PORT', listen_port),
        'public_port': CONFIG.get('WEBHOOK_PUBLIC_PORT', public_port),
    }), 200


@app.route('/okpay', methods=['POST', 'GET'])
def handle_okpay_callback():
    """OKPay payment notification endpoint.

    This endpoint is used both for server-to-server callbacks and (sometimes) browser redirects.
    Different providers may send different payload formats; so we accept JSON, form, and querystring.
    """

    def _first(d, keys):
        for k in keys:
            if not isinstance(d, dict):
                continue
            v = d.get(k)
            if v is None:
                continue
            sv = str(v).strip()
            if sv != "":
                return v
        return None

    try:
        # 1) Parse callback payload (JSON / form / querystring)
        callback_data = request.get_json(silent=True)
        if callback_data is None:
            callback_data = request.form.to_dict(flat=True)

        if (callback_data is None) or (callback_data == {}):
            callback_data = request.args.to_dict(flat=True)

        if not isinstance(callback_data, dict):
            logger.warning(f"收到无法解析的OKPay回调类型: {type(callback_data)}")
            return "success", 200

        logger.info(f"收到OKPay回调({request.method}): {callback_data}")

        # 2) Some gateways wrap the real payload in `data`
        payment_info = callback_data.get('data') if isinstance(callback_data.get('data'), dict) else callback_data
        if not isinstance(payment_info, dict):
            payment_info = callback_data

        # 3) Extract fields (be tolerant to variants)
        order_id = _first(payment_info, ['order_id', 'orderId', 'order_no', 'orderNo', 'oid', 'out_trade_no', 'trade_no'])
        if (not order_id) and isinstance(payment_info.get('order'), dict):
            order_id = _first(payment_info.get('order'), ['order_id', 'orderId', 'order_no', 'orderNo', 'oid'])

        amount_str = _first(payment_info, ['amount', 'money', 'total_amount', 'pay_amount', 'value', 'price'])
        coin = _first(payment_info, ['coin', 'currency', 'ccy'])

        if not order_id:
            logger.warning(f"OKPay回调缺少 order_id，已忽略: {callback_data}")
            return "success", 200

        # 4) Use DB as source of truth when callback does not carry amount/coin
        with db_lock:
            conn = get_db_connection()
            try:
                c = conn.cursor()
                c.execute("SELECT user_id, amount, status FROM okpay_orders WHERE order_id = ?", (str(order_id),))
                order = c.fetchone()

                if not order:
                    logger.warning(f"OKPay回调订单不存在(可能已处理/无此订单): {order_id}")
                    return "success", 200

                if order['status'] == 'paid':
                    logger.info(f"OKPay回调重复通知，订单已支付: {order_id}")
                    return "success", 200

                # amount
                amount = None
                if amount_str is not None:
                    try:
                        amount = float(str(amount_str))
                    except Exception:
                        amount = None

                if amount is None:
                    amount = float(order['amount'])

                # coin
                coin = str(coin).strip() if coin is not None else 'USDT'
                if not coin:
                    coin = 'USDT'

                user_id = order['user_id']
                now = int(time.time())

                # 5) Transactional DB update (idempotent)
                c.execute("BEGIN TRANSACTION;")
                c.execute(
                    "UPDATE okpay_orders SET status = 'paid' WHERE order_id = ? AND status != 'paid'",
                    (str(order_id),)
                )
                c.execute(
                    "INSERT OR IGNORE INTO sponsorships (user_id, amount_usdt, order_id, timestamp) VALUES (?, ?, ?, ?)",
                    (user_id, amount, str(order_id), now)
                )
                c.execute(
                    """
                    INSERT INTO sponsors (user_id, total_amount_usdt, last_sponsored_at)
                    VALUES (?, ?, ?)
                    ON CONFLICT(user_id) DO UPDATE SET
                        total_amount_usdt = total_amount_usdt + excluded.total_amount_usdt,
                        last_sponsored_at = excluded.last_sponsored_at
                    """,
                    (user_id, amount, now)
                )
                c.execute("COMMIT;")
                conn.commit()

            except sqlite3.Error as e:
                conn.rollback()
                logger.error(f"OKPay回调数据库操作失败: {e}")
                raise
            finally:
                conn.close()

        # 6) Notify user
        try:
            bot.send_message(
                user_id,
                f"✅ *{escape_markdown('赞助成功！')}*\n\n"
                f"{escape_markdown('非常感谢您的支持，您的每一份赞助都是我们前进的动力！')}\n\n"
                f"*{escape_markdown('订单号:')}* `{escape_for_code(str(order_id))}`\n"
                f"*{escape_markdown('金额:')}* `{escape_for_code(f'{amount:.4f}')} {escape_for_code(coin)}`",
                parse_mode="MarkdownV2"
            )
            logger.info(f"已通知用户 {user_id} 订单 {order_id} 支付成功")
        except Exception as e:
            logger.error(f"通知用户 {user_id} 赞助成功失败: {e}")

        # IMPORTANT: Many gateways require plain `success`
        return "success", 200

    except Exception as e:
        logger.exception(f"OKPay回调处理异常: {e}")
        # still return success to prevent retries storm
        return "success", 200


def run_server():
    logger.info(f"回调服务器正在启动，监听 0.0.0.0:{CONFIG['WEBHOOK_PORT']}...")
    try:
        log = logging.getLogger('werkzeug')
        log.setLevel(logging.ERROR) # 屏蔽 Flask 的常规日志输出
        app.run(host='0.0.0.0', port=CONFIG["WEBHOOK_PORT"], debug=False, use_reloader=False)
    except Exception as e:
        logger.exception("回调服务器运行错误")
# ---------------------- 新增结束 ----------------------


# ---------------------- 文本处理与序列化函数 ----------------------
def escape_markdown(text: str) -> str:
    """Escapes characters for general MarkdownV2 text."""
    if not isinstance(text, str):
        text = str(text)
    escape_chars = r'_*[]()~`>#+-=|{}.!'
    return re.sub(f'([{re.escape(escape_chars)}])', r'\\\1', text)

def escape_for_code(text: str) -> str:
    """Escapes characters for use inside a MarkdownV2 code block ('`' and '\')."""
    if not isinstance(text, str):
        text = str(text)
    return text.replace('\\', '\\\\').replace('`', '\\`')

def _sanitize_for_link_text(text: str) -> str:
    """Removes characters that conflict with Markdown link syntax."""
    if not isinstance(text, str):
        text = str(text)
    return re.sub(r'[\[\]]', '', text)

def truncate_for_link_text(text: str, max_bytes: int = 60) -> str:
    text_str = str(text or '用户').strip()
    if not text_str:
        text_str = '用户'
    encoded = text_str.encode('utf-8')
    if len(encoded) <= max_bytes:
        return text_str
    truncated_encoded = encoded[:max_bytes]
    return truncated_encoded.decode('utf-8', 'ignore') + '…'

def serialize_message(message: types.Message) -> dict:
    if not message:
        return None
    
    text = None
    if message.content_type == 'text':
        text = message.html_text
    elif message.caption:
        text = message.html_caption
    
    data = {
        'content_type': message.content_type,
        'text': text,
        'parse_mode': 'HTML' if text else None
    }

    file_id = None
    if message.photo:
        file_id = message.photo[-1].file_id
    elif message.video:
        file_id = message.video.file_id
    elif message.document:
        file_id = message.document.file_id
    elif message.audio:
        file_id = message.audio.file_id
    elif message.voice:
        file_id = message.voice.file_id
    elif message.sticker:
        file_id = message.sticker.file_id
    elif message.animation:
        file_id = message.animation.file_id
    
    if file_id:
        data['file_id'] = file_id
        
    if data.get('text') or data.get('file_id'):
        return data
    return None

def send_serialized_message(chat_id: int, serialized_data: dict, business_connection_id: str = None):
    if not serialized_data or not isinstance(serialized_data, dict):
        print(f"💥 [SendSerialized] Invalid data provided. Expected dict, got {type(serialized_data)}.")
        return

    try:
        content_type = serialized_data.get('content_type')
        text = serialized_data.get('text')
        file_id = serialized_data.get('file_id')
        parse_mode = serialized_data.get('parse_mode')

        kwargs = {'chat_id': chat_id}
        if business_connection_id:
            kwargs['business_connection_id'] = business_connection_id

        if content_type == 'text':
            bot.send_message(text=text, parse_mode=parse_mode, **kwargs)
        elif content_type == 'photo':
            bot.send_photo(photo=file_id, caption=text, parse_mode=parse_mode, **kwargs)
        elif content_type == 'document':
            bot.send_document(document=file_id, caption=text, parse_mode=parse_mode, **kwargs)
        elif content_type == 'video':
            bot.send_video(video=file_id, caption=text, parse_mode=parse_mode, **kwargs)
        elif content_type == 'audio':
            bot.send_audio(audio=file_id, caption=text, parse_mode=parse_mode, **kwargs)
        elif content_type == 'voice':
            bot.send_voice(voice=file_id, caption=text, parse_mode=parse_mode, **kwargs)
        elif content_type == 'sticker':
            bot.send_sticker(sticker=file_id, **kwargs)
        elif content_type == 'animation':
            bot.send_animation(animation=file_id, caption=text, parse_mode=parse_mode, **kwargs)
        else:
            if text:
                bot.send_message(text=text, parse_mode=parse_mode, **kwargs)
        return True
    except ApiTelegramException as e:
        print(f"💥 [SendSerialized] 发送消息失败 (Chat: {chat_id}, Conn: {business_connection_id}). 错误: {e.description}")
    except Exception as e:
        print(f"💥 [SendSerialized] 发送消息时发生未知错误: {e}")
    return False

# ---------------------- 广告与一言 API ----------------------
AD_CONTACT_ADMIN = "@lamsvip"
AD_OFFICIAL_CHANNEL = CONFIG.get('REQUIRED_CHANNEL', '@your_channel')

AD_TEXT_PREFIX = (
    f"👑 作者 [{escape_markdown(AD_CONTACT_ADMIN)}](https://t.me/{AD_CONTACT_ADMIN.lstrip('@')}) "
    f"\\| 📢 频道 [{escape_markdown(AD_OFFICIAL_CHANNEL)}](https://t.me/{AD_OFFICIAL_CHANNEL.lstrip('@')})"
)
ADVERTISEMENT_TEXT = AD_TEXT_PREFIX

def get_hitokoto():
    try:
        response = requests.get("https://v1.hitokoto.cn/", timeout=5)
        response.raise_for_status()
        data = response.json()
        quote = data.get('hitokoto')
        source = data.get('from', '未知来源')
        return f"_{escape_markdown(f'“{quote}”')}_\n— {escape_markdown(source)}"
    except requests.exceptions.RequestException as e:
        print(f"⚠️ 获取一言失败: {e}")
        return None


# ---------------------- 文件与数据库读写 ----------------------
def load_json_file(filename, lock):
    with lock:
        try:
            with open(filename, "r", encoding="utf-8") as f:
                return json.load(f)
        except (FileNotFoundError, json.JSONDecodeError):
            return {}

def save_json_file(filename, data, lock):
    with lock:
        try:
            with open(filename, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            return True
        except Exception as e:
            print(f"⚠️ 写入文件 '{filename}' 失败: {e}")
            return False

def load_reports():
    """Load scammer reports.

    现在优先从 SQLite 数据库读取（持久化，不会因进程重启/临时文件丢失而消失）。
    同时保持与旧版 reports.json 的结构兼容：{"pending": {...}, "verified": {...}}。
    """
    # 1) 优先数据库
    try:
        with db_lock:
            conn = get_db_connection()
            cur = conn.cursor()
            # 表可能在旧版本中不存在
            cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='scam_reports'")
            if not cur.fetchone():
                conn.close()
                raise RuntimeError("scam_reports table not found")
            cur.execute("SELECT report_id, status, primary_key, record_json FROM scam_reports")
            rows = cur.fetchall()
            conn.close()

        data = {"pending": {}, "verified": {}}
        for r in rows:
            try:
                record = json.loads(r["record_json"]) if r["record_json"] else {}
            except Exception:
                record = {}
            if r["status"] == "pending":
                data["pending"][r["report_id"]] = record
            elif r["status"] == "verified":
                key = r["primary_key"] or r["report_id"]
                data["verified"][key] = record

        # DB 为空时，回退到文件（并在后续写入时完成迁移）
        if not data["pending"] and not data["verified"]:
            file_data = load_json_file(CONFIG["REPORTS_FILE"], reports_lock) or {}
            if "pending" not in file_data: file_data["pending"] = {}
            if "verified" not in file_data: file_data["verified"] = {}
            return file_data

        if "pending" not in data: data["pending"] = {}
        if "verified" not in data: data["verified"] = {}
        return data

    except Exception:
        # 2) 兜底：旧版 JSON 文件
        data = load_json_file(CONFIG["REPORTS_FILE"], reports_lock) or {}
        if "pending" not in data: data["pending"] = {}
        if "verified" not in data: data["verified"] = {}
        return data


def save_reports(data):
    """Save reports to DB + JSON mirror.

    - DB: 作为持久化主存储
    - JSON: 作为兼容/网页展示的镜像文件
    """
    if not isinstance(data, dict):
        data = {"pending": {}, "verified": {}}
    if "pending" not in data: data["pending"] = {}
    if "verified" not in data: data["verified"] = {}

    db_ok = True
    try:
        now = int(time.time())
        with db_lock:
            conn = get_db_connection()
            cur = conn.cursor()
            # 确保表存在（旧库升级）
            cur.execute('''
                CREATE TABLE IF NOT EXISTS scam_reports (
                    report_id TEXT PRIMARY KEY,
                    status TEXT NOT NULL,
                    primary_key TEXT,
                    record_json TEXT NOT NULL,
                    created_at INTEGER NOT NULL
                )
            ''')
            cur.execute("DELETE FROM scam_reports")

            for submission_id, record in (data.get("pending") or {}).items():
                try:
                    record_json = json.dumps(record, ensure_ascii=False)
                except Exception:
                    record_json = "{}"
                cur.execute(
                    "INSERT OR REPLACE INTO scam_reports (report_id, status, primary_key, record_json, created_at) VALUES (?, ?, ?, ?, ?)",
                    (str(submission_id), "pending", None, record_json, now)
                )

            for primary_key, record in (data.get("verified") or {}).items():
                try:
                    record_json = json.dumps(record, ensure_ascii=False)
                except Exception:
                    record_json = "{}"
                report_id = f"verified:{str(primary_key)}"
                cur.execute(
                    "INSERT OR REPLACE INTO scam_reports (report_id, status, primary_key, record_json, created_at) VALUES (?, ?, ?, ?, ?)",
                    (report_id, "verified", str(primary_key), record_json, now)
                )

            # 唯一索引（仅 verified 的 primary_key）
            try:
                cur.execute('CREATE UNIQUE INDEX IF NOT EXISTS idx_scam_reports_primary_verified ON scam_reports(primary_key) WHERE status="verified"')
            except Exception:
                pass
            cur.execute('CREATE INDEX IF NOT EXISTS idx_scam_reports_status ON scam_reports(status)')

            conn.commit()
            conn.close()
    except Exception as e:
        print(f"⚠️ 写入投稿数据库失败: {e}")
        db_ok = False

    file_ok = save_json_file(CONFIG["REPORTS_FILE"], data, reports_lock)
    return db_ok and file_ok


def load_channels():
    """Load monitored channels/groups.

    现在优先从 SQLite 数据库读取（持久化），并兼容旧版 channels.json。
    返回值仍保持原逻辑：['@xxx', -100..., 123...] 混合。
    """
    # 1) 优先数据库
    with channels_lock:
        try:
            with db_lock:
                conn = get_db_connection()
                cur = conn.cursor()
                cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='monitor_channels'")
                if not cur.fetchone():
                    conn.close()
                    raise RuntimeError("monitor_channels table not found")
                cur.execute("SELECT channel FROM monitor_channels ORDER BY added_at ASC")
                rows = cur.fetchall()
                conn.close()

            processed = []
            for r in rows:
                ch = (r["channel"] if isinstance(r, sqlite3.Row) else r[0])
                if ch is None:
                    continue
                ch = str(ch).strip()
                if not ch:
                    continue
                if ch.startswith('@'):
                    processed.append(ch)
                elif ch.isdigit() or (ch.startswith('-') and ch[1:].isdigit()):
                    try:
                        processed.append(int(ch))
                    except ValueError:
                        pass
                else:
                    processed.append(f"@{ch.lstrip('@')}")
            if processed:
                return processed
        except Exception:
            pass

        # 2) 兜底：旧版 JSON 文件
        try:
            with open(CONFIG["CHANNELS_FILE"], "r", encoding="utf-8") as f:
                channels = json.load(f)
                processed = []
                for ch in channels:
                    if isinstance(ch, str):
                        if ch.startswith('@'): processed.append(ch)
                        elif ch.isdigit() or (ch.startswith('-') and ch[1:].isdigit()):
                            try: processed.append(int(ch))
                            except ValueError: pass
                        else: processed.append(f'@{ch}')
                    elif isinstance(ch, int): processed.append(ch)
                return processed
        except Exception:
            return []


def save_channels(channels):
    """Save monitored channels to DB + JSON mirror."""
    global target_channels
    with channels_lock:
        valid_channels = []
        for ch in channels:
            if isinstance(ch, str) and ch.startswith('@'):
                valid_channels.append(ch)
            elif isinstance(ch, int):
                valid_channels.append(ch)
            elif isinstance(ch, str) and (ch.isdigit() or (ch.startswith('-') and ch[1:].isdigit())):
                try:
                    valid_channels.append(int(ch))
                except ValueError:
                    pass

        unique_channels = sorted(list(set(valid_channels)), key=lambda x: str(x).lower())

        db_ok = True
        try:
            base_ts = int(time.time())
            with db_lock:
                conn = get_db_connection()
                cur = conn.cursor()
                cur.execute('''
                    CREATE TABLE IF NOT EXISTS monitor_channels (
                        channel TEXT PRIMARY KEY,
                        added_at INTEGER NOT NULL
                    )
                ''')
                cur.execute("DELETE FROM monitor_channels")
                for idx, ch in enumerate(unique_channels):
                    ch_str = str(ch) if isinstance(ch, int) else str(ch)
                    cur.execute(
                        "INSERT OR REPLACE INTO monitor_channels(channel, added_at) VALUES(?, ?)",
                        (ch_str, base_ts + idx)
                    )
                conn.commit()
                conn.close()
        except Exception as e:
            print(f"⚠️ 写入频道数据库失败: {e}")
            db_ok = False

        file_ok = True
        try:
            with open(CONFIG["CHANNELS_FILE"], "w", encoding="utf-8") as f:
                json.dump([str(ch) if isinstance(ch, int) else ch for ch in unique_channels], f, ensure_ascii=False, indent=2)
        except Exception as e:
            print(f"⚠️ 保存监控频道列表文件失败: {e}")
            file_ok = False

        if db_ok:
            target_channels = unique_channels
            print(f"✅ 成功保存监控频道列表(DB): {unique_channels}")
        else:
            print(f"⚠️ 频道列表仅写入文件(未写入DB): {unique_channels}")

        return db_ok and file_ok


# ---------------------- 数据库管理 ----------------------
def get_db_connection():
    conn = sqlite3.connect(CONFIG["DATABASE_FILE"], timeout=15, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    with db_lock:
        conn = get_db_connection()
        c = conn.cursor()
        c.execute('''
            CREATE TABLE IF NOT EXISTS users (
                user_id INTEGER PRIMARY KEY,
                username TEXT,
                first_name TEXT,
                last_name TEXT,
                bio TEXT,
                phone TEXT,
                last_seen INTEGER,
                active_usernames_json TEXT,
                business_bio TEXT,
                business_location_json TEXT,
                business_work_hours_json TEXT
            )
        ''')
        c.execute('''
            CREATE TABLE IF NOT EXISTS username_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                old_username TEXT,
                new_username TEXT,
                change_date INTEGER NOT NULL,
                FOREIGN KEY (user_id) REFERENCES users (user_id) ON DELETE CASCADE
            )
        ''')
        c.execute('''
            CREATE TABLE IF NOT EXISTS name_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                old_first_name TEXT,
                new_first_name TEXT,
                old_last_name TEXT,
                new_last_name TEXT,
                change_date INTEGER NOT NULL,
                FOREIGN KEY (user_id) REFERENCES users (user_id) ON DELETE CASCADE
            )
        ''')
        c.execute('''
            CREATE TABLE IF NOT EXISTS bio_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                old_bio TEXT,
                new_bio TEXT,
                change_date INTEGER NOT NULL,
                FOREIGN KEY (user_id) REFERENCES users (user_id) ON DELETE CASCADE
            )
        ''')
        c.execute('''
            CREATE TABLE IF NOT EXISTS phone_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                old_phone TEXT,
                new_phone TEXT,
                change_date INTEGER NOT NULL,
                FOREIGN KEY (user_id) REFERENCES users (user_id) ON DELETE CASCADE
            )
        ''')
        c.execute('''
            CREATE TABLE IF NOT EXISTS message_history (
                message_id INTEGER NOT NULL,
                chat_id INTEGER NOT NULL,
                user_id INTEGER NOT NULL,
                text TEXT,
                message_date INTEGER NOT NULL,
                link TEXT,
                PRIMARY KEY (message_id, chat_id),
                FOREIGN KEY (user_id) REFERENCES users (user_id) ON DELETE CASCADE
            )
        ''')
        c.execute('''
            CREATE TABLE IF NOT EXISTS chat_info (
                chat_id INTEGER PRIMARY KEY,
                title TEXT,
                username TEXT,
                last_updated INTEGER
            )
        ''')
        c.execute('''
            CREATE TABLE IF NOT EXISTS bot_interactors (
                user_id INTEGER PRIMARY KEY,
                last_interaction_date INTEGER NOT NULL
            )
        ''')
        c.execute('''
            CREATE TABLE IF NOT EXISTS poll_broadcast_jobs (
                id TEXT PRIMARY KEY,
                admin_id INTEGER NOT NULL,
                question TEXT NOT NULL,
                options_json TEXT NOT NULL,
                is_anonymous INTEGER NOT NULL DEFAULT 0,
                allows_multiple_answers INTEGER NOT NULL DEFAULT 0,
                target_count INTEGER NOT NULL DEFAULT 0,
                success_count INTEGER NOT NULL DEFAULT 0,
                fail_count INTEGER NOT NULL DEFAULT 0,
                status TEXT NOT NULL DEFAULT 'draft',
                closes_at INTEGER NOT NULL,
                created_at INTEGER NOT NULL,
                started_at INTEGER,
                finished_at INTEGER,
                summary_sent_at INTEGER
            )
        ''')
        c.execute('''
            CREATE TABLE IF NOT EXISTS poll_broadcast_messages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                job_id TEXT NOT NULL,
                user_id INTEGER NOT NULL,
                chat_id INTEGER NOT NULL,
                message_id INTEGER NOT NULL,
                poll_id TEXT NOT NULL,
                created_at INTEGER NOT NULL,
                UNIQUE(job_id, user_id),
                UNIQUE(job_id, poll_id)
            )
        ''')
        c.execute('''
            CREATE TABLE IF NOT EXISTS poll_broadcast_answers (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                job_id TEXT NOT NULL,
                poll_id TEXT NOT NULL,
                user_id INTEGER,
                option_ids_json TEXT NOT NULL,
                updated_at INTEGER NOT NULL,
                UNIQUE(job_id, poll_id, user_id)
            )
        ''')
        c.execute('CREATE INDEX IF NOT EXISTS idx_poll_jobs_status_closes_at ON poll_broadcast_jobs(status, closes_at)')
        c.execute('CREATE INDEX IF NOT EXISTS idx_poll_messages_poll_id ON poll_broadcast_messages(poll_id)')
        c.execute('CREATE INDEX IF NOT EXISTS idx_poll_answers_job_id ON poll_broadcast_answers(job_id)')
        c.execute('''
            CREATE TABLE IF NOT EXISTS business_connections (
                connection_id TEXT PRIMARY KEY,
                user_id INTEGER NOT NULL,
                is_enabled BOOLEAN DEFAULT 1,
                last_updated INTEGER NOT NULL
            )
        ''')
        c.execute('''
            CREATE TABLE IF NOT EXISTS checked_contacts (
                user_id INTEGER NOT NULL,
                contact_id INTEGER NOT NULL,
                last_checked INTEGER NOT NULL,
                is_scammer BOOLEAN DEFAULT 0,
                PRIMARY KEY (user_id, contact_id)
            )
        ''')
        

        # --- Tables for Chatlog Search / Web Jump Feature ---
        c.execute('''
            CREATE TABLE IF NOT EXISTS system_settings (
                key TEXT PRIMARY KEY,
                value TEXT,
                updated INTEGER NOT NULL
            )
        ''')
        c.execute('''
            CREATE TABLE IF NOT EXISTS web_query_tokens (
                token TEXT PRIMARY KEY,
                target_user_id INTEGER NOT NULL,
                requester_user_id INTEGER,
                created INTEGER NOT NULL,
                expires INTEGER NOT NULL
            )
        ''')
        c.execute('''
            CREATE TABLE IF NOT EXISTS web_verify_states (
                state TEXT PRIMARY KEY,
                tg_user_id INTEGER,
                next_path TEXT,
                created INTEGER NOT NULL,
                expires INTEGER NOT NULL,
                used INTEGER NOT NULL DEFAULT 0,
                approved INTEGER NOT NULL DEFAULT 0
            )
        ''')
        c.execute('''
            CREATE TABLE IF NOT EXISTS web_verified_users (
                tg_user_id INTEGER PRIMARY KEY,
                verified_at INTEGER NOT NULL,
                expires INTEGER NOT NULL
            )
        ''')

        c.execute('''
            CREATE TABLE IF NOT EXISTS invite_settings (
                id INTEGER PRIMARY KEY CHECK (id = 1),
                required_count INTEGER NOT NULL DEFAULT 1,
                updated INTEGER NOT NULL
            )
        ''')
        c.execute('''
            CREATE TABLE IF NOT EXISTS user_invites (
                inviter_id INTEGER NOT NULL,
                invited_user_id INTEGER NOT NULL,
                status TEXT NOT NULL DEFAULT 'active',
                invited_at INTEGER NOT NULL,
                left_at INTEGER,
                PRIMARY KEY (inviter_id, invited_user_id)
            )
        ''')

        
        # --- web verify schema upgrades (safe for existing DB) ---
        try:
            c.execute("ALTER TABLE web_verify_states ADD COLUMN approved INTEGER NOT NULL DEFAULT 0")
        except Exception:
            pass

        c.execute('''
            CREATE TABLE IF NOT EXISTS web_verify_pending (
                state TEXT PRIMARY KEY,
                tg_user_id INTEGER NOT NULL,
                created INTEGER NOT NULL,
                expires INTEGER NOT NULL,
                notified INTEGER NOT NULL DEFAULT 0
            )
        ''')

        try:
            now_ts = int(time.time())
            default_enabled = '1' if str(CONFIG.get('CHATLOG_FEATURE_DEFAULT','1')).strip().lower() not in ('0','false','no') else '0'
            c.execute(
                "INSERT OR IGNORE INTO system_settings (key, value, updated) VALUES (?, ?, ?)",
                ('chatlog_feature_enabled', default_enabled, now_ts)
            )
            crawl_default_enabled = '1' if str(CONFIG.get('CRAWLER_FEATURE_DEFAULT','1')).strip().lower() not in ('0','false','no') else '0'
            c.execute(
                "INSERT OR IGNORE INTO system_settings (key, value, updated) VALUES (?, ?, ?)",
                ('crawler_enabled', crawl_default_enabled, now_ts)
            )
            invite_required_default = int(CONFIG.get('INVITE_REQUIRED_COUNT_DEFAULT', 1) or 1)
            if invite_required_default < 0:
                invite_required_default = 0
            c.execute(
                "INSERT OR IGNORE INTO invite_settings (id, required_count, updated) VALUES (1, ?, ?)",
                (invite_required_default, now_ts)
            )
        except Exception:
            pass

        # --- Tables for Business Ledger Feature ---
        c.execute('''
            CREATE TABLE IF NOT EXISTS business_ledgers (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                contact_id INTEGER NOT NULL,
                balance REAL NOT NULL DEFAULT 0.0,
                currency TEXT NOT NULL DEFAULT '$',
                auto_pin BOOLEAN NOT NULL DEFAULT 0,
                UNIQUE(user_id, contact_id)
            )
        ''')
        c.execute('''
            CREATE TABLE IF NOT EXISTS ledger_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ledger_id INTEGER NOT NULL,
                timestamp INTEGER NOT NULL,
                amount REAL NOT NULL,
                new_balance REAL NOT NULL,
                description TEXT,
                FOREIGN KEY (ledger_id) REFERENCES business_ledgers (id) ON DELETE CASCADE
            )
        ''')

        # --- Tables for Premium Auto-Reply Features ---
        c.execute('''
            CREATE TABLE IF NOT EXISTS offline_replies (
                user_id INTEGER PRIMARY KEY,
                is_enabled BOOLEAN DEFAULT 0,
                reply_message_json TEXT,
                last_updated INTEGER,
                FOREIGN KEY (user_id) REFERENCES users (user_id) ON DELETE CASCADE
            )
        ''')
        c.execute('''
            CREATE TABLE IF NOT EXISTS keyword_replies (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                keyword TEXT NOT NULL,
                reply_message_json TEXT NOT NULL,
                last_updated INTEGER,
                FOREIGN KEY (user_id) REFERENCES users (user_id) ON DELETE CASCADE
            )
        ''')
        
        # ---------------------- 新增：赞助功能数据库表 ----------------------
        c.execute('''
            CREATE TABLE IF NOT EXISTS okpay_orders (
                order_id TEXT PRIMARY KEY,
                user_id INTEGER NOT NULL,
                amount REAL NOT NULL,
                status TEXT NOT NULL DEFAULT 'pending',
                timestamp INTEGER NOT NULL
            )
        ''')
        c.execute('''
            CREATE TABLE IF NOT EXISTS sponsorships (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                amount_usdt REAL NOT NULL,
                order_id TEXT UNIQUE,
                timestamp INTEGER NOT NULL,
                FOREIGN KEY (user_id) REFERENCES users (user_id) ON DELETE SET NULL
            )
        ''')
        c.execute('''
            CREATE TABLE IF NOT EXISTS sponsors (
                user_id INTEGER PRIMARY KEY,
                total_amount_usdt REAL NOT NULL DEFAULT 0.0,
                last_sponsored_at INTEGER NOT NULL,
                FOREIGN KEY (user_id) REFERENCES users (user_id) ON DELETE CASCADE
            )
        ''')
        # ---------------------- 新增结束 ----------------------


        # --- Indexes ---
        c.execute('CREATE INDEX IF NOT EXISTS idx_message_history_user_id ON message_history (user_id, message_date DESC)')
        c.execute('CREATE INDEX IF NOT EXISTS idx_username_history_user_id ON username_history (user_id, change_date DESC)')
        c.execute('CREATE INDEX IF NOT EXISTS idx_username_history_new_username ON username_history (new_username)')
        c.execute('CREATE INDEX IF NOT EXISTS idx_users_username ON users (username)')
        c.execute('CREATE INDEX IF NOT EXISTS idx_name_history_user_id ON name_history (user_id, change_date DESC)')
        c.execute('CREATE INDEX IF NOT EXISTS idx_bio_history_user_id ON bio_history (user_id, change_date DESC)')
        c.execute('CREATE INDEX IF NOT EXISTS idx_phone_history_user_id ON phone_history (user_id, change_date DESC)')
        c.execute('CREATE INDEX IF NOT EXISTS idx_message_history_chat_user ON message_history (chat_id, user_id)')
        c.execute('CREATE INDEX IF NOT EXISTS idx_chat_info_chat_id ON chat_info (chat_id)')
        c.execute('CREATE INDEX IF NOT EXISTS idx_bot_interactors_user_id ON bot_interactors (user_id)')
        c.execute('CREATE INDEX IF NOT EXISTS idx_business_connections_user_id ON business_connections (user_id)')
        c.execute('CREATE INDEX IF NOT EXISTS idx_checked_contacts_user_contact ON checked_contacts (user_id, contact_id)')
        c.execute('CREATE INDEX IF NOT EXISTS idx_offline_replies_user_id ON offline_replies (user_id)')
        c.execute('CREATE UNIQUE INDEX IF NOT EXISTS idx_keyword_replies_user_keyword ON keyword_replies (user_id, keyword)')
        c.execute('CREATE INDEX IF NOT EXISTS idx_business_ledgers_user_contact ON business_ledgers (user_id, contact_id)')
        c.execute('CREATE INDEX IF NOT EXISTS idx_ledger_history_ledger_id ON ledger_history (ledger_id)')

        # ---------------------- 新增：赞助功能数据库索引 ----------------------
        c.execute('CREATE INDEX IF NOT EXISTS idx_okpay_orders_user_id ON okpay_orders (user_id)')
        c.execute('CREATE INDEX IF NOT EXISTS idx_sponsors_total_amount ON sponsors (total_amount_usdt DESC)')
        c.execute('CREATE INDEX IF NOT EXISTS idx_web_query_tokens_expires ON web_query_tokens (expires)')
        c.execute("CREATE INDEX IF NOT EXISTS idx_user_invites_inviter_status ON user_invites (inviter_id, status)")
        c.execute("CREATE INDEX IF NOT EXISTS idx_user_invites_invited_user ON user_invites (invited_user_id)")

        # ---------------------- 新增结束 ----------------------


        # ---------------------- 新增：频道/投稿 持久化（SQLite） ----------------------
        c.execute('''
            CREATE TABLE IF NOT EXISTS monitor_channels (
                channel TEXT PRIMARY KEY,
                added_at INTEGER NOT NULL
            )
        ''')
        c.execute('''
            CREATE TABLE IF NOT EXISTS scam_reports (
                report_id TEXT PRIMARY KEY,
                status TEXT NOT NULL,
                primary_key TEXT,
                record_json TEXT NOT NULL,
                created_at INTEGER NOT NULL
            )
        ''')
        c.execute('CREATE INDEX IF NOT EXISTS idx_scam_reports_status ON scam_reports(status)')
        try:
            c.execute('CREATE UNIQUE INDEX IF NOT EXISTS idx_scam_reports_primary_verified ON scam_reports(primary_key) WHERE status="verified"')
        except Exception:
            pass

        # --- 一次性迁移：channels.json -> monitor_channels（仅当表为空时） ---
        try:
            c.execute("SELECT COUNT(1) FROM monitor_channels")
            _cnt = c.fetchone()[0]
            if _cnt == 0 and os.path.exists(CONFIG["CHANNELS_FILE"]):
                try:
                    with open(CONFIG["CHANNELS_FILE"], "r", encoding="utf-8") as f:
                        _chs = json.load(f) or []
                except Exception:
                    _chs = []
                _now = int(time.time())
                for _ch in _chs:
                    _s = str(_ch).strip()
                    if not _s:
                        continue
                    if not _s.startswith('@') and not (_s.isdigit() or (_s.startswith('-') and _s[1:].isdigit())):
                        _s = '@' + _s.lstrip('@')
                    c.execute("INSERT OR IGNORE INTO monitor_channels(channel, added_at) VALUES(?, ?)", (_s, _now))
        except Exception as _e:
            print(f"⚠️ 迁移频道列表到数据库失败: {_e}")

        # --- 一次性迁移：reports.json -> scam_reports（仅当表为空时） ---
        try:
            c.execute("SELECT COUNT(1) FROM scam_reports")
            _rcnt = c.fetchone()[0]
            if _rcnt == 0 and os.path.exists(CONFIG["REPORTS_FILE"]):
                _data = load_json_file(CONFIG["REPORTS_FILE"], reports_lock) or {}
                if "pending" not in _data: _data["pending"] = {}
                if "verified" not in _data: _data["verified"] = {}
                _now = int(time.time())
                for _sid, _rec in (_data.get("pending") or {}).items():
                    try:
                        _j = json.dumps(_rec, ensure_ascii=False)
                    except Exception:
                        _j = "{}"
                    c.execute(
                        "INSERT OR REPLACE INTO scam_reports (report_id, status, primary_key, record_json, created_at) VALUES (?, ?, ?, ?, ?)",
                        (str(_sid), "pending", None, _j, _now)
                    )
                for _pk, _rec in (_data.get("verified") or {}).items():
                    try:
                        _j = json.dumps(_rec, ensure_ascii=False)
                    except Exception:
                        _j = "{}"
                    _rid = f"verified:{str(_pk)}"
                    c.execute(
                        "INSERT OR REPLACE INTO scam_reports (report_id, status, primary_key, record_json, created_at) VALUES (?, ?, ?, ?, ?)",
                        (_rid, "verified", str(_pk), _j, _now)
                    )
        except Exception as _e:
            print(f"⚠️ 迁移投稿数据到数据库失败: {_e}")

        conn.commit()
        
        try:
            c.execute("ALTER TABLE users ADD COLUMN business_bio TEXT")
            c.execute("ALTER TABLE users ADD COLUMN business_location_json TEXT")
            c.execute("ALTER TABLE users ADD COLUMN business_work_hours_json TEXT")
            conn.commit()
            print("🗃️ 数据库 'users' 表已更新，增加了 Business 字段。")
        except sqlite3.OperationalError as e:
            if "duplicate column name" not in str(e):
                raise e
        
        conn.close()
    print(f"🗃️ {escape_markdown('数据库初始化完成')} \\({escape_markdown(BOT_VERSION.split('|')[0].strip())} Schema\\)\\.")




# ---------------------- 新增：聊天记录查询/跳转 & 管理辅助 ----------------------

def get_setting_from_db(key, default=None):
    try:
        with db_lock:
            conn = get_db_connection()
            cur = conn.cursor()
            cur.execute("SELECT value FROM system_settings WHERE key = ?", (key,))
            row = cur.fetchone()
            conn.close()
        return row[0] if row else default
    except Exception:
        return default


def set_setting_in_db(key, value):
    try:
        now_ts = int(time.time())
        with db_lock:
            conn = get_db_connection()
            cur = conn.cursor()
            cur.execute(
                "INSERT INTO system_settings (key, value, updated) VALUES (?, ?, ?) "
                "ON CONFLICT(key) DO UPDATE SET value=excluded.value, updated=excluded.updated",
                (key, str(value), now_ts),
            )
            conn.commit()
            conn.close()
        return True
    except Exception:
        return False


def is_chatlog_enabled():
    v = get_setting_from_db('chatlog_feature_enabled', None)
    if v is None:
        default_enabled = str(CONFIG.get('CHATLOG_FEATURE_DEFAULT', '1')).strip() not in ('0','false','False','no','NO')
        return bool(default_enabled)
    return str(v).strip() not in ('0','false','False','no','NO')


def is_crawler_enabled():
    v = get_setting_from_db('crawler_enabled', None)
    if v is None:
        default_enabled = str(CONFIG.get('CRAWLER_FEATURE_DEFAULT', '1')).strip() not in ('0','false','False','no','NO')
        return bool(default_enabled)
    return str(v).strip() not in ('0','false','False','no','NO')


def set_crawler_enabled(enabled: bool) -> bool:
    return set_setting_in_db('crawler_enabled', '1' if enabled else '0')


def _reset_new_key(user_id: int) -> str:
    return f"resetnew_user_{int(user_id)}"


def is_user_reset_as_new(user_id: int) -> bool:
    v = get_setting_from_db(_reset_new_key(user_id), '0')
    return str(v).strip() in ('1', 'true', 'True', 'yes', 'YES')


def mark_user_reset_as_new(user_id: int, enabled: bool) -> bool:
    return set_setting_in_db(_reset_new_key(user_id), '1' if enabled else '0')


def get_invite_required_count() -> int:
    try:
        with db_lock:
            conn = get_db_connection()
            cur = conn.cursor()
            cur.execute("SELECT required_count FROM invite_settings WHERE id = 1")
            row = cur.fetchone()
            conn.close()
        if row:
            value = int(row[0])
            return value if value >= 0 else 0
    except Exception:
        pass
    fallback = int(CONFIG.get('INVITE_REQUIRED_COUNT_DEFAULT', 1) or 1)
    return fallback if fallback >= 0 else 0


def set_invite_required_count(new_count: int) -> bool:
    try:
        count = int(new_count)
        if count < 0:
            count = 0
        now_ts = int(time.time())
        with db_lock:
            conn = get_db_connection()
            cur = conn.cursor()
            cur.execute(
                "INSERT INTO invite_settings (id, required_count, updated) VALUES (1, ?, ?) "
                "ON CONFLICT(id) DO UPDATE SET required_count=excluded.required_count, updated=excluded.updated",
                (count, now_ts),
            )
            conn.commit()
            conn.close()
        return True
    except Exception:
        return False


def get_user_invite_stats(user_id: int) -> tuple:
    try:
        with db_lock:
            conn = get_db_connection()
            cur = conn.cursor()
            cur.execute(
                "SELECT COUNT(1) FROM user_invites WHERE inviter_id = ?",
                (int(user_id),),
            )
            total = int((cur.fetchone() or [0])[0] or 0)
            cur.execute(
                "SELECT COUNT(1) FROM user_invites WHERE inviter_id = ? AND status = 'active'",
                (int(user_id),),
            )
            active = int((cur.fetchone() or [0])[0] or 0)
            conn.close()
        return active, total
    except Exception:
        return 0, 0


def user_can_use_bot(user_id: int) -> bool:
    if int(user_id) == int(CONFIG.get("ADMIN_ID", 0)):
        return True
    required = get_invite_required_count()
    if required <= 0:
        return True
    active, _ = get_user_invite_stats(int(user_id))
    return active >= required


def get_invite_gate_message(user_id: int) -> tuple:
    required = get_invite_required_count()
    active, total = get_user_invite_stats(int(user_id))
    shortfall = max(required - active, 0)
    text = (
        f"🚫 *{escape_markdown('访问受限')}*\n\n"
        f"{escape_markdown('目前需要邀请新用户后才能永久使用机器人。')}\n"
        f"{escape_markdown('这是为了给项目引入一些新血液，感谢理解与支持。')}\n\n"
        f"*{escape_markdown('规则说明')}*\n"
        f"• {escape_markdown('被邀请人必须先从你的专属链接进入机器人')}\n"
        f"• {escape_markdown('并加入官方频道后，才记为有效邀请')}\n"
        f"• {escape_markdown('如果邀请对象是老用户，不会计入邀请人数')}\n"
        f"• {escape_markdown('若你邀请的人后续退频道，会清空你的邀请进度，需要重新邀请')}\n\n"
        f"*{escape_markdown('当前进度')}* {escape_markdown(str(active))}/{escape_markdown(str(required))}"
        f" {escape_markdown('(历史累计:')} {escape_markdown(str(total))}{escape_markdown(')')}\n"
        f"{escape_markdown('还差')} *{escape_markdown(str(shortfall))}* {escape_markdown('人')}"
    )
    return text, required


def make_invite_deeplink(user_id: int) -> str:
    try:
        me = bot.get_me()
        username = (getattr(me, 'username', '') or '').strip()
        if username:
            return f"https://t.me/{username}?start=inv_{int(user_id)}"
    except Exception:
        pass
    return ""


def invite_link_text(user_id: int) -> str:
    link = make_invite_deeplink(user_id)
    if not link:
        return ""
    return (
        f"\n\n*{escape_markdown('你的邀请链接（可直接复制转发）')}*\n"
        f"`{escape_for_code(link)}`"
    )


def create_web_query_token(target_user_id: int, requester_user_id: int = None, ttl_sec: int = None) -> str:
    # 返回 token，用于跳转网页：/chatlog/token/<token>
    ttl = int(ttl_sec or CONFIG.get('CHATLOG_TOKEN_TTL_SEC', 900) or 900)
    now_ts = int(time.time())
    token = YOUR_TELEGRAM_BOT_TOKEN
    expires = now_ts + ttl
    try:
        with db_lock:
            conn = get_db_connection()
            cur = conn.cursor()
            cur.execute(
                "INSERT INTO web_query_tokens (token, target_user_id, requester_user_id, created, expires) VALUES (?, ?, ?, ?, ?)",
                (token, int(target_user_id), int(requester_user_id) if requester_user_id else None, now_ts, expires),
            )
            conn.commit()
            conn.close()
    except Exception as e:
        print(f"⚠️ 创建 web_query_token 失败: {e}")
    return token


def cleanup_expired_web_tokens():
    try:
        now_ts = int(time.time())
        with db_lock:
            conn = get_db_connection()
            cur = conn.cursor()
            cur.execute("DELETE FROM web_query_tokens WHERE expires < ?", (now_ts,))
            conn.commit()
            conn.close()
    except Exception:
        pass


def get_db_size_bytes():
    try:
        db_path = CONFIG.get('DATABASE_FILE')
        if db_path and os.path.exists(db_path):
            return os.path.getsize(db_path)
    except Exception:
        pass
    return 0


def count_table_rows(table_name: str) -> int:
    try:
        with db_lock:
            conn = get_db_connection()
            cur = conn.cursor()
            cur.execute(f"SELECT COUNT(*) FROM {table_name}")
            n = int(cur.fetchone()[0])
            conn.close()
        return n
    except Exception:
        return 0


def delete_oldest_messages(limit_count: int) -> int:
    try:
        n = int(limit_count or 0)
        if n <= 0:
            return 0
        with db_lock:
            conn = get_db_connection()
            cur = conn.cursor()

            # 优先按 message_date 删除最旧记录；若索引/页损坏则退化为 rowid 顺序，避免一直返回 0
            try:
                cur.execute("SELECT rowid FROM message_history ORDER BY message_date ASC LIMIT ?", (n,))
                rows = cur.fetchall()
            except Exception:
                cur.execute("SELECT rowid FROM message_history ORDER BY rowid ASC LIMIT ?", (n,))
                rows = cur.fetchall()

            if not rows:
                conn.close()
                return 0

            rowids = [r[0] for r in rows]
            cur.execute(f"DELETE FROM message_history WHERE rowid IN ({','.join(['?']*len(rowids))})", rowids)
            deleted = cur.rowcount
            conn.commit()
            conn.close()
        return int(deleted or 0)
    except Exception as e:
        print(f"⚠️ 删除最旧聊天记录失败: {e}")
        return -1


def delete_messages_by_keywords(keywords):
    # keywords: list[str]
    try:
        if not keywords:
            return 0
        total_deleted = 0
        with db_lock:
            conn = get_db_connection()
            cur = conn.cursor()
            for kw in keywords:
                kw = (kw or '').strip()
                if not kw:
                    continue
                cur.execute("DELETE FROM message_history WHERE text LIKE ?", (f"%{kw}%",))
                total_deleted += int(cur.rowcount or 0)
            conn.commit()
            conn.close()
        return total_deleted
    except Exception as e:
        print(f"⚠️ 按关键词删除失败: {e}")
        return -1

def vacuum_database():
    try:
        with db_lock:
            conn = get_db_connection()
            cur = conn.cursor()
            cur.execute('VACUUM')
            conn.commit()
            conn.close()
        return True
    except Exception as e:
        print(f"⚠️ VACUUM 失败: {e}")
        return False




def _escape_url_md(url: str) -> str:
    """Escape a URL for Telegram MarkdownV2 link target."""
    if not url:
        return ''
    return str(url).replace('\\', '\\\\').replace(')', '\\)').replace('(', '\\(')

def _chat_open_link(chat_id: int, username: str) -> str:
    """Best-effort link to open chat/channel itself."""
    try:
        if username:
            u = str(username).lstrip('@')
            return f"https://t.me/{u}"
        cid = abs(int(chat_id))
        s = str(cid)
        if s.startswith("100"):
            s = s[3:]
            return f"https://t.me/c/{s}/1"
    except Exception:
        pass
    return ''

def _message_link(chat_id: int, username: str, msg_id: int) -> str:
    """Best-effort link to open a specific message."""
    try:
        if username:
            u = str(username).lstrip('@')
            return f"https://t.me/{u}/{int(msg_id)}"
        cid = abs(int(chat_id))
        s = str(cid)
        if s.startswith("100"):
            s = s[3:]
            return f"https://t.me/c/{s}/{int(msg_id)}"
    except Exception:
        pass
    return ''



def send_chatlog_link(message, target_user_id: int):
    """在机器人里直接展示聊天记录预览 + 提供网页完整记录按钮（无需额外指令）。"""
    if not is_chatlog_enabled():
        return

    base_url = (os.getenv('WEB_BASE_URL', '') or CONFIG.get('WEB_BASE_URL', '') or '').strip()
    has_base_url = bool(base_url)

    # 1) 查询最近消息 + 群/频道汇总（从本地 history.db）
    recent_msgs = []
    top_chats = []
    try:
        with db_lock:
            conn = get_db_connection()
            cur = conn.cursor()

            cur.execute(
                """
                SELECT m.chat_id, m.message_id, m.text, m.link, m.message_date,
                       COALESCE(ci.title, '频道/群') AS title,
                       COALESCE(ci.username, '') AS username
                FROM message_history m
                LEFT JOIN chat_info ci ON ci.chat_id = m.chat_id
                WHERE m.user_id = ?
                ORDER BY m.message_date DESC
                LIMIT 10
                """,
                (int(target_user_id),),
            )
            recent_msgs = [dict(r) for r in cur.fetchall()]

            cur.execute(
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
                LIMIT 8
                """,
                (int(target_user_id),),
            )
            top_chats = [dict(r) for r in cur.fetchall()]
            conn.close()
    except Exception:
        recent_msgs = []
        top_chats = []

    if not recent_msgs and not top_chats:
        # 没有任何聊天记录时，仅给出网页按钮（如果配置了）或提示
        if has_base_url:
            cleanup_expired_web_tokens()
            token = YOUR_TELEGRAM_BOT_TOKEN
            url = base_url.rstrip('/') + f"/chatlog/token/{token}"
            markup = types.InlineKeyboardMarkup()
            markup.add(types.InlineKeyboardButton('📜 打开网页查看（暂无记录）', url=url))
            bot.send_message(
                message.chat.id,
                '📜 未在数据库中发现该用户的聊天记录（可能还未抓取到）。',
                reply_parameters=ReplyParameters(message_id=message.message_id, allow_sending_without_reply=True),
                reply_markup=markup,
                disable_web_page_preview=True,
            )
        else:
            bot.send_message(message.chat.id, '📜 未在数据库中发现该用户的聊天记录。', disable_web_page_preview=True)
        return

    # 2) 组装 MarkdownV2 文本
    lines = []
    lines.append(f"📜 *{escape_markdown('聊天记录预览')}*")
    lines.append(f"{escape_markdown('用户ID:')} `{escape_for_code(str(target_user_id))}`")
    lines.append("")

    if recent_msgs:
        lines.append(f"🕘 *{escape_markdown('最近 10 条发言')}*")
        for r in recent_msgs:
            ts = int(r.get('message_date') or 0)
            dt = ''
            try:
                dt = datetime.fromtimestamp(ts, tz=CHINA_TZ).strftime('%m-%d %H:%M') if ts else ''
            except Exception:
                dt = ''
            title = (r.get('title') or '频道/群').strip()
            username = (r.get('username') or '').strip()
            chat_url = _chat_open_link(int(r.get('chat_id')), username)
            msg_url = (r.get('link') or '').strip() or _message_link(int(r.get('chat_id')), username, int(r.get('message_id') or 0))
            text = (r.get('text') or '').replace('\n', ' ').strip()
            if len(text) > 80:
                text = text[:80] + '…'

            title_link = escape_markdown(_sanitize_for_link_text(title))
            chat_part = f"[{title_link}]({_escape_url_md(chat_url)})" if chat_url else escape_markdown(title)
            jump_part = f" [跳转]({_escape_url_md(msg_url)})" if msg_url else ""
            lines.append(f"› `{escape_for_code(dt)}` \| {chat_part} \- {escape_markdown(text)}{jump_part}")
        lines.append("")

    if top_chats:
        lines.append(f"👥 *{escape_markdown('参与的群/频道')}*")
        for c in top_chats:
            title = (c.get('title') or '频道/群').strip()
            username = (c.get('username') or '').strip()
            chat_url = _chat_open_link(int(c.get('chat_id')), username)
            cnt = int(c.get('msg_count') or 0)
            title_link = escape_markdown(_sanitize_for_link_text(title))
            chat_part = f"[{title_link}]({_escape_url_md(chat_url)})" if chat_url else escape_markdown(title)
            lines.append(f"› {chat_part} \- *{escape_markdown(str(cnt))}* {escape_markdown('条')}")
        lines.append("")

    # 3) 网页完整记录按钮（可选）
    markup = None
    if has_base_url:
        cleanup_expired_web_tokens()
        token = YOUR_TELEGRAM_BOT_TOKEN
        url = base_url.rstrip('/') + f"/chatlog/token/{token}"
        markup = types.InlineKeyboardMarkup()
        markup.add(types.InlineKeyboardButton('📜 打开网页查看完整记录', url=url))

    text_to_send = "\n".join(lines).strip()

    try:
        should_reply = not (getattr(message, 'text', None) and message.text.startswith('/start bizChat'))
        reply_params = ReplyParameters(message_id=message.message_id, allow_sending_without_reply=True) if should_reply else None
        bot.send_message(
            message.chat.id,
            text_to_send,
            reply_parameters=reply_params,
            reply_markup=markup,
            parse_mode="MarkdownV2",
            disable_web_page_preview=True,
        )
    except ApiTelegramException as e:
        # 兜底：去掉 Markdown 格式
        safe_text = re.sub(r'[_*\[\]()~`>#+\-=|{}.!\\]', '', text_to_send)[:4096]
        bot.send_message(message.chat.id, safe_text, reply_markup=markup, disable_web_page_preview=True, parse_mode=None)
    except Exception as e:
        print(f"⚠️ 发送聊天记录预览失败: {e}")

# --------------------------------------------------------------------
# ---------------------- 日志发送逻辑 ----------------------
def send_log_to_channel(text):
    if CONFIG.get("LOG_CHANNEL_ID"):
        with log_buffer_lock:
            log_buffer.append(text)

def log_batcher_thread():
    while True:
        time.sleep(CONFIG["LOG_BATCH_INTERVAL"])
        
        with log_buffer_lock:
            if not log_buffer:
                continue
            logs_to_send = list(log_buffer)
            log_buffer.clear()

        if not logs_to_send:
            continue
            
        message_parts = []
        current_part = f"📝 *批量日志更新* \\| {escape_markdown(datetime.now(timezone.utc).astimezone(CHINA_TZ).strftime('%Y-%m-%d %H:%M:%S CST'))}\n\n"
        
        for log_item in logs_to_send:
            if len(current_part) + len(log_item) + 2 > CONFIG["LOG_MAX_MESSAGE_LENGTH"]:
                message_parts.append(current_part)
                current_part = f"📝 *批量日志更新 \\| 续*\n\n"
            current_part += log_item + "\n\n"
        message_parts.append(current_part)

        for part in message_parts:
            try:
                bot.send_message(CONFIG["LOG_CHANNEL_ID"], part, parse_mode="MarkdownV2", disable_web_page_preview=True)
                if len(message_parts) > 1:
                    time.sleep(1)
            except Exception as e:
                print(f"⚠️ 批量发送日志到频道失败: {e}")
                print(f"❌ 已丢弃失败的日志批次以防止循环错误。内容:\n{part[:1000]}...")
                break

# ---------------------- 用户与消息处理 (核心数据抓取逻辑) ----------------------
async def update_user_in_db(user_from_event: User):
    """
    This is the core function for synchronizing user data.
    It fetches the latest full user profile from Telegram's API
    and updates the local database, creating change history records.
    (FIXED VERSION 2)
    """
    if not is_crawler_enabled():
        return

    if not user_from_event or not isinstance(user_from_event, User) or user_from_event.bot:
        return

    now = int(time.time())
    user_id = user_from_event.id
    
    canonical_user = None
    full_bio = None
    business_bio = None
    business_location_json = None
    business_work_hours_json = None

    try:
        full_user_info = await client(GetFullUserRequest(user_id))
        
        if hasattr(full_user_info, 'users') and full_user_info.users:
            found_user = next((u for u in full_user_info.users if u.id == user_id), None)
            if found_user:
                canonical_user = found_user
        
        if hasattr(full_user_info, 'full_user'):
            full_user_data = full_user_info.full_user
            full_bio = full_user_data.about
            business_bio = getattr(full_user_data, 'business_bio', None)

            if getattr(full_user_data, 'business_location', None):
                loc = full_user_data.business_location
                loc_data = {'address': loc.address}
                if isinstance(loc.geo_point, GeoPoint):
                    loc_data['geo'] = {'lat': loc.geo_point.lat, 'long': loc.geo_point.long}
                business_location_json = json.dumps(loc_data)
            
            if getattr(full_user_data, 'business_work_hours', None):
                wh = full_user_data.business_work_hours
                wh_data = {'timezone_id': wh.timezone_id, 'periods': []}
                if hasattr(wh, 'periods') and wh.periods is not None:
                    wh_data['periods'] = [{'start_minute': p.start_minute, 'end_minute': p.end_minute} for p in wh.periods]
                business_work_hours_json = json.dumps(wh_data)

    except (PeerIdInvalidError, TypeError, ValueError):
        print(f"ℹ️ [Profile-Update] 无法获取用户 {user_id} 的完整信息 (可能已删除/无效). 使用事件数据作为备用。")
    except Exception as e:
        print(f"⚠️ [Profile-Error] 获取用户 {user_id} 完整资料时发生未知错误: {e}. 使用事件数据作为备用。")
    
    if not canonical_user:
        canonical_user = user_from_event

    with db_lock:
        conn = get_db_connection()
        c = conn.cursor()
        
        c.execute("SELECT * FROM users WHERE user_id = ?", (user_id,))
        db_user = c.fetchone()
        
        api_active_usernames = set()
        if canonical_user.username:
            api_active_usernames.add(canonical_user.username)
        if hasattr(canonical_user, 'usernames') and canonical_user.usernames:
            for u_obj in canonical_user.usernames:
                if u_obj.active:
                    api_active_usernames.add(u_obj.username)
        
        new_active_usernames_json = json.dumps(sorted(list(api_active_usernames)))
        
        new_rep_username = canonical_user.username
        if new_rep_username is None and api_active_usernames:
            new_rep_username = sorted(list(api_active_usernames))[0]
        
        display_name = (canonical_user.first_name or "") + (" " + canonical_user.last_name if canonical_user.last_name else "")
        user_link_for_log = f"[{escape_markdown(truncate_for_link_text(display_name or f'User {user_id}'))}](tg://user?id={user_id})"

        if not db_user: # New user, insert everything
            c.execute('''
                INSERT INTO users (user_id, username, first_name, last_name, bio, phone, last_seen, active_usernames_json,
                                   business_bio, business_location_json, business_work_hours_json)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (user_id, new_rep_username, canonical_user.first_name, canonical_user.last_name, full_bio, canonical_user.phone, now, new_active_usernames_json,
                  business_bio, business_location_json, business_work_hours_json))
            
            log_entries = []
            if api_active_usernames:
                c.execute('''INSERT INTO username_history (user_id, new_username, change_date) VALUES (?, ?, ?)''', (user_id, new_rep_username, now))
                log_entries.append(f"✍️ *{escape_markdown('用户名:')}* " + ", ".join([f"`@{escape_for_code(u)}`" for u in sorted(list(api_active_usernames))]))

            if display_name.strip():
                c.execute('''INSERT INTO name_history (user_id, new_first_name, new_last_name, change_date) VALUES (?, ?, ?, ?)''', (user_id, canonical_user.first_name, canonical_user.last_name, now))
                log_entries.append(f"👤 *{escape_markdown('姓名:')}* {escape_markdown(display_name.strip())}")

            if full_bio:
                c.execute('''INSERT INTO bio_history (user_id, new_bio, change_date) VALUES (?, ?, ?)''', (user_id, full_bio, now))
            
            if canonical_user.phone:
                 c.execute('''INSERT INTO phone_history (user_id, new_phone, change_date) VALUES (?, ?, ?)''', (user_id, canonical_user.phone, now))
                 log_entries.append(f"📱 *{escape_markdown('手机:')}* `{escape_for_code(canonical_user.phone)}`")

            if log_entries:
                log_text = f"✅ *{escape_markdown('新用户入库')}*\n*{escape_markdown('用户:')}* {user_link_for_log} \\| `{user_id}`\n" + "\n".join(log_entries)
                send_log_to_channel(log_text)
        
        else: # Existing user, check for changes and update
            # Name change detection
            old_db_name = ((db_user['first_name'] or "") + " " + (db_user['last_name'] or "")).strip()
            api_name = display_name.strip()
            if api_name != old_db_name:
                c.execute('''INSERT INTO name_history (user_id, old_first_name, new_first_name, old_last_name, new_last_name, change_date) VALUES (?, ?, ?, ?, ?, ?)''',
                          (user_id, db_user['first_name'], canonical_user.first_name, db_user['last_name'], canonical_user.last_name, now))
                send_log_to_channel(f"🔄 *{escape_markdown('姓名变更')}*\n*{escape_markdown('用户:')}* {user_link_for_log}\n*{escape_markdown('旧:')}* {escape_markdown(old_db_name or '无')}\n*{escape_markdown('新:')}* {escape_markdown(api_name or '无')}")

            # Username change detection (ROBUST FIX)
            old_rep_username_from_db = db_user['username']
            if new_rep_username != old_rep_username_from_db:
                c.execute('''INSERT INTO username_history (user_id, old_username, new_username, change_date) VALUES (?, ?, ?, ?)''',
                          (user_id, old_rep_username_from_db, new_rep_username, now))
                log_msg = (f"🔄 *{escape_markdown('主用户名变更')}*\n"
                           f"*{escape_markdown('用户:')}* {user_link_for_log}\n"
                           f"*{escape_markdown('旧:')}* {escape_markdown(f'@{old_rep_username_from_db}' if old_rep_username_from_db else '无')}\n"
                           f"*{escape_markdown('新:')}* {escape_markdown(f'@{new_rep_username}' if new_rep_username else '无')}")
                send_log_to_channel(log_msg)

            # Active usernames list change detection (to avoid duplicate logs)
            old_active_usernames_set = set(json.loads(db_user['active_usernames_json'])) if db_user['active_usernames_json'] else set()
            if api_active_usernames != old_active_usernames_set and new_rep_username == old_rep_username_from_db:
                added = api_active_usernames - old_active_usernames_set
                removed = old_active_usernames_set - api_active_usernames
                if added or removed:
                    log_parts = [f"🔄 *{escape_markdown('用户名列表变更')}*\n*{escape_markdown('用户:')}* {user_link_for_log}"]
                    if added: log_parts.append(f"*{escape_markdown('增加:')}* " + ", ".join([f"`@{escape_for_code(u)}`" for u in sorted(list(added))]))
                    if removed: log_parts.append(f"*{escape_markdown('移除:')}* " + ", ".join([f"`@{escape_for_code(u)}`" for u in sorted(list(removed))]))
                    send_log_to_channel('\n'.join(log_parts))

            # Bio change detection
            if full_bio is not None and full_bio != db_user['bio']:
                 c.execute('''INSERT INTO bio_history (user_id, old_bio, new_bio, change_date) VALUES (?, ?, ?, ?)''', (user_id, db_user['bio'], full_bio, now))
                 send_log_to_channel(f"🔄 *{escape_markdown('简介变更')}*\n*{escape_markdown('用户:')}* {user_link_for_log}\n*{escape_markdown('新简介:')}* {escape_markdown(full_bio or '空')}")

            # Phone change detection
            if canonical_user.phone and canonical_user.phone != db_user['phone']:
                c.execute('''INSERT INTO phone_history (user_id, old_phone, new_phone, change_date) VALUES (?, ?, ?, ?)''', (user_id, db_user['phone'], canonical_user.phone, now))
                send_log_to_channel(f"🔄 *{escape_markdown('手机变更')}*\n*{escape_markdown('用户:')}* {user_link_for_log}\n*{escape_markdown('旧:')}* `{escape_for_code(db_user['phone'] or '无')}`\n*{escape_markdown('新:')}* `{escape_for_code(canonical_user.phone)}`")

            # Update the main users table with all new information
            c.execute('''
                UPDATE users
                SET username = ?, first_name = ?, last_name = ?, bio = ?, phone = ?, last_seen = ?, active_usernames_json = ?,
                    business_bio = ?, business_location_json = ?, business_work_hours_json = ?
                WHERE user_id = ?
            ''', (
                new_rep_username,
                canonical_user.first_name,
                canonical_user.last_name,
                full_bio,
                canonical_user.phone,
                now,
                new_active_usernames_json,
                business_bio,
                business_location_json,
                business_work_hours_json,
                user_id
            ))
        conn.commit()
        conn.close()


def _write_message_to_db_sync(message, chat_id, link):
    with db_lock:
        conn = get_db_connection()
        c = conn.cursor()
        try:
            if message.chat and isinstance(message.chat, (Chat, Channel)):
                all_usernames = []
                primary_username = getattr(message.chat, 'username', None)
                if primary_username:
                    all_usernames.append(primary_username)
                if hasattr(message.chat, 'usernames') and message.chat.usernames:
                    for u in message.chat.usernames:
                        if u.active and u.username not in all_usernames:
                            all_usernames.append(u.username)
                
                username_to_store = primary_username
                if not username_to_store and all_usernames:
                    username_to_store = all_usernames[0]

                c.execute('''
                    INSERT INTO chat_info (chat_id, title, username, last_updated)
                    VALUES (?, ?, ?, ?)
                    ON CONFLICT(chat_id) DO UPDATE SET
                        title = excluded.title,
                        username = excluded.username,
                        last_updated = excluded.last_updated
                ''', (chat_id, getattr(message.chat, 'title', None), username_to_store, int(time.time())))

            c.execute('''
                INSERT OR IGNORE INTO message_history (message_id, chat_id, user_id, text, message_date, link)
                VALUES (?, ?, ?, ?, ?, ?)
            ''', (message.id, chat_id, message.sender_id, message.text, int(message.date.timestamp()), link))
            
            conn.commit()
        except sqlite3.IntegrityError:
            pass
        finally:
            conn.close()

async def save_message_to_db_async(message: telebot.types.Message):
    if not is_crawler_enabled():
        return

    if not message or not message.sender_id or not isinstance(message.sender, User) or message.sender.bot:
        return
    
    await update_user_in_db(message.sender)
    
    chat_id = utils.get_peer_id(message.peer_id)
    link = ""

    if hasattr(message.chat, 'username') and message.chat.username:
        link = f"https://t.me/{message.chat.username}/{message.id}"
    elif hasattr(message.peer_id, 'channel_id'):
        full_channel_id = utils.get_peer_id(PeerChannel(message.peer_id.channel_id))
        if str(full_channel_id).startswith("-100"):
             short_id = str(full_channel_id)[4:]
             link = f"https://t.me/c/{short_id}/{message.id}"
    
    await asyncio.to_thread(_write_message_to_db_sync, message, chat_id, link)


def _resolve_historic_query_to_id(query: str):
    query_norm = query.lower().strip().lstrip('@')
    
    with db_lock:
        conn = get_db_connection()
        c = conn.cursor()

        if query_norm.isdigit():
            user_id_int = int(query_norm)
            c.execute("SELECT user_id FROM users WHERE user_id = ?", (user_id_int,))
            res = c.fetchone()
            if res:
                conn.close()
                return res['user_id']
        
        c.execute("SELECT user_id FROM users WHERE LOWER(username) = ?", (query_norm,))
        res = c.fetchone()
        if res:
            conn.close()
            return res['user_id']
        
        search_pattern = f'%"{query_norm}"%'
        c.execute("SELECT user_id FROM users WHERE active_usernames_json LIKE ?", (search_pattern,))
        res = c.fetchone()
        if res:
            conn.close()
            return res['user_id']

        c.execute("SELECT user_id FROM username_history WHERE LOWER(new_username) = ? ORDER BY change_date DESC LIMIT 1", (query_norm,))
        res = c.fetchone()
        if res:
            conn.close()
            return res['user_id']
            
    return None

def _get_profile_state_at_timestamp(c: sqlite3.Cursor, user_id: int, timestamp: int) -> dict:
    c.execute("""
        SELECT new_first_name, new_last_name FROM name_history
        WHERE user_id = ? AND change_date <= ?
        ORDER BY change_date DESC, id DESC LIMIT 1
    """, (user_id, timestamp))
    name_row = c.fetchone()
    full_name = None
    if name_row:
        full_name = f"{name_row['new_first_name'] or ''} {name_row['new_last_name'] or ''}".strip()

    c.execute("""
        SELECT new_username FROM username_history
        WHERE user_id = ? AND change_date <= ?
        ORDER BY change_date DESC, id DESC LIMIT 1
    """, (user_id, timestamp))
    user_row = c.fetchone()
    username = user_row['new_username'] if user_row else None
    
    return {'name': full_name, 'username': username}


def query_user_history_from_db(user_id: int):
    history = {
        "user_id": user_id, "current_profile": None,
        "profile_history": [],
    }
    
    with db_lock:
        conn = get_db_connection()
        c = conn.cursor()

        c.execute("SELECT * FROM users WHERE user_id = ?", (user_id,))
        current_profile = c.fetchone()
        if not current_profile:
            conn.close()
            return None
        history['current_profile'] = current_profile

        all_events = []
        c.execute("SELECT change_date, new_username as detail FROM username_history WHERE user_id = ?", (user_id,))
        for row in c.fetchall():
            all_events.append({'date': row['change_date'], 'detail': row['detail']})
        
        c.execute("SELECT change_date, new_first_name, new_last_name FROM name_history WHERE user_id = ?", (user_id,))
        for row in c.fetchall():
            name = f"{row['new_first_name'] or ''} {row['new_last_name'] or ''}".strip()
            all_events.append({'date': row['change_date'], 'detail': name})

        all_events.sort(key=lambda x: x['date'], reverse=True)
        unique_timestamps = sorted(list(set(event['date'] for event in all_events)), reverse=True)
        
        reconstructed_history = []
        for ts in unique_timestamps:
            snapshot = _get_profile_state_at_timestamp(c, user_id, ts)
            if not reconstructed_history or \
               snapshot['name'] != reconstructed_history[-1]['name'] or \
               snapshot['username'] != reconstructed_history[-1]['username']:
                reconstructed_history.append({
                    'timestamp': ts,
                    'name': snapshot['name'],
                    'username': snapshot['username']
                })
        
        final_display_history = []
        if reconstructed_history:
            for i, snap in enumerate(reconstructed_history):
                keep = True
                if snap['username'] is None and i > 0:
                    if i < len(reconstructed_history) - 1:
                        prev_snap = reconstructed_history[i-1]
                        next_snap = reconstructed_history[i+1]
                        if prev_snap['name'] == next_snap['name']:
                            keep = False
                
                if keep:
                    final_display_history.append(snap)

        truly_final_history = []
        if final_display_history:
            truly_final_history.append(final_display_history[0])
            for i in range(1, len(final_display_history)):
                current_snap = final_display_history[i]
                last_added_snap = truly_final_history[-1]
                if current_snap.get('name') != last_added_snap.get('name') or \
                   current_snap.get('username') != last_added_snap.get('username'):
                    truly_final_history.append(current_snap)

        history['profile_history'] = truly_final_history
        conn.close()
    return history


def query_bio_history_from_db(user_id: int):
    with db_lock:
        conn = get_db_connection()
        c = conn.cursor()
        c.execute("SELECT new_bio, change_date FROM bio_history WHERE user_id = ? AND new_bio IS NOT NULL AND new_bio != '' ORDER BY change_date DESC", (user_id,))
        history_rows = c.fetchall()
        conn.close()

    if not history_rows:
        return []

    deduplicated_history = []
    last_bio = None
    for row in history_rows:
        current_bio = row['new_bio'].strip() if row['new_bio'] else ''
        if current_bio != last_bio:
            deduplicated_history.append({'bio': row['new_bio'], 'date': row['change_date']})
            last_bio = current_bio
            
    return deduplicated_history

def query_phone_history_from_db(user_id: int):
    with db_lock:
        conn = get_db_connection()
        c = conn.cursor()
        c.execute("SELECT new_phone FROM phone_history WHERE user_id = ? AND new_phone IS NOT NULL ORDER BY change_date DESC", (user_id,))
        history = list(dict.fromkeys([row['new_phone'] for row in c.fetchall()]))
        conn.close()
    return history


def query_spoken_groups_from_db(user_id: int):
    with db_lock:
        conn = get_db_connection()
        c = conn.cursor()
        c.execute("SELECT DISTINCT chat_id FROM message_history WHERE user_id = ?", (user_id,))
        spoken_in_chat_ids = {row['chat_id'] for row in c.fetchall()}
        conn.close()
    return spoken_in_chat_ids

def get_chat_info_from_db(chat_id: int):
    with db_lock:
        conn = get_db_connection()
        c = conn.cursor()
        c.execute("SELECT title, username FROM chat_info WHERE chat_id = ?", (chat_id,))
        res = c.fetchone()
        conn.close()
        if res:
            return {'title': res['title'], 'username': res['username']}
        return None

def _get_user_name_from_db(user_id: int) -> str:
    with db_lock:
        conn = get_db_connection()
        c = conn.cursor()
        c.execute("SELECT first_name, last_name FROM users WHERE user_id = ?", (user_id,))
        user_row = c.fetchone()
        conn.close()
        if user_row:
            name = f"{user_row['first_name'] or ''} {user_row['last_name'] or ''}".strip()
            return name if name else str(user_id)
    return str(user_id)

# ---------------------- 活跃用户追踪 ----------------------
active_users = {}
def update_active_user(user_id):
    active_users[user_id] = time.time()

def get_online_user_count():
    threshold_time = time.time() - CONFIG["ONLINE_THRESHOLD"]
    return sum(1 for last_seen in active_users.values() if last_seen > threshold_time)

# ---------------------- Telethon 客户端与核心逻辑 ----------------------
client = TelegramClient(
    CONFIG["telegram_session"], CONFIG["api_id"], CONFIG["api_hash"],
    system_version="4.16.30-vxCUSTOM", device_model="Pixel 7 Pro", app_version="10.10.0"
)
telethon_loop = None
target_channels = []

async def get_user_status_async(user_id: int):
    if not client.is_connected():
        return 'unknown'
    try:
        full_user = await client(GetFullUserRequest(user_id))
        if not hasattr(full_user, 'full_user') or not hasattr(full_user.full_user, 'status'):
            return 'unknown'

        status = full_user.full_user.status
        if isinstance(status, UserStatusOnline):
            return 'online'
        if isinstance(status, UserStatusOffline):
            return 'offline'
        return 'away'
    except Exception as e:
        print(f"⚠️ [Telethon-StatusCheck] 无法获取用户 {user_id} 的状态: {e}")
        return 'unknown'


# ---------------------- 监控频道及共同群组搜索 ----------------------
async def get_common_groups_with_user(user_id: int):
    if not is_crawler_enabled():
        return []

    if not client.is_connected() or not user_id:
        return []

    common_groups = []
    dialog_count = 0
    checked_count = 0
    try:
        async for dialog in client.iter_dialogs():
            dialog_count += 1
            if not dialog.entity or not isinstance(dialog.entity, (Chat, Channel)):
                continue
            if dialog.is_group or (dialog.is_channel and getattr(dialog.entity, 'megagroup', False)):
                checked_count += 1
                try:
                    await client.get_permissions(dialog.entity, user_id)
                    
                    about_text = None
                    all_usernames = []
                    
                    primary_username = dialog.entity.username if hasattr(dialog.entity, 'username') else None
                    if primary_username:
                        all_usernames.append(primary_username)

                    try:
                        full_entity_request = None
                        if isinstance(dialog.entity, Channel):
                            full_entity_request = GetFullChannelRequest(dialog.entity.id)
                        elif isinstance(dialog.entity, Chat):
                            full_entity_request = GetFullChatRequest(dialog.entity.id)

                        if full_entity_request:
                            full_entity = await client(full_entity_request)
                            
                            if hasattr(full_entity, 'full_chat') and hasattr(full_entity.full_chat, 'about'):
                                about_text = full_entity.full_chat.about

                            chat_obj = None
                            if hasattr(full_entity, 'chats') and full_entity.chats:
                                chat_obj = full_entity.chats[0]

                            if chat_obj and hasattr(chat_obj, 'usernames') and chat_obj.usernames:
                                for u in chat_obj.usernames:
                                    if u.active and u.username not in all_usernames:
                                        all_usernames.append(u.username)
                    except Exception:
                        pass
                    
                    common_groups.append({
                        'title': getattr(dialog, 'title', 'Unknown Title'),
                        'id': dialog.id,
                        'about': about_text,
                        'usernames': all_usernames
                    })
                except UserNotParticipantError:
                    continue
                except (ChatAdminRequiredError, ChannelPrivateError, ValueError, PeerIdInvalidError):
                    continue
                except Exception as e:
                    error_str = str(e).lower()
                    safe_title = getattr(dialog, 'title', f"ID:{getattr(dialog, 'id', 'Unknown')}")
                    if "bot can't be participant" not in error_str and "chat not found" not in error_str:
                         print(f"⚠️ [Common-Group-Scan] Skipped '{safe_title}' due to a permission or access error: {type(e).__name__} - {e}")
                    continue
    except Exception as e:
        print(f"💥 [Common-Group-Scan] Major error during dialog iteration for user {user_id}: {e}")
    
    print(f"✅ [Common-Group-Scan] Scan complete. Checked {checked_count}/{dialog_count} chats. Found {len(common_groups)} common groups with user {user_id}.")
    return common_groups


async def search_monitored_channels_for_user(user_id: int = None, raw_query: str = None):
    """
    在监控频道中搜索与指定用户相关的曝光消息。

    之前的实现完全依赖 Telegram 服务器端的 search 参数，有时对纯数字 ID
    （例如 `tg://user?id=102466844` 这种形式）命中不稳定，导致明明频道里有
    曝光，机器人却提示“未找到记录”。

    新实现：
    1. 仍然优先使用服务器端搜索（效率更高）；
    2. 如果某个频道没有命中，再本地扫描该频道最近 N 条消息，在文本中用字符串
       匹配和低成本的关键字搜索做二次兜底。
    这样可以显著提高在“全网骗子公示”等曝光频道中按 ID / 用户名搜索的成功率。
    """
    if not is_crawler_enabled():
        return []

    if not client.is_connected() or (not user_id and not raw_query):
        return []

    # 构造搜索关键词集合
    search_queries = set()

    if user_id:
        # 直接以数字 ID 作为一个关键词
        search_queries.add(str(user_id))

        # 从本地数据库补充用户名 / 历史用户名
        with db_lock:
            conn = get_db_connection()
            c = conn.cursor()

            c.execute("SELECT username FROM users WHERE user_id = ?", (user_id,))
            user = c.fetchone()
            if user and user['username']:
                search_queries.add(user['username'])

            c.execute("SELECT new_username FROM username_history WHERE user_id = ?", (user_id,))
            for row in c.fetchall():
                if row['new_username']:
                    search_queries.add(row['new_username'])
            conn.close()

        # 尝试实时获取最新用户名
        try:
            live_user = await client.get_entity(user_id)
            if getattr(live_user, "username", None):
                search_queries.add(live_user.username)
        except Exception:
            pass

    if raw_query:
        q = raw_query.strip()
        if q:
            # 兼容 @username / 纯用户名 两种写法
            search_queries.add(q.lstrip("@"))
            search_queries.add(q)

    # 清理空字符串
    search_queries = [q for q in search_queries if q]

    if not search_queries:
        return []

    # 要扫描的频道列表：优先使用内存中的 target_channels，兜底从文件加载一次
    channels_to_scan = target_channels or load_channels()
    if not channels_to_scan:
        print("⚠️ [Scam-Scan] 当前没有配置任何监控频道，无法进行扫描。")
        return []

    found_messages = []
    processed_links = set()

    log_terms = ", ".join(search_queries)
    log_target = f"user {user_id}" if user_id else f"raw query '{raw_query}'"
    print(f"📡 [Scam-Scan] 开始为 {log_target} 在 {len(channels_to_scan)} 个监控频道中搜索 (关键词: {log_terms})...")

    scan_depth = CONFIG.get("SCAM_CHANNEL_SCAN_DEPTH", CONFIG.get("SCAM_CHANNEL_SEARCH_LIMIT", 50))
    start_ts = time.time()

    async def build_hit(channel_entity, message):
        """根据消息构造返回给上层的命中结构。"""
        link = ""
        if hasattr(message.chat, "username") and message.chat.username:
            link = f"https://t.me/{message.chat.username}/{message.id}"
        elif hasattr(message.peer_id, "channel_id"):
            try:
                full_channel_id = utils.get_peer_id(PeerChannel(message.peer_id.channel_id))
                full_channel_id_str = str(full_channel_id)
                short_id = full_channel_id_str[4:] if full_channel_id_str.startswith("-100") else full_channel_id_str
                link = f"https://t.me/c/{short_id}/{message.id}"
            except Exception:
                link = ""

        if link and link not in processed_links:
            processed_links.add(link)
            found_messages.append(
                {
                    "link": link,
                    "text": message.text or "",
                    "chat_title": getattr(channel_entity, "title", str(channel_entity.id)),
                }
            )

    for channel_ref in channels_to_scan:
        # 超时保护，避免在频道非常多时阻塞太久
        if time.time() - start_ts > CONFIG.get("SCAM_CHANNEL_SEARCH_TIMEOUT", 40):
            print("⏱ [Scam-Scan] 搜索超时，提前结束扫描。")
            break

        try:
            channel_entity = await client.get_entity(channel_ref)
        except (ValueError, PeerIdInvalidError, ChannelPrivateError) as e:
            print(f"⚠️ [Scam-Scan] 无法访问频道 '{channel_ref}': {type(e).__name__}。跳过...")
            continue
        except ChatAdminRequiredError:
            print(f"ℹ️ [Scam-Scan] 频道 '{channel_ref}' 需要管理员权限进行搜索。跳过...")
            continue
        except Exception as e:
            print(f"💥 [Scam-Scan] 获取频道 '{channel_ref}' 信息时发生错误: {type(e).__name__} - {e}")
            continue

        channel_title = getattr(channel_entity, "title", str(channel_ref))
        channel_has_hit = False

        # --- 1) 优先使用服务器端 search（效率更高） ---
        for query in search_queries:
            try:
                async for message in client.iter_messages(
                    channel_entity,
                    limit=CONFIG.get("SCAM_CHANNEL_SEARCH_LIMIT", 5),
                    search=query,
                ):
                    await build_hit(channel_entity, message)
                    channel_has_hit = True
            except ChatAdminRequiredError:
                print(f"ℹ️ [Scam-Scan] 频道 '{channel_ref}' 不允许机器人使用 search。改用本地扫描。")
                break  # 跳出 search 循环，执行本地扫描
            except Exception as e:
                print(f"⚠️ [Scam-Scan] 使用 search 在频道 '{channel_ref}' 搜索时出错: {type(e).__name__} - {e}")
                break

        # --- 2) 如果服务器端 search 没命中，再做一次本地扫描兜底 ---
        if not channel_has_hit:
            try:
                async for message in client.iter_messages(channel_entity, limit=scan_depth):
                    if not (message.message or message.text):
                        continue
                    text_lower = (message.message or message.text or "").lower()

                    hit = False
                    # 先匹配所有关键词
                    for q in search_queries:
                        q_norm = q.lower().lstrip("@")
                        if q_norm and q_norm in text_lower:
                            hit = True
                            break

                    # 再额外兜底一次纯数字 ID（包含在 tg://user?id=XXXX 这类链接里）
                    if not hit and user_id and str(user_id) in text_lower:
                        hit = True

                    if hit:
                        await build_hit(channel_entity, message)
                        channel_has_hit = True
            except Exception as e:
                print(f"⚠️ [Scam-Scan] 本地扫描频道 '{channel_ref}' 时出错: {type(e).__name__} - {e}")

        await asyncio.sleep(0.5)

    print(f"✅ [Scam-Scan] 完成搜索，为 {log_target} 找到 {len(found_messages)} 条风险提及。")
    return found_messages



# ---------------------- Telethon 事件监听 (数据抓取引擎) ----------------------
@client.on(events.NewMessage(incoming=True, func=lambda e: (e.is_group or e.is_channel) and not e.is_private))
async def historical_message_handler(event):
    if not is_crawler_enabled():
        return
    try:
        message = event.message
        if not message or not message.sender:
            return

        if isinstance(message.sender, User) and not message.sender.bot:
            await save_message_to_db_async(message)

        if message.text:
            found_ids = set(re.findall(r'[=/\s:](\d{7,12})(?!\d)', message.text))
            
            if not found_ids:
                return

            with db_lock:
                conn = get_db_connection()
                c = conn.cursor()
                placeholders = ','.join('?' for _ in found_ids)
                c.execute(f"SELECT user_id FROM users WHERE user_id IN ({placeholders})", list(found_ids))
                existing_ids = {str(row['user_id']) for row in c.fetchall()}
                conn.close()

            new_ids_to_check = found_ids - existing_ids

            if new_ids_to_check:
                print(f"ℹ️ [Proactive-Scan] 发现 {len(new_ids_to_check)} 个新用户ID: {new_ids_to_check}")
                for user_id_str in new_ids_to_check:
                    try:
                        user_id_int = int(user_id_str)
                        await asyncio.sleep(0.5)
                        entity = await client.get_entity(user_id_int)
                        if isinstance(entity, User) and not entity.bot:
                            print(f"✅ [Proactive-Sync] 主动同步被提及的新用户 {user_id_int}")
                            await update_user_in_db(entity)
                    except (ValueError, TypeError, PeerIdInvalidError):
                        pass
                    except Exception as e:
                        print(f"⚠️ [Proactive-Sync] 检查新ID {user_id_str} 时出错: {e}")

    except Exception as e:
        print(f"💥 [historical_message_handler] 发生严重错误: {e}")
        traceback.print_exc()

@client.on(events.UserUpdate)
async def user_update_handler(event):
    if not is_crawler_enabled():
        return
    if not event.user_id: return
    try:
        user_to_update = await client.get_entity(event.user_id)
        if user_to_update and isinstance(user_to_update, User) and not user_to_update.bot:
            await update_user_in_db(user_to_update)
    except (ValueError, PeerIdInvalidError):
        return
    except Exception as e:
        print(f"💥 [User Update Error] 处理用户更新失败 (ID: {event.user_id}): {e}")
        traceback.print_exc()

async def join_target_channels():
    print("🤝 正在尝试加入配置文件中的监控频道...")
    channels_to_join = load_channels()
    for channel in channels_to_join:
        try:
            await client(JoinChannelRequest(channel))
            print(f"✅ 成功加入: {channel}")
            await asyncio.sleep(5)
        except UserAlreadyParticipantError:
            print(f"ℹ️ 已在 '{channel}' 中，无需重复加入。")
        except (ValueError, PeerIdInvalidError):
            print(f"⚠️ 无法加入 '{channel}': 频道/群组不存在或链接无效。")
        except (UserNotParticipantError, ChannelPrivateError):
            print(f"ℹ️ 无法访问私有频道 '{channel}' 或非成员。")
        except ChannelsTooMuchError:
            print("❌ 无法加入更多频道：已达到 Telegram 账户的频道/群组上限。")
            break
        except FloodWaitError as e:
            print(f"⏳ 加入频道遭遇 FloodWait，将等待 {e.seconds} 秒...")
            await asyncio.sleep(e.seconds)
        except Exception as e:
            print(f"💥 加入频道 '{channel}' 时发生未知错误: {type(e).__name__} - {e}")
    print("✅ 频道加入流程完成。")


async def _start_telethon_async():
    global target_channels
    print("🚀 正在启动 Telethon 客户端...")
    
    try:
        await client.start(bot_token=CONFIG["BOT_TOKEN"] if not CONFIG.get("api_hash") else None)
    except FloodWaitError as e:
        print(f"❌ Telethon启动时遭遇FloodWait: {e.seconds}s. 请稍后重试。")
        return
    except (ApiIdInvalidError, AuthKeyDuplicatedError) as e:
        print(f"❌ CRITICAL Telethon ERROR: {type(e).__name__}. 请检查配置或删除 .session 文件。")
        return

    print("✅ Telethon 客户端已成功启动。")
    me = await client.get_me()
    print(f"🤖 登录账号: @{me.username} (ID: {me.id})")
    
    target_channels = load_channels()
    await join_target_channels()

    print(f"👂 Telethon 开始在所有已加入的群组和频道中进行实时数据抓取...")
    await client.run_until_disconnected()

def start_telethon():
    global telethon_loop
    loop = asyncio.new_event_loop()
    telethon_loop = loop
    asyncio.set_event_loop(loop)

    while True:
        try:
            print("🚀 [Telethon] 正在启动或尝试重新连接...")
            loop.run_until_complete(_start_telethon_async())

        except (KeyboardInterrupt, asyncio.CancelledError):
            print("🛑 [Telethon] 循环被用户中断。")
            break
        
        except Exception as e:
            print(f"💥 [Telethon] 运行期间发生严重错误: {e}")
            traceback.print_exc()
            if client.is_connected():
                try:
                    loop.run_until_complete(client.disconnect())
                except Exception as disconnect_e:
                    print(f"⚠️ [Telethon] 尝试断开连接时也发生错误: {disconnect_e}")

        print("ℹ️ [Telethon] 客户端已断开。将在 30 秒后尝试重启以确保服务持续...")
        time.sleep(30)

    print("🚪 Telethon 线程已完全停止。")
    if client.is_connected():
        loop.run_until_complete(client.disconnect())
    loop.stop()
    loop.close()
    print("✅ Telethon 资源已释放。")

# ---------------------- Telebot 初始化与成员检查 ----------------------

bot = telebot.TeleBot(CONFIG["BOT_TOKEN"])
try:
    _me = bot.get_me()
    _u = getattr(_me, 'username', None) or ''
    if _u:
        with open('bot_username.txt', 'w', encoding='utf-8') as _f:
            _f.write(_u.strip())
except Exception as _e:
    print(f"⚠️ 获取 bot 用户名失败: {_e}")

bot_name_cache = None

# --- 全局安全包装：自动处理 MarkdownV2 解析错误 ---
_original_send_message = bot.send_message
_original_reply_to = bot.reply_to

def _safe_sanitize_markdown_text(text: str) -> str:
    """移除所有 MarkdownV2 特殊字符，作为兜底纯文本发送。"""
    if not isinstance(text, str):
        text = str(text)
    # 与 escape_markdown 中的字符集合保持一致
    return re.sub(r'[_*\[\]()~`>#+\-=|{}.!\\]', '', text)

def _safe_send_message_wrapper(chat_id, text, *args, **kwargs):
    try:
        return _original_send_message(chat_id, text, *args, **kwargs)
    except ApiTelegramException as e:
        desc = getattr(e, 'description', '') or str(e)
        if "can't parse entities" in desc.lower():
            safe_text = _safe_sanitize_markdown_text(text)
            # 出错时不要再使用 MarkdownV2
            kwargs.pop("parse_mode", None)
            try:
                return _original_send_message(chat_id, safe_text, *args, **kwargs)
            except Exception:
                # 最终兜底：只打印日志，不再抛出，避免轮询线程崩溃
                print(f"⚠️ [_safe_send_message_wrapper] 发送纯文本兜底消息仍失败: {type(e).__name__}: {e}")
                return None
        raise

def _safe_reply_to_wrapper(message, text, *args, **kwargs):
    try:
        return _original_reply_to(message, text, *args, **kwargs)
    except ApiTelegramException as e:
        desc = getattr(e, 'description', '') or str(e)
        if "can't parse entities" in desc.lower():
            safe_text = _safe_sanitize_markdown_text(text)
            kwargs.pop("parse_mode", None)
            try:
                return _original_reply_to(message, safe_text, *args, **kwargs)
            except Exception:
                print(f"⚠️ [_safe_reply_to_wrapper] 回复纯文本兜底消息仍失败: {type(e).__name__}: {e}")
                return None
        raise

bot.send_message = _safe_send_message_wrapper
bot.reply_to = _safe_reply_to_wrapper

def get_bot_name():
    global bot_name_cache
    if bot_name_cache is None:
        try:
            me = bot.get_me()
            bot_name_cache = me.first_name or "BOT"
        except Exception:
            bot_name_cache = "BOT"
    return bot_name_cache


membership_cache = {}
MEMBERSHIP_CACHE_DURATION = 5
user_submission_state = {}
# 用于避免并发提交导致状态竞争 (例如 /done 被重复触发)
submission_state_lock = threading.Lock()
admin_broadcast_state = {}
invite_membership_last_check = 0

def check_membership(func):
    @functools.wraps(func)
    def wrapper(message_or_call, *args, **kwargs):

        # 允许“网页版验证/邀请”参数的 /start 在未入群时继续执行（用于记录 pending 或邀请流程）
        try:
            if isinstance(message_or_call, types.Message):
                _t = (message_or_call.text or "").strip()
                if _t.startswith("/start"):
                    _parts = _t.split(maxsplit=1)
                    if len(_parts) > 1:
                        _arg = (_parts[1] or "").strip()
                        if _arg.startswith("inv_"):
                            is_invite_start = True
                            return func(message_or_call, *args, **kwargs)
                        if _arg.startswith("webv_") or _arg.startswith("webverify_"):
                            return func(message_or_call, *args, **kwargs)
        except Exception:
            pass

        user = message_or_call.from_user
        
        if user.id in user_settings_state:
            del user_settings_state[user.id]
        if user.id in user_sponsorship_state: # 新增：清理赞助状态
            del user_sponsorship_state[user.id]

        if not user.is_bot:
            if telethon_loop and telethon_loop.is_running():
                telethon_user = User(
                    id=user.id, first_name=user.first_name, last_name=user.last_name,
                    username=user.username, bot=user.is_bot, access_hash=0,
                )
                asyncio.run_coroutine_threadsafe(update_user_in_db(telethon_user), telethon_loop)
            
            with db_lock:
                conn = get_db_connection()
                try:
                    c = conn.cursor()
                    now = int(time.time())
                    c.execute('''
                        INSERT INTO bot_interactors (user_id, last_interaction_date) VALUES (?, ?)
                        ON CONFLICT(user_id) DO UPDATE SET last_interaction_date = ?
                    ''', (user.id, now, now))
                    conn.commit()
                finally:
                    conn.close()

        if user.id == CONFIG["ADMIN_ID"]:
            return func(message_or_call, *args, **kwargs)

        required_channel = CONFIG.get("REQUIRED_CHANNEL")
        if not required_channel or not required_channel.startswith('@'):
            return func(message_or_call, *args, **kwargs)

        now = time.time()
        
        cache_entry = membership_cache.get(user.id)
        if cache_entry and (now - cache_entry[0] < MEMBERSHIP_CACHE_DURATION):
            if cache_entry[1]:
                if user_can_use_bot(user.id):
                    return func(message_or_call, *args, **kwargs)
                gate_text, _required = get_invite_gate_message(user.id)
                gate_text = gate_text + invite_link_text(user.id)
                markup = types.InlineKeyboardMarkup(row_width=1)
                markup.add(types.InlineKeyboardButton("📢 官方频道", url=f"https://t.me/{required_channel.lstrip('@')}"))
                try:
                    chat_id = message_or_call.message.chat.id if isinstance(message_or_call, types.CallbackQuery) else message_or_call.chat.id
                    bot.send_message(chat_id, gate_text, reply_markup=markup, disable_web_page_preview=True, parse_mode="MarkdownV2")
                    if isinstance(message_or_call, types.CallbackQuery):
                        bot.answer_callback_query(message_or_call.id)
                except Exception as e:
                    print(f"💥 发送邀请门槛提示失败(缓存路径): {e}")
                return
        
        is_member = False
        try:
            chat_member = bot.get_chat_member(required_channel, user.id)
            is_member = chat_member.status in ['member', 'administrator', 'creator']
            membership_cache[user.id] = (now, is_member)
        except ApiTelegramException as e:
            if e.result_json and 'description' in e.result_json and 'user not found' in e.result_json['description']:
                is_member = False
            else:
                print(f"⚠️ 检查成员资格失败 (User: {user.id}): {e.description}")
            membership_cache[user.id] = (now, is_member)
        
        if is_member:
            if user_can_use_bot(user.id):
                return func(message_or_call, *args, **kwargs)

            gate_text, _required = get_invite_gate_message(user.id)
            gate_text = gate_text + invite_link_text(user.id)
            markup = types.InlineKeyboardMarkup(row_width=1)
            markup.add(types.InlineKeyboardButton("📢 官方频道", url=f"https://t.me/{required_channel.lstrip('@')}"))
            try:
                chat_id = message_or_call.message.chat.id if isinstance(message_or_call, types.CallbackQuery) else message_or_call.chat.id
                bot.send_message(chat_id, gate_text, reply_markup=markup, disable_web_page_preview=True, parse_mode="MarkdownV2")
                if isinstance(message_or_call, types.CallbackQuery):
                    bot.answer_callback_query(message_or_call.id)
            except Exception as e:
                print(f"💥 发送邀请门槛提示失败: {e}")
            return
        else:
            join_text = (
                f"🚫 *{escape_markdown('访问受限')}*\n\n"
                f"{escape_markdown('请先加入我们的官方频道才能使用此功能：')}\n"
                f"➡️ {escape_markdown(required_channel)}\n\n"
                f"{escape_markdown('感谢您的支持！')}"
            )
            markup = types.InlineKeyboardMarkup()
            markup.add(types.InlineKeyboardButton("➡️ 点击加入", url=f"https://t.me/{required_channel.lstrip('@')}"))
            try:
                chat_id = message_or_call.message.chat.id if isinstance(message_or_call, types.CallbackQuery) else message_or_call.chat.id
                bot.send_message(chat_id, join_text, reply_markup=markup, disable_web_page_preview=True, parse_mode="MarkdownV2")
                if isinstance(message_or_call, types.CallbackQuery):
                    bot.answer_callback_query(message_or_call.id)
            except Exception as e:
                print(f"💥 发送加群提示失败: {e}")
    return wrapper

def premium_only(func):
    @functools.wraps(func)
    def wrapper(message_or_call, *args, **kwargs):
        user = message_or_call.from_user
        if not getattr(user, 'is_premium', False):
            try:
                chat_id = message_or_call.message.chat.id if isinstance(message_or_call, types.CallbackQuery) else message_or_call.chat.id
                bot.send_message(chat_id, f"💎 *{escape_markdown('高级功能专属')}*\n\n{escape_markdown('抱歉，此功能仅向尊贵的 Telegram Premium 大会员用户开放。')}", parse_mode="MarkdownV2")
                if isinstance(message_or_call, types.CallbackQuery):
                    bot.answer_callback_query(message_or_call.id)
            except Exception as e:
                print(f"💥 发送 Premium 提示失败: {e}")
            return
        return func(message_or_call, *args, **kwargs)
    return wrapper



def is_user_in_required_channel(user_id: int) -> bool:
    """检查用户是否加入强制频道（支持 channel / supergroup）。"""
    try:
        member = bot.get_chat_member(WEB_REQUIRED_CHANNEL, user_id)
        status = getattr(member, "status", None)
        return status in ("member", "administrator", "creator")
    except Exception:
        return False


def get_required_channel_membership_status(user_id: int):
    """Return True/False if known, or None if API/network error."""
    try:
        member = bot.get_chat_member(WEB_REQUIRED_CHANNEL, int(user_id))
        status = getattr(member, "status", None)
        return status in ("member", "administrator", "creator")
    except ApiTelegramException as e:
        try:
            desc = getattr(e, 'description', '') or str(e)
            print(f"⚠️ 检查成员资格失败 (User: {user_id}): {desc}")
        except Exception:
            pass
        return None
    except Exception:
        return None


def process_invite_start(message: types.Message, start_arg: str) -> bool:
    if not start_arg.startswith('inv_'):
        return False

    invited_user_id = int(message.from_user.id)
    payload = start_arg.split('_', 1)[1].strip() if '_' in start_arg else ''
    if not payload.isdigit():
        bot.reply_to(message, f"⚠️ *{escape_markdown('邀请参数无效，请使用正确邀请链接。')}*", parse_mode='MarkdownV2')
        return True

    inviter_id = int(payload)
    if inviter_id == invited_user_id:
        try:
            bot.send_message(inviter_id, f"⚠️ *{escape_markdown('邀请失败：对方使用了自己的邀请链接，不能计入。')}*", parse_mode='MarkdownV2')
        except Exception:
            pass
        bot.reply_to(message, f"⚠️ *{escape_markdown('该邀请链接当前不可用。')}*", parse_mode='MarkdownV2')
        return True

    if invited_user_id == int(CONFIG.get("ADMIN_ID", 0)):
        try:
            bot.send_message(inviter_id, f"⚠️ *{escape_markdown('邀请失败：该账号为管理员账号，不参与邀请统计。')}*", parse_mode='MarkdownV2')
        except Exception:
            pass
        bot.reply_to(message, f"⚠️ *{escape_markdown('该邀请链接当前不可用。')}*", parse_mode='MarkdownV2')
        return True

    required_channel = CONFIG.get("REQUIRED_CHANNEL") or WEB_REQUIRED_CHANNEL

    # 如果被邀请用户在点链接时就已经在频道里了，按“老用户”处理（防刷）
    # 但管理员可通过 /resetnew <用户ID> 临时放行一次老用户校验
    if is_user_in_required_channel(invited_user_id) and not is_user_reset_as_new(invited_user_id):
        try:
            bot.send_message(
                inviter_id,
                f"⚠️ *{escape_markdown('邀请失败：对方已在频道内，属于老用户，不能计入邀请人数。')}*",
                parse_mode='MarkdownV2'
            )
        except Exception:
            pass
        bot.reply_to(message, f"⚠️ *{escape_markdown('该邀请链接当前不可用。')}*", parse_mode='MarkdownV2')
        return True

    try:
        with db_lock:
            conn = get_db_connection()
            c = conn.cursor()

            # 防止邀请“已与机器人互动过”的用户
            c.execute("SELECT 1 FROM bot_interactors WHERE user_id = ?", (invited_user_id,))
            existed_before = c.fetchone() is not None
            if existed_before:
                conn.close()
                try:
                    bot.send_message(
                        inviter_id,
                        f"⚠️ *{escape_markdown('邀请失败：对方已是老用户，不能计入邀请。请邀请新用户。')}*",
                        parse_mode='MarkdownV2'
                    )
                except Exception:
                    pass
                bot.reply_to(message, f"⚠️ *{escape_markdown('该邀请链接当前不可用。')}*", parse_mode='MarkdownV2')
                return True

            # 防止同一个新用户被重复计入/反复刷
            c.execute(
                "SELECT status FROM user_invites WHERE invited_user_id = ? LIMIT 1",
                (invited_user_id,),
            )
            prev = c.fetchone()
            if prev:
                conn.close()
                try:
                    bot.send_message(
                        inviter_id,
                        f"⚠️ *{escape_markdown('邀请失败：该用户已参与过邀请流程，不能重复计入。')}*",
                        parse_mode='MarkdownV2'
                    )
                except Exception:
                    pass
                bot.reply_to(message, f"⚠️ *{escape_markdown('该邀请链接当前不可用。')}*", parse_mode='MarkdownV2')
                return True

            # 防刷：限制每个邀请人的待审核邀请数量
            c.execute(
                "SELECT COUNT(1) FROM user_invites WHERE inviter_id = ? AND status = 'pending'",
                (inviter_id,),
            )
            pending_count = int((c.fetchone() or [0])[0] or 0)
            if pending_count >= 50:
                conn.close()
                try:
                    bot.send_message(
                        inviter_id,
                        f"⚠️ *{escape_markdown('邀请失败：你的待审核邀请过多，请等待部分用户完成入群后再继续邀请。')}*",
                        parse_mode='MarkdownV2'
                    )
                except Exception:
                    pass
                bot.reply_to(message, f"⚠️ *{escape_markdown('该邀请链接当前不可用。')}*", parse_mode='MarkdownV2')
                return True

            now_ts = int(time.time())
            c.execute(
                "INSERT OR REPLACE INTO user_invites (inviter_id, invited_user_id, status, invited_at, left_at) VALUES (?, ?, 'pending', ?, NULL)",
                (inviter_id, invited_user_id, now_ts),
            )
            conn.commit()
            conn.close()

        # resetnew 临时放行为一次性：成功进入 pending 后立即回收
        if is_user_reset_as_new(invited_user_id):
            mark_user_reset_as_new(invited_user_id, False)
    except Exception as e:
        print(f"⚠️ 处理邀请入库失败: {e}")
        try:
            bot.send_message(inviter_id, f"❌ *{escape_markdown('邀请失败：系统记录邀请时出错，请稍后重试。')}*", parse_mode='MarkdownV2')
        except Exception:
            pass
        bot.reply_to(message, f"⚠️ *{escape_markdown('该邀请链接当前不可用。')}*", parse_mode='MarkdownV2')
        return True

    wait_text = (
        f"🚫 *{escape_markdown('还差一步')}*\n\n"
        f"{escape_markdown('你已通过邀请链接进入。接下来请加入官方频道，加入后会自动计入邀请成功。')}\n"
        f"➡️ {escape_markdown(required_channel)}\n\n"
        f"{escape_markdown('注：加入后无需再次 /start。')}"
    )
    join_markup = types.InlineKeyboardMarkup()
    join_markup.add(types.InlineKeyboardButton("加入频道", url=f"https://t.me/{required_channel.lstrip('@')}"))
    bot.reply_to(message, wait_text, reply_markup=join_markup, disable_web_page_preview=True, parse_mode='MarkdownV2')

    try:
        bot.send_message(inviter_id, f"🕒 *{escape_markdown('邀请已记录')}*\n{escape_markdown('新用户已打开邀请链接，等待其加入频道后计入成功。')}", parse_mode='MarkdownV2')
    except Exception:
        pass

    return True


# ---------------------- Bot 命令处理 (UI 美化) ----------------------
@bot.message_handler(commands=['invite'])
@check_membership
def handle_invite(message: types.Message):
    uid = int(message.from_user.id)
    active, total = get_user_invite_stats(uid)
    required = get_invite_required_count()
    text = (
        f"📨 *{escape_markdown('邀请进度')}*\n\n"
        f"*{escape_markdown('当前有效邀请:')}* {escape_markdown(str(active))}/{escape_markdown(str(required))}\n"
        f"{escape_markdown('历史累计:')} {escape_markdown(str(total))}\n\n"
        f"{escape_markdown('邀请规则：新用户必须先通过你的链接进入机器人，再加入官方频道，才算成功。')}"
    )
    text = text + invite_link_text(uid)
    markup = types.InlineKeyboardMarkup(row_width=1)
    required_channel = CONFIG.get("REQUIRED_CHANNEL")
    if required_channel and required_channel.startswith('@'):
        markup.add(types.InlineKeyboardButton("📢 官方频道", url=f"https://t.me/{required_channel.lstrip('@')}"))
    bot.reply_to(message, text, reply_markup=markup, disable_web_page_preview=True, parse_mode="MarkdownV2")


@bot.message_handler(commands=['start'])
@check_membership
def handle_start(message, is_edit=False):
    update_active_user(message.from_user.id)
    
    command_parts = message.text.split(maxsplit=1)
    # 邀请流程：/start inv_<inviter_id>
    if len(command_parts) > 1:
        _arg = (command_parts[1] or '').strip()
        if _arg.startswith('inv_'):
            if process_invite_start(message, _arg):
                return

    # 网页版验证：网页端生成 state 后，引导用户打开机器人 /start webv_<state> 进行验证
    if len(command_parts) > 1:
        _arg = (command_parts[1] or '').strip()
        if _arg.startswith('webv_') or _arg.startswith('webverify_'):
            _state = _arg.split('_', 1)[1].strip() if '_' in _arg else ''
            if not re.fullmatch(r"[0-9a-fA-F]{16,64}", _state):
                bot.reply_to(message, f"⚠️ *{escape_markdown('验证参数无效，请回到网页重新验证。')}*", parse_mode='MarkdownV2')
                return
            web_base = os.environ.get('WEB_BASE_URL', '').strip().rstrip('/')
            if not web_base:
                web_base = f"http://{CONFIG.get('SERVER_PUBLIC_IP','127.0.0.1')}:{os.environ.get('WEB_PORT','1319')}"
            now_ts = int(time.time())
            exp_ts = now_ts + 15 * 60
            in_required = False
            try:
                in_required = is_user_in_required_channel(message.from_user.id)
            except Exception:
                in_required = False
            try:
                with db_lock:
                    conn = get_db_connection()
                    c = conn.cursor()
                    c.execute(
                        "INSERT OR IGNORE INTO web_verify_states (state, tg_user_id, next_path, created, expires, used, approved) VALUES (?, ?, NULL, ?, ?, 0, ?)",
                        (_state, message.from_user.id, now_ts, exp_ts, 1 if in_required else 0),
                    )
                    c.execute(
                        "UPDATE web_verify_states SET tg_user_id = ?, expires = ?, used = 0, approved = ? WHERE state = ?",
                        (message.from_user.id, exp_ts, 1 if in_required else 0, _state),
                    )
                    if not in_required:
                        c.execute(
                            "INSERT OR REPLACE INTO web_verify_pending (state, tg_user_id, created, expires, notified) VALUES (?, ?, ?, ?, 0)",
                            (_state, message.from_user.id, now_ts, exp_ts),
                        )
                    else:
                        c.execute("DELETE FROM web_verify_pending WHERE state = ?", (_state,))
                    conn.commit()
                    conn.close()
            except Exception as _e:
                print(f"⚠️ 写入网页验证状态失败: {_e}")
            if in_required:
                cb_url = f"{web_base}/verify/callback?state={_state}"
                markup = types.InlineKeyboardMarkup()
                markup.add(types.InlineKeyboardButton("✅ 返回网页继续", url=cb_url))
                ok_text = (
                    f"✅ *{escape_markdown('验证通过')}*\n\n"
                    f"{escape_markdown('请点击下方按钮返回网页继续使用。')}"
                )
                bot.reply_to(message, ok_text, reply_markup=markup, disable_web_page_preview=True, parse_mode='MarkdownV2')
            else:
                wait_text = (
                    f"🚫 *{escape_markdown('访问受限')}*\n\n"
                    f"{escape_markdown('请先加入我们的官方频道才能使用此功能：')}\n"
                    f"➡️ @{escape_markdown(str(WEB_REQUIRED_CHANNEL).lstrip('@'))}\n\n"
                    f"{escape_markdown('你加入后将自动通过验证，无需再次 /start。')}"
                )
                join_markup = types.InlineKeyboardMarkup()
                join_markup.add(types.InlineKeyboardButton("加入频道", url=WEB_REQUIRED_CHANNEL_URL))
                bot.reply_to(message, wait_text, reply_markup=join_markup, disable_web_page_preview=True, parse_mode='MarkdownV2')
            return
    if len(command_parts) > 1 and command_parts[1].startswith('bizChat'):
        payload = command_parts[1]
        target_id_str = payload.replace('bizChat', '').strip()
        
        with db_lock:
            conn = get_db_connection()
            c = conn.cursor()
            c.execute("SELECT 1 FROM business_connections WHERE user_id = ? AND is_enabled = 1", (message.from_user.id,))
            is_business_user = c.fetchone()
            conn.close()

        if is_business_user and target_id_str.isdigit():
            print(f"✅ [Business Query] Triggered by user {message.from_user.id} for contact {target_id_str} via deep link.")
            try:
                bot.send_message(message.chat.id, f"ℹ️ *{escape_markdown('正在为您自动查询联系人...')}*\n*{escape_markdown('目标ID:')}* `{target_id_str}`", parse_mode="MarkdownV2")
            except Exception as e:
                print(f"⚠️ [Business Query] Failed to send pre-query notification: {e}")
            
            trigger_query_flow(
                message=message,
                query=target_id_str
            )
            return

    hitokoto_quote = get_hitokoto()
    
    welcome_text = [
        f"🛡️ *猎诈卫士* `{escape_for_code(BOT_VERSION.split('|')[0].strip())}`",
        f"你好，{escape_markdown(message.from_user.first_name)}\\! 我是您的电报安全助手。",
    ]
    
    if hitokoto_quote:
        welcome_text.append(f"\n*{escape_markdown('每日一言')}*\n{hitokoto_quote}")

    welcome_text.extend([
        f"\n*{escape_markdown('——— 功能导航 ———')}*",
        f"{escape_markdown('您可以直接使用下方按钮，或发送相应命令：')}",
        f"`/tougao` {escape_markdown('• 投稿诈骗者信息')}",
        f"`/sponsor` {escape_markdown('• 赞助支持我们')}",
        f"`/leaderboard` {escape_markdown('• 查看赞助排行')}",
        f"_/Tip: 直接转发用户消息、发送其用户名或ID，即可快速查询\\./_",
    ])
    
    markup = types.InlineKeyboardMarkup(row_width=3) # 改为3列
    markup.add(
        types.InlineKeyboardButton("🔍 查询记录", callback_data="query"),
        types.InlineKeyboardButton("✍️ 投稿骗子", callback_data="tougao"),
        types.InlineKeyboardButton("❤️ 赞助我们", callback_data="sponsor"),
        types.InlineKeyboardButton("💎 高级功能", callback_data="premium:main"),
        types.InlineKeyboardButton("📊 运行状态", callback_data="stats"),
        types.InlineKeyboardButton("🏆 赞助排行", callback_data="leaderboard")
    )
    final_text = "\n".join(welcome_text) + f"\n\n{ADVERTISEMENT_TEXT}"
    
    if is_edit:
        try:
            bot.edit_message_text(final_text, message.chat.id, message.message_id, reply_markup=markup, disable_web_page_preview=True, parse_mode="MarkdownV2")
        except ApiTelegramException:
            pass
    else:
        bot.reply_to(message, final_text, reply_markup=markup, disable_web_page_preview=True, parse_mode="MarkdownV2")

# ---------------------- 新增：赞助与排行榜功能 ----------------------
@bot.message_handler(commands=['sponsor'])
@check_membership
def handle_sponsor(message):
    user_id = message.from_user.id
    user_sponsorship_state[user_id] = True # 标记用户进入赞助流程
    prompt_text = (
        f"❤️ *{escape_markdown('赞助支持')}*\n\n"
        f"{escape_markdown('感谢您对本项目的关注与支持！您的每一份赞助都将用于服务器维护和功能开发，帮助我们为更多人提供服务。只可以okpay钱包支付')}\n\n"
        f"*{escape_markdown('这个是okpay钱包，请输入您希望赞助的金额 (USDT)，例如: 0.01')}*"
    )
    bot.reply_to(message, prompt_text, parse_mode="MarkdownV2")
    bot.register_next_step_handler(message, process_sponsor_amount)

def process_sponsor_amount(message):
    user_id = message.from_user.id
    if user_id not in user_sponsorship_state:
        # 如果用户未处于赞助流程，则正常处理消息
        handle_all_other_messages(message)
        return
    
    del user_sponsorship_state[user_id] # 清理状态
    
    try:
        clean_text = re.sub(r'[^\d.]', '', message.text)
        amount = float(clean_text)
        if amount <= 0.0:
            raise ValueError("金额必须为正数")
        
        bot.reply_to(message, escape_markdown("⏳ 正在为您创建支付订单，请稍候..."), parse_mode="MarkdownV2")
        
        response = okpay_client.pay_link(amount)
        
        if not response or 'data' not in response or not response['data']:
            error_msg = response.get('error') or response.get('msg', '未知错误') if isinstance(response, dict) else '无响应'
            bot.send_message(user_id, f"❌ 创建订单失败: {escape_markdown(str(error_msg))}", parse_mode="MarkdownV2")
            logger.error(f"为用户 {user_id} 创建订单失败: {response}")
            return
        
        order_id = response['data']['order_id']
        pay_url = response['data']['pay_url']

        # 将订单信息存入数据库
        with db_lock:
            conn = get_db_connection()
            c = conn.cursor()
            c.execute(
                "INSERT INTO okpay_orders (order_id, user_id, amount, status, timestamp) VALUES (?, ?, ?, ?, ?)",
                (order_id, user_id, amount, 'pending', int(time.time()))
            )
            conn.commit()
            conn.close()

        markup = types.InlineKeyboardMarkup()
        markup.add(types.InlineKeyboardButton("🔗 点击支付 (USDT)", url=pay_url))

        bot.send_message(
            user_id,
            f"🛒 *{escape_markdown('订单创建成功!')}*\n\n"
            f"*{escape_markdown('订单号:')}* `{escape_for_code(order_id)}`\n"
            f"*{escape_markdown('金额:')}* `{amount:.2f} USDT`\n"
            f"*{escape_markdown('有效期:')}* {escape_markdown('10分钟')}\n\n"
            f"{escape_markdown('请点击下方按钮，在OKPay页面中完成支付:')}",
            parse_mode='MarkdownV2',
            reply_markup=markup
        )
        logger.info(f"用户 {user_id} 订单创建成功，订单号: {order_id}")

    except (ValueError, TypeError):
        bot.reply_to(message, f"⚠️ *{escape_markdown('金额无效')}*\\n{escape_markdown('请输入一个有效的数字 (例如: 10 或 10.5)。')}", parse_mode="MarkdownV2")
    except Exception as e:
        logger.exception(f"处理赞助金额时发生错误: {e}")
        bot.reply_to(message, f"❌ {escape_markdown('处理请求时发生错误，请稍后重试。')}", parse_mode="MarkdownV2")


@bot.message_handler(commands=['leaderboard'])
@check_membership
def handle_leaderboard(message):
    with db_lock:
        conn = get_db_connection()
        c = conn.cursor()
        c.execute("""
            SELECT s.user_id, s.total_amount_usdt, u.first_name, u.last_name
            FROM sponsors s
            LEFT JOIN users u ON s.user_id = u.user_id
            ORDER BY s.total_amount_usdt DESC
            LIMIT 10
        """)
        top_sponsors = c.fetchall()
        conn.close()

    if not top_sponsors:
        text = f"🏆 *{escape_markdown('赞助排行榜')}*\n\n{escape_markdown('目前还没有赞助记录，期待您的支持！')}\n\n{ADVERTISEMENT_TEXT}"
        bot.reply_to(message, text, parse_mode="MarkdownV2", disable_web_page_preview=True)
        return

    leaderboard_parts = [f"🏆 *{escape_markdown('赞助排行榜 Top 10')}*"]
    medals = ["🥇", "🥈", "🥉"]
    
    for i, sponsor in enumerate(top_sponsors):
        user_name = f"{sponsor['first_name'] or ''} {sponsor['last_name'] or ''}".strip()
        if not user_name:
            user_name = f"用户 {sponsor['user_id']}"
        
        rank_icon = medals[i] if i < 3 else f"*{i + 1}\\.*"
        
        line = (f"{rank_icon} [{escape_markdown(truncate_for_link_text(user_name))}](tg://user?id={sponsor['user_id']}) "
                f"\\- `{sponsor['total_amount_usdt']:.2f} USDT`")
        leaderboard_parts.append(line)
        
    final_text = "\n\n".join(leaderboard_parts) + f"\n\n{ADVERTISEMENT_TEXT}"
    bot.reply_to(message, final_text, parse_mode="MarkdownV2", disable_web_page_preview=True)
# ---------------------- 新增结束 ----------------------


@bot.message_handler(commands=['premium_features'])
@check_membership
@premium_only
def handle_premium_info_command(message):
    handle_premium_main_menu(message)

def handle_premium_main_menu(message_or_call):
    is_call = isinstance(message_or_call, types.CallbackQuery)
    message = message_or_call.message if is_call else message_or_call
    chat_id = message.chat.id
    message_id = message.message_id

    try:
        bot_username = bot.get_me().username
    except Exception:
        bot_username = "本机器人"

    info_text = (
        f"💎 *{escape_markdown('高级功能 · 大会员专属')}*\n"
        f"{escape_markdown('感谢您的支持！以下是为您开放的专属功能。所有功能均需连接 Telegram Business 方可使用。')}\n\n"
        f"1️⃣ *{escape_markdown('自动私聊风险检测')}*\n"
        f"_{escape_markdown('我将自动在私聊中检测联系人风险，并用您的身份发送警告。')}_\n"
        f"*› {escape_markdown('启用:')}* {escape_markdown(f'前往`设置` > `Telegram Business` > `聊天机器人`，添加 `@{bot_username}`。')}\n\n"
        f"2️⃣ *{escape_markdown('互动式记账本')}*\n"
        f"_{escape_markdown('一个基于您个人指令的快速记账工具。在与他人的私聊中使用命令即可记账。')}_\n"
        f"*› {escape_markdown('帮助:')}* {escape_markdown('/jz')}\n\n"
        f"3️⃣ *{escape_markdown('关键词自动回复')}*\n"
        f"_{escape_markdown('当私聊消息包含您设置的关键词时，自动发送预设回复。')}_\n\n"
        f"4️⃣ *{escape_markdown('离线自动回复')}*\n"
        f"_{escape_markdown('当您不在线时，自动回复收到的第一条消息，避免怠慢。')}_\n\n"
        f"👇 *{escape_markdown('选择下方按钮管理您的功能:')}*"
    )
    
    markup = types.InlineKeyboardMarkup(row_width=2)
    markup.add(
        types.InlineKeyboardButton("📒 管理账本", callback_data="premium:ledger"),
        types.InlineKeyboardButton("📝 关键词回复", callback_data="premium:keyword_reply")
    )
    markup.add(
        types.InlineKeyboardButton("🔍 账本分析", callback_data="premium:analyze"),
        types.InlineKeyboardButton("📊 账本统计", callback_data="premium:stats")
    )
    markup.add(
        types.InlineKeyboardButton("🌙 离线回复", callback_data="premium:offline_reply"),
        types.InlineKeyboardButton("🔙 返回主菜单", callback_data="start_menu")
    )
    
    if is_call:
        try:
            bot.edit_message_text(info_text, chat_id, message_id, reply_markup=markup, parse_mode="MarkdownV2")
        except ApiTelegramException:
            bot.answer_callback_query(message_or_call.id)
    else:
        bot.reply_to(message_or_call, info_text, reply_markup=markup, parse_mode="MarkdownV2")


@bot.message_handler(commands=['stats'])
@check_membership
def handle_stats(message):
    update_active_user(message.from_user.id)
    online_count = get_online_user_count()
    
    with db_lock:
        conn = get_db_connection()
        c = conn.cursor()
        c.execute("SELECT COUNT(*) FROM users")
        total_users = c.fetchone()[0]
        c.execute("SELECT COUNT(*) FROM bot_interactors")
        interacted_users = c.fetchone()[0]
        c.execute("SELECT COUNT(*) FROM message_history")
        total_messages = c.fetchone()[0]
        c.execute("SELECT COUNT(id) FROM username_history")
        total_username_changes = c.fetchone()[0]
        c.execute("SELECT COUNT(id) FROM name_history")
        total_name_changes = c.fetchone()[0]
        conn.close()

    reports = load_reports()
    verified_count = len(reports.get('verified', {}))
    channel_count = len(load_channels())
    backend_status = '✅ 在线' if telethon_loop and client.is_connected() else '❌ 离线'
    
    stats_text = (
        f"📊 *{escape_markdown('机器人状态概览')}*\n"
        f"*{'─' * 20}*\n"
        f"🟢 *{escape_markdown('在线用户:')}* `{online_count}` {escape_markdown('人')}\n"
        f"📡 *{escape_markdown('可达用户:')}* `{interacted_users}` {escape_markdown('人')}\n"
        f"👥 *{escape_markdown('总收录用户:')}* `{total_users}`\n"
        f"✉️ *{escape_markdown('总记录消息:')}* `{total_messages}`\n"
        f"🔄 *{escape_markdown('身份变更:')}* `{total_username_changes + total_name_changes}` {escape_markdown('次')}\n"
        f"📝 *{escape_markdown('已验证投稿:')}* `{verified_count}` {escape_markdown('条')}\n"
        f"📺 *{escape_markdown('监控频道数:')}* `{channel_count}` {escape_markdown('个')}\n"
        f"⚙️ *{escape_markdown('后台引擎:')}* {escape_markdown(backend_status)}\n"
        f"📡 *{escape_markdown('数据抓取:')}* {escape_markdown('开启' if is_crawler_enabled() else '关闭')}\n"
        f"`{escape_for_code(BOT_VERSION)}`\n"
        f"*{'─' * 20}*\n"
        f"{ADVERTISEMENT_TEXT}"
    )
    bot.reply_to(message, stats_text, disable_web_page_preview=True, parse_mode="MarkdownV2")

@bot.message_handler(commands=['admin', 'addchannel', 'removechannel', 'listchannels', 'delreport', 'broadcast', 'pollbroadcast', 'pollresult', 'cancel_broadcast', 'setinvite', 'invite_status', 'resetnew'])
@check_membership
def handle_admin_commands(message):
    is_admin = message.from_user.id == CONFIG["ADMIN_ID"]
    command_parts = message.text.split()
    command = command_parts[0]
    
    if not is_admin and command in ['/admin', '/addchannel', '/removechannel', '/delreport', '/broadcast', '/pollbroadcast', '/pollresult', '/cancel_broadcast', '/setinvite', '/resetnew']:
        bot.reply_to(message, escape_markdown("🚫 *无权限*"), parse_mode="MarkdownV2")
        return
    if command == '/admin':
        status = "开启✅" if is_chatlog_enabled() else "关闭⛔"
        crawl_status = "开启✅" if is_crawler_enabled() else "关闭⛔"
        admin_text = (
            f"🛠️ *{escape_markdown('管理员控制面板')}*\n"
            f"*{'─' * 20}*\n\n"
            f"*{escape_markdown('频道管理')}*\n"
            f"`/addchannel <@频道/ID>`  {escape_markdown('添加监控')}\n"
            f"`/removechannel <@频道/ID>`  {escape_markdown('移除监控')}\n"
            f"`/listchannels`  {escape_markdown('查看列表')}\n"
            f"_{escape_markdown('注: 添加后机器人会自动尝试加入')}_\n\n"
            f"*{escape_markdown('投稿管理')}*\n"
            f"`/delreport <用户ID/名>`  {escape_markdown('删除已验证投稿')}\n\n"
            f"*{escape_markdown('聊天记录/数据库')}*\n"
            f"*{escape_markdown('功能状态')}:* {escape_markdown(status)}\n"
            f"`/dbsize`  {escape_markdown('数据库大小')}\n"
            f"`/delmsgs <N>`  {escape_markdown('删最旧N条')}\n"
            f"`/delmsgkw <关键词1,关键词2>`  {escape_markdown('按关键词删除')}\n"
            f"`/vacuumdb`  {escape_markdown('压缩数据库')}\n"
            f"`/chatlog_on`  {escape_markdown('开启跳转')}\n"
            f"`/chatlog_off`  {escape_markdown('关闭跳转')}\n\n"
            f"*{escape_markdown('数据抓取')}*\n"
            f"*{escape_markdown('功能状态')}:* {escape_markdown(crawl_status)}\n"
            f"`/crawl_status`  {escape_markdown('查看开关')}\n"
            f"`/crawl_off`  {escape_markdown('暂停抓取')}\n"
            f"`/crawl_on`  {escape_markdown('恢复抓取')}\n\n"
            f"*{escape_markdown('广播功能')}*\n"
            f"`/broadcast`  {escape_markdown('群发消息')}\n"
            f"`/pollbroadcast`  {escape_markdown('群发投票')}\n"
            f"`/pollresult <任务ID>`  {escape_markdown('查看结果')}\n"
            f"`/cancel_broadcast`  {escape_markdown('取消广播')}\n\n"
            f"*{escape_markdown('邀请门槛')}*\n"
            f"`/invite_status`  {escape_markdown('查看门槛')}\n"
            f"`/setinvite <人数>`  {escape_markdown('设置门槛')}\n"
            f"`/resetnew <用户ID>`  {escape_markdown('临时放行一次')}"
        )
        bot.reply_to(message, admin_text, parse_mode="MarkdownV2")

    elif command in ['/addchannel', '/removechannel']:
        if len(command_parts) < 2:
            bot.reply_to(message, escape_markdown("⚠️ 格式错误，请提供频道用户名或ID。"), parse_mode="MarkdownV2")
            return
        
        channel_input = command_parts[1].strip()
        target = None
        if channel_input.startswith('@'):
            target = channel_input
        elif channel_input.isdigit() or (channel_input.startswith('-') and channel_input[1:].isdigit()):
            try: target = int(channel_input)
            except ValueError:
                bot.reply_to(message, escape_markdown("⚠️ ID 格式错误。"), parse_mode="MarkdownV2")
                return
        else:
            bot.reply_to(message, escape_markdown("⚠️ 格式错误，频道应以@开头或为数字ID。"), parse_mode="MarkdownV2")
            return

        current_channels = load_channels()
        if command == '/addchannel':
            if any(str(c).lower() == str(target).lower() for c in current_channels):
                reply_text = f"{escape_markdown('ℹ️ 频道 ')}\`{escape_for_code(str(target))}\`{escape_markdown(' 已存在。')}"
                bot.reply_to(message, reply_text, parse_mode="MarkdownV2")
                return
            current_channels.append(target)
            if save_channels(current_channels):
                reply_text = f"✅ {escape_markdown('成功添加 ')}\`{escape_for_code(str(target))}\`{escape_markdown(' 到监控列表。')}"
                bot.reply_to(message, reply_text, parse_mode="MarkdownV2")
            else:
                bot.reply_to(message, escape_markdown(f"❌ 添加失败，无法写入文件。"), parse_mode="MarkdownV2")
        
        elif command == '/removechannel':
            original_len = len(current_channels)
            new_channels = [c for c in current_channels if str(c).lower() != str(target).lower()]
            if len(new_channels) < original_len:
                if save_channels(new_channels):
                    reply_text = f"✅ {escape_markdown('成功移除 ')}\`{escape_for_code(str(target))}\`{escape_markdown('。')}"
                    bot.reply_to(message, reply_text, parse_mode="MarkdownV2")
                else:
                    bot.reply_to(message, escape_markdown(f"❌ 移除失败，无法写入文件。"), parse_mode="MarkdownV2")
            else:
                reply_text = f"⚠️ {escape_markdown('未在列表中找到 ')}\`{escape_for_code(str(target))}\`{escape_markdown('。')}"
                bot.reply_to(message, reply_text, parse_mode="MarkdownV2")
    
    elif command == '/listchannels':
        current_channels = load_channels()
        if not current_channels:
            response_text = escape_markdown("ℹ️ 当前没有设置任何监控频道。")
        else:
            channels_text = "\n".join([f"📺 `{escape_for_code(str(ch))}`" for ch in current_channels])
            response_text = f"📝 *{escape_markdown('当前监控的频道/群组列表:')}*\n\n{channels_text}"
        bot.reply_to(message, response_text + f"\n\n{ADVERTISEMENT_TEXT}", parse_mode="MarkdownV2")

    elif command == '/delreport':
        if len(command_parts) < 2:
            bot.reply_to(message, escape_markdown("格式错误。用法: `/delreport <用户ID或用户名>`"), parse_mode="MarkdownV2")
            return

        query = command_parts[1].strip().lower().lstrip('@')
        reports = load_reports()
        key_to_delete = None

        if query.isdigit() and query in reports['verified']:
            key_to_delete = query
        else:
            for key, record in reports['verified'].items():
                if record.get('usernames') and query in [u.lower() for u in record.get('usernames', [])]:
                    key_to_delete = key
                    break
        
        if key_to_delete:
            del reports['verified'][key_to_delete]
            save_reports(reports)
            reply_text = f"✅ {escape_markdown('成功删除关于 ')}\`{escape_for_code(query)}\`{escape_markdown(' 的已验证报告。')}"
            bot.reply_to(message, reply_text, parse_mode="MarkdownV2")
        else:
            reply_text = f"⚠️ {escape_markdown('未在已验证报告中找到 ')}\`{escape_for_code(query)}\`{escape_markdown('。')}"
            bot.reply_to(message, reply_text, parse_mode="MarkdownV2")

    elif command == '/broadcast':
        handle_broadcast(message)
    elif command == '/pollbroadcast':
        handle_poll_broadcast(message)
    elif command == '/pollresult':
        if len(command_parts) < 2:
            bot.reply_to(message, escape_markdown('用法：/pollresult <任务ID>'), parse_mode="MarkdownV2")
            return
        summary = _build_poll_summary(command_parts[1].strip())
        if not summary:
            bot.reply_to(message, escape_markdown('未找到该投票任务。'), parse_mode="MarkdownV2")
            return
        bot.reply_to(message, summary, parse_mode="MarkdownV2", disable_web_page_preview=True)
    elif command == '/cancel_broadcast':
        handle_cancel_broadcast(message)
    elif command == '/invite_status':
        required = get_invite_required_count()
        msg = (
            f"📌 *{escape_markdown('邀请门槛设置')}*\n"
            f"{escape_markdown('当前需要有效邀请')} *{escape_markdown(str(required))}* {escape_markdown('人后才可永久使用机器人。')}"
        )
        bot.reply_to(message, msg, parse_mode="MarkdownV2")
    elif command == '/setinvite':
        if len(command_parts) < 2 or not command_parts[1].strip().isdigit():
            bot.reply_to(message, escape_markdown("⚠️ 格式错误。用法: /setinvite <人数>"), parse_mode="MarkdownV2")
            return
        new_count = int(command_parts[1].strip())
        if new_count < 0:
            bot.reply_to(message, escape_markdown("⚠️ 人数不能小于 0。"), parse_mode="MarkdownV2")
            return
        if set_invite_required_count(new_count):
            ok = (
                f"✅ *{escape_markdown('设置成功')}*\n"
                f"{escape_markdown('新的邀请门槛:')} *{escape_markdown(str(new_count))}*"
            )
            bot.reply_to(message, ok, parse_mode="MarkdownV2")
        else:
            bot.reply_to(message, escape_markdown("❌ 设置失败，请稍后再试。"), parse_mode="MarkdownV2")
    elif command == '/resetnew':
        if len(command_parts) < 2 or not command_parts[1].strip().isdigit():
            bot.reply_to(message, escape_markdown("⚠️ 格式错误。用法: /resetnew <用户ID>"), parse_mode="MarkdownV2")
            return

        target_user_id = int(command_parts[1].strip())
        if target_user_id == int(CONFIG.get("ADMIN_ID", 0)):
            bot.reply_to(message, escape_markdown("⚠️ 不能重置管理员账号。"), parse_mode="MarkdownV2")
            return

        deleted_interactor = 0
        deleted_invited = 0
        deleted_as_inviter = 0
        try:
            with db_lock:
                conn = get_db_connection()
                c = conn.cursor()
                c.execute("DELETE FROM bot_interactors WHERE user_id = ?", (target_user_id,))
                deleted_interactor = c.rowcount or 0
                c.execute("DELETE FROM user_invites WHERE invited_user_id = ?", (target_user_id,))
                deleted_invited = c.rowcount or 0
                c.execute("DELETE FROM user_invites WHERE inviter_id = ?", (target_user_id,))
                deleted_as_inviter = c.rowcount or 0
                conn.commit()
                conn.close()

            membership_cache.pop(target_user_id, None)

            # 允许该用户下一次以“新用户”身份通过老用户校验
            mark_user_reset_as_new(target_user_id, True)

            done_text = (
                f"✅ *{escape_markdown('重置完成')}*\n"
                f"*{escape_markdown('用户ID:')}* `{escape_for_code(str(target_user_id))}`\n"
                f"{escape_markdown('已清理老用户痕迹与邀请记录，并已放行一次老用户校验。')}\n"
                f"{escape_markdown('清理项:')} {escape_markdown('interactor=')}{escape_markdown(str(deleted_interactor))}, "
                f"{escape_markdown('invited=')}{escape_markdown(str(deleted_invited))}, "
                f"{escape_markdown('inviter=')}{escape_markdown(str(deleted_as_inviter))}"
            )
            bot.reply_to(message, done_text, parse_mode="MarkdownV2")
        except Exception as e:
            print(f"⚠️ /resetnew 执行失败: {e}")
            bot.reply_to(message, escape_markdown("❌ 重置失败，请稍后再试。"), parse_mode="MarkdownV2")

# ---------------------- Poll Broadcast Flow ----------------------
def _parse_poll_options(raw_text):
    lines = [line.strip() for line in (raw_text or '').splitlines()]
    options = []
    seen = set()
    for line in lines:
        if not line:
            continue
        line = re.sub(r'^[-•*\d\.\)\(\s]+', '', line).strip()
        if not line or line in seen:
            continue
        seen.add(line)
        options.append(line)
    return options


def _parse_poll_deadline(raw_text):
    txt = (raw_text or '').strip()
    now = datetime.now(CHINA_TZ)
    if not txt:
        raise ValueError('empty deadline')
    low = txt.lower()
    m = re.fullmatch(r'(\d+)\s*([mhd])', low)
    if m:
        value = int(m.group(1))
        unit = m.group(2)
        if value <= 0:
            raise ValueError('invalid duration')
        delta = {'m': timedelta(minutes=value), 'h': timedelta(hours=value), 'd': timedelta(days=value)}[unit]
        return int((now + delta).timestamp())
    for fmt in ('%Y-%m-%d %H:%M', '%Y/%m/%d %H:%M', '%m-%d %H:%M', '%m/%d %H:%M'):
        try:
            dt = datetime.strptime(txt, fmt)
            if '%Y' not in fmt:
                dt = dt.replace(year=now.year)
            dt = dt.replace(tzinfo=CHINA_TZ)
            if dt.timestamp() <= now.timestamp() and '%Y' not in fmt:
                dt = dt.replace(year=now.year + 1)
            return int(dt.timestamp())
        except Exception:
            pass
    raise ValueError('bad deadline format')


def _fmt_dt_cn(ts):
    return datetime.fromtimestamp(int(ts), CHINA_TZ).strftime('%Y-%m-%d %H:%M')


def _get_bot_interactor_ids():
    with db_lock:
        conn = get_db_connection()
        c = conn.cursor()
        c.execute('SELECT user_id FROM bot_interactors ORDER BY last_interaction_date DESC')
        rows = c.fetchall()
        conn.close()
    return [int(r['user_id']) for r in rows]


def _create_poll_job(admin_id, question, options, closes_at, target_count=0):
    job_id = f"poll_{int(time.time())}_{uuid.uuid4().hex[:8]}"
    now_ts = int(time.time())
    with db_lock:
        conn = get_db_connection()
        c = conn.cursor()
        c.execute(
            '''INSERT INTO poll_broadcast_jobs
               (id, admin_id, question, options_json, is_anonymous, allows_multiple_answers, target_count, status, closes_at, created_at)
               VALUES (?, ?, ?, ?, 0, 0, ?, 'scheduled', ?, ?)''',
            (job_id, int(admin_id), question, json.dumps(options, ensure_ascii=False), int(target_count), int(closes_at), now_ts)
        )
        conn.commit()
        conn.close()
    return job_id


def _record_poll_message(job_id, user_id, sent_message):
    poll = getattr(sent_message, 'poll', None)
    poll_id = getattr(poll, 'id', None)
    if not poll_id:
        return
    with db_lock:
        conn = get_db_connection()
        c = conn.cursor()
        c.execute(
            '''INSERT OR REPLACE INTO poll_broadcast_messages
               (job_id, user_id, chat_id, message_id, poll_id, created_at)
               VALUES (?, ?, ?, ?, ?, ?)''',
            (job_id, int(user_id), int(sent_message.chat.id), int(sent_message.message_id), str(poll_id), int(time.time()))
        )
        conn.commit()
        conn.close()


def _update_poll_job_counts(job_id, success_count, fail_count):
    with db_lock:
        conn = get_db_connection()
        c = conn.cursor()
        c.execute(
            'UPDATE poll_broadcast_jobs SET success_count=?, fail_count=?, started_at=COALESCE(started_at, ?), status=? WHERE id=?',
            (int(success_count), int(fail_count), int(time.time()), 'running', job_id)
        )
        conn.commit()
        conn.close()


def _get_job_by_poll_id(poll_id):
    with db_lock:
        conn = get_db_connection()
        c = conn.cursor()
        c.execute('''SELECT j.*, m.user_id AS target_user_id FROM poll_broadcast_messages m JOIN poll_broadcast_jobs j ON j.id=m.job_id WHERE m.poll_id=? LIMIT 1''', (str(poll_id),))
        row = c.fetchone()
        conn.close()
    return row


def _save_poll_answer(job_id, poll_id, user_id, option_ids):
    option_ids = sorted(list({int(x) for x in (option_ids or [])}))
    with db_lock:
        conn = get_db_connection()
        c = conn.cursor()
        c.execute(
            '''INSERT OR REPLACE INTO poll_broadcast_answers
               (job_id, poll_id, user_id, option_ids_json, updated_at)
               VALUES (?, ?, ?, ?, ?)''',
            (job_id, str(poll_id), int(user_id), json.dumps(option_ids), int(time.time()))
        )
        conn.commit()
        conn.close()


def _load_poll_job(job_id):
    with db_lock:
        conn = get_db_connection()
        c = conn.cursor()
        c.execute('SELECT * FROM poll_broadcast_jobs WHERE id=?', (job_id,))
        row = c.fetchone()
        conn.close()
    return row


def _build_poll_summary(job_id):
    job = _load_poll_job(job_id)
    if not job:
        return None
    try:
        options = json.loads(job['options_json'] or '[]')
    except Exception:
        options = []
    counts = [0 for _ in options]
    voters = 0
    with db_lock:
        conn = get_db_connection()
        c = conn.cursor()
        c.execute('SELECT option_ids_json FROM poll_broadcast_answers WHERE job_id=?', (job_id,))
        rows = c.fetchall()
        conn.close()
    for row in rows:
        try:
            chosen = json.loads(row['option_ids_json'] or '[]')
        except Exception:
            chosen = []
        if chosen:
            voters += 1
        for idx in chosen:
            idx = int(idx)
            if 0 <= idx < len(counts):
                counts[idx] += 1
    lines = [
        "📊 投票结果汇总",
        f"任务ID: {job_id}",
        f"问题: {job['question']}",
        f"截止时间: {_fmt_dt_cn(job['closes_at'])}",
        f"发送成功/失败: {job['success_count']} / {job['fail_count']}",
        f"目标人数: {job['target_count']}",
        f"参与人数: {voters}",
        ''
    ]
    for i, option in enumerate(options):
        vote = counts[i]
        pct = (vote / voters * 100.0) if voters else 0.0
        lines.append(f"{i+1}. {option} — {vote} ({pct:.1f}%)")
    return '\n'.join(lines)


def _close_due_poll_jobs():
    now_ts = int(time.time())
    with db_lock:
        conn = get_db_connection()
        c = conn.cursor()
        c.execute("SELECT * FROM poll_broadcast_jobs WHERE status IN ('scheduled','running') AND closes_at <= ? ORDER BY closes_at ASC LIMIT 20", (now_ts,))
        due_jobs = c.fetchall()
        conn.close()
    for job in due_jobs:
        job_id = job['id']
        with db_lock:
            conn = get_db_connection()
            c = conn.cursor()
            c.execute('SELECT chat_id, message_id FROM poll_broadcast_messages WHERE job_id=?', (job_id,))
            rows = c.fetchall()
            conn.close()
        for row in rows:
            try:
                bot.stop_poll(int(row['chat_id']), int(row['message_id']))
            except Exception:
                pass
            time.sleep(0.05)
        summary = _build_poll_summary(job_id)
        with db_lock:
            conn = get_db_connection()
            c = conn.cursor()
            c.execute("UPDATE poll_broadcast_jobs SET status='completed', finished_at=?, summary_sent_at=? WHERE id=?", (now_ts, now_ts, job_id))
            conn.commit()
            conn.close()
        if summary:
            try:
                bot.send_message(int(job['admin_id']), summary, parse_mode='MarkdownV2', disable_web_page_preview=True)
            except Exception as e:
                logger.warning(f'发送投票汇总失败 {job_id}: {e}')


def poll_job_watcher_thread():
    logger.info('✅ 投票群发截止检测线程已启动。')
    while True:
        try:
            _close_due_poll_jobs()
        except Exception as e:
            logger.warning(f'投票截止线程异常: {e}')
        time.sleep(15)


def handle_poll_broadcast(message):
J3456789
    if admin_id in poll_broadcast_state:
        bot.reply_to(message, escape_markdown('您当前有一个正在进行的投票群发任务，请先完成或使用 /cancel_broadcast 取消当前流程。'), parse_mode='MarkdownV2')
        return
    poll_broadcast_state[admin_id] = {'step': 'awaiting_question'}
    bot.reply_to(
        message,
        f"🗳️ *{escape_markdown('投票群发')}*\n\n"
        f"*{escape_markdown('第 1/4 步：请发送投票问题。')}*\n"
        f"_{escape_markdown('例如：你最想先加哪个功能？')}_\n\n"
        f"*{escape_markdown('随时可发送')}* `/cancel_broadcast` *{escape_markdown('取消。')}*",
        parse_mode='MarkdownV2'
    )
    bot.register_next_step_handler(message, process_poll_broadcast_step)


def process_poll_broadcast_step(message):
J3456789
    if not admin_id or admin_id not in poll_broadcast_state:
        return
    if message.text and message.text.strip().lower() == '/cancel_broadcast':
        poll_broadcast_state.pop(admin_id, None)
        bot.reply_to(message, escape_markdown('✅ 投票群发已取消。'), parse_mode='MarkdownV2')
        return
    state = poll_broadcast_state.get(admin_id, {})
    step = state.get('step')
    if step == 'awaiting_question':
        question = (message.text or '').strip()
        if not question or question.startswith('/'):
            bot.reply_to(message, escape_markdown('请发送有效的投票问题文本。'), parse_mode='MarkdownV2')
            bot.register_next_step_handler(message, process_poll_broadcast_step)
            return
        state['question'] = question
        state['step'] = 'awaiting_options'
        poll_broadcast_state[admin_id] = state
        bot.reply_to(message, f"📝 *{escape_markdown('第 2/4 步：请逐行发送投票选项')}*\n\n{escape_markdown('至少 2 项，最多 10 项。')}\n_{escape_markdown('示例：')}\nA方案\nB方案\nC方案_", parse_mode='MarkdownV2')
        bot.register_next_step_handler(message, process_poll_broadcast_step)
        return
    if step == 'awaiting_options':
        options = _parse_poll_options(message.text or '')
        if len(options) < 2 or len(options) > 10:
            bot.reply_to(message, escape_markdown('投票选项需要 2 到 10 项，请重新发送，每行一个。'), parse_mode='MarkdownV2')
            bot.register_next_step_handler(message, process_poll_broadcast_step)
            return
        state['options'] = options
        state['step'] = 'awaiting_deadline'
        poll_broadcast_state[admin_id] = state
        bot.reply_to(message, f"⏰ *{escape_markdown('第 3/4 步：请发送截止时间')}*\n\n{escape_markdown('支持格式：')} `2026-03-08 18:00`、`03-08 18:00`、`6h`、`2d`\n_{escape_markdown('默认按中国时间 UTC+8 理解。')}_", parse_mode='MarkdownV2')
        bot.register_next_step_handler(message, process_poll_broadcast_step)
        return
    if step == 'awaiting_deadline':
        try:
            closes_at = _parse_poll_deadline(message.text or '')
        except Exception:
            bot.reply_to(message, escape_markdown('截止时间格式不对，请重新发送，例如 2026-03-08 18:00 或 6h。'), parse_mode='MarkdownV2')
            bot.register_next_step_handler(message, process_poll_broadcast_step)
            return
        user_ids = _get_bot_interactor_ids()
        state['closes_at'] = closes_at
        state['target_count'] = len(user_ids)
        state['step'] = 'awaiting_confirmation'
        poll_broadcast_state[admin_id] = state
        option_preview = '\n'.join([f"{i+1}. {escape_markdown(opt)}" for i, opt in enumerate(state['options'])])
        preview = (
            f"🧾 *{escape_markdown('第 4/4 步：确认投票群发')}*\n\n"
            f"*{escape_markdown('问题:')}* {escape_markdown(state['question'])}\n"
            f"*{escape_markdown('选项:')}*\n{option_preview}\n\n"
            f"*{escape_markdown('截止时间:')}* {escape_markdown(_fmt_dt_cn(closes_at))}\n"
            f"*{escape_markdown('目标人数:')}* `{escape_for_code(str(len(user_ids)))}`\n\n"
            f"{escape_markdown('确认发送可回复：')} `CONFIRM_POLL` / `CONFIRMPOLL` / `confirm` / `确认发送`\n"
            f"{escape_markdown('取消请发送：')} /cancel_broadcast"
        )
        bot.reply_to(message, preview, parse_mode='MarkdownV2')
        bot.register_next_step_handler(message, process_poll_broadcast_step)
        return
    if step == 'awaiting_confirmation':
        confirm_text = (message.text or '').strip()
        confirm_normalized = re.sub(r'[\s_]+', '', confirm_text).lower()
        if confirm_text != '确认发送' and confirm_normalized not in ('confirmpoll', 'confirm'):
            poll_broadcast_state.pop(admin_id, None)
            bot.reply_to(message, escape_markdown('❌ 已取消投票群发。'), parse_mode='MarkdownV2')
            return
        state = poll_broadcast_state.pop(admin_id, None) or {}
        question = state.get('question')
        options = state.get('options') or []
        closes_at = int(state.get('closes_at') or 0)
        user_ids = _get_bot_interactor_ids()
        job_id = _create_poll_job(admin_id, question, options, closes_at, len(user_ids))
        bot.reply_to(message, escape_markdown('✅ 投票群发任务已开始，截止后会自动把结果汇总发给您。'), parse_mode='MarkdownV2')

        def _runner():
            success = 0
            fail = 0
            for uid in user_ids:
                try:
                    sent = bot.send_poll(
                        chat_id=int(uid),
                        question=question,
                        options=options,
                        is_anonymous=False,
                        allows_multiple_answers=False
                    )
                    _record_poll_message(job_id, uid, sent)
                    success += 1
                except ApiTelegramException as e:
                    fail += 1
                    if getattr(e, 'error_code', None) not in [400, 403]:
                        logger.warning(f"[PollBroadcast] send to {uid} failed: {getattr(e, 'description', e)}")
                except Exception as e:
                    fail += 1
                    logger.warning(f"[PollBroadcast] send to {uid} failed: {e}")
                _update_poll_job_counts(job_id, success, fail)
                time.sleep(0.12)
            try:
                bot.send_message(
                    admin_id,
                    f"🗳️ *{escape_markdown('投票群发已发送完成')}*\n"
                    f"*{escape_markdown('任务ID:')}* `{escape_for_code(job_id)}`\n"
                    f"*{escape_markdown('发送成功:')}* `{escape_for_code(str(success))}`\n"
                    f"*{escape_markdown('发送失败:')}* `{escape_for_code(str(fail))}`\n"
                    f"*{escape_markdown('截止时间:')}* {escape_markdown(_fmt_dt_cn(closes_at))}",
                    parse_mode='MarkdownV2'
                )
            except Exception:
                pass

        threading.Thread(target=_runner, daemon=True).start()
        return


@bot.poll_answer_handler()
def handle_poll_answer_update(poll_answer):
    try:
        poll_id = getattr(poll_answer, 'poll_id', None)
        user = getattr(poll_answer, 'user', None)
        user_id = getattr(user, 'id', None)
        option_ids = getattr(poll_answer, 'option_ids', None) or []
        if not poll_id or user_id is None:
            return
        row = _get_job_by_poll_id(poll_id)
        if not row:
            return
        _save_poll_answer(row['id'], poll_id, user_id, option_ids)
    except Exception as e:
        logger.warning(f'处理投票回答失败: {e}')


# ---------------------- Broadcast Flow ----------------------
def handle_broadcast(message):
J3456789
    if admin_id in admin_broadcast_state:
        bot.reply_to(message, escape_markdown("您当前有一个正在进行的广播任务。请先完成或使用 /cancel_broadcast 取消。"), parse_mode="MarkdownV2")
        return

    admin_broadcast_state[admin_id] = {"step": "awaiting_content"}
    prompt_text = (
        f"📢 *{escape_markdown('开始广播流程')}*\n\n"
        f"*{escape_markdown('第 1/2 步: 请发送您想要广播的完整消息（可以是文本、图片、视频等）。')}*\n\n"
        f"*{escape_markdown('随时可以发送')}* `/cancel_broadcast` *{escape_markdown('来中止。')}*"
    )
    bot.reply_to(message, prompt_text, parse_mode="MarkdownV2")
    bot.register_next_step_handler(message, process_broadcast_content)

def process_broadcast_content(message):
J3456789
    if admin_id not in admin_broadcast_state:
        return

    if message.text and message.text.strip().lower() == '/cancel_broadcast':
        del admin_broadcast_state[admin_id]
        bot.reply_to(message, escape_markdown("✅ 广播已取消。"), parse_mode="MarkdownV2")
        return

    admin_broadcast_state[admin_id]['message_to_send'] = message
    admin_broadcast_state[admin_id]['step'] = 'awaiting_confirmation'

    with db_lock:
        conn = get_db_connection()
        c = conn.cursor()
        c.execute("SELECT COUNT(user_id) FROM bot_interactors")
        total_users = c.fetchone()[0]
        conn.close()

    confirmation_text = (
        f"❓ *{escape_markdown('第 2/2 步: 请确认广播')}*\n\n"
        f"{escape_markdown(f'此消息将被发送给 {total_users} 位曾与机器人私聊的用户。')}\n\n"
        f"*{escape_markdown('预览如下:')}*"
    )
    bot.send_message(admin_id, confirmation_text, parse_mode="MarkdownV2")
    bot.copy_message(chat_id=admin_id, from_chat_id=message.chat.id, message_id=message.message_id)

    final_prompt = (
        f"*{escape_markdown('如果确认无误，请输入')}* `CONFIRM` *{escape_markdown('来立即开始广播。')}*\n"
        f"{escape_markdown('输入任何其他内容或发送 /cancel_broadcast 将取消。')}"
    )
    bot.send_message(admin_id, final_prompt, parse_mode="MarkdownV2")
    bot.register_next_step_handler(message, execute_broadcast)

def execute_broadcast(message):
J3456789
    if admin_id not in admin_broadcast_state or admin_broadcast_state[admin_id].get('step') != 'awaiting_confirmation':
        return

    if not message.text or message.text.strip() != 'CONFIRM':
        del admin_broadcast_state[admin_id]
        bot.reply_to(message, escape_markdown("❌ 操作已取消。广播未发送。"), parse_mode="MarkdownV2")
        return

    broadcast_data = admin_broadcast_state.pop(admin_id)
    message_to_send = broadcast_data['message_to_send']

    bot.reply_to(message, escape_markdown("✅ 确认成功！广播任务已在后台开始，完成后会通知您。"), parse_mode="MarkdownV2")

    def broadcast_thread_func():
        with db_lock:
            conn = get_db_connection()
            c = conn.cursor()
            c.execute("SELECT user_id FROM bot_interactors")
            all_user_ids = [row['user_id'] for row in c.fetchall()]
            conn.close()

        success_count = 0
        fail_count = 0
        start_time = time.time()

        for user_id in all_user_ids:
            try:
                bot.copy_message(
                    chat_id=user_id,
                    from_chat_id=message_to_send.chat.id,
                    message_id=message_to_send.message_id
                )
                success_count += 1
            except ApiTelegramException as e:
                fail_count += 1
                if e.error_code not in [400, 403]:
                    print(f"[Broadcast] Failed to send to {user_id}: {e.description}")
            except Exception as e:
                fail_count += 1
                print(f"[Broadcast] Unknown error for {user_id}: {e}")
            time.sleep(0.1)

        end_time = time.time()
        duration = round(end_time - start_time, 2)
        
        summary_text = (
            f"🏁 *{escape_markdown('广播完成！')}*\n\n"
            f"⏱️ *{escape_markdown('总耗时:')}* `{duration}` {escape_markdown('秒')}\n"
            f"✅ *{escape_markdown('发送成功:')}* `{success_count}`\n"
            f"❌ *{escape_markdown('发送失败:')}* `{fail_count}`\n"
            f"👥 *{escape_markdown('总目标数:')}* `{len(all_user_ids)}`"
        )
        bot.send_message(admin_id, summary_text, parse_mode="MarkdownV2")

    threading.Thread(target=broadcast_thread_func, daemon=True).start()

def handle_cancel_broadcast(message):
J3456789
    if admin_id == CONFIG['ADMIN_ID'] and admin_id in admin_broadcast_state:
        del admin_broadcast_state[admin_id]
        bot.reply_to(message, escape_markdown("✅ 当前的广播会话已成功取消。"), parse_mode="MarkdownV2")
    else:
        bot.reply_to(message, escape_markdown("ℹ️ 当前没有正在进行的广播任务。"), parse_mode="MarkdownV2")




# ---------------------- 新增：管理员数据库管理命令 ----------------------
# 仅管理员可用：查看数据库大小/删除聊天记录/关键词批量删除/开关聊天记录功能

def _is_admin_user(message):
    try:
        return getattr(message.from_user, 'id', None) == CONFIG.get('ADMIN_ID')
    except Exception:
        return False


@bot.message_handler(commands=['dbsize'])
def handle_dbsize_cmd(message):
    if not _is_admin_user(message):
        return
    try:
        size_b = get_db_size_bytes()
        n_users = count_table_rows('users')
        n_msgs = count_table_rows('message_history')
        n_chats = count_table_rows('chat_info')
        enabled = is_chatlog_enabled()
        txt = (
            f"🗃️ 数据库大小: {size_b} bytes ({size_b/1024/1024:.2f} MB)\n"
            f"📁 DB路径: {CONFIG.get('DATABASE_FILE')}\n"
            f"👥 users: {n_users}\n"
            f"💬 message_history: {n_msgs}\n"
            f"🏷️ chat_info: {n_chats}\n"
            f"📜 聊天记录功能: {'开启' if enabled else '关闭'}"
        )
        bot.reply_to(message, txt, parse_mode=None)
    except Exception as e:
        bot.reply_to(message, f"❌ 获取失败: {e}", parse_mode=None)


@bot.message_handler(commands=['delmsgs'])
def handle_delmsgs_cmd(message):
    if not _is_admin_user(message):
        return
    raw = (message.text or '').split(maxsplit=1)
    if len(raw) < 2 or not raw[1].strip().isdigit():
        bot.reply_to(message, '用法: /delmsgs 10000  （删除最旧的 N 条聊天记录）', parse_mode=None)
        return
    n = int(raw[1].strip())
    deleted = delete_oldest_messages(n)
    if deleted < 0:
        bot.reply_to(message, '❌ 删除失败：数据库可能存在损坏页（malformed）。建议先做数据库修复。', parse_mode=None)
        return
    bot.reply_to(message, f"✅ 已删除 {deleted} 条最旧聊天记录。\n提示：数据库文件大小可能不会立刻变小，可执行 /vacuumdb。", parse_mode=None)


@bot.message_handler(commands=['delmsgkw'])
def handle_delmsgkw_cmd(message):
    if not _is_admin_user(message):
        return
    raw = (message.text or '').split(maxsplit=1)
    if len(raw) < 2 or not raw[1].strip():
        bot.reply_to(message, '用法: /delmsgkw 关键词1,关键词2  （按关键词批量删除）', parse_mode=None)
        return
    kws = [k.strip() for k in raw[1].split(',') if k.strip()]
    deleted = delete_messages_by_keywords(kws)
    if deleted < 0:
        bot.reply_to(message, '❌ 关键词删除失败：数据库可能存在损坏页（malformed）。建议先做数据库修复。', parse_mode=None)
        return
    bot.reply_to(message, f"✅ 已按关键词删除 {deleted} 条聊天记录。\n提示：可执行 /vacuumdb 压缩。", parse_mode=None)


@bot.message_handler(commands=['vacuumdb'])
def handle_vacuumdb_cmd(message):
    if not _is_admin_user(message):
        return
    ok = vacuum_database()
    bot.reply_to(message, "✅ 已执行 VACUUM。" if ok else "❌ VACUUM 失败。", parse_mode=None)


@bot.message_handler(commands=['chatlog_on'])
def handle_chatlog_on_cmd(message):
    if not _is_admin_user(message):
        return
    set_setting_in_db('chatlog_feature_enabled', '1')
    bot.reply_to(message, '✅ 已开启聊天记录查询/跳转功能。', parse_mode=None)


@bot.message_handler(commands=['chatlog_off'])
def handle_chatlog_off_cmd(message):
    if not _is_admin_user(message):
        return
    set_setting_in_db('chatlog_feature_enabled', '0')
    bot.reply_to(message, '✅ 已关闭聊天记录查询/跳转功能。', parse_mode=None)


@bot.message_handler(commands=['crawl_on'])
def handle_crawl_on_cmd(message):
    if not _is_admin_user(message):
        return
    ok = set_crawler_enabled(True)
    bot.reply_to(message, '✅ 已开启数据抓取功能。' if ok else '❌ 开启数据抓取失败。', parse_mode=None)


@bot.message_handler(commands=['crawl_off'])
def handle_crawl_off_cmd(message):
    if not _is_admin_user(message):
        return
    ok = set_crawler_enabled(False)
    bot.reply_to(message, '✅ 已关闭数据抓取功能。新消息/用户资料/频道扫描将暂停。' if ok else '❌ 关闭数据抓取失败。', parse_mode=None)


@bot.message_handler(commands=['crawl_status'])
def handle_crawl_status_cmd(message):
    if not _is_admin_user(message):
        return
    bot.reply_to(message, f"📡 数据抓取状态: {'开启' if is_crawler_enabled() else '关闭'}", parse_mode=None)

# --------------------------------------------------------------------

# ---------------------- Query Flow (REFACTORED) ----------------------
@bot.message_handler(commands=['cxzbf'])
@check_membership
def handle_text_query(message):
    query = message.text[len('/cxzbf'):].strip()
    if not query:
        reply_text = f"⚠️ *{escape_markdown('请输入查询关键词')}*\n{escape_markdown('可以直接发送用户名或ID，或者使用命令：')}`/cxzbf @username`"
        bot.reply_to(message, reply_text, parse_mode="MarkdownV2")
        return
    trigger_query_flow(message, query)

@bot.message_handler(commands=['chatlog'])
@check_membership
def handle_chatlog_cmd(message):
    """仅发送聊天记录网页跳转链接。用法: /chatlog @username 或 /chatlog 123"""
    q = message.text[len('/chatlog'):].strip() if message.text else ''
    if not q:
        bot.reply_to(message, '用法: /chatlog @username 或 /chatlog 123', parse_mode=None)
        return
    if telethon_loop is None or not client.is_connected():
        bot.reply_to(message, '后台服务正在初始化...请稍候再试。', parse_mode=None)
        return

    if not is_chatlog_enabled():
        bot.reply_to(message, '管理员已关闭聊天记录查询功能。', parse_mode=None)
        return

    query_cleaned = q.strip().lstrip('@')
    resolved_id = None
    user_to_sync = None

    if query_cleaned.isdigit():
        try:
            resolved_id = int(query_cleaned)
        except Exception:
            resolved_id = None

    try:
        entity_query = int(query_cleaned) if query_cleaned.isdigit() else query_cleaned
        future = asyncio.run_coroutine_threadsafe(client.get_entity(entity_query), telethon_loop)
        live_user = future.result(timeout=CONFIG.get('TELETHON_TIMEOUT', 45))
        if live_user and isinstance(live_user, User) and not live_user.bot:
            user_to_sync = live_user
            resolved_id = live_user.id
    except Exception:
        pass

    if not resolved_id:
        resolved_id = _resolve_historic_query_to_id(q)

    if not resolved_id:
        bot.reply_to(message, '未能识别该用户的 Telegram ID（可能已注销或隐私限制）。', parse_mode=None)
        return

    # 尝试同步一次（失败也不影响跳转）
    try:
        if not user_to_sync:
            entity_future = asyncio.run_coroutine_threadsafe(client.get_entity(resolved_id), telethon_loop)
            user_to_sync = entity_future.result(timeout=CONFIG.get('TELETHON_TIMEOUT', 45))
        if user_to_sync and isinstance(user_to_sync, User):
            update_future = asyncio.run_coroutine_threadsafe(update_user_in_db(user_to_sync), telethon_loop)
            update_future.result(timeout=CONFIG.get('TELETHON_TIMEOUT', 45))
    except Exception:
        pass

    send_chatlog_link(message, resolved_id)

@bot.message_handler(func=lambda m: m.chat.type == 'private' and m.text and not m.text.startswith('/') and (m.text.strip().isdigit() or (m.text.strip().startswith('@') and len(m.text.strip()) > 1)))
@check_membership
def handle_direct_query(message):
    query = message.text.strip()
    trigger_query_flow(message, query)

@bot.message_handler(func=lambda m: m.forward_from is not None, content_types=ALL_CONTENT_TYPES)
@check_membership
def handle_forwarded_query(message):
    sender = message.forward_from
    if sender.is_bot:
        bot.reply_to(message, escape_markdown("ℹ️ 无法查询机器人账户。"), parse_mode="MarkdownV2")
        return
    
    query_term = str(sender.id)
    trigger_query_flow(message, query_term)

@bot.message_handler(func=lambda m: m.forward_sender_name is not None, content_types=ALL_CONTENT_TYPES)
@check_membership
def handle_hidden_forward(message):
    update_active_user(message.from_user.id)
    response_text = (
        f"👤 *{escape_markdown('用户信息 | 隐藏转发')}*\n\n"
        f"{escape_markdown('此用户已隐藏其转发身份，无法获取其 Telegram ID，因此无法提供查询服务。')}"
    )
    bot.reply_to(message, response_text + f"\n\n{ADVERTISEMENT_TEXT}", parse_mode="MarkdownV2")

def trigger_query_flow(message, query):
    update_active_user(message.from_user.id)

    if telethon_loop is None or not client.is_connected():
        bot.reply_to(message, f"⏳ *{escape_markdown('后台服务正在初始化...')}*\n{escape_markdown('请稍候几秒再试。')}", parse_mode="MarkdownV2")
        return

    waiting_message = None
    try:
        should_reply = not (message.text and message.text.startswith('/start bizChat'))
        reply_params = ReplyParameters(message_id=message.message_id, allow_sending_without_reply=True) if should_reply else None
        waiting_message = bot.send_message(message.chat.id, escape_markdown("⏳ 正在数据库中检索并同步最新资料... ⚡️"), reply_parameters=reply_params, parse_mode="MarkdownV2")
    except Exception as e:
        print(f"⚠️ 发送等待消息失败: {e}")

    def perform_query_and_send_results():
        try:
            resolved_id = None
            user_to_sync = None
            query_cleaned = query.strip().lstrip('@')
            
            # --- ID Resolution ---
            if query_cleaned.isdigit():
                try:
                    resolved_id = int(query_cleaned)
                    print(f"ℹ️ [ID-Resolve] Query is numeric. Tentative ID: {resolved_id}")
                except (ValueError, TypeError):
                    resolved_id = None
            
            try:
                entity_query = int(query_cleaned) if query_cleaned.isdigit() else query_cleaned
                future = asyncio.run_coroutine_threadsafe(client.get_entity(entity_query), telethon_loop)
                live_user = future.result(timeout=CONFIG["TELETHON_TIMEOUT"])
                if live_user and isinstance(live_user, User) and not live_user.bot:
                    user_to_sync = live_user
                    resolved_id = live_user.id
                    print(f"✅ [ID-Resolve] API resolved '{query}' to ID: {resolved_id}")
            except (FuturesTimeoutError, TelethonTimeoutError):
                print(f"⚠️ [ID-Resolve] API lookup for '{query}' timed out.")
            except (ValueError, TypeError, UsernameInvalidError, PeerIdInvalidError):
                print(f"ℹ️ [ID-Resolve] API could not find user '{query}'.")
            except Exception as e:
                print(f"💥 [ID-Resolve] Unexpected error for '{query}': {e}")

            if not resolved_id:
                resolved_id = _resolve_historic_query_to_id(query)
                if resolved_id:
                    print(f"✅ [ID-Resolve] Found ID {resolved_id} for '{query}' in historical DB.")

            # --- Main Logic Branch: If an ID was found ---
            if resolved_id:
                print(f"✅ [Query-Start] Proceeding with User ID: {resolved_id}. Attempting sync...")
                try:
                    if not user_to_sync:
                        entity_future = asyncio.run_coroutine_threadsafe(client.get_entity(resolved_id), telethon_loop)
                        user_to_sync = entity_future.result(timeout=CONFIG["TELETHON_TIMEOUT"])
                    if user_to_sync and isinstance(user_to_sync, User):
                        update_future = asyncio.run_coroutine_threadsafe(update_user_in_db(user_to_sync), telethon_loop)
                        update_future.result(timeout=CONFIG["TELETHON_TIMEOUT"])
                        print(f"✅ [Sync-Complete] DB synchronized for user {resolved_id}.")
                except Exception as e:
                    print(f"⚠️ [Sync-Error] Sync failed for user {resolved_id}: {e}. Report will use existing/scanned data.")
                
                # --- Always Gather All Available Data ---
                scam_channel_hits = []
                try:
                    search_future = asyncio.run_coroutine_threadsafe(search_monitored_channels_for_user(user_id=resolved_id), telethon_loop)
                    scam_channel_hits = search_future.result(timeout=CONFIG["SCAM_CHANNEL_SEARCH_TIMEOUT"])
                except Exception as e:
                    print(f"💥 [Scam-Scan] Error searching channels for user {resolved_id}: {type(e).__name__}")
                
                common_groups = []
                try:
                    groups_future = asyncio.run_coroutine_threadsafe(get_common_groups_with_user(resolved_id), telethon_loop)
                    common_groups = groups_future.result(timeout=CONFIG["COMMON_GROUPS_TIMEOUT"])
                except Exception as e:
                    print(f"💥 [Common-Groups] Error getting common groups for user {resolved_id}: {type(e).__name__}")

                db_history = query_user_history_from_db(resolved_id)
                phone_history = query_phone_history_from_db(resolved_id)
                bio_history = query_bio_history_from_db(resolved_id)
                spoken_in_group_ids = query_spoken_groups_from_db(resolved_id)
                reports = load_reports()
                verified_report = reports.get('verified', {}).get(str(resolved_id))

                if waiting_message:
                    try: bot.delete_message(waiting_message.chat.id, waiting_message.message_id)
                    except Exception: pass

                # --- Decision Point: Is there anything to report? ---
                if db_history or scam_channel_hits or verified_report:
                    send_query_result(
                        message=message, query=query, resolved_id=resolved_id, db_history=db_history,
                        verified_info=verified_report, scam_channel_hits=scam_channel_hits,
                        common_groups=common_groups, spoken_in_group_ids=spoken_in_group_ids,
                        phone_history=phone_history, bio_history=bio_history
                    )
                else:
                    reply_text = f"📭 {escape_markdown('已识别用户ID ')}\`{escape_for_code(str(resolved_id))}\`{escape_markdown('，但未在其历史记录、官方投稿或监控频道中发现任何相关信息。')}"
                    bot.reply_to(message, reply_text, parse_mode="MarkdownV2")
                return

            # --- Fallback Branch: If NO ID was found ---
            print(f"ℹ️ [Fallback-Scan] Could not resolve '{query}' to an ID. Searching raw text in channels.")
            
            partial_hits = []
            try:
                search_future = asyncio.run_coroutine_threadsafe(search_monitored_channels_for_user(raw_query=query), telethon_loop)
                partial_hits = search_future.result(timeout=CONFIG["SCAM_CHANNEL_SEARCH_TIMEOUT"])
            except Exception as e:
                print(f"💥 [Fallback-Scan] Channel search failed: {e}")

            if waiting_message:
                try: bot.delete_message(waiting_message.chat.id, waiting_message.message_id)
                except Exception: pass

            if partial_hits:
                header = f"⚠️ *{escape_markdown('部分匹配结果')}*\n\n{escape_markdown('无法直接识别用户 ')}\`{escape_for_code(query)}\`{escape_markdown('，可能因为对方隐私设置严格或已注销。')}\n\n{escape_markdown('但是，我们在监控频道中找到了包含此ID或用户名的提及记录:')}"
                risk_header = f"🔍 *{escape_markdown('风险记录')} \\({len(partial_hits)} {escape_markdown('条')}\\)*"
                risk_parts = [
                    risk_header
                ] + [
                    f"› [{escape_markdown(_sanitize_for_link_text(hit['chat_title']))}]({_escape_url_md(hit['link'])})"
                    for hit in partial_hits
                ]
                final_text = header + "\n\n" + "\n".join(risk_parts) + f"\n\n{ADVERTISEMENT_TEXT}"
                bot.reply_to(message, final_text, parse_mode="MarkdownV2", disable_web_page_preview=True)
            else:
                reply_text = f"📭 {escape_markdown('未在数据库中找到与 ')}\`{escape_for_code(query)}\`{escape_markdown(' 相关的任何用户记录，各监控频道中也无相关内容。此用户可能不存在或与诈骗无关。')}"
                bot.reply_to(message, reply_text, parse_mode="MarkdownV2")

        except Exception as e:
            if waiting_message:
                try: bot.delete_message(waiting_message.chat.id, waiting_message.message_id)
                except Exception: pass
            print(f"❌ 查询流程出错: {e}")
            traceback.print_exc()
            error_text = escape_markdown(f"❌ 查询失败，请稍后重试或联系管理员。错误: {type(e).__name__}")
            bot.reply_to(message, error_text, parse_mode="MarkdownV2")
            
    threading.Thread(target=perform_query_and_send_results, daemon=True).start()

def send_query_result(message, query, resolved_id, db_history, verified_info, scam_channel_hits, common_groups, spoken_in_group_ids, phone_history, bio_history):
    chat_id = message.chat.id
    message_parts_md = []
    
    # --- Risk Assessment ---
    warning_source = None
    if verified_info:
        warning_source = "官方验证投稿"
    elif scam_channel_hits:
        warning_source = "反诈频道曝光"
    if warning_source:
        message_parts_md.append(f"🚨 *高风险警报* 🚨\n*{escape_markdown('风险来源:')}* {escape_markdown(warning_source)}")

    # --- Main Profile Info (Handles missing DB data) ---
    if db_history and db_history.get('current_profile'):
        profile = db_history['current_profile']
        profile_keys = profile.keys()
        user_id = db_history['user_id']
        display_name = (f"{profile['first_name'] or ''} {profile['last_name'] or ''}").strip()
        
        user_summary_parts = [f"👤 *{escape_markdown('用户资料')}*"]
        user_summary_parts.append(f"› *ID:* `{user_id}`")
        if display_name:
            user_summary_parts.append(f"› *Name:* {escape_markdown(display_name)}")
        
        active_usernames = json.loads(profile['active_usernames_json']) if 'active_usernames_json' in profile_keys and profile['active_usernames_json'] else []
        if active_usernames:
            user_summary_parts.append(f"› *Username:* {', '.join([f'@{escape_markdown(u)}' for u in active_usernames])}")
        if 'phone' in profile_keys and profile['phone']:
            user_summary_parts.append(f"› *Phone:* `{escape_for_code(profile['phone'])}`")
        if 'bio' in profile_keys and profile['bio']:
            user_summary_parts.append(f"› *Bio:* {escape_markdown(profile['bio'])}")
        
        message_parts_md.append("\n".join(user_summary_parts))

        business_parts = []
        if 'business_bio' in profile_keys and profile['business_bio']:
            business_parts.append(f"› *简介:* {escape_markdown(profile['business_bio'])}")
        
        if 'business_location_json' in profile_keys and profile['business_location_json']:
            try:
                loc_data = json.loads(profile['business_location_json'])
                if loc_data.get('address'): business_parts.append(f"› *位置:* {escape_markdown(loc_data['address'])}")
            except: pass

        if 'business_work_hours_json' in profile_keys and profile['business_work_hours_json']:
            try:
                wh_data = json.loads(profile['business_work_hours_json'])
                if wh_data and wh_data.get('periods'):
                    periods = [f"{divmod(p['start_minute'], 60)[0]:02d}:{divmod(p['start_minute'], 60)[1]:02d}-{divmod(p['end_minute'], 60)[0]:02d}:{divmod(p['end_minute'], 60)[1]:02d}" for p in wh_data['periods']]
                    business_parts.append(f"› *时间:* {escape_markdown(', '.join(periods))} ({wh_data.get('timezone_id', '')})")
                else:
                    business_parts.append(f"› *时间:* {escape_markdown('7 × 24 小时营业')}")
            except: pass
            
        if business_parts:
            message_parts_md.append(f"🏢 *{escape_markdown('营业信息')}*\n" + "\n".join(business_parts))
    else:
        user_id = resolved_id
        header = f"👤 *{escape_markdown('用户资料 (暂无记录)')}*"
        id_line = f"› *ID:* `{user_id}`"
        note_line = f"_{escape_markdown('提示：数据库暂未记录该用户的详细资料（不代表隐私/权限受限）。')}_"
        message_parts_md.append(f"{header}\n{id_line}\n{note_line}")
        
    # --- Scam Channel Hits ---
    if scam_channel_hits:
        risk_header = f"🔍 *{escape_markdown('风险记录')} \\({len(scam_channel_hits)} {escape_markdown('条')}\\)*"
        risk_parts = [
            risk_header
        ] + [
            f"› [{escape_markdown(_sanitize_for_link_text(hit['chat_title']))}]({_escape_url_md(hit['link'])})"
            for hit in scam_channel_hits
        ]
        message_parts_md.append("\n".join(risk_parts))

    # --- History Sections (if available) ---
    profile_history = db_history.get('profile_history', []) if db_history else []
    if len(profile_history) > 1:
        history_header = f"📜 *{escape_markdown('历史变动')} \\({len(profile_history)} {escape_markdown('条')}\\)*"
        event_blocks = []
        for e in profile_history:
            formatted_time = escape_for_code(datetime.fromtimestamp(e['timestamp'], tz=CHINA_TZ).strftime('%Y-%m-%d %H:%M'))
            name_str = escape_for_code(e.get('name') or '无')
            username_part = f"@{e.get('username')}" if e.get('username') else '无'
            username_str = escape_markdown(username_part)
            event_blocks.append(f"`{formatted_time}`\n › N: `{name_str}`\n › U: {username_str}")
        
        full_history_text = history_header + "\n" + "\n\n".join(event_blocks)
        message_parts_md.append(full_history_text)


    # --- Common Groups ---
    all_common_groups_dict = {group['id']: group for group in common_groups}
    if spoken_in_group_ids:
        for chat_id_from_db in spoken_in_group_ids:
            if chat_id_from_db not in all_common_groups_dict:
                db_info = get_chat_info_from_db(chat_id_from_db)
                if db_info: all_common_groups_dict[chat_id_from_db] = {'id': chat_id_from_db, 'title': db_info.get('title'), 'usernames': [db_info['username']] if db_info.get('username') else [], 'about': None}
    if all_common_groups_dict:
        group_list = sorted(all_common_groups_dict.values(), key=lambda g: str(g.get('title', '')).lower())
        group_lines = []
        for group in group_list:
            title = group.get('title') or f"群组ID: {group.get('id')}"
            public_usernames = [u for u in group.get('usernames', []) if u]
            line_parts = ["›"]
            if public_usernames:
                line_parts.append(" ".join([f"@{escape_markdown(u)}" for u in public_usernames]) + " \\-")
            else:
                line_parts.append(escape_markdown("[私密]"))
            line_parts.append(escape_markdown(title))
            # 加上可打开的群/频道链接（尽力而为）
            _chat_url = ""
            if public_usernames:
                _chat_url = f"https://t.me/{public_usernames[0].lstrip('@')}"
            else:
                try:
                    _cid = abs(int(group.get('id')))
                    _s = str(_cid)
                    if _s.startswith("100"):
                        _s = _s[3:]
                        _chat_url = f"https://t.me/c/{_s}/1"
                except Exception:
                    _chat_url = ""
            # MarkdownV2 中裸露的 URL 会因为 '.' 等字符导致解析失败。
            # 这里用一个最简单的可点击链接标记，避免触发 fallback（fallback 会把 '.' 去掉，导致 https://tme）。
            if _chat_url:
                line_parts.append(f"[🔗]({_escape_url_md(_chat_url)})")
            group_lines.append(" ".join(line_parts))
        unique_group_lines = sorted(list(dict.fromkeys(group_lines)), key=str.lower)
        group_header = f"👥 *{escape_markdown('共同群组')} \\({len(unique_group_lines)} {escape_markdown('个')}\\)*"
        message_parts_md.append(f"{group_header}\n" + "\n".join(unique_group_lines))
    
    # --- Bio & Phone History ---
    if bio_history:
        bio_header = f"📝 *Bio {escape_markdown('历史')} \\({len(bio_history)} {escape_markdown('条')}\\)*"
        message_parts_md.append(f"{bio_header}\n" + "\n\n".join([f"› `{escape_for_code(datetime.fromtimestamp(h['date'], tz=CHINA_TZ).strftime('%Y-%m-%d'))}`\n  `{escape_for_code((h['bio'] or '').strip() or '空')}`" for h in bio_history]))
    if phone_history:
        phone_header = f"📱 *{escape_markdown('绑定号码')} \\({len(phone_history)} {escape_markdown('个')}\\)*"
        message_parts_md.append(f"{phone_header}\n" + "\n".join([f"› `{escape_for_code(phone)}`" for phone in phone_history]))
        
    # --- Final Assembly and Sending ---
    main_text = "\n\n".join(filter(None, message_parts_md))
    full_message = main_text.strip() + f"\n\n*────────────────────*\n{ADVERTISEMENT_TEXT}"
    
    try:
        should_reply = not (message.text and message.text.startswith('/start bizChat'))
        reply_params = ReplyParameters(message_id=message.message_id, allow_sending_without_reply=True) if should_reply else None
        bot.send_message(chat_id, full_message, reply_parameters=reply_params, disable_web_page_preview=True, parse_mode="MarkdownV2")
    except ApiTelegramException as e:
        if "message is too long" in str(e).lower():
            try:
                safe_main_text = re.sub(r'[_*`\\]', '', main_text)
                file_content = f"--- User Report for {resolved_id} ---\n\n{safe_main_text}"
                file_to_send = io.BytesIO(file_content.encode('utf-8'))
                file_to_send.name = f"report_{resolved_id}.txt"
                summary_text = f"⚠️ *{escape_markdown('报告过长')}*\n{escape_markdown('详细报告已生成文件发送。')}\n\n{ADVERTISEMENT_TEXT}"
                bot.send_message(chat_id, summary_text, parse_mode="MarkdownV2", disable_web_page_preview=True)
                bot.send_document(chat_id, file_to_send)
            except Exception as file_e:
                print(f"💥 创建或发送文件报告失败: {file_e}")
                bot.send_message(chat_id, f"❌ {escape_markdown('报告过长且无法生成文件。')}", parse_mode="MarkdownV2")
        elif "message is not modified" not in str(e).lower():
            safe_text = re.sub(r'[_*\[\]()~`>#+\-=|{}.!\\]', '', full_message)
            fallback_message = (f"⚠️ 报告包含特殊字符，无法以格式化形式发送。以下是纯文本版本：\n\n{safe_text}")[:4096]
            bot.send_message(chat_id, fallback_message, disable_web_page_preview=True, parse_mode=None)

    if verified_info and verified_info.get('evidence_messages'):
        bot.send_message(chat_id, f"*{escape_markdown('以下是官方验证投稿的证据：')}*", parse_mode="MarkdownV2")
        for ev in verified_info['evidence_messages']:
            try:
                bot.forward_message(chat_id, ev['chat_id'], ev['message_id'])
                time.sleep(0.5)
            except Exception as e:
                print(f"❌ 转发证据失败: {e}")
                bot.send_message(chat_id, f"_{escape_markdown('一份证据无法转发(可能已被删除)。')}_", parse_mode="MarkdownV2")



    # --- Chatlog Link (Web) ---
    try:
        send_chatlog_link(message, resolved_id)
    except Exception as _e:
        pass

# --- Tougao (投稿) handlers ---
@bot.message_handler(commands=['tougao'])
@check_membership
def handle_tougao(message):
    update_active_user(message.from_user.id)
    user_id = message.from_user.id
    if user_id in user_submission_state:
        bot.reply_to(message, escape_markdown("您当前有一个正在进行的投稿，请先完成或取消。"), parse_mode="MarkdownV2")
        return
    
    user_submission_state[user_id] = {"step": "awaiting_ids", "evidence": []}
    prompt_text = (
        f"✍️ *{escape_markdown('开始投稿流程')}*\n\n"
        f"*{escape_markdown('第 1/3 步: 请发送诈骗犯的 Telegram User ID。')}*\n"
        f"{escape_markdown('如果知道多个ID，请每行输入一个。如果不知道，请发送“无”。')}"
    )
    bot.reply_to(message, prompt_text, parse_mode="MarkdownV2")
    bot.register_next_step_handler(message, process_scammer_ids)

def process_scammer_ids(message):
    user_id = message.from_user.id
    if user_id not in user_submission_state: return

    ids_text = message.text.strip()
    if ids_text.lower() in ['无', 'none', 'null', '']:
        user_submission_state[user_id]['ids'] = []
    else:
        raw_ids = ids_text.splitlines()
        valid_ids = [line.strip() for line in raw_ids if line.strip().isdigit()]
        if len(valid_ids) != len(raw_ids):
            bot.reply_to(message, escape_markdown("⚠️ 输入包含了无效的ID格式，请只输入数字ID，每行一个。请重新开始 /tougao。"), parse_mode="MarkdownV2")
            del user_submission_state[user_id]
            return
        user_submission_state[user_id]['ids'] = valid_ids

    user_submission_state[user_id]['step'] = "awaiting_usernames"
    prompt_text = (
        f"✅ *{escape_markdown('ID 已收到。')}*\n\n"
        f"*{escape_markdown('第 2/3 步: 请发送诈骗犯的 Telegram 用户名 (不带@)。')}*\n"
        f"{escape_markdown('如果知道多个，请每行输入一个。如果不知道，请发送“无”。')}"
    )
    bot.reply_to(message, prompt_text, parse_mode="MarkdownV2")
    bot.register_next_step_handler(message, process_scammer_usernames)

def process_scammer_usernames(message):
    user_id = message.from_user.id
    if user_id not in user_submission_state: return

    usernames_text = message.text.strip()
    if usernames_text.lower() in ['无', 'none', 'null', '']:
        user_submission_state[user_id]['usernames'] = []
    else:
        user_submission_state[user_id]['usernames'] = [u.strip().lstrip('@') for u in usernames_text.splitlines()]

    if not user_submission_state[user_id]['ids'] and not user_submission_state[user_id]['usernames']:
        bot.reply_to(message, escape_markdown("❌ 错误：您必须至少提供一个ID或用户名。请使用 /tougao 重新开始。"), parse_mode="MarkdownV2")
        del user_submission_state[user_id]
        return
        
    user_submission_state[user_id]['step'] = "awaiting_evidence"
    prompt_text = (
        f"✅ *{escape_markdown('用户名已收到。')}*\n\n"
        f"*{escape_markdown('第 3/3 步: 请发送所有相关证据。')}*\n"
        f"{escape_markdown('这可以是文字说明、截图、聊天记录文件等。')}\n\n"
        f"*{escape_markdown('发送完所有证据后，请务必发送')}* `{escape_markdown(DONE_SUBMISSION_COMMAND)}` *{escape_markdown('来完成投稿。')}*"
    )
    bot.reply_to(message, prompt_text, parse_mode="MarkdownV2")
    bot.register_next_step_handler(message, process_evidence)

def process_evidence(message):
    user_id = message.from_user.id
    if user_id not in user_submission_state or user_submission_state[user_id]['step'] != "awaiting_evidence":
        handle_all_other_messages(message)
        return

    if message.text and message.text.strip().lower() == DONE_SUBMISSION_COMMAND:
        finalize_submission(message)
        return

    user_submission_state[user_id]['evidence'].append(message)
    bot.register_next_step_handler(message, process_evidence)

def finalize_submission(message):
    user_id = message.from_user.id

    # 防止并发 /done 导致重复提交/状态竞争
    with submission_state_lock:
        submission_data = user_submission_state.get(user_id)
        if not submission_data:
            return
        if submission_data.get('_finalizing'):
            return
        submission_data['_finalizing'] = True
        submission_data['step'] = "finalizing"

    # 至少需要一条证据
    if not submission_data.get('evidence'):
        bot.reply_to(
            message,
            escape_markdown("⚠️ 您没有提交任何证据。投稿至少需要一条证据。请发送证据后再发送 /done。"),
            parse_mode="MarkdownV2",
        )
        with submission_state_lock:
            # 允许继续提交证据
            cur = user_submission_state.get(user_id)
            if cur is not None:
                cur['_finalizing'] = False
                cur['step'] = "awaiting_evidence"
        bot.register_next_step_handler(message, process_evidence)
        return

    submission_id = str(uuid.uuid4())

    serialized_data = [serialize_message(msg) for msg in submission_data['evidence']]

    pending_submission = {
        "submitter_id": user_id,
        "submitter_username": message.from_user.username,
        "submission_time": time.time(),
        "user_ids": submission_data.get('ids', []),
        "usernames": submission_data.get('usernames', []),
        "evidence_messages": [
            {"chat_id": msg.chat.id, "message_id": msg.message_id} for msg in submission_data['evidence']
        ],
        "evidence_data": [ev for ev in serialized_data if ev],
    }

    reports = load_reports()
    reports['pending'][submission_id] = pending_submission
    save_reports(reports)

    # 清理状态（pop 避免 KeyError）
    with submission_state_lock:
        user_submission_state.pop(user_id, None)

    bot.reply_to(
        message,
        escape_markdown("✅ 您的投稿已成功提交，正在等待管理员审核。感谢您的贡献！"),
        parse_mode="MarkdownV2",
    )

    ids_str = ", ".join(submission_data.get('ids', [])) or "无"
    users_str = ", ".join([f"@{u}" for u in submission_data.get('usernames', [])]) or "无"
    submitter_name = (message.from_user.first_name or "") + (
        " " + (message.from_user.last_name or "") if message.from_user.last_name else ""
    )

    submitter_name_trunc = truncate_for_link_text(submitter_name or '用户')
    sanitized_submitter_name = _sanitize_for_link_text(submitter_name_trunc)
    escaped_submitter_name = escape_markdown(sanitized_submitter_name)

    admin_text = (
        f"📢 *{escape_markdown('新的诈骗者投稿待审核')}*\n\n"
        f"› *{escape_markdown('投稿人:')}* [{escaped_submitter_name}](tg://user?id={user_id}) \| `{user_id}`\n"
        f"› *{escape_markdown('诈骗犯ID:')}* `{escape_for_code(ids_str)}`\n"
        f"› *{escape_markdown('诈骗犯用户:')}* `{escape_for_code(users_str)}`\n\n"
        f"*{escape_markdown('提交的证据如下:')}*"
    )

    try:
        bot.send_message(CONFIG["ADMIN_ID"], admin_text, parse_mode="MarkdownV2")
        for msg in submission_data['evidence']:
            bot.forward_message(CONFIG["ADMIN_ID"], msg.chat.id, msg.message_id)

        markup = types.InlineKeyboardMarkup()
        approve_btn = types.InlineKeyboardButton("✅ 批准", callback_data=f"approve_sub:{submission_id}")
        reject_btn = types.InlineKeyboardButton("❌ 拒绝", callback_data=f"reject_sub:{submission_id}")
        markup.add(approve_btn, reject_btn)
        bot.send_message(CONFIG["ADMIN_ID"], "请审核以上投稿：", reply_markup=markup, parse_mode=None)
    except Exception as e:
        print(f"💥 发送投稿给管理员失败: {e}")

@bot.callback_query_handler(func=lambda call: call.data.startswith('approve_sub:') or call.data.startswith('reject_sub:'))
def handle_submission_review(call):
    if call.from_user.id != CONFIG["ADMIN_ID"]:
        bot.answer_callback_query(call.id, "无权限操作")
        return

    action, submission_id = call.data.split(':', 1)
    reports = load_reports()
    
    if submission_id not in reports['pending']:
        bot.answer_callback_query(call.id, "此投稿已被处理或不存在。")
        try:
            bot.edit_message_text(
                "此投稿已被处理。",
                call.message.chat.id,
                call.message.message_id,
                reply_markup=None,
                parse_mode=None,
            )
        except ApiTelegramException as e:
            # Telegram 会在内容/按钮完全一致时返回 400: message is not modified
            if "message is not modified" not in str(e):
                raise

        return

    submission = reports['pending'][submission_id]
    submitter_id = submission['submitter_id']

    if action == 'approve_sub':
        all_ids = submission.get('user_ids', [])
        all_users = submission.get('usernames', [])
        
        if not all_ids and not all_users:
            bot.edit_message_text("❌ 批准失败：投稿缺少用户ID和用户名。", call.message.chat.id, call.message.message_id, parse_mode=None)
            bot.answer_callback_query(call.id, "批准失败")
            return

        primary_key = str(all_ids[0]) if all_ids else all_users[0].lower()
        
        verified_entry = {
            "user_ids": all_ids, "usernames": all_users,
            "evidence_messages": submission.get('evidence_messages', []),
            "evidence_data": submission.get('evidence_data', []),
            "submitter_id": submitter_id,
            "approval_time": time.time(), "approved_by": call.from_user.id
        }
        reports['verified'][primary_key] = verified_entry
        del reports['pending'][submission_id]
        save_reports(reports)
        
        edit_text = f"✅ {escape_markdown('已批准投稿 ')}\`{escape_for_code(primary_key)}\`{escape_markdown('。')}"
        bot.edit_message_text(edit_text, call.message.chat.id, call.message.message_id, reply_markup=None, parse_mode="MarkdownV2")
        bot.answer_callback_query(call.id, "已批准")
        try:
            notify_text = f"🎉 *{escape_markdown('投稿已批准')}*\n{escape_markdown('好消息！您提交的关于')} `{escape_for_code(primary_key)}` {escape_markdown('的投稿已被管理员批准。感谢您的贡献！')}"
            bot.send_message(submitter_id, notify_text, parse_mode="MarkdownV2")
        except Exception as e:
            print(f"通知用户 {submitter_id} 批准失败: {e}")

    elif action == 'reject_sub':
        bot.answer_callback_query(call.id)
        prompt_text = (
            f"❗️ *{escape_markdown('您已选择拒绝此投稿。')}*\n\n"
            f"*{escape_markdown('请直接回复此消息，输入拒绝的理由。')}*\n"
            f"*{escape_markdown('如果不想提供理由，请发送')}* `/skip`"
        )
        markup = types.ForceReply(selective=False, input_field_placeholder="输入拒绝理由...")
        prompt_message = bot.send_message(call.message.chat.id, prompt_text, reply_markup=markup, parse_mode="MarkdownV2")
        
        bot.register_next_step_handler(prompt_message, process_rejection_reason, submission_id, call.message.message_id)
        
        try:
            bot.edit_message_text("⏳ 等待输入拒绝理由...", call.message.chat.id, call.message.message_id, reply_markup=None, parse_mode="MarkdownV2")
        except Exception:
            pass

def process_rejection_reason(message, submission_id, original_review_message_id):
    reason = message.text.strip()
    reports = load_reports()
    
    if submission_id not in reports['pending']:
        bot.reply_to(message, "此投稿似乎已被其他操作处理。", parse_mode=None)
        return

    submission = reports['pending'][submission_id]
    submitter_id = submission['submitter_id']

    del reports['pending'][submission_id]
    save_reports(reports)

    rejection_text_for_admin = f"❌ {escape_markdown('已拒绝投稿 | ID: ')}`{escape_for_code(f'{submission_id[:8]}...')}`"
    user_notification = f"很遗憾，您提交的投稿已被管理员拒绝。"
    
    if reason.lower() != '/skip':
        rejection_text_for_admin += f"\n*{escape_markdown('原因:')}* {escape_markdown(reason)}"
        user_notification += f"\n\n*{escape_markdown('管理员留言:')}* {escape_markdown(reason)}"
        
    bot.reply_to(message, "操作完成。", parse_mode=None)
    
    try:
        bot.edit_message_text(
            rejection_text_for_admin,
            message.chat.id,
            original_review_message_id,
            parse_mode="MarkdownV2"
        )
    except Exception as e:
        print(f"编辑最终拒绝消息失败: {e}")
        
    try:
        bot.send_message(submitter_id, user_notification, parse_mode="MarkdownV2")
    except Exception as e:
        print(f"通知用户 {submitter_id} 拒绝失败: {e}")

# ====================================================================
# START OF BUSINESS LEDGER LOGIC
# ====================================================================

def get_or_create_ledger(user_id: int, contact_id: int):
    with db_lock:
        conn = get_db_connection()
        c = conn.cursor()
        c.execute("SELECT * FROM business_ledgers WHERE user_id = ? AND contact_id = ?", (user_id, contact_id))
        ledger = c.fetchone()
        if not ledger:
            c.execute("SELECT currency, auto_pin FROM business_ledgers WHERE user_id = ? AND contact_id = 0", (user_id,))
            global_settings = c.fetchone()
            
            if not global_settings:
                c.execute("INSERT INTO business_ledgers (user_id, contact_id) VALUES (?, 0)", (user_id,))
                conn.commit()
                global_currency = '$'
                global_auto_pin = 0
            else:
                global_currency = global_settings['currency']
                global_auto_pin = global_settings['auto_pin']

            c.execute("INSERT INTO business_ledgers (user_id, contact_id, currency, auto_pin) VALUES (?, ?, ?, ?)", (user_id, contact_id, global_currency, global_auto_pin))
            conn.commit()
            c.execute("SELECT * FROM business_ledgers WHERE user_id = ? AND contact_id = ?", (user_id, contact_id))
            ledger = c.fetchone()
        conn.close()
    return ledger

def handle_ledger_command(message: types.Message):
    user_id = message.from_user.id
    contact_id = message.chat.id
    text = message.text.strip()
    
    ledger = get_or_create_ledger(user_id, contact_id)
    currency = ledger['currency']
    
    if text.startswith(('+', '-')):
        match = re.match(r'([+\-])\s*([\d,.]+)\s*(.*)', text, re.DOTALL)
        if not match: return 
        
        sign, amount_str, description_text = match.groups()
        
        description = None
        if message.reply_to_message and (message.reply_to_message.text or message.reply_to_message.caption):
             description = (message.reply_to_message.text or message.reply_to_message.caption).strip()
        else:
             description = description_text.strip() or None
        
        try:
            amount = float(amount_str.replace(',', ''))
            if amount < 0: return
        except ValueError: return
            
        if sign == '-': amount = -amount

        with db_lock:
            conn = get_db_connection()
            c = conn.cursor()
            c.execute("UPDATE business_ledgers SET balance = balance + ? WHERE id = ?", (amount, ledger['id']))
            conn.commit()
            
            c.execute("SELECT balance FROM business_ledgers WHERE id = ?", (ledger['id'],))
            new_balance = c.fetchone()['balance']
            
            c.execute(
                "INSERT INTO ledger_history (ledger_id, timestamp, amount, new_balance, description) VALUES (?, ?, ?, ?, ?)",
                (ledger['id'], int(time.time()), amount, new_balance, description)
            )
            conn.commit()
            conn.close()

        action_text = "入金" if amount > 0 else "出金"
        response_text = f"{action_text} {currency} {abs(amount):.2f}, 剩余 {currency} {new_balance:.2f}"
        
        if description:
            response_text += f"\n备注: {description}"

        sent_message = bot.send_message(
            chat_id=contact_id,
            text=response_text,
            business_connection_id=message.business_connection_id,
            reply_to_message_id=message.message_id,
            parse_mode=None
        )

        if ledger['auto_pin']:
            try:
                bot.pin_chat_message(
                    chat_id=contact_id,
                    message_id=sent_message.message_id,
                    disable_notification=True,
                    business_connection_id=message.business_connection_id
                )
            except ApiTelegramException as e:
                print(f"💥 [Ledger-AutoPin] Failed to auto-pin message: {e.description}")

    elif text == '//':
        with db_lock:
            conn = get_db_connection()
            c = conn.cursor()
            c.execute("SELECT balance FROM business_ledgers WHERE id = ?", (ledger['id'],))
            old_balance = c.fetchone()['balance']
            
            c.execute("UPDATE business_ledgers SET balance = 0.0 WHERE id = ?", (ledger['id'],))
            c.execute(
                "INSERT INTO ledger_history (ledger_id, timestamp, amount, new_balance, description) VALUES (?, ?, ?, ?, ?)",
                (ledger['id'], int(time.time()), -old_balance, 0.0, "清账成功")
            )
            conn.commit()
            conn.close()

        response_text = f"清账成功, 剩余 {currency} 0.00"
        bot.send_message(
            chat_id=contact_id,
            text=response_text,
            business_connection_id=message.business_connection_id,
            reply_to_message_id=message.message_id,
            parse_mode=None
        )

    elif text.lower().startswith('/l'):
        parts = text.lower().replace('/l', '').strip().split(':')
        limit = 5
        offset = 0
        try:
            if len(parts) == 1 and parts[0]: limit = int(parts[0])
            elif len(parts) == 2:
                limit = int(parts[0]) if parts[0] else 5
                offset = int(parts[1]) if parts[1] else 0
        except (ValueError, IndexError): pass

        with db_lock:
            conn = get_db_connection()
            c = conn.cursor()
            c.execute(
                "SELECT * FROM ledger_history WHERE ledger_id = ? ORDER BY timestamp DESC LIMIT ? OFFSET ?",
                (ledger['id'], limit, offset)
            )
            history_rows = c.fetchall()
            conn.close()
        
        if not history_rows:
            bot.send_message(contact_id, "没有找到记账记录。", business_connection_id=message.business_connection_id, parse_mode=None)
            return

        history_text_parts = [f"最近 {len(history_rows)} 条记录:"]
        for row in history_rows:
            date_str = datetime.fromtimestamp(row['timestamp'], tz=CHINA_TZ).strftime('%m-%d %H:%M')
            action = "入金" if row['amount'] > 0 else "出金"
            desc_part = f" \\- {escape_markdown(row['description'] or '')}" if row['description'] else ""
            entry = f"`{escape_for_code(date_str)}` {action} `{currency} {abs(row['amount']):.2f}`{desc_part}\n  *剩余:* `{currency} {row['new_balance']:.2f}`"
            history_text_parts.append(entry)
            
        bot.send_message(
            contact_id,
            "\n".join(history_text_parts),
            business_connection_id=message.business_connection_id,
            parse_mode="MarkdownV2"
        )

    elif text.lower() in ['/$', '/¥', '/usdt', '/default']:
        new_currency = '$'
        if text.lower() == '/¥': new_currency = '¥'
        if text.lower() == '/usdt': new_currency = 'USDT'
        
        with db_lock:
            conn = get_db_connection()
            c = conn.cursor()
            c.execute("UPDATE business_ledgers SET currency = ? WHERE id = ?", (new_currency, ledger['id']))
            conn.commit()
            conn.close()
        
        bot.send_message(
            chat_id=contact_id,
            text=f"此对话货币单位已切换为: {new_currency}",
            business_connection_id=message.business_connection_id,
            parse_mode=None
        )

@bot.message_handler(commands=['jz'])
@check_membership
@premium_only
def show_ledger_settings_command(message: types.Message):
    show_ledger_settings(message)

def show_ledger_settings(message_or_call):
    is_call = isinstance(message_or_call, types.CallbackQuery)
    message = message_or_call.message if is_call else message_or_call
    user_id = message_or_call.from_user.id
    chat_id = message.chat.id
    message_id = message.message_id
    
    if chat_id < 0 and not is_call:
        bot.reply_to(message, "请在与我的私聊中使用此命令打开设置面板。", parse_mode=None)
        return

    ledger = get_or_create_ledger(user_id, 0)
    
    settings_text = (
        f"*{escape_markdown('您正在设置记录账本')}*\n\n"
        f"> 📝 *{escape_markdown('命令说明')}*\n"
        f"> `{escape_for_code('±金额[备注]')}`\n"
        f"> {escape_markdown('金额前使用’+’表示入金,’-’表示出金;如需备注,请在金额后加一个空格并附上说明文字。')}\n"
        f"> \n"
        f"> `{escape_for_code('/l[显示条数]:[起始位置]')}`\n"
        f"> {escape_markdown('默认不加数字时显示最近5条记录;可通过指定数字调整显示的条数和起始位置,例如/l10:3表示从第3条记录之后开始,显示10条记录')}\n"
        f"> \n"
        f"> `{escape_for_code('//')}`\n"
        f"> {escape_markdown('金额归零。')}\n"
        f"> \n"
        f"> `{escape_for_code('/货币[*]')}`\n"
        f"> {escape_markdown('切换对话中的货币单位,支持的选项包括:/usdt、/$、/¥,使用/default可恢复默认货币单位。')}\n\n"
        f"💴 *{escape_markdown('货币单位')}*\n"
        f"> `{escape_for_code(ledger['currency'])}`\n\n"
        f"⚙️ *{escape_markdown('高级选项')}*\n"
        f"> {'🟢' if ledger['auto_pin'] else '🔴'} {escape_markdown('自动置顶')}\n"
        f"> {escape_markdown('开启后, 出入金时会自动置顶消息。')}"
    )

    markup = types.InlineKeyboardMarkup(row_width=3)
    
    auto_pin_status_text = f"{'🟢 已开启' if ledger['auto_pin'] else '🔴 已关闭'}"
    markup.add(
        types.InlineKeyboardButton(auto_pin_status_text, callback_data="ledger_set:toggle_pin"),
        types.InlineKeyboardButton("自动置顶", callback_data="ledger_set:toggle_pin")
    )
    
    currency_btns = []
    for c in ['$', '¥', 'USDT']:
        text = f"⦿ {c}" if ledger['currency'] == c else c
        currency_btns.append(types.InlineKeyboardButton(text, callback_data=f"ledger_set:currency:{c}"))

    markup.row(*currency_btns)
    
    markup.add(types.InlineKeyboardButton("↩️ 返回", callback_data="premium:main"))
    
    if is_call:
        try:
            bot.edit_message_text(settings_text, chat_id, message_id, reply_markup=markup, parse_mode="MarkdownV2")
        except ApiTelegramException as e:
            if "message is not modified" not in str(e).lower(): print(f"Error editing ledger settings: {e}")
    else:
        bot.reply_to(message, settings_text, reply_markup=markup, parse_mode="MarkdownV2")


@bot.callback_query_handler(func=lambda call: call.data.startswith('ledger_set:'))
@check_membership
@premium_only
def handle_ledger_settings_callback(call: types.CallbackQuery):
    user_id = call.from_user.id
    parts = call.data.split(':')
    action = parts[1]
    
    bot.answer_callback_query(call.id)

    if action == "dummy":
        return

    ledger = get_or_create_ledger(user_id, 0)
    with db_lock:
        conn = get_db_connection()
        c = conn.cursor()
        if action == "toggle_pin":
            new_status = not ledger['auto_pin']
            c.execute("UPDATE business_ledgers SET auto_pin = ? WHERE user_id = ?", (new_status, user_id))
        elif action == "currency":
            new_currency = parts[2]
            c.execute("UPDATE business_ledgers SET currency = ? WHERE user_id = ?", (new_currency, user_id))
        conn.commit()
        conn.close()

    show_ledger_settings(call)

# ====================================================================
# END OF BUSINESS LEDGER LOGIC
# ====================================================================

# ====================================================================
# START OF LEDGER ANALYSIS AND STATS LOGIC
# ====================================================================

def query_ledger_stats(user_id: int, contact_id: int = None, start_time: int = None):
    stats = {'income': 0.0, 'outcome': 0.0, 'count': 0}
    
    with db_lock:
        conn = get_db_connection()
        c = conn.cursor()
        
        base_query = """
            SELECT 
                SUM(CASE WHEN lh.amount > 0 THEN lh.amount ELSE 0 END) as income,
                SUM(CASE WHEN lh.amount < 0 THEN lh.amount ELSE 0 END) as outcome,
                COUNT(lh.id) as count
            FROM ledger_history lh
            JOIN business_ledgers bl ON lh.ledger_id = bl.id
            WHERE bl.user_id = ?
        """
        params = [user_id]

        if contact_id is not None:
            base_query += " AND bl.contact_id = ?"
            params.append(contact_id)
        else:
            base_query += " AND bl.contact_id != 0"

        if start_time is not None:
            base_query += " AND lh.timestamp >= ?"
            params.append(start_time)

        c.execute(base_query, tuple(params))
        result = c.fetchone()
        conn.close()

        if result and result['count'] > 0:
            stats['income'] = result['income'] or 0.0
            stats['outcome'] = abs(result['outcome'] or 0.0)
            stats['count'] = result['count']

    return stats

def show_ledger_stats_menu(call: types.CallbackQuery, page=0):
    user_id = call.from_user.id
    chat_id = call.message.chat.id
    message_id = call.message.message_id
    
    overall_stats = query_ledger_stats(user_id)
    net = overall_stats['income'] - overall_stats['outcome']
    
    text_parts = [
        f"📊 *{escape_markdown('账本统计总览')}*",
        f"*{'─'*20}*",
        f"🟢 *{escape_markdown('总收入:')}* `{overall_stats['income']:.2f}`",
        f"🔴 *{escape_markdown('总支出:')}* `{overall_stats['outcome']:.2f}`",
        f"🔵 *{escape_markdown('净利润:')}* `{'%.2f' % net}`",
        f"🔄 *{escape_markdown('总笔数:')}* `{overall_stats['count']}`",
        f"*{'─'*20}*",
        f"{escape_markdown('以下是按聊天伙伴的独立统计:')}"
    ]
    
    markup = types.InlineKeyboardMarkup(row_width=1)
    
    with db_lock:
        conn = get_db_connection()
        c = conn.cursor()
        c.execute("SELECT contact_id, balance, currency FROM business_ledgers WHERE user_id = ? AND contact_id != 0 ORDER BY id DESC", (user_id,))
        ledgers = c.fetchall()
        conn.close()

    if not ledgers:
        text_parts.append(f"\n_{escape_markdown('暂无与任何人的独立账本记录。')}_")
    
    page_size = 5
    start = page * page_size
    end = start + page_size
    
    for ledger in ledgers[start:end]:
        contact_name = _get_user_name_from_db(ledger['contact_id'])
        sanitized_name = escape_markdown(truncate_for_link_text(contact_name, 30))
        btn_text = f"{sanitized_name} ({ledger['currency']} {ledger['balance']:.2f})"
        markup.add(types.InlineKeyboardButton(btn_text, callback_data=f"ledger_stats_contact:{ledger['contact_id']}"))

    nav_buttons = []
    if page > 0:
        nav_buttons.append(types.InlineKeyboardButton("⬅️ 上一页", callback_data=f"ledger_stats_page:{page-1}"))
    if end < len(ledgers):
        nav_buttons.append(types.InlineKeyboardButton("下一页 ➡️", callback_data=f"ledger_stats_page:{page+1}"))
    if nav_buttons:
        markup.row(*nav_buttons)

    markup.add(types.InlineKeyboardButton("🔙 返回高级功能", callback_data="premium:main"))
    
    try:
        bot.edit_message_text("\n".join(text_parts), chat_id, message_id, reply_markup=markup, parse_mode="MarkdownV2")
    except ApiTelegramException as e:
        if "message is not modified" not in str(e).lower():
            print(f"Error in show_ledger_stats_menu: {e}")

def show_ledger_analysis_menu(call: types.CallbackQuery):
    user_id = call.from_user.id
    chat_id = call.message.chat.id
    message_id = call.message.message_id
    
    now = datetime.now(CHINA_TZ)
    
    seven_days_ago = int((now - timedelta(days=7)).timestamp())
    stats_7d = query_ledger_stats(user_id, start_time=seven_days_ago)
    
    thirty_days_ago = int((now - timedelta(days=30)).timestamp())
    stats_30d = query_ledger_stats(user_id, start_time=thirty_days_ago)
    
    start_of_month = int(now.replace(day=1, hour=0, minute=0, second=0, microsecond=0).timestamp())
    stats_month = query_ledger_stats(user_id, start_time=start_of_month)

    text = (
        f"🔍 *{escape_markdown('账本数据分析(所有聊天汇总)')}*\n\n"
        f"*{escape_markdown('近7日:')}*\n"
        f"> 🟢 *{escape_markdown('收入:')}* `{stats_7d['income']:.2f}`\n"
        f"> 🔴 *{escape_markdown('支出:')}* `{stats_7d['outcome']:.2f}`\n"
        f"> 🔄 *{escape_markdown('笔数:')}* `{stats_7d['count']}`\n\n"
        f"*{escape_markdown('近30日:')}*\n"
        f"> 🟢 *{escape_markdown('收入:')}* `{stats_30d['income']:.2f}`\n"
        f"> 🔴 *{escape_markdown('支出:')}* `{stats_30d['outcome']:.2f}`\n"
        f"> 🔄 *{escape_markdown('笔数:')}* `{stats_30d['count']}`\n\n"
        f"*{escape_markdown('本月 (' + str(now.month) + '月):')}*\n"
        f"> 🟢 *{escape_markdown('收入:')}* `{stats_month['income']:.2f}`\n"
        f"> 🔴 *{escape_markdown('支出:')}* `{stats_month['outcome']:.2f}`\n"
        f"> 🔄 *{escape_markdown('笔数:')}* `{stats_month['count']}`"
    )

    markup = types.InlineKeyboardMarkup()
    markup.add(types.InlineKeyboardButton("🔄 刷新数据", callback_data="premium:analyze"))
    markup.add(types.InlineKeyboardButton("🔙 返回高级功能", callback_data="premium:main"))
    
    try:
        bot.edit_message_text(text, chat_id, message_id, reply_markup=markup, parse_mode="MarkdownV2")
    except ApiTelegramException as e:
        if "message is not modified" not in str(e).lower():
            print(f"Error in show_ledger_analysis_menu: {e}")

@bot.callback_query_handler(func=lambda call: call.data.startswith('ledger_stats_'))
@check_membership
@premium_only
def handle_ledger_stats_callbacks(call: types.CallbackQuery):
    user_id = call.from_user.id
    action, payload = call.data.split(':', 1)

    if action == "ledger_stats_page":
        page = int(payload)
        show_ledger_stats_menu(call, page=page)
    elif action == "ledger_stats_contact":
        contact_id = int(payload)
        stats = query_ledger_stats(user_id, contact_id)
        contact_name = _get_user_name_from_db(contact_id)
        
        net = stats['income'] - stats['outcome']
        
        text = (
            f"📊 *{escape_markdown('与 ' + truncate_for_link_text(contact_name) + ' 的账本统计')}*\n"
            f"*{'─'*20}*\n"
            f"🟢 *{escape_markdown('总收入:')}* `{stats['income']:.2f}`\n"
            f"🔴 *{escape_markdown('总支出:')}* `{stats['outcome']:.2f}`\n"
            f"🔵 *{escape_markdown('净利润:')}* `{'%.2f' % net}`\n"
            f"🔄 *{escape_markdown('总笔数:')}* `{stats['count']}`"
        )
        markup = types.InlineKeyboardMarkup()
        markup.add(types.InlineKeyboardButton("🔙 返回统计列表", callback_data="premium:stats"))
        bot.edit_message_text(text, call.message.chat.id, call.message.message_id, reply_markup=markup, parse_mode="MarkdownV2")

# ====================================================================
# END OF LEDGER ANALYSIS AND STATS LOGIC
# ====================================================================

# ---------------------- Business 业务连接与消息处理 ----------------------

def perform_background_scam_check(business_connection_id: str, chat_id: int, business_user_id: int, contact_user: types.User):
    if not telethon_loop or not client.is_connected() or not contact_user or contact_user.is_bot:
        return

    contact_id = contact_user.id
    now = int(time.time())

    with db_lock:
        conn = get_db_connection()
        c = conn.cursor()
        c.execute(
            "SELECT last_checked FROM checked_contacts WHERE user_id = ? AND contact_id = ?",
            (business_user_id, contact_id)
        )
        last_check = c.fetchone()
        conn.close()

    if last_check and (now - last_check['last_checked'] < CONFIG.get("BUSINESS_SCAN_COOLDOWN", 86400)):
        return

    print(f"🕵️ [Business-Scan] 开始为用户 {business_user_id} 自动检测联系人 {contact_id} (@{contact_user.username})...")

    is_scammer = False
    warning_reason = ""
    evidence_data_to_send = []

    reports = load_reports()
    verified_report = reports.get('verified', {}).get(str(contact_id))
    if not verified_report and contact_user.username:
        for record in reports.get('verified', {}).values():
            if record.get('usernames') and contact_user.username.lower() in [u.lower() for u in record.get('usernames', [])]:
                verified_report = record
                break
    
    if verified_report:
        is_scammer = True
        warning_reason = "此联系人已被【官方验证】为诈骗者。"
        
        evidence_data_from_report = verified_report.get('evidence_data', [])
        evidence_data_to_send = evidence_data_from_report if isinstance(evidence_data_from_report, list) else [evidence_data_from_report]


    scam_channel_hits = []
    if not is_scammer:
        try:
            future = asyncio.run_coroutine_threadsafe(search_monitored_channels_for_user(user_id=contact_id), telethon_loop)
            scam_channel_hits = future.result(timeout=CONFIG.get("SCAM_CHANNEL_SEARCH_TIMEOUT", 40))
        except Exception as e:
            print(f"💥 [Business-Scan] 自动检测时搜索频道失败 (联系人: {contact_id}): {type(e).__name__} - {e}")

        if scam_channel_hits:
            is_scammer = True
            reason_parts = ["此联系人在【反诈频道】中有以下曝光记录："]
            links = [
                f"› [{escape_markdown(truncate_for_link_text(hit['chat_title']))}]({_escape_url_md(hit['link'])})"
                for hit in scam_channel_hits[:3]
            ]
            reason_parts.extend(links)
            warning_reason = "\n".join(reason_parts)

    if is_scammer:
        contact_name = (contact_user.first_name or "") + (" " + (contact_user.last_name or "") if contact_user.last_name else "")
        contact_name = contact_name.strip() or f"User ID {contact_id}"
        
        username_mention = f"@{escape_markdown(contact_user.username)}" if contact_user.username else 'N/A'

        warning_message_md = (
            f"🚨 *{escape_markdown('安全警报 (自动检测)')}* 🚨\n\n"
            f"{escape_markdown('联系人')} *{escape_markdown(contact_name)}* "
            f"\\({username_mention} \\| `{contact_id}`\\) "
            f"{escape_markdown('存在高风险记录。')}\n\n"
            f"*{escape_markdown('原因:')}* {warning_reason}\n\n"
            f"*{escape_markdown('请谨慎交易，注意防范风险。')}*"
        )
        try:
            bot.send_message(
                chat_id=chat_id,
                text=warning_message_md,
                business_connection_id=business_connection_id,
                parse_mode="MarkdownV2",
                disable_web_page_preview=True
            )

            if evidence_data_to_send:
                for evidence in evidence_data_to_send:
                    if evidence:
                        send_serialized_message(chat_id, evidence, business_connection_id=business_connection_id)
                        time.sleep(0.5)
            
            print(f"✅ [Business-Scan] 成功在聊天 {chat_id} 中发送关于 {contact_id} 的警告和证据。")

        except Exception as e:
            print(f"💥 [Business-Scan] 无法在聊天 {chat_id} 中发送主警告: {e}")
            try:
                fallback_text = f"⚠️ *{escape_markdown('自动安全检测失败')}*\n\n{escape_markdown('尝试在与联系人的聊天中发送警告时出错，请在私聊中查看此警告。')}\n\n" + warning_message_md
                bot.send_message(business_user_id, fallback_text, parse_mode="MarkdownV2", disable_web_page_preview=True)
            except Exception as e2:
                print(f"💥 [Business-Scan] 回退私聊警告也发送失败 (用户: {business_user_id}): {e2}")

    with db_lock:
        conn = get_db_connection()
        c = conn.cursor()
        c.execute('''
            INSERT INTO checked_contacts (user_id, contact_id, last_checked, is_scammer) VALUES (?, ?, ?, ?)
            ON CONFLICT(user_id, contact_id) DO UPDATE SET
                last_checked = excluded.last_checked,
                is_scammer = excluded.is_scammer
        ''', (business_user_id, contact_id, now, is_scammer))
        conn.commit()
        conn.close()

@bot.business_connection_handler(func=lambda conn: True)
def handle_business_connection(connection: types.BusinessConnection):
    user_id = connection.user_chat_id
    conn_id = connection.id
    is_enabled = connection.is_enabled
    now = int(time.time())

    with db_lock:
        conn = get_db_connection()
        c = conn.cursor()
        c.execute('''
            INSERT INTO business_connections (connection_id, user_id, is_enabled, last_updated) VALUES (?, ?, ?, ?)
            ON CONFLICT(connection_id) DO UPDATE SET
                is_enabled = excluded.is_enabled,
                last_updated = excluded.last_updated
        ''', (conn_id, user_id, is_enabled, now))
        conn.commit()
        conn.close()

    status_text = "启用" if is_enabled else "禁用"
    log_msg = f"🤝 *{escape_markdown('业务连接更新')}*\n" \
              f"› *{escape_markdown('用户:')}* [{escape_markdown(str(user_id))}](tg://user?id={user_id})\n" \
              f"› *{escape_markdown('状态:')}* {escape_markdown(status_text)}"
    send_log_to_channel(log_msg)
    
    if is_enabled:
        bot.send_message(user_id, f"✅ *{escape_markdown('连接成功！')}*\n{escape_markdown('猎诈卫士已成为您的私人安全助理。您现在可以前往')} {escape_markdown('/premium_features')} {escape_markdown('设置自动回复等高级功能。')}", parse_mode="MarkdownV2")
    else:
        bot.send_message(user_id, f"ℹ️ *{escape_markdown('连接已禁用')}*\n{escape_markdown('所有 Business 相关功能（如自动回复、记账）已暂停。')}", parse_mode="MarkdownV2")


@bot.business_message_handler(func=lambda msg: True)
def handle_business_message(message: types.Message):
    if not message.business_connection_id or not message.chat:
        return
        
    with db_lock:
        conn = get_db_connection()
        c = conn.cursor()
        c.execute("SELECT user_id FROM business_connections WHERE connection_id = ? AND is_enabled = 1", (message.business_connection_id,))
        res = c.fetchone()
        conn.close()

    if not res: return
    
    business_user_id = res['user_id']
    sender_is_business_user = message.from_user and message.from_user.id == business_user_id
    
    if sender_is_business_user and message.text:
        handle_ledger_command(message)
        return

    else:
        contact_user_object = message.from_user
        if not contact_user_object or contact_user_object.is_bot:
            return

        if message.text:
            with db_lock:
                conn = get_db_connection()
                c = conn.cursor()
                c.execute("SELECT keyword, reply_message_json FROM keyword_replies WHERE user_id = ?", (business_user_id,))
                keywords = c.fetchall()
                conn.close()
            
            for row in keywords:
                keyword = row['keyword']
                if keyword.lower() in message.text.lower():
                    print(f"✅ [Keyword-Reply] Triggered for user {business_user_id} by keyword '{keyword}'")
                    reply_data_from_db = json.loads(row['reply_message_json'])
                    reply_data_list = reply_data_from_db if isinstance(reply_data_from_db, list) else [reply_data_from_db]

                    for i, reply_data in enumerate(reply_data_list):
                        send_serialized_message(message.chat.id, reply_data, business_connection_id=message.business_connection_id)
                        if i < len(reply_data_list) - 1:
                            time.sleep(0.5)
                    return

        with db_lock:
            conn = get_db_connection()
            c = conn.cursor()
            c.execute("SELECT is_enabled, reply_message_json FROM offline_replies WHERE user_id = ?", (business_user_id,))
            offline_settings = c.fetchone()
            conn.close()
        
        if offline_settings and offline_settings['is_enabled'] and offline_settings['reply_message_json']:
            now = time.time()
            cooldown_key = (business_user_id, contact_user_object.id)
            last_sent_time = offline_reply_cooldown_cache.get(cooldown_key, 0)

            if now - last_sent_time > CONFIG.get("OFFLINE_REPLY_COOLDOWN", 300):
                try:
                    future = asyncio.run_coroutine_threadsafe(get_user_status_async(business_user_id), telethon_loop)
                    status = future.result(timeout=10)
                    
                    if status != 'online':
                        print(f"✅ [Offline-Reply] Triggered for user {business_user_id} (status: {status})")
                        reply_data_from_db = json.loads(offline_settings['reply_message_json'])
                        reply_data_list = reply_data_from_db if isinstance(reply_data_from_db, list) else [reply_data_from_db]

                        all_sent = True
                        for i, reply_data in enumerate(reply_data_list):
                            if not send_serialized_message(message.chat.id, reply_data, business_connection_id=message.business_connection_id):
                                all_sent = False
                                break
                            if i < len(reply_data_list) - 1:
                                time.sleep(0.5)

                        if all_sent:
                            offline_reply_cooldown_cache[cooldown_key] = now
                        return
                    else:
                        print(f"ℹ️ [Offline-Reply] Skipped for user {business_user_id}. Reason: User status is 'online'.")
                except (FuturesTimeoutError, Exception) as e:
                    print(f"⚠️ [Offline-Reply] Could not check status for user {business_user_id}: {e}")
            else:
                print(f"ℹ️ [Offline-Reply] Cooldown active for user {business_user_id} and contact {contact_user_object.id}. Skipping.")

        threading.Thread(
            target=perform_background_scam_check,
            args=(message.business_connection_id, message.chat.id, business_user_id, contact_user_object),
            daemon=True
        ).start()

# ---------------------- 高级功能回调与设置流程 ----------------------

@bot.callback_query_handler(func=lambda call: call.data.startswith(('premium:', 'offline:', 'keyword:')))
@check_membership
@premium_only
def handle_all_premium_callbacks(call: types.CallbackQuery):
    action_parts = call.data.split(':')
    menu = action_parts[0]
    action = action_parts[1] if len(action_parts) > 1 else 'main'
    user_id = call.from_user.id
    message = call.message
    chat_id = message.chat.id
    
    bot.answer_callback_query(call.id)

    if menu == 'premium':
        if action == 'main': handle_premium_main_menu(call)
        elif action == 'ledger': show_ledger_settings(call)
        elif action == 'offline_reply': handle_offline_reply_menu(call)
        elif action == 'keyword_reply': handle_keyword_reply_menu(call, is_main_menu=True)
        elif action == 'stats': show_ledger_stats_menu(call)
        elif action == 'analyze': show_ledger_analysis_menu(call)
    
    elif menu == 'offline':
        if action == 'toggle':
            with db_lock:
                conn = get_db_connection()
                c = conn.cursor()
                c.execute("SELECT is_enabled FROM offline_replies WHERE user_id = ?", (user_id,))
                res = c.fetchone()
                new_status = not (res['is_enabled'] if res else False)
                c.execute("INSERT INTO offline_replies (user_id, is_enabled, last_updated) VALUES (?, ?, ?) ON CONFLICT(user_id) DO UPDATE SET is_enabled=excluded.is_enabled, last_updated=excluded.last_updated",
                          (user_id, new_status, int(time.time())))
                conn.commit()
                conn.close()
            handle_offline_reply_menu(call, toggled=True)
        elif action == 'set':
            user_settings_state[user_id] = {'flow': 'awaiting_offline_reply', 'messages': []}
            prompt_text = (
                f"📲 *{escape_markdown('设置离线回复')}*\n\n"
                f"{escape_markdown('请发送您希望作为自动回复的一条或多条消息。')}\n\n"
                f"*{escape_markdown('发送完毕后，请输入')}* `{escape_markdown(DONE_SUBMISSION_COMMAND)}` *{escape_markdown('来完成设置。')}*"
            )
            bot.send_message(chat_id, prompt_text, parse_mode="MarkdownV2")
            bot.register_next_step_handler(message, process_settings_flow)
        elif action == 'view':
            with db_lock:
                conn = get_db_connection()
                c = conn.cursor()
                c.execute("SELECT reply_message_json FROM offline_replies WHERE user_id = ?", (user_id,))
                res = c.fetchone()
                conn.close()
            if res and res['reply_message_json']:
                bot.send_message(chat_id, f"👀 *{escape_markdown('您当前的离线回复预览如下:')}*", parse_mode="MarkdownV2")
                reply_data_from_db = json.loads(res['reply_message_json'])
                reply_list = reply_data_from_db if isinstance(reply_data_from_db, list) else [reply_data_from_db]
                for reply_data in reply_list:
                    send_serialized_message(chat_id, reply_data)
                    time.sleep(0.5)
            else:
                bot.send_message(chat_id, f"ℹ️ {escape_markdown('您尚未设置离线回复内容。')}", parse_mode="MarkdownV2")

    elif menu == 'keyword':
        if action == 'add':
            user_settings_state[user_id] = {'flow': 'awaiting_keyword_keyword'}
            prompt_text = f"⌨️ {escape_markdown('请输入您要设置的关键词（不区分大小写，一行一个）。')}"
            bot.send_message(chat_id, prompt_text, parse_mode="MarkdownV2")
            bot.register_next_step_handler(message, process_settings_flow)
        elif action == 'del':
            if len(action_parts) > 2:
                keyword_id_to_del = action_parts[2]
                with db_lock:
                    conn = get_db_connection()
                    c = conn.cursor()
                    c.execute("DELETE FROM keyword_replies WHERE id = ? AND user_id = ?", (keyword_id_to_del, user_id))
                    conn.commit()
                    conn.close()
                bot.answer_callback_query(call.id, "已删除")
                handle_keyword_reply_menu(call, is_main_menu=False)
        elif action == 'view':
            if len(action_parts) > 2:
                keyword_id_to_view = action_parts[2]
                with db_lock:
                    conn = get_db_connection()
                    c = conn.cursor()
                    c.execute("SELECT keyword, reply_message_json FROM keyword_replies WHERE id = ? AND user_id = ?", (keyword_id_to_view, user_id))
                    res = c.fetchone()
                    conn.close()
                if res and res['reply_message_json']:
                    bot.send_message(chat_id, f"👀 *{escape_markdown('关键词“' + res['keyword'] + '”的回复预览如下:')}*", parse_mode="MarkdownV2")
                    reply_data_from_db = json.loads(res['reply_message_json'])
                    reply_list = reply_data_from_db if isinstance(reply_data_from_db, list) else [reply_data_from_db]
                    for reply_data in reply_list:
                        send_serialized_message(chat_id, reply_data)
                        time.sleep(0.5)
                else:
                    bot.send_message(chat_id, f"ℹ️ {escape_markdown('找不到该关键词的回复内容。')}", parse_mode="MarkdownV2")

def handle_offline_reply_menu(call, toggled=False):
    user_id = call.from_user.id
    with db_lock:
        conn = get_db_connection()
        c = conn.cursor()
        c.execute("SELECT is_enabled FROM offline_replies WHERE user_id = ?", (user_id,))
        res = c.fetchone()
        conn.close()
    
    is_enabled = res['is_enabled'] if res else False
    status_text = "✅ 已启用" if is_enabled else "❌ 已禁用"
    toggle_text = "🌙 禁用" if is_enabled else "☀️ 启用"

    text = f"🌙 *{escape_markdown('离线自动回复管理')}*\n\n*{escape_markdown('当前状态:')}* {escape_markdown(status_text)}\n\n{escape_markdown('当您在Telegram上显示为离线或离开时，此功能会自动回复联系人发来的第一条消息。可以设置多条消息作为回复序列。')}"
    markup = types.InlineKeyboardMarkup(row_width=2)
    markup.add(
        types.InlineKeyboardButton(toggle_text, callback_data="offline:toggle"),
        types.InlineKeyboardButton("📝 设置回复内容", callback_data="offline:set"),
        types.InlineKeyboardButton("👀 查看当前回复", callback_data="offline:view"),
        types.InlineKeyboardButton("🔙 返回", callback_data="premium:main")
    )
    if toggled:
        bot.answer_callback_query(call.id, f"已{toggle_text.split(' ')[1]}")
    bot.edit_message_text(text, call.message.chat.id, call.message.message_id, reply_markup=markup, parse_mode="MarkdownV2")

def handle_keyword_reply_menu(call, is_main_menu=False, deleted=False):
    user_id = call.from_user.id
    chat_id = call.message.chat.id
    message_id = call.message.message_id
    
    with db_lock:
        conn = get_db_connection()
        c = conn.cursor()
        c.execute("SELECT id, keyword FROM keyword_replies WHERE user_id = ? ORDER BY keyword", (user_id,))
        keywords = c.fetchall()
        conn.close()
    
    text_parts = [f"📝 *{escape_markdown('关键词自动回复管理')}*"]
    if not keywords:
        text_parts.append(f"\n{escape_markdown('您还没有设置任何关键词。点击下方按钮添加。')}")
    else:
        text_parts.append(f"\n{escape_markdown('以下是您当前的关键词列表。您可以查看或删除它们。')}")

    markup = types.InlineKeyboardMarkup(row_width=2)
    if keywords:
        for row in keywords:
            markup.add(
                types.InlineKeyboardButton(f"👀 {row['keyword']}", callback_data=f"keyword:view:{row['id']}"),
                types.InlineKeyboardButton(f"🗑️ 删除", callback_data=f"keyword:del:{row['id']}")
            )
    
    markup.add(
        types.InlineKeyboardButton("➕ 添加新关键词", callback_data="keyword:add"),
        types.InlineKeyboardButton("🔙 返回", callback_data="premium:main")
    )
    
    try:
        if is_main_menu or deleted:
            if deleted:
                bot.answer_callback_query(call.id, "已删除")
            bot.edit_message_text("\n".join(text_parts), chat_id, message_id, reply_markup=markup, parse_mode="MarkdownV2")
        elif not is_main_menu and call.data.startswith("keyword:del"):
             bot.edit_message_text("\n".join(text_parts), chat_id, message_id, reply_markup=markup, parse_mode="MarkdownV2")

    except ApiTelegramException as e:
        if "message is not modified" not in str(e).lower():
             print(f"Error in handle_keyword_reply_menu: {e}")

def process_settings_flow(message: types.Message):
    user_id = message.from_user.id
    if user_id not in user_settings_state: return

    state = user_settings_state[user_id]
    flow = state['flow']

    if flow == 'awaiting_offline_reply':
        if message.text and message.text.strip() == DONE_SUBMISSION_COMMAND:
            if not state.get('messages'):
                bot.reply_to(message, f"⚠️ {escape_markdown('您还没有发送任何要作为回复的消息。请发送消息或取消。')}", parse_mode="MarkdownV2")
                bot.register_next_step_handler(message, process_settings_flow)
                return

            serialized_list = [msg for msg in [serialize_message(m) for m in state['messages']] if msg is not None]
            if not serialized_list:
                bot.reply_to(message, f"❌ {escape_markdown('设置失败，未能识别您发送的任何消息格式。')}", parse_mode="MarkdownV2")
                del user_settings_state[user_id]
                return

            with db_lock:
                conn = get_db_connection()
                c = conn.cursor()
                c.execute("INSERT INTO offline_replies (user_id, reply_message_json, last_updated) VALUES (?, ?, ?) ON CONFLICT(user_id) DO UPDATE SET reply_message_json=excluded.reply_message_json, last_updated=excluded.last_updated",
                          (user_id, json.dumps(serialized_list), int(time.time())))
                conn.commit()
                conn.close()
            
            bot.reply_to(message, f"✅ *{escape_markdown('离线回复已更新！')}*\n{escape_markdown('您可以在菜单中启用它。')}", parse_mode="MarkdownV2")
            del user_settings_state[user_id]
        else:
            state['messages'].append(message)
            bot.register_next_step_handler(message, process_settings_flow)

    elif flow == 'awaiting_keyword_keyword':
        keyword = message.text.strip()
        if not keyword or '\n' in keyword:
            bot.reply_to(message, f"❌ *{escape_markdown('关键词无效')}*\n{escape_markdown('关键词不能为空，且只能包含一行。请重新输入。')}", parse_mode="MarkdownV2")
            bot.register_next_step_handler(message, process_settings_flow)
            return
        
        state['flow'] = 'awaiting_keyword_reply'
        state['keyword'] = keyword
        state['messages'] = []
        prompt_text = (
            f"✅ *{escape_markdown('关键词已收到！')}*\n\n"
            f"{escape_markdown('现在，请发送您希望绑定到关键词“' + keyword + '”的一条或多条回复消息。')}\n\n"
            f"*{escape_markdown('发送完毕后，请输入')}* `{escape_markdown(DONE_SUBMISSION_COMMAND)}` *{escape_markdown('来完成。')}*"
        )
        bot.reply_to(message, prompt_text, parse_mode="MarkdownV2")
        bot.register_next_step_handler(message, process_settings_flow)
        
    elif flow == 'awaiting_keyword_reply':
        if message.text and message.text.strip() == DONE_SUBMISSION_COMMAND:
            if not state.get('messages'):
                bot.reply_to(message, f"⚠️ {escape_markdown('您还没有发送任何要作为回复的消息。请发送消息或取消。')}", parse_mode="MarkdownV2")
                bot.register_next_step_handler(message, process_settings_flow)
                return
            
            keyword = state['keyword']
            serialized_list = [msg for msg in [serialize_message(m) for m in state['messages']] if msg is not None]
            if not serialized_list:
                bot.reply_to(message, f"❌ {escape_markdown('设置失败，未能识别您发送的任何消息格式。')}", parse_mode="MarkdownV2")
                del user_settings_state[user_id]
                return

            with db_lock:
                conn = get_db_connection()
                c = conn.cursor()
                lower_keyword = keyword.lower()
                c.execute("DELETE FROM keyword_replies WHERE user_id = ? AND keyword = ?", (user_id, lower_keyword))
                c.execute("INSERT INTO keyword_replies (user_id, keyword, reply_message_json, last_updated) VALUES (?, ?, ?, ?)",
                          (user_id, lower_keyword, json.dumps(serialized_list), int(time.time())))
                conn.commit()
                conn.close()

            bot.reply_to(message, f"✅ *{escape_markdown('关键词回复设置成功！')}*\n*{escape_markdown('关键词:')}* `{escape_for_code(keyword)}`", parse_mode="MarkdownV2")
            del user_settings_state[user_id]
        else:
            state['messages'].append(message)
            bot.register_next_step_handler(message, process_settings_flow)

# ---------------------- General Callback & Fallback Handlers ----------------------
@bot.callback_query_handler(func=lambda call: call.data in ["query", "forward", "stats", "tougao", "start_menu", "sponsor", "leaderboard"])
@check_membership
def main_menu_callback_handler(call):
    update_active_user(call.from_user.id)
    chat_id = call.message.chat.id
    
    proxy_message = copy.copy(call.message)
    proxy_message.from_user = call.from_user

    if call.data == "start_menu":
        bot.answer_callback_query(call.id)
        handle_start(proxy_message, is_edit=True)
        return

    bot.answer_callback_query(call.id)
    prompts = {
        "query": f"⌨️ *{escape_markdown('请直接发送目标的【用户名】或【ID】')}*\n{escape_markdown('您也可以【转发】目标用户的消息来查询。')}",
        "forward": f"📤 *{escape_markdown('请转发目标用户的消息给我')}*"
    }
    
    if call.data in prompts:
        bot.send_message(chat_id, prompts[call.data], parse_mode="MarkdownV2")
    elif call.data == "stats":
        handle_stats(proxy_message)
    elif call.data == "tougao":
        handle_tougao(proxy_message)
    elif call.data == "sponsor":
        handle_sponsor(proxy_message)
    elif call.data == "leaderboard":
        handle_leaderboard(proxy_message)

    try:
        if call.data in ["query", "forward", "tougao", "sponsor"]:
             bot.edit_message_reply_markup(chat_id, call.message.message_id, reply_markup=None)
    except Exception: pass


@bot.message_handler(func=lambda m: m.chat.type == 'private' and m.text and m.text.strip().startswith(('+', '-', '//')) and not m.business_connection_id)
@check_membership
def handle_unconnected_ledger_command(message: types.Message):
    if getattr(message.from_user, 'is_premium', False):
        try:
            bot_username = bot.get_me().username
            part3_text = f"点击 `添加机器人` 并选择 `@{bot_username}`"
            guidance_text = (
                f"💎 *{escape_markdown('启用记账功能')}*\n\n"
                f"{escape_markdown('检测到您是尊贵的 Telegram Premium 用户！要使用记账功能，请先将我连接到您的商业版账户:')}\n\n"
                f"1\\. {escape_markdown('前往 `设置` > `Telegram Business`')}\n"
                f"2\\. {escape_markdown('选择 `聊天机器人`')}\n"
                f"3\\. {escape_markdown(part3_text)}\n\n"
                f"*{escape_markdown('连接成功后，您就可以在与他人的私聊中使用 `+`、`-` 等命令了。')}*"
            )
            bot.reply_to(message, guidance_text, parse_mode="MarkdownV2")
        except Exception as e:
            print(f"💥 Failed to send business connection guide: {e}")

@bot.message_handler(func=lambda message: True, content_types=ALL_CONTENT_TYPES)
@check_membership
def handle_all_other_messages(message):
    user_id = message.from_user.id
    if user_id in user_settings_state:
        process_settings_flow(message)
        return
    if user_id in user_submission_state and user_submission_state[user_id].get('step') == "awaiting_evidence":
        process_evidence(message)
        return
    if user_id in user_sponsorship_state:
        process_sponsor_amount(message)
        return
    if user_id == CONFIG['ADMIN_ID'] and user_id in admin_broadcast_state:
        return

    update_active_user(user_id)
    if message.chat.type == 'private' and message.text and message.text.startswith('/'):
        known_commands = [
            '/start', '/cxzbf', '/stats', '/admin', '/addchannel',
            '/crawl_on', '/crawl_off', '/crawl_status',
            '/removechannel', '/listchannels', '/tougao', '/delreport',
            DONE_SUBMISSION_COMMAND, '/broadcast', '/cancel_broadcast',
            '/premium_features', '/jz', '/sponsor', '/leaderboard',
            '/setinvite', '/invite_status', '/invite'
        ]
        if message.text.split()[0] not in known_commands:
            bot.reply_to(message, f"🤔 *{escape_markdown('无法识别的命令。')}*\n{escape_markdown('请使用')} /start {escape_markdown('查看可用命令。')}" + f"\n\n{ADVERTISEMENT_TEXT}", parse_mode="MarkdownV2")



def invite_autodetect_thread():
    """自动处理邀请：pending -> active；如被邀请人退频道，清空邀请人当前邀请进度并通知。"""
    logger.info("✅ 邀请自动检测线程已启动。")
    while True:
        try:
            db_path = CONFIG.get("DATABASE_FILE", "history.db")
            if not os.path.exists(db_path):
                time.sleep(8)
                continue

            # 1) pending 邀请：检测是否已入频道，入群后计为成功
            with db_lock:
                conn = get_db_connection()
                c = conn.cursor()
                c.execute(
                    "SELECT inviter_id, invited_user_id FROM user_invites WHERE status = 'pending' ORDER BY invited_at ASC LIMIT 200"
                )
                pending_rows = c.fetchall() or []
                conn.close()

            for row in pending_rows:
                inviter_id = int(row["inviter_id"])
                invited_user_id = int(row["invited_user_id"])
                member_status = get_required_channel_membership_status(invited_user_id)
                if member_status is not True:
                    continue

                try:
                    with db_lock:
                        conn = get_db_connection()
                        c = conn.cursor()
                        c.execute(
                            "UPDATE user_invites SET status = 'active', left_at = NULL WHERE inviter_id = ? AND invited_user_id = ? AND status = 'pending'",
                            (inviter_id, invited_user_id),
                        )
                        changed = c.rowcount
                        conn.commit()
                        conn.close()

                    if changed:
                        active, _ = get_user_invite_stats(inviter_id)
                        required = get_invite_required_count()
                        if user_can_use_bot(inviter_id):
                            msg = (
                                f"🎉 *{escape_markdown('邀请成功')}*\n\n"
                                f"{escape_markdown('你邀请的新用户已加入频道，已满足条件，可以永久使用机器人。')}\n"
                                f"*{escape_markdown('当前有效邀请:')}* {escape_markdown(str(active))}/{escape_markdown(str(required))}"
                            )
                        else:
                            remain = max(required - active, 0)
                            msg = (
                                f"✅ *{escape_markdown('邀请成功')}*\n\n"
                                f"{escape_markdown('你邀请的新用户已加入频道，已计入有效邀请。')}\n"
                                f"*{escape_markdown('当前有效邀请:')}* {escape_markdown(str(active))}/{escape_markdown(str(required))}\n"
                                f"{escape_markdown('还差')} *{escape_markdown(str(remain))}* {escape_markdown('人即可永久使用')}"
                            )
                        try:
                            bot.send_message(inviter_id, msg, parse_mode='MarkdownV2')
                        except Exception:
                            pass
                        try:
                            bot.send_message(invited_user_id, f"✅ *{escape_markdown('你已完成邀请流程，欢迎使用机器人。')}*", parse_mode='MarkdownV2')
                        except Exception:
                            pass
                except Exception as e:
                    logger.info(f"⚠️ pending 邀请自动转 active 失败: {e}")

            # 2) active 邀请：若被邀请人退频道，清空邀请人数并通知
            with db_lock:
                conn = get_db_connection()
                c = conn.cursor()
                c.execute(
                    "SELECT inviter_id, invited_user_id FROM user_invites WHERE status = 'active' ORDER BY invited_at ASC LIMIT 200"
                )
                active_rows = c.fetchall() or []
                conn.close()

            for row in active_rows:
                inviter_id = int(row["inviter_id"])
                invited_user_id = int(row["invited_user_id"])
                member_status = get_required_channel_membership_status(invited_user_id)
                if member_status is not False:
                    continue

                try:
                    with db_lock:
                        conn = get_db_connection()
                        c = conn.cursor()
                        now_ts = int(time.time())
                        c.execute(
                            "UPDATE user_invites SET status = 'left', left_at = ? WHERE inviter_id = ? AND invited_user_id = ? AND status = 'active'",
                            (now_ts, inviter_id, invited_user_id),
                        )
                        changed1 = c.rowcount
                        if changed1:
                            c.execute(
                                "UPDATE user_invites SET status = 'reset', left_at = ? WHERE inviter_id = ? AND status IN ('active','pending')",
                                (now_ts, inviter_id),
                            )
                        conn.commit()
                        conn.close()

                    if changed1:
                        alert = (
                            f"⚠️ *{escape_markdown('邀请进度已清空')}*\n\n"
                            f"{escape_markdown('你邀请的用户已退出官方频道。根据规则，你的邀请人数已清空，请重新邀请新用户。')}"
                        )
                        alert = alert + invite_link_text(inviter_id)
                        markup = types.InlineKeyboardMarkup(row_width=1)
                        required_channel = CONFIG.get("REQUIRED_CHANNEL")
                        if required_channel and required_channel.startswith('@'):
                            markup.add(types.InlineKeyboardButton("📢 官方频道", url=f"https://t.me/{required_channel.lstrip('@')}"))
                        try:
                            bot.send_message(inviter_id, alert, reply_markup=markup, disable_web_page_preview=True, parse_mode='MarkdownV2')
                        except Exception:
                            pass
                except Exception as e:
                    logger.info(f"⚠️ active 邀请退群处理失败: {e}")

            time.sleep(8)
        except Exception as e:
            logger.info(f"⚠️ 邀请自动检测线程异常: {e}")
            time.sleep(8)


def web_verify_autodetect_thread():
    """自动检测：用户加入频道后，无需再 /start，自动把对应 state 标记为 approved，并私聊通知返回网页。"""
    logger.info("✅ 网页验证自动检测线程已启动。")
    while True:
        try:
            db_path = CONFIG.get("DATABASE_FILE", "history.db")
            if not os.path.exists(db_path):
                time.sleep(5)
                continue

            now_ts = int(time.time())
            rows = []
            with db_lock:
                conn = get_db_connection()
                c = conn.cursor()
                try:
                    c.execute(
                        "SELECT state, tg_user_id, created, expires, notified FROM web_verify_pending WHERE expires >= ? AND notified = 0 ORDER BY created ASC LIMIT 50",
                        (now_ts,),
                    )
                    rows = c.fetchall() or []
                except Exception:
                    rows = []
                conn.close()

            for r in rows:
                state = r["state"]
                uid = int(r["tg_user_id"])
                exp = int(r["expires"] or 0)
                if exp < now_ts:
                    continue

                ok = False
                try:
                    ok = is_user_in_required_channel(uid)
                except Exception:
                    ok = False
                if not ok:
                    continue

                # 标记 approved + notified
                try:
                    with db_lock:
                        conn = get_db_connection()
                        c = conn.cursor()
                        c.execute("UPDATE web_verify_states SET approved = 1, used = 0 WHERE state = ?", (state,))
                        c.execute("UPDATE web_verify_pending SET notified = 1 WHERE state = ?", (state,))
                        conn.commit()
                        conn.close()
                except Exception as e:
                    logger.info(f"⚠️ 自动通过写库失败: {e}")

                # 主动通知用户
                try:
                    web_base = os.environ.get("WEB_BASE_URL", "").strip().rstrip("/")
                    if not web_base:
                        web_base = f"http://{CONFIG.get('SERVER_PUBLIC_IP','127.0.0.1')}:{os.environ.get('WEB_PORT','1319')}"
                    cb_url = f"{web_base}/verify/callback?state={state}"
                    markup = types.InlineKeyboardMarkup()
                    markup.add(types.InlineKeyboardButton("✅ 返回网页继续", url=cb_url))
                    bot.send_message(uid, "✅ 验证通过！网页将自动解锁（如仍在验证页会自动跳转）。", reply_markup=markup, disable_web_page_preview=True)
                except Exception as e:
                    logger.info(f"⚠️ 自动通知失败(可能用户未私聊/屏蔽机器人): {e}")

            time.sleep(5)
        except Exception as e:
            logger.info(f"⚠️ 网页验证自动检测线程异常: {e}")
            time.sleep(5)

if __name__ == '__main__':
    # 配置日志记录
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(name)s - %(message)s')
    logger.info(f"🚀 初始化 猎诈卫士 Bot ({BOT_VERSION})...")
    
    get_bot_name()

    for fname, default_content in [
        (CONFIG["CHANNELS_FILE"], '[]'),
        (CONFIG["REPORTS_FILE"], '{"pending": {}, "verified": {}}'),
    ]:
        if not os.path.exists(fname):
            with open(fname, 'w', encoding='utf-8') as f: f.write(default_content)
            logger.info(f"📄 创建默认文件: {fname}")
    
    init_db()

    # --- 启动顺序优化 ---

    # 1. 优先启动 Flask Web 服务器，确保端口监听成功
    server_thread = threading.Thread(target=run_server, name="FlaskWebhookThread", daemon=True)
    server_thread.start()
    logger.info("✅ OKPay 回调服务器线程已启动。")
    time.sleep(3) # 等待3秒，确保 Flask 有足够时间完成初始化和端口绑定

    # 2. 启动日志批量处理器
    log_thread = threading.Thread(target=log_batcher_thread, name="LogBatcherThread", daemon=True)
    log_thread.start()
    logger.info("✅ 日志批量处理器线程已启动。")

    # 2.5 启动网页版验证自动检测线程
    webv_thread = threading.Thread(target=web_verify_autodetect_thread, name="WebVerifyAutoDetectThread", daemon=True)
    webv_thread.start()

    # 2.6 启动邀请自动检测线程
    invite_thread = threading.Thread(target=invite_autodetect_thread, name="InviteAutoDetectThread", daemon=True)
    invite_thread.start()

    # 2.7 启动投票群发截止检测线程
    poll_watch_thread = threading.Thread(target=poll_job_watcher_thread, name="PollJobWatcherThread", daemon=True)
    poll_watch_thread.start()

    # 3. 启动 Telethon 客户端
    telethon_thread = threading.Thread(target=start_telethon, name="TelethonThread", daemon=True)
    telethon_thread.start()
    logger.info("✅ Telethon 客户端线程启动。等待连接...")
    time.sleep(5) # 等待 Telethon 开始连接

    # 4. 最后启动 Telebot 轮询，这是主阻塞进程
    logger.info("🤖 Telebot 准备开始轮询 (使用带自愈功能的 infinity_polling)...")
    while True:
        try:
            logger.info(f"🟢 {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} - Telebot 轮询循环已启动/重启。")
            bot.infinity_polling(
                timeout=CONFIG.get('TELEBOT_API_TIMEOUT', 40),
                long_polling_timeout=CONFIG.get('TELEBOT_POLLING_TIMEOUT', 30),
                allowed_updates=telebot.util.update_types
            )
        except KeyboardInterrupt:
            logger.info("\n🛑 正在关闭...")
            break
        except (requests.exceptions.ReadTimeout, requests.exceptions.ConnectionError) as e:
            logger.warning(f"🔥 Telebot 网络错误: {type(e).__name__}. 轮询中断。15秒后重启...")
            time.sleep(15)
        except ApiTelegramException as e:
            logger.error(f"🔥 Telebot API 错误: {e}。15秒后重启...")
            time.sleep(15)
        except Exception as e:
            logger.exception(f"💥 Telebot 轮询出现未处理的致命错误: {e}")
            logger.info("🔁 发生严重错误，30秒后将自动重启轮询...")
            time.sleep(30)
    
    # --- 清理部分（保持不变）---
    logger.info("🧹 开始清理和关闭程序...")
    bot.stop_polling()
    if telethon_loop and telethon_loop.is_running():
        logger.info("🔌 正在断开 Telethon 连接...")
        future = asyncio.run_coroutine_threadsafe(client.disconnect(), telethon_loop)
        try:
            future.result(timeout=5)
            logger.info("✅ Telethon 已断开。")
        except Exception as e:
            logger.warning(f"⚠️ Telethon 断开连接超时或失败: {e}")
        
        if telethon_loop.is_running():
             telethon_loop.call_soon_threadsafe(telethon_loop.stop)
    
    if 'telethon_thread' in locals() and telethon_thread.is_alive():
        logger.info("⏳ 等待 Telethon 线程结束...")
        telethon_thread.join(timeout=10)
        if telethon_thread.is_alive():
            logger.warning("⚠️ Telethon 线程未能正常结束。")

    logger.info("🚪 主线程退出。程序已关闭。")



# ===================== Web 控制台功能（新增） =====================
# 通过已有的 Flask 实例 `app` 提供一个简单的网页控制台：
# - /           总览 + 快速查询入口
# - /query      风险查询（ID / @用户名）
# - /stats      运行状态（与 /stats 命令类似）
# - /leaderboard 赞助排行榜（与 /leaderboard 命令类似）
#
# 说明：
# 1panel 只需要将域名反向代理到本程序监听的端口（CONFIG["WEBHOOK_PORT"]，默认 1010）即可。
# Telegram 机器人原有功能保持不变。

from flask import render_template_string, request as flask_request
import html as _html_mod

WEB_BASE_HTML = """
<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8">
  <title>{{ title }} - ZPF 猎诈卫士</title>
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <style>
    * { box-sizing:border-box; }
    body {
      margin:0;
      font-family:-apple-system, system-ui, BlinkMacSystemFont, "Segoe UI", sans-serif;
      background:#020617;
      color:#e5e7eb;
    }
    a { color:#38bdf8; text-decoration:none; }
    a:hover { text-decoration:underline; }
    header {
      position:sticky;
      top:0;
      z-index:10;
      background:#0f172a;
      border-bottom:1px solid #1f2937;
      padding:12px 20px;
      display:flex;
      justify-content:space-between;
      align-items:center;
    }
    header .brand {
      font-weight:600;
      font-size:15px;
      display:flex;
      align-items:center;
      gap:8px;
    }
    .pill {
      display:inline-block;
      padding:2px 10px;
      border-radius:999px;
      font-size:11px;
      background:#111827;
      color:#9ca3af;
    }
    nav a {
      margin-left:12px;
      font-size:14px;
      opacity:0.8;
    }
    nav a.active {
      opacity:1;
      color:#f97316;
      font-weight:600;
    }
    .container {
      max-width:960px;
      margin:24px auto 40px;
      padding:0 16px;
    }
    .card {
      background:#020617;
      border-radius:18px;
      padding:20px 22px;
      border:1px solid #1f2937;
      box-shadow:0 18px 45px rgba(15,23,42,0.85);
    }
    h1 { font-size:22px; margin:0 0 10px; }
    h2 { font-size:18px; margin:16px 0 8px; }
    p { margin:6px 0; }
    .muted { color:#9ca3af; font-size:13px; }
    .input-row {
      margin:16px 0 8px;
      display:flex;
      gap:8px;
      flex-wrap:wrap;
    }
    .input-row input[type=text] {
      flex:1 1 220px;
      padding:8px 12px;
      border-radius:999px;
      border:1px solid #1f2937;
      background:#020617;
      color:#e5e7eb;
      outline:none;
      font-size:14px;
    }
    .input-row input[type=text]:focus {
      border-color:#38bdf8;
    }
    .input-row button {
      padding:8px 18px;
      border-radius:999px;
      border:none;
      background:#38bdf8;
      color:#0f172a;
      font-weight:600;
      font-size:14px;
      cursor:pointer;
      white-space:nowrap;
    }
    .input-row button:hover {
      opacity:0.95;
    }
    table {
      width:100%;
      border-collapse:collapse;
      margin-top:10px;
      font-size:13px;
    }
    th, td {
      border-bottom:1px solid #111827;
      padding:8px 6px;
      text-align:left;
      vertical-align:top;
    }
    th { font-weight:600; color:#e5e7eb; }
    code {
      background:#020617;
      padding:2px 4px;
      border-radius:4px;
      font-size:12px;
    }
    .badge {
      display:inline-block;
      padding:2px 8px;
      border-radius:999px;
      font-size:11px;
      background:#1d4ed8;
      color:#e5e7eb;
    }
    .danger { color:#fecaca; }
    .ok { color:#bbf7d0; }
    .section {
      margin-top:12px;
      padding-top:8px;
      border-top:1px dashed #1f2937;
    }
    ul { margin:6px 0 0 18px; padding:0; }
    li { margin:2px 0; }
    @media (max-width:640px) {
      header { flex-direction:column; align-items:flex-start; gap:6px; }
      nav a { margin-left:0; margin-right:12px; }
    }
  </style>
</head>
<body>
  <header>
    <div class="brand">
      <span>🛡 ZPF 猎诈卫士 · 控制台</span>
      <span class="pill">{{ version }}</span>
    </div>
    <nav>
      <a href="/" class="{% if active=='home' %}active{% endif %}">总览</a>
      <a href="/query" class="{% if active=='query' %}active{% endif %}">风险查询</a>
      <a href="/stats" class="{% if active=='stats' %}active{% endif %}">运行状态</a>
      <a href="/leaderboard" class="{% if active=='leaderboard' %}active{% endif %}">赞助榜</a>
    </nav>
  </header>
  <div class="container">
    <div class="card">
      {{ content|safe }}
    </div>
  </div>
</body>
</html>
"""

def _render_web_page(inner_html: str, title: str = "控制台", active: str = "home"):
    """将内容片段塞进统一的页面骨架里。inner_html 视为安全 HTML 片段。"""
    return render_template_string(
        WEB_BASE_HTML,
        title=title,
        active=active,
        version=BOT_VERSION,
        content=inner_html
    )

@app.route("/", methods=["GET"])
def web_index():
    """总览页面 + 快速查询表单"""
    inner = """
    <h1>控制台总览</h1>
    <p class="muted">通过网页直接查看机器人运行状态、发起风险查询、查看赞助排行榜。</p>
    <div class="section">
      <h2>⚡ 快速风险查询</h2>
      <form class="input-row" method="get" action="/query">
        <input type="text" name="q" placeholder="输入 Telegram 数字 ID 或 @用户名，例如 123456789 或 @username">
        <button type="submit">立即查询</button>
      </form>
      <p class="muted">提示：网页查询会尽量复用机器人数据库和监控频道，但结果仅供参考，最终请以官方信息为准。</p>
    </div>
    <div class="section">
      <h2>🔧 部署说明（1panel 简要）</h2>
      <p class="muted">
        1. 在 1panel 中创建应用容器或 Python 项目，确保映射端口与 <code>CONFIG["WEBHOOK_PORT"]</code> 一致（默认 1010）。<br>
        2. 将域名反向代理到该端口，即可通过浏览器访问本控制台。<br>
        3. Telegram 机器人端仍需保持进程运行（如使用守护进程 / Supervisor / 容器方式）。
      </p>
    </div>
    """
    return _render_web_page(inner, title="总览", active="home")


@app.route("/stats", methods=["GET"])
def web_stats():
    """展示与 /stats 命令类似的运行统计"""
    online_count = get_online_user_count()
    with db_lock:
        conn = get_db_connection()
        c = conn.cursor()
        c.execute("SELECT COUNT(*) FROM users")
        total_users = c.fetchone()[0]
        c.execute("SELECT COUNT(*) FROM bot_interactors")
        interacted_users = c.fetchone()[0]
        c.execute("SELECT COUNT(*) FROM message_history")
        total_messages = c.fetchone()[0]
        c.execute("SELECT COUNT(id) FROM username_history")
        total_username_changes = c.fetchone()[0]
        c.execute("SELECT COUNT(id) FROM name_history")
        total_name_changes = c.fetchone()[0]
        conn.close()

    reports = load_reports()
    verified_count = len(reports.get('verified', {}))
    channel_count = len(load_channels())
    backend_status = '在线' if telethon_loop and client.is_connected() else '离线'

    inner = f"""
    <h1>运行状态</h1>
    <p class="muted">当前统计来自 SQLite 数据库：<code>{_html_mod.escape(str(CONFIG.get("DATABASE_FILE", "")))}</code></p>
    <table>
      <tr><th>指标</th><th>数值</th><th>说明</th></tr>
      <tr><td>在线活跃用户</td><td>{online_count}</td><td>最近 {CONFIG.get("ONLINE_THRESHOLD", 300)} 秒内有交互的用户估计数</td></tr>
      <tr><td>可达用户</td><td>{interacted_users}</td><td>曾与机器人有过私聊的用户数</td></tr>
      <tr><td>总收录用户</td><td>{total_users}</td><td>已写入数据库的 Telegram 账户数量</td></tr>
      <tr><td>记录消息总数</td><td>{total_messages}</td><td>message_history 表中的消息记录</td></tr>
      <tr><td>身份变更记录</td><td>{total_username_changes + total_name_changes}</td><td>用户名 + 昵称 变更次数</td></tr>
      <tr><td>已验证投稿</td><td>{verified_count}</td><td>通过人工审核的诈骗投稿总数</td></tr>
      <tr><td>监控频道数</td><td>{channel_count}</td><td>当前正在监控的公开频道 / 群组</td></tr>
      <tr><td>后台引擎状态</td><td>{'<span class="ok">✅ 在线</span>' if backend_status == '在线' else '<span class="danger">❌ 离线</span>'}</td><td>Telethon 抓取引擎是否已连接</td></tr>
    </table>
    """
    return _render_web_page(inner, title="运行状态", active="stats")


@app.route("/leaderboard", methods=["GET"])
def web_leaderboard():
    """赞助排行榜网页版"""
    with db_lock:
        conn = get_db_connection()
        c = conn.cursor()
        c.execute("""
            SELECT s.user_id, s.total_amount_usdt, u.first_name, u.last_name
            FROM sponsors s
            LEFT JOIN users u ON s.user_id = u.user_id
            ORDER BY s.total_amount_usdt DESC
            LIMIT 20
        """)
        top_sponsors = c.fetchall()
        conn.close()

    if not top_sponsors:
        inner = """
        <h1>赞助排行榜</h1>
        <p class="muted">目前还没有赞助记录，期待您的第一笔支持 🫶</p>
        """
        return _render_web_page(inner, title="赞助排行榜", active="leaderboard")

    rows = []
    medals = ["🥇", "🥈", "🥉"]
    for i, sponsor in enumerate(top_sponsors):
        name = f"{sponsor['first_name'] or ''} {sponsor['last_name'] or ''}".strip() or f"用户 {sponsor['user_id']}"
        rank_icon = medals[i] if i < len(medals) else f"#{i+1}"
        rows.append(f"""
        <tr>
          <td>{rank_icon}</td>
          <td>{_html_mod.escape(name)}</td>
          <td>{sponsor['total_amount_usdt']:.4f} USDT</td>
        </tr>
        """)

    inner = f"""
    <h1>赞助排行榜</h1>
    <p class="muted">感谢每一位赞助者的支持！下面是前 {len(top_sponsors)} 名累计赞助金额排行。</p>
    <table>
      <tr><th>名次</th><th>昵称</th><th>累计金额</th></tr>
      {''.join(rows)}
    </table>
    """
    return _render_web_page(inner, title="赞助排行榜", active="leaderboard")


@app.route("/query", methods=["GET", "POST"])
def web_query():
    """
    风险查询（网页）：
    - 输入 数字 ID 或 @用户名
    - 尽量复用数据库和监控频道，给出基础画像 + 风险曝光链接
    """
    q = (flask_request.values.get("q") or "").strip()
    escaped_q = _html_mod.escape(q)
    result_html_parts = []

    if q:
        resolved_id = None
        cleaned = q.lstrip("@").strip()

        # 尝试直接数字 ID
        if cleaned.isdigit():
            try:
                resolved_id = int(cleaned)
            except ValueError:
                resolved_id = None

        # 如果不是纯数字，尝试从历史中解析
        if not resolved_id:
            try:
                resolved_id = _resolve_historic_query_to_id(q)
            except Exception:
                resolved_id = None

        scam_hits = []
        common_groups = []
        db_history = None
        phone_history = []
        bio_history = []
        spoken_ids = set()

        if resolved_id:
            # 先查本地数据库
            db_history = query_user_history_from_db(resolved_id)
            phone_history = query_phone_history_from_db(resolved_id)
            bio_history = query_bio_history_from_db(resolved_id)
            spoken_ids = query_spoken_groups_from_db(resolved_id)

            # 再尝试从 Telethon 引擎获取监控频道命中和共同群组
            if telethon_loop and telethon_loop.is_running():
                try:
                    future1 = asyncio.run_coroutine_threadsafe(
                        search_monitored_channels_for_user(user_id=resolved_id),
                        telethon_loop
                    )
                    scam_hits = future1.result(timeout=CONFIG.get("SCAM_CHANNEL_SEARCH_TIMEOUT", 40))
                except Exception as e:
                    print(f"[WEB-QUERY] scam search failed: {e}")
                try:
                    future2 = asyncio.run_coroutine_threadsafe(
                        get_common_groups_with_user(resolved_id),
                        telethon_loop
                    )
                    common_groups = future2.result(timeout=CONFIG.get("COMMON_GROUPS_TIMEOUT", 90))
                except Exception as e:
                    print(f"[WEB-QUERY] common group search failed: {e}")
        else:
            # 解析不到 ID，则作为原始关键字在监控频道全文搜索
            if telethon_loop and telethon_loop.is_running():
                try:
                    future = asyncio.run_coroutine_threadsafe(
                        search_monitored_channels_for_user(raw_query=q),
                        telethon_loop
                    )
                    scam_hits = future.result(timeout=CONFIG.get("SCAM_CHANNEL_SEARCH_TIMEOUT", 40))
                except Exception as e:
                    print(f"[WEB-QUERY] raw scam search failed: {e}")

        # === 拼装结果 HTML ===
        if resolved_id:
            result_html_parts.append("<h2>👤 查询对象</h2>")
            result_html_parts.append(f"<p class='muted'>解析到的 Telegram ID：<code>{resolved_id}</code></p>")

            if db_history and db_history.get("current_profile"):
                profile = db_history["current_profile"]
                display_name = ((profile["first_name"] or "") + " " + (profile["last_name"] or "")).strip()
                active_usernames = []
                if profile.get("active_usernames_json"):
                    try:
                        active_usernames = json.loads(profile["active_usernames_json"])
                    except Exception:
                        active_usernames = []

                result_html_parts.append("<div class='section'><h3>基础资料</h3><ul>")
                result_html_parts.append(f"<li><b>昵称：</b>{_html_mod.escape(display_name) if display_name else '（未知）'}</li>")
                if active_usernames:
                    result_html_parts.append("<li><b>用户名：</b>" + ", ".join(f"@{_html_mod.escape(u)}" for u in active_usernames) + "</li>")
                if profile.get("phone"):
                    result_html_parts.append(f"<li><b>绑定手机：</b><code>{_html_mod.escape(profile['phone'])}</code></li>")
                if profile.get("bio"):
                    result_html_parts.append(f"<li><b>当前 Bio：</b>{_html_mod.escape(profile['bio'])}</li>")
                result_html_parts.append("</ul></div>")
            else:
                result_html_parts.append("<p class='muted'>数据库中没有该 ID 的详细档案，可能机器人尚未见过此人。</p>")

            # Bio & 电话历史
            if bio_history:
                result_html_parts.append("<div class='section'><h3>Bio 历史</h3><ul>")
                for item in bio_history[:5]:
                    ts = datetime.fromtimestamp(item["date"], tz=CHINA_TZ).strftime("%Y-%m-%d")
                    bio_text = (item["bio"] or "").strip() or "空"
                    result_html_parts.append(f"<li><code>{_html_mod.escape(ts)}</code> - {_html_mod.escape(bio_text)}</li>")
                if len(bio_history) > 5:
                    result_html_parts.append(f"<li class='muted'>共 {len(bio_history)} 条，仅显示最近 5 条。</li>")
                result_html_parts.append("</ul></div>")

            if phone_history:
                result_html_parts.append("<div class='section'><h3>历史绑定号码</h3><ul>")
                for ph in phone_history[:8]:
                    result_html_parts.append(f"<li><code>{_html_mod.escape(ph)}</code></li>")
                if len(phone_history) > 8:
                    result_html_parts.append(f"<li class='muted'>共 {len(phone_history)} 个，仅显示前 8 个。</li>")
                result_html_parts.append("</ul></div>")

            # 共同群组（简单展示名称与可能的 @）
            all_groups = {g["id"]: g for g in common_groups or []}
            for gid in spoken_ids or []:
                if gid not in all_groups:
                    info = get_chat_info_from_db(gid)
                    if info:
                        all_groups[gid] = {
                            "id": gid,
                            "title": info.get("title"),
                            "usernames": [info["username"]] if info.get("username") else []
                        }
            if all_groups:
                result_html_parts.append("<div class='section'><h3>共同群组（数据库 + 扫描结果）</h3><ul>")
                for g in sorted(all_groups.values(), key=lambda x: str(x.get("title") or "")):
                    title = g.get("title") or f"群组 ID {g.get('id')}"
                    uns = [u for u in g.get("usernames") or [] if u]
                    line = _html_mod.escape(title)
                    if uns:
                        line = ", ".join(f"@{_html_mod.escape(u)}" for u in uns) + " · " + line
                    result_html_parts.append(f"<li>{line}</li>")
                result_html_parts.append("</ul></div>")

        # 风险曝光链接
        if scam_hits:
            result_html_parts.append("<div class='section'><h3 class='danger'>⚠ 风险曝光记录</h3><p class='muted'>以下为监控频道 / 群组中命中的消息，仅供参考：</p><ul>")
            for hit in scam_hits[:20]:
                title = _html_mod.escape(hit.get("chat_title") or "未知频道")
                link = _html_mod.escape(hit.get("link") or "#")
                result_html_parts.append(f"<li><a href=\"{link}\" target=\"_blank\">{title}</a></li>")
            if len(scam_hits) > 20:
                result_html_parts.append(f"<li class='muted'>共 {len(scam_hits)} 条命中，仅显示前 20 条。</li>")
            result_html_parts.append("</ul></div>")
        else:
            if q:
                result_html_parts.append("<div class='section'><h3>风险曝光</h3><p class='muted'>未在当前监控频道中发现与该查询相关的曝光记录。（不代表绝对安全）</p></div>")

        if not result_html_parts:
            result_html_parts.append("<p class='muted'>未能根据当前输入解析到有效用户，也未在监控频道中找到相关记录。</p>")

    # 页面骨架 + 表单 + 结果
    inner = f"""
    <h1>风险查询</h1>
    <p class="muted">在此输入 Telegram 数字 ID 或 @用户名，系统会尝试从本地数据库与监控频道中提取相关信息。</p>
    <form class="input-row" method="get" action="/query">
      <input type="text" name="q" value="{escaped_q}" placeholder="例如：123456789 或 @username">
      <button type="submit">查询</button>
    </form>
    {''.join(result_html_parts) if q else ''}
    """
    return _render_web_page(inner, title="风险查询", active="query")


# --- 簡單測試用首頁（Web 控制台占位） ---
from flask import render_template_string

@app.route("/", methods=["GET"])
def web_index_simple():
    html = """
    <!doctype html>
    <html lang="zh-CN">
    <head>
      <meta charset="utf-8">
      <title>ZPF 机器人 · 控制台</title>
    </head>
    <body style="font-family: -apple-system, system-ui, BlinkMacSystemFont, 'Segoe UI', sans-serif;">
      <h1>ZPF 机器人 Web 已运行</h1>
      <p>如果你能看到這個頁面，說明 Flask + 端口映射都正常了。</p>
      <p>之後可以在這裡做：風險查詢 / 運行狀態 / 贊助榜 等完整控制台。</p>
    </body>
    </html>
    """
    return render_template_string(html)


# ---- 统一兜底路由，避免 404 Not Found ----
@app.route('/', defaults={'path': ''}, methods=['GET'])
@app.route('/<path:path>', methods=['GET'])
def catch_all(path):
    """兼容性兜底：任何 GET 路由都返回控制台首頁，避免 404。"""
    try:
        return web_index()
    except Exception:
        # 如果上面的 web_index 有問題，就返回一個極簡文本頁。
        return "ZPF Web 控制台已運行 (兜底路由)。請聯繫開發者檢查模板。", 200
