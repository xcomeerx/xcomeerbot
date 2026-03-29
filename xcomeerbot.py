import logging
import os
import sqlite3
from contextlib import contextmanager
from pathlib import Path
from typing import Optional

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram import BotCommand, BotCommandScopeChat, BotCommandScopeDefault
from telegram.ext import (
    Application,
    ApplicationBuilder,
    CallbackQueryHandler,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    filters,

)

from telegram import (
    BotCommand,
    BotCommandScopeChat,
    BotCommandScopeDefault,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    Update,
)

# =========================
# CONFIG
# =========================

TOKEN = "8706161056:AAH1ITbhQzfQt0DaSyLxvotLZmPXiHtZx1g"
if not TOKEN:
    raise RuntimeError("Не установлена переменная окружения BOT_TOKEN")

ADMIN_ID = int(os.getenv("ADMIN_ID", "1234501696"))
DB_PATH = Path(os.getenv("DB_PATH", "support_bot.db"))

PAYMENT_DETAILS = os.getenv(
    "PAYMENT_DETAILS",
    "💳 Реквизиты для оплаты:\n"
    "Т-Банк\n"
    "Карта: 2200702014407550\n"
    "Получатель: Данил К.\n\n"
    "После оплаты нажми кнопку «Я оплатил» и отправь скрин или чек."
)

MAX_TEXT_PREVIEW = 300
MAX_FILE_SIZE_MB = int(os.getenv("MAX_FILE_SIZE_MB", "50"))
MAX_FILE_SIZE_BYTES = MAX_FILE_SIZE_MB * 1024 * 1024
SPAM_WINDOW_SECONDS = int(os.getenv("SPAM_WINDOW_SECONDS", "5"))
SPAM_MAX_MESSAGES = int(os.getenv("SPAM_MAX_MESSAGES", "4"))
ORDER_EXPIRATION_HOURS = int(os.getenv("ORDER_EXPIRATION_HOURS", "48"))
AUTO_EXPIRE_CHECK_MINUTES = int(os.getenv("AUTO_EXPIRE_CHECK_MINUTES", "60"))

logging.basicConfig(
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)

# user_data keys
UD_REPLY_TO_USER_ID = "reply_to_user_id"
UD_WAITING_PAYMENT_PROOF_ORDER_ID = "waiting_payment_proof_order_id"
UD_SEND_READY_FILE_ORDER_ID = "send_ready_file_order_id"
UD_SEND_READY_FILE_USER_ID = "send_ready_file_user_id"
UD_BROADCAST_MODE = "broadcast_mode"
UD_BROADCAST_TEXT = "broadcast_text"

# =========================
# ORDER STATUSES
# =========================

STATUS_NEW = "new"
STATUS_AWAITING_PAYMENT = "awaiting_payment"
STATUS_AWAITING_PROOF = "awaiting_proof"
STATUS_PROOF_SENT = "proof_sent"
STATUS_PAYMENT_CONFIRMED = "payment_confirmed"
STATUS_PAYMENT_NOT_FOUND = "payment_not_found"
STATUS_IN_PROGRESS = "in_progress"
STATUS_READY_TO_SEND = "ready_to_send"
STATUS_DONE = "done"
STATUS_CANCELLED = "cancelled"
STATUS_EXPIRED = "expired"

ALLOWED_STATUS_TRANSITIONS = {
    STATUS_NEW: {STATUS_AWAITING_PAYMENT, STATUS_CANCELLED},
    STATUS_AWAITING_PAYMENT: {STATUS_AWAITING_PROOF, STATUS_PAYMENT_CONFIRMED, STATUS_PAYMENT_NOT_FOUND, STATUS_CANCELLED, STATUS_EXPIRED},
    STATUS_AWAITING_PROOF: {STATUS_PROOF_SENT, STATUS_PAYMENT_NOT_FOUND, STATUS_CANCELLED, STATUS_EXPIRED},
    STATUS_PROOF_SENT: {STATUS_PAYMENT_CONFIRMED, STATUS_PAYMENT_NOT_FOUND, STATUS_CANCELLED},
    STATUS_PAYMENT_NOT_FOUND: {STATUS_AWAITING_PROOF, STATUS_PROOF_SENT, STATUS_PAYMENT_CONFIRMED, STATUS_CANCELLED},
    STATUS_PAYMENT_CONFIRMED: {STATUS_IN_PROGRESS, STATUS_READY_TO_SEND, STATUS_CANCELLED},
    STATUS_IN_PROGRESS: {STATUS_READY_TO_SEND, STATUS_CANCELLED},
    STATUS_READY_TO_SEND: {STATUS_DONE, STATUS_CANCELLED},
    STATUS_DONE: set(),
    STATUS_CANCELLED: set(),
    STATUS_EXPIRED: set(),
}

PAYMENT_REQUIRED_SERVICE_CODES = {
    "beat_wav",
    "beat_trackout",
    "mix_master",
    "mix_trackout",
    "mix_revision",
    "mix_censor",
}

# =========================
# DATABASE
# =========================

def get_db_connection() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, timeout=10)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


@contextmanager
def db_cursor():
    conn = get_db_connection()
    try:
        cur = conn.cursor()
        yield conn, cur
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def ensure_column_exists(table_name: str, column_name: str, column_type: str) -> None:
    allowed_tables = {
        "users",
        "admin_message_links",
        "dialogs",
        "orders",
        "order_status_history",
        "admin_state",
        "message_rate_limit",
        "order_payment_proofs",
        "dialog_state",
    }
    if table_name not in allowed_tables:
        raise ValueError(f"Недопустимое имя таблицы: {table_name}")

    with db_cursor() as (_, cur):
        cur.execute(f"PRAGMA table_info({table_name})")
        columns = [row["name"] for row in cur.fetchall()]
        if column_name not in columns:
            cur.execute(f"ALTER TABLE {table_name} ADD COLUMN {column_name} {column_type}")


def init_db() -> None:
    with db_cursor() as (_, cur):
        cur.execute("""
            CREATE TABLE IF NOT EXISTS users (
                user_id INTEGER PRIMARY KEY,
                username TEXT,
                full_name TEXT,
                first_seen TEXT DEFAULT CURRENT_TIMESTAMP,
                last_seen TEXT DEFAULT CURRENT_TIMESTAMP
            )
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS admin_message_links (
                admin_message_id INTEGER PRIMARY KEY,
                user_id INTEGER NOT NULL,
                order_id INTEGER,
                message_kind TEXT DEFAULT 'generic',
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS dialogs (
                user_id INTEGER PRIMARY KEY,
                last_message_text TEXT,
                last_message_type TEXT,
                updated_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS dialog_state (
                user_id INTEGER PRIMARY KEY,
                state TEXT DEFAULT 'open',
                updated_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS orders (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                service_code TEXT NOT NULL,
                service_title TEXT NOT NULL,
                service_price TEXT NOT NULL,
                service_price_value INTEGER,
                service_price_currency TEXT,
                status TEXT DEFAULT 'new',
                proof_type TEXT,
                proof_note TEXT,
                proof_sent_at TEXT,
                proof_attempts INTEGER DEFAULT 0,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS order_status_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                order_id INTEGER NOT NULL,
                old_status TEXT,
                new_status TEXT NOT NULL,
                changed_by_user_id INTEGER,
                note TEXT,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS order_payment_proofs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                order_id INTEGER NOT NULL,
                user_id INTEGER NOT NULL,
                proof_type TEXT NOT NULL,
                proof_note TEXT,
                telegram_file_id TEXT,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS admin_state (
                admin_user_id INTEGER PRIMARY KEY,
                reply_to_user_id INTEGER,
                send_ready_file_order_id INTEGER,
                send_ready_file_user_id INTEGER,
                broadcast_mode INTEGER DEFAULT 0,
                broadcast_text TEXT,
                updated_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS message_rate_limit (
                user_id INTEGER PRIMARY KEY,
                window_started_at TEXT DEFAULT CURRENT_TIMESTAMP,
                message_count INTEGER DEFAULT 0,
                last_message_text TEXT,
                updated_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
        """)

    ensure_column_exists("admin_message_links", "order_id", "INTEGER")
    ensure_column_exists("admin_message_links", "message_kind", "TEXT DEFAULT 'generic'")
    ensure_column_exists("orders", "proof_type", "TEXT")
    ensure_column_exists("orders", "proof_note", "TEXT")
    ensure_column_exists("orders", "proof_sent_at", "TEXT")
    ensure_column_exists("orders", "proof_attempts", "INTEGER DEFAULT 0")
    ensure_column_exists("orders", "service_price_value", "INTEGER")
    ensure_column_exists("orders", "service_price_currency", "TEXT")

    with db_cursor() as (_, cur):
        cur.execute("CREATE INDEX IF NOT EXISTS idx_dialogs_updated_at ON dialogs(updated_at DESC)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_orders_user_id ON orders(user_id)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_orders_status_created_at ON orders(status, created_at DESC, id DESC)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_orders_created_at ON orders(created_at DESC, id DESC)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_admin_message_links_user_id ON admin_message_links(user_id)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_admin_message_links_order_id ON admin_message_links(order_id)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_admin_message_links_kind ON admin_message_links(message_kind)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_order_status_history_order_id ON order_status_history(order_id, created_at DESC)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_order_payment_proofs_order_id ON order_payment_proofs(order_id, created_at DESC)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_dialog_state_state ON dialog_state(state, updated_at DESC)")


def migrate_order_statuses() -> None:
    with db_cursor() as (_, cur):
        cur.execute(
            """
            UPDATE orders
            SET status = ?
            WHERE status = ?
            """,
            (STATUS_AWAITING_PAYMENT, STATUS_NEW),
        )


def upsert_user(user_id: int, username: Optional[str], full_name: str) -> None:
    with db_cursor() as (_, cur):
        cur.execute("""
            INSERT INTO users (user_id, username, full_name, last_seen)
            VALUES (?, ?, ?, CURRENT_TIMESTAMP)
            ON CONFLICT(user_id) DO UPDATE SET
                username=excluded.username,
                full_name=excluded.full_name,
                last_seen=CURRENT_TIMESTAMP
        """, (user_id, username, full_name))

        cur.execute("""
            INSERT INTO dialog_state (user_id, state, updated_at)
            VALUES (?, 'open', CURRENT_TIMESTAMP)
            ON CONFLICT(user_id) DO NOTHING
        """, (user_id,))


def update_dialog(user_id: int, last_message_text: str, last_message_type: str) -> None:
    with db_cursor() as (_, cur):
        cur.execute("""
            INSERT INTO dialogs (user_id, last_message_text, last_message_type, updated_at)
            VALUES (?, ?, ?, CURRENT_TIMESTAMP)
            ON CONFLICT(user_id) DO UPDATE SET
                last_message_text=excluded.last_message_text,
                last_message_type=excluded.last_message_type,
                updated_at=CURRENT_TIMESTAMP
        """, (user_id, last_message_text, last_message_type))

        cur.execute("""
            INSERT INTO dialog_state (user_id, state, updated_at)
            VALUES (?, 'open', CURRENT_TIMESTAMP)
            ON CONFLICT(user_id) DO UPDATE SET
                state='open',
                updated_at=CURRENT_TIMESTAMP
        """, (user_id,))


def set_dialog_state(user_id: int, state: str) -> None:
    with db_cursor() as (_, cur):
        cur.execute("""
            INSERT INTO dialog_state (user_id, state, updated_at)
            VALUES (?, ?, CURRENT_TIMESTAMP)
            ON CONFLICT(user_id) DO UPDATE SET
                state=excluded.state,
                updated_at=CURRENT_TIMESTAMP
        """, (user_id, state))


def get_dialog_state(user_id: int) -> str:
    with db_cursor() as (_, cur):
        cur.execute("SELECT state FROM dialog_state WHERE user_id = ?", (user_id,))
        row = cur.fetchone()
        return row["state"] if row else "open"


def save_admin_message_link(
    admin_message_id: int,
    user_id: int,
    order_id: Optional[int] = None,
    message_kind: str = "generic",
) -> None:
    with db_cursor() as (_, cur):
        cur.execute("""
            INSERT OR REPLACE INTO admin_message_links (admin_message_id, user_id, order_id, message_kind)
            VALUES (?, ?, ?, ?)
        """, (admin_message_id, user_id, order_id, message_kind))


def get_user_id_by_admin_message(admin_message_id: int) -> Optional[int]:
    with db_cursor() as (_, cur):
        cur.execute("""
            SELECT user_id
            FROM admin_message_links
            WHERE admin_message_id = ?
        """, (admin_message_id,))
        row = cur.fetchone()
        return row["user_id"] if row else None


def get_recent_dialogs(limit: int = 20):
    with db_cursor() as (_, cur):
        cur.execute("""
            SELECT d.user_id, d.last_message_text, d.last_message_type, d.updated_at,
                   u.username, u.full_name, ds.state
            FROM dialogs d
            LEFT JOIN users u ON u.user_id = d.user_id
            LEFT JOIN dialog_state ds ON ds.user_id = d.user_id
            ORDER BY d.updated_at DESC
            LIMIT ?
        """, (limit,))
        return cur.fetchall()


def get_user_info(user_id: int):
    with db_cursor() as (_, cur):
        cur.execute("""
            SELECT user_id, username, full_name
            FROM users
            WHERE user_id = ?
        """, (user_id,))
        return cur.fetchone()


def parse_price(price_text: str) -> tuple[Optional[int], Optional[str]]:
    digits = "".join(ch for ch in price_text if ch.isdigit())
    if digits:
        return int(digits), "RUB"
    return None, None


def create_order(
    user_id: int,
    service_code: str,
    service_title: str,
    service_price: str,
    status: str = STATUS_NEW,
) -> int:
    price_value, price_currency = parse_price(service_price)
    with db_cursor() as (_, cur):
        cur.execute("""
            INSERT INTO orders (
                user_id, service_code, service_title, service_price,
                service_price_value, service_price_currency, status
            )
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (user_id, service_code, service_title, service_price, price_value, price_currency, status))
        return cur.lastrowid


def get_order(order_id: int):
    with db_cursor() as (_, cur):
        cur.execute("SELECT * FROM orders WHERE id = ?", (order_id,))
        return cur.fetchone()


def can_transition_order(current_status: str, new_status: str) -> bool:
    return new_status in ALLOWED_STATUS_TRANSITIONS.get(current_status, set())


def add_order_status_history(
    order_id: int,
    old_status: Optional[str],
    new_status: str,
    changed_by_user_id: Optional[int] = None,
    note: Optional[str] = None,
) -> None:
    with db_cursor() as (_, cur):
        cur.execute("""
            INSERT INTO order_status_history (order_id, old_status, new_status, changed_by_user_id, note)
            VALUES (?, ?, ?, ?, ?)
        """, (order_id, old_status, new_status, changed_by_user_id, note))


def get_order_status_history(order_id: int, limit: int = 20):
    with db_cursor() as (_, cur):
        cur.execute("""
            SELECT *
            FROM order_status_history
            WHERE order_id = ?
            ORDER BY created_at DESC, id DESC
            LIMIT ?
        """, (order_id, limit))
        return cur.fetchall()


def add_payment_proof(order_id: int, user_id: int, proof_type: str, proof_note: str, telegram_file_id: Optional[str]) -> None:
    with db_cursor() as (_, cur):
        cur.execute("""
            INSERT INTO order_payment_proofs (order_id, user_id, proof_type, proof_note, telegram_file_id)
            VALUES (?, ?, ?, ?, ?)
        """, (order_id, user_id, proof_type, proof_note, telegram_file_id))


def get_order_payment_proofs(order_id: int, limit: int = 10):
    with db_cursor() as (_, cur):
        cur.execute("""
            SELECT *
            FROM order_payment_proofs
            WHERE order_id = ?
            ORDER BY created_at DESC, id DESC
            LIMIT ?
        """, (order_id, limit))
        return cur.fetchall()


def update_order_status(
    order_id: int,
    status: str,
    proof_type: Optional[str] = None,
    proof_note: Optional[str] = None,
    force: bool = False,
    changed_by_user_id: Optional[int] = None,
    note: Optional[str] = None,
) -> None:
    order = get_order(order_id)
    if not order:
        raise ValueError(f"Заказ {order_id} не найден")

    current_status = order["status"]
    if status != current_status and not force and not can_transition_order(current_status, status):
        raise ValueError(f"Недопустимый переход статуса: {current_status} -> {status}")

    with db_cursor() as (_, cur):
        cur.execute("""
            UPDATE orders
            SET status = ?,
                proof_type = COALESCE(?, proof_type),
                proof_note = COALESCE(?, proof_note)
            WHERE id = ?
        """, (status, proof_type, proof_note, order_id))

    if status != current_status:
        add_order_status_history(order_id, current_status, status, changed_by_user_id, note)


def mark_order_proof_sent(order_id: int, proof_type: str, proof_note: str, changed_by_user_id: Optional[int] = None) -> None:
    order = get_order(order_id)
    if not order:
        raise ValueError(f"Заказ {order_id} не найден")

    current_status = order["status"]
    if current_status != STATUS_PROOF_SENT and not can_transition_order(current_status, STATUS_PROOF_SENT):
        raise ValueError(f"Недопустимый переход статуса: {current_status} -> {STATUS_PROOF_SENT}")

    with db_cursor() as (_, cur):
        cur.execute("""
            UPDATE orders
            SET status = ?,
                proof_type = ?,
                proof_note = ?,
                proof_sent_at = CURRENT_TIMESTAMP,
                proof_attempts = COALESCE(proof_attempts, 0) + 1
            WHERE id = ?
        """, (STATUS_PROOF_SENT, proof_type, proof_note, order_id))

    if current_status != STATUS_PROOF_SENT:
        add_order_status_history(order_id, current_status, STATUS_PROOF_SENT, changed_by_user_id, "Пользователь отправил подтверждение оплаты")


def get_orders(limit: int = 20, status: Optional[str] = None):
    with db_cursor() as (_, cur):
        if status:
            cur.execute("""
                SELECT o.*, u.username, u.full_name
                FROM orders o
                LEFT JOIN users u ON u.user_id = o.user_id
                WHERE o.status = ?
                ORDER BY o.created_at DESC, o.id DESC
                LIMIT ?
            """, (status, limit))
        else:
            cur.execute("""
                SELECT o.*, u.username, u.full_name
                FROM orders o
                LEFT JOIN users u ON u.user_id = o.user_id
                ORDER BY o.created_at DESC, o.id DESC
                LIMIT ?
            """, (limit,))
        return cur.fetchall()


def get_user_orders(user_id: int, limit: int = 10):
    with db_cursor() as (_, cur):
        cur.execute("""
            SELECT *
            FROM orders
            WHERE user_id = ?
            ORDER BY created_at DESC, id DESC
            LIMIT ?
        """, (user_id, limit))
        return cur.fetchall()


def get_stats():
    with db_cursor() as (_, cur):
        cur.execute("SELECT COUNT(*) AS cnt FROM users")
        users_count = cur.fetchone()["cnt"]

        cur.execute("SELECT COUNT(*) AS cnt FROM dialogs")
        dialogs_count = cur.fetchone()["cnt"]

        cur.execute("SELECT COUNT(*) AS cnt FROM orders")
        orders_count = cur.fetchone()["cnt"]

        cur.execute("""
            SELECT status, COUNT(*) AS cnt
            FROM orders
            GROUP BY status
            ORDER BY cnt DESC
        """)
        by_status = cur.fetchall()

        cur.execute("SELECT COALESCE(SUM(service_price_value), 0) AS total_revenue FROM orders WHERE status = ?", (STATUS_DONE,))
        total_revenue = cur.fetchone()["total_revenue"]

        return {
            "users_count": users_count,
            "dialogs_count": dialogs_count,
            "orders_count": orders_count,
            "by_status": by_status,
            "total_revenue": total_revenue,
        }


def get_all_user_ids():
    with db_cursor() as (_, cur):
        cur.execute("SELECT user_id FROM users ORDER BY user_id ASC")
        return [row["user_id"] for row in cur.fetchall()]


def get_active_user_ids():
    with db_cursor() as (_, cur):
        cur.execute("SELECT user_id FROM dialog_state WHERE state = 'open' ORDER BY user_id ASC")
        return [row["user_id"] for row in cur.fetchall()]


def get_admin_state(admin_user_id: int) -> dict:
    with db_cursor() as (_, cur):
        cur.execute("SELECT * FROM admin_state WHERE admin_user_id = ?", (admin_user_id,))
        row = cur.fetchone()
        if row:
            return dict(row)
        return {
            "admin_user_id": admin_user_id,
            "reply_to_user_id": None,
            "send_ready_file_order_id": None,
            "send_ready_file_user_id": None,
            "broadcast_mode": 0,
            "broadcast_text": None,
        }


def upsert_admin_state(
    admin_user_id: int,
    reply_to_user_id: Optional[int] = None,
    send_ready_file_order_id: Optional[int] = None,
    send_ready_file_user_id: Optional[int] = None,
    broadcast_mode: Optional[int] = None,
    broadcast_text: Optional[str] = None,
) -> None:
    state = get_admin_state(admin_user_id)
    values = {
        "reply_to_user_id": state.get("reply_to_user_id") if reply_to_user_id is None else reply_to_user_id,
        "send_ready_file_order_id": state.get("send_ready_file_order_id") if send_ready_file_order_id is None else send_ready_file_order_id,
        "send_ready_file_user_id": state.get("send_ready_file_user_id") if send_ready_file_user_id is None else send_ready_file_user_id,
        "broadcast_mode": state.get("broadcast_mode") if broadcast_mode is None else broadcast_mode,
        "broadcast_text": state.get("broadcast_text") if broadcast_text is None else broadcast_text,
    }
    with db_cursor() as (_, cur):
        cur.execute("""
            INSERT INTO admin_state (
                admin_user_id, reply_to_user_id, send_ready_file_order_id,
                send_ready_file_user_id, broadcast_mode, broadcast_text, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            ON CONFLICT(admin_user_id) DO UPDATE SET
                reply_to_user_id=excluded.reply_to_user_id,
                send_ready_file_order_id=excluded.send_ready_file_order_id,
                send_ready_file_user_id=excluded.send_ready_file_user_id,
                broadcast_mode=excluded.broadcast_mode,
                broadcast_text=excluded.broadcast_text,
                updated_at=CURRENT_TIMESTAMP
        """, (
            admin_user_id,
            values["reply_to_user_id"],
            values["send_ready_file_order_id"],
            values["send_ready_file_user_id"],
            values["broadcast_mode"],
            values["broadcast_text"],
        ))


def clear_admin_state(admin_user_id: int, *fields: str) -> None:
    current = get_admin_state(admin_user_id)
    update_values = {
        "reply_to_user_id": current.get("reply_to_user_id"),
        "send_ready_file_order_id": current.get("send_ready_file_order_id"),
        "send_ready_file_user_id": current.get("send_ready_file_user_id"),
        "broadcast_mode": current.get("broadcast_mode"),
        "broadcast_text": current.get("broadcast_text"),
    }
    for field in fields:
        if field in update_values:
            update_values[field] = 0 if field == "broadcast_mode" else None
    with db_cursor() as (_, cur):
        cur.execute("""
            INSERT INTO admin_state (
                admin_user_id, reply_to_user_id, send_ready_file_order_id,
                send_ready_file_user_id, broadcast_mode, broadcast_text, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            ON CONFLICT(admin_user_id) DO UPDATE SET
                reply_to_user_id=excluded.reply_to_user_id,
                send_ready_file_order_id=excluded.send_ready_file_order_id,
                send_ready_file_user_id=excluded.send_ready_file_user_id,
                broadcast_mode=excluded.broadcast_mode,
                broadcast_text=excluded.broadcast_text,
                updated_at=CURRENT_TIMESTAMP
        """, (
            admin_user_id,
            update_values["reply_to_user_id"],
            update_values["send_ready_file_order_id"],
            update_values["send_ready_file_user_id"],
            update_values["broadcast_mode"],
            update_values["broadcast_text"],
        ))


def is_user_rate_limited(user_id: int, message_text: str) -> bool:
    with db_cursor() as (_, cur):
        cur.execute("SELECT * FROM message_rate_limit WHERE user_id = ?", (user_id,))
        row = cur.fetchone()
        if not row:
            cur.execute("""
                INSERT INTO message_rate_limit (user_id, message_count, last_message_text, updated_at)
                VALUES (?, 1, ?, CURRENT_TIMESTAMP)
            """, (user_id, message_text[:100] if message_text else None))
            return False

        cur.execute("""
            SELECT (
                CAST(strftime('%s', 'now') AS INTEGER) - CAST(strftime('%s', window_started_at) AS INTEGER)
            ) AS diff
            FROM message_rate_limit
            WHERE user_id = ?
        """, (user_id,))
        diff = cur.fetchone()["diff"]

        if diff is None or diff > SPAM_WINDOW_SECONDS:
            cur.execute("""
                UPDATE message_rate_limit
                SET window_started_at = CURRENT_TIMESTAMP,
                    message_count = 1,
                    last_message_text = ?,
                    updated_at = CURRENT_TIMESTAMP
                WHERE user_id = ?
            """, (message_text[:100] if message_text else None, user_id))
            return False

        new_count = row["message_count"] + 1
        cur.execute("""
            UPDATE message_rate_limit
            SET message_count = ?,
                last_message_text = ?,
                updated_at = CURRENT_TIMESTAMP
            WHERE user_id = ?
        """, (new_count, message_text[:100] if message_text else None, user_id))
        return new_count > SPAM_MAX_MESSAGES


# =========================
# BUSINESS LOGIC
# =========================

def get_status_text(status: str) -> str:
    mapping = {
        STATUS_NEW: "Новая заявка",
        STATUS_AWAITING_PAYMENT: "Ожидается оплата",
        STATUS_AWAITING_PROOF: "Ожидается подтверждение оплаты",
        STATUS_PROOF_SENT: "Скрин/документ отправлен",
        STATUS_PAYMENT_CONFIRMED: "Оплата подтверждена",
        STATUS_PAYMENT_NOT_FOUND: "Оплата не найдена",
        STATUS_IN_PROGRESS: "Заказ в работе",
        STATUS_READY_TO_SEND: "Готов к отправке клиенту",
        STATUS_DONE: "Заказ завершён",
        STATUS_CANCELLED: "Заказ отменён",
        STATUS_EXPIRED: "Срок заявки истёк",
    }
    return mapping.get(status, status)


def format_order_row(order) -> str:
    username = f"@{order['username']}" if "username" in order.keys() and order["username"] else "нет"
    full_name = order["full_name"] if "full_name" in order.keys() and order["full_name"] else "Неизвестно"
    return (
        f"Заказ №{order['id']}\n"
        f"Клиент: {full_name} | ID: {order['user_id']} | {username}\n"
        f"Услуга: {order['service_title']}\n"
        f"Цена: {order['service_price']}\n"
        f"Статус: {get_status_text(order['status'])}\n"
        f"Создан: {order['created_at']}"
    )


def service_catalog():
    return {
        "beat_wav": {
            "title": "Биты / WAV",
            "price": "1999₽",
            "requires_payment": True,
            "user_text": (
                "🎵 БИТЫ / WAV\n\n"
                "Цена: 1999₽\n"
                "Если хочешь заказать эту услугу, нажми кнопку ниже."
            ),
        },
        "beat_trackout": {
            "title": "Биты / Trackout",
            "price": "3999₽",
            "requires_payment": True,
            "user_text": (
                "🎵 БИТЫ / Trackout\n\n"
                "Цена: 3999₽\n"
                "Если хочешь заказать эту услугу, нажми кнопку ниже."
            ),
        },
        "beat_custom": {
            "title": "Биты / EX / Custom",
            "price": "Писать в лс",
            "requires_payment": False,
            "user_text": (
                "🎵 БИТЫ / EX / Custom\n\n"
                "Цена обсуждается лично.\n"
                "Нажми кнопку ниже, и я отправлю заявку админу."
            ),
        },
        "mix_master": {
            "title": "Сведение / Mix + Master",
            "price": "5999₽",
            "requires_payment": True,
            "user_text": (
                "🎚 СВЕДЕНИЕ / Mix + Master\n\n"
                "Цена: 5999₽\n"
                "Включено: 3 бесплатные правки.\n"
                "Остальные правки: 300₽ за 1."
            ),
        },
        "mix_trackout": {
            "title": "Сведение / Mix Trackout",
            "price": "1999₽",
            "requires_payment": True,
            "user_text": (
                "🎚 СВЕДЕНИЕ / Mix Trackout\n\n"
                "Цена: 1999₽\n"
                "Включено: 3 бесплатные правки.\n"
                "Остальные правки: 300₽ за 1."
            ),
        },
        "mix_revision": {
            "title": "Сведение / Доп. правка",
            "price": "300₽",
            "requires_payment": True,
            "user_text": (
                "🎚 СВЕДЕНИЕ / Дополнительная правка\n\n"
                "Цена: 300₽ за 1 правку."
            ),
        },
        "mix_censor": {
            "title": "Сведение / Censor",
            "price": "200₽",
            "requires_payment": True,
            "user_text": (
                "🎚 СВЕДЕНИЕ / Censor\n\n"
                "Цена: 200₽."
            ),
        },
    }


def build_user_header(user) -> str:
    username = f"@{user.username}" if user.username else "нет"
    return (
        "Новое сообщение от пользователя\n"
        f"ID: {user.id}\n"
        f"Имя: {user.full_name}\n"
        f"Username: {username}"
    )


def full_price_text() -> str:
    return (
        "💰 ПРАЙС\n\n"
        "🎵 БИТЫ\n"
        "⭐ WAV — 1999₽\n"
        "⭐ Trackout — 3999₽\n"
        "⭐ EX / Custom — договорная\n\n"
        "🎚 СВЕДЕНИЕ\n"
        "⭐ Mix + Master — 5999₽\n"
        "⭐ Mix Trackout — 1999₽\n"
        "⭐️ Платные правки — 300₽ за 1\n"
        "⭐ Censor — 200₽\n\n"
        "⏳ СРОКИ\n"
        "От 4 дней до 1 недели"
    )


def is_payment_required(service_code: str) -> bool:
    return service_code in PAYMENT_REQUIRED_SERVICE_CODES


def text_preview(text: str) -> str:
    return (text or "")[:MAX_TEXT_PREVIEW]


def get_message_file_size(message) -> int:
    candidates = [message.document, message.video, message.audio, message.voice]
    for item in candidates:
        if item and getattr(item, "file_size", None):
            return item.file_size
    if message.photo:
        return message.photo[-1].file_size or 0
    return 0


def is_allowed_payment_proof(message) -> bool:
    if message.photo:
        return True
    if message.document:
        mime = (message.document.mime_type or "").lower()
        filename = (message.document.file_name or "").lower()
        return mime == "application/pdf" or filename.endswith(".pdf")
    return False


def format_history_rows(rows) -> str:
    if not rows:
        return "История статусов пуста."
    parts = []
    for row in rows:
        parts.append(
            f"{row['created_at']} | {get_status_text(row['old_status']) if row['old_status'] else '—'} → {get_status_text(row['new_status'])}"
            f" | note: {row['note'] or 'нет'}"
        )
    return "\n".join(parts)


# =========================
# KEYBOARDS
# =========================

def main_menu_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🛒 Заказать", callback_data="main:order")],
        [InlineKeyboardButton("📦 Мои заказы", callback_data="main:my_orders")],
        [InlineKeyboardButton("✉️ Написать", callback_data="main:write")],
        [InlineKeyboardButton("🛟 Поддержка", callback_data="main:support")],
        [InlineKeyboardButton("💰 Прайс", callback_data="main:price")],
    ])


def order_menu_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🎵 Биты", callback_data="menu:beats")],
        [InlineKeyboardButton("🎚 Сведение", callback_data="menu:mix")],
        [InlineKeyboardButton("💰 Прайс", callback_data="menu:price")],
        [InlineKeyboardButton("⏳ Сроки", callback_data="menu:terms")],
        [InlineKeyboardButton("⬅️ Назад в меню", callback_data="menu:back_to_main")],
    ])


def beats_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("⭐ WAV — 1999₽", callback_data="order:beat_wav")],
        [InlineKeyboardButton("⭐ Trackout — 3999₽", callback_data="order:beat_trackout")],
        [InlineKeyboardButton("⭐ EX / Custom — договорная", callback_data="order:beat_custom")],
        [InlineKeyboardButton("⬅️ Назад", callback_data="menu:back_order")],
    ])


def mix_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("⭐ Mix + Master — 5999₽", callback_data="order:mix_master")],
        [InlineKeyboardButton("⭐ Mix Trackout — 1999₽", callback_data="order:mix_trackout")],
        [InlineKeyboardButton("⭐ Доп. правка — 300₽", callback_data="order:mix_revision")],
        [InlineKeyboardButton("⭐ Censor — 200₽", callback_data="order:mix_censor")],
        [InlineKeyboardButton("⬅️ Назад", callback_data="menu:back_order")],
    ])


def order_confirm_keyboard(service_code: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("✅ Оформить заявку", callback_data=f"confirm:{service_code}")],
        [InlineKeyboardButton("⬅️ Назад", callback_data="menu:back_order")],
    ])


def payment_wait_keyboard(order_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("💸 Я оплатил", callback_data=f"paid:{order_id}")],
        [InlineKeyboardButton("⬅️ В услуги", callback_data="menu:back_order")],
    ])


def admin_reply_keyboard(user_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("Ответить", callback_data=f"reply:{user_id}")],
        [InlineKeyboardButton("Закрыть диалог", callback_data=f"dialog_close:{user_id}")],
    ])


def admin_order_status_keyboard(order_id: int, user_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("Ответить", callback_data=f"reply:{user_id}")],
        [InlineKeyboardButton("История", callback_data=f"history:{order_id}")],
        [
            InlineKeyboardButton("✅ Оплата подтверждена", callback_data=f"status:{order_id}:{STATUS_PAYMENT_CONFIRMED}"),
            InlineKeyboardButton("❌ Оплата не найдена", callback_data=f"status:{order_id}:{STATUS_PAYMENT_NOT_FOUND}"),
        ],
        [
            InlineKeyboardButton("🛠 Заказ в работе", callback_data=f"status:{order_id}:{STATUS_IN_PROGRESS}"),
            InlineKeyboardButton("📦 Готов к отправке", callback_data=f"status:{order_id}:{STATUS_READY_TO_SEND}"),
        ],
        [
            InlineKeyboardButton("🚫 Отменить заказ", callback_data=f"status:{order_id}:{STATUS_CANCELLED}"),
        ],
    ])


def broadcast_confirm_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("✅ Подтвердить рассылку", callback_data="broadcast:confirm")],
        [InlineKeyboardButton("❌ Отменить", callback_data="broadcast:cancel")],
    ])


def crm_main_keyboard() -> InlineKeyboardMarkup:
    stats = get_stats()
    by_status = {row["status"]: row["cnt"] for row in stats["by_status"]}
    return InlineKeyboardMarkup([
        [InlineKeyboardButton(f"💳 Ждут оплату ({by_status.get(STATUS_AWAITING_PAYMENT, 0)})", callback_data=f"crm:list:{STATUS_AWAITING_PAYMENT}")],
        [InlineKeyboardButton(f"🧾 Ждут чек ({by_status.get(STATUS_AWAITING_PROOF, 0)})", callback_data=f"crm:list:{STATUS_AWAITING_PROOF}")],
        [InlineKeyboardButton(f"📨 Чек отправлен ({by_status.get(STATUS_PROOF_SENT, 0)})", callback_data=f"crm:list:{STATUS_PROOF_SENT}")],
        [InlineKeyboardButton(f"🛠 В работе ({by_status.get(STATUS_IN_PROGRESS, 0)})", callback_data=f"crm:list:{STATUS_IN_PROGRESS}")],
        [InlineKeyboardButton(f"📦 Готовые ({by_status.get(STATUS_READY_TO_SEND, 0)})", callback_data=f"crm:list:{STATUS_READY_TO_SEND}")],
        [InlineKeyboardButton(f"✅ Завершённые ({by_status.get(STATUS_DONE, 0)})", callback_data=f"crm:list:{STATUS_DONE}")],
        [InlineKeyboardButton(f"🚫 Отменённые ({by_status.get(STATUS_CANCELLED, 0)})", callback_data=f"crm:list:{STATUS_CANCELLED}")],
        [InlineKeyboardButton(f"⌛ Просроченные ({by_status.get(STATUS_EXPIRED, 0)})", callback_data=f"crm:list:{STATUS_EXPIRED}")],
        [InlineKeyboardButton("📋 Все заказы", callback_data="crm:list:all")],
    ])


def crm_orders_list_keyboard(rows) -> InlineKeyboardMarkup:
    buttons = []
    for order in rows[:20]:
        buttons.append([
            InlineKeyboardButton(
                f"№{order['id']} | {order['service_title'][:18]}",
                callback_data=f"crm:order:{order['id']}",
            )
        ])
    buttons.append([InlineKeyboardButton("⬅️ В CRM", callback_data="crm:main")])
    return InlineKeyboardMarkup(buttons)


def crm_order_keyboard(order_id: int, user_id: int, status: str) -> InlineKeyboardMarkup:
    rows = [[InlineKeyboardButton("💬 Ответить клиенту", callback_data=f"reply:{user_id}")]]

    if status in {STATUS_AWAITING_PAYMENT, STATUS_AWAITING_PROOF, STATUS_PROOF_SENT, STATUS_PAYMENT_NOT_FOUND}:
        rows.append([
            InlineKeyboardButton("✅ Оплата подтверждена", callback_data=f"status:{order_id}:{STATUS_PAYMENT_CONFIRMED}"),
            InlineKeyboardButton("❌ Оплата не найдена", callback_data=f"status:{order_id}:{STATUS_PAYMENT_NOT_FOUND}"),
        ])

    if status in {STATUS_PAYMENT_CONFIRMED, STATUS_IN_PROGRESS}:
        rows.append([
            InlineKeyboardButton("🛠 В работу", callback_data=f"status:{order_id}:{STATUS_IN_PROGRESS}"),
            InlineKeyboardButton("📦 Готов к отправке", callback_data=f"status:{order_id}:{STATUS_READY_TO_SEND}"),
        ])

    if status not in {STATUS_DONE, STATUS_CANCELLED, STATUS_EXPIRED}:
        rows.append([
            InlineKeyboardButton("🚫 Отменить", callback_data=f"status:{order_id}:{STATUS_CANCELLED}")
        ])

    rows.append([
        InlineKeyboardButton("📜 История", callback_data=f"crm:history:{order_id}"),
        InlineKeyboardButton("⬅️ В CRM", callback_data="crm:main"),
    ])
    return InlineKeyboardMarkup(rows)


# =========================
# MESSAGE HELPERS
# =========================

async def send_user_screen(context: ContextTypes.DEFAULT_TYPE, chat_id: int, text: str, reply_markup=None) -> None:
    await context.bot.send_message(chat_id=chat_id, text=text, reply_markup=reply_markup)


async def edit_or_send(query, context: ContextTypes.DEFAULT_TYPE, user_id: int, text: str, reply_markup=None) -> None:
    try:
        await query.edit_message_text(text=text, reply_markup=reply_markup)
    except Exception:
        await send_user_screen(context, user_id, text, reply_markup=reply_markup)


async def notify_admin(context: ContextTypes.DEFAULT_TYPE, text: str) -> None:
    await context.bot.send_message(chat_id=ADMIN_ID, text=text)


async def show_main_menu(context: ContextTypes.DEFAULT_TYPE, user_id: int, query=None) -> None:
    text = (
        "Главное меню:\n"
        "🛒 Заказать — выбрать услугу\n"
        "📦 Мои заказы — посмотреть статусы\n"
        "✉️ Написать — отправить сообщение\n"
        "🛟 Поддержка — помощь и связь\n"
        "💰 Прайс — посмотреть цены"
    )
    if query:
        await edit_or_send(query, context, user_id, text, reply_markup=main_menu_keyboard())
        return
    await send_user_screen(context, user_id, text, reply_markup=main_menu_keyboard())


async def show_order_menu(context: ContextTypes.DEFAULT_TYPE, user_id: int, query=None) -> None:
    text = "Выбери раздел:"
    if query:
        await edit_or_send(query, context, user_id, text, reply_markup=order_menu_keyboard())
        return
    await send_user_screen(context, user_id, text, reply_markup=order_menu_keyboard())


async def notify_admin_about_order(
    context: ContextTypes.DEFAULT_TYPE,
    order_id: int,
    user,
    service_title: str,
    service_price: str,
    status: str = STATUS_NEW,
) -> None:
    admin_text = (
        "🛒 НОВАЯ ЗАЯВКА\n\n"
        f"Заказ №{order_id}\n"
        f"Услуга: {service_title}\n"
        f"Цена: {service_price}\n"
        f"Статус: {get_status_text(status)}\n\n"
        "Клиент:\n"
        f"ID: {user.id}\n"
        f"Имя: {user.full_name}\n"
        f"Username: {'@' + user.username if user.username else 'нет'}"
    )

    sent = await context.bot.send_message(
        chat_id=ADMIN_ID,
        text=admin_text,
        reply_markup=admin_order_status_keyboard(order_id, user.id),
    )
    save_admin_message_link(sent.message_id, user.id, order_id, "order_notice")


# =========================
# USER SIDE
# =========================

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user = update.effective_user
    upsert_user(user.id, user.username, user.full_name)
    await show_main_menu(context, user.id)


async def my_orders_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user = update.effective_user
    orders = get_user_orders(user.id, 10)

    if not orders:
        await update.message.reply_text("У тебя пока нет заказов.")
        return

    parts = ["Твои последние заказы:\n"]
    for order in orders:
        parts.append(
            f"№{order['id']} | {order['service_title']} | {order['service_price']}\n"
            f"Статус: {get_status_text(order['status'])}\n"
            f"Создан: {order['created_at']}\n"
        )

    await update.message.reply_text("\n".join(parts))


async def handle_payment_proof(update: Update, context: ContextTypes.DEFAULT_TYPE, waiting_order_id: int) -> bool:
    message = update.message
    user = update.effective_user
    order = get_order(waiting_order_id)

    if not order or order["user_id"] != user.id:
        context.user_data.pop(UD_WAITING_PAYMENT_PROOF_ORDER_ID, None)
        return False

    if get_message_file_size(message) > MAX_FILE_SIZE_BYTES:
        await send_user_screen(
            context,
            user.id,
            f"Файл слишком большой. Максимум: {MAX_FILE_SIZE_MB} МБ.",
        )
        return True

    if not is_allowed_payment_proof(message):
        await send_user_screen(
            context,
            user.id,
            "Для подтверждения оплаты пришли:\n"
            "• фото со скрином оплаты\n"
            "или\n"
            "• PDF-документ с чеком/договором.",
        )
        return True

    info_text = (
        "💰 ПОДТВЕРЖДЕНИЕ ОПЛАТЫ\n\n"
        f"Заказ №{order['id']}\n"
        f"Услуга: {order['service_title']}\n"
        f"Цена: {order['service_price']}\n"
        f"Статус: {get_status_text(STATUS_PROOF_SENT)}\n"
        f"Попыток отправки подтверждения: {(order['proof_attempts'] or 0) + 1}\n\n"
        "Клиент:\n"
        f"ID: {user.id}\n"
        f"Имя: {user.full_name}\n"
        f"Username: {'@' + user.username if user.username else 'нет'}"
    )

    try:
        if message.photo:
            mark_order_proof_sent(waiting_order_id, "photo", "Скрин оплаты", changed_by_user_id=user.id)
            add_payment_proof(order['id'], user.id, "photo", "Скрин оплаты", message.photo[-1].file_id)
            update_dialog(user.id, "[Подтверждение оплаты: фото]", "payment_proof")

            sent = await context.bot.send_message(
                chat_id=ADMIN_ID,
                text=info_text,
                reply_markup=admin_order_status_keyboard(order["id"], user.id),
            )
            save_admin_message_link(sent.message_id, user.id, order["id"], "payment_proof_info")

            sent_photo = await context.bot.send_photo(
                chat_id=ADMIN_ID,
                photo=message.photo[-1].file_id,
                caption=f"Скрин оплаты по заказу №{order['id']}",
                reply_markup=admin_order_status_keyboard(order["id"], user.id),
            )
            save_admin_message_link(sent_photo.message_id, user.id, order["id"], "payment_proof_file")

            await notify_admin(context, f"🔔 Пользователь {user.full_name} отправил фото-подтверждение по заказу №{order['id']}")

        elif message.document:
            mark_order_proof_sent(waiting_order_id, "document", message.document.file_name or "Документ", changed_by_user_id=user.id)
            add_payment_proof(order['id'], user.id, "document", message.document.file_name or "Документ", message.document.file_id)
            update_dialog(user.id, "[Подтверждение оплаты: документ]", "payment_proof")

            sent = await context.bot.send_message(
                chat_id=ADMIN_ID,
                text=info_text,
                reply_markup=admin_order_status_keyboard(order["id"], user.id),
            )
            save_admin_message_link(sent.message_id, user.id, order["id"], "payment_proof_info")

            sent_doc = await context.bot.send_document(
                chat_id=ADMIN_ID,
                document=message.document.file_id,
                caption=f"Документ/чек по заказу №{order['id']}",
                reply_markup=admin_order_status_keyboard(order["id"], user.id),
            )
            save_admin_message_link(sent_doc.message_id, user.id, order["id"], "payment_proof_file")

            await notify_admin(context, f"🔔 Пользователь {user.full_name} отправил документ-подтверждение по заказу №{order['id']}")
    except ValueError as e:
        await send_user_screen(context, user.id, str(e), reply_markup=main_menu_keyboard())
        context.user_data.pop(UD_WAITING_PAYMENT_PROOF_ORDER_ID, None)
        return True

    await send_user_screen(
        context,
        user.id,
        f"✅ Подтверждение оплаты по заказу №{order['id']} отправлено.\nЯ проверю его и свяжусь с тобой.",
        reply_markup=main_menu_keyboard(),
    )
    context.user_data.pop(UD_WAITING_PAYMENT_PROOF_ORDER_ID, None)
    return True


async def forward_user_message_to_admin(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    message = update.message
    user = update.effective_user

    if user.id == ADMIN_ID:
        return

    upsert_user(user.id, user.username, user.full_name)

    if is_user_rate_limited(user.id, message.text or message.caption or ""):
        await send_user_screen(
            context,
            user.id,
            "Ты отправляешь сообщения слишком часто. Подожди несколько секунд и попробуй снова.",
            reply_markup=main_menu_keyboard(),
        )
        return

    waiting_order_id = context.user_data.get(UD_WAITING_PAYMENT_PROOF_ORDER_ID)
    if waiting_order_id:
        handled = await handle_payment_proof(update, context, waiting_order_id)
        if handled:
            return

    file_size = get_message_file_size(message)
    if file_size > MAX_FILE_SIZE_BYTES:
        await send_user_screen(
            context,
            user.id,
            f"Файл слишком большой. Максимум: {MAX_FILE_SIZE_MB} МБ.",
            reply_markup=main_menu_keyboard(),
        )
        return

    caption_or_text = message.text or message.caption or ""

    if message.text:
        update_dialog(user.id, text_preview(caption_or_text), "text")
        sent = await context.bot.send_message(
            chat_id=ADMIN_ID,
            text=f"{build_user_header(user)}\n\nТекст:\n{message.text}",
            reply_markup=admin_reply_keyboard(user.id),
        )
        save_admin_message_link(sent.message_id, user.id, message_kind="user_text")

    elif message.photo:
        update_dialog(user.id, text_preview(caption_or_text) or "[Фото]", "photo")
        sent = await context.bot.send_photo(
            chat_id=ADMIN_ID,
            photo=message.photo[-1].file_id,
            caption=(
                f"{build_user_header(user)}\n\n"
                f"Тип: Фото\n"
                f"Подпись: {caption_or_text or 'нет'}"
            ),
            reply_markup=admin_reply_keyboard(user.id),
        )
        save_admin_message_link(sent.message_id, user.id, message_kind="user_photo")

    elif message.video:
        update_dialog(user.id, text_preview(caption_or_text) or "[Видео]", "video")
        sent = await context.bot.send_video(
            chat_id=ADMIN_ID,
            video=message.video.file_id,
            caption=(
                f"{build_user_header(user)}\n\n"
                f"Тип: Видео\n"
                f"Подпись: {caption_or_text or 'нет'}"
            ),
            reply_markup=admin_reply_keyboard(user.id),
        )
        save_admin_message_link(sent.message_id, user.id, message_kind="user_video")

    elif message.voice:
        update_dialog(user.id, "[Голосовое сообщение]", "voice")
        sent = await context.bot.send_voice(
            chat_id=ADMIN_ID,
            voice=message.voice.file_id,
            caption=f"{build_user_header(user)}\n\nТип: Голосовое сообщение",
            reply_markup=admin_reply_keyboard(user.id),
        )
        save_admin_message_link(sent.message_id, user.id, message_kind="user_voice")

    elif message.document:
        preview = text_preview(caption_or_text) or f"[Документ: {message.document.file_name or 'файл'}]"
        update_dialog(user.id, preview, "document")
        sent = await context.bot.send_document(
            chat_id=ADMIN_ID,
            document=message.document.file_id,
            caption=(
                f"{build_user_header(user)}\n\n"
                f"Тип: Документ\n"
                f"Файл: {message.document.file_name or 'без названия'}\n"
                f"Подпись: {caption_or_text or 'нет'}"
            ),
            reply_markup=admin_reply_keyboard(user.id),
        )
        save_admin_message_link(sent.message_id, user.id, message_kind="user_document")

    elif message.sticker:
        update_dialog(user.id, "[Стикер]", "sticker")
        sent = await context.bot.send_sticker(
            chat_id=ADMIN_ID,
            sticker=message.sticker.file_id,
            reply_markup=admin_reply_keyboard(user.id),
        )
        save_admin_message_link(sent.message_id, user.id, message_kind="user_sticker")

        sent_info = await context.bot.send_message(
            chat_id=ADMIN_ID,
            text=f"{build_user_header(user)}\n\nТип: Стикер",
            reply_markup=admin_reply_keyboard(user.id),
        )
        save_admin_message_link(sent_info.message_id, user.id, message_kind="user_sticker_info")

    elif message.audio:
        update_dialog(user.id, "[Аудио]", "audio")
        sent = await context.bot.send_audio(
            chat_id=ADMIN_ID,
            audio=message.audio.file_id,
            caption=caption_or_text or "",
            reply_markup=admin_reply_keyboard(user.id),
        )
        save_admin_message_link(sent.message_id, user.id, message_kind="user_audio")

        sent_info = await context.bot.send_message(
            chat_id=ADMIN_ID,
            text=f"{build_user_header(user)}\n\nТип: Аудио",
            reply_markup=admin_reply_keyboard(user.id),
        )
        save_admin_message_link(sent_info.message_id, user.id, message_kind="user_audio_info")

    else:
        update_dialog(user.id, "[Неподдерживаемый тип сообщения]", "other")
        sent = await context.bot.send_message(
            chat_id=ADMIN_ID,
            text=f"{build_user_header(user)}\n\nПришёл неподдерживаемый тип сообщения.",
            reply_markup=admin_reply_keyboard(user.id),
        )
        save_admin_message_link(sent.message_id, user.id, message_kind="user_other")

    await send_user_screen(
        context,
        user.id,
        "✅ Мы получили твоё сообщение.\nАдминистратор скоро ответит.",
        reply_markup=main_menu_keyboard(),
    )


# =========================
# ADMIN COMMANDS
# =========================

def build_crm_list_text(rows, title: str) -> str:
    if not rows:
        return f"{title}\n\nНичего не найдено."

    parts = [f"{title}\n"]
    for order in rows[:20]:
        full_name = order["full_name"] if "full_name" in order.keys() and order["full_name"] else "Неизвестно"
        parts.append(
            f"№{order['id']} | {order['service_title']} | {order['service_price']}\n"
            f"Клиент: {full_name} | ID: {order['user_id']}\n"
            f"Статус: {get_status_text(order['status'])}\n"
        )
    return "\n".join(parts)


def build_crm_order_text(order, user_info) -> str:
    username = f"@{user_info['username']}" if user_info and user_info["username"] else "нет"
    full_name = user_info["full_name"] if user_info and user_info["full_name"] else "Неизвестно"

    return (
        f"📦 Заказ №{order['id']}\n\n"
        f"Клиент: {full_name}\n"
        f"ID: {order['user_id']}\n"
        f"Username: {username}\n"
        f"Услуга: {order['service_title']}\n"
        f"Код услуги: {order['service_code']}\n"
        f"Цена: {order['service_price']}\n"
        f"Статус: {get_status_text(order['status'])}\n"
        f"Тип подтверждения: {order['proof_type'] or 'нет'}\n"
        f"Примечание: {order['proof_note'] or 'нет'}\n"
        f"Попыток подтверждения: {order['proof_attempts'] or 0}\n"
        f"Создан: {order['created_at']}"
    )

async def crm_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.effective_user.id != ADMIN_ID:
        return

    stats = get_stats()

    await update.message.reply_text(
        "📊 CRM\n\n"
        f"Пользователей: {stats['users_count']}\n"
        f"Активных диалогов: {stats['dialogs_count']}\n"
        f"Всего заказов: {stats['orders_count']}\n"
        f"Завершённая выручка: {stats['total_revenue']}₽\n\n"
        "Выбери раздел:",
        reply_markup=crm_main_keyboard(),
    )

async def dialogs_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.effective_user.id != ADMIN_ID:
        return

    dialogs = get_recent_dialogs()

    if not dialogs:
        await update.message.reply_text("Диалогов пока нет.")
        return

    buttons = []
    text_parts = ["Последние диалоги:\n"]

    for row in dialogs:
        username = f"@{row['username']}" if row["username"] else "нет"
        preview = (row["last_message_text"] or "[без текста]")[:40]
        state_label = "🟢 открыт" if (row["state"] or "open") == "open" else "⚫ закрыт"

        text_parts.append(
            f"ID: {row['user_id']} | {row['full_name']} | {username} | {state_label}\n"
            f"Последнее: {preview}\n"
        )

        buttons.append([
            InlineKeyboardButton(
                f"{row['full_name']} ({row['user_id']})",
                callback_data=f"reply:{row['user_id']}",
            )
        ])

    await update.message.reply_text(
        "\n".join(text_parts),
        reply_markup=InlineKeyboardMarkup(buttons),
    )


async def orders_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.effective_user.id != ADMIN_ID:
        return

    status = None
    if context.args:
        status = context.args[0].strip()

    rows = get_orders(limit=30, status=status)

    if not rows:
        if status:
            await update.message.reply_text(f"Заказов со статусом '{status}' не найдено.")
        else:
            await update.message.reply_text("Заказов пока нет.")
        return

    parts = ["Последние заказы:\n"]
    for row in rows:
        parts.append(
            f"№{row['id']} | {row['service_title']} | {row['service_price']}\n"
            f"Статус: {get_status_text(row['status'])}\n"
            f"Клиент: {row['full_name']} | ID: {row['user_id']}\n"
        )

    await update.message.reply_text("\n".join(parts))


async def order_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.effective_user.id != ADMIN_ID:
        return

    if not context.args:
        await update.message.reply_text("Использование: /order 15")
        return

    try:
        order_id = int(context.args[0])
    except ValueError:
        await update.message.reply_text("ID заказа должен быть числом.")
        return

    order = get_order(order_id)
    if not order:
        await update.message.reply_text("Заказ не найден.")
        return

    user_info = get_user_info(order["user_id"])
    username = f"@{user_info['username']}" if user_info and user_info["username"] else "нет"
    full_name = user_info["full_name"] if user_info and user_info["full_name"] else "Неизвестно"
    proofs = get_order_payment_proofs(order_id, 3)

    text = (
        f"Заказ №{order['id']}\n"
        f"Клиент: {full_name}\n"
        f"ID: {order['user_id']}\n"
        f"Username: {username}\n"
        f"Услуга: {order['service_title']}\n"
        f"Код услуги: {order['service_code']}\n"
        f"Цена: {order['service_price']}\n"
        f"Статус: {get_status_text(order['status'])}\n"
        f"Тип подтверждения: {order['proof_type'] or 'нет'}\n"
        f"Примечание: {order['proof_note'] or 'нет'}\n"
        f"Попыток подтверждения: {order['proof_attempts'] or 0}\n"
        f"Создан: {order['created_at']}\n"
        f"Последних подтверждений: {len(proofs)}"
    )

    await update.message.reply_text(
        text,
        reply_markup=admin_order_status_keyboard(order["id"], order["user_id"]),
    )


async def stats_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.effective_user.id != ADMIN_ID:
        return

    stats = get_stats()

    parts = [
        "📊 Статистика\n",
        f"Пользователей: {stats['users_count']}",
        f"Активных диалогов: {stats['dialogs_count']}",
        f"Всего заказов: {stats['orders_count']}",
        f"Завершённая выручка: {stats['total_revenue']}₽",
        "",
        "По статусам:",
    ]

    if stats["by_status"]:
        for row in stats["by_status"]:
            parts.append(f"- {get_status_text(row['status'])}: {row['cnt']}")
    else:
        parts.append("- данных пока нет")

    await update.message.reply_text("\n".join(parts))


async def history_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.effective_user.id != ADMIN_ID:
        return
    if not context.args:
        await update.message.reply_text("Использование: /history 15")
        return
    try:
        order_id = int(context.args[0])
    except ValueError:
        await update.message.reply_text("ID заказа должен быть числом.")
        return

    rows = get_order_status_history(order_id, 20)
    await update.message.reply_text(f"История заказа №{order_id}:\n\n{format_history_rows(rows)}")


async def close_dialog_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.effective_user.id != ADMIN_ID:
        return
    if not context.args:
        await update.message.reply_text("Использование: /close_dialog USER_ID")
        return
    try:
        user_id = int(context.args[0])
    except ValueError:
        await update.message.reply_text("USER_ID должен быть числом.")
        return
    set_dialog_state(user_id, "closed")
    await update.message.reply_text(f"Диалог с пользователем {user_id} закрыт.")


async def open_dialog_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.effective_user.id != ADMIN_ID:
        return
    if not context.args:
        await update.message.reply_text("Использование: /open_dialog USER_ID")
        return
    try:
        user_id = int(context.args[0])
    except ValueError:
        await update.message.reply_text("USER_ID должен быть числом.")
        return
    set_dialog_state(user_id, "open")
    await update.message.reply_text(f"Диалог с пользователем {user_id} открыт.")


async def broadcast_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.effective_user.id != ADMIN_ID:
        return

    upsert_admin_state(ADMIN_ID, broadcast_mode=1, broadcast_text=None)
    context.user_data[UD_BROADCAST_MODE] = True
    context.user_data.pop(UD_BROADCAST_TEXT, None)
    await update.message.reply_text(
        "Режим безопасной рассылки включён.\n"
        "Теперь отправь ОДНО текстовое сообщение.\n"
        "Я покажу предпросмотр, количество получателей и попрошу подтверждение.\n"
        "Для отмены используй /cancel"
    )


async def cancel_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.effective_user.id != ADMIN_ID:
        return

    context.user_data.pop(UD_REPLY_TO_USER_ID, None)
    context.user_data.pop(UD_SEND_READY_FILE_ORDER_ID, None)
    context.user_data.pop(UD_SEND_READY_FILE_USER_ID, None)
    context.user_data.pop(UD_BROADCAST_MODE, None)
    context.user_data.pop(UD_BROADCAST_TEXT, None)

    clear_admin_state(
        ADMIN_ID,
        "reply_to_user_id",
        "send_ready_file_order_id",
        "send_ready_file_user_id",
        "broadcast_mode",
        "broadcast_text",
    )

    await update.message.reply_text("Режимы выключены.")


def expire_old_orders(changed_by_user_id: Optional[int] = None) -> int:
    modifier = f'-{ORDER_EXPIRATION_HOURS} hours'
    with db_cursor() as (_, cur):
        cur.execute(
            f"""
            SELECT id, user_id, status
            FROM orders
            WHERE status IN (?, ?)
              AND datetime(created_at) <= datetime('now', ?)
            """,
            (STATUS_AWAITING_PAYMENT, STATUS_AWAITING_PROOF, modifier),
        )
        rows = cur.fetchall()

    expired_count = 0
    for row in rows:
        try:
            update_order_status(
                row["id"],
                STATUS_EXPIRED,
                force=False,
                changed_by_user_id=changed_by_user_id,
                note="Срок оплаты/подтверждения истёк",
            )
            expired_count += 1
        except ValueError:
            continue
    return expired_count


async def expire_orders_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.effective_user.id != ADMIN_ID:
        return

    expired_count = expire_old_orders(changed_by_user_id=ADMIN_ID)
    await update.message.reply_text(
        f"Помечено просроченными: {expired_count} (порог: {ORDER_EXPIRATION_HOURS} ч.)"
    )


async def auto_expire_orders_job(context: ContextTypes.DEFAULT_TYPE) -> None:
    expired_count = expire_old_orders(changed_by_user_id=ADMIN_ID)
    if expired_count > 0:
        logger.info("Автопроверка просроченных заказов: %s", expired_count)
        try:
            await notify_admin(
                context,
                f"⏰ Автопроверка: помечено просроченными {expired_count} заказ(ов).",
            )
        except Exception:
            logger.exception("Не удалось отправить уведомление админу о просроченных заказах")


async def send_admin_content_to_user(message, context: ContextTypes.DEFAULT_TYPE, user_id: int) -> bool:
    if get_message_file_size(message) > MAX_FILE_SIZE_BYTES:
        raise ValueError(f"Файл слишком большой для отправки. Максимум: {MAX_FILE_SIZE_MB} МБ.")

    if message.text:
        await context.bot.send_message(chat_id=user_id, text=message.text)
        update_dialog(user_id, f"[Ответ админа] {text_preview(message.text)}", "admin_text")
        return True

    if message.photo:
        await context.bot.send_photo(
            chat_id=user_id,
            photo=message.photo[-1].file_id,
            caption=message.caption or "",
        )
        update_dialog(user_id, "[Ответ админа: Фото]", "admin_photo")
        return True

    if message.video:
        await context.bot.send_video(
            chat_id=user_id,
            video=message.video.file_id,
            caption=message.caption or "",
        )
        update_dialog(user_id, "[Ответ админа: Видео]", "admin_video")
        return True

    if message.voice:
        await context.bot.send_voice(chat_id=user_id, voice=message.voice.file_id)
        update_dialog(user_id, "[Ответ админа: Голосовое]", "admin_voice")
        return True

    if message.audio:
        await context.bot.send_audio(
            chat_id=user_id,
            audio=message.audio.file_id,
            caption=message.caption or "",
        )
        update_dialog(user_id, "[Ответ админа: Аудио]", "admin_audio")
        return True

    if message.document:
        await context.bot.send_document(
            chat_id=user_id,
            document=message.document.file_id,
            caption=message.caption or "",
        )
        update_dialog(user_id, "[Ответ админа: Документ]", "admin_document")
        return True

    if message.sticker:
        await context.bot.send_sticker(chat_id=user_id, sticker=message.sticker.file_id)
        update_dialog(user_id, "[Ответ админа: Стикер]", "admin_sticker")
        return True

    return False


async def admin_reply(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    message = update.message

    if update.effective_user.id != ADMIN_ID:
        return

    state = get_admin_state(ADMIN_ID)

    if state.get("broadcast_mode"):
        if not state.get("broadcast_text"):
            if not message.text:
                await message.reply_text("Для рассылки сейчас поддерживается только текст.")
                return

            upsert_admin_state(ADMIN_ID, broadcast_mode=1, broadcast_text=message.text)
            recipients = get_active_user_ids() or get_all_user_ids()
            preview_receivers = ", ".join(str(uid) for uid in recipients[:5]) or "нет"
            await message.reply_text(
                "Предпросмотр рассылки:\n\n"
                f"{message.text}\n\n"
                f"Получателей: {len(recipients)}\n"
                f"Первые получатели: {preview_receivers}\n\n"
                "Подтверди отправку или отмени.",
                reply_markup=broadcast_confirm_keyboard(),
            )
            return

        await message.reply_text(
            "У тебя уже есть подготовленная рассылка.\n"
            "Подтверди её кнопкой ниже или отмени через /cancel.",
            reply_markup=broadcast_confirm_keyboard(),
        )
        return

    send_ready_file_order_id = state.get("send_ready_file_order_id")
    send_ready_file_user_id = state.get("send_ready_file_user_id")

    if send_ready_file_order_id and send_ready_file_user_id:
        order = get_order(send_ready_file_order_id)

        try:
            sent_ok = await send_admin_content_to_user(message, context, send_ready_file_user_id)
        except ValueError as e:
            await message.reply_text(str(e))
            return

        if not sent_ok:
            await message.reply_text("Этот тип файла пока не поддерживается для отправки клиенту.")
            return

        if order:
            try:
                update_order_status(
                    send_ready_file_order_id,
                    STATUS_DONE,
                    order["proof_type"],
                    order["proof_note"],
                    changed_by_user_id=ADMIN_ID,
                    note="Результат отправлен клиенту",
                )
            except ValueError as e:
                await message.reply_text(str(e))
                return

            await send_user_screen(
                context,
                send_ready_file_user_id,
                f"✅ Заказ №{send_ready_file_order_id} завершён.\n"
                "Если понадобятся правки или уточнения, просто напиши в этот чат.",
                reply_markup=main_menu_keyboard(),
            )
            await notify_admin(context, f"✅ Заказ №{send_ready_file_order_id} завершён и отправлен клиенту.")

        clear_admin_state(ADMIN_ID, "send_ready_file_order_id", "send_ready_file_user_id")
        context.user_data.pop(UD_SEND_READY_FILE_ORDER_ID, None)
        context.user_data.pop(UD_SEND_READY_FILE_USER_ID, None)

        await message.reply_text("Готовый файл или сообщение отправлены клиенту.")
        return

    user_id = None

    if message.reply_to_message:
        user_id = get_user_id_by_admin_message(message.reply_to_message.message_id)

    if not user_id:
        user_id = state.get("reply_to_user_id") or context.user_data.get(UD_REPLY_TO_USER_ID)

    if not user_id:
        await message.reply_text(
            "Не выбран диалог.\n"
            "Нажми кнопку «Ответить», используй /dialogs или ответь reply на сообщение пользователя."
        )
        return

    try:
        sent_ok = await send_admin_content_to_user(message, context, user_id)
    except ValueError as e:
        await message.reply_text(str(e))
        return

    if not sent_ok:
        await message.reply_text("Этот тип сообщения пока не поддерживается.")
        return

    clear_admin_state(ADMIN_ID, "reply_to_user_id")
    context.user_data.pop(UD_REPLY_TO_USER_ID, None)
    await message.reply_text("Ответ отправлен. Режим ответа сброшен.")


# =========================
# CALLBACKS
# =========================

async def button_click(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()

    data = query.data
    if not data:
        return

    user_id = query.from_user.id

    if data == "crm:main":
        if user_id != ADMIN_ID:
            return
        await query.edit_message_text(
    "📊 CRM\n\nВыбери раздел:",
    reply_markup=crm_main_keyboard(),
)

        return

    if data.startswith("crm:list:"):
        if user_id != ADMIN_ID:
            return
        status = data.split(":", 2)[2]
        rows = get_orders(limit=20, status=None if status == "all" else status)
        titles = {
            "all": "📋 Все заказы",
            STATUS_AWAITING_PAYMENT: "💳 Ожидают оплату",
            STATUS_AWAITING_PROOF: "🧾 Ожидают чек",
            STATUS_PROOF_SENT: "📨 Чек отправлен",
            STATUS_IN_PROGRESS: "🛠 В работе",
            STATUS_READY_TO_SEND: "📦 Готовы к отправке",
            STATUS_DONE: "✅ Завершённые",
            STATUS_CANCELLED: "🚫 Отменённые",
            STATUS_EXPIRED: "⌛ Просроченные",
        }
        await query.edit_message_text(
            build_crm_list_text(rows, titles.get(status, "📋 Заказы")),
            reply_markup=crm_orders_list_keyboard(rows),
        )
        return

    if data.startswith("crm:order:"):
        if user_id != ADMIN_ID:
            return
        order_id = int(data.split(":", 2)[2])
        order = get_order(order_id)
        if not order:
            await query.edit_message_text("Заказ не найден.", reply_markup=crm_main_keyboard())
            return
        user_info = get_user_info(order["user_id"])
        await query.edit_message_text(
            build_crm_order_text(order, user_info),
            reply_markup=crm_order_keyboard(order["id"], order["user_id"], order["status"]),
        )
        return

    if data.startswith("crm:history:"):
        if user_id != ADMIN_ID:
            return
        order_id = int(data.split(":", 2)[2])
        rows = get_order_status_history(order_id)
        if not rows:
            text = f"История по заказу №{order_id} пуста."
        else:
            parts = [f"📜 История заказа №{order_id}\n"]
            for row in rows[:20]:
                parts.append(
    f"{row['created_at']}\n"
    f"{get_status_text(row['old_status']) if row['old_status'] else '—'} → {get_status_text(row['new_status'])}\n"
    f"Комментарий: {row['note'] or 'нет'}\n"
)
            text = "\n".join(parts)
        order = get_order(order_id)
        back_markup = crm_order_keyboard(order_id, order['user_id'], order['status']) if order else crm_main_keyboard()
        await query.edit_message_text(text, reply_markup=back_markup)
        return

    if data == "main:order":
        await show_order_menu(context, user_id, query=query)
        return

    if data == "main:my_orders":
        orders = get_user_orders(user_id, 10)
        if not orders:
            await edit_or_send(query, context, user_id, "У тебя пока нет заказов.", reply_markup=main_menu_keyboard())
            return
        parts = ["Твои последние заказы:\n"]
        for order in orders:
            parts.append(
                f"№{order['id']} | {order['service_title']} | {order['service_price']}\n"
                f"Статус: {get_status_text(order['status'])}\n"
                f"Создан: {order['created_at']}\n"
            )
        await edit_or_send(query, context, user_id, "\n".join(parts), reply_markup=main_menu_keyboard())
        return

    if data == "main:write":
        await edit_or_send(
            query,
            context,
            user_id,
            "Отправь сообщение. Администратор скоро ответит.",
            reply_markup=main_menu_keyboard(),
        )
        return

    if data == "main:support":
        await edit_or_send(
            query,
            context,
            user_id,
            "Опиши свой вопрос одним сообщением, либо отправь фото, видео, голосовое или документ.",
            reply_markup=main_menu_keyboard(),
        )
        return

    if data == "main:price":
        await edit_or_send(
            query,
            context,
            user_id,
            full_price_text(),
            reply_markup=main_menu_keyboard(),
        )
        return

    if data == "broadcast:cancel":
        if user_id != ADMIN_ID:
            return
        clear_admin_state(ADMIN_ID, "broadcast_mode", "broadcast_text")
        context.user_data.pop(UD_BROADCAST_MODE, None)
        context.user_data.pop(UD_BROADCAST_TEXT, None)
        await query.message.reply_text("Рассылка отменена.")
        return

    if data == "broadcast:confirm":
        if user_id != ADMIN_ID:
            return

        state = get_admin_state(ADMIN_ID)
        broadcast_text = state.get("broadcast_text")
        if not broadcast_text:
            await query.message.reply_text("Нет подготовленного текста для рассылки.")
            return

        recipients = get_active_user_ids() or get_all_user_ids()
        sent_count = 0
        failed_count = 0

        for target_user_id in recipients:
            try:
                await context.bot.send_message(
                    chat_id=target_user_id,
                    text="ℹ️ Массовая рассылка\n\n" + broadcast_text,
                )
                sent_count += 1
            except Exception as e:
                logger.warning("Не удалось отправить рассылку пользователю %s: %s", target_user_id, e)
                failed_count += 1

        clear_admin_state(ADMIN_ID, "broadcast_mode", "broadcast_text")
        context.user_data.pop(UD_BROADCAST_MODE, None)
        context.user_data.pop(UD_BROADCAST_TEXT, None)
        await query.message.reply_text(
            f"Рассылка завершена.\nУспешно: {sent_count}\nОшибок: {failed_count}"
        )
        return

    if data.startswith("dialog_close:"):
        if user_id != ADMIN_ID:
            return
        target_user_id = int(data.split(":", 1)[1])
        set_dialog_state(target_user_id, "closed")
        await query.message.reply_text(f"Диалог с пользователем {target_user_id} закрыт.")
        return

    if data.startswith("reply:"):
        if user_id != ADMIN_ID:
            return

        target_user_id = int(data.split(":")[1])
        context.user_data[UD_REPLY_TO_USER_ID] = target_user_id
        upsert_admin_state(ADMIN_ID, reply_to_user_id=target_user_id)
        set_dialog_state(target_user_id, "open")

        user_info = get_user_info(target_user_id)
        if user_info:
            username = f"@{user_info['username']}" if user_info["username"] else "нет"
            text = (
                "Режим ответа включён.\n"
                f"Кому: {user_info['full_name']} | ID: {user_info['user_id']} | {username}\n\n"
                "Теперь просто отправь сообщение, фото, видео, голосовое, документ, аудио или стикер."
            )
        else:
            text = "Режим ответа включён."

        await query.message.reply_text(text)
        return

    if data.startswith("history:"):
        if user_id != ADMIN_ID:
            return
        order_id = int(data.split(":", 1)[1])
        rows = get_order_status_history(order_id, 20)
        await query.message.reply_text(f"История заказа №{order_id}:\n\n{format_history_rows(rows)}")
        return

    if data.startswith("status:"):
        if user_id != ADMIN_ID:
            return

        parts = data.split(":")
        if len(parts) != 3:
            await query.message.reply_text("Ошибка статуса.")
            return

        order_id = int(parts[1])
        new_status = parts[2]
        order = get_order(order_id)

        if not order:
            await query.message.reply_text("Заказ не найден.")
            return

        current_status = order["status"]

        if not can_transition_order(current_status, new_status) and new_status != current_status:
            await query.message.reply_text(
                f"Недопустимый переход статуса: {get_status_text(current_status)} → {get_status_text(new_status)}"
            )
            return

        try:
            if new_status == STATUS_PAYMENT_CONFIRMED:
                update_order_status(
                    order_id,
                    STATUS_PAYMENT_CONFIRMED,
                    order["proof_type"],
                    order["proof_note"],
                    changed_by_user_id=ADMIN_ID,
                    note="Администратор подтвердил оплату",
                )
                await send_user_screen(
                    context,
                    order["user_id"],
                    "✅ Оплата подтверждена.\n"
                    f"Заказ №{order_id}\n"
                    "Спасибо. Скоро начну работу по заказу:\n"
                    f"{order['service_title']}",
                    reply_markup=main_menu_keyboard(),
                )
                await notify_admin(context, f"💵 Заказ №{order_id}: оплата подтверждена.")
                await query.message.reply_text(
                    f"Статус заказа №{order_id} обновлён: {get_status_text(STATUS_PAYMENT_CONFIRMED)}"
                )
                return

            if new_status == STATUS_PAYMENT_NOT_FOUND:
                update_order_status(
                    order_id,
                    STATUS_PAYMENT_NOT_FOUND,
                    order["proof_type"],
                    order["proof_note"],
                    changed_by_user_id=ADMIN_ID,
                    note="Администратор не нашёл оплату",
                )
                await send_user_screen(
                    context,
                    order["user_id"],
                    "❌ Я пока не смог подтвердить оплату.\n"
                    f"Заказ №{order_id}\n"
                    "Пожалуйста, проверь перевод и при необходимости отправь скрин ещё раз.",
                    reply_markup=main_menu_keyboard(),
                )
                await notify_admin(context, f"⚠️ Заказ №{order_id}: оплата не найдена.")
                await query.message.reply_text(
                    f"Статус заказа №{order_id} обновлён: {get_status_text(STATUS_PAYMENT_NOT_FOUND)}"
                )
                return

            if new_status == STATUS_IN_PROGRESS:
                update_order_status(
                    order_id,
                    STATUS_IN_PROGRESS,
                    order["proof_type"],
                    order["proof_note"],
                    changed_by_user_id=ADMIN_ID,
                    note="Заказ взят в работу",
                )
                await send_user_screen(
                    context,
                    order["user_id"],
                    f"🛠 Твой заказ №{order_id} уже в работе.\nУслуга: {order['service_title']}",
                    reply_markup=main_menu_keyboard(),
                )
                await notify_admin(context, f"🛠 Заказ №{order_id} переведён в работу.")
                await query.message.reply_text(
                    f"Статус заказа №{order_id} обновлён: {get_status_text(STATUS_IN_PROGRESS)}"
                )
                return

            if new_status == STATUS_READY_TO_SEND:
                update_order_status(
                    order_id,
                    STATUS_READY_TO_SEND,
                    order["proof_type"],
                    order["proof_note"],
                    changed_by_user_id=ADMIN_ID,
                    note="Заказ готов к отправке",
                )
                context.user_data[UD_SEND_READY_FILE_ORDER_ID] = order_id
                context.user_data[UD_SEND_READY_FILE_USER_ID] = order["user_id"]
                upsert_admin_state(ADMIN_ID, send_ready_file_order_id=order_id, send_ready_file_user_id=order["user_id"])

                await send_user_screen(
                    context,
                    order["user_id"],
                    f"📦 Заказ №{order_id} готов.\nСейчас отправлю результат отдельным сообщением.",
                    reply_markup=main_menu_keyboard(),
                )

                await notify_admin(context, f"📦 Заказ №{order_id} готов к отправке клиенту.")
                await query.message.reply_text(
                    f"Статус заказа №{order_id} обновлён: {get_status_text(STATUS_READY_TO_SEND)}\n\n"
                    "Теперь отправь клиенту готовый файл или сообщение."
                )
                return

            if new_status == STATUS_CANCELLED:
                update_order_status(
                    order_id,
                    STATUS_CANCELLED,
                    order["proof_type"],
                    order["proof_note"],
                    changed_by_user_id=ADMIN_ID,
                    note="Заказ отменён администратором",
                )
                await send_user_screen(
                    context,
                    order["user_id"],
                    f"🚫 Заказ №{order_id} отменён.\nЕсли это ошибка, напиши в этот чат.",
                    reply_markup=main_menu_keyboard(),
                )
                await query.message.reply_text(
                    f"Статус заказа №{order_id} обновлён: {get_status_text(STATUS_CANCELLED)}"
                )
                return
        except ValueError as e:
            await query.message.reply_text(str(e))
            return

        await query.message.reply_text("Неизвестный статус.")
        return

    if data == "menu:beats":
        await edit_or_send(
            query,
            context,
            user_id,
            "🎵 Выбери тип битов:",
            reply_markup=beats_keyboard(),
        )
        return

    if data == "menu:mix":
        await edit_or_send(
            query,
            context,
            user_id,
            "🎚 Выбери услугу по сведению:",
            reply_markup=mix_keyboard(),
        )
        return

    if data == "menu:price":
        await edit_or_send(
            query,
            context,
            user_id,
            full_price_text(),
            reply_markup=order_menu_keyboard(),
        )
        return

    if data == "menu:terms":
        await edit_or_send(
            query,
            context,
            user_id,
            "⏳ Сроки выполнения:\n\nОт 4 дней до 1 недели.",
            reply_markup=order_menu_keyboard(),
        )
        return

    if data == "menu:back_order":
        await show_order_menu(context, user_id, query=query)
        return

    if data == "menu:back_to_main":
        await show_main_menu(context, user_id, query=query)
        return

    if data.startswith("order:"):
        service_code = data.split(":", 1)[1]
        service = service_catalog().get(service_code)

        if not service:
            await edit_or_send(
                query,
                context,
                user_id,
                "Услуга не найдена.",
                reply_markup=main_menu_keyboard(),
            )
            return

        await edit_or_send(
            query,
            context,
            user_id,
            service["user_text"],
            reply_markup=order_confirm_keyboard(service_code),
        )
        return

    if data.startswith("confirm:"):
        service_code = data.split(":", 1)[1]
        service = service_catalog().get(service_code)

        if not service:
            await edit_or_send(
                query,
                context,
                user_id,
                "Услуга не найдена.",
                reply_markup=main_menu_keyboard(),
            )
            return

        user = query.from_user
        upsert_user(user.id, user.username, user.full_name)

        initial_status = STATUS_AWAITING_PAYMENT if service["requires_payment"] else STATUS_NEW
        order_id = create_order(
            user_id=user.id,
            service_code=service_code,
            service_title=service["title"],
            service_price=service["price"],
            status=initial_status,
        )
        add_order_status_history(order_id, None, initial_status, user.id, "Создание заказа")

        update_dialog(user.id, f"[Заявка] {service['title']}", "order")
        await notify_admin_about_order(context, order_id, user, service["title"], service["price"], initial_status)

        if service["requires_payment"]:
            await edit_or_send(
                query,
                context,
                user_id,
                "✅ Заявка отправлена.\n\n"
                f"Заказ №{order_id}\n"
                f"Услуга: {service['title']}\n"
                f"Цена: {service['price']}\n"
                f"Статус: {get_status_text(STATUS_AWAITING_PAYMENT)}\n\n"
                f"{PAYMENT_DETAILS}",
                reply_markup=payment_wait_keyboard(order_id),
            )
        else:
            await edit_or_send(
                query,
                context,
                user_id,
                "✅ Заявка отправлена.\n\n"
                f"Заказ №{order_id}\n"
                f"Услуга: {service['title']}\n"
                "Цена обсуждается лично. Я свяжусь с тобой в этом чате.",
                reply_markup=main_menu_keyboard(),
            )
        return

    if data.startswith("paid:"):
        order_id = int(data.split(":", 1)[1])
        order = get_order(order_id)

        if not order:
            await edit_or_send(
                query,
                context,
                user_id,
                "Заказ не найден.",
                reply_markup=main_menu_keyboard(),
            )
            return

        if order["user_id"] != user_id:
            await edit_or_send(
                query,
                context,
                user_id,
                "Это не твой заказ.",
                reply_markup=main_menu_keyboard(),
            )
            return

        if not is_payment_required(order["service_code"]):
            await edit_or_send(
                query,
                context,
                user_id,
                "Для этого заказа оплата не требуется на этом этапе. Я свяжусь с тобой для обсуждения деталей.",
                reply_markup=main_menu_keyboard(),
            )
            return

        if order["status"] in {STATUS_PROOF_SENT, STATUS_PAYMENT_CONFIRMED, STATUS_IN_PROGRESS, STATUS_READY_TO_SEND, STATUS_DONE}:
            await edit_or_send(
                query,
                context,
                user_id,
                f"По заказу №{order_id} подтверждение уже отправлено или заказ уже обрабатывается.\n"
                "Если нужно, просто пришли новый скрин или PDF-документ в этот чат.",
                reply_markup=main_menu_keyboard(),
            )
            context.user_data[UD_WAITING_PAYMENT_PROOF_ORDER_ID] = order_id
            return

        try:
            update_order_status(
                order_id,
                STATUS_AWAITING_PROOF,
                order["proof_type"],
                order["proof_note"],
                changed_by_user_id=user_id,
                note="Пользователь нажал кнопку 'Я оплатил'",
            )
        except ValueError as e:
            await edit_or_send(query, context, user_id, str(e), reply_markup=main_menu_keyboard())
            return

        context.user_data[UD_WAITING_PAYMENT_PROOF_ORDER_ID] = order_id

        await edit_or_send(
            query,
            context,
            user_id,
            "💸 Отлично.\n"
            f"Заказ №{order_id}\n\n"
            "Теперь пришли одним сообщением:\n\n"
            "• скрин оплаты как фото\n"
            "или\n"
            "• PDF-документ с чеком/договором\n\n"
            "Я передам это на проверку.",
        )
        return


# =========================
# ERROR HANDLER
# =========================

async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    logger.exception("Необработанная ошибка", exc_info=context.error)


# =========================
# MAIN
# =========================

async def setup_bot_commands(app: Application) -> None:
    user_commands = [
        BotCommand("start", "Запустить бота"),
        BotCommand("myorders", "Мои заказы"),
        BotCommand("cancel", "Отменить текущий режим"),
    ]

    admin_commands = [
        BotCommand("start", "Запустить бота"),
        BotCommand("dialogs", "Последние диалоги"),
        BotCommand("orders", "Список заказов"),
        BotCommand("order", "Открыть заказ по ID"),
        BotCommand("stats", "Статистика"),
        BotCommand("broadcast", "Сделать рассылку"),
        BotCommand("crm", "Открыть CRM"),
        BotCommand("history", "История статусов заказа"),
        BotCommand("close_dialog", "Закрыть диалог"),
        BotCommand("open_dialog", "Открыть диалог"),
        BotCommand("expire_orders", "Пометить просроченные"),
        BotCommand("cancel", "Отменить текущий режим"),
        BotCommand("myorders", "Мои заказы"),
    ]

    await app.bot.set_my_commands(
        user_commands,
        scope=BotCommandScopeDefault(),
    )

    await app.bot.set_my_commands(
        admin_commands,
        scope=BotCommandScopeChat(chat_id=ADMIN_ID),
    )

def build_application() -> Application:
    app = ApplicationBuilder().token(TOKEN).build()

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("dialogs", dialogs_command))
    app.add_handler(CommandHandler("crm", crm_command))
    app.add_handler(CommandHandler("orders", orders_command))
    app.add_handler(CommandHandler("order", order_command))
    app.add_handler(CommandHandler("history", history_command))
    app.add_handler(CommandHandler("close_dialog", close_dialog_command))
    app.add_handler(CommandHandler("open_dialog", open_dialog_command))
    app.add_handler(CommandHandler("expire_orders", expire_orders_command))
    app.add_handler(CommandHandler("stats", stats_command))
    app.add_handler(CommandHandler("broadcast", broadcast_command))
    app.add_handler(CommandHandler("myorders", my_orders_command))
    app.add_handler(CommandHandler("cancel", cancel_command))
    app.add_handler(CallbackQueryHandler(button_click))

    user_message_filter = (
        ~filters.User(user_id=ADMIN_ID)
        & (
            filters.TEXT
            | filters.PHOTO
            | filters.VIDEO
            | filters.VOICE
            | filters.AUDIO
            | filters.Document.ALL
            | filters.Sticker.ALL
        )
    )

    admin_message_filter = (
        filters.User(user_id=ADMIN_ID)
        & (
            filters.TEXT
            | filters.PHOTO
            | filters.VIDEO
            | filters.VOICE
            | filters.AUDIO
            | filters.Document.ALL
            | filters.Sticker.ALL
        )
        & ~filters.COMMAND
    )

    app.add_handler(MessageHandler(user_message_filter, forward_user_message_to_admin))
    app.add_handler(MessageHandler(admin_message_filter, admin_reply))
    app.add_error_handler(error_handler)

    if app.job_queue:
        app.job_queue.run_repeating(
            auto_expire_orders_job,
            interval=AUTO_EXPIRE_CHECK_MINUTES * 60,
            first=30,
            name="auto_expire_orders",
        )

    return app


def main() -> None:
    logger.info("Инициализация базы данных...")
    init_db()
    migrate_order_statuses()

    logger.info(
        "Запуск бота... auto-expire=%s мин, expiration=%s ч",
        AUTO_EXPIRE_CHECK_MINUTES,
        ORDER_EXPIRATION_HOURS,
    )
    app = build_application()
    app.post_init = setup_bot_commands
    app.run_polling()


if __name__ == "__main__":
    main()
