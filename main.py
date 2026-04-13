import asyncio
import html
import random
import os
import time
from datetime import datetime, timedelta

import pytz
from dotenv import load_dotenv
from psycopg2.extras import RealDictCursor
from psycopg2 import pool
from aiogram import Bot, Dispatcher, F
from aiogram.types import (
    Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton,
    LabeledPrice, PreCheckoutQuery, ReplyKeyboardMarkup, KeyboardButton,
)
from aiogram.filters import CommandStart, Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage

load_dotenv()
TOKEN            = os.getenv("BOT_TOKEN")
DATABASE_URL     = os.getenv("DATABASE_URL")
ADMIN_ID         = int(os.getenv("ADMIN_ID", "0"))
STARTING_BALANCE = 1_000

bot = Bot(token=TOKEN)
dp  = Dispatcher(storage=MemoryStorage())

db_pool: pool.SimpleConnectionPool | None = None


# ── Cooldown protection ────────────────────────────────────────────────────

_COOLDOWNS: dict[int, float] = {}

def check_cooldown(user_id: int, seconds: float = 1.0) -> bool:
    now = time.monotonic()
    if now - _COOLDOWNS.get(user_id, 0.0) < seconds:
        return False
    _COOLDOWNS[user_id] = now
    return True


# ── DB pool ────────────────────────────────────────────────────────────────

def get_db_pool() -> pool.SimpleConnectionPool:
    global db_pool
    if db_pool is None:
        db_pool = pool.SimpleConnectionPool(1, 10, DATABASE_URL)
    return db_pool

def db_connect():
    return get_db_pool().getconn()

def db_release(conn):
    get_db_pool().putconn(conn)


# ── DB init ────────────────────────────────────────────────────────────────

def init_db():
    conn = db_connect()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS users (
                    user_id         BIGINT PRIMARY KEY,
                    username        TEXT,
                    balance         BIGINT  DEFAULT 1000,
                    wins            INTEGER DEFAULT 0,
                    losses          INTEGER DEFAULT 0,
                    last_message_id INTEGER
                )
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS history (
                    id         SERIAL  PRIMARY KEY,
                    user_id    BIGINT  NOT NULL,
                    amount     BIGINT  NOT NULL,
                    is_win     BOOLEAN NOT NULL,
                    game_type  TEXT    DEFAULT 'roulette',
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (user_id) REFERENCES users(user_id)
                )
            """)
            cur.execute(
                "CREATE INDEX IF NOT EXISTS idx_history_user_id ON history(user_id)"
            )
            cur.execute("""
                CREATE TABLE IF NOT EXISTS farms (
                    user_id      BIGINT PRIMARY KEY,
                    level        INTEGER   DEFAULT 1,
                    last_collect TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (user_id) REFERENCES users(user_id)
                )
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS businesses (
                    user_id      BIGINT NOT NULL,
                    biz_type     TEXT   NOT NULL,
                    last_collect TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (user_id, biz_type),
                    FOREIGN KEY (user_id) REFERENCES users(user_id)
                )
            """)
            cur.execute(
                "CREATE INDEX IF NOT EXISTS idx_businesses_user ON businesses(user_id)"
            )
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        db_release(conn)


# ── DB helpers ─────────────────────────────────────────────────────────────

def get_user(user_id: int) -> dict | None:
    conn = db_connect()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT * FROM users WHERE user_id = %s", (user_id,))
            return cur.fetchone()
    finally:
        db_release(conn)

def get_or_create_user(user_id: int, username: str) -> int:
    conn = db_connect()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """INSERT INTO users (user_id, username, balance)
                   VALUES (%s, %s, %s)
                   ON CONFLICT (user_id) DO UPDATE SET username = EXCLUDED.username
                   RETURNING balance""",
                (user_id, username, STARTING_BALANCE),
            )
            row = cur.fetchone()
            conn.commit()
            return row[0] if row else STARTING_BALANCE
    finally:
        db_release(conn)

def get_balance(user_id: int) -> int:
    conn = db_connect()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT balance FROM users WHERE user_id = %s", (user_id,))
            row = cur.fetchone()
            return row[0] if row else 0
    finally:
        db_release(conn)

def atomic_update_balance(user_id: int, delta: int) -> int | None:
    conn = db_connect()
    try:
        with conn.cursor() as cur:
            if delta < 0:
                cur.execute(
                    """UPDATE users
                       SET balance = balance + %s
                       WHERE user_id = %s AND balance >= %s
                       RETURNING balance""",
                    (delta, user_id, -delta),
                )
            else:
                cur.execute(
                    """UPDATE users
                       SET balance = balance + %s
                       WHERE user_id = %s
                       RETURNING balance""",
                    (delta, user_id),
                )
            row = cur.fetchone()
            conn.commit()
            return row[0] if row else None
    except Exception:
        conn.rollback()
        return None
    finally:
        db_release(conn)

def update_balance(user_id: int, delta: int, win: bool, game_type: str = "roulette") -> int | None:
    col  = "wins" if win else "losses"
    conn = db_connect()
    row  = None
    try:
        with conn.cursor() as cur:
            if delta < 0:
                cur.execute(
                    f"""UPDATE users
                        SET balance = balance + %s,
                            {col} = {col} + 1
                        WHERE user_id = %s AND balance >= %s
                        RETURNING balance""",
                    (delta, user_id, -delta),
                )
            else:
                cur.execute(
                    f"""UPDATE users
                        SET balance = balance + %s,
                            {col} = {col} + 1
                        WHERE user_id = %s
                        RETURNING balance""",
                    (delta, user_id),
                )
            row = cur.fetchone()
            conn.commit()
    except Exception:
        conn.rollback()
        return None
    finally:
        db_release(conn)

    if row:
        add_history(user_id, abs(delta), win, game_type)
        return row[0]
    return None

def reset_balance(user_id: int):
    conn = db_connect()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE users SET balance = %s WHERE user_id = %s",
                (STARTING_BALANCE, user_id),
            )
        conn.commit()
    finally:
        db_release(conn)

def set_balance(user_id: int, amount: int):
    conn = db_connect()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE users SET balance = %s WHERE user_id = %s", (amount, user_id)
            )
        conn.commit()
    finally:
        db_release(conn)

def get_all_users() -> list[int]:
    conn = db_connect()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT user_id FROM users")
            return [r[0] for r in cur.fetchall()]
    finally:
        db_release(conn)

def get_all_users_full() -> list[dict]:
    conn = db_connect()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT * FROM users ORDER BY user_id")
            return cur.fetchall()
    finally:
        db_release(conn)

def get_leaderboard(limit: int = 10) -> list[dict]:
    conn = db_connect()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                "SELECT username, balance, wins, losses FROM users ORDER BY balance DESC LIMIT %s",
                (limit,),
            )
            return cur.fetchall()
    finally:
        db_release(conn)

def add_daily_bonus(user_id: int):
    conn = db_connect()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE users SET balance = balance + 10000 WHERE user_id = %s", (user_id,)
            )
        conn.commit()
    finally:
        db_release(conn)

def add_history(user_id: int, amount: int, is_win: bool, game_type: str = "roulette"):
    conn = db_connect()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO history (user_id, amount, is_win, game_type) VALUES (%s, %s, %s, %s)",
                (user_id, amount, is_win, game_type),
            )
        conn.commit()
    finally:
        db_release(conn)

def get_history(user_id: int, limit: int = 10) -> list[dict]:
    conn = db_connect()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """SELECT amount, is_win, game_type, created_at
                   FROM history WHERE user_id = %s
                   ORDER BY created_at DESC LIMIT %s""",
                (user_id, limit),
            )
            return cur.fetchall()
    finally:
        db_release(conn)

def get_user_history_stats(user_id: int) -> dict | None:
    conn = db_connect()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """SELECT
                   SUM(CASE WHEN is_win     THEN amount ELSE 0 END) AS total_won,
                   SUM(CASE WHEN NOT is_win THEN amount ELSE 0 END) AS total_lost,
                   COUNT(CASE WHEN is_win     THEN 1 END) AS win_count,
                   COUNT(CASE WHEN NOT is_win THEN 1 END) AS lose_count
                   FROM history
                   WHERE user_id = %s AND game_type NOT IN ('admin_topup','farm','business')""",
                (user_id,),
            )
            return cur.fetchone()
    finally:
        db_release(conn)

def clear_all_history():
    conn = db_connect()
    try:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM history")
        conn.commit()
    finally:
        db_release(conn)


# ── Farm DB ────────────────────────────────────────────────────────────────

FARM_BUY_COST      = 5_000
FARM_UPGRADE_COSTS = {
    1: 3_000,  2: 8_000,   3: 20_000,  4: 50_000,
    5: 120_000, 6: 300_000, 7: 700_000, 8: 1_500_000, 9: 3_000_000,
}
FARM_INCOME = {
    1: 200,  2: 500,   3: 1_200,  4: 3_000,  5: 7_000,
    6: 15_000, 7: 35_000, 8: 80_000, 9: 180_000, 10: 400_000,
}
FARM_LEVEL_NAMES = {
    1: "Малинка",  2: "Ноутбук",  3: "Десктоп",  4: "Стойка",
    5: "Мини-ферма", 6: "Ферма", 7: "Датацентр", 8: "Мега-ферма",
    9: "Гиперцентр", 10: "Квантовая ферма",
}

def get_farm(user_id: int) -> dict | None:
    conn = db_connect()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT * FROM farms WHERE user_id = %s", (user_id,))
            return cur.fetchone()
    finally:
        db_release(conn)

def buy_farm(user_id: int):
    conn = db_connect()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO farms (user_id, level, last_collect) "
                "VALUES (%s, 1, CURRENT_TIMESTAMP) ON CONFLICT DO NOTHING",
                (user_id,),
            )
        conn.commit()
    finally:
        db_release(conn)

def upgrade_farm(user_id: int):
    conn = db_connect()
    try:
        with conn.cursor() as cur:
            cur.execute("UPDATE farms SET level = level + 1 WHERE user_id = %s", (user_id,))
        conn.commit()
    finally:
        db_release(conn)

def collect_farm(user_id: int):
    conn = db_connect()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE farms SET last_collect = CURRENT_TIMESTAMP WHERE user_id = %s",
                (user_id,),
            )
        conn.commit()
    finally:
        db_release(conn)

def get_farm_pending(farm: dict) -> int:
    if not farm:
        return 0
    last = farm["last_collect"]
    if last.tzinfo is None:
        last = last.replace(tzinfo=pytz.utc)
    hours = min((datetime.now(pytz.utc) - last).total_seconds() / 3600, 24)
    return int(hours * FARM_INCOME.get(farm["level"], 0))


# ── Business DB ────────────────────────────────────────────────────────────

BUSINESSES: dict[str, dict] = {
    "kiosk":      {"name": "Ларёк",          "emoji": "🏪", "cost":       10_000, "income":       500},
    "cafe":       {"name": "Кафе",           "emoji": "☕", "cost":       50_000, "income":     2_500},
    "shop247":    {"name": "Магазин 24/7",   "emoji": "🏬", "cost":      150_000, "income":     7_500},
    "restaurant": {"name": "Ресторан",       "emoji": "🍽", "cost":      500_000, "income":    25_000},
    "club":       {"name": "Ночной клуб",    "emoji": "🎸", "cost":    1_500_000, "income":    75_000},
    "casino_biz": {"name": "Казино",         "emoji": "🎰", "cost":    5_000_000, "income":   250_000},
    "mall":       {"name": "Торговый центр", "emoji": "🏢", "cost":   15_000_000, "income":   750_000},
    "hotel":      {"name": "Отель",          "emoji": "🏨", "cost":   50_000_000, "income": 2_500_000},
}

def get_user_businesses(user_id: int) -> dict[str, dict]:
    conn = db_connect()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT * FROM businesses WHERE user_id = %s", (user_id,))
            return {row["biz_type"]: dict(row) for row in cur.fetchall()}
    finally:
        db_release(conn)

def buy_business(user_id: int, biz_type: str):
    conn = db_connect()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO businesses (user_id, biz_type, last_collect) "
                "VALUES (%s, %s, CURRENT_TIMESTAMP) ON CONFLICT DO NOTHING",
                (user_id, biz_type),
            )
        conn.commit()
    finally:
        db_release(conn)

def collect_business(user_id: int, biz_type: str):
    conn = db_connect()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE businesses SET last_collect = CURRENT_TIMESTAMP "
                "WHERE user_id = %s AND biz_type = %s",
                (user_id, biz_type),
            )
        conn.commit()
    finally:
        db_release(conn)

def get_business_pending(row: dict, biz_type: str) -> int:
    if not row or biz_type not in BUSINESSES:
        return 0
    last = row["last_collect"]
    if last.tzinfo is None:
        last = last.replace(tzinfo=pytz.utc)
    hours = min((datetime.now(pytz.utc) - last).total_seconds() / 3600, 24)
    return int(hours * BUSINESSES[biz_type]["income"])


# ── States ─────────────────────────────────────────────────────────────────

class BetState(StatesGroup):
    choosing_bet_type = State()
    choosing_amount   = State()

class CoinState(StatesGroup):
    choosing_side   = State()
    choosing_amount = State()

class AdminState(StatesGroup):
    choosing_action   = State()
    choosing_user     = State()
    editing_balance   = State()
    sending_broadcast = State()

class DonateState(StatesGroup):
    entering_custom_amount = State()

class RocketState(StatesGroup):
    choosing_amount = State()
    in_game         = State()

class MinesweeperState(StatesGroup):
    choosing_amount = State()
    choosing_mines  = State()
    in_game         = State()


# ── Formatting helpers ─────────────────────────────────────────────────────

def noun_form(count: int, singular: str, gen24: str, gen5: str) -> str:
    n = int(count) % 100
    if n % 10 == 1 and n != 11:
        return singular
    if n % 10 in (2, 3, 4) and n not in (12, 13, 14):
        return gen24
    return gen5

def fmt(n: int) -> str:
    if n >= 1_000_000:
        v = n / 1_000_000
        return (f"{int(v)}кк" if v == int(v) else f"{v:.1f}".rstrip("0").rstrip(".") + "кк")
    if n >= 1_000:
        v = n / 1_000
        return (f"{int(v)}к" if v == int(v) else f"{v:.1f}".rstrip("0").rstrip(".") + "к")
    return str(n)

def format_chips(amount: int) -> str:
    if amount >= 1_000_000:
        return fmt(amount) + " фишек"
    if amount >= 1_000:
        base = amount / 1_000
        return f"{fmt(amount)} {noun_form(round(base, 1), 'фишка', 'фишки', 'фишек')}"
    return f"{amount} {noun_form(amount, 'фишка', 'фишки', 'фишек')}"

def te(emoji_id: str, emoji: str) -> str:
    return f'<tg-emoji emoji-id="{emoji_id}">{emoji}</tg-emoji>'

# tg-emoji константы
E_CASINO     = te("5258882890059091157", "🎰")
E_COIN       = te("5904462880941545555", "🪙")
E_COIN2      = te("5890848474563352982", "🪙")
E_CHART      = te("5870930636742595124", "📊")
E_CHART2     = te("5870921681735781843", "📊")
E_WIN        = te("5870633910337015697", "✅")
E_LOSE       = te("5870657884844462243", "❌")
E_USER       = te("5870994129244131212", "👤")
E_USERS      = te("5870772616305839506", "👥")
E_GEAR       = te("5870982283724328568", "⚙️")
E_RESET      = te("5345906554510012647", "🔄")
E_FARM       = te("5778672437122045013", "📦")
E_CLOCK      = te("5983150113483134607", "⏰")
E_GIFT       = te("6032644646587338669", "🎁")
E_PARTY      = te("6041731551845159060", "🎉")
E_PEN        = te("5870676941614354370", "🖋")
E_BACK       = te("5893057118545646106", "📰")
E_ROCKET     = te("5373139891223741704", "🚀")
E_BOMB       = te("5373141891321699086", "💣")
E_HORN       = te("6039422865189638057", "📣")
E_ALPHA      = te("5771851822897566479", "🔡")
E_ATM        = te("5879814368572478751", "🏧")
E_PING       = te("5983150113483134607", "⏰")
E_HISTORY    = te("5870930636742595124", "📊")
E_BRIEFCASE  = te("5773781976905421370", "💼")
E_TAG        = te("5886285355279193209", "🏷")
E_BOX        = te("5884479287171485878", "📦")


# ── Roulette helpers ───────────────────────────────────────────────────────

RED_NUMBERS   = {1,3,5,7,9,12,14,16,18,19,21,23,25,27,30,32,34,36}
BLACK_NUMBERS = {2,4,6,8,10,11,13,15,17,20,22,24,26,28,29,31,33,35}

BET_LABELS = {
    "red":    "Красное",   "black":  "Чёрное",
    "even":   "ЧЁТНОЕ",    "odd":    "НЕЧЁТНОЕ",
    "1-18":   "1–18",      "19-36":  "19–36",
    "1st12":  "1ST 12",    "2nd12":  "2ND 12",    "3rd12":  "3RD 12",
    "2to1_1": "2to1 (ряд 1)", "2to1_2": "2to1 (ряд 2)", "2to1_3": "2to1 (ряд 3)",
}

def spin_wheel() -> int:
    return random.randint(0, 36)

def number_color(n: int) -> str:
    if n == 0:            return "🟢"
    if n in RED_NUMBERS:  return "🔴"
    return "⚫"

def check_bet(bet_type: str, number: int) -> bool:
    if bet_type == "red":    return number in RED_NUMBERS
    if bet_type == "black":  return number in BLACK_NUMBERS
    if bet_type == "even":   return number != 0 and number % 2 == 0
    if bet_type == "odd":    return number != 0 and number % 2 == 1
    if bet_type == "1-18":   return 1 <= number <= 18
    if bet_type == "19-36":  return 19 <= number <= 36
    if bet_type == "1st12":  return 1 <= number <= 12
    if bet_type == "2nd12":  return 13 <= number <= 24
    if bet_type == "3rd12":  return 25 <= number <= 36
    if bet_type == "2to1_1": return number in {1,4,7,10,13,16,19,22,25,28,31,34}
    if bet_type == "2to1_2": return number in {2,5,8,11,14,17,20,23,26,29,32,35}
    if bet_type == "2to1_3": return number in {3,6,9,12,15,18,21,24,27,30,33,36}
    if bet_type.startswith("num_"):
        return number == int(bet_type[4:])
    return False

def payout_multiplier(bet_type: str) -> int:
    if bet_type.startswith("num_"): return 35
    if bet_type in ("1st12","2nd12","3rd12","2to1_1","2to1_2","2to1_3"): return 2
    return 1

def flip_coin() -> str:
    return random.choice(["heads", "tails"])


# ── Inline keyboards ───────────────────────────────────────────────────────

def _btn(text: str, callback_data: str | None = None, url: str | None = None,
         icon: str | None = None) -> InlineKeyboardButton:
    kwargs: dict = {"text": text}
    if callback_data:
        kwargs["callback_data"] = callback_data
    if url:
        kwargs["url"] = url
    if icon:
        kwargs["icon_custom_emoji_id"] = icon
    return InlineKeyboardButton(**kwargs)

def main_reply_kb() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [
                KeyboardButton(text="🎰 Казино",  icon_custom_emoji_id="5258882890059091157"),
                KeyboardButton(text="👤 Профиль", icon_custom_emoji_id="5870994129244131212"),
            ],
            [
                KeyboardButton(text="🪙 Баланс",          icon_custom_emoji_id="5904462880941545555"),
                KeyboardButton(text="🔄 Сбросить баланс",  icon_custom_emoji_id="5345906554510012647"),
            ],
            [
                KeyboardButton(text="⛏ Ферма",   icon_custom_emoji_id="5778672437122045013"),
                KeyboardButton(text="💼 Бизнес",  icon_custom_emoji_id="5773781976905421370"),
            ],
            [
                KeyboardButton(text="📊 История",     icon_custom_emoji_id="5870930636742595124"),
                KeyboardButton(text="🏆 Топ игроков", icon_custom_emoji_id="5870921681735781843"),
            ],
        ],
        resize_keyboard=True,
        is_persistent=True,
    )

def main_menu_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            _btn("Рулетка",        "open_roulette",    icon="5258882890059091157"),
            _btn("Орёл или Решка", "open_coin",        icon="5774585885154131652"),
        ],
        [
            _btn("Ракета", "open_rocket",      icon="5373139891223741704"),
            _btn("Сапёр",  "open_minesweeper", icon="5373141891321699086"),
        ],
        [
            _btn("Биткоин-ферма", "open_farm",     icon="5778672437122045013"),
            _btn("Бизнес",        "open_business", icon="5773781976905421370"),
        ],
        [
            _btn("Профиль",         "stats", icon="5870921681735781843"),
            _btn("Сбросить баланс", "reset", icon="5345906554510012647"),
        ],
        [
            _btn("🏆 Топ игроков", "leaderboard", icon="5870921681735781843"),
        ],
    ])

def stats_menu_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [_btn("История ставок",   "stats_history", icon="5870930636742595124")],
        [_btn("Накормить автора", "donate",         icon="5904462880941545555")],
        [_btn("Назад в меню",     "back_main",      icon="5893057118545646106")],
    ])

def game_result_kb(game_type: str, balance: int = 1) -> InlineKeyboardMarkup:
    repeat = "repeat_coin" if game_type == "coin" else "repeat_roulette"
    rows = [[_btn("Повторить", repeat, icon="5345906554510012647")]]
    if balance <= 0:
        rows.append([_btn("Сбросить баланс", "reset", icon="5345906554510012647")])
    rows.append([_btn("В меню", "back_main", icon="5893057118545646106")])
    return InlineKeyboardMarkup(inline_keyboard=rows)

def bet_type_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            _btn("🔴 Красное", "bet_red"),
            _btn("⚫ Чёрное",  "bet_black"),
        ],
        [_btn("1–18", "bet_1-18"), _btn("19–36", "bet_19-36")],
        [_btn("ЧЁТНОЕ", "bet_even"), _btn("НЕЧЁТНОЕ", "bet_odd")],
        [
            _btn("1ST 12", "bet_1st12"),
            _btn("2ND 12", "bet_2nd12"),
            _btn("3RD 12", "bet_3rd12"),
        ],
        [
            _btn("2to1 ряд 1", "bet_2to1_1"),
            _btn("2to1 ряд 2", "bet_2to1_2"),
            _btn("2to1 ряд 3", "bet_2to1_3"),
        ],
        [_btn("Конкретное число (x35)", "bet_number", icon="5771851822897566479")],
        [_btn("Назад", "back_main", icon="5893057118545646106")],
    ])

def bet_amount_kb(balance: int, back: str = "back_bet_type") -> InlineKeyboardMarkup:
    q1, q2, q3, q4 = (
        max(1, round(balance * 0.25)),
        max(1, round(balance * 0.50)),
        max(1, round(balance * 0.75)),
        balance,
    )
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            _btn(f"25% ({fmt(q1)})", f"amount_{q1}"),
            _btn(f"75% ({fmt(q3)})", f"amount_{q3}"),
        ],
        [
            _btn(f"50% ({fmt(q2)})",    f"amount_{q2}"),
            _btn(f"Ва-банк ({fmt(q4)})", f"amount_{q4}", icon="6041731551845159060"),
        ],
        [_btn("Ввести вручную", "amount_custom", icon="5870676941614354370")],
        [_btn("Назад", back, icon="5893057118545646106")],
    ])

def coin_side_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            _btn("Орёл",  "coin_heads", icon="5774585885154131652"),
            _btn("Решка", "coin_tails", icon="5904462880941545555"),
        ],
        [_btn("Назад", "back_main", icon="5893057118545646106")],
    ])

def rocket_amount_kb(balance: int) -> InlineKeyboardMarkup:
    q1, q2, q3, q4 = (
        max(1, round(balance * 0.25)),
        max(1, round(balance * 0.50)),
        max(1, round(balance * 0.75)),
        balance,
    )
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            _btn(f"25% ({fmt(q1)})", f"rocket_amount_{q1}"),
            _btn(f"75% ({fmt(q3)})", f"rocket_amount_{q3}"),
        ],
        [
            _btn(f"50% ({fmt(q2)})",    f"rocket_amount_{q2}"),
            _btn(f"Ва-банк ({fmt(q4)})", f"rocket_amount_{q4}", icon="6041731551845159060"),
        ],
        [_btn("Ввести вручную", "rocket_amount_custom", icon="5870676941614354370")],
        [_btn("Назад", "back_main", icon="5893057118545646106")],
    ])

def rocket_game_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[[
        _btn("Дальше",  "rocket_next",    icon="5373139891223741704"),
        _btn("Забрать", "rocket_cashout", icon="5904462880941545555"),
    ]])

def _minesweeper_mines_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            _btn("3 мины (x1.1)", "ms_mines_3"),
            _btn("5 мин (x1.3)",  "ms_mines_5"),
        ],
        [
            _btn("7 мин (x2.0)",  "ms_mines_7"),
            _btn("10 мин (x2.8)", "ms_mines_10"),
        ],
        [_btn("Назад", "back_main", icon="5893057118545646106")],
    ])

def _minesweeper_kb(revealed: list, opened_count: int) -> InlineKeyboardMarkup:
    """Кнопка «Забрать» показывается только если открыта хотя бы одна ячейка."""
    buttons = []
    for r, row in enumerate(revealed):
        buttons.append([
            InlineKeyboardButton(
                text="✅" if row[c] else "🟫",
                callback_data=f"ms_cell_{r}_{c}",
            )
            for c in range(5)
        ])
    if opened_count > 0:
        buttons.append([_btn("Забрать", "ms_cashout", icon="5870633910337015697")])
    buttons.append([_btn("В меню",  "back_main",  icon="5893057118545646106")])
    return InlineKeyboardMarkup(inline_keyboard=buttons)

def admin_menu_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [_btn("Список пользователей", "admin_users",         icon="5870772616305839506")],
        [_btn("Рассылка сообщения",   "admin_broadcast",     icon="6039422865189638057")],
        [_btn("Очистить историю",     "admin_clear_history", icon="5870657884844462243")],
        [_btn("Вернуться в меню",     "back_main",           icon="5893057118545646106")],
    ])

def users_list_kb(users: list, page: int = 0) -> InlineKeyboardMarkup:
    per_page   = 5
    page_users = users[page * per_page : (page + 1) * per_page]
    buttons = [
        [_btn(
            (u["username"] or ("ID " + str(u["user_id"]))) + f' ({format_chips(u["balance"])})',
            f"admin_edit_user_{u['user_id']}",
            icon="5870994129244131212",
        )]
        for u in page_users
    ]
    nav = []
    if page > 0:
        nav.append(_btn("◁ Назад", f"admin_users_page_{page-1}"))
    if (page + 1) * per_page < len(users):
        nav.append(_btn("Вперёд ▷", f"admin_users_page_{page+1}"))
    if nav:
        buttons.append(nav)
    buttons.append([_btn("В меню админа", "admin_back", icon="5893057118545646106")])
    return InlineKeyboardMarkup(inline_keyboard=buttons)

def edit_user_kb(user_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [_btn("Пополнить баланс", f"admin_edit_balance_{user_id}", icon="5904462880941545555")],
        [_btn("История ставок",   f"admin_user_history_{user_id}", icon="5870930636742595124")],
        [_btn("Назад к списку",   "admin_users_back",              icon="5893057118545646106")],
    ])

def donate_kb() -> InlineKeyboardMarkup:
    rows = [
        [_btn(f"{n} звёзд", f"donate_{n}", icon="5870982283724328568")]
        for n in (50, 100, 250, 500, 1000)
    ]
    rows.append([_btn("Своя сумма",   "donate_custom", icon="5870676941614354370")])
    rows.append([_btn("Назад в меню", "back_main",     icon="5893057118545646106")])
    return InlineKeyboardMarkup(inline_keyboard=rows)

def farm_kb(user_id: int) -> InlineKeyboardMarkup:
    farm = get_farm(user_id)
    bal  = get_balance(user_id)
    if not farm:
        rows = []
        if bal >= FARM_BUY_COST:
            rows.append([_btn(f"Купить ферму ({fmt(FARM_BUY_COST)})", "farm_buy", icon="5778672437122045013")])
        else:
            rows.append([_btn(f"Нужно {fmt(FARM_BUY_COST)} фишек", "farm_noop")])
        rows.append([_btn("Назад", "back_main", icon="5893057118545646106")])
        return InlineKeyboardMarkup(inline_keyboard=rows)

    level, pending = farm["level"], get_farm_pending(farm)
    rows = []
    if pending > 0:
        rows.append([_btn(f"Собрать +{fmt(pending)}", "farm_collect", icon="5870633910337015697")])
    if level < 10:
        cost = FARM_UPGRADE_COSTS.get(level, 0)
        if bal >= cost:
            rows.append([_btn(f"Улучшить до ур.{level+1} ({fmt(cost)})", "farm_upgrade", icon="5870930636742595124")])
        else:
            rows.append([_btn(f"Нужно {fmt(cost)} для улучшения", "farm_noop")])
    rows.append([_btn("Назад", "back_main", icon="5893057118545646106")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


# ── Business keyboards ─────────────────────────────────────────────────────

def business_list_kb(owned: dict[str, dict]) -> InlineKeyboardMarkup:
    rows = []
    for biz_type, info in BUSINESSES.items():
        row_data = owned.get(biz_type)
        if row_data:
            pending = get_business_pending(row_data, biz_type)
            label   = (
                f'{info["emoji"]} {info["name"]} (+{fmt(pending)})'
                if pending else
                f'{info["emoji"]} {info["name"]} ✅'
            )
        else:
            label = f'{info["emoji"]} {info["name"]} — {fmt(info["cost"])}'
        rows.append([_btn(label, f"biz_info_{biz_type}")])

    total_pending = sum(
        get_business_pending(owned[bt], bt) for bt in owned if bt in BUSINESSES
    )
    if total_pending > 0:
        rows.append([_btn(
            f"Собрать всё (+{fmt(total_pending)})",
            "biz_collect_all",
            icon="5870633910337015697",
        )])
    rows.append([_btn("Назад в меню", "back_main", icon="5893057118545646106")])
    return InlineKeyboardMarkup(inline_keyboard=rows)

def business_detail_kb(biz_type: str, is_owned: bool, pending: int) -> InlineKeyboardMarkup:
    rows = []
    if is_owned:
        if pending > 0:
            rows.append([_btn(f"Собрать +{fmt(pending)}", f"biz_collect_{biz_type}", icon="5870633910337015697")])
        else:
            rows.append([_btn("Ещё нет дохода ⏳", "biz_noop")])
    else:
        cost = BUSINESSES[biz_type]["cost"]
        rows.append([_btn(
            f"Купить за {fmt(cost)}",
            f"biz_buy_{biz_type}",
            icon="5904462880941545555",
        )])
    rows.append([_btn("К списку бизнесов", "open_business", icon="5893057118545646106")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


# ── Text builders ──────────────────────────────────────────────────────────

def farm_text(user_id: int) -> str:
    farm    = get_farm(user_id)
    bal     = get_balance(user_id)
    pending = get_farm_pending(farm)
    if not farm:
        return (
            f'{E_FARM} <b>Биткоин-ферма</b>\n\n'
            f'У вас ещё нет фермы.\n\n'
            f'{E_COIN} Стоимость: <b>{format_chips(FARM_BUY_COST)}</b>\n'
            f'{E_CHART} Доход: <b>{format_chips(FARM_INCOME[1])} / час</b>\n\n'
            f'{E_COIN} Ваш баланс: <b>{format_chips(bal)}</b>'
        )
    level = farm["level"]
    name  = FARM_LEVEL_NAMES.get(level, f"Уровень {level}")
    last  = farm["last_collect"]
    if last.tzinfo is None:
        last = last.replace(tzinfo=pytz.utc)
    hours = min((datetime.now(pytz.utc) - last).total_seconds() / 3600, 24)
    text  = (
        f'{E_FARM} <b>Биткоин-ферма</b>\n\n'
        f'⛏ Тип: <b>{name}</b>\n'
        f'{E_CHART} Уровень: <b>{level} / 10</b>\n'
        f'{E_COIN} Доход: <b>{format_chips(FARM_INCOME.get(level, 0))} / час</b>\n'
        f'{E_CLOCK} Накоплено за {hours:.1f}ч: <b>{format_chips(pending)}</b>\n'
    )
    if level < 10 and (cost := FARM_UPGRADE_COSTS.get(level)):
        text += f'{E_CHART} Улучшение до ур.{level+1}: <b>{format_chips(cost)}</b>\n'
    text += f'\n{E_COIN} Ваш баланс: <b>{format_chips(bal)}</b>'
    return text

def business_list_text(user_id: int, owned: dict[str, dict]) -> str:
    bal           = get_balance(user_id)
    owned_count   = len(owned)
    total_income  = sum(BUSINESSES[bt]["income"] for bt in owned if bt in BUSINESSES)
    total_pending = sum(get_business_pending(owned[bt], bt) for bt in owned if bt in BUSINESSES)

    lines = [f'{E_BRIEFCASE} <b>Бизнес-империя</b>\n']
    lines.append(f'{E_COIN} Баланс: <b>{format_chips(bal)}</b>')
    lines.append(f'🏢 Бизнесов: <b>{owned_count} / {len(BUSINESSES)}</b>')
    if total_income:
        lines.append(f'{E_CHART} Суммарный доход: <b>{format_chips(total_income)} / час</b>')
    if total_pending:
        lines.append(f'{E_GIFT} Накоплено к сбору: <b>{format_chips(total_pending)}</b>')
    lines.append('\n<b>Выберите бизнес:</b>')
    return "\n".join(lines)

def business_detail_text(biz_type: str, owned_row: dict | None) -> str:
    info     = BUSINESSES[biz_type]
    is_owned = owned_row is not None
    pending  = get_business_pending(owned_row, biz_type) if is_owned else 0

    lines = [f'{info["emoji"]} <b>{info["name"]}</b>\n']
    lines.append(f'{E_COIN} Стоимость: <b>{format_chips(info["cost"])}</b>')
    lines.append(f'{E_CHART} Доход: <b>{format_chips(info["income"])} / час</b>')
    if is_owned:
        last = owned_row["last_collect"]
        if last.tzinfo is None:
            last = last.replace(tzinfo=pytz.utc)
        hours = min((datetime.now(pytz.utc) - last).total_seconds() / 3600, 24)
        lines.append(f'{E_CLOCK} Накоплено за {hours:.1f}ч: <b>{format_chips(pending)}</b>')
        lines.append(f'\n{E_WIN} <b>Бизнес куплен</b>')
    else:
        lines.append(f'\n{E_LOSE} Ещё не куплен')
    return "\n".join(lines)

def profile_text(user: dict) -> str:
    wins, losses = user["wins"], user["losses"]
    total = wins + losses
    rate  = round(wins / total * 100, 1) if total else 0
    return (
        f'{E_CHART2} <b>Профиль</b>\n\n'
        f'{E_USER} Игрок: <b>{html.escape(user["username"] or "Неизвестно")}</b>\n'
        f'{E_COIN} Баланс: <b>{format_chips(user["balance"])}</b>\n'
        f'{E_WIN} Побед: <b>{wins}</b>\n'
        f'{E_LOSE} Поражений: <b>{losses}</b>\n'
        f'{E_CHART} Процент побед: <b>{rate}%</b>'
    )

def history_text(user_id: int) -> str:
    history = get_history(user_id, limit=15)
    game_names = {
        "coin": "Монета", "rocket": "Ракета",
        "minesweeper": "Сапёр", "admin_topup": "Пополнение",
        "farm": "Ферма", "business": "Бизнес",
    }
    if not history:
        return f'{E_HISTORY} <b>Нет истории ставок</b>'

    lines = [f'{E_HISTORY} <b>История (последние 15):</b>\n']
    for e in history:
        em   = E_WIN if e["is_win"] else E_LOSE
        sign = "+" if e["is_win"] else "-"
        lines.append(f'{em} {sign}{fmt(e["amount"])} — {game_names.get(e["game_type"], "Рулетка")}')

    stats = get_user_history_stats(user_id)
    if stats and (stats["win_count"] or stats["lose_count"]):
        won  = stats["total_won"]  or 0
        lost = stats["total_lost"] or 0
        net  = won - lost
        lines.append("")
        lines.append(f'{E_WIN} Выигрыши: <b>+{fmt(won)}</b>')
        lines.append(f'{E_LOSE} Проигрыши: <b>-{fmt(lost)}</b>')
        sign  = "+" if net > 0 else ""
        emoji = E_WIN if net > 0 else (E_LOSE if net < 0 else E_COIN)
        lines.append(f'{emoji} Итог: <b>{sign}{fmt(net)}</b>')
    return "\n".join(lines)

def leaderboard_text() -> str:
    top = get_leaderboard(10)
    if not top:
        return f'{E_CHART2} <b>Топ пуст</b>'
    medals = ["🥇","🥈","🥉","4️⃣","5️⃣","6️⃣","7️⃣","8️⃣","9️⃣","🔟"]
    lines  = [f'{E_CHART2} <b>Топ-10 игроков</b>\n']
    for i, u in enumerate(top):
        name = html.escape(u["username"] or "Неизвестно")
        lines.append(f'{medals[i]} <b>{name}</b> — {format_chips(u["balance"])}')
    return "\n".join(lines)

def _rocket_text(amount: int, multiplier: float) -> str:
    potential = int(amount * multiplier)
    stars     = "⭐" * min(int(multiplier), 10)
    return (
        f'{E_ROCKET} <b>Ракета летит!</b>\n\n'
        f'{E_COIN} Ставка: <b>{format_chips(amount)}</b>\n'
        f'{E_CHART} Множитель: <b>x{multiplier:.2f}</b>  {stars}\n'
        f'{E_ATM} Можно забрать: <b>{format_chips(potential)}</b>\n\n'
        f'Нажми <b>«Дальше»</b> чтобы лететь выше\n'
        f'или <b>«Забрать»</b> чтобы зафиксировать!'
    )

def _minesweeper_text(amount: int, field: list, revealed: list, mines: int) -> str:
    base_mult    = {3: 1.1, 5: 1.3, 7: 2.0, 10: 2.8}.get(mines, 2.0)
    opened_count = sum(sum(row) for row in revealed)
    multiplier   = base_mult * (1.0 + opened_count * 0.05)
    potential    = int(amount * multiplier)
    lines = [
        f'{E_BOMB} <b>Сапёр</b>\n',
        f'{E_COIN} Ставка: <b>{format_chips(amount)}</b>',
        f'{E_BOMB} Мин: <b>{mines}</b>',
        f'{E_CHART} Множитель: <b>x{multiplier:.2f}</b>',
        f'{E_ATM} Можно забрать: <b>{format_chips(potential)}</b>\n',
        '<b>Открывай ячейки:</b>',
    ]
    for r, row in enumerate(revealed):
        row_str = ""
        for c, is_open in enumerate(row):
            if is_open:
                row_str += "💥" if field[r][c] else "✅"
            else:
                row_str += "🟫"
        lines.append(row_str)
    if opened_count == 0:
        lines.append(f'\n<i>Открой хотя бы одну ячейку, чтобы забрать выигрыш</i>')
    return "\n".join(lines)


# ── Utility ────────────────────────────────────────────────────────────────

async def safe_edit_or_send(cq: CallbackQuery, text: str, reply_markup=None):
    try:
        await cq.message.edit_text(text, parse_mode="HTML", reply_markup=reply_markup)
    except Exception:
        try:
            await cq.message.delete()
        except Exception:
            pass
        await cq.message.answer(text, parse_mode="HTML", reply_markup=reply_markup)

def generate_crash_point() -> float:
    r = random.random()
    if r < 0.40:   return round(random.uniform(1.0,  1.5), 2)
    elif r < 0.65: return round(random.uniform(1.5,  2.5), 2)
    elif r < 0.80: return round(random.uniform(2.5,  4.0), 2)
    elif r < 0.92: return round(random.uniform(4.0,  8.0), 2)
    else:          return round(random.uniform(8.0, 20.0), 2)

def next_multiplier(current: float) -> float:
    return round(current + round(random.uniform(0.1, 0.6), 2), 2)

def generate_minesweeper_field(size: int = 5, mines: int = 5) -> list:
    field  = [[False] * size for _ in range(size)]
    placed = 0
    while placed < mines:
        r, c = random.randint(0, size - 1), random.randint(0, size - 1)
        if not field[r][c]:
            field[r][c] = True
            placed += 1
    return field


# ── /start /help /ping ─────────────────────────────────────────────────────

@dp.message(CommandStart())
async def cmd_start(msg: Message, state: FSMContext):
    await state.clear()
    bal  = get_or_create_user(msg.from_user.id, msg.from_user.username or "игрок")
    text = (
        f'{E_CASINO} <b>Добро пожаловать в Казино!</b>\n\n'
        f'{E_COIN} Ваш баланс: <b>{format_chips(bal)}</b>\n\n'
        f'Используйте меню ниже для навигации.'
    )
    await msg.answer(text, parse_mode="HTML", reply_markup=main_reply_kb())
    await msg.answer(
        f'{E_CASINO} <b>Выберите игру:</b>',
        parse_mode="HTML",
        reply_markup=main_menu_kb(),
    )

@dp.message(Command("help"))
async def cmd_help(msg: Message):
    text = (
        f'{E_CASINO} <b>Помощь</b>\n\n'
        '<b>Команды:</b>\n'
        '/start — главное меню\n'
        '/help — эта справка\n'
        '/ping — проверить отклик\n\n'
        '<b>Игры:</b>\n'
        f'{E_CASINO} <b>Рулетка</b> — классическая европейская рулетка\n'
        f'{E_ROCKET} <b>Ракета</b> — останови в нужный момент\n'
        f'{E_BOMB} <b>Сапёр</b> — открывай клетки без мин\n'
        f'{E_COIN} <b>Монета</b> — орёл или решка (x2)\n\n'
        f'{E_FARM} <b>Ферма</b> — пассивный доход каждый час\n'
        f'{E_BRIEFCASE} <b>Бизнес</b> — купи бизнес и получай прибыль\n\n'
        f'{E_GIFT} Каждый день в <b>12:00 МСК</b> начисляется бонус +10 000 фишек!'
    )
    await msg.answer(text, parse_mode="HTML", reply_markup=main_menu_kb())

@dp.message(Command("ping"))
async def cmd_ping(msg: Message):
    t0   = time.perf_counter()
    sent = await msg.answer(f'{E_PING} ПОНГ...', parse_mode="HTML")
    ms   = round((time.perf_counter() - t0) * 1000, 2)
    await sent.edit_text(
        f'{E_PING} <b>ПОНГ</b>\n\n{E_CHART} Отклик: <b>{ms} ms</b>',
        parse_mode="HTML",
    )


# ── Reply keyboard handlers ────────────────────────────────────────────────

@dp.message(F.text == "🎰 Казино")
async def reply_casino(msg: Message, state: FSMContext):
    await state.clear()
    bal = get_or_create_user(msg.from_user.id, msg.from_user.username or "игрок")
    await msg.answer(
        f'{E_CASINO} <b>Казино</b>\n{E_COIN} Баланс: <b>{format_chips(bal)}</b>',
        parse_mode="HTML",
        reply_markup=main_menu_kb(),
    )

@dp.message(F.text == "👤 Профиль")
async def reply_profile(msg: Message):
    user = get_user(msg.from_user.id)
    if not user:
        await msg.answer(f'{E_LOSE} Сначала запустите /start', parse_mode="HTML")
        return
    await msg.answer(profile_text(user), parse_mode="HTML", reply_markup=stats_menu_kb())

@dp.message(F.text == "🪙 Баланс")
async def reply_balance(msg: Message):
    bal = get_balance(msg.from_user.id)
    await msg.answer(
        f'{E_COIN} Ваш баланс: <b>{format_chips(bal)}</b>', parse_mode="HTML"
    )

@dp.message(F.text == "🔄 Сбросить баланс")
async def reply_reset(msg: Message, state: FSMContext):
    reset_balance(msg.from_user.id)
    await state.clear()
    await msg.answer(
        f'{E_RESET} <b>Баланс сброшен!</b>\n{E_COIN} Новый баланс: <b>1 000 фишек</b>',
        parse_mode="HTML",
        reply_markup=main_menu_kb(),
    )

@dp.message(F.text == "⛏ Ферма")
async def reply_farm(msg: Message):
    uid = msg.from_user.id
    await msg.answer(farm_text(uid), parse_mode="HTML", reply_markup=farm_kb(uid))

@dp.message(F.text == "💼 Бизнес")
async def reply_business(msg: Message):
    uid   = msg.from_user.id
    owned = get_user_businesses(uid)
    await msg.answer(
        business_list_text(uid, owned),
        parse_mode="HTML",
        reply_markup=business_list_kb(owned),
    )

@dp.message(F.text == "📊 История")
async def reply_history(msg: Message):
    await msg.answer(
        history_text(msg.from_user.id),
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [_btn("Назад в статистику", "stats", icon="5893057118545646106")],
        ]),
    )

@dp.message(F.text == "🏆 Топ игроков")
async def reply_leaderboard(msg: Message):
    await msg.answer(
        leaderboard_text(),
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [_btn("Назад в меню", "back_main", icon="5893057118545646106")],
        ]),
    )


# ── Navigation callbacks ───────────────────────────────────────────────────

@dp.callback_query(F.data == "back_main")
async def back_main(cq: CallbackQuery, state: FSMContext):
    await state.clear()
    bal = get_balance(cq.from_user.id)
    await safe_edit_or_send(
        cq,
        f'{E_CASINO} <b>Казино</b>\n{E_COIN} Баланс: <b>{format_chips(bal)}</b>',
        reply_markup=main_menu_kb(),
    )

@dp.callback_query(F.data == "balance")
async def show_balance(cq: CallbackQuery):
    await cq.answer(f'Ваш баланс: {format_chips(get_balance(cq.from_user.id))}', show_alert=True)

@dp.callback_query(F.data == "stats")
async def show_stats(cq: CallbackQuery):
    user = get_user(cq.from_user.id)
    if not user:
        await cq.answer("Сначала запустите /start", show_alert=True)
        return
    await safe_edit_or_send(cq, profile_text(user), reply_markup=stats_menu_kb())

@dp.callback_query(F.data == "stats_history")
async def show_history(cq: CallbackQuery):
    await safe_edit_or_send(
        cq,
        history_text(cq.from_user.id),
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [_btn("Назад в статистику", "stats", icon="5893057118545646106")],
        ]),
    )

@dp.callback_query(F.data == "leaderboard")
async def show_leaderboard(cq: CallbackQuery):
    await safe_edit_or_send(
        cq,
        leaderboard_text(),
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [_btn("Назад в меню", "back_main", icon="5893057118545646106")],
        ]),
    )

@dp.callback_query(F.data == "reset")
async def reset_handler(cq: CallbackQuery, state: FSMContext):
    reset_balance(cq.from_user.id)
    await state.clear()
    await cq.answer("Баланс сброшен до 1 000 фишек!", show_alert=True)
    await safe_edit_or_send(
        cq,
        f'{E_RESET} <b>Баланс сброшен!</b>\n{E_COIN} Новый баланс: <b>1 000 фишек</b>',
        reply_markup=main_menu_kb(),
    )


# ── Donate ─────────────────────────────────────────────────────────────────

@dp.callback_query(F.data == "donate")
async def open_donate(cq: CallbackQuery):
    await safe_edit_or_send(
        cq,
        f'{E_GIFT} <b>Накормить автора</b>\n\nВыберите количество звёзд для поддержки разработки\n(звёзды идут разработчику, спасибо! 💙)',
        reply_markup=donate_kb(),
    )

@dp.callback_query(F.data.regexp(r"^donate_(\d+)$"))
async def process_donation(cq: CallbackQuery):
    amount = int(cq.data.split("_")[1])
    await bot.send_invoice(
        chat_id=cq.from_user.id,
        title="Поддержка разработчика",
        description=f"Спасибо за {amount} ⭐! Это помогает развивать бота 💙",
        payload=f"donate_{amount}",
        currency="XTR",
        prices=[LabeledPrice(label=f"Поддержка ({amount} ⭐)", amount=amount)],
        provider_token="",
    )

@dp.callback_query(F.data == "donate_custom")
async def ask_custom_donate(cq: CallbackQuery, state: FSMContext):
    await state.set_state(DonateState.entering_custom_amount)
    await safe_edit_or_send(
        cq,
        f'{E_PEN} <b>Введите сумму в звёздах (⭐)</b>\n\nМинимум: 1 ⭐, максимум: 10 000 ⭐',
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [_btn("Назад", "donate", icon="5893057118545646106")]
        ]),
    )

@dp.message(DonateState.entering_custom_amount)
async def process_custom_donate(msg: Message, state: FSMContext):
    try:
        amount = int(msg.text.strip())
        assert 1 <= amount <= 10_000
    except Exception:
        await msg.answer(f'{E_LOSE} Сумма должна быть от 1 до 10 000 ⭐', parse_mode="HTML")
        return
    await state.clear()
    await bot.send_invoice(
        chat_id=msg.from_user.id,
        title="Поддержка разработчика",
        description=f"Спасибо за {amount} ⭐! 💙",
        payload=f"donate_{amount}",
        currency="XTR",
        prices=[LabeledPrice(label=f"Поддержка ({amount} ⭐)", amount=amount)],
        provider_token="",
    )

@dp.pre_checkout_query()
async def pre_checkout(pcq: PreCheckoutQuery):
    await bot.answer_pre_checkout_query(pcq.id, ok=True)

@dp.message(lambda m: m.successful_payment is not None)
async def successful_payment(msg: Message):
    payment = msg.successful_payment
    if not payment.invoice_payload.startswith("donate_"):
        return
    amount = int(payment.invoice_payload.split("_")[1])
    await msg.answer(
        f'{E_PARTY} <b>Спасибо за {amount} звёзд!</b>\n\nВаша поддержка очень важна для развития бота! 💙',
        parse_mode="HTML",
        reply_markup=main_menu_kb(),
    )


# ── Farm callbacks ─────────────────────────────────────────────────────────

@dp.callback_query(F.data == "open_farm")
async def open_farm(cq: CallbackQuery):
    uid = cq.from_user.id
    await safe_edit_or_send(cq, farm_text(uid), reply_markup=farm_kb(uid))
    await cq.answer()

@dp.callback_query(F.data == "farm_noop")
async def farm_noop(cq: CallbackQuery):
    await cq.answer("Недостаточно фишек!", show_alert=True)

@dp.callback_query(F.data == "farm_buy")
async def farm_buy_handler(cq: CallbackQuery):
    uid = cq.from_user.id
    if get_farm(uid):
        await cq.answer("У вас уже есть ферма!", show_alert=True)
        return
    new_bal = atomic_update_balance(uid, -FARM_BUY_COST)
    if new_bal is None:
        await cq.answer("Недостаточно фишек!", show_alert=True)
        return
    buy_farm(uid)
    await cq.answer("Ферма куплена! ⛏", show_alert=True)
    await safe_edit_or_send(cq, farm_text(uid), reply_markup=farm_kb(uid))

@dp.callback_query(F.data == "farm_collect")
async def farm_collect_handler(cq: CallbackQuery):
    uid  = cq.from_user.id
    farm = get_farm(uid)
    if not farm:
        await cq.answer("У вас нет фермы!", show_alert=True)
        return
    pending = get_farm_pending(farm)
    if pending <= 0:
        await cq.answer("Ещё нечего собирать, подождите!", show_alert=True)
        return
    atomic_update_balance(uid, pending)
    collect_farm(uid)
    add_history(uid, pending, True, game_type="farm")
    await cq.answer(f"Собрано +{fmt(pending)} фишек! ✅", show_alert=True)
    await safe_edit_or_send(cq, farm_text(uid), reply_markup=farm_kb(uid))

@dp.callback_query(F.data == "farm_upgrade")
async def farm_upgrade_handler(cq: CallbackQuery):
    uid  = cq.from_user.id
    farm = get_farm(uid)
    if not farm:
        await cq.answer("У вас нет фермы!", show_alert=True)
        return
    level = farm["level"]
    if level >= 10:
        await cq.answer("Максимальный уровень!", show_alert=True)
        return
    cost    = FARM_UPGRADE_COSTS.get(level, 0)
    new_bal = atomic_update_balance(uid, -cost)
    if new_bal is None:
        await cq.answer("Недостаточно фишек!", show_alert=True)
        return
    upgrade_farm(uid)
    new_name = FARM_LEVEL_NAMES.get(level + 1, f"Уровень {level + 1}")
    await cq.answer(f"Ферма улучшена до уровня {level + 1} — {new_name}! 🎉", show_alert=True)
    await safe_edit_or_send(cq, farm_text(uid), reply_markup=farm_kb(uid))


# ── Business callbacks ─────────────────────────────────────────────────────

@dp.callback_query(F.data == "open_business")
async def open_business(cq: CallbackQuery):
    uid   = cq.from_user.id
    owned = get_user_businesses(uid)
    await safe_edit_or_send(
        cq,
        business_list_text(uid, owned),
        reply_markup=business_list_kb(owned),
    )
    await cq.answer()

@dp.callback_query(F.data.regexp(r"^biz_info_\w+$"))
async def biz_info(cq: CallbackQuery):
    biz_type = cq.data[len("biz_info_"):]
    if biz_type not in BUSINESSES:
        await cq.answer("Неизвестный бизнес.", show_alert=True)
        return
    owned     = get_user_businesses(cq.from_user.id)
    owned_row = owned.get(biz_type)
    pending   = get_business_pending(owned_row, biz_type) if owned_row else 0
    await safe_edit_or_send(
        cq,
        business_detail_text(biz_type, owned_row),
        reply_markup=business_detail_kb(biz_type, owned_row is not None, pending),
    )
    await cq.answer()

@dp.callback_query(F.data.regexp(r"^biz_buy_\w+$"))
async def biz_buy(cq: CallbackQuery):
    biz_type = cq.data[len("biz_buy_"):]
    if biz_type not in BUSINESSES:
        await cq.answer("Неизвестный бизнес.", show_alert=True)
        return
    uid  = cq.from_user.id
    info = BUSINESSES[biz_type]

    if get_user_businesses(uid).get(biz_type):
        await cq.answer("Этот бизнес уже куплен!", show_alert=True)
        return

    new_bal = atomic_update_balance(uid, -info["cost"])
    if new_bal is None:
        await cq.answer(
            f'Недостаточно фишек! Нужно {format_chips(info["cost"])}',
            show_alert=True,
        )
        return

    buy_business(uid, biz_type)
    await cq.answer(f'{info["emoji"]} {info["name"]} куплен!', show_alert=True)

    owned_row = get_user_businesses(uid).get(biz_type)
    await safe_edit_or_send(
        cq,
        business_detail_text(biz_type, owned_row),
        reply_markup=business_detail_kb(biz_type, True, 0),
    )

@dp.callback_query(F.data == "biz_collect_all")
async def biz_collect_all(cq: CallbackQuery):
    uid   = cq.from_user.id
    owned = get_user_businesses(uid)
    total = 0
    for biz_type, row in owned.items():
        if biz_type not in BUSINESSES:
            continue
        pending = get_business_pending(row, biz_type)
        if pending > 0:
            atomic_update_balance(uid, pending)
            collect_business(uid, biz_type)
            add_history(uid, pending, True, game_type="business")
            total += pending

    if total == 0:
        await cq.answer("Нечего собирать!", show_alert=True)
        return

    await cq.answer(f'Собрано +{fmt(total)} фишек! ✅', show_alert=True)
    owned = get_user_businesses(uid)
    await safe_edit_or_send(
        cq,
        business_list_text(uid, owned),
        reply_markup=business_list_kb(owned),
    )

@dp.callback_query(F.data.regexp(r"^biz_collect_(?!all)\w+$"))
async def biz_collect(cq: CallbackQuery):
    biz_type = cq.data[len("biz_collect_"):]
    if biz_type not in BUSINESSES:
        await cq.answer("Неизвестный бизнес.", show_alert=True)
        return
    uid   = cq.from_user.id
    owned = get_user_businesses(uid)
    row   = owned.get(biz_type)
    if not row:
        await cq.answer("Этот бизнес не куплен.", show_alert=True)
        return

    pending = get_business_pending(row, biz_type)
    if pending <= 0:
        await cq.answer("Ещё нечего собирать!", show_alert=True)
        return

    atomic_update_balance(uid, pending)
    collect_business(uid, biz_type)
    add_history(uid, pending, True, game_type="business")
    await cq.answer(f'Собрано +{fmt(pending)} фишек! ✅', show_alert=True)

    owned_row = get_user_businesses(uid).get(biz_type)
    await safe_edit_or_send(
        cq,
        business_detail_text(biz_type, owned_row),
        reply_markup=business_detail_kb(biz_type, True, 0),
    )

@dp.callback_query(F.data == "biz_noop")
async def biz_noop(cq: CallbackQuery):
    await cq.answer("Подождите, доход накапливается!", show_alert=True)


# ── Roulette ───────────────────────────────────────────────────────────────

@dp.callback_query(F.data == "open_roulette")
async def open_roulette(cq: CallbackQuery, state: FSMContext):
    bal = get_balance(cq.from_user.id)
    if bal <= 0:
        await cq.answer("У вас нет фишек! Сбросьте баланс.", show_alert=True)
        return
    await state.set_state(BetState.choosing_bet_type)
    await safe_edit_or_send(
        cq,
        f'{E_CASINO} <b>Европейская Рулетка</b>\n{E_COIN} Баланс: <b>{format_chips(bal)}</b>\n\n<b>Выберите тип ставки:</b>',
        reply_markup=bet_type_kb(),
    )

@dp.callback_query(BetState.choosing_bet_type, F.data.startswith("bet_"))
async def choose_bet_type(cq: CallbackQuery, state: FSMContext):
    raw = cq.data[4:]
    if raw == "number":
        await state.update_data(bet_type="pending_number")
        await state.set_state(BetState.choosing_amount)
        await safe_edit_or_send(
            cq,
            f'{E_ALPHA} <b>Введите число от 0 до 36:</b>',
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [_btn("Назад", "back_bet_type", icon="5893057118545646106")]
            ]),
        )
        return
    await state.update_data(bet_type=raw)
    await state.set_state(BetState.choosing_amount)
    bal = get_balance(cq.from_user.id)
    await safe_edit_or_send(
        cq,
        f'{E_CASINO} <b>Ставка: {BET_LABELS.get(raw, raw)}</b>\n{E_COIN} Баланс: <b>{format_chips(bal)}</b>\n\n<b>Выберите сумму ставки:</b>',
        reply_markup=bet_amount_kb(bal),
    )

@dp.callback_query(F.data == "back_bet_type")
async def back_bet_type(cq: CallbackQuery, state: FSMContext):
    await state.set_state(BetState.choosing_bet_type)
    bal = get_balance(cq.from_user.id)
    await safe_edit_or_send(
        cq,
        f'{E_CASINO} <b>Европейская Рулетка</b>\n{E_COIN} Баланс: <b>{format_chips(bal)}</b>\n\n<b>Выберите тип ставки:</b>',
        reply_markup=bet_type_kb(),
    )

def _roulette_result_text(bet_type: str, amount: int, result: int, won: bool, mult: int, new_bal: int) -> str:
    color = number_color(result)
    label = BET_LABELS.get(bet_type, bet_type.replace("num_", "число "))
    if won:
        profit  = amount * mult
        outcome = f'{E_PARTY} <b>ПОБЕДА!</b>\n{E_COIN2} +{format_chips(profit)} (x{mult})'
    else:
        outcome = f'{E_LOSE} <b>Поражение.</b>\n{E_COIN} -{format_chips(amount)}'
    text = (
        f'{E_CASINO} <b>Шарик остановился на:</b> {color} <b>{result}</b>\n\n'
        f'🎲 Ваша ставка: <b>{label}</b> — <b>{format_chips(amount)}</b>\n'
        f'{outcome}\n\n'
        f'{E_COIN} Новый баланс: <b>{format_chips(new_bal)}</b>'
    )
    if new_bal <= 0:
        text += f'\n\n{E_LOSE} <b>Вы банкрот!</b> Нажмите «Сбросить баланс».'
    return text

@dp.message(BetState.choosing_amount)
async def handle_roulette_text_input(msg: Message, state: FSMContext):
    data = await state.get_data()

    if data.get("bet_type") == "pending_number":
        try:
            n = int(msg.text.strip())
            assert 0 <= n <= 36
        except Exception:
            await msg.answer("❗ Введите целое число от <b>0</b> до <b>36</b>.", parse_mode="HTML")
            return
        await state.update_data(bet_type=f"num_{n}")
        bal = get_balance(msg.from_user.id)
        await msg.answer(
            f'{E_ALPHA} <b>Ставка на число {n}</b> (выплата x35)\n{E_COIN} Баланс: <b>{format_chips(bal)}</b>\n\n<b>Выберите сумму ставки:</b>',
            parse_mode="HTML",
            reply_markup=bet_amount_kb(bal),
        )
        return

    if data.get("waiting_custom"):
        bal = get_balance(msg.from_user.id)
        try:
            amount = int(msg.text.strip())
            assert 1 <= amount <= bal
        except Exception:
            await msg.answer(f"❗ Введите целое число от <b>1</b> до <b>{bal}</b>.", parse_mode="HTML")
            return
        bet_type = data.get("bet_type", "")
        result   = spin_wheel()
        won      = check_bet(bet_type, result)
        mult     = payout_multiplier(bet_type)
        delta    = amount * mult if won else -amount
        new_bal  = update_balance(msg.from_user.id, delta, win=won, game_type="roulette")
        if new_bal is None:
            await msg.answer(f'{E_LOSE} Недостаточно средств!', parse_mode="HTML")
            return
        await state.update_data(last_bet={"game": "roulette", "bet_type": bet_type, "amount": amount})
        await state.set_state(None)
        await msg.answer(
            _roulette_result_text(bet_type, amount, result, won, mult, new_bal),
            parse_mode="HTML",
            reply_markup=game_result_kb("roulette", new_bal),
        )

@dp.callback_query(BetState.choosing_amount, F.data == "amount_custom")
async def ask_custom_roulette_amount(cq: CallbackQuery, state: FSMContext):
    await state.update_data(waiting_custom=True)
    bal = get_balance(cq.from_user.id)
    await safe_edit_or_send(
        cq,
        f'{E_PEN} <b>Введите сумму ставки:</b>\n{E_COIN} Баланс: <b>{format_chips(bal)}</b>',
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [_btn("Назад", "back_bet_type", icon="5893057118545646106")]
        ]),
    )

@dp.callback_query(BetState.choosing_amount, F.data.regexp(r"^amount_\d+$"))
async def place_bet(cq: CallbackQuery, state: FSMContext):
    if not check_cooldown(cq.from_user.id):
        await cq.answer("Не так быстро!", show_alert=False)
        return
    amount   = int(cq.data.split("_")[1])
    data     = await state.get_data()
    bet_type = data.get("bet_type", "")

    if not bet_type or bet_type == "pending_number":
        await cq.answer("Сначала выберите тип ставки.", show_alert=True)
        return
    if amount <= 0:
        await cq.answer("Сумма должна быть больше 0.", show_alert=True)
        return

    result  = spin_wheel()
    won     = check_bet(bet_type, result)
    mult    = payout_multiplier(bet_type)
    delta   = amount * mult if won else -amount
    new_bal = update_balance(cq.from_user.id, delta, win=won, game_type="roulette")
    if new_bal is None:
        await cq.answer("Недостаточно средств!", show_alert=True)
        return

    await state.update_data(last_bet={"game": "roulette", "bet_type": bet_type, "amount": amount})
    await state.set_state(None)
    await safe_edit_or_send(
        cq,
        _roulette_result_text(bet_type, amount, result, won, mult, new_bal),
        reply_markup=game_result_kb("roulette", new_bal),
    )

@dp.callback_query(F.data == "repeat_roulette")
async def repeat_roulette(cq: CallbackQuery, state: FSMContext):
    if not check_cooldown(cq.from_user.id):
        await cq.answer("Не так быстро!", show_alert=False)
        return
    data = await state.get_data()
    last = data.get("last_bet")
    if not last or last.get("game") != "roulette":
        await cq.answer("Нет сохранённой ставки.", show_alert=False)
        return
    bet_type, amount = last["bet_type"], last["amount"]
    result  = spin_wheel()
    won     = check_bet(bet_type, result)
    mult    = payout_multiplier(bet_type)
    delta   = amount * mult if won else -amount
    new_bal = update_balance(cq.from_user.id, delta, win=won, game_type="roulette")
    if new_bal is None:
        await cq.answer(f"Недостаточно фишек! Нужно {amount}.", show_alert=True)
        return
    await safe_edit_or_send(
        cq,
        _roulette_result_text(bet_type, amount, result, won, mult, new_bal),
        reply_markup=game_result_kb("roulette", new_bal),
    )


# ── Coin ───────────────────────────────────────────────────────────────────

@dp.callback_query(F.data == "open_coin")
async def open_coin(cq: CallbackQuery, state: FSMContext):
    bal = get_balance(cq.from_user.id)
    if bal <= 0:
        await cq.answer("У вас нет фишек! Сбросьте баланс.", show_alert=True)
        return
    await state.set_state(CoinState.choosing_side)
    await safe_edit_or_send(
        cq,
        f'🪙 <b>Орёл или Решка</b>\n{E_COIN} Баланс: <b>{format_chips(bal)}</b>\n\n<b>Выберите сторону:</b>',
        reply_markup=coin_side_kb(),
    )

@dp.callback_query(CoinState.choosing_side, F.data.startswith("coin_"))
async def choose_coin_side(cq: CallbackQuery, state: FSMContext):
    side  = cq.data.split("_")[1]
    label = "Орёл" if side == "heads" else "Решка"
    await state.update_data(coin_choice=side)
    await state.set_state(CoinState.choosing_amount)
    bal = get_balance(cq.from_user.id)
    await safe_edit_or_send(
        cq,
        f'🪙 <b>Ставка: {label}</b>\n{E_COIN} Баланс: <b>{format_chips(bal)}</b>\n\n<b>Выберите сумму ставки:</b>',
        reply_markup=bet_amount_kb(bal, back="back_main"),
    )

def _coin_result_text(side_label: str, result_label: str, amount: int, won: bool, new_bal: int) -> str:
    outcome = (
        f'{E_PARTY} <b>ПОБЕДА!</b>\n{E_COIN2} +{format_chips(amount * 2)} (x2)'
        if won else
        f'{E_LOSE} <b>Поражение.</b>\n{E_COIN} -{format_chips(amount)}'
    )
    text = (
        f'🪙 <b>Результат:</b> {result_label}\n\n'
        f'🎲 Ваша ставка: <b>{side_label}</b> — <b>{format_chips(amount)}</b>\n'
        f'{outcome}\n\n'
        f'{E_COIN} Новый баланс: <b>{format_chips(new_bal)}</b>'
    )
    if new_bal <= 0:
        text += f'\n\n{E_LOSE} <b>Вы банкрот!</b> Нажмите «Сбросить баланс».'
    return text

async def _resolve_coin(uid: int, choice: str, amount: int, state: FSMContext) -> tuple[str, int] | None:
    if amount <= 0:
        return None
    result  = flip_coin()
    won     = choice == result
    delta   = amount if won else -amount
    new_bal = update_balance(uid, delta, win=won, game_type="coin")
    if new_bal is None:
        return None
    await state.update_data(last_bet={"game": "coin", "choice": choice, "amount": amount})
    await state.set_state(None)
    side_label   = "Орёл" if choice == "heads" else "Решка"
    result_label = "Орёл" if result == "heads" else "Решка"
    return _coin_result_text(side_label, result_label, amount, won, new_bal), new_bal

@dp.callback_query(CoinState.choosing_amount, F.data == "amount_custom")
async def ask_custom_coin_amount(cq: CallbackQuery, state: FSMContext):
    await state.update_data(waiting_custom=True)
    bal = get_balance(cq.from_user.id)
    await safe_edit_or_send(
        cq,
        f'{E_PEN} <b>Введите сумму ставки:</b>\n{E_COIN} Баланс: <b>{format_chips(bal)}</b>',
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [_btn("Назад", "back_main", icon="5893057118545646106")]
        ]),
    )

@dp.message(CoinState.choosing_amount)
async def handle_coin_amount_input(msg: Message, state: FSMContext):
    data = await state.get_data()
    if not data.get("waiting_custom"):
        return
    bal = get_balance(msg.from_user.id)
    try:
        amount = int(msg.text.strip())
        assert 1 <= amount <= bal
    except Exception:
        await msg.answer(f"❗ Введите целое число от <b>1</b> до <b>{bal}</b>.", parse_mode="HTML")
        return
    res = await _resolve_coin(msg.from_user.id, data["coin_choice"], amount, state)
    if res is None:
        await msg.answer(f'{E_LOSE} Недостаточно средств!', parse_mode="HTML")
        return
    text, new_bal = res
    await msg.answer(text, parse_mode="HTML", reply_markup=game_result_kb("coin", new_bal))

@dp.callback_query(CoinState.choosing_amount, F.data.regexp(r"^amount_\d+$"))
async def place_coin_bet(cq: CallbackQuery, state: FSMContext):
    if not check_cooldown(cq.from_user.id):
        await cq.answer("Не так быстро!", show_alert=False)
        return
    amount = int(cq.data.split("_")[1])
    data   = await state.get_data()
    choice = data.get("coin_choice", "")
    if not choice:
        await cq.answer("Сначала выберите сторону.", show_alert=True)
        return
    if amount <= 0:
        await cq.answer("Сумма должна быть больше 0.", show_alert=True)
        return
    res = await _resolve_coin(cq.from_user.id, choice, amount, state)
    if res is None:
        await cq.answer("Недостаточно средств!", show_alert=True)
        return
    text, new_bal = res
    await safe_edit_or_send(cq, text, reply_markup=game_result_kb("coin", new_bal))

@dp.callback_query(F.data == "repeat_coin")
async def repeat_coin(cq: CallbackQuery, state: FSMContext):
    if not check_cooldown(cq.from_user.id):
        await cq.answer("Не так быстро!", show_alert=False)
        return
    data = await state.get_data()
    last = data.get("last_bet")
    if not last or last.get("game") != "coin":
        await cq.answer("Нет сохранённой ставки.", show_alert=False)
        return
    res = await _resolve_coin(cq.from_user.id, last["choice"], last["amount"], state)
    if res is None:
        await cq.answer(f"Недостаточно фишек! Нужно {last['amount']}.", show_alert=True)
        return
    text, new_bal = res
    await safe_edit_or_send(cq, text, reply_markup=game_result_kb("coin", new_bal))


# ── Rocket ─────────────────────────────────────────────────────────────────

async def _start_rocket(target, state: FSMContext, uid: int, amount: int, is_msg: bool = False):
    crash = generate_crash_point()
    await state.set_state(RocketState.in_game)
    await state.update_data(rocket_amount=amount, rocket_multiplier=1.0,
                             rocket_crash=crash, waiting_custom=False)
    text = _rocket_text(amount, 1.0)
    if is_msg:
        await target.answer(text, parse_mode="HTML", reply_markup=rocket_game_kb())
    else:
        try:
            await target.edit_text(text, parse_mode="HTML", reply_markup=rocket_game_kb())
        except Exception:
            await target.answer(text, parse_mode="HTML", reply_markup=rocket_game_kb())

@dp.callback_query(F.data == "open_rocket")
async def open_rocket(cq: CallbackQuery, state: FSMContext):
    bal = get_balance(cq.from_user.id)
    if bal <= 0:
        await cq.answer("У вас нет фишек! Сбросьте баланс.", show_alert=True)
        return
    await state.set_state(RocketState.choosing_amount)
    await safe_edit_or_send(
        cq,
        f'{E_ROCKET} <b>Ракета</b>\n{E_COIN} Баланс: <b>{format_chips(bal)}</b>\n\n'
        'Ракета взлетает и множитель растёт.\n'
        'Нажми <b>«Дальше»</b> чтобы продолжить, или <b>«Забрать»</b> чтобы зафиксировать.\n'
        'Если ракета взорвётся — ставка сгорает!\n\n<b>Выбери сумму ставки:</b>',
        reply_markup=rocket_amount_kb(bal),
    )

@dp.callback_query(RocketState.choosing_amount, F.data == "rocket_amount_custom")
async def rocket_custom_cb(cq: CallbackQuery, state: FSMContext):
    await state.update_data(waiting_custom=True)
    bal = get_balance(cq.from_user.id)
    await safe_edit_or_send(
        cq,
        f'{E_PEN} <b>Введите сумму ставки:</b>\n{E_COIN} Баланс: <b>{format_chips(bal)}</b>',
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [_btn("Назад", "open_rocket", icon="5893057118545646106")]
        ]),
    )
    await cq.answer()

@dp.callback_query(RocketState.choosing_amount, F.data.regexp(r"^rocket_amount_\d+$"))
async def rocket_set_amount(cq: CallbackQuery, state: FSMContext):
    amount = int(cq.data.replace("rocket_amount_", ""))
    if amount <= 0:
        await cq.answer("Сумма должна быть больше 0.", show_alert=True)
        return
    bal = get_balance(cq.from_user.id)
    if amount > bal:
        await cq.answer("Недостаточно средств!", show_alert=True)
        return
    await _start_rocket(cq.message, state, cq.from_user.id, amount)
    await cq.answer()

@dp.message(RocketState.choosing_amount)
async def rocket_custom_amount(msg: Message, state: FSMContext):
    data = await state.get_data()
    if not data.get("waiting_custom"):
        return
    bal = get_balance(msg.from_user.id)
    try:
        amount = int(msg.text.strip())
        assert 1 <= amount <= bal
    except Exception:
        await msg.answer(f"❗ Введите целое число от <b>1</b> до <b>{bal}</b>.", parse_mode="HTML")
        return
    await _start_rocket(msg, state, msg.from_user.id, amount, is_msg=True)

@dp.callback_query(RocketState.in_game, F.data == "rocket_next")
async def rocket_next(cq: CallbackQuery, state: FSMContext):
    if not check_cooldown(cq.from_user.id, 0.5):
        await cq.answer()
        return
    data        = await state.get_data()
    amount      = data["rocket_amount"]
    multiplier  = data["rocket_multiplier"]
    crash_point = data["rocket_crash"]
    new_mult    = next_multiplier(multiplier)

    if new_mult >= crash_point:
        new_bal = update_balance(cq.from_user.id, -amount, win=False, game_type="rocket")
        if new_bal is None:
            new_bal = get_balance(cq.from_user.id)
        await state.clear()
        text = (
            f'{E_LOSE} <b>РАКЕТА ВЗОРВАЛАСЬ!</b>\n\n'
            f'{E_CHART} Множитель дошёл до: <b>x{crash_point:.2f}</b>\n'
            f'{E_COIN} Ставка: <b>{format_chips(amount)}</b>\n'
            f'{E_LOSE} Потеряно: <b>-{format_chips(amount)}</b>\n\n'
            f'{E_COIN} Новый баланс: <b>{format_chips(new_bal)}</b>'
        )
        await safe_edit_or_send(cq, text, reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [_btn("Сыграть снова", "open_rocket", icon="5373139891223741704")],
            [_btn("В меню",        "back_main",   icon="5893057118545646106")],
        ]))
    else:
        await state.update_data(rocket_multiplier=new_mult)
        try:
            await cq.message.edit_text(
                _rocket_text(amount, new_mult), parse_mode="HTML", reply_markup=rocket_game_kb()
            )
        except Exception:
            pass
    await cq.answer()

@dp.callback_query(RocketState.in_game, F.data == "rocket_cashout")
async def rocket_cashout(cq: CallbackQuery, state: FSMContext):
    data       = await state.get_data()
    amount     = data["rocket_amount"]
    multiplier = data["rocket_multiplier"]
    payout     = int(amount * multiplier)
    net_profit = payout - amount

    if net_profit == 0:
        new_bal = get_balance(cq.from_user.id)
    else:
        new_bal = update_balance(cq.from_user.id, net_profit, win=True, game_type="rocket")
        if new_bal is None:
            new_bal = get_balance(cq.from_user.id)

    await state.clear()
    text = (
        f'{E_WIN} <b>Вы забрали выигрыш!</b>\n\n'
        f'{E_CHART} Множитель: <b>x{multiplier:.2f}</b>\n'
        f'{E_COIN} Ставка: <b>{format_chips(amount)}</b>\n'
        f'{E_PARTY} Выигрыш: <b>+{format_chips(net_profit)}</b>\n\n'
        f'{E_COIN} Новый баланс: <b>{format_chips(new_bal)}</b>'
    )
    await safe_edit_or_send(cq, text, reply_markup=InlineKeyboardMarkup(inline_keyboard=[
        [_btn("Сыграть снова", "open_rocket", icon="5373139891223741704")],
        [_btn("В меню",        "back_main",   icon="5893057118545646106")],
    ]))
    await cq.answer()


# ── Minesweeper ────────────────────────────────────────────────────────────

@dp.callback_query(F.data == "open_minesweeper")
async def open_minesweeper(cq: CallbackQuery, state: FSMContext):
    bal = get_balance(cq.from_user.id)
    if bal <= 0:
        await cq.answer("У вас нет фишек! Сбросьте баланс.", show_alert=True)
        return
    await state.set_state(MinesweeperState.choosing_amount)
    await safe_edit_or_send(
        cq,
        f'{E_BOMB} <b>Сапёр</b>\n{E_COIN} Баланс: <b>{format_chips(bal)}</b>\n\n'
        'Открывай ячейки и не попадись на мину!\n'
        'Чем больше ячеек откроешь — тем выше множитель.\n\n<b>Выбери сумму ставки:</b>',
        reply_markup=bet_amount_kb(bal, back="back_main"),
    )

@dp.callback_query(MinesweeperState.choosing_amount, F.data == "amount_custom")
async def ms_amount_custom(cq: CallbackQuery, state: FSMContext):
    await state.update_data(waiting_custom=True)
    bal = get_balance(cq.from_user.id)
    await safe_edit_or_send(
        cq,
        f'{E_PEN} <b>Введите сумму ставки:</b>\n{E_COIN} Баланс: <b>{format_chips(bal)}</b>',
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [_btn("Назад", "back_main", icon="5893057118545646106")]
        ]),
    )

@dp.callback_query(MinesweeperState.choosing_amount, F.data.regexp(r"^amount_\d+$"))
async def ms_set_amount(cq: CallbackQuery, state: FSMContext):
    amount = int(cq.data.replace("amount_", ""))
    if amount <= 0:
        await cq.answer("Сумма должна быть больше 0.", show_alert=True)
        return
    bal = get_balance(cq.from_user.id)
    if amount > bal:
        await cq.answer("Недостаточно средств!", show_alert=True)
        return
    await state.set_state(MinesweeperState.choosing_mines)
    await state.update_data(minesweeper_amount=amount)
    await safe_edit_or_send(
        cq,
        f'{E_BOMB} <b>Выбери количество мин</b>\n\n{E_COIN} Ставка: <b>{format_chips(amount)}</b>\n\nБольше мин — выше множитель и больше риск!',
        reply_markup=_minesweeper_mines_kb(),
    )
    await cq.answer()

@dp.message(MinesweeperState.choosing_amount)
async def ms_custom_amount(msg: Message, state: FSMContext):
    data = await state.get_data()
    if not data.get("waiting_custom"):
        return
    bal = get_balance(msg.from_user.id)
    try:
        amount = int(msg.text.strip())
        assert 1 <= amount <= bal
    except Exception:
        await msg.answer(f"❗ Введите целое число от <b>1</b> до <b>{bal}</b>.", parse_mode="HTML")
        return
    await state.set_state(MinesweeperState.choosing_mines)
    await state.update_data(minesweeper_amount=amount, waiting_custom=False)
    await msg.answer(
        f'{E_BOMB} <b>Выбери количество мин</b>\n\n{E_COIN} Ставка: <b>{format_chips(amount)}</b>\n\nБольше мин — выше множитель и больше риск!',
        parse_mode="HTML",
        reply_markup=_minesweeper_mines_kb(),
    )

@dp.callback_query(MinesweeperState.choosing_mines, F.data.regexp(r"^ms_mines_\d+$"))
async def ms_start_game(cq: CallbackQuery, state: FSMContext):
    mines    = int(cq.data.split("_")[-1])
    data     = await state.get_data()
    amount   = data["minesweeper_amount"]
    field    = generate_minesweeper_field(size=5, mines=mines)
    revealed = [[False] * 5 for _ in range(5)]
    opened   = 0
    await state.set_state(MinesweeperState.in_game)
    await state.update_data(minesweeper_field=field, minesweeper_revealed=revealed,
                             minesweeper_mines=mines)
    await safe_edit_or_send(
        cq,
        _minesweeper_text(amount, field, revealed, mines),
        reply_markup=_minesweeper_kb(revealed, opened),
    )
    await cq.answer()

@dp.callback_query(MinesweeperState.in_game, F.data.regexp(r"^ms_cell_\d+_\d+$"))
async def ms_open_cell(cq: CallbackQuery, state: FSMContext):
    uid      = cq.from_user.id
    parts    = cq.data.split("_")
    row, col = int(parts[2]), int(parts[3])
    data     = await state.get_data()
    field    = data["minesweeper_field"]
    revealed = data["minesweeper_revealed"]
    amount   = data["minesweeper_amount"]
    mines    = data["minesweeper_mines"]

    if revealed[row][col]:
        await cq.answer("Эта ячейка уже открыта!", show_alert=False)
        return

    revealed[row][col] = True
    opened = sum(sum(r) for r in revealed)

    if field[row][col]:
        # Попал на мину
        new_bal = update_balance(uid, -amount, win=False, game_type="minesweeper")
        if new_bal is None:
            new_bal = get_balance(uid)
        await state.clear()
        text = (
            f'{E_LOSE} <b>МИНА!</b>\n\n'
            f'{E_COIN} Ставка: <b>{format_chips(amount)}</b>\n'
            f'{E_LOSE} Потеряно: <b>-{format_chips(amount)}</b>\n\n'
            f'{E_COIN} Новый баланс: <b>{format_chips(new_bal)}</b>'
        )
        if new_bal <= 0:
            text += f'\n\n{E_LOSE} <b>Вы банкрот!</b> Нажмите «Сбросить баланс».'
        await safe_edit_or_send(cq, text, reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [_btn("Сыграть снова", "open_minesweeper", icon="5373141891321699086")],
            [_btn("В меню",        "back_main",         icon="5893057118545646106")],
            *([[_btn("Сбросить баланс", "reset", icon="5345906554510012647")]] if new_bal <= 0 else []),
        ]))
    else:
        await state.update_data(minesweeper_revealed=revealed)
        try:
            await cq.message.edit_text(
                _minesweeper_text(amount, field, revealed, mines),
                parse_mode="HTML",
                reply_markup=_minesweeper_kb(revealed, opened),
            )
        except Exception:
            pass
    await cq.answer()

@dp.callback_query(MinesweeperState.in_game, F.data == "ms_cashout")
async def ms_cashout(cq: CallbackQuery, state: FSMContext):
    uid      = cq.from_user.id
    data     = await state.get_data()
    amount   = data["minesweeper_amount"]
    revealed = data["minesweeper_revealed"]
    mines    = data["minesweeper_mines"]
    opened   = sum(sum(row) for row in revealed)

    # Защита: нельзя забрать без открытых ячеек
    if opened == 0:
        await cq.answer("Сначала открой хотя бы одну ячейку!", show_alert=True)
        return

    base   = {3: 1.1, 5: 1.3, 7: 2.0, 10: 2.8}.get(mines, 2.0)
    mult   = base * (1.0 + opened * 0.05)
    profit = int(amount * mult) - amount

    if profit == 0:
        new_bal = get_balance(uid)
    else:
        new_bal = update_balance(uid, profit, win=True, game_type="minesweeper")
        if new_bal is None:
            new_bal = get_balance(uid)

    await state.clear()
    text = (
        f'{E_WIN} <b>Вы забрали выигрыш!</b>\n\n'
        f'{E_WIN} Открыто ячеек: <b>{opened}</b>\n'
        f'{E_CHART} Множитель: <b>x{mult:.2f}</b>\n'
        f'{E_COIN} Ставка: <b>{format_chips(amount)}</b>\n'
        f'{E_PARTY} Выигрыш: <b>+{format_chips(profit)}</b>\n\n'
        f'{E_COIN} Новый баланс: <b>{format_chips(new_bal)}</b>'
    )
    await safe_edit_or_send(cq, text, reply_markup=InlineKeyboardMarkup(inline_keyboard=[
        [_btn("Сыграть снова", "open_minesweeper", icon="5373141891321699086")],
        [_btn("В меню",        "back_main",         icon="5893057118545646106")],
    ]))
    await cq.answer()


# ── Admin ──────────────────────────────────────────────────────────────────

@dp.message(Command("admin"))
async def cmd_admin(msg: Message, state: FSMContext):
    if msg.from_user.id != ADMIN_ID:
        await msg.answer(f'{E_LOSE} У вас нет доступа к админ-панели', parse_mode="HTML")
        return
    await state.set_state(AdminState.choosing_action)
    await msg.answer(
        f'{E_GEAR} <b>Админ-панель</b>\n\nВыберите действие:',
        parse_mode="HTML",
        reply_markup=admin_menu_kb(),
    )

@dp.callback_query(F.data == "admin_users")
async def show_users_list(cq: CallbackQuery, state: FSMContext):
    if cq.from_user.id != ADMIN_ID:
        await cq.answer("Только для админа.", show_alert=True)
        return
    await state.set_state(AdminState.choosing_user)
    users = get_all_users_full()
    await safe_edit_or_send(
        cq,
        f'{E_USERS} <b>Список пользователей:</b>\n\nВсего: {len(users)}\n\nВыберите для редактирования:',
        reply_markup=users_list_kb(users, 0),
    )

@dp.callback_query(F.data.regexp(r"^admin_users_page_\d+$"))
async def paginate_users(cq: CallbackQuery):
    if cq.from_user.id != ADMIN_ID:
        return
    page  = int(cq.data.split("_")[-1])
    users = get_all_users_full()
    await safe_edit_or_send(
        cq,
        f'{E_USERS} <b>Список пользователей:</b>\n\nВсего: {len(users)}\n\nВыберите для редактирования:',
        reply_markup=users_list_kb(users, page),
    )

@dp.callback_query(F.data.regexp(r"^admin_edit_user_\d+$"))
async def edit_user_menu(cq: CallbackQuery, state: FSMContext):
    if cq.from_user.id != ADMIN_ID:
        return
    uid  = int(cq.data.split("_")[-1])
    user = get_user(uid)
    if not user:
        await cq.answer("Пользователь не найден.", show_alert=True)
        return
    text = (
        f'{E_USER} <b>Профиль пользователя:</b>\n\n'
        f'ID: <code>{uid}</code>\n'
        f'Имя: <b>{html.escape(user["username"] or "Неизвестно")}</b>\n'
        f'{E_COIN} Баланс: <b>{format_chips(user["balance"])}</b>\n'
        f'{E_WIN} Побед: <b>{user["wins"]}</b>\n'
        f'{E_LOSE} Поражений: <b>{user["losses"]}</b>\n\n'
        f'Выберите действие:'
    )
    await state.update_data(admin_user_id=uid)
    await safe_edit_or_send(cq, text, reply_markup=edit_user_kb(uid))

@dp.callback_query(F.data.regexp(r"^admin_user_history_\d+$"))
async def admin_user_history(cq: CallbackQuery):
    if cq.from_user.id != ADMIN_ID:
        return
    uid  = int(cq.data.split("_")[-1])
    user = get_user(uid)
    if not user:
        await cq.answer("Пользователь не найден.", show_alert=True)
        return
    await safe_edit_or_send(
        cq,
        history_text(uid),
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [_btn("Назад к пользователю", f"admin_edit_user_{uid}", icon="5893057118545646106")],
        ]),
    )

@dp.callback_query(F.data.regexp(r"^admin_edit_balance_\d+$"))
async def ask_new_balance(cq: CallbackQuery, state: FSMContext):
    if cq.from_user.id != ADMIN_ID:
        return
    uid = int(cq.data.split("_")[-1])
    await state.update_data(admin_user_id=uid)
    await state.set_state(AdminState.editing_balance)
    await safe_edit_or_send(
        cq,
        f'{E_COIN} Текущий баланс: <b>{format_chips(get_balance(uid))}</b>\n\nВведите сумму, которую хотите <b>добавить</b>:',
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [_btn("Назад", "admin_users_back", icon="5893057118545646106")]
        ]),
    )

@dp.message(AdminState.editing_balance)
async def process_new_balance(msg: Message, state: FSMContext):
    if msg.from_user.id != ADMIN_ID:
        return
    try:
        add_amount = int(msg.text.strip())
        assert add_amount >= 0
    except Exception:
        await msg.answer(f'{E_LOSE} Введите корректное неотрицательное число.', parse_mode="HTML")
        return
    data    = await state.get_data()
    uid     = data["admin_user_id"]
    old_bal = get_balance(uid)
    new_bal = old_bal + add_amount
    set_balance(uid, new_bal)
    add_history(uid, add_amount, True, game_type="admin_topup")
    user = get_user(uid)
    await state.clear()
    await msg.answer(
        f'{E_WIN} <b>Баланс обновлён!</b>\n\n'
        f'{E_USER} Пользователь: <b>{html.escape(user["username"] or "Неизвестно")}</b> (ID: {uid})\n'
        f'{E_COIN} Было: <b>{format_chips(old_bal)}</b>\n'
        f'{E_COIN2} Добавлено: <b>+{format_chips(add_amount)}</b>\n'
        f'{E_WIN} Итого: <b>{format_chips(new_bal)}</b>',
        parse_mode="HTML",
        reply_markup=admin_menu_kb(),
    )
    try:
        await bot.send_message(
            uid,
            f'{E_COIN} <b>Баланс пополнен!</b>\n\n'
            f'{E_COIN2} Добавлено: <b>+{format_chips(add_amount)}</b>\n'
            f'{E_WIN} Текущий баланс: <b>{format_chips(new_bal)}</b>',
            parse_mode="HTML",
        )
    except Exception:
        pass

@dp.callback_query(F.data == "admin_broadcast")
async def broadcast_menu(cq: CallbackQuery, state: FSMContext):
    if cq.from_user.id != ADMIN_ID:
        await cq.answer("Только для админа.", show_alert=True)
        return
    users = get_all_users_full()
    await state.set_state(AdminState.sending_broadcast)
    await safe_edit_or_send(
        cq,
        f'{E_HORN} <b>Рассылка сообщений</b>\n\nАдресатов: {len(users)}\n\n'
        'Введите сообщение для всех пользователей.\n<i>Поддерживает HTML форматирование.</i>',
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [_btn("Отмена", "admin_back", icon="5870657884844462243")]
        ]),
    )

@dp.message(AdminState.sending_broadcast)
async def process_broadcast(msg: Message, state: FSMContext):
    if msg.from_user.id != ADMIN_ID:
        return
    broadcast_text = msg.text or ""
    users = get_all_users_full()
    ok = fail = 0
    for u in users:
        try:
            await bot.send_message(u["user_id"], broadcast_text, parse_mode="HTML")
            ok += 1
        except Exception:
            fail += 1
    await state.clear()
    await msg.answer(
        f'{E_WIN} <b>Рассылка завершена!</b>\n\nУспешно: <b>{ok}</b>\nОшибок: <b>{fail}</b>',
        parse_mode="HTML",
        reply_markup=admin_menu_kb(),
    )

@dp.callback_query(F.data == "admin_back")
async def admin_back_to_menu(cq: CallbackQuery, state: FSMContext):
    if cq.from_user.id != ADMIN_ID:
        return
    await state.clear()
    await state.set_state(AdminState.choosing_action)
    await safe_edit_or_send(
        cq,
        f'{E_GEAR} <b>Админ-панель</b>\n\nВыберите действие:',
        reply_markup=admin_menu_kb(),
    )

@dp.callback_query(F.data == "admin_clear_history")
async def admin_clear_history(cq: CallbackQuery):
    if cq.from_user.id != ADMIN_ID:
        return
    clear_all_history()
    await safe_edit_or_send(
        cq,
        f'{E_WIN} <b>История ставок очищена!</b>',
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [_btn("Назад в админ-панель", "admin_back", icon="5893057118545646106")]
        ]),
    )
    await cq.answer("История очищена.", show_alert=True)

@dp.callback_query(F.data == "admin_users_back")
async def admin_back_to_users(cq: CallbackQuery, state: FSMContext):
    if cq.from_user.id != ADMIN_ID:
        return
    await state.set_state(AdminState.choosing_user)
    users = get_all_users_full()
    await safe_edit_or_send(
        cq,
        f'{E_USERS} <b>Список пользователей:</b>\n\nВсего: {len(users)}\n\nВыберите для редактирования:',
        reply_markup=users_list_kb(users, 0),
    )


# ── Background tasks ───────────────────────────────────────────────────────

async def daily_bonus_task():
    msk = pytz.timezone("Europe/Moscow")
    while True:
        now    = datetime.now(msk)
        target = now.replace(hour=12, minute=0, second=0, microsecond=0)
        if now >= target:
            target += timedelta(days=1)
        await asyncio.sleep((target - now).total_seconds())

        for uid in get_all_users():
            add_daily_bonus(uid)
            new_bal = get_balance(uid)

            bonus_msg = (
                f'{E_GIFT} <b>Ежедневный бонус!</b>\n\n'
                f'{E_COIN2} Начислено <b>+10 000 фишек</b>\n'
                f'{E_COIN} Текущий баланс: <b>{format_chips(new_bal)}</b>\n'
            )

            biz_owned = get_user_businesses(uid)
            if biz_owned:
                biz_pending = sum(
                    get_business_pending(biz_owned[bt], bt)
                    for bt in biz_owned if bt in BUSINESSES
                )
                if biz_pending:
                    bonus_msg += f'{E_BRIEFCASE} Бизнесы накопили: <b>{format_chips(biz_pending)}</b>\n'

            farm = get_farm(uid)
            if farm:
                farm_pending = get_farm_pending(farm)
                if farm_pending:
                    bonus_msg += f'{E_FARM} Ферма накопила: <b>{format_chips(farm_pending)}</b>\n'

            bonus_msg += '\n<i>Удачной игры!</i>'

            try:
                await bot.send_message(uid, bonus_msg, parse_mode="HTML")
            except Exception:
                pass

        await asyncio.sleep(60)


# ── Main ───────────────────────────────────────────────────────────────────

async def main():
    init_db()
    print("🎰 Casino bot started!")
    asyncio.create_task(daily_bonus_task())
    try:
        await dp.start_polling(bot)
    finally:
        global db_pool
        if db_pool is not None:
            db_pool.closeall()
            print("🗄 Database connection pool closed.")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("🛑 Бот остановлен.")