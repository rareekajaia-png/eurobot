import asyncio
import random
import os
import time
from datetime import datetime, timedelta
import pytz
from dotenv import load_dotenv
import psycopg2
from psycopg2.extras import RealDictCursor
from psycopg2 import pool
from aiogram import Bot, Dispatcher, F
from aiogram.types import (
    Message, CallbackQuery,
    InlineKeyboardMarkup, InlineKeyboardButton,
    LabeledPrice, PreCheckoutQuery,
)
from aiogram.filters import CommandStart, Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage

load_dotenv()
TOKEN            = os.getenv("BOT_TOKEN")
DATABASE_URL     = os.getenv("DATABASE_URL")
ADMIN_ID         = int(os.getenv("ADMIN_ID", "0"))
STARTING_BALANCE = 1000

bot = Bot(token=TOKEN)
dp  = Dispatcher(storage=MemoryStorage())

db_pool: pool.SimpleConnectionPool | None = None

last_bets:     dict = {}
last_messages: dict = {}


# ── DB pool ───────────────────────────────────────────────────────────────────

def get_db_pool() -> pool.SimpleConnectionPool:
    global db_pool
    if db_pool is None:
        db_pool = pool.SimpleConnectionPool(1, 10, DATABASE_URL)
    return db_pool

def db_connect():
    return get_db_pool().getconn()

def db_release(conn):
    get_db_pool().putconn(conn)


# ── Message store helpers ─────────────────────────────────────────────────────

async def delete_old_message(user_id: int):
    if user_id in last_messages:
        try:
            await bot.delete_message(user_id, last_messages[user_id])
        except Exception:
            pass

def store_message(user_id: int, msg_id: int):
    last_messages[user_id] = msg_id
    save_message_id(user_id, msg_id)

async def cleanup_old_messages():
    for user_id, msg_id in (load_all_message_ids() or []):
        try:
            await bot.delete_message(user_id, msg_id)
            last_messages[user_id] = None
        except Exception:
            pass

async def safe_edit_or_send(cq: CallbackQuery, text: str, reply_markup=None):
    """Пробует edit_text, при ошибке отправляет новое сообщение."""
    try:
        await cq.message.edit_text(text, parse_mode="HTML", reply_markup=reply_markup)
        store_message(cq.from_user.id, cq.message.message_id)
    except Exception:
        try:
            await cq.message.delete()
        except Exception:
            pass
        response = await cq.message.answer(text, parse_mode="HTML", reply_markup=reply_markup)
        store_message(cq.from_user.id, response.message_id)


# ── DB init ───────────────────────────────────────────────────────────────────

def _exec(sql: str, params=None, ignore_errors=True):
    conn = db_connect()
    try:
        with conn.cursor() as cur:
            cur.execute(sql, params)
        conn.commit()
    except Exception:
        conn.rollback()
        if not ignore_errors:
            raise
    finally:
        db_release(conn)

def init_db():
    _exec("""
        CREATE TABLE IF NOT EXISTS users (
            user_id         BIGINT PRIMARY KEY,
            username        TEXT,
            balance         INTEGER DEFAULT 1000,
            wins            INTEGER DEFAULT 0,
            losses          INTEGER DEFAULT 0,
            last_message_id INTEGER
        )
    """)
    _exec("ALTER TABLE users ADD COLUMN IF NOT EXISTS last_message_id INTEGER")
    _exec("""
        CREATE TABLE IF NOT EXISTS history (
            id         SERIAL PRIMARY KEY,
            user_id    BIGINT NOT NULL,
            amount     INTEGER NOT NULL,
            is_win     BOOLEAN NOT NULL,
            game_type  TEXT DEFAULT 'roulette',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (user_id) REFERENCES users(user_id)
        )
    """)
    _exec("CREATE INDEX IF NOT EXISTS idx_history_user_id ON history(user_id)")
    _exec("""
        CREATE TABLE IF NOT EXISTS farms (
            user_id      BIGINT PRIMARY KEY,
            level        INTEGER DEFAULT 1,
            last_collect TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (user_id) REFERENCES users(user_id)
        )
    """)


# ── DB helpers ────────────────────────────────────────────────────────────────

def get_user(user_id: int):
    conn = db_connect()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM users WHERE user_id=%s", (user_id,))
            return cur.fetchone()
    finally:
        db_release(conn)

def get_or_create_user(user_id: int, username: str) -> int:
    conn = db_connect()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """INSERT INTO users (user_id, username, balance) VALUES (%s,%s,%s)
                   ON CONFLICT (user_id) DO UPDATE SET username = EXCLUDED.username
                   RETURNING balance""",
                (user_id, username, STARTING_BALANCE),
            )
            result = cur.fetchone()
            conn.commit()
            return result[0] if result else STARTING_BALANCE
    finally:
        db_release(conn)

def get_balance(user_id: int) -> int:
    conn = db_connect()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT balance FROM users WHERE user_id=%s", (user_id,))
            row = cur.fetchone()
        return row[0] if row else 0
    finally:
        db_release(conn)

def update_balance(user_id: int, delta: int, win: bool, game_type: str = "roulette"):
    col = "wins" if win else "losses"
    conn = db_connect()
    try:
        with conn.cursor() as cur:
            cur.execute(
                f"UPDATE users SET balance = balance + %s, {col} = {col} + 1 WHERE user_id=%s",
                (delta, user_id),
            )
        conn.commit()
    finally:
        db_release(conn)
    add_history(user_id, abs(delta), win, game_type)

def set_balance(user_id: int, amount: int):
    conn = db_connect()
    try:
        with conn.cursor() as cur:
            cur.execute("UPDATE users SET balance=%s WHERE user_id=%s", (amount, user_id))
        conn.commit()
    finally:
        db_release(conn)

def reset_balance(user_id: int):
    set_balance(user_id, STARTING_BALANCE)

def get_all_users() -> list[int]:
    conn = db_connect()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT user_id FROM users")
            return [r[0] for r in cur.fetchall()]
    finally:
        db_release(conn)

def get_all_users_full():
    conn = db_connect()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT * FROM users ORDER BY user_id")
            return cur.fetchall()
    finally:
        db_release(conn)

def add_daily_bonus(user_id: int):
    conn = db_connect()
    try:
        with conn.cursor() as cur:
            cur.execute("UPDATE users SET balance = balance + 500 WHERE user_id=%s", (user_id,))
        conn.commit()
    finally:
        db_release(conn)

def save_message_id(user_id: int, msg_id: int):
    conn = db_connect()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE users SET last_message_id=%s WHERE user_id=%s", (msg_id, user_id)
            )
        conn.commit()
    finally:
        db_release(conn)

def load_all_message_ids():
    conn = db_connect()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT user_id, last_message_id FROM users WHERE last_message_id IS NOT NULL"
            )
            return cur.fetchall()
    finally:
        db_release(conn)

def add_history(user_id: int, amount: int, is_win: bool, game_type: str = "roulette"):
    conn = db_connect()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO history (user_id, amount, is_win, game_type) VALUES (%s,%s,%s,%s)",
                (user_id, amount, is_win, game_type),
            )
        conn.commit()
    finally:
        db_release(conn)

def get_history(user_id: int, limit: int = 10):
    conn = db_connect()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """SELECT amount, is_win, game_type, created_at FROM history
                   WHERE user_id=%s ORDER BY created_at DESC LIMIT %s""",
                (user_id, limit),
            )
            return cur.fetchall()
    finally:
        db_release(conn)

def get_user_history_stats(user_id: int):
    conn = db_connect()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """SELECT
                   SUM(CASE WHEN is_win     THEN amount ELSE 0 END) AS total_won,
                   SUM(CASE WHEN NOT is_win THEN amount ELSE 0 END) AS total_lost,
                   COUNT(CASE WHEN is_win     THEN 1 END) AS win_count,
                   COUNT(CASE WHEN NOT is_win THEN 1 END) AS lose_count
                   FROM history WHERE user_id=%s AND game_type != 'admin_topup'""",
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


# ── Farm DB ───────────────────────────────────────────────────────────────────

FARM_BUY_COST      = 5000
FARM_UPGRADE_COSTS = {
    1: 3000, 2: 8000, 3: 20000, 4: 50000,
    5: 120000, 6: 300000, 7: 700000, 8: 1500000, 9: 3000000,
}
FARM_INCOME = {
    1: 200, 2: 500, 3: 1200, 4: 3000, 5: 7000,
    6: 15000, 7: 35000, 8: 80000, 9: 180000, 10: 400000,
}
FARM_LEVEL_NAMES = {
    1: "Малинка",    2: "Ноутбук",   3: "Десктоп",    4: "Стойка",
    5: "Мини-ферма", 6: "Ферма",     7: "Датацентр",  8: "Мега-ферма",
    9: "Гиперцентр", 10: "Квантовая ферма",
}

def get_farm(user_id: int):
    conn = db_connect()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT * FROM farms WHERE user_id=%s", (user_id,))
            return cur.fetchone()
    finally:
        db_release(conn)

def buy_farm(user_id: int):
    conn = db_connect()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO farms (user_id, level, last_collect) VALUES (%s,1,CURRENT_TIMESTAMP)"
                " ON CONFLICT DO NOTHING",
                (user_id,),
            )
        conn.commit()
    finally:
        db_release(conn)

def upgrade_farm(user_id: int):
    conn = db_connect()
    try:
        with conn.cursor() as cur:
            cur.execute("UPDATE farms SET level = level + 1 WHERE user_id=%s", (user_id,))
        conn.commit()
    finally:
        db_release(conn)

def collect_farm(user_id: int):
    conn = db_connect()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE farms SET last_collect = CURRENT_TIMESTAMP WHERE user_id=%s", (user_id,)
            )
        conn.commit()
    finally:
        db_release(conn)

def get_farm_pending(farm) -> int:
    if not farm:
        return 0
    last = farm["last_collect"]
    if last.tzinfo is None:
        last = last.replace(tzinfo=pytz.utc)
    hours = min((datetime.now(pytz.utc) - last).total_seconds() / 3600, 24)
    return int(hours * FARM_INCOME.get(farm["level"], 0))


# ── Minesweeper multiplier ────────────────────────────────────────────────────

MS_STEP = {3: 0.15, 5: 0.20, 7: 0.30, 10: 0.45}

def ms_multiplier(opened_count: int, mines: int) -> float:
    return round(1.0 + opened_count * MS_STEP.get(mines, 0.20), 2)


# ── States ────────────────────────────────────────────────────────────────────

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


# ── Roulette helpers ──────────────────────────────────────────────────────────

RED_NUMBERS   = {1,3,5,7,9,12,14,16,18,19,21,23,25,27,30,32,34,36}
BLACK_NUMBERS = {2,4,6,8,10,11,13,15,17,20,22,24,26,28,29,31,33,35}

BET_LABELS = {
    "red":    "Красное",    "black":  "Чёрное",
    "even":   "ЧЁТНОЕ",     "odd":    "НЕЧЁТНОЕ",
    "1-18":   "1–18",       "19-36":  "19–36",
    "1st12":  "1ST 12",     "2nd12":  "2ND 12",    "3rd12":  "3RD 12",
    "2to1_1": "2to1 (ряд 1)", "2to1_2": "2to1 (ряд 2)", "2to1_3": "2to1 (ряд 3)",
}

GAME_NAMES = {
    "coin": "Монета", "rocket": "Ракета", "minesweeper": "Сапер",
    "admin_topup": "Пополнение от админа", "farm": "Ферма",
}

def noun_form(count, s1, s2, s5):
    n = int(count) % 100
    if n % 10 == 1 and n != 11:        return s1
    if n % 10 in (2,3,4) and n not in (12,13,14): return s2
    return s5

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
        return f"{fmt(amount)} фишек"
    if amount >= 1_000:
        base = amount / 1_000
        base_int = round(base, 1)
    else:
        base_int = amount
    return f"{fmt(amount)} {noun_form(base_int, 'фишка', 'фишки', 'фишек')}"

def spin_wheel() -> int:
    return random.randint(0, 36)

def number_color(n: int) -> str:
    if n == 0:             return "🟢"
    if n in RED_NUMBERS:   return "🔴"
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


# ── Keyboards ─────────────────────────────────────────────────────────────────

def _btn(text: str, cb: str = None, url: str = None, emoji_id: str = None) -> InlineKeyboardButton:
    kwargs = {"text": text}
    if cb:       kwargs["callback_data"] = cb
    if url:      kwargs["url"] = url
    if emoji_id: kwargs["icon_custom_emoji_id"] = emoji_id
    return InlineKeyboardButton(**kwargs)

def main_menu_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            _btn("Рулетка",       "open_roulette",     emoji_id="5258882890059091157"),
            _btn("Орёл или Решка","open_coin",          emoji_id="5774585885154131652"),
        ],
        [
            _btn("Ракета",        "open_rocket",        emoji_id="5373139891223741704"),
            _btn("Сапер",         "open_minesweeper",   emoji_id="5373141891321699086"),
        ],
        [_btn("Биткоин-ферма",    "open_farm",          emoji_id="5904462880941545555")],
        [
            _btn("Профиль",       "stats",              emoji_id="5870921681735781843"),
            _btn("Сбросить баланс","reset",             emoji_id="5345906554510012647"),
        ],
    ])

def farm_kb(user_id: int):
    farm = get_farm(user_id)
    bal  = get_balance(user_id)
    back = [_btn("Назад", "back_main", emoji_id="5893057118545646106")]

    if not farm:
        row = (
            [_btn(f"Купить ферму ({fmt(FARM_BUY_COST)})", "farm_buy", emoji_id="5904462880941545555")]
            if bal >= FARM_BUY_COST
            else [_btn(f"Нужно {fmt(FARM_BUY_COST)} фишек", "farm_noop")]
        )
        return InlineKeyboardMarkup(inline_keyboard=[row, back])

    level   = farm["level"]
    pending = get_farm_pending(farm)
    rows    = []

    if pending > 0:
        rows.append([_btn(f"Собрать +{fmt(pending)}", "farm_collect", emoji_id="5870633910337015697")])

    if level < 10:
        cost = FARM_UPGRADE_COSTS.get(level, 0)
        rows.append(
            [_btn(f"Улучшить до ур.{level+1} ({fmt(cost)})", "farm_upgrade", emoji_id="5870930636742595124")]
            if bal >= cost
            else [_btn(f"Нужно {fmt(cost)} для улучшения", "farm_noop")]
        )

    rows.append(back)
    return InlineKeyboardMarkup(inline_keyboard=rows)

def _percent_amount_kb(balance: int, prefix: str, back_cb: str):
    q1, q2, q3, q4 = (max(1, round(balance * p)) for p in (0.25, 0.50, 0.75, 1.0))
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            _btn(f"25%  ({fmt(q1)})", f"{prefix}{q1}"),
            _btn(f"75%  ({fmt(q3)})", f"{prefix}{q3}"),
        ],
        [
            _btn(f"50%  ({fmt(q2)})", f"{prefix}{q2}"),
            _btn(f"Ва-банк ({fmt(q4)})", f"{prefix}{q4}", emoji_id="6041731551845159060"),
        ],
        [_btn("Ввести вручную", f"{prefix}custom", emoji_id="5870676941614354370")],
        [_btn("Назад", back_cb, emoji_id="5893057118545646106")],
    ])

def bet_amount_kb(balance: int):
    return _percent_amount_kb(balance, "amount_", "back_bet_type")

def rocket_amount_kb(balance: int):
    return _percent_amount_kb(balance, "rocket_amount_", "back_main")

def bet_type_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [_btn("Красное","bet_red", emoji_id="5870657884844462243"),
         _btn("Чёрное", "bet_black", emoji_id="5870657884844462243")],
        [_btn("1-18",  "bet_1-18"),  _btn("19-36","bet_19-36")],
        [_btn("ЧЁТНОЕ","bet_even"),  _btn("НЕЧЁТНОЕ","bet_odd")],
        [_btn("1ST 12","bet_1st12"), _btn("2ND 12","bet_2nd12"), _btn("3RD 12","bet_3rd12")],
        [_btn("2to1 (ряд 1)","bet_2to1_1"),
         _btn("2to1 (ряд 2)","bet_2to1_2"),
         _btn("2to1 (ряд 3)","bet_2to1_3")],
        [_btn("Конкретное число (x35)","bet_number", emoji_id="5771851822897566479")],
        [_btn("Назад","back_main", emoji_id="5893057118545646106")],
    ])

def coin_side_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [_btn("Орёл","coin_heads", emoji_id="5774585885154131652"),
         _btn("Решка","coin_tails", emoji_id="5904462880941545555")],
        [_btn("Назад","back_main", emoji_id="5893057118545646106")],
    ])

def rocket_game_kb():
    return InlineKeyboardMarkup(inline_keyboard=[[
        _btn("Дальше",  "rocket_next",    emoji_id="5373139891223741704"),
        _btn("Забрать", "rocket_cashout", emoji_id="5904462880941545555"),
    ]])

def game_result_kb(game_type: str):
    repeat = "repeat_coin" if game_type == "coin" else "repeat_roulette"
    return InlineKeyboardMarkup(inline_keyboard=[
        [_btn("Повторить", repeat,      emoji_id="5345906554510012647")],
        [_btn("В меню",    "back_main", emoji_id="5893057118545646106")],
    ])

def stats_menu_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [_btn("История ставок",  "stats_history", emoji_id="5870930636742595124")],
        [_btn("Накормить автора","donate",         emoji_id="5904462880941545555")],
        [_btn("Назад в меню",    "back_main",      emoji_id="5893057118545646106")],
    ])

def admin_menu_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [_btn("Список пользователей","admin_users",          emoji_id="5870772616305839506")],
        [_btn("Рассылка сообщения",  "admin_broadcast",      emoji_id="6039422865189638057")],
        [_btn("Очистить историю",    "admin_clear_history",  emoji_id="5870657884844462243")],
        [_btn("Вернуться в меню",    "back_main",            emoji_id="5893057118545646106")],
    ])

def donate_kb():
    rows = [
        [_btn(f"{stars} звёзд", f"donate_{stars}", emoji_id="5870982283724328568")]
        for stars in (50, 100, 250, 500, 1000)
    ]
    rows += [
        [_btn("Своя сумма",    "donate_custom", emoji_id="5870676941614354370")],
        [_btn("Назад в меню",  "back_main",     emoji_id="5893057118545646106")],
    ]
    return InlineKeyboardMarkup(inline_keyboard=rows)

def users_list_kb(users: list, page: int = 0):
    per_page   = 5
    start      = page * per_page
    page_users = users[start:start + per_page]
    rows = [
        [_btn(
            f"{u['username'] or f'ID {u[\"user_id\"]}'}  ({format_chips(u['balance'])})",
            f"admin_edit_user_{u['user_id']}",
            emoji_id="5870994129244131212",
        )]
        for u in page_users
    ]
    nav = []
    if page > 0:
        nav.append(_btn("◁ Назад", f"admin_users_page_{page-1}"))
    if start + per_page < len(users):
        nav.append(_btn("Вперед ▷", f"admin_users_page_{page+1}"))
    if nav:
        rows.append(nav)
    rows.append([_btn("В меню админа", "admin_back", emoji_id="5893057118545646106")])
    return InlineKeyboardMarkup(inline_keyboard=rows)

def edit_user_kb(user_id: int):
    return InlineKeyboardMarkup(inline_keyboard=[
        [_btn("Пополнить баланс", f"admin_edit_balance_{user_id}", emoji_id="5904462880941545555")],
        [_btn("История ставок",   f"admin_user_history_{user_id}", emoji_id="5870930636742595124")],
        [_btn("Назад к списку",   "admin_users_back",              emoji_id="5893057118545646106")],
    ])

def _minesweeper_mines_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            _btn("3 мины  (+0.15/яч.)", "ms_mines_3"),
            _btn("5 мин   (+0.20/яч.)", "ms_mines_5"),
        ],
        [
            _btn("7 мин   (+0.30/яч.)", "ms_mines_7"),
            _btn("10 мин  (+0.45/яч.)", "ms_mines_10"),
        ],
        [_btn("Назад", "back_main", emoji_id="5893057118545646106")],
    ])

def _minesweeper_kb(revealed: list):
    rows = [
        [
            _btn("✅" if revealed[r][c] else "🟫", f"ms_cell_{r}_{c}")
            for c in range(5)
        ]
        for r in range(5)
    ]
    rows.append([_btn("Забрать", "ms_cashout", emoji_id="5870633910337015697")])
    rows.append([_btn("В меню",  "back_main",  emoji_id="5893057118545646106")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


# ── Text helpers ──────────────────────────────────────────────────────────────

E = {
    "slot":    '<tg-emoji emoji-id="5258882890059091157">🎰</tg-emoji>',
    "coin_g":  '<tg-emoji emoji-id="5774585885154131652">🪙</tg-emoji>',
    "chip":    '<tg-emoji emoji-id="5904462880941545555">🪙</tg-emoji>',
    "chart":   '<tg-emoji emoji-id="5870930636742595124">📊</tg-emoji>',
    "ok":      '<tg-emoji emoji-id="5870633910337015697">✅</tg-emoji>',
    "fail":    '<tg-emoji emoji-id="5870657884844462243">❌</tg-emoji>',
    "pencil":  '<tg-emoji emoji-id="5870676941614354370">🖋</tg-emoji>',
    "reload":  '<tg-emoji emoji-id="5345906554510012647">🔄</tg-emoji>',
    "rocket":  '<tg-emoji emoji-id="5373139891223741704">🚀</tg-emoji>',
    "mine":    '<tg-emoji emoji-id="5373141891321699086">💣</tg-emoji>',
    "party":   '<tg-emoji emoji-id="6041731551845159060">🎉</tg-emoji>',
    "profit":  '<tg-emoji emoji-id="5890848474563352982">🪙</tg-emoji>',
    "clock":   '<tg-emoji emoji-id="5983150113483134607">⏰</tg-emoji>',
    "gift":    '<tg-emoji emoji-id="6032644646587338669">🎁</tg-emoji>',
    "gear":    '<tg-emoji emoji-id="5870982283724328568">⚙️</tg-emoji>',
    "people":  '<tg-emoji emoji-id="5870772616305839506">👥</tg-emoji>',
    "person":  '<tg-emoji emoji-id="5870994129244131212">👤</tg-emoji>',
    "horn":    '<tg-emoji emoji-id="6039422865189638057">📣</tg-emoji>',
    "bank":    '<tg-emoji emoji-id="5879814368572478751">🏧</tg-emoji>',
    "box":     '<tg-emoji emoji-id="5778672437122045013">📦</tg-emoji>',
    "stats2":  '<tg-emoji emoji-id="5870921681735781843">📊</tg-emoji>',
    "abc":     '<tg-emoji emoji-id="5771851822897566479">🔡</tg-emoji>',
}

def farm_text(user_id: int) -> str:
    farm    = get_farm(user_id)
    bal     = get_balance(user_id)
    pending = get_farm_pending(farm)

    if not farm:
        return (
            f'{E["chip"]} <b>Биткоин-ферма</b>\n\n'
            f'У вас ещё нет фермы.\n\n'
            f'{E["chip"]} Стоимость: <b>{format_chips(FARM_BUY_COST)}</b>\n'
            f'{E["chart"]} Доход: <b>{format_chips(FARM_INCOME[1])} / час</b>\n\n'
            f'{E["chip"]} Ваш баланс: <b>{format_chips(bal)}</b>'
        )

    level       = farm["level"]
    name        = FARM_LEVEL_NAMES.get(level, f"Уровень {level}")
    income_hour = FARM_INCOME.get(level, 0)
    upg_cost    = FARM_UPGRADE_COSTS.get(level)

    last = farm["last_collect"]
    if last.tzinfo is None:
        last = last.replace(tzinfo=pytz.utc)
    hours = min((datetime.now(pytz.utc) - last).total_seconds() / 3600, 24)

    text = (
        f'{E["chip"]} <b>Биткоин-ферма</b>\n\n'
        f'⛏ Тип: <b>{name}</b>\n'
        f'{E["chart"]} Уровень: <b>{level} / 10</b>\n'
        f'{E["chip"]} Доход: <b>{format_chips(income_hour)} / час</b>\n'
        f'{E["clock"]} Накоплено за {hours:.1f}ч: <b>{format_chips(pending)}</b>\n'
    )
    if level < 10 and upg_cost:
        text += f'{E["chart"]} Улучшение до ур.{level+1}: <b>{format_chips(upg_cost)}</b>\n'
    text += f'\n{E["chip"]} Ваш баланс: <b>{format_chips(bal)}</b>'
    return text

def _minesweeper_text(amount: int, field: list, revealed: list, mines: int = 5) -> str:
    opened   = sum(cell for row in revealed for cell in row)
    mult     = ms_multiplier(opened, mines)
    potential = int(amount * mult)
    step     = MS_STEP.get(mines, 0.20)
    text = (
        f'{E["mine"]} <b>Сапер</b>\n\n'
        f'{E["chip"]} Ставка: <b>{format_chips(amount)}</b>\n'
        f'{E["mine"]} Мин: <b>{mines}</b>  (+{step} за ячейку)\n'
        f'{E["chart"]} Множитель: <b>x{mult:.2f}</b>\n'
        f'{E["bank"]} Можно забрать: <b>{format_chips(potential)}</b>\n\n'
        f'<b>Открывай ячейки:</b>\n'
    )
    for r, row in enumerate(revealed):
        for c, is_open in enumerate(row):
            text += ("💥" if field[r][c] else "✅") if is_open else "🟫"
        text += "\n"
    return text

def _rocket_text(amount: int, multiplier: float) -> str:
    potential = int(amount * multiplier)
    stars     = "⭐" * min(int(multiplier), 10)
    return (
        f'{E["rocket"]} <b>Ракета летит!</b>\n\n'
        f'{E["chip"]} Ставка: <b>{format_chips(amount)}</b>\n'
        f'{E["chart"]} Множитель: <b>x{multiplier:.2f}</b>  {stars}\n'
        f'{E["bank"]} Можно забрать: <b>{format_chips(potential)}</b>\n\n'
        f'Нажми <b>«Дальше»</b> чтобы лететь выше\n'
        f'или <b>«Забрать»</b> чтобы зафиксировать!'
    )


# ── Rocket helpers ────────────────────────────────────────────────────────────

def generate_crash_point() -> float:
    r = random.random()
    if r < 0.40:   return round(random.uniform(1.0,  1.5), 2)
    if r < 0.65:   return round(random.uniform(1.5,  2.5), 2)
    if r < 0.80:   return round(random.uniform(2.5,  4.0), 2)
    if r < 0.92:   return round(random.uniform(4.0,  8.0), 2)
    return             round(random.uniform(8.0, 20.0),    2)

def next_multiplier(current: float) -> float:
    return round(current + round(random.uniform(0.1, 0.6), 2), 2)


# ── Minesweeper helpers ───────────────────────────────────────────────────────

def generate_minesweeper_field(size: int = 5, mines: int = 5) -> list:
    field  = [[False]*size for _ in range(size)]
    placed = 0
    while placed < mines:
        r, c = random.randint(0, size-1), random.randint(0, size-1)
        if not field[r][c]:
            field[r][c] = True
            placed += 1
    return field


# ── Handlers: start / ping / back ─────────────────────────────────────────────

@dp.message(CommandStart())
async def cmd_start(msg: Message, state: FSMContext):
    try: await msg.delete()
    except Exception: pass
    await delete_old_message(msg.from_user.id)
    bal = get_or_create_user(msg.from_user.id, msg.from_user.username or "игрок")
    await state.clear()
    text = (
        f'{E["slot"]} <b>Добро пожаловать в Казино!</b>\n\n'
        f'{E["chip"]} Ваш баланс: <b>{format_chips(bal)}</b>\n\n'
        f'{E["box"]} Доступные игры:\n'
    )
    resp = await msg.answer(text, parse_mode="HTML", reply_markup=main_menu_kb())
    store_message(msg.from_user.id, resp.message_id)


@dp.message(Command("ping"))
async def cmd_ping(msg: Message):
    try: await msg.delete()
    except Exception: pass
    await delete_old_message(msg.from_user.id)
    t0   = time.perf_counter()
    sent = await msg.answer(f'{E["clock"]} ПОНГ...', parse_mode="HTML")
    store_message(msg.from_user.id, sent.message_id)
    ms = round((time.perf_counter() - t0) * 1000, 2)
    await sent.edit_text(
        f'{E["clock"]} <b>ПОНГ</b>\n\n{E["chart"]} Отклик: <b>{ms} ms</b>',
        parse_mode="HTML",
    )


@dp.callback_query(F.data == "back_main")
async def back_main(cq: CallbackQuery, state: FSMContext):
    await state.clear()
    bal = get_balance(cq.from_user.id)
    await safe_edit_or_send(
        cq,
        f'{E["slot"]} <b>Казино</b>\n{E["chip"]} Баланс: <b>{format_chips(bal)}</b>',
        reply_markup=main_menu_kb(),
    )


@dp.callback_query(F.data == "balance")
async def show_balance(cq: CallbackQuery):
    await cq.answer(f'Ваш баланс: {format_chips(get_balance(cq.from_user.id))}', show_alert=True)


# ── Handlers: stats ───────────────────────────────────────────────────────────

@dp.callback_query(F.data == "stats")
async def show_stats(cq: CallbackQuery):
    row = get_user(cq.from_user.id)
    if not row:
        await cq.answer("Сначала запустите /start", show_alert=True)
        return
    _, username, balance, wins, losses, *_ = row
    total = wins + losses
    rate  = round(wins / total * 100, 1) if total else 0
    text = (
        f'{E["stats2"]} <b>Профиль</b>\n\n'
        f'{E["person"]} Игрок: <b>{username or "Неизвестно"}</b>\n'
        f'{E["chip"]} Баланс: <b>{format_chips(balance)}</b>\n'
        f'{E["ok"]} Побед: <b>{wins}</b>\n'
        f'{E["fail"]} Поражений: <b>{losses}</b>\n'
        f'{E["chart"]} Процент побед: <b>{rate}%</b>'
    )
    await safe_edit_or_send(cq, text, reply_markup=stats_menu_kb())


@dp.callback_query(F.data == "stats_history")
async def show_history(cq: CallbackQuery):
    uid     = cq.from_user.id
    history = get_history(uid, limit=15)

    if not history:
        text = f'{E["chart"]} <b>Нет истории ставок</b>'
    else:
        text = f'{E["chart"]} <b>История ставок (последние 15):</b>\n\n'
        for e in history:
            emoji = E["ok"] if e["is_win"] else E["fail"]
            sign  = "+" if e["is_win"] else "-"
            name  = GAME_NAMES.get(e["game_type"], "Рулетка")
            text += f'{emoji} {sign}{e["amount"]} — {name}\n'

    stats = get_user_history_stats(uid)
    if stats and (stats["win_count"] or stats["lose_count"]):
        won  = stats["total_won"]  or 0
        lost = stats["total_lost"] or 0
        net  = won - lost
        text += f'\n{E["ok"]} Выигрыши: <b>+{won}</b>\n'
        text += f'{E["fail"]} Проигрыши: <b>-{lost}</b>\n'
        text += (
            f'{E["ok"]} Итог: <b>+{net}</b>' if net > 0 else
            f'{E["fail"]} Итог: <b>{net}</b>'  if net < 0 else
            f'{E["chip"]} Итог: <b>0</b>'
        )

    await safe_edit_or_send(cq, text, reply_markup=InlineKeyboardMarkup(inline_keyboard=[
        [_btn("Назад в статистику", "stats", emoji_id="5893057118545646106")],
    ]))


# ── Handlers: reset ───────────────────────────────────────────────────────────

@dp.callback_query(F.data == "reset")
async def reset_handler(cq: CallbackQuery, state: FSMContext):
    reset_balance(cq.from_user.id)
    await state.clear()
    await cq.answer("Баланс сброшен до 1000 фишек!", show_alert=True)
    await safe_edit_or_send(
        cq,
        f'{E["reload"]} <b>Баланс сброшен!</b>\n{E["chip"]} Новый баланс: <b>1000 фишек</b>',
        reply_markup=main_menu_kb(),
    )


# ── Handlers: donate ──────────────────────────────────────────────────────────

@dp.callback_query(F.data == "donate")
async def open_donate(cq: CallbackQuery):
    await safe_edit_or_send(
        cq,
        f'{E["gift"]} <b>Накормить автора</b>\n\nВыберите количество звезд для поддержки разработки\n(звезды идут разработчику, спасибо! 💙)',
        reply_markup=donate_kb(),
    )

async def _send_invoice(chat_id: int, amount: int):
    await bot.send_invoice(
        chat_id=chat_id,
        title="Поддержка разработчика",
        description=f"Спасибо за {amount} ⭐! Это помогает развивать бота 💙",
        payload=f"donate_{amount}",
        currency="XTR",
        prices=[LabeledPrice(label=f"Поддержка ({amount} ⭐)", amount=amount)],
        provider_token="",
    )

@dp.callback_query(F.data.startswith("donate_") & (F.data != "donate_custom"))
async def process_donation(cq: CallbackQuery):
    try:
        amount = int(cq.data.split("_")[1])
    except (ValueError, IndexError):
        await cq.answer("Некорректная сумма", show_alert=True)
        return
    await _send_invoice(cq.from_user.id, amount)

@dp.callback_query(F.data == "donate_custom")
async def ask_custom_donate(cq: CallbackQuery, state: FSMContext):
    await state.set_state(DonateState.entering_custom_amount)
    await safe_edit_or_send(
        cq,
        f'{E["pencil"]} <b>Введите сумму в звёздах (⭐)</b>\n\nМинимум: 1 ⭐, максимум: 10000 ⭐',
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [_btn("Назад", "donate", emoji_id="5893057118545646106")]
        ]),
    )

@dp.message(DonateState.entering_custom_amount)
async def process_custom_donate(msg: Message, state: FSMContext):
    try:
        amount = int(msg.text.strip())
        assert 1 <= amount <= 10000
    except Exception:
        await msg.answer(f'{E["fail"]} Сумма должна быть от 1 до 10000 ⭐', parse_mode="HTML")
        return
    await state.clear()
    await _send_invoice(msg.from_user.id, amount)

@dp.pre_checkout_query()
async def pre_checkout(pcq: PreCheckoutQuery):
    await bot.answer_pre_checkout_query(pcq.id, ok=True)

@dp.message(lambda m: m.successful_payment is not None)
async def successful_payment(msg: Message):
    p = msg.successful_payment
    if not p.invoice_payload.startswith("donate_"):
        return
    amount = int(p.invoice_payload.split("_")[1])
    await msg.answer(
        f'{E["party"]} <b>Спасибо за {amount} звёзд!</b>\n\nВаша поддержка очень важна для развития бота! 💙',
        parse_mode="HTML",
        reply_markup=main_menu_kb(),
    )


# ── Handlers: farm ────────────────────────────────────────────────────────────

@dp.callback_query(F.data == "open_farm")
async def open_farm(cq: CallbackQuery):
    await safe_edit_or_send(cq, farm_text(cq.from_user.id), reply_markup=farm_kb(cq.from_user.id))
    await cq.answer()

@dp.callback_query(F.data == "farm_noop")
async def farm_noop(cq: CallbackQuery):
    await cq.answer("Недостаточно фишек!", show_alert=True)

@dp.callback_query(F.data == "farm_buy")
async def farm_buy_handler(cq: CallbackQuery):
    uid = cq.from_user.id
    if get_balance(uid) < FARM_BUY_COST:
        await cq.answer("Недостаточно фишек!", show_alert=True); return
    if get_farm(uid):
        await cq.answer("У вас уже есть ферма!", show_alert=True); return
    conn = db_connect()
    try:
        with conn.cursor() as cur:
            cur.execute("UPDATE users SET balance = balance - %s WHERE user_id=%s",
                        (FARM_BUY_COST, uid))
        conn.commit()
    finally:
        db_release(conn)
    buy_farm(uid)
    await cq.answer("Ферма куплена! ⛏", show_alert=True)
    await safe_edit_or_send(cq, farm_text(uid), reply_markup=farm_kb(uid))

@dp.callback_query(F.data == "farm_collect")
async def farm_collect_handler(cq: CallbackQuery):
    uid  = cq.from_user.id
    farm = get_farm(uid)
    if not farm:
        await cq.answer("У вас нет фермы!", show_alert=True); return
    pending = get_farm_pending(farm)
    if pending <= 0:
        await cq.answer("Ещё нечего собирать, подождите!", show_alert=True); return
    conn = db_connect()
    try:
        with conn.cursor() as cur:
            cur.execute("UPDATE users SET balance = balance + %s WHERE user_id=%s", (pending, uid))
        conn.commit()
    finally:
        db_release(conn)
    collect_farm(uid)
    add_history(uid, pending, True, "farm")
    await cq.answer(f"Собрано +{fmt(pending)} фишек! ✅", show_alert=True)
    await safe_edit_or_send(cq, farm_text(uid), reply_markup=farm_kb(uid))

@dp.callback_query(F.data == "farm_upgrade")
async def farm_upgrade_handler(cq: CallbackQuery):
    uid  = cq.from_user.id
    farm = get_farm(uid)
    if not farm:
        await cq.answer("У вас нет фермы!", show_alert=True); return
    level = farm["level"]
    if level >= 10:
        await cq.answer("Максимальный уровень!", show_alert=True); return
    cost = FARM_UPGRADE_COSTS.get(level, 0)
    if get_balance(uid) < cost:
        await cq.answer("Недостаточно фишек!", show_alert=True); return
    conn = db_connect()
    try:
        with conn.cursor() as cur:
            cur.execute("UPDATE users SET balance = balance - %s WHERE user_id=%s", (cost, uid))
        conn.commit()
    finally:
        db_release(conn)
    upgrade_farm(uid)
    new_level = level + 1
    await cq.answer(f"Ферма улучшена до ур.{new_level} — {FARM_LEVEL_NAMES.get(new_level)}! 🎉", show_alert=True)
    await safe_edit_or_send(cq, farm_text(uid), reply_markup=farm_kb(uid))


# ── Handlers: roulette ────────────────────────────────────────────────────────

def _roulette_result_text(amount, bet_type, result, won, mult, new_bal):
    color = number_color(result)
    label = BET_LABELS.get(bet_type, bet_type.replace("num_", "число "))
    if won:
        profit  = amount * mult
        outcome = f'{E["party"]} <b>ПОБЕДА!</b>\n{E["profit"]} +{format_chips(profit)} (x{mult})'
    else:
        outcome = f'{E["fail"]} <b>Поражение.</b>\n{E["chip"]} -{format_chips(amount)}'
    text = (
        f'{E["slot"]} <b>Шарик остановился на:</b> {color} <b>{result}</b>\n\n'
        f'🎲 Ваша ставка: <b>{label}</b> — <b>{format_chips(amount)}</b>\n'
        f'{outcome}\n\n'
        f'{E["chip"]} Новый баланс: <b>{format_chips(new_bal)}</b>'
    )
    if new_bal <= 0:
        text += f'\n\n{E["fail"]} <b>Вы банкрот!</b> Нажмите «Сбросить баланс».'
    return text

def _do_roulette(user_id, amount, bet_type):
    result = spin_wheel()
    won    = check_bet(bet_type, result)
    mult   = payout_multiplier(bet_type)
    if won:
        update_balance(user_id, amount * mult, win=True,  game_type="roulette")
    else:
        update_balance(user_id, -amount,       win=False, game_type="roulette")
    return result, won, mult, get_balance(user_id)

@dp.callback_query(F.data == "open_roulette")
async def open_roulette(cq: CallbackQuery, state: FSMContext):
    bal = get_balance(cq.from_user.id)
    if bal <= 0:
        await cq.answer("У вас нет фишек! Сбросьте баланс.", show_alert=True); return
    await state.set_state(BetState.choosing_bet_type)
    await safe_edit_or_send(
        cq,
        f'{E["slot"]} <b>Европейская Рулетка</b>\n{E["chip"]} Баланс: <b>{format_chips(bal)}</b>\n\n<b>Выберите тип ставки:</b>',
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
            f'{E["abc"]} <b>Введите число от 0 до 36:</b>',
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [_btn("Назад", "back_bet_type", emoji_id="5893057118545646106")]
            ]),
        )
        return
    await state.update_data(bet_type=raw)
    await state.set_state(BetState.choosing_amount)
    bal = get_balance(cq.from_user.id)
    await safe_edit_or_send(
        cq,
        f'{E["slot"]} <b>Ставка: {BET_LABELS.get(raw, raw)}</b>\n{E["chip"]} Баланс: <b>{format_chips(bal)}</b>\n\n<b>Выберите сумму ставки:</b>',
        reply_markup=bet_amount_kb(bal),
    )

@dp.callback_query(F.data == "back_bet_type")
async def back_bet_type(cq: CallbackQuery, state: FSMContext):
    await state.set_state(BetState.choosing_bet_type)
    bal = get_balance(cq.from_user.id)
    await safe_edit_or_send(
        cq,
        f'{E["slot"]} <b>Европейская Рулетка</b>\n{E["chip"]} Баланс: <b>{format_chips(bal)}</b>\n\n<b>Выберите тип ставки:</b>',
        reply_markup=bet_type_kb(),
    )

@dp.callback_query(BetState.choosing_amount, F.data == "amount_custom")
async def ask_custom_amount(cq: CallbackQuery, state: FSMContext):
    await state.update_data(waiting_custom=True)
    bal = get_balance(cq.from_user.id)
    await safe_edit_or_send(
        cq,
        f'{E["pencil"]} <b>Введите сумму ставки:</b>\n{E["chip"]} Баланс: <b>{format_chips(bal)}</b>',
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [_btn("Назад", "back_bet_type", emoji_id="5893057118545646106")]
        ]),
    )

@dp.callback_query(BetState.choosing_amount, F.data.startswith("amount_"))
async def place_bet(cq: CallbackQuery, state: FSMContext):
    amount   = int(cq.data.split("_")[1])
    data     = await state.get_data()
    bet_type = data.get("bet_type", "")
    if not bet_type or bet_type == "pending_number":
        await cq.answer("Сначала выберите тип ставки.", show_alert=True); return
    bal = get_balance(cq.from_user.id)
    if amount > bal:
        await cq.answer("Недостаточно средств!", show_alert=True); return
    last_bets[cq.from_user.id] = {"game": "roulette", "bet_type": bet_type, "amount": amount}
    result, won, mult, new_bal = _do_roulette(cq.from_user.id, amount, bet_type)
    await state.clear()
    await safe_edit_or_send(
        cq,
        _roulette_result_text(amount, bet_type, result, won, mult, new_bal),
        reply_markup=game_result_kb("roulette"),
    )

@dp.message(BetState.choosing_amount)
async def handle_number_input(msg: Message, state: FSMContext):
    data = await state.get_data()
    uid  = msg.from_user.id
    bal  = get_balance(uid)

    if data.get("waiting_custom"):
        try:
            amount = int(msg.text.strip())
            assert 1 <= amount <= bal
        except Exception:
            await msg.answer(f"❗ Введите целое число от <b>1</b> до <b>{bal}</b>.", parse_mode="HTML"); return
        bet_type = data.get("bet_type", "")
        await state.update_data(waiting_custom=False)
        last_bets[uid] = {"game": "roulette", "bet_type": bet_type, "amount": amount}
        result, won, mult, new_bal = _do_roulette(uid, amount, bet_type)
        await state.clear()
        await msg.answer(_roulette_result_text(amount, bet_type, result, won, mult, new_bal),
                         parse_mode="HTML", reply_markup=game_result_kb("roulette"))
        return

    if data.get("bet_type") != "pending_number":
        return
    try:
        n = int(msg.text.strip())
        assert 0 <= n <= 36
    except Exception:
        await msg.answer("❗ Введите целое число от <b>0</b> до <b>36</b>.", parse_mode="HTML"); return
    await state.update_data(bet_type=f"num_{n}")
    await msg.answer(
        f'{E["abc"]} <b>Ставка на число {n}</b> (выплата x35)\n{E["chip"]} Баланс: <b>{format_chips(bal)}</b>\n\n<b>Выберите сумму ставки:</b>',
        parse_mode="HTML", reply_markup=bet_amount_kb(bal),
    )

@dp.callback_query(F.data == "repeat_roulette")
async def repeat_roulette(cq: CallbackQuery, state: FSMContext):
    uid = cq.from_user.id
    bet = last_bets.get(uid)
    if not bet or bet.get("game") != "roulette":
        await cq.answer("Нет сохраненной ставки, выберите новую.", show_alert=False); return
    bet_type, amount = bet["bet_type"], bet["amount"]
    if amount > get_balance(uid):
        await cq.answer(f'Недостаточно фишек! Нужно {amount}.', show_alert=True); return
    result, won, mult, new_bal = _do_roulette(uid, amount, bet_type)
    await safe_edit_or_send(cq, _roulette_result_text(amount, bet_type, result, won, mult, new_bal),
                             reply_markup=game_result_kb("roulette"))


# ── Handlers: coin ────────────────────────────────────────────────────────────

def _coin_result_text(amount, coin_choice, result, won, new_bal):
    side_label   = "Орёл" if coin_choice == "heads" else "Решка"
    result_label = "Орёл" if result == "heads"      else "Решка"
    if won:
        outcome = f'{E["party"]} <b>ПОБЕДА!</b>\n{E["profit"]} +{format_chips(amount * 2)} (x2)'
    else:
        outcome = f'{E["fail"]} <b>Поражение.</b>\n{E["chip"]} -{format_chips(amount)}'
    text = (
        f'{E["coin_g"]} <b>Результат:</b> {result_label}\n\n'
        f'🎲 Ваша ставка: <b>{side_label}</b> — <b>{format_chips(amount)}</b>\n'
        f'{outcome}\n\n'
        f'{E["chip"]} Новый баланс: <b>{format_chips(new_bal)}</b>'
    )
    if new_bal <= 0:
        text += f'\n\n{E["fail"]} <b>Вы банкрот!</b> Нажмите «Сбросить баланс».'
    return text

def _do_coin(user_id, amount, coin_choice):
    result = flip_coin()
    won    = coin_choice == result
    if won:
        update_balance(user_id,  amount * 2, win=True,  game_type="coin")
    else:
        update_balance(user_id, -amount,     win=False, game_type="coin")
    return result, won, get_balance(user_id)

@dp.callback_query(F.data == "open_coin")
async def open_coin(cq: CallbackQuery, state: FSMContext):
    bal = get_balance(cq.from_user.id)
    if bal <= 0:
        await cq.answer("У вас нет фишек! Сбросьте баланс.", show_alert=True); return
    await state.set_state(CoinState.choosing_side)
    await safe_edit_or_send(
        cq,
        f'{E["coin_g"]} <b>Орёл или Решка</b>\n{E["chip"]} Баланс: <b>{format_chips(bal)}</b>\n\n<b>Выберите сторону фишки:</b>',
        reply_markup=coin_side_kb(),
    )

@dp.callback_query(CoinState.choosing_side, F.data.startswith("coin_"))
async def choose_coin_side(cq: CallbackQuery, state: FSMContext):
    side = cq.data.split("_")[1]
    await state.update_data(coin_choice=side)
    await state.set_state(CoinState.choosing_amount)
    bal  = get_balance(cq.from_user.id)
    label = "Орёл" if side == "heads" else "Решка"
    await safe_edit_or_send(
        cq,
        f'{E["coin_g"]} <b>Ставка: {label}</b>\n{E["chip"]} Баланс: <b>{format_chips(bal)}</b>\n\n<b>Выберите сумму ставки:</b>',
        reply_markup=bet_amount_kb(bal),
    )

@dp.callback_query(CoinState.choosing_amount, F.data == "amount_custom")
async def ask_custom_coin_amount(cq: CallbackQuery, state: FSMContext):
    await state.update_data(waiting_custom=True)
    bal = get_balance(cq.from_user.id)
    await safe_edit_or_send(
        cq,
        f'{E["pencil"]} <b>Введите сумму ставки:</b>\n{E["chip"]} Баланс: <b>{format_chips(bal)}</b>',
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [_btn("Назад", "back_main", emoji_id="5893057118545646106")]
        ]),
    )

@dp.callback_query(CoinState.choosing_amount, F.data.startswith("amount_"))
async def place_coin_bet(cq: CallbackQuery, state: FSMContext):
    amount = int(cq.data.split("_")[1])
    data   = await state.get_data()
    choice = data.get("coin_choice", "")
    if not choice:
        await cq.answer("Сначала выберите сторону фишки.", show_alert=True); return
    if amount > get_balance(cq.from_user.id):
        await cq.answer("Недостаточно средств!", show_alert=True); return
    last_bets[cq.from_user.id] = {"game": "coin", "choice": choice, "amount": amount}
    result, won, new_bal = _do_coin(cq.from_user.id, amount, choice)
    await state.clear()
    await safe_edit_or_send(cq, _coin_result_text(amount, choice, result, won, new_bal),
                             reply_markup=game_result_kb("coin"))

@dp.message(CoinState.choosing_amount)
async def handle_coin_amount_input(msg: Message, state: FSMContext):
    data   = await state.get_data()
    uid    = msg.from_user.id
    choice = data.get("coin_choice", "")
    if not data.get("waiting_custom"):
        return
    bal = get_balance(uid)
    try:
        amount = int(msg.text.strip())
        assert 1 <= amount <= bal
    except Exception:
        await msg.answer(f"❗ Введите целое число от <b>1</b> до <b>{bal}</b>.", parse_mode="HTML"); return
    last_bets[uid] = {"game": "coin", "choice": choice, "amount": amount}
    result, won, new_bal = _do_coin(uid, amount, choice)
    await state.clear()
    await msg.answer(_coin_result_text(amount, choice, result, won, new_bal),
                     parse_mode="HTML", reply_markup=game_result_kb("coin"))

@dp.callback_query(F.data == "repeat_coin")
async def repeat_coin(cq: CallbackQuery, state: FSMContext):
    uid = cq.from_user.id
    bet = last_bets.get(uid)
    if not bet or bet.get("game") != "coin":
        await cq.answer("Нет сохраненной ставки, выберите новую.", show_alert=False); return
    choice, amount = bet["choice"], bet["amount"]
    if amount > get_balance(uid):
        await cq.answer(f"Недостаточно фишек! Нужно {amount}.", show_alert=True); return
    result, won, new_bal = _do_coin(uid, amount, choice)
    await safe_edit_or_send(cq, _coin_result_text(amount, choice, result, won, new_bal),
                             reply_markup=game_result_kb("coin"))


# ── Handlers: rocket ──────────────────────────────────────────────────────────

async def _start_rocket_game(target, state: FSMContext, user_id: int, amount: int, is_msg=False):
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
        await cq.answer("У вас нет фишек! Сбросьте баланс.", show_alert=True); return
    await state.set_state(RocketState.choosing_amount)
    await safe_edit_or_send(
        cq,
        f'{E["rocket"]} <b>Ракета</b>\n{E["chip"]} Баланс: <b>{format_chips(bal)}</b>\n\n'
        f'Ракета взлетает и множитель растёт.\nНажми <b>«Дальше»</b> чтобы лететь выше,\n'
        f'или <b>«Забрать»</b> чтобы зафиксировать выигрыш.\nЕсли ракета взорвётся — ставка сгорает!\n\n'
        f'<b>Выбери сумму ставки:</b>',
        reply_markup=rocket_amount_kb(bal),
    )

@dp.callback_query(RocketState.choosing_amount, F.data == "rocket_amount_custom")
async def rocket_amount_custom(cq: CallbackQuery, state: FSMContext):
    await state.update_data(waiting_custom=True)
    bal = get_balance(cq.from_user.id)
    await safe_edit_or_send(
        cq,
        f'{E["pencil"]} <b>Введите сумму ставки:</b>\n{E["chip"]} Баланс: <b>{format_chips(bal)}</b>',
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [_btn("Назад", "open_rocket", emoji_id="5893057118545646106")]
        ]),
    )
    await cq.answer()

@dp.callback_query(RocketState.choosing_amount, F.data.startswith("rocket_amount_"))
async def rocket_set_amount(cq: CallbackQuery, state: FSMContext):
    amount = int(cq.data.replace("rocket_amount_", ""))
    if amount > get_balance(cq.from_user.id):
        await cq.answer("Недостаточно средств!", show_alert=True); return
    await _start_rocket_game(cq.message, state, cq.from_user.id, amount)
    await cq.answer()

@dp.message(RocketState.choosing_amount)
async def rocket_custom_amount(msg: Message, state: FSMContext):
    if not (await state.get_data()).get("waiting_custom"):
        return
    bal = get_balance(msg.from_user.id)
    try:
        amount = int(msg.text.strip())
        assert 1 <= amount <= bal
    except Exception:
        await msg.answer(f"❗ Введите целое число от <b>1</b> до <b>{bal}</b>.", parse_mode="HTML"); return
    await _start_rocket_game(msg, state, msg.from_user.id, amount, is_msg=True)

@dp.callback_query(RocketState.in_game, F.data == "rocket_next")
async def rocket_next(cq: CallbackQuery, state: FSMContext):
    uid  = cq.from_user.id
    data = await state.get_data()
    amount, multiplier, crash = (
        data["rocket_amount"], data["rocket_multiplier"], data["rocket_crash"]
    )
    new_mult = next_multiplier(multiplier)
    if new_mult >= crash:
        update_balance(uid, -amount, win=False, game_type="rocket")
        await state.clear()
        text = (
            f'{E["fail"]} <b>РАКЕТА ВЗОРВАЛАСЬ!</b>\n\n'
            f'{E["chart"]} Множитель дошёл до: <b>x{crash:.2f}</b>\n'
            f'{E["chip"]} Ставка: <b>{format_chips(amount)}</b>\n'
            f'{E["fail"]} Вы потеряли: <b>-{format_chips(amount)}</b>\n\n'
            f'{E["chip"]} Новый баланс: <b>{format_chips(get_balance(uid))}</b>'
        )
        await safe_edit_or_send(cq, text, reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [_btn("Сыграть снова", "open_rocket", emoji_id="5373139891223741704")],
            [_btn("В меню",        "back_main",   emoji_id="5893057118545646106")],
        ]))
    else:
        await state.update_data(rocket_multiplier=new_mult)
        try:
            await cq.message.edit_text(_rocket_text(amount, new_mult),
                                        parse_mode="HTML", reply_markup=rocket_game_kb())
        except Exception:
            pass
    await cq.answer()

@dp.callback_query(RocketState.in_game, F.data == "rocket_cashout")
async def rocket_cashout(cq: CallbackQuery, state: FSMContext):
    uid  = cq.from_user.id
    data = await state.get_data()
    amount, multiplier = data["rocket_amount"], data["rocket_multiplier"]
    net = int(amount * multiplier) - amount
    update_balance(uid, net, win=True, game_type="rocket")
    await state.clear()
    await safe_edit_or_send(
        cq,
        f'{E["ok"]} <b>Вы забрали выигрыш!</b>\n\n'
        f'{E["chart"]} Множитель: <b>x{multiplier:.2f}</b>\n'
        f'{E["chip"]} Ставка: <b>{format_chips(amount)}</b>\n'
        f'{E["party"]} Выигрыш: <b>+{format_chips(net)}</b>\n\n'
        f'{E["chip"]} Новый баланс: <b>{format_chips(get_balance(uid))}</b>',
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [_btn("Сыграть снова", "open_rocket", emoji_id="5373139891223741704")],
            [_btn("В меню",        "back_main",   emoji_id="5893057118545646106")],
        ]),
    )
    await cq.answer()


# ── Handlers: minesweeper ─────────────────────────────────────────────────────

@dp.callback_query(F.data == "open_minesweeper")
async def open_minesweeper(cq: CallbackQuery, state: FSMContext):
    bal = get_balance(cq.from_user.id)
    if bal <= 0:
        await cq.answer("У вас нет фишек! Сбросьте баланс.", show_alert=True); return
    await state.set_state(MinesweeperState.choosing_amount)
    await safe_edit_or_send(
        cq,
        f'{E["mine"]} <b>Сапер</b>\n{E["chip"]} Баланс: <b>{format_chips(bal)}</b>\n\n'
        f'Открывай ячейки и не попадись на мину!\n'
        f'Каждая безопасная ячейка увеличивает множитель.\n'
        f'Множитель при кешауте всегда больше 1x!\n\n<b>Выбери сумму ставки:</b>',
        reply_markup=bet_amount_kb(bal),
    )

@dp.callback_query(MinesweeperState.choosing_amount, F.data == "amount_custom")
async def minesweeper_amount_custom(cq: CallbackQuery, state: FSMContext):
    await state.update_data(waiting_custom=True)
    bal = get_balance(cq.from_user.id)
    await safe_edit_or_send(
        cq,
        f'{E["pencil"]} <b>Введите сумму ставки:</b>\n{E["chip"]} Баланс: <b>{format_chips(bal)}</b>',
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [_btn("Назад", "back_main", emoji_id="5893057118545646106")]
        ]),
    )

@dp.callback_query(MinesweeperState.choosing_amount, F.data.startswith("amount_"))
async def minesweeper_set_amount(cq: CallbackQuery, state: FSMContext):
    amount = int(cq.data.replace("amount_", ""))
    if amount > get_balance(cq.from_user.id):
        await cq.answer("Недостаточно средств!", show_alert=True); return
    await state.set_state(MinesweeperState.choosing_mines)
    await state.update_data(minesweeper_amount=amount)
    await safe_edit_or_send(
        cq,
        f'{E["mine"]} <b>Выбери количество мин</b>\n\n'
        f'{E["chip"]} Ставка: <b>{format_chips(amount)}</b>\n\n'
        f'Больше мин → выше рост множителя за ячейку!',
        reply_markup=_minesweeper_mines_kb(),
    )
    await cq.answer()

@dp.message(MinesweeperState.choosing_amount)
async def minesweeper_custom_amount(msg: Message, state: FSMContext):
    if not (await state.get_data()).get("waiting_custom"):
        return
    bal = get_balance(msg.from_user.id)
    try:
        amount = int(msg.text.strip())
        assert 1 <= amount <= bal
    except Exception:
        await msg.answer(f"❗ Введите целое число от <b>1</b> до <b>{bal}</b>.", parse_mode="HTML"); return
    await state.set_state(MinesweeperState.choosing_mines)
    await state.update_data(minesweeper_amount=amount, waiting_custom=False)
    await msg.answer(
        f'{E["mine"]} <b>Выбери количество мин</b>\n\n'
        f'{E["chip"]} Ставка: <b>{format_chips(amount)}</b>\n\n'
        f'Больше мин → выше рост множителя за ячейку!',
        parse_mode="HTML",
        reply_markup=_minesweeper_mines_kb(),
    )

@dp.callback_query(MinesweeperState.choosing_mines, F.data.startswith("ms_mines_"))
async def minesweeper_start_game(cq: CallbackQuery, state: FSMContext):
    mines  = int(cq.data.split("_")[-1])
    data   = await state.get_data()
    amount = data["minesweeper_amount"]
    field    = generate_minesweeper_field(5, mines)
    revealed = [[False]*5 for _ in range(5)]
    await state.set_state(MinesweeperState.in_game)
    await state.update_data(minesweeper_field=field, minesweeper_revealed=revealed,
                             minesweeper_mines=mines)
    await safe_edit_or_send(cq, _minesweeper_text(amount, field, revealed, mines),
                             reply_markup=_minesweeper_kb(revealed))
    await cq.answer()

@dp.callback_query(MinesweeperState.in_game, F.data.startswith("ms_cell_"))
async def minesweeper_open_cell(cq: CallbackQuery, state: FSMContext):
    uid    = cq.from_user.id
    parts  = cq.data.split("_")
    row, col = int(parts[2]), int(parts[3])
    data   = await state.get_data()
    field    = data["minesweeper_field"]
    revealed = data["minesweeper_revealed"]
    amount   = data["minesweeper_amount"]
    mines    = data["minesweeper_mines"]

    if revealed[row][col]:
        await cq.answer("Эта ячейка уже открыта!"); return

    revealed[row][col] = True

    if field[row][col]:  # мина!
        update_balance(uid, -amount, win=False, game_type="minesweeper")
        await state.clear()
        await safe_edit_or_send(
            cq,
            f'{E["fail"]} <b>МИНА!</b>\n\n'
            f'{E["chip"]} Ставка: <b>{format_chips(amount)}</b>\n'
            f'{E["fail"]} Вы потеряли: <b>-{format_chips(amount)}</b>\n\n'
            f'{E["chip"]} Новый баланс: <b>{format_chips(get_balance(uid))}</b>',
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [_btn("Сыграть снова", "open_minesweeper", emoji_id="5373141891321699086")],
                [_btn("В меню",        "back_main",        emoji_id="5893057118545646106")],
            ]),
        )
    else:
        await state.update_data(minesweeper_revealed=revealed)
        try:
            await cq.message.edit_text(
                _minesweeper_text(amount, field, revealed, mines),
                parse_mode="HTML", reply_markup=_minesweeper_kb(revealed),
            )
        except Exception:
            pass
    await cq.answer()

@dp.callback_query(MinesweeperState.in_game, F.data == "ms_cashout")
async def minesweeper_cashout(cq: CallbackQuery, state: FSMContext):
    uid  = cq.from_user.id
    data = await state.get_data()
    amount   = data["minesweeper_amount"]
    revealed = data["minesweeper_revealed"]
    mines    = data["minesweeper_mines"]
    opened   = sum(cell for row in revealed for cell in row)

    if opened == 0:
        await cq.answer("Сначала откройте хотя бы одну ячейку!", show_alert=True); return

    mult   = ms_multiplier(opened, mines)
    net    = int(amount * mult) - amount
    update_balance(uid, net, win=True, game_type="minesweeper")
    await state.clear()
    await safe_edit_or_send(
        cq,
        f'{E["ok"]} <b>Вы забрали выигрыш!</b>\n\n'
        f'{E["ok"]} Открыто ячеек: <b>{opened}</b>\n'
        f'{E["chart"]} Множитель: <b>x{mult:.2f}</b>\n'
        f'{E["chip"]} Ставка: <b>{format_chips(amount)}</b>\n'
        f'{E["party"]} Выигрыш: <b>+{format_chips(net)}</b>\n\n'
        f'{E["chip"]} Новый баланс: <b>{format_chips(get_balance(uid))}</b>',
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [_btn("Сыграть снова", "open_minesweeper", emoji_id="5373141891321699086")],
            [_btn("В меню",        "back_main",        emoji_id="5893057118545646106")],
        ]),
    )
    await cq.answer()


# ── Handlers: admin ───────────────────────────────────────────────────────────

def _admin_check(user_id: int) -> bool:
    return user_id == ADMIN_ID

@dp.message(Command("admin"))
async def cmd_admin(msg: Message, state: FSMContext):
    if not _admin_check(msg.from_user.id):
        await msg.answer(f'{E["fail"]} У вас нет доступа к админ-панели', parse_mode="HTML"); return
    try: await msg.delete()
    except Exception: pass
    await delete_old_message(msg.from_user.id)
    await state.set_state(AdminState.choosing_action)
    resp = await msg.answer(
        f'{E["gear"]} <b>Админ-панель</b>\n\nВыберите действие:',
        parse_mode="HTML", reply_markup=admin_menu_kb(),
    )
    store_message(msg.from_user.id, resp.message_id)

@dp.callback_query(F.data == "admin_back")
async def admin_back(cq: CallbackQuery, state: FSMContext):
    if not _admin_check(cq.from_user.id): return
    await state.clear()
    await state.set_state(AdminState.choosing_action)
    await safe_edit_or_send(cq, f'{E["gear"]} <b>Админ-панель</b>\n\nВыберите действие:',
                             reply_markup=admin_menu_kb())

@dp.callback_query(F.data == "admin_users")
async def show_users_list(cq: CallbackQuery, state: FSMContext):
    if not _admin_check(cq.from_user.id):
        await cq.answer("Только админ", show_alert=True); return
    await state.set_state(AdminState.choosing_user)
    users = get_all_users_full()
    await safe_edit_or_send(
        cq,
        f'{E["people"]} <b>Список пользователей:</b>\n\nВсего: {len(users)}\n\nВыберите пользователя:',
        reply_markup=users_list_kb(users, 0),
    )

@dp.callback_query(F.data.startswith("admin_users_page_"))
async def paginate_users(cq: CallbackQuery):
    if not _admin_check(cq.from_user.id): return
    page  = int(cq.data.split("_")[-1])
    users = get_all_users_full()
    await safe_edit_or_send(
        cq,
        f'{E["people"]} <b>Список пользователей:</b>\n\nВсего: {len(users)}\n\nВыберите пользователя:',
        reply_markup=users_list_kb(users, page),
    )

@dp.callback_query(F.data.startswith("admin_edit_user_"))
async def edit_user_menu(cq: CallbackQuery, state: FSMContext):
    if not _admin_check(cq.from_user.id): return
    uid = int(cq.data.split("_")[-1])
    row = get_user(uid)
    if not row:
        await cq.answer("Пользователь не найден", show_alert=True); return
    _, username, balance, wins, losses, *_ = row
    await state.update_data(admin_user_id=uid)
    await safe_edit_or_send(
        cq,
        f'{E["person"]} <b>Профиль пользователя:</b>\n\n'
        f'ID: <code>{uid}</code>\nИмя: <b>{username or "Неизвестно"}</b>\n'
        f'{E["chip"]} Баланс: <b>{format_chips(balance)}</b>\n'
        f'{E["ok"]} Побед: <b>{wins}</b>\n{E["fail"]} Поражений: <b>{losses}</b>\n\nВыберите действие:',
        reply_markup=edit_user_kb(uid),
    )

@dp.callback_query(F.data.startswith("admin_user_history_"))
async def admin_user_history(cq: CallbackQuery):
    if not _admin_check(cq.from_user.id): return
    uid     = int(cq.data.split("_")[-1])
    row     = get_user(uid)
    if not row:
        await cq.answer("Пользователь не найден", show_alert=True); return
    username = row[1]
    history  = get_history(uid, 15)
    name_str = username or f"ID {uid}"

    if not history:
        text = f'{E["chart"]} <b>История ставок {name_str}:</b>\n\nНет истории'
    else:
        text = f'{E["chart"]} <b>История ставок {name_str} (последние 15):</b>\n\n'
        for e in history:
            emoji = E["ok"] if e["is_win"] else E["fail"]
            sign  = "+" if e["is_win"] else "-"
            name  = GAME_NAMES.get(e["game_type"], "Рулетка")
            text += f'{emoji} {sign}{e["amount"]} — {name}\n'

    stats = get_user_history_stats(uid)
    if stats and (stats["win_count"] or stats["lose_count"]):
        won  = stats["total_won"]  or 0
        lost = stats["total_lost"] or 0
        net  = won - lost
        text += f'\n{E["ok"]} Выигрыши: <b>+{won}</b>\n{E["fail"]} Проигрыши: <b>-{lost}</b>\n'
        text += (f'{E["ok"]} Итог: <b>+{net}</b>' if net > 0 else
                 f'{E["fail"]} Итог: <b>{net}</b>' if net < 0 else
                 f'{E["chip"]} Итог: <b>0</b>')

    await safe_edit_or_send(cq, text, reply_markup=InlineKeyboardMarkup(inline_keyboard=[
        [_btn("Назад к пользователю", f"admin_edit_user_{uid}", emoji_id="5893057118545646106")],
    ]))

@dp.callback_query(F.data.startswith("admin_edit_balance_"))
async def ask_new_balance(cq: CallbackQuery, state: FSMContext):
    if not _admin_check(cq.from_user.id): return
    uid = int(cq.data.split("_")[-1])
    await state.update_data(admin_user_id=uid)
    await state.set_state(AdminState.editing_balance)
    await safe_edit_or_send(
        cq,
        f'{E["chip"]} Текущий баланс: <b>{format_chips(get_balance(uid))}</b>\n\nВведите сумму для добавления:',
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [_btn("Назад", "admin_users_back", emoji_id="5893057118545646106")]
        ]),
    )

@dp.message(AdminState.editing_balance)
async def process_new_balance(msg: Message, state: FSMContext):
    if not _admin_check(msg.from_user.id): return
    try: await msg.delete()
    except Exception: pass
    try:
        add_amount = int(msg.text.strip())
        assert add_amount >= 0
    except Exception:
        await msg.answer(f'{E["fail"]} Введите корректное число (≥ 0)', parse_mode="HTML"); return

    data    = await state.get_data()
    uid     = data["admin_user_id"]
    old_bal = get_balance(uid)
    new_bal = old_bal + add_amount
    set_balance(uid, new_bal)
    add_history(uid, add_amount, True, "admin_topup")

    user = get_user(uid)
    uname = user[1] if user else "Неизвестно"
    await state.clear()
    await msg.answer(
        f'{E["ok"]} <b>Баланс обновлён!</b>\n\n'
        f'{E["person"]} Пользователь: <b>{uname}</b> (ID: {uid})\n'
        f'{E["chip"]} Было: <b>{format_chips(old_bal)}</b>\n'
        f'{E["profit"]} Добавлено: <b>+{format_chips(add_amount)}</b>\n'
        f'{E["ok"]} Итого: <b>{format_chips(new_bal)}</b>',
        parse_mode="HTML", reply_markup=admin_menu_kb(),
    )
    try:
        await bot.send_message(
            uid,
            f'{E["chip"]} <b>Баланс пополнен!</b>\n\n'
            f'{E["profit"]} Добавлено: <b>+{format_chips(add_amount)}</b>\n'
            f'{E["ok"]} Текущий баланс: <b>{format_chips(new_bal)}</b>',
            parse_mode="HTML",
        )
    except Exception:
        pass

@dp.callback_query(F.data == "admin_broadcast")
async def broadcast_menu(cq: CallbackQuery, state: FSMContext):
    if not _admin_check(cq.from_user.id):
        await cq.answer("Только админ", show_alert=True); return
    users = get_all_users_full()
    await state.set_state(AdminState.sending_broadcast)
    await safe_edit_or_send(
        cq,
        f'{E["horn"]} <b>Рассылка сообщений</b>\n\nАдресатов: {len(users)}\n\nВведите сообщение:\n\n<i>Поддерживает HTML</i>',
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [_btn("Отмена", "admin_back", emoji_id="5870657884844462243")]
        ]),
    )

@dp.message(AdminState.sending_broadcast)
async def process_broadcast(msg: Message, state: FSMContext):
    if not _admin_check(msg.from_user.id): return
    text = msg.text
    try: await msg.delete()
    except Exception: pass
    ok = fail = 0
    for u in get_all_users_full():
        try:
            await bot.send_message(u["user_id"], text, parse_mode="HTML")
            ok += 1
        except Exception:
            fail += 1
    await state.clear()
    await msg.answer(
        f'{E["ok"]} <b>Рассылка завершена!</b>\n\nУспешно: <b>{ok}</b>\nОшибок: <b>{fail}</b>',
        parse_mode="HTML", reply_markup=admin_menu_kb(),
    )

@dp.callback_query(F.data == "admin_clear_history")
async def admin_clear_history(cq: CallbackQuery):
    if not _admin_check(cq.from_user.id): return
    clear_all_history()
    await safe_edit_or_send(
        cq,
        f'{E["ok"]} <b>История ставок очищена!</b>',
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [_btn("Назад в админ-панель", "admin_back", emoji_id="5893057118545646106")]
        ]),
    )
    await cq.answer("История очищена", show_alert=True)

@dp.callback_query(F.data == "admin_users_back")
async def admin_back_to_users(cq: CallbackQuery, state: FSMContext):
    if not _admin_check(cq.from_user.id): return
    await state.set_state(AdminState.choosing_user)
    users = get_all_users_full()
    await safe_edit_or_send(
        cq,
        f'{E["people"]} <b>Список пользователей:</b>\n\nВсего: {len(users)}\n\nВыберите пользователя:',
        reply_markup=users_list_kb(users, 0),
    )


# ── Background: daily bonus ───────────────────────────────────────────────────

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
            bal = get_balance(uid)
            try:
                await bot.send_message(
                    uid,
                    f'{E["gift"]} <b>Ежедневный бонус!</b>\n\n'
                    f'{E["profit"]} Вам начислено <b>+500 фишек</b>\n'
                    f'{E["chip"]} Текущий баланс: <b>{format_chips(bal)}</b>\n\n<i>Удачной игры!</i>',
                    parse_mode="HTML",
                )
            except Exception:
                pass
        await asyncio.sleep(60)


# ── Main ──────────────────────────────────────────────────────────────────────

async def main():
    init_db()
    print("🎰 Casino bot started!")
    await cleanup_old_messages()
    asyncio.create_task(daily_bonus_task())
    try:
        await dp.start_polling(bot)
    finally:
        global db_pool
        if db_pool is not None:
            db_pool.closeall()
            print("🗄️ Database connection pool closed")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("🛑 Бот остановлен.")