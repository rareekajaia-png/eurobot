import asyncio
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
    LabeledPrice, PreCheckoutQuery, ReplyKeyboardMarkup, KeyboardButton
)
from aiogram.filters import CommandStart, Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage

load_dotenv()
TOKEN = os.getenv("BOT_TOKEN")
DATABASE_URL = os.getenv("DATABASE_URL")
STARTING_BALANCE = 1000

bot = Bot(token=TOKEN)
dp = Dispatcher(storage=MemoryStorage())

db_pool = None

def get_db_pool():
    global db_pool
    if db_pool is None:
        db_pool = pool.SimpleConnectionPool(1, 10, DATABASE_URL)
    return db_pool

last_bets = {}
last_messages = {}


# ── Reply-клавиатура (нижняя панель) ─────────────────────────────────────────

def main_reply_kb() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [
                KeyboardButton(
                    text="🎰 Казино",
                    icon_custom_emoji_id="5258882890059091157"
                ),
                KeyboardButton(
                    text="👤 Профиль",
                    icon_custom_emoji_id="5870994129244131212"
                ),
            ],
            [
                KeyboardButton(
                    text="🪙 Баланс",
                    icon_custom_emoji_id="5904462880941545555"
                ),
                KeyboardButton(
                    text="🔄 Сбросить баланс",
                    icon_custom_emoji_id="5345906554510012647"
                ),
            ],
            [
                KeyboardButton(
                    text="⛏ Ферма",
                    icon_custom_emoji_id="5904462880941545555"
                ),
                KeyboardButton(
                    text="📊 История",
                    icon_custom_emoji_id="5870930636742595124"
                ),
            ],
        ],
        resize_keyboard=True,
        is_persistent=True,
    )


async def safe_edit_or_send(cq: CallbackQuery, text: str, reply_markup=None):
    """Пробует edit_text, при ошибке отправляет новое сообщение."""
    try:
        await cq.message.edit_text(text, parse_mode="HTML", reply_markup=reply_markup)
    except Exception:
        try:
            await cq.message.delete()
        except:
            pass
        await cq.message.answer(text, parse_mode="HTML", reply_markup=reply_markup)


def db_connect():
    return get_db_pool().getconn()

def db_release(conn):
    get_db_pool().putconn(conn)

def init_db():
    conn = db_connect()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS users (
                    user_id         BIGINT PRIMARY KEY,
                    username        TEXT,
                    balance         INTEGER DEFAULT 1000,
                    wins            INTEGER DEFAULT 0,
                    losses          INTEGER DEFAULT 0,
                    last_message_id INTEGER
                )
            """)
        conn.commit()
    except:
        conn.rollback()
    finally:
        db_release(conn)

    conn = db_connect()
    try:
        with conn.cursor() as cur:
            cur.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS last_message_id INTEGER")
        conn.commit()
    except:
        conn.rollback()
    finally:
        db_release(conn)

    conn = db_connect()
    try:
        with conn.cursor() as cur:
            cur.execute("""
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
            cur.execute("CREATE INDEX IF NOT EXISTS idx_history_user_id ON history(user_id)")
        conn.commit()
    except:
        conn.rollback()
    finally:
        db_release(conn)

    conn = db_connect()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS farms (
                    user_id      BIGINT PRIMARY KEY,
                    level        INTEGER DEFAULT 1,
                    last_collect TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (user_id) REFERENCES users(user_id)
                )
            """)
        conn.commit()
    except:
        conn.rollback()
    finally:
        db_release(conn)


# ── DB helpers ──────────────────────────────────────────────────────────────

def get_user(user_id: int):
    conn = db_connect()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM users WHERE user_id=%s", (user_id,))
            return cur.fetchone()
    finally:
        db_release(conn)

def get_or_create_user(user_id: int, username: str):
    conn = db_connect()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """INSERT INTO users (user_id, username, balance) VALUES (%s,%s,%s)
                   ON CONFLICT (user_id) DO UPDATE SET username = EXCLUDED.username
                   RETURNING balance""",
                (user_id, username, STARTING_BALANCE)
            )
            result = cur.fetchone()
            conn.commit()
            return result[0] if result else STARTING_BALANCE
    finally:
        db_release(conn)

def update_balance(user_id: int, delta: int, win: bool, game_type: str = "roulette"):
    col = "wins" if win else "losses"
    conn = db_connect()
    try:
        with conn.cursor() as cur:
            cur.execute(
                f"UPDATE users SET balance = balance + %s, {col} = {col} + 1 WHERE user_id=%s",
                (delta, user_id)
            )
        conn.commit()
    finally:
        db_release(conn)
    add_history(user_id, abs(delta), win, game_type)

def get_balance(user_id: int) -> int:
    conn = db_connect()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT balance FROM users WHERE user_id=%s", (user_id,))
            row = cur.fetchone()
        return row[0] if row else 0
    finally:
        db_release(conn)

def get_all_users():
    conn = db_connect()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT user_id FROM users")
            return [r[0] for r in cur.fetchall()]
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

def reset_balance(user_id: int):
    conn = db_connect()
    try:
        with conn.cursor() as cur:
            cur.execute("UPDATE users SET balance = %s WHERE user_id=%s", (STARTING_BALANCE, user_id))
        conn.commit()
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

def set_balance(user_id: int, amount: int):
    conn = db_connect()
    try:
        with conn.cursor() as cur:
            cur.execute("UPDATE users SET balance = %s WHERE user_id=%s", (amount, user_id))
        conn.commit()
    finally:
        db_release(conn)

def add_history(user_id: int, amount: int, is_win: bool, game_type: str = "roulette"):
    conn = db_connect()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO history (user_id, amount, is_win, game_type) VALUES (%s, %s, %s, %s)",
                (user_id, amount, is_win, game_type)
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
                (user_id, limit)
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
                   SUM(CASE WHEN is_win THEN amount ELSE 0 END) as total_won,
                   SUM(CASE WHEN NOT is_win THEN amount ELSE 0 END) as total_lost,
                   COUNT(CASE WHEN is_win THEN 1 END) as win_count,
                   COUNT(CASE WHEN NOT is_win THEN 1 END) as lose_count
                   FROM history WHERE user_id=%s AND game_type != 'admin_topup'""",
                (user_id,)
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


# ── Farm DB ──────────────────────────────────────────────────────────────────

FARM_BUY_COST = 5000
FARM_UPGRADE_COSTS = {
    1: 3000, 2: 8000, 3: 20000, 4: 50000,
    5: 120000, 6: 300000, 7: 700000, 8: 1500000, 9: 3000000
}
FARM_INCOME = {
    1: 200, 2: 500, 3: 1200, 4: 3000, 5: 7000,
    6: 15000, 7: 35000, 8: 80000, 9: 180000, 10: 400000
}
FARM_LEVEL_NAMES = {
    1: "Малинка", 2: "Ноутбук", 3: "Десктоп", 4: "Стойка",
    5: "Мини-ферма", 6: "Ферма", 7: "Датацентр", 8: "Мега-ферма",
    9: "Гиперцентр", 10: "Квантовая ферма"
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
                "INSERT INTO farms (user_id, level, last_collect) VALUES (%s, 1, CURRENT_TIMESTAMP) ON CONFLICT DO NOTHING",
                (user_id,)
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
                "UPDATE farms SET last_collect = CURRENT_TIMESTAMP WHERE user_id=%s",
                (user_id,)
            )
        conn.commit()
    finally:
        db_release(conn)

def get_farm_pending(farm) -> int:
    if not farm:
        return 0
    last = farm['last_collect']
    if last.tzinfo is None:
        last = last.replace(tzinfo=pytz.utc)
    now = datetime.now(pytz.utc)
    hours = (now - last).total_seconds() / 3600
    hours = min(hours, 24)
    income_per_hour = FARM_INCOME.get(farm['level'], 0)
    return int(hours * income_per_hour)


# ── States ───────────────────────────────────────────────────────────────────

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

def noun_form(count, singular, genitive_2_4, genitive_5plus):
    count = int(count) % 100
    if count % 10 == 1 and count != 11:
        return singular
    elif count % 10 in (2, 3, 4) and count not in (12, 13, 14):
        return genitive_2_4
    else:
        return genitive_5plus

def format_chips(amount: int) -> str:
    if amount >= 1_000_000:
        millions = amount / 1_000_000
        if millions == int(millions):
            formatted = f"{int(millions)}кк"
        else:
            formatted = f"{millions:.1f}".rstrip('0').rstrip('.') + "кк"
        return f"{formatted} фишек"
    elif amount >= 1_000:
        num = amount / 1_000
        if num == int(num):
            formatted = f"{int(num)}к"
            base_num = int(num)
        else:
            formatted = f"{num:.1f}".rstrip('0').rstrip('.') + "к"
            base_num = round(num, 1)
    else:
        formatted = str(amount)
        base_num = amount

    return f"{formatted} {noun_form(base_num, 'фишка', 'фишки', 'фишек')}"

def fmt(n: int) -> str:
    if n >= 1_000_000:
        val = n / 1_000_000
        if val == int(val):
            return f"{int(val)}кк"
        return f"{val:.1f}".rstrip('0').rstrip('.') + "кк"
    elif n >= 1_000:
        val = n / 1_000
        if val == int(val):
            return f"{int(val)}к"
        return f"{val:.1f}".rstrip('0').rstrip('.') + "к"
    return str(n)

def spin_wheel() -> int:
    return random.randint(0, 36)

def number_color(n: int) -> str:
    if n == 0: return "🟢"
    if n in RED_NUMBERS: return "🔴"
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

def check_coin_bet(choice: str, result: str) -> bool:
    return choice == result

BET_LABELS = {
    "red":    "Красное",
    "black":  "Чёрное",
    "even":   "ЧЁТНОЕ",
    "odd":    "НЕЧЁТНОЕ",
    "1-18":   "1–18",
    "19-36":  "19–36",
    "1st12":  "1ST 12",
    "2nd12":  "2ND 12",
    "3rd12":  "3RD 12",
    "2to1_1": "2to1 (ряд 1)",
    "2to1_2": "2to1 (ряд 2)",
    "2to1_3": "2to1 (ряд 3)",
}

ADMIN_ID = int(os.getenv("ADMIN_ID", "0"))


# ── Inline keyboards ─────────────────────────────────────────────────────────

def main_menu_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="Рулетка", callback_data="open_roulette",
                                 icon_custom_emoji_id="5258882890059091157"),
            InlineKeyboardButton(text="Орёл или Решка", callback_data="open_coin",
                                 icon_custom_emoji_id="5774585885154131652"),
        ],
        [
            InlineKeyboardButton(text="Ракета", callback_data="open_rocket",
                                 icon_custom_emoji_id="5373139891223741704"),
            InlineKeyboardButton(text="Сапер",  callback_data="open_minesweeper",
                                 icon_custom_emoji_id="5373141891321699086"),
        ],
        [
            InlineKeyboardButton(text="Биткоин-ферма", callback_data="open_farm",
                                 icon_custom_emoji_id="5904462880941545555"),
        ],
        [
            InlineKeyboardButton(text="Профиль", callback_data="stats",
                                 icon_custom_emoji_id="5870921681735781843"),
            InlineKeyboardButton(text="Сбросить баланс", callback_data="reset",
                                 icon_custom_emoji_id="5345906554510012647"),
        ],
    ])

def farm_kb(user_id: int):
    farm = get_farm(user_id)
    bal  = get_balance(user_id)

    if not farm:
        can_buy = bal >= FARM_BUY_COST
        buttons = []
        if can_buy:
            buttons.append([InlineKeyboardButton(
                text=f"Купить ферму ({fmt(FARM_BUY_COST)})",
                callback_data="farm_buy",
                icon_custom_emoji_id="5904462880941545555"
            )])
        else:
            buttons.append([InlineKeyboardButton(
                text=f"Нужно {fmt(FARM_BUY_COST)} фишек",
                callback_data="farm_noop"
            )])
        buttons.append([InlineKeyboardButton(
            text="Назад", callback_data="back_main",
            icon_custom_emoji_id="5893057118545646106"
        )])
        return InlineKeyboardMarkup(inline_keyboard=buttons)

    level   = farm['level']
    pending = get_farm_pending(farm)
    buttons = []

    if pending > 0:
        buttons.append([InlineKeyboardButton(
            text=f"Собрать +{fmt(pending)}",
            callback_data="farm_collect",
            icon_custom_emoji_id="5870633910337015697"
        )])

    if level < 10:
        upgrade_cost = FARM_UPGRADE_COSTS.get(level, 0)
        can_upgrade  = bal >= upgrade_cost
        upgrade_text = f"Улучшить до ур.{level+1} ({fmt(upgrade_cost)})"
        if can_upgrade:
            buttons.append([InlineKeyboardButton(
                text=upgrade_text, callback_data="farm_upgrade",
                icon_custom_emoji_id="5870930636742595124"
            )])
        else:
            buttons.append([InlineKeyboardButton(
                text=f"Нужно {fmt(upgrade_cost)} для улучшения",
                callback_data="farm_noop"
            )])

    buttons.append([InlineKeyboardButton(
        text="Назад", callback_data="back_main",
        icon_custom_emoji_id="5893057118545646106"
    )])
    return InlineKeyboardMarkup(inline_keyboard=buttons)

def rocket_game_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="Дальше",  callback_data="rocket_next",
                                 icon_custom_emoji_id="5373139891223741704"),
            InlineKeyboardButton(text="Забрать", callback_data="rocket_cashout",
                                 icon_custom_emoji_id="5904462880941545555"),
        ],
    ])

def rocket_amount_kb(balance: int):
    q1 = max(1, round(balance * 0.25))
    q2 = max(1, round(balance * 0.50))
    q3 = max(1, round(balance * 0.75))
    q4 = balance
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text=f"25%  ({fmt(q1)})", callback_data=f"rocket_amount_{q1}"),
            InlineKeyboardButton(text=f"75%  ({fmt(q3)})", callback_data=f"rocket_amount_{q3}"),
        ],
        [
            InlineKeyboardButton(text=f"50%  ({fmt(q2)})", callback_data=f"rocket_amount_{q2}"),
            InlineKeyboardButton(text=f"Ва-банк ({fmt(q4)})", callback_data=f"rocket_amount_{q4}",
                                 icon_custom_emoji_id="6041731551845159060"),
        ],
        [InlineKeyboardButton(text="Ввести вручную", callback_data="rocket_amount_custom",
                              icon_custom_emoji_id="5870676941614354370")],
        [InlineKeyboardButton(text="Назад", callback_data="back_main",
                              icon_custom_emoji_id="5893057118545646106")],
    ])

def bet_type_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="Красное", callback_data="bet_red",
                                 icon_custom_emoji_id="5870657884844462243"),
            InlineKeyboardButton(text="Чёрное",  callback_data="bet_black",
                                 icon_custom_emoji_id="5870657884844462243"),
        ],
        [
            InlineKeyboardButton(text="1-18",  callback_data="bet_1-18"),
            InlineKeyboardButton(text="19-36", callback_data="bet_19-36"),
        ],
        [
            InlineKeyboardButton(text="ЧЁТНОЕ",   callback_data="bet_even"),
            InlineKeyboardButton(text="НЕЧЁТНОЕ", callback_data="bet_odd"),
        ],
        [
            InlineKeyboardButton(text="1ST 12", callback_data="bet_1st12"),
            InlineKeyboardButton(text="2ND 12", callback_data="bet_2nd12"),
            InlineKeyboardButton(text="3RD 12", callback_data="bet_3rd12"),
        ],
        [
            InlineKeyboardButton(text="2to1 (ряд 1)", callback_data="bet_2to1_1"),
            InlineKeyboardButton(text="2to1 (ряд 2)", callback_data="bet_2to1_2"),
            InlineKeyboardButton(text="2to1 (ряд 3)", callback_data="bet_2to1_3"),
        ],
        [InlineKeyboardButton(text="Конкретное число (x35)", callback_data="bet_number",
                              icon_custom_emoji_id="5771851822897566479")],
        [InlineKeyboardButton(text="Назад", callback_data="back_main",
                              icon_custom_emoji_id="5893057118545646106")],
    ])

def coin_side_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="Орёл", callback_data="coin_heads",
                                 icon_custom_emoji_id="5774585885154131652"),
            InlineKeyboardButton(text="Решка", callback_data="coin_tails",
                                 icon_custom_emoji_id="5904462880941545555"),
        ],
        [InlineKeyboardButton(text="Назад", callback_data="back_main",
                              icon_custom_emoji_id="5893057118545646106")],
    ])

def bet_amount_kb(balance: int):
    q1 = max(1, round(balance * 0.25))
    q2 = max(1, round(balance * 0.50))
    q3 = max(1, round(balance * 0.75))
    q4 = balance
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text=f"25%  ({fmt(q1)})", callback_data=f"amount_{q1}"),
            InlineKeyboardButton(text=f"75%  ({fmt(q3)})", callback_data=f"amount_{q3}"),
        ],
        [
            InlineKeyboardButton(text=f"50%  ({fmt(q2)})", callback_data=f"amount_{q2}"),
            InlineKeyboardButton(text=f"Ва-банк ({fmt(q4)})", callback_data=f"amount_{q4}",
                                 icon_custom_emoji_id="6041731551845159060"),
        ],
        [InlineKeyboardButton(text="Ввести вручную", callback_data="amount_custom",
                              icon_custom_emoji_id="5870676941614354370")],
        [InlineKeyboardButton(text="Назад", callback_data="back_bet_type",
                              icon_custom_emoji_id="5893057118545646106")],
    ])

def admin_menu_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Список пользователей", callback_data="admin_users",
                              icon_custom_emoji_id="5870772616305839506")],
        [InlineKeyboardButton(text="Рассылка сообщения",  callback_data="admin_broadcast",
                              icon_custom_emoji_id="6039422865189638057")],
        [InlineKeyboardButton(text="Очистить историю",    callback_data="admin_clear_history",
                              icon_custom_emoji_id="5870657884844462243")],
        [InlineKeyboardButton(text="Вернуться в меню",    callback_data="back_main",
                              icon_custom_emoji_id="5893057118545646106")],
    ])

def stats_menu_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="История ставок", callback_data="stats_history",
                              icon_custom_emoji_id="5870930636742595124")],
        [InlineKeyboardButton(text="Накормить автора", callback_data="donate",
                              icon_custom_emoji_id="5904462880941545555")],
        [InlineKeyboardButton(text="Назад в меню", callback_data="back_main",
                              icon_custom_emoji_id="5893057118545646106")],
    ])

def game_result_kb(game_type: str):
    repeat_data = "repeat_coin" if game_type == "coin" else "repeat_roulette"
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Повторить", callback_data=repeat_data,
                              icon_custom_emoji_id="5345906554510012647")],
        [InlineKeyboardButton(text="В меню", callback_data="back_main",
                              icon_custom_emoji_id="5893057118545646106")],
    ])

def donate_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="50 звёзд",   callback_data="donate_50",
                              icon_custom_emoji_id="5870982283724328568")],
        [InlineKeyboardButton(text="100 звёзд",  callback_data="donate_100",
                              icon_custom_emoji_id="5870982283724328568")],
        [InlineKeyboardButton(text="250 звёзд",  callback_data="donate_250",
                              icon_custom_emoji_id="5870982283724328568")],
        [InlineKeyboardButton(text="500 звёзд",  callback_data="donate_500",
                              icon_custom_emoji_id="5870982283724328568")],
        [InlineKeyboardButton(text="1000 звёзд", callback_data="donate_1000",
                              icon_custom_emoji_id="5870982283724328568")],
        [InlineKeyboardButton(text="Своя сумма", callback_data="donate_custom",
                              icon_custom_emoji_id="5870676941614354370")],
        [InlineKeyboardButton(text="Назад в меню", callback_data="back_main",
                              icon_custom_emoji_id="5893057118545646106")],
    ])

def users_list_kb(users: list, page: int = 0):
    per_page  = 5
    start_idx = page * per_page
    end_idx   = start_idx + per_page
    page_users = users[start_idx:end_idx]

    buttons = []
    for user in page_users:
        uid      = user['user_id']
        username = user['username'] or f"ID {uid}"
        balance  = user['balance']
        buttons.append([InlineKeyboardButton(
            text=f"{username} ({format_chips(balance)})",
            callback_data=f"admin_edit_user_{uid}",
            icon_custom_emoji_id="5870994129244131212"
        )])

    nav = []
    if page > 0:
        nav.append(InlineKeyboardButton(text="Назад", callback_data=f"admin_users_page_{page-1}",
                                        icon_custom_emoji_id="5893057118545646106"))
    if end_idx < len(users):
        nav.append(InlineKeyboardButton(text="Вперед", callback_data=f"admin_users_page_{page+1}",
                                        icon_custom_emoji_id="5893057118545646106"))
    if nav:
        buttons.append(nav)

    buttons.append([InlineKeyboardButton(text="В меню админа", callback_data="admin_back",
                                         icon_custom_emoji_id="5893057118545646106")])
    return InlineKeyboardMarkup(inline_keyboard=buttons)

def edit_user_kb(user_id: int):
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Пополнить баланс", callback_data=f"admin_edit_balance_{user_id}",
                              icon_custom_emoji_id="5904462880941545555")],
        [InlineKeyboardButton(text="История ставок",   callback_data=f"admin_user_history_{user_id}",
                              icon_custom_emoji_id="5870930636742595124")],
        [InlineKeyboardButton(text="Назад к списку",   callback_data="admin_users_back",
                              icon_custom_emoji_id="5893057118545646106")],
    ])

def _minesweeper_mines_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="3 мины (x1.1)",  callback_data="ms_mines_3"),
            InlineKeyboardButton(text="5 мин (x1.3)",   callback_data="ms_mines_5"),
        ],
        [
            InlineKeyboardButton(text="7 мин (x2.0)",   callback_data="ms_mines_7"),
            InlineKeyboardButton(text="10 мин (x2.8)",  callback_data="ms_mines_10"),
        ],
        [InlineKeyboardButton(text="Назад", callback_data="back_main",
                              icon_custom_emoji_id="5893057118545646106")],
    ])

def _minesweeper_kb(revealed: list):
    buttons = []
    for row_idx in range(5):
        row_buttons = []
        for col_idx in range(5):
            t = "✅" if revealed[row_idx][col_idx] else "🟫"
            row_buttons.append(InlineKeyboardButton(text=t, callback_data=f"ms_cell_{row_idx}_{col_idx}"))
        buttons.append(row_buttons)
    buttons.append([InlineKeyboardButton(text="Забрать", callback_data="ms_cashout",
                                         icon_custom_emoji_id="5870633910337015697")])
    buttons.append([InlineKeyboardButton(text="В меню", callback_data="back_main",
                                         icon_custom_emoji_id="5893057118545646106")])
    return InlineKeyboardMarkup(inline_keyboard=buttons)


# ── Farm text helper ──────────────────────────────────────────────────────────

def farm_text(user_id: int) -> str:
    farm    = get_farm(user_id)
    bal     = get_balance(user_id)
    pending = get_farm_pending(farm)

    if not farm:
        return (
            f'<tg-emoji emoji-id="5904462880941545555">🪙</tg-emoji> <b>Биткоин-ферма</b>\n\n'
            f'У вас ещё нет фермы.\n\n'
            f'<tg-emoji emoji-id="5904462880941545555">🪙</tg-emoji> Стоимость: <b>{format_chips(FARM_BUY_COST)}</b>\n'
            f'<tg-emoji emoji-id="5870930636742595124">📊</tg-emoji> Доход: <b>{format_chips(FARM_INCOME[1])} / час</b>\n\n'
            f'<tg-emoji emoji-id="5904462880941545555">🪙</tg-emoji> Ваш баланс: <b>{format_chips(bal)}</b>'
        )

    level        = farm['level']
    income_hour  = FARM_INCOME.get(level, 0)
    name         = FARM_LEVEL_NAMES.get(level, f"Уровень {level}")
    upgrade_cost = FARM_UPGRADE_COSTS.get(level)

    last = farm['last_collect']
    if last.tzinfo is None:
        last = last.replace(tzinfo=pytz.utc)
    now   = datetime.now(pytz.utc)
    hours = (now - last).total_seconds() / 3600
    hours = min(hours, 24)

    text = (
        f'<tg-emoji emoji-id="5904462880941545555">🪙</tg-emoji> <b>Биткоин-ферма</b>\n\n'
        f'⛏ Тип: <b>{name}</b>\n'
        f'<tg-emoji emoji-id="5870930636742595124">📊</tg-emoji> Уровень: <b>{level} / 10</b>\n'
        f'<tg-emoji emoji-id="5904462880941545555">🪙</tg-emoji> Доход: <b>{format_chips(income_hour)} / час</b>\n'
        f'<tg-emoji emoji-id="5983150113483134607">⏰</tg-emoji> Накоплено за {hours:.1f}ч: <b>{format_chips(pending)}</b>\n'
    )
    if level < 10 and upgrade_cost:
        text += f'<tg-emoji emoji-id="5870930636742595124">📊</tg-emoji> Улучшение до ур.{level+1}: <b>{format_chips(upgrade_cost)}</b>\n'
    text += f'\n<tg-emoji emoji-id="5904462880941545555">🪙</tg-emoji> Ваш баланс: <b>{format_chips(bal)}</b>'
    return text


# ── Handlers ──────────────────────────────────────────────────────────────────

@dp.message(CommandStart())
async def cmd_start(msg: Message, state: FSMContext):
    await state.clear()
    bal = get_or_create_user(msg.from_user.id, msg.from_user.username or "игрок")
    text = (
        f'<tg-emoji emoji-id="5258882890059091157">🎰</tg-emoji> <b>Добро пожаловать в Казино!</b>\n\n'
        f'<tg-emoji emoji-id="5904462880941545555">🪙</tg-emoji> Ваш баланс: <b>{format_chips(bal)}</b>\n\n'
        f'<tg-emoji emoji-id="5778672437122045013">📦</tg-emoji> Доступные игры:'
    )
    await msg.answer(text, parse_mode="HTML", reply_markup=main_reply_kb())
    await msg.answer(
        '<tg-emoji emoji-id="5778672437122045013">📦</tg-emoji> <b>Выберите игру:</b>',
        parse_mode="HTML",
        reply_markup=main_menu_kb()
    )


@dp.message(Command("ping"))
async def cmd_ping(msg: Message):
    start_time = time.perf_counter()
    sent_msg = await msg.answer(
        '<tg-emoji emoji-id="5983150113483134607">⏰</tg-emoji> ПОНГ...',
        parse_mode="HTML"
    )
    end_time = time.perf_counter()
    response_time_ms = round((end_time - start_time) * 1000, 2)
    await sent_msg.edit_text(
        f'<tg-emoji emoji-id="5983150113483134607">⏰</tg-emoji> <b>ПОНГ</b>\n\n'
        f'<tg-emoji emoji-id="5870930636742595124">📊</tg-emoji> Отклик: <b>{response_time_ms} ms</b>',
        parse_mode="HTML"
    )


# ── Reply keyboard text handlers ──────────────────────────────────────────────

@dp.message(F.text == "🎰 Казино")
async def reply_casino(msg: Message, state: FSMContext):
    await state.clear()
    bal = get_balance(msg.from_user.id)
    if not bal and bal != 0:
        get_or_create_user(msg.from_user.id, msg.from_user.username or "игрок")
        bal = STARTING_BALANCE
    text = (
        f'<tg-emoji emoji-id="5258882890059091157">🎰</tg-emoji> <b>Казино</b>\n'
        f'<tg-emoji emoji-id="5904462880941545555">🪙</tg-emoji> Баланс: <b>{format_chips(bal)}</b>'
    )
    await msg.answer(text, parse_mode="HTML", reply_markup=main_menu_kb())


@dp.message(F.text == "👤 Профиль")
async def reply_profile(msg: Message, state: FSMContext):
    row = get_user(msg.from_user.id)
    if not row:
        await msg.answer(
            '<tg-emoji emoji-id="5870657884844462243">❌</tg-emoji> Сначала запустите /start',
            parse_mode="HTML"
        )
        return
    _, username, balance, wins, losses, *_ = row
    total = wins + losses
    rate  = round(wins / total * 100, 1) if total else 0
    text = (
        f'<tg-emoji emoji-id="5870921681735781843">📊</tg-emoji> <b>Профиль</b>\n\n'
        f'<tg-emoji emoji-id="5870994129244131212">👤</tg-emoji> Игрок: <b>{username or "Неизвестно"}</b>\n'
        f'<tg-emoji emoji-id="5904462880941545555">🪙</tg-emoji> Баланс: <b>{format_chips(balance)}</b>\n'
        f'<tg-emoji emoji-id="5870633910337015697">✅</tg-emoji> Побед: <b>{wins}</b>\n'
        f'<tg-emoji emoji-id="5870657884844462243">❌</tg-emoji> Поражений: <b>{losses}</b>\n'
        f'<tg-emoji emoji-id="5870930636742595124">📊</tg-emoji> Процент побед: <b>{rate}%</b>'
    )
    await msg.answer(text, parse_mode="HTML", reply_markup=stats_menu_kb())


@dp.message(F.text == "🪙 Баланс")
async def reply_balance(msg: Message):
    bal = get_balance(msg.from_user.id)
    await msg.answer(
        f'<tg-emoji emoji-id="5904462880941545555">🪙</tg-emoji> Ваш баланс: <b>{format_chips(bal)}</b>',
        parse_mode="HTML"
    )


@dp.message(F.text == "🔄 Сбросить баланс")
async def reply_reset(msg: Message, state: FSMContext):
    reset_balance(msg.from_user.id)
    await state.clear()
    await msg.answer(
        f'<tg-emoji emoji-id="5345906554510012647">🔄</tg-emoji> <b>Баланс сброшен!</b>\n'
        f'<tg-emoji emoji-id="5904462880941545555">🪙</tg-emoji> Новый баланс: <b>1000 фишек</b>',
        parse_mode="HTML",
        reply_markup=main_menu_kb()
    )


@dp.message(F.text == "⛏ Ферма")
async def reply_farm(msg: Message):
    user_id = msg.from_user.id
    text = farm_text(user_id)
    await msg.answer(text, parse_mode="HTML", reply_markup=farm_kb(user_id))


@dp.message(F.text == "📊 История")
async def reply_history(msg: Message):
    user_id = msg.from_user.id
    history = get_history(user_id, limit=15)

    if not history:
        text = f'<tg-emoji emoji-id="5870930636742595124">📊</tg-emoji> <b>Нет истории ставок</b>'
    else:
        text = f'<tg-emoji emoji-id="5870930636742595124">📊</tg-emoji> <b>История ставок (последние 15):</b>\n\n'
        for entry in history:
            status_emoji = (
                f'<tg-emoji emoji-id="5870633910337015697">✅</tg-emoji>'
                if entry['is_win'] else
                f'<tg-emoji emoji-id="5870657884844462243">❌</tg-emoji>'
            )
            game_names = {"coin": "Монета", "rocket": "Ракета", "minesweeper": "Сапер",
                          "admin_topup": "Пополнение", "farm": "Ферма"}
            game_name = game_names.get(entry['game_type'], "Рулетка")
            sign = '+' if entry['is_win'] else '-'
            text += f'{status_emoji} {sign}{entry["amount"]} — {game_name}\n'

    stats = get_user_history_stats(user_id)
    if stats and (stats['win_count'] or stats['lose_count']):
        total_won  = stats['total_won']  or 0
        total_lost = stats['total_lost'] or 0
        text += f'\n<tg-emoji emoji-id="5870633910337015697">✅</tg-emoji> Выигрыши: <b>+{total_won}</b>\n'
        text += f'<tg-emoji emoji-id="5870657884844462243">❌</tg-emoji> Проигрыши: <b>-{total_lost}</b>\n'
        net = total_won - total_lost
        if net > 0:
            text += f'<tg-emoji emoji-id="5870633910337015697">✅</tg-emoji> Итог: <b>+{net}</b>'
        elif net < 0:
            text += f'<tg-emoji emoji-id="5870657884844462243">❌</tg-emoji> Итог: <b>{net}</b>'
        else:
            text += f'<tg-emoji emoji-id="5904462880941545555">🪙</tg-emoji> Итог: <b>0</b>'

    await msg.answer(text, parse_mode="HTML", reply_markup=InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Назад в статистику", callback_data="stats",
                              icon_custom_emoji_id="5893057118545646106")],
    ]))


# ── Callback handlers ─────────────────────────────────────────────────────────

@dp.callback_query(F.data == "back_main")
async def back_main(cq: CallbackQuery, state: FSMContext):
    await state.clear()
    bal = get_balance(cq.from_user.id)
    text = (
        f'<tg-emoji emoji-id="5258882890059091157">🎰</tg-emoji> <b>Казино</b>\n'
        f'<tg-emoji emoji-id="5904462880941545555">🪙</tg-emoji> Баланс: <b>{format_chips(bal)}</b>'
    )
    await safe_edit_or_send(cq, text, reply_markup=main_menu_kb())


@dp.callback_query(F.data == "balance")
async def show_balance(cq: CallbackQuery):
    bal = get_balance(cq.from_user.id)
    await cq.answer(f'Ваш баланс: {format_chips(bal)}', show_alert=True)


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
        f'<tg-emoji emoji-id="5870921681735781843">📊</tg-emoji> <b>Профиль</b>\n\n'
        f'<tg-emoji emoji-id="5870994129244131212">👤</tg-emoji> Игрок: <b>{username or "Неизвестно"}</b>\n'
        f'<tg-emoji emoji-id="5904462880941545555">🪙</tg-emoji> Баланс: <b>{format_chips(balance)}</b>\n'
        f'<tg-emoji emoji-id="5870633910337015697">✅</tg-emoji> Побед: <b>{wins}</b>\n'
        f'<tg-emoji emoji-id="5870657884844462243">❌</tg-emoji> Поражений: <b>{losses}</b>\n'
        f'<tg-emoji emoji-id="5870930636742595124">📊</tg-emoji> Процент побед: <b>{rate}%</b>'
    )
    await safe_edit_or_send(cq, text, reply_markup=stats_menu_kb())


@dp.callback_query(F.data == "stats_history")
async def show_history(cq: CallbackQuery):
    user_id = cq.from_user.id
    history = get_history(user_id, limit=15)

    if not history:
        text = f'<tg-emoji emoji-id="5870930636742595124">📊</tg-emoji> <b>Нет истории ставок</b>'
    else:
        text = f'<tg-emoji emoji-id="5870930636742595124">📊</tg-emoji> <b>История ставок (последние 15):</b>\n\n'
        for entry in history:
            status_emoji = (
                f'<tg-emoji emoji-id="5870633910337015697">✅</tg-emoji>'
                if entry['is_win'] else
                f'<tg-emoji emoji-id="5870657884844462243">❌</tg-emoji>'
            )
            game_names = {"coin": "Монета", "rocket": "Ракета", "minesweeper": "Сапер",
                          "admin_topup": "Пополнение", "farm": "Ферма"}
            game_name = game_names.get(entry['game_type'], "Рулетка")
            sign = '+' if entry['is_win'] else '-'
            text += f'{status_emoji} {sign}{entry["amount"]} — {game_name}\n'

    stats = get_user_history_stats(user_id)
    if stats and (stats['win_count'] or stats['lose_count']):
        total_won  = stats['total_won']  or 0
        total_lost = stats['total_lost'] or 0
        text += f'\n<tg-emoji emoji-id="5870633910337015697">✅</tg-emoji> Выигрыши: <b>+{total_won}</b>\n'
        text += f'<tg-emoji emoji-id="5870657884844462243">❌</tg-emoji> Проигрыши: <b>-{total_lost}</b>\n'
        net = total_won - total_lost
        if net > 0:
            text += f'<tg-emoji emoji-id="5870633910337015697">✅</tg-emoji> Итог: <b>+{net}</b>'
        elif net < 0:
            text += f'<tg-emoji emoji-id="5870657884844462243">❌</tg-emoji> Итог: <b>{net}</b>'
        else:
            text += f'<tg-emoji emoji-id="5904462880941545555">🪙</tg-emoji> Итог: <b>0</b>'

    await safe_edit_or_send(cq, text, reply_markup=InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Назад в статистику", callback_data="stats",
                              icon_custom_emoji_id="5893057118545646106")],
    ]))


@dp.callback_query(F.data == "donate")
async def open_donate(cq: CallbackQuery):
    text = (
        '<tg-emoji emoji-id="6032644646587338669">🎁</tg-emoji> <b>Накормить автора</b>\n\n'
        'Выберите количество звезд для поддержки разработки\n'
        '(звезды идут разработчику, спасибо! 💙)'
    )
    await safe_edit_or_send(cq, text, reply_markup=donate_kb())


@dp.callback_query(F.data.startswith("donate_") & (F.data != "donate_custom"))
async def process_donation(cq: CallbackQuery):
    amount_str = cq.data.split("_")[1]
    try:
        amount = int(amount_str)
    except:
        await cq.answer("Некорректная сумма", show_alert=True)
        return
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
async def ask_custom_donate_amount(cq: CallbackQuery, state: FSMContext):
    await state.set_state(DonateState.entering_custom_amount)
    await safe_edit_or_send(
        cq,
        '<tg-emoji emoji-id="5870676941614354370">🖋</tg-emoji> <b>Введите сумму в звездах (⭐)</b>\n\n'
        'Минимум: 1 ⭐, максимум: 10000 ⭐',
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="Назад", callback_data="donate",
                                  icon_custom_emoji_id="5893057118545646106")]
        ])
    )


@dp.message(DonateState.entering_custom_amount)
async def process_custom_donate_amount(msg: Message, state: FSMContext):
    try:
        amount = int(msg.text.strip())
        if amount < 1 or amount > 10000:
            raise ValueError
    except:
        await msg.answer(
            '<tg-emoji emoji-id="5870657884844462243">❌</tg-emoji> Сумма должна быть от 1 до 10000 ⭐',
            parse_mode="HTML"
        )
        return
    await state.clear()
    await bot.send_invoice(
        chat_id=msg.from_user.id,
        title="Поддержка разработчика",
        description=f"Спасибо за {amount} ⭐! Это помогает развивать бота 💙",
        payload=f"donate_{amount}",
        currency="XTR",
        prices=[LabeledPrice(label=f"Поддержка ({amount} ⭐)", amount=amount)],
        provider_token="",
    )


@dp.callback_query(F.data == "reset")
async def reset_handler(cq: CallbackQuery, state: FSMContext):
    reset_balance(cq.from_user.id)
    await state.clear()
    await cq.answer('Баланс сброшен до 1000 фишек!', show_alert=True)
    await safe_edit_or_send(
        cq,
        f'<tg-emoji emoji-id="5345906554510012647">🔄</tg-emoji> <b>Баланс сброшен!</b>\n'
        f'<tg-emoji emoji-id="5904462880941545555">🪙</tg-emoji> Новый баланс: <b>1000 фишек</b>',
        reply_markup=main_menu_kb()
    )


@dp.pre_checkout_query()
async def process_pre_checkout_query(pre_checkout_query: PreCheckoutQuery):
    await bot.answer_pre_checkout_query(pre_checkout_query.id, ok=True)


@dp.message(lambda msg: msg.successful_payment is not None)
async def process_successful_payment(msg: Message):
    payment = msg.successful_payment
    if not payment.invoice_payload.startswith("donate_"):
        return
    amount = int(payment.invoice_payload.split("_")[1])
    await msg.answer(
        f'<tg-emoji emoji-id="6041731551845159060">🎉</tg-emoji> <b>Спасибо за {amount} звезд!</b>\n\n'
        f'Ваша поддержка очень важна для развития бота! 💙',
        parse_mode="HTML",
        reply_markup=main_menu_kb()
    )


# ── Farm handlers ─────────────────────────────────────────────────────────────

@dp.callback_query(F.data == "open_farm")
async def open_farm(cq: CallbackQuery):
    user_id = cq.from_user.id
    text = farm_text(user_id)
    await safe_edit_or_send(cq, text, reply_markup=farm_kb(user_id))
    await cq.answer()


@dp.callback_query(F.data == "farm_noop")
async def farm_noop(cq: CallbackQuery):
    await cq.answer("Недостаточно фишек!", show_alert=True)


@dp.callback_query(F.data == "farm_buy")
async def farm_buy_handler(cq: CallbackQuery):
    user_id = cq.from_user.id
    bal     = get_balance(user_id)
    if bal < FARM_BUY_COST:
        await cq.answer("Недостаточно фишек!", show_alert=True)
        return
    if get_farm(user_id):
        await cq.answer("У вас уже есть ферма!", show_alert=True)
        return

    conn = db_connect()
    try:
        with conn.cursor() as cur:
            cur.execute("UPDATE users SET balance = balance - %s WHERE user_id=%s",
                        (FARM_BUY_COST, user_id))
        conn.commit()
    finally:
        db_release(conn)
    buy_farm(user_id)

    await cq.answer("Ферма куплена! ⛏", show_alert=True)
    text = farm_text(user_id)
    await safe_edit_or_send(cq, text, reply_markup=farm_kb(user_id))


@dp.callback_query(F.data == "farm_collect")
async def farm_collect_handler(cq: CallbackQuery):
    user_id = cq.from_user.id
    farm    = get_farm(user_id)
    if not farm:
        await cq.answer("У вас нет фермы!", show_alert=True)
        return

    pending = get_farm_pending(farm)
    if pending <= 0:
        await cq.answer("Ещё нечего собирать, подождите!", show_alert=True)
        return

    conn = db_connect()
    try:
        with conn.cursor() as cur:
            cur.execute("UPDATE users SET balance = balance + %s WHERE user_id=%s",
                        (pending, user_id))
        conn.commit()
    finally:
        db_release(conn)
    collect_farm(user_id)
    add_history(user_id, pending, True, game_type="farm")

    await cq.answer(f"Собрано +{fmt(pending)} фишек! ✅", show_alert=True)
    text = farm_text(user_id)
    await safe_edit_or_send(cq, text, reply_markup=farm_kb(user_id))


@dp.callback_query(F.data == "farm_upgrade")
async def farm_upgrade_handler(cq: CallbackQuery):
    user_id = cq.from_user.id
    farm    = get_farm(user_id)
    if not farm:
        await cq.answer("У вас нет фермы!", show_alert=True)
        return

    level = farm['level']
    if level >= 10:
        await cq.answer("Максимальный уровень!", show_alert=True)
        return

    upgrade_cost = FARM_UPGRADE_COSTS.get(level, 0)
    bal = get_balance(user_id)
    if bal < upgrade_cost:
        await cq.answer("Недостаточно фишек!", show_alert=True)
        return

    conn = db_connect()
    try:
        with conn.cursor() as cur:
            cur.execute("UPDATE users SET balance = balance - %s WHERE user_id=%s",
                        (upgrade_cost, user_id))
        conn.commit()
    finally:
        db_release(conn)
    upgrade_farm(user_id)

    new_level = level + 1
    new_name  = FARM_LEVEL_NAMES.get(new_level, f"Уровень {new_level}")
    await cq.answer(f"Ферма улучшена до уровня {new_level} — {new_name}! 🎉", show_alert=True)
    text = farm_text(user_id)
    await safe_edit_or_send(cq, text, reply_markup=farm_kb(user_id))


# ── Roulette handlers ─────────────────────────────────────────────────────────

@dp.callback_query(F.data == "open_roulette")
async def open_roulette(cq: CallbackQuery, state: FSMContext):
    bal = get_balance(cq.from_user.id)
    if bal <= 0:
        await cq.answer('У вас нет фишек! Сбросьте баланс.', show_alert=True)
        return
    await state.set_state(BetState.choosing_bet_type)
    text = (
        f'<tg-emoji emoji-id="5258882890059091157">🎰</tg-emoji> <b>Европейская Рулетка</b>\n'
        f'<tg-emoji emoji-id="5904462880941545555">🪙</tg-emoji> Баланс: <b>{format_chips(bal)}</b>\n\n'
        f'<b>Выберите тип ставки:</b>'
    )
    await safe_edit_or_send(cq, text, reply_markup=bet_type_kb())


@dp.callback_query(BetState.choosing_bet_type, F.data.startswith("bet_"))
async def choose_bet_type(cq: CallbackQuery, state: FSMContext):
    raw = cq.data[4:]
    if raw == "number":
        await state.update_data(bet_type="pending_number")
        await state.set_state(BetState.choosing_amount)
        await safe_edit_or_send(
            cq,
            '<tg-emoji emoji-id="5771851822897566479">🔡</tg-emoji> <b>Введите число от 0 до 36:</b>',
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="Назад", callback_data="back_bet_type",
                                      icon_custom_emoji_id="5893057118545646106")]
            ])
        )
        return

    await state.update_data(bet_type=raw)
    await state.set_state(BetState.choosing_amount)
    bal   = get_balance(cq.from_user.id)
    label = BET_LABELS.get(raw, raw)
    text = (
        f'<tg-emoji emoji-id="5258882890059091157">🎰</tg-emoji> <b>Ставка: {label}</b>\n'
        f'<tg-emoji emoji-id="5904462880941545555">🪙</tg-emoji> Баланс: <b>{format_chips(bal)}</b>\n\n'
        f'<b>Выберите сумму ставки:</b>'
    )
    await safe_edit_or_send(cq, text, reply_markup=bet_amount_kb(bal))


@dp.callback_query(F.data == "back_bet_type")
async def back_bet_type(cq: CallbackQuery, state: FSMContext):
    await state.set_state(BetState.choosing_bet_type)
    bal = get_balance(cq.from_user.id)
    text = (
        f'<tg-emoji emoji-id="5258882890059091157">🎰</tg-emoji> <b>Европейская Рулетка</b>\n'
        f'<tg-emoji emoji-id="5904462880941545555">🪙</tg-emoji> Баланс: <b>{format_chips(bal)}</b>\n\n'
        f'<b>Выберите тип ставки:</b>'
    )
    await safe_edit_or_send(cq, text, reply_markup=bet_type_kb())


@dp.message(BetState.choosing_amount)
async def handle_number_input(msg: Message, state: FSMContext):
    data = await state.get_data()

    if data.get("waiting_custom"):
        bal = get_balance(msg.from_user.id)
        try:
            amount = int(msg.text.strip())
            assert 1 <= amount <= bal
        except:
            await msg.answer(f"❗ Введите целое число от <b>1</b> до <b>{bal}</b>.", parse_mode="HTML")
            return
        await state.update_data(waiting_custom=False)
        bet_type = data.get("bet_type", "")
        last_bets[msg.from_user.id] = {'game': 'roulette', 'bet_type': bet_type, 'amount': amount}

        result = spin_wheel()
        won    = check_bet(bet_type, result)
        mult   = payout_multiplier(bet_type)
        color  = number_color(result)
        if won:
            profit = amount * mult
            update_balance(msg.from_user.id, profit, win=True, game_type="roulette")
            outcome_text = (
                f'<tg-emoji emoji-id="6041731551845159060">🎉</tg-emoji> <b>ПОБЕДА!</b>\n'
                f'<tg-emoji emoji-id="5890848474563352982">🪙</tg-emoji> +{format_chips(profit)} (x{mult})'
            )
        else:
            update_balance(msg.from_user.id, -amount, win=False, game_type="roulette")
            outcome_text = (
                f'<tg-emoji emoji-id="5870657884844462243">❌</tg-emoji> <b>Поражение.</b>\n'
                f'<tg-emoji emoji-id="5904462880941545555">🪙</tg-emoji> -{format_chips(amount)}'
            )
        new_bal = get_balance(msg.from_user.id)
        label   = BET_LABELS.get(bet_type, bet_type.replace("num_", "число "))
        text = (
            f'<tg-emoji emoji-id="5258882890059091157">🎰</tg-emoji> <b>Шарик остановился на:</b> {color} <b>{result}</b>\n\n'
            f'🎲 Ваша ставка: <b>{label}</b> — <b>{format_chips(amount)}</b>\n'
            f'{outcome_text}\n\n'
            f'<tg-emoji emoji-id="5904462880941545555">🪙</tg-emoji> Новый баланс: <b>{format_chips(new_bal)}</b>'
        )
        await state.clear()
        if new_bal <= 0:
            text += f'\n\n<tg-emoji emoji-id="5870657884844462243">❌</tg-emoji> <b>Вы банкрот!</b> Нажмите «Сбросить баланс».'
        await msg.answer(text, parse_mode="HTML", reply_markup=game_result_kb("roulette"))
        return

    if data.get("bet_type") != "pending_number":
        return
    try:
        n = int(msg.text.strip())
        assert 0 <= n <= 36
    except:
        await msg.answer("❗ Введите целое число от <b>0</b> до <b>36</b>.", parse_mode="HTML")
        return

    await state.update_data(bet_type=f"num_{n}")
    bal = get_balance(msg.from_user.id)
    await msg.answer(
        f'<tg-emoji emoji-id="5771851822897566479">🔡</tg-emoji> <b>Ставка на число {n}</b> (выплата x35)\n'
        f'<tg-emoji emoji-id="5904462880941545555">🪙</tg-emoji> Баланс: <b>{format_chips(bal)}</b>\n\n<b>Выберите сумму ставки:</b>',
        parse_mode="HTML",
        reply_markup=bet_amount_kb(bal)
    )


@dp.callback_query(BetState.choosing_amount, F.data == "amount_custom")
async def ask_custom_amount(cq: CallbackQuery, state: FSMContext):
    await state.update_data(waiting_custom=True)
    bal = get_balance(cq.from_user.id)
    await safe_edit_or_send(
        cq,
        f'<tg-emoji emoji-id="5870676941614354370">🖋</tg-emoji> <b>Введите сумму ставки:</b>\n'
        f'<tg-emoji emoji-id="5904462880941545555">🪙</tg-emoji> Баланс: <b>{format_chips(bal)}</b>',
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="Назад", callback_data="back_bet_type",
                                  icon_custom_emoji_id="5893057118545646106")]
        ])
    )


@dp.callback_query(BetState.choosing_amount, F.data.startswith("amount_"))
async def place_bet(cq: CallbackQuery, state: FSMContext):
    amount   = int(cq.data.split("_")[1])
    data     = await state.get_data()
    bet_type = data.get("bet_type", "")

    if not bet_type or bet_type == "pending_number":
        await cq.answer('Сначала выберите тип ставки.', show_alert=True)
        return

    bal = get_balance(cq.from_user.id)
    if amount > bal:
        await cq.answer('Недостаточно средств!', show_alert=True)
        return

    last_bets[cq.from_user.id] = {'game': 'roulette', 'bet_type': bet_type, 'amount': amount}
    result = spin_wheel()
    color  = number_color(result)
    won    = check_bet(bet_type, result)
    mult   = payout_multiplier(bet_type)

    if won:
        profit = amount * mult
        update_balance(cq.from_user.id, profit, win=True, game_type="roulette")
        outcome_text = (
            f'<tg-emoji emoji-id="6041731551845159060">🎉</tg-emoji> <b>ПОБЕДА!</b>\n'
            f'<tg-emoji emoji-id="5890848474563352982">🪙</tg-emoji> +{format_chips(profit)} (x{mult})'
        )
    else:
        update_balance(cq.from_user.id, -amount, win=False, game_type="roulette")
        outcome_text = (
            f'<tg-emoji emoji-id="5870657884844462243">❌</tg-emoji> <b>Поражение.</b>\n'
            f'<tg-emoji emoji-id="5904462880941545555">🪙</tg-emoji> -{format_chips(amount)}'
        )

    new_bal = get_balance(cq.from_user.id)
    label   = BET_LABELS.get(bet_type, bet_type.replace("num_", "число "))
    text = (
        f'<tg-emoji emoji-id="5258882890059091157">🎰</tg-emoji> <b>Шарик остановился на:</b> {color} <b>{result}</b>\n\n'
        f'🎲 Ваша ставка: <b>{label}</b> — <b>{format_chips(amount)}</b>\n'
        f'{outcome_text}\n\n'
        f'<tg-emoji emoji-id="5904462880941545555">🪙</tg-emoji> Новый баланс: <b>{format_chips(new_bal)}</b>'
    )
    await state.clear()
    if new_bal <= 0:
        text += f'\n\n<tg-emoji emoji-id="5870657884844462243">❌</tg-emoji> <b>Вы банкрот!</b> Нажмите «Сбросить баланс».'
    await safe_edit_or_send(cq, text, reply_markup=game_result_kb("roulette"))


# ── Coin handlers ─────────────────────────────────────────────────────────────

@dp.callback_query(F.data == "open_coin")
async def open_coin(cq: CallbackQuery, state: FSMContext):
    bal = get_balance(cq.from_user.id)
    if bal <= 0:
        await cq.answer('У вас нет фишек! Сбросьте баланс.', show_alert=True)
        return
    await state.set_state(CoinState.choosing_side)
    text = (
        f'<tg-emoji emoji-id="5774585885154131652">🪙</tg-emoji> <b>Орёл или Решка</b>\n'
        f'<tg-emoji emoji-id="5904462880941545555">🪙</tg-emoji> Баланс: <b>{format_chips(bal)}</b>\n\n'
        f'<b>Выберите сторону фишки:</b>'
    )
    await safe_edit_or_send(cq, text, reply_markup=coin_side_kb())


@dp.callback_query(CoinState.choosing_side, F.data.startswith("coin_"))
async def choose_coin_side(cq: CallbackQuery, state: FSMContext):
    side_raw   = cq.data.split("_")[1]
    side_label = "Орёл" if side_raw == "heads" else "Решка"
    await state.update_data(coin_choice=side_raw)
    await state.set_state(CoinState.choosing_amount)
    bal = get_balance(cq.from_user.id)
    text = (
        f'<tg-emoji emoji-id="5774585885154131652">🪙</tg-emoji> <b>Ставка: {side_label}</b>\n'
        f'<tg-emoji emoji-id="5904462880941545555">🪙</tg-emoji> Баланс: <b>{format_chips(bal)}</b>\n\n'
        f'<b>Выберите сумму ставки:</b>'
    )
    await safe_edit_or_send(cq, text, reply_markup=bet_amount_kb(bal))


@dp.callback_query(CoinState.choosing_amount, F.data == "amount_custom")
async def ask_custom_coin_amount(cq: CallbackQuery, state: FSMContext):
    await state.update_data(waiting_custom=True)
    bal = get_balance(cq.from_user.id)
    await safe_edit_or_send(
        cq,
        f'<tg-emoji emoji-id="5870676941614354370">🖋</tg-emoji> <b>Введите сумму ставки:</b>\n'
        f'<tg-emoji emoji-id="5904462880941545555">🪙</tg-emoji> Баланс: <b>{format_chips(bal)}</b>',
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="Назад", callback_data="back_main",
                                  icon_custom_emoji_id="5893057118545646106")]
        ])
    )


@dp.message(CoinState.choosing_amount)
async def handle_coin_amount_input(msg: Message, state: FSMContext):
    data        = await state.get_data()
    coin_choice = data.get("coin_choice", "")

    if data.get("waiting_custom"):
        bal = get_balance(msg.from_user.id)
        try:
            amount = int(msg.text.strip())
            assert 1 <= amount <= bal
        except:
            await msg.answer(f"❗ Введите целое число от <b>1</b> до <b>{bal}</b>.", parse_mode="HTML")
            return

        last_bets[msg.from_user.id] = {'game': 'coin', 'choice': coin_choice, 'amount': amount}
        result = flip_coin()
        won    = check_coin_bet(coin_choice, result)

        side_label   = "Орёл" if coin_choice == "heads" else "Решка"
        result_label = "Орёл" if result == "heads" else "Решка"

        if won:
            profit = amount * 2
            update_balance(msg.from_user.id, profit, win=True, game_type="coin")
            outcome_text = (
                f'<tg-emoji emoji-id="6041731551845159060">🎉</tg-emoji> <b>ПОБЕДА!</b>\n'
                f'<tg-emoji emoji-id="5890848474563352982">🪙</tg-emoji> +{format_chips(profit)} (x2)'
            )
        else:
            update_balance(msg.from_user.id, -amount, win=False, game_type="coin")
            outcome_text = (
                f'<tg-emoji emoji-id="5870657884844462243">❌</tg-emoji> <b>Поражение.</b>\n'
                f'<tg-emoji emoji-id="5904462880941545555">🪙</tg-emoji> -{format_chips(amount)}'
            )

        new_bal = get_balance(msg.from_user.id)
        text = (
            f'<tg-emoji emoji-id="5774585885154131652">🪙</tg-emoji> <b>Результат:</b> {result_label}\n\n'
            f'🎲 Ваша ставка: <b>{side_label}</b> — <b>{format_chips(amount)}</b>\n'
            f'{outcome_text}\n\n'
            f'<tg-emoji emoji-id="5904462880941545555">🪙</tg-emoji> Новый баланс: <b>{format_chips(new_bal)}</b>'
        )
        await state.clear()
        if new_bal <= 0:
            text += f'\n\n<tg-emoji emoji-id="5870657884844462243">❌</tg-emoji> <b>Вы банкрот!</b> Нажмите «Сбросить баланс».'
        await msg.answer(text, parse_mode="HTML", reply_markup=game_result_kb("coin"))


@dp.callback_query(CoinState.choosing_amount, F.data.startswith("amount_"))
async def place_coin_bet(cq: CallbackQuery, state: FSMContext):
    amount      = int(cq.data.split("_")[1])
    data        = await state.get_data()
    coin_choice = data.get("coin_choice", "")

    if not coin_choice:
        await cq.answer('Сначала выберите сторону фишки.', show_alert=True)
        return

    bal = get_balance(cq.from_user.id)
    if amount > bal:
        await cq.answer('Недостаточно средств!', show_alert=True)
        return

    last_bets[cq.from_user.id] = {'game': 'coin', 'choice': coin_choice, 'amount': amount}
    result = flip_coin()
    won    = check_coin_bet(coin_choice, result)

    side_label   = "Орёл" if coin_choice == "heads" else "Решка"
    result_label = "Орёл" if result == "heads" else "Решка"

    if won:
        profit = amount * 2
        update_balance(cq.from_user.id, profit, win=True, game_type="coin")
        outcome_text = (
            f'<tg-emoji emoji-id="6041731551845159060">🎉</tg-emoji> <b>ПОБЕДА!</b>\n'
            f'<tg-emoji emoji-id="5890848474563352982">🪙</tg-emoji> +{format_chips(profit)} (x2)'
        )
    else:
        update_balance(cq.from_user.id, -amount, win=False, game_type="coin")
        outcome_text = (
            f'<tg-emoji emoji-id="5870657884844462243">❌</tg-emoji> <b>Поражение.</b>\n'
            f'<tg-emoji emoji-id="5904462880941545555">🪙</tg-emoji> -{format_chips(amount)}'
        )

    new_bal = get_balance(cq.from_user.id)
    text = (
        f'<tg-emoji emoji-id="5774585885154131652">🪙</tg-emoji> <b>Результат:</b> {result_label}\n\n'
        f'🎲 Ваша ставка: <b>{side_label}</b> — <b>{format_chips(amount)}</b>\n'
        f'{outcome_text}\n\n'
        f'<tg-emoji emoji-id="5904462880941545555">🪙</tg-emoji> Новый баланс: <b>{format_chips(new_bal)}</b>'
    )
    await state.clear()
    if new_bal <= 0:
        text += f'\n\n<tg-emoji emoji-id="5870657884844462243">❌</tg-emoji> <b>Вы банкрот!</b> Нажмите «Сбросить баланс».'
    await safe_edit_or_send(cq, text, reply_markup=game_result_kb("coin"))


@dp.callback_query(F.data == "repeat_roulette")
async def repeat_roulette(cq: CallbackQuery, state: FSMContext):
    user_id = cq.from_user.id
    if user_id not in last_bets or last_bets[user_id].get('game') != 'roulette':
        await cq.answer("Нет сохраненной ставки, выберите новую.", show_alert=False)
        return

    bet_data = last_bets[user_id]
    bet_type = bet_data.get('bet_type')
    amount   = bet_data.get('amount')
    bal      = get_balance(user_id)

    if amount > bal:
        await cq.answer(f'Недостаточно фишек! Нужно {amount}, а у вас {bal}.', show_alert=True)
        return

    result = spin_wheel()
    color  = number_color(result)
    won    = check_bet(bet_type, result)
    mult   = payout_multiplier(bet_type)

    if won:
        profit = amount * mult
        update_balance(user_id, profit, win=True, game_type="roulette")
        outcome_text = (
            f'<tg-emoji emoji-id="6041731551845159060">🎉</tg-emoji> <b>ПОБЕДА!</b>\n'
            f'<tg-emoji emoji-id="5890848474563352982">🪙</tg-emoji> +{format_chips(profit)} (x{mult})'
        )
    else:
        update_balance(user_id, -amount, win=False, game_type="roulette")
        outcome_text = (
            f'<tg-emoji emoji-id="5870657884844462243">❌</tg-emoji> <b>Поражение.</b>\n'
            f'<tg-emoji emoji-id="5904462880941545555">🪙</tg-emoji> -{format_chips(amount)}'
        )

    new_bal = get_balance(user_id)
    label   = BET_LABELS.get(bet_type, bet_type.replace("num_", "число "))
    text = (
        f'<tg-emoji emoji-id="5258882890059091157">🎰</tg-emoji> <b>Шарик остановился на:</b> {color} <b>{result}</b>\n\n'
        f'🎲 Ваша ставка: <b>{label}</b> — <b>{format_chips(amount)}</b>\n'
        f'{outcome_text}\n\n'
        f'<tg-emoji emoji-id="5904462880941545555">🪙</tg-emoji> Новый баланс: <b>{format_chips(new_bal)}</b>'
    )
    if new_bal <= 0:
        text += f'\n\n<tg-emoji emoji-id="5870657884844462243">❌</tg-emoji> <b>Вы банкрот!</b> Нажмите «Сбросить баланс».'
    await safe_edit_or_send(cq, text, reply_markup=game_result_kb("roulette"))


@dp.callback_query(F.data == "repeat_coin")
async def repeat_coin(cq: CallbackQuery, state: FSMContext):
    user_id = cq.from_user.id
    if user_id not in last_bets or last_bets[user_id].get('game') != 'coin':
        await cq.answer("Нет сохраненной ставки, выберите новую.", show_alert=False)
        return

    bet_data    = last_bets[user_id]
    coin_choice = bet_data.get('choice')
    amount      = bet_data.get('amount')
    bal         = get_balance(user_id)

    if amount > bal:
        await cq.answer(f'Недостаточно фишек! Нужно {amount}, а у вас {bal}.', show_alert=True)
        return

    result = flip_coin()
    won    = check_coin_bet(coin_choice, result)

    side_label   = "Орёл" if coin_choice == "heads" else "Решка"
    result_label = "Орёл" if result == "heads" else "Решка"

    if won:
        profit = amount * 2
        update_balance(user_id, profit, win=True, game_type="coin")
        outcome_text = (
            f'<tg-emoji emoji-id="6041731551845159060">🎉</tg-emoji> <b>ПОБЕДА!</b>\n'
            f'<tg-emoji emoji-id="5890848474563352982">🪙</tg-emoji> +{format_chips(profit)} (x2)'
        )
    else:
        update_balance(user_id, -amount, win=False, game_type="coin")
        outcome_text = (
            f'<tg-emoji emoji-id="5870657884844462243">❌</tg-emoji> <b>Поражение.</b>\n'
            f'<tg-emoji emoji-id="5904462880941545555">🪙</tg-emoji> -{format_chips(amount)}'
        )

    new_bal = get_balance(user_id)
    text = (
        f'<tg-emoji emoji-id="5774585885154131652">🪙</tg-emoji> <b>Результат:</b> {result_label}\n\n'
        f'🎲 Ваша ставка: <b>{side_label}</b> — <b>{format_chips(amount)}</b>\n'
        f'{outcome_text}\n\n'
        f'<tg-emoji emoji-id="5904462880941545555">🪙</tg-emoji> Новый баланс: <b>{format_chips(new_bal)}</b>'
    )
    if new_bal <= 0:
        text += f'\n\n<tg-emoji emoji-id="5870657884844462243">❌</tg-emoji> <b>Вы банкрот!</b> Нажмите «Сбросить баланс».'
    await safe_edit_or_send(cq, text, reply_markup=game_result_kb("coin"))


# ── Admin handlers ────────────────────────────────────────────────────────────

@dp.message(Command("admin"))
async def cmd_admin(msg: Message, state: FSMContext):
    if msg.from_user.id != ADMIN_ID:
        await msg.answer(
            '<tg-emoji emoji-id="5870657884844462243">❌</tg-emoji> У вас нет доступа к админ-панели',
            parse_mode="HTML"
        )
        return
    await state.set_state(AdminState.choosing_action)
    text = (
        '<tg-emoji emoji-id="5870982283724328568">⚙️</tg-emoji> <b>Админ-панель</b>\n\n'
        'Выберите действие:'
    )
    await msg.answer(text, parse_mode="HTML", reply_markup=admin_menu_kb())


@dp.callback_query(F.data == "admin_users")
async def show_users_list(cq: CallbackQuery, state: FSMContext):
    if cq.from_user.id != ADMIN_ID:
        await cq.answer('Только админ может это делать', show_alert=True)
        return
    await state.set_state(AdminState.choosing_user)
    users = get_all_users_full()
    text = (
        f'<tg-emoji emoji-id="5870772616305839506">👥</tg-emoji> <b>Список пользователей:</b>\n\n'
        f'Всего: {len(users)} пользователей\n\n'
        f'Выберите пользователя для редактирования:'
    )
    await safe_edit_or_send(cq, text, reply_markup=users_list_kb(users, 0))


@dp.callback_query(F.data.startswith("admin_users_page_"))
async def paginate_users(cq: CallbackQuery, state: FSMContext):
    if cq.from_user.id != ADMIN_ID:
        return
    page  = int(cq.data.split("_")[-1])
    users = get_all_users_full()
    text = (
        f'<tg-emoji emoji-id="5870772616305839506">👥</tg-emoji> <b>Список пользователей:</b>\n\n'
        f'Всего: {len(users)} пользователей\n\n'
        f'Выберите пользователя для редактирования:'
    )
    await safe_edit_or_send(cq, text, reply_markup=users_list_kb(users, page))


@dp.callback_query(F.data.startswith("admin_edit_user_"))
async def edit_user_menu(cq: CallbackQuery, state: FSMContext):
    if cq.from_user.id != ADMIN_ID:
        return
    user_id = int(cq.data.split("_")[-1])
    row     = get_user(user_id)
    if not row:
        await cq.answer('Пользователь не найден', show_alert=True)
        return
    _, username, balance, wins, losses, *_ = row
    text = (
        f'<tg-emoji emoji-id="5870994129244131212">👤</tg-emoji> <b>Профиль пользователя:</b>\n\n'
        f'ID: <code>{user_id}</code>\n'
        f'Имя: <b>{username or "Неизвестно"}</b>\n'
        f'<tg-emoji emoji-id="5904462880941545555">🪙</tg-emoji> Баланс: <b>{format_chips(balance)}</b>\n'
        f'<tg-emoji emoji-id="5870633910337015697">✅</tg-emoji> Побед: <b>{wins}</b>\n'
        f'<tg-emoji emoji-id="5870657884844462243">❌</tg-emoji> Поражений: <b>{losses}</b>\n\n'
        f'Выберите действие:'
    )
    await state.update_data(admin_user_id=user_id)
    await safe_edit_or_send(cq, text, reply_markup=edit_user_kb(user_id))


@dp.callback_query(F.data.startswith("admin_user_history_"))
async def admin_show_user_history(cq: CallbackQuery):
    if cq.from_user.id != ADMIN_ID:
        return
    user_id  = int(cq.data.split("_")[-1])
    row      = get_user(user_id)
    if not row:
        await cq.answer('Пользователь не найден', show_alert=True)
        return
    username = row[1]
    history  = get_history(user_id, limit=15)

    if not history:
        text = (
            f'<tg-emoji emoji-id="5870930636742595124">📊</tg-emoji> '
            f'<b>История ставок {username or f"ID {user_id}"}:</b>\n\nНет истории ставок'
        )
    else:
        text = (
            f'<tg-emoji emoji-id="5870930636742595124">📊</tg-emoji> '
            f'<b>История ставок {username or f"ID {user_id}"} (последние 15):</b>\n\n'
        )
        for entry in history:
            status_emoji = (
                f'<tg-emoji emoji-id="5870633910337015697">✅</tg-emoji>'
                if entry['is_win'] else
                f'<tg-emoji emoji-id="5870657884844462243">❌</tg-emoji>'
            )
            game_names = {"coin": "Монета", "rocket": "Ракета", "minesweeper": "Сапер",
                          "admin_topup": "Пополнение от админа", "farm": "Ферма"}
            game_name = game_names.get(entry['game_type'], "Рулетка")
            sign = '+' if entry['is_win'] else '-'
            text += f'{status_emoji} {sign}{entry["amount"]} — {game_name}\n'

    stats = get_user_history_stats(user_id)
    if stats and (stats['win_count'] or stats['lose_count']):
        total_won  = stats['total_won']  or 0
        total_lost = stats['total_lost'] or 0
        text += f'\n<tg-emoji emoji-id="5870633910337015697">✅</tg-emoji> Выигрыши: <b>+{total_won}</b>\n'
        text += f'<tg-emoji emoji-id="5870657884844462243">❌</tg-emoji> Проигрыши: <b>-{total_lost}</b>\n'
        net = total_won - total_lost
        if net > 0:
            text += f'<tg-emoji emoji-id="5870633910337015697">✅</tg-emoji> Итог: <b>+{net}</b>'
        elif net < 0:
            text += f'<tg-emoji emoji-id="5870657884844462243">❌</tg-emoji> Итог: <b>{net}</b>'
        else:
            text += f'<tg-emoji emoji-id="5904462880941545555">🪙</tg-emoji> Итог: <b>0</b>'

    await safe_edit_or_send(cq, text, reply_markup=InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Назад к пользователю", callback_data=f"admin_edit_user_{user_id}",
                              icon_custom_emoji_id="5893057118545646106")],
    ]))


@dp.callback_query(F.data.startswith("admin_edit_balance_"))
async def ask_new_balance(cq: CallbackQuery, state: FSMContext):
    if cq.from_user.id != ADMIN_ID:
        return
    user_id = int(cq.data.split("_")[-1])
    await state.update_data(admin_user_id=user_id)
    await state.set_state(AdminState.editing_balance)
    current_balance = get_balance(user_id)
    text = (
        f'<tg-emoji emoji-id="5904462880941545555">🪙</tg-emoji> Текущий баланс: <b>{format_chips(current_balance)}</b>\n\n'
        f'Введите сумму которую хотите <b>добавить</b>:\n'
    )
    await safe_edit_or_send(cq, text, reply_markup=InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Назад", callback_data="admin_users_back",
                              icon_custom_emoji_id="5893057118545646106")]
    ]))


@dp.message(AdminState.editing_balance)
async def process_new_balance(msg: Message, state: FSMContext):
    if msg.from_user.id != ADMIN_ID:
        return
    try:
        new_balance = int(msg.text.strip())
        assert new_balance >= 0
    except:
        await msg.answer(
            '<tg-emoji emoji-id="5870657884844462243">❌</tg-emoji> Введите корректное число (больше или равно 0)',
            parse_mode="HTML"
        )
        return

    data    = await state.get_data()
    user_id = data.get("admin_user_id")

    old_balance = get_balance(user_id)
    new_total   = old_balance + new_balance
    set_balance(user_id, new_total)
    add_history(user_id, new_balance, True, game_type="admin_topup")

    user     = get_user(user_id)
    _, username, *_ = user

    text = (
        f'<tg-emoji emoji-id="5870633910337015697">✅</tg-emoji> <b>Баланс обновлён!</b>\n\n'
        f'<tg-emoji emoji-id="5870994129244131212">👤</tg-emoji> Пользователь: <b>{username or "Неизвестно"}</b> (ID: {user_id})\n'
        f'<tg-emoji emoji-id="5904462880941545555">🪙</tg-emoji> Было: <b>{format_chips(old_balance)}</b>\n'
        f'<tg-emoji emoji-id="5890848474563352982">🪙</tg-emoji> Добавлено: <b>+{format_chips(new_balance)}</b>\n'
        f'<tg-emoji emoji-id="5870633910337015697">✅</tg-emoji> Итого: <b>{format_chips(new_total)}</b>'
    )
    await state.clear()
    await msg.answer(text, parse_mode="HTML", reply_markup=admin_menu_kb())

    try:
        await bot.send_message(
            user_id,
            f'<tg-emoji emoji-id="5904462880941545555">🪙</tg-emoji> <b>Баланс пополнен!</b>\n\n'
            f'<tg-emoji emoji-id="5890848474563352982">🪙</tg-emoji> Добавлено: <b>+{format_chips(new_balance)}</b>\n'
            f'<tg-emoji emoji-id="5870633910337015697">✅</tg-emoji> Текущий баланс: <b>{format_chips(new_total)}</b>',
            parse_mode="HTML"
        )
    except Exception:
        pass


@dp.callback_query(F.data == "admin_broadcast")
async def broadcast_menu(cq: CallbackQuery, state: FSMContext):
    if cq.from_user.id != ADMIN_ID:
        await cq.answer('Только админ может это делать', show_alert=True)
        return

    users = get_all_users_full()
    text = (
        f'<tg-emoji emoji-id="6039422865189638057">📣</tg-emoji> <b>Рассылка сообщений</b>\n\n'
        f'Адресатов: {len(users)} пользователей\n\n'
        f'Введите сообщение, которое нужно отправить всем пользователям:\n\n'
        f'<i>Поддерживает HTML форматирование</i>'
    )
    await state.set_state(AdminState.sending_broadcast)
    await safe_edit_or_send(cq, text, reply_markup=InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Отмена", callback_data="admin_back",
                              icon_custom_emoji_id="5870657884844462243")]
    ]))


@dp.message(AdminState.sending_broadcast)
async def process_broadcast(msg: Message, state: FSMContext):
    if msg.from_user.id != ADMIN_ID:
        return

    broadcast_text = msg.text
    users = get_all_users_full()

    success_count = 0
    fail_count    = 0
    for user in users:
        try:
            await bot.send_message(user['user_id'], broadcast_text, parse_mode="HTML")
            success_count += 1
        except Exception:
            fail_count += 1

    text = (
        f'<tg-emoji emoji-id="5870633910337015697">✅</tg-emoji> <b>Рассылка завершена!</b>\n\n'
        f'Успешно отправлено: <b>{success_count}</b>\n'
        f'Ошибок: <b>{fail_count}</b>'
    )
    await state.clear()
    await msg.answer(text, parse_mode="HTML", reply_markup=admin_menu_kb())


@dp.callback_query(F.data == "admin_back")
async def admin_back_to_menu(cq: CallbackQuery, state: FSMContext):
    if cq.from_user.id != ADMIN_ID:
        return
    await state.clear()
    await state.set_state(AdminState.choosing_action)
    text = (
        '<tg-emoji emoji-id="5870982283724328568">⚙️</tg-emoji> <b>Админ-панель</b>\n\n'
        'Выберите действие:'
    )
    await safe_edit_or_send(cq, text, reply_markup=admin_menu_kb())


@dp.callback_query(F.data == "admin_clear_history")
async def admin_clear_history(cq: CallbackQuery):
    if cq.from_user.id != ADMIN_ID:
        return
    clear_all_history()
    text = '<tg-emoji emoji-id="5870633910337015697">✅</tg-emoji> <b>История ставок очищена!</b>\n\nВсе записи из таблицы history удалены.'
    await safe_edit_or_send(cq, text, reply_markup=InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Назад в админ-панель", callback_data="admin_back",
                              icon_custom_emoji_id="5893057118545646106")],
    ]))
    await cq.answer("История очищена", show_alert=True)


@dp.callback_query(F.data == "admin_users_back")
async def admin_back_to_users(cq: CallbackQuery, state: FSMContext):
    if cq.from_user.id != ADMIN_ID:
        return
    await state.set_state(AdminState.choosing_user)
    users = get_all_users_full()
    text = (
        f'<tg-emoji emoji-id="5870772616305839506">👥</tg-emoji> <b>Список пользователей:</b>\n\n'
        f'Всего: {len(users)} пользователей\n\n'
        f'Выберите пользователя для редактирования:'
    )
    await safe_edit_or_send(cq, text, reply_markup=users_list_kb(users, 0))


# ── Rocket handlers ───────────────────────────────────────────────────────────

def generate_crash_point() -> float:
    r = random.random()
    if r < 0.40:   return round(random.uniform(1.0,  1.5),  2)
    elif r < 0.65: return round(random.uniform(1.5,  2.5),  2)
    elif r < 0.80: return round(random.uniform(2.5,  4.0),  2)
    elif r < 0.92: return round(random.uniform(4.0,  8.0),  2)
    else:          return round(random.uniform(8.0, 20.0),  2)

def next_multiplier(current: float) -> float:
    return round(current + round(random.uniform(0.1, 0.6), 2), 2)

def _rocket_text(amount: int, multiplier: float) -> str:
    potential = int(amount * multiplier)
    stars     = "⭐" * min(int(multiplier), 10)
    return (
        f'<tg-emoji emoji-id="5373139891223741704">🚀</tg-emoji> <b>Ракета летит!</b>\n\n'
        f'<tg-emoji emoji-id="5904462880941545555">🪙</tg-emoji> Ставка: <b>{format_chips(amount)}</b>\n'
        f'<tg-emoji emoji-id="5870930636742595124">📊</tg-emoji> Множитель: <b>x{multiplier:.2f}</b>  {stars}\n'
        f'<tg-emoji emoji-id="5879814368572478751">🏧</tg-emoji> Можно забрать: <b>{format_chips(potential)}</b>\n\n'
        f'Нажми <b>«Дальше»</b> чтобы лететь выше\n'
        f'или <b>«Забрать»</b> чтобы зафиксировать!'
    )

async def _start_rocket_game(message, state: FSMContext, user_id: int, amount: int, is_message: bool = False):
    crash_point = generate_crash_point()
    await state.set_state(RocketState.in_game)
    await state.update_data(rocket_amount=amount, rocket_multiplier=1.0,
                             rocket_crash=crash_point, waiting_custom=False)
    text = _rocket_text(amount, 1.0)
    if is_message:
        await message.answer(text, parse_mode="HTML", reply_markup=rocket_game_kb())
    else:
        try:
            await message.edit_text(text, parse_mode="HTML", reply_markup=rocket_game_kb())
        except Exception:
            await message.answer(text, parse_mode="HTML", reply_markup=rocket_game_kb())


@dp.callback_query(F.data == "open_rocket")
async def open_rocket(cq: CallbackQuery, state: FSMContext):
    bal = get_balance(cq.from_user.id)
    if bal <= 0:
        await cq.answer("У вас нет фишек! Сбросьте баланс.", show_alert=True)
        return
    await state.set_state(RocketState.choosing_amount)
    text = (
        f'<tg-emoji emoji-id="5373139891223741704">🚀</tg-emoji> <b>Ракета</b>\n'
        f'<tg-emoji emoji-id="5904462880941545555">🪙</tg-emoji> Баланс: <b>{format_chips(bal)}</b>\n\n'
        f'Ракета взлетает и множитель растёт.\n'
        f'Нажми <b>«Дальше»</b> чтобы продолжить лететь,\n'
        f'или <b>«Забрать»</b> чтобы зафиксировать выигрыш.\n'
        f'Если ракета взорвётся — ставка сгорает!\n\n'
        f'<b>Выбери сумму ставки:</b>'
    )
    await safe_edit_or_send(cq, text, reply_markup=rocket_amount_kb(bal))


@dp.callback_query(RocketState.choosing_amount, F.data == "rocket_amount_custom")
async def rocket_amount_custom_cb(cq: CallbackQuery, state: FSMContext):
    await state.update_data(waiting_custom=True)
    bal = get_balance(cq.from_user.id)
    await safe_edit_or_send(
        cq,
        f'<tg-emoji emoji-id="5870676941614354370">🖋</tg-emoji> <b>Введите сумму ставки:</b>\n'
        f'<tg-emoji emoji-id="5904462880941545555">🪙</tg-emoji> Баланс: <b>{format_chips(bal)}</b>',
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="Назад", callback_data="open_rocket",
                                  icon_custom_emoji_id="5893057118545646106")]
        ])
    )
    await cq.answer()


@dp.callback_query(RocketState.choosing_amount, F.data.startswith("rocket_amount_"))
async def rocket_set_amount(cq: CallbackQuery, state: FSMContext):
    amount = int(cq.data.replace("rocket_amount_", ""))
    bal    = get_balance(cq.from_user.id)
    if amount > bal:
        await cq.answer("Недостаточно средств!", show_alert=True)
        return
    await _start_rocket_game(cq.message, state, cq.from_user.id, amount)
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
    except:
        await msg.answer(f"❗ Введите целое число от <b>1</b> до <b>{bal}</b>.", parse_mode="HTML")
        return
    await _start_rocket_game(msg, state, msg.from_user.id, amount, is_message=True)


@dp.callback_query(RocketState.in_game, F.data == "rocket_next")
async def rocket_next(cq: CallbackQuery, state: FSMContext):
    user_id = cq.from_user.id
    data    = await state.get_data()
    amount      = data.get("rocket_amount", 0)
    multiplier  = data.get("rocket_multiplier", 1.0)
    crash_point = data.get("rocket_crash", 1.0)

    new_multiplier = next_multiplier(multiplier)

    if new_multiplier >= crash_point:
        update_balance(user_id, -amount, win=False, game_type="rocket")
        await state.clear()
        text = (
            f'<tg-emoji emoji-id="5870657884844462243">❌</tg-emoji> <b>РАКЕТА ВЗОРВАЛАСЬ!</b>\n\n'
            f'<tg-emoji emoji-id="5870930636742595124">📊</tg-emoji> Множитель дошёл до: <b>x{crash_point:.2f}</b>\n'
            f'<tg-emoji emoji-id="5904462880941545555">🪙</tg-emoji> Ставка: <b>{format_chips(amount)}</b>\n'
            f'<tg-emoji emoji-id="5870657884844462243">❌</tg-emoji> Вы потеряли: <b>-{format_chips(amount)}</b>\n\n'
            f'<tg-emoji emoji-id="5904462880941545555">🪙</tg-emoji> Новый баланс: <b>{format_chips(get_balance(user_id))}</b>'
        )
        await safe_edit_or_send(cq, text, reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="Сыграть снова", callback_data="open_rocket",
                                  icon_custom_emoji_id="5373139891223741704")],
            [InlineKeyboardButton(text="В меню", callback_data="back_main",
                                  icon_custom_emoji_id="5893057118545646106")],
        ]))
    else:
        await state.update_data(rocket_multiplier=new_multiplier)
        try:
            await cq.message.edit_text(_rocket_text(amount, new_multiplier),
                                        parse_mode="HTML", reply_markup=rocket_game_kb())
        except Exception:
            pass
    await cq.answer()


@dp.callback_query(RocketState.in_game, F.data == "rocket_cashout")
async def rocket_cashout(cq: CallbackQuery, state: FSMContext):
    user_id    = cq.from_user.id
    data       = await state.get_data()
    amount     = data.get("rocket_amount", 0)
    multiplier = data.get("rocket_multiplier", 1.0)

    net_profit = int(amount * multiplier) - amount
    update_balance(user_id, net_profit, win=True, game_type="rocket")
    await state.clear()

    text = (
        f'<tg-emoji emoji-id="5870633910337015697">✅</tg-emoji> <b>Вы забрали выигрыш!</b>\n\n'
        f'<tg-emoji emoji-id="5870930636742595124">📊</tg-emoji> Множитель: <b>x{multiplier:.2f}</b>\n'
        f'<tg-emoji emoji-id="5904462880941545555">🪙</tg-emoji> Ставка: <b>{format_chips(amount)}</b>\n'
        f'<tg-emoji emoji-id="6041731551845159060">🎉</tg-emoji> Выигрыш: <b>+{format_chips(net_profit)}</b>\n\n'
        f'<tg-emoji emoji-id="5904462880941545555">🪙</tg-emoji> Новый баланс: <b>{format_chips(get_balance(user_id))}</b>'
    )
    await safe_edit_or_send(cq, text, reply_markup=InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Сыграть снова", callback_data="open_rocket",
                              icon_custom_emoji_id="5373139891223741704")],
        [InlineKeyboardButton(text="В меню", callback_data="back_main",
                              icon_custom_emoji_id="5893057118545646106")],
    ]))
    await cq.answer()


# ── Minesweeper handlers ──────────────────────────────────────────────────────

def generate_minesweeper_field(size: int = 5, mines: int = 5):
    field  = [[False]*size for _ in range(size)]
    placed = 0
    while placed < mines:
        r, c = random.randint(0, size-1), random.randint(0, size-1)
        if not field[r][c]:
            field[r][c] = True
            placed += 1
    return field

def _minesweeper_text(amount: int, field: list, revealed: list, mines: int = 5) -> str:
    base_mult    = {3: 1.1, 5: 1.3, 7: 2.0, 10: 2.8}.get(mines, 2.0)
    opened_count = sum(sum(row) for row in revealed)
    multiplier   = base_mult * (1.0 + opened_count * 0.05)
    potential    = int(amount * multiplier)
    text = (
        f'<tg-emoji emoji-id="5373141891321699086">💣</tg-emoji> <b>Сапер</b>\n\n'
        f'<tg-emoji emoji-id="5904462880941545555">🪙</tg-emoji> Ставка: <b>{format_chips(amount)}</b>\n'
        f'<tg-emoji emoji-id="5373141891321699086">💣</tg-emoji> Мин: <b>{mines}</b>\n'
        f'<tg-emoji emoji-id="5870930636742595124">📊</tg-emoji> Множитель: <b>x{multiplier:.2f}</b>\n'
        f'<tg-emoji emoji-id="5879814368572478751">🏧</tg-emoji> Можно забрать: <b>{format_chips(potential)}</b>\n\n'
        f'<b>Открывай ячейки:</b>\n'
    )
    for row_idx, row in enumerate(revealed):
        for col_idx, is_revealed in enumerate(row):
            if is_revealed:
                text += "💥" if field[row_idx][col_idx] else "✅"
            else:
                text += "🟫"
        text += "\n"
    return text


@dp.callback_query(F.data == "open_minesweeper")
async def open_minesweeper(cq: CallbackQuery, state: FSMContext):
    bal = get_balance(cq.from_user.id)
    if bal <= 0:
        await cq.answer("У вас нет фишек! Сбросьте баланс.", show_alert=True)
        return
    await state.set_state(MinesweeperState.choosing_amount)
    text = (
        f'<tg-emoji emoji-id="5373141891321699086">💣</tg-emoji> <b>Сапер</b>\n'
        f'<tg-emoji emoji-id="5904462880941545555">🪙</tg-emoji> Баланс: <b>{format_chips(bal)}</b>\n\n'
        f'Открывай ячейки и не попадись на мину!\n'
        f'Чем больше ячеек откроешь — тем выше множитель.\n\n'
        f'<b>Выбери сумму ставки:</b>'
    )
    await safe_edit_or_send(cq, text, reply_markup=bet_amount_kb(bal))


@dp.callback_query(MinesweeperState.choosing_amount, F.data.startswith("amount_"), F.data != "amount_custom")
async def minesweeper_set_amount(cq: CallbackQuery, state: FSMContext):
    amount = int(cq.data.replace("amount_", ""))
    bal    = get_balance(cq.from_user.id)
    if amount > bal:
        await cq.answer("Недостаточно средств!", show_alert=True)
        return
    await state.set_state(MinesweeperState.choosing_mines)
    await state.update_data(minesweeper_amount=amount)
    text = (
        f'<tg-emoji emoji-id="5373141891321699086">💣</tg-emoji> <b>Выбери количество мин</b>\n\n'
        f'<tg-emoji emoji-id="5904462880941545555">🪙</tg-emoji> Ставка: <b>{format_chips(amount)}</b>\n\n'
        f'Больше мин — выше множитель и больше риск!'
    )
    await safe_edit_or_send(cq, text, reply_markup=_minesweeper_mines_kb())
    await cq.answer()


@dp.callback_query(MinesweeperState.choosing_amount, F.data == "amount_custom")
async def minesweeper_amount_custom_cb(cq: CallbackQuery, state: FSMContext):
    await state.update_data(waiting_custom=True)
    bal = get_balance(cq.from_user.id)
    await safe_edit_or_send(
        cq,
        f'<tg-emoji emoji-id="5870676941614354370">🖋</tg-emoji> <b>Введите сумму ставки:</b>\n'
        f'<tg-emoji emoji-id="5904462880941545555">🪙</tg-emoji> Баланс: <b>{format_chips(bal)}</b>',
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="Назад", callback_data="back_main",
                                  icon_custom_emoji_id="5893057118545646106")]
        ])
    )


@dp.message(MinesweeperState.choosing_amount)
async def minesweeper_custom_amount(msg: Message, state: FSMContext):
    data = await state.get_data()
    if not data.get("waiting_custom"):
        return
    bal = get_balance(msg.from_user.id)
    try:
        amount = int(msg.text.strip())
        assert 1 <= amount <= bal
    except:
        await msg.answer(f"❗ Введите целое число от <b>1</b> до <b>{bal}</b>.", parse_mode="HTML")
        return
    await state.set_state(MinesweeperState.choosing_mines)
    await state.update_data(minesweeper_amount=amount, waiting_custom=False)
    text = (
        f'<tg-emoji emoji-id="5373141891321699086">💣</tg-emoji> <b>Выбери количество мин</b>\n\n'
        f'<tg-emoji emoji-id="5904462880941545555">🪙</tg-emoji> Ставка: <b>{format_chips(amount)}</b>\n\n'
        f'Больше мин — выше множитель и больше риск!'
    )
    await msg.answer(text, parse_mode="HTML", reply_markup=_minesweeper_mines_kb())


@dp.callback_query(MinesweeperState.choosing_mines, F.data.startswith("ms_mines_"))
async def minesweeper_start_game(cq: CallbackQuery, state: FSMContext):
    mines_count = int(cq.data.split("_")[-1])
    data        = await state.get_data()
    amount      = data.get("minesweeper_amount", 0)
    field       = generate_minesweeper_field(size=5, mines=mines_count)
    revealed    = [[False]*5 for _ in range(5)]
    await state.set_state(MinesweeperState.in_game)
    await state.update_data(minesweeper_field=field, minesweeper_revealed=revealed,
                             minesweeper_mines=mines_count)
    text = _minesweeper_text(amount, field, revealed, mines_count)
    await safe_edit_or_send(cq, text, reply_markup=_minesweeper_kb(revealed))
    await cq.answer()


@dp.callback_query(MinesweeperState.in_game, F.data.startswith("ms_cell_"))
async def minesweeper_open_cell(cq: CallbackQuery, state: FSMContext):
    user_id  = cq.from_user.id
    parts    = cq.data.split("_")
    row, col = int(parts[2]), int(parts[3])
    data     = await state.get_data()
    field       = data.get("minesweeper_field", [])
    revealed    = data.get("minesweeper_revealed", [])
    amount      = data.get("minesweeper_amount", 0)
    mines_count = data.get("minesweeper_mines", 5)

    if revealed[row][col]:
        await cq.answer("Эта ячейка уже открыта!", show_alert=False)
        return

    revealed[row][col] = True

    if field[row][col]:
        update_balance(user_id, -amount, win=False, game_type="minesweeper")
        await state.clear()
        text = (
            f'<tg-emoji emoji-id="5870657884844462243">❌</tg-emoji> <b>МИНА!</b>\n\n'
            f'<tg-emoji emoji-id="5904462880941545555">🪙</tg-emoji> Ставка: <b>{format_chips(amount)}</b>\n'
            f'<tg-emoji emoji-id="5870657884844462243">❌</tg-emoji> Вы потеряли: <b>-{format_chips(amount)}</b>\n\n'
            f'<tg-emoji emoji-id="5904462880941545555">🪙</tg-emoji> Новый баланс: <b>{format_chips(get_balance(user_id))}</b>'
        )
        await safe_edit_or_send(cq, text, reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="Сыграть снова", callback_data="open_minesweeper",
                                  icon_custom_emoji_id="5373141891321699086")],
            [InlineKeyboardButton(text="В меню", callback_data="back_main",
                                  icon_custom_emoji_id="5893057118545646106")],
        ]))
    else:
        await state.update_data(minesweeper_revealed=revealed)
        text = _minesweeper_text(amount, field, revealed, mines_count)
        try:
            await cq.message.edit_text(text, parse_mode="HTML", reply_markup=_minesweeper_kb(revealed))
        except Exception:
            pass
    await cq.answer()


@dp.callback_query(MinesweeperState.in_game, F.data == "ms_cashout")
async def minesweeper_cashout(cq: CallbackQuery, state: FSMContext):
    user_id  = cq.from_user.id
    data     = await state.get_data()
    amount      = data.get("minesweeper_amount", 0)
    revealed    = data.get("minesweeper_revealed", [])
    mines_count = data.get("minesweeper_mines", 5)
    opened_count = sum(sum(row) for row in revealed)
    base_mult    = {3: 1.1, 5: 1.3, 7: 2.0, 10: 2.8}.get(mines_count, 2.0)
    multiplier   = base_mult * (1.0 + opened_count * 0.05)
    net_profit   = int(amount * multiplier) - amount

    update_balance(user_id, net_profit, win=True, game_type="minesweeper")
    await state.clear()
    text = (
        f'<tg-emoji emoji-id="5870633910337015697">✅</tg-emoji> <b>Вы забрали выигрыш!</b>\n\n'
        f'<tg-emoji emoji-id="5870633910337015697">✅</tg-emoji> Открыто ячеек: <b>{opened_count}</b>\n'
        f'<tg-emoji emoji-id="5870930636742595124">📊</tg-emoji> Множитель: <b>x{multiplier:.2f}</b>\n'
        f'<tg-emoji emoji-id="5904462880941545555">🪙</tg-emoji> Ставка: <b>{format_chips(amount)}</b>\n'
        f'<tg-emoji emoji-id="6041731551845159060">🎉</tg-emoji> Выигрыш: <b>+{format_chips(net_profit)}</b>\n\n'
        f'<tg-emoji emoji-id="5904462880941545555">🪙</tg-emoji> Новый баланс: <b>{format_chips(get_balance(user_id))}</b>'
    )
    await safe_edit_or_send(cq, text, reply_markup=InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Сыграть снова", callback_data="open_minesweeper",
                              icon_custom_emoji_id="5373141891321699086")],
        [InlineKeyboardButton(text="В меню", callback_data="back_main",
                              icon_custom_emoji_id="5893057118545646106")],
    ]))
    await cq.answer()


# ── Background tasks ──────────────────────────────────────────────────────────

async def daily_bonus_task():
    msk = pytz.timezone("Europe/Moscow")
    while True:
        now    = datetime.now(msk)
        target = now.replace(hour=12, minute=0, second=0, microsecond=0)
        if now >= target:
            target += timedelta(days=1)
        await asyncio.sleep((target - now).total_seconds())

        for user_id in get_all_users():
            add_daily_bonus(user_id)
            new_bal = get_balance(user_id)
            try:
                await bot.send_message(
                    user_id,
                    f'<tg-emoji emoji-id="6032644646587338669">🎁</tg-emoji> <b>Ежедневный бонус!</b>\n\n'
                    f'<tg-emoji emoji-id="5890848474563352982">🪙</tg-emoji> Вам начислено <b>+500 фишек</b>\n'
                    f'<tg-emoji emoji-id="5904462880941545555">🪙</tg-emoji> Текущий баланс: <b>{format_chips(new_bal)}</b>\n\n'
                    f'<i>Удачной игры!</i>',
                    parse_mode="HTML"
                )
            except Exception:
                pass
        await asyncio.sleep(60)


# ── Main ──────────────────────────────────────────────────────────────────────

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
            print("🗄️ Database connection pool closed")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("🛑 Бот остановлен.")