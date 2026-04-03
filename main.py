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
from aiogram.types import Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton, LabeledPrice, PreCheckoutQuery
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

last_bets = {}  # {user_id: {'game': 'coin'/'roulette', 'choice'/'bet_type': ..., 'amount': ...}}

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
                    user_id    BIGINT PRIMARY KEY,
                    username   TEXT,
                    balance    INTEGER DEFAULT 1000,
                    wins       INTEGER DEFAULT 0,
                    losses     INTEGER DEFAULT 0
                )
            """)
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
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_history_user_id ON history(user_id)
            """)
        conn.commit()
    finally:
        db_release(conn)

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
            cur.execute(
                "UPDATE users SET balance = %s WHERE user_id=%s",
                (STARTING_BALANCE, user_id)
            )
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
            cur.execute(
                "UPDATE users SET balance = %s WHERE user_id=%s",
                (amount, user_id)
            )
        conn.commit()
    finally:
        db_release(conn)

def add_history(user_id: int, amount: int, is_win: bool, game_type: str = "roulette"):
    conn = db_connect()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """INSERT INTO history (user_id, amount, is_win, game_type)
                   VALUES (%s, %s, %s, %s)""",
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
                """SELECT amount, is_win, game_type, created_at
                   FROM history
                   WHERE user_id=%s
                   ORDER BY created_at DESC
                   LIMIT %s""",
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
                   FROM history
                   WHERE user_id=%s AND game_type != 'admin_topup'""",
                (user_id,)
            )
            return cur.fetchone()
    finally:
        db_release(conn)

def clear_all_history():
    """Удалить всю историю ставок."""
    conn = db_connect()
    try:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM history")
        conn.commit()
    finally:
        db_release(conn)


class BetState(StatesGroup):
    choosing_bet_type = State()
    choosing_amount   = State()

class CoinState(StatesGroup):
    choosing_side = State()
    choosing_amount = State()

class AdminState(StatesGroup):
    choosing_action = State()
    choosing_user = State()
    editing_balance = State()
    sending_broadcast = State()

class DonateState(StatesGroup):
    entering_custom_amount = State()

class RocketState(StatesGroup):
    choosing_amount = State()
    in_game = State()

class MinesweeperState(StatesGroup):
    choosing_amount = State()
    in_game = State()

RED_NUMBERS   = {1,3,5,7,9,12,14,16,18,19,21,23,25,27,30,32,34,36}
BLACK_NUMBERS = {2,4,6,8,10,11,13,15,17,20,22,24,26,28,29,31,33,35}

def spin_wheel() -> int:
    return random.randint(0, 36)

def number_color(n: int) -> str:
    if n == 0:   return "🟢"
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

def main_menu_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(
            text="Рулетка",
            callback_data="open_roulette",
            icon_custom_emoji_id="5258882890059091157"
        )],
        [InlineKeyboardButton(
            text="Орёл или Решка",
            callback_data="open_coin",
            icon_custom_emoji_id="5774585885154131652"
        )],
        [InlineKeyboardButton(
            text="Ракета",
            callback_data="open_rocket",
        )],
        [InlineKeyboardButton(
            text="Сапер",
            callback_data="open_minesweeper",
        )],
        [
            InlineKeyboardButton(
                text="Статистика",
                callback_data="stats",
                icon_custom_emoji_id="5870921681735781843"
            ),
        ],
        [InlineKeyboardButton(
            text="Накормить автора",
            callback_data="donate",
            icon_custom_emoji_id="5904462880941545555"
        )],
        [InlineKeyboardButton(
            text="Сбросить баланс",
            callback_data="reset",
            icon_custom_emoji_id="5345906554510012647"
        )],
    ])

def rocket_game_kb():
    """Кнопки во время игры в ракету"""
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="🚀 Дальше", callback_data="rocket_next"),
            InlineKeyboardButton(text="💰 Забрать", callback_data="rocket_cashout"),
        ],
    ])

def rocket_amount_kb(balance: int):
    """Клавиатура выбора ставки для ракеты"""
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
            InlineKeyboardButton(text=f"Ва-банк ({fmt(q4)})", callback_data=f"rocket_amount_{q4}"),
        ],
        [InlineKeyboardButton(
            text="Ввести вручную",
            callback_data="rocket_amount_custom",
            icon_custom_emoji_id="5870676941614354370"
        )],
        [InlineKeyboardButton(
            text="Назад",
            callback_data="back_main",
            icon_custom_emoji_id="5893057118545646106"
        )],
    ])

def bet_type_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="Красное",  callback_data="bet_red",
                                 icon_custom_emoji_id="5870657884844462243"),
            InlineKeyboardButton(text="Чёрное",   callback_data="bet_black",
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
            InlineKeyboardButton(text="1ST 12",  callback_data="bet_1st12"),
            InlineKeyboardButton(text="2ND 12",  callback_data="bet_2nd12"),
            InlineKeyboardButton(text="3RD 12",  callback_data="bet_3rd12"),
        ],
        [
            InlineKeyboardButton(text="2to1 (ряд 1)", callback_data="bet_2to1_1"),
            InlineKeyboardButton(text="2to1 (ряд 2)", callback_data="bet_2to1_2"),
            InlineKeyboardButton(text="2to1 (ряд 3)", callback_data="bet_2to1_3"),
        ],
        [InlineKeyboardButton(
            text="Конкретное число (x35)",
            callback_data="bet_number",
            icon_custom_emoji_id="5771851822897566479"
        )],
        [InlineKeyboardButton(
            text="Назад",
            callback_data="back_main",
            icon_custom_emoji_id="5893057118545646106"
        )],
    ])

def coin_side_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="Орёл", callback_data="coin_heads",
                                 icon_custom_emoji_id="5774585885154131652"),
            InlineKeyboardButton(text="Решка", callback_data="coin_tails",
                                 icon_custom_emoji_id="5904462880941545555"),
        ],
        [InlineKeyboardButton(
            text="Назад",
            callback_data="back_main",
            icon_custom_emoji_id="5893057118545646106"
        )],
    ])

def fmt(n: int) -> str:
    if n >= 1000:
        val = n / 1000
        return f"{val:.1f}к".replace(".0к", "к")
    return str(n)

def bet_amount_kb(balance: int):
    q1 = max(1, round(balance * 0.25))
    q2 = max(1, round(balance * 0.50))
    q3 = max(1, round(balance * 0.75))
    q4 = balance
    buttons = [
        [
            InlineKeyboardButton(text=f"25%  ({fmt(q1)})",  callback_data=f"amount_{q1}"),
            InlineKeyboardButton(text=f"75%  ({fmt(q3)})",  callback_data=f"amount_{q3}"),
        ],
        [
            InlineKeyboardButton(text=f"50%  ({fmt(q2)})",  callback_data=f"amount_{q2}"),
            InlineKeyboardButton(
                text=f"Ва-банк ({fmt(q4)})",
                callback_data=f"amount_{q4}",
                icon_custom_emoji_id="6041731551845159060"
            ),
        ],
        [InlineKeyboardButton(
            text="Ввести вручную",
            callback_data="amount_custom",
            icon_custom_emoji_id="5870676941614354370"
        )],
        [InlineKeyboardButton(
            text="Назад",
            callback_data="back_bet_type",
            icon_custom_emoji_id="5893057118545646106"
        )],
    ]
    return InlineKeyboardMarkup(inline_keyboard=buttons)

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

def admin_menu_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(
            text="Список пользователей",
            callback_data="admin_users",
            icon_custom_emoji_id="5870772616305839506"
        )],
        [InlineKeyboardButton(
            text="Рассылка сообщения",
            callback_data="admin_broadcast",
            icon_custom_emoji_id="6039422865189638057"
        )],
        [InlineKeyboardButton(
            text="Очистить историю",
            callback_data="admin_clear_history",
            icon_custom_emoji_id="5870657884844462243"
        )],
        [InlineKeyboardButton(
            text="Вернуться в меню",
            callback_data="back_main",
            icon_custom_emoji_id="5893057118545646106"
        )],
    ])

def stats_menu_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(
            text="История ставок",
            callback_data="stats_history",
            icon_custom_emoji_id="5870930636742595124"
        )],
        [InlineKeyboardButton(
            text="Назад в меню",
            callback_data="back_main",
            icon_custom_emoji_id="5893057118545646106"
        )],
    ])

def game_result_kb(game_type: str):
    if game_type == "coin":
        repeat_data = "repeat_coin"
    else:
        repeat_data = "repeat_roulette"

    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(
            text="Повторить",
            callback_data=repeat_data,
            icon_custom_emoji_id="5345906554510012647"
        )],
        [InlineKeyboardButton(
            text="В меню",
            callback_data="back_main",
            icon_custom_emoji_id="5893057118545646106"
        )],
    ])

def donate_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="⭐️50",   callback_data="donate_50",
                              icon_custom_emoji_id="5870982283724328568")],
        [InlineKeyboardButton(text="⭐️100",  callback_data="donate_100",
                              icon_custom_emoji_id="5870982283724328568")],
        [InlineKeyboardButton(text="⭐️250",  callback_data="donate_250",
                              icon_custom_emoji_id="5870982283724328568")],
        [InlineKeyboardButton(text="⭐️500",  callback_data="donate_500",
                              icon_custom_emoji_id="5870982283724328568")],
        [InlineKeyboardButton(text="⭐️1000", callback_data="donate_1000",
                              icon_custom_emoji_id="5870982283724328568")],
        [InlineKeyboardButton(
            text="Своя сумма",
            callback_data="donate_custom",
            icon_custom_emoji_id="5870676941614354370"
        )],
        [InlineKeyboardButton(
            text="Назад в меню",
            callback_data="back_main",
            icon_custom_emoji_id="5893057118545646106"
        )],
    ])

def users_list_kb(users: list, page: int = 0):
    per_page = 5
    start_idx = page * per_page
    end_idx = start_idx + per_page
    page_users = users[start_idx:end_idx]

    buttons = []
    for user in page_users:
        user_id = user['user_id']
        username = user['username'] or f"ID {user_id}"
        balance = user['balance']
        buttons.append([InlineKeyboardButton(
            text=f"{username} ({balance} монет)",
            callback_data=f"admin_edit_user_{user_id}",
            icon_custom_emoji_id="5870994129244131212"
        )])

    nav_buttons = []
    if page > 0:
        nav_buttons.append(InlineKeyboardButton(
            text="Назад",
            callback_data=f"admin_users_page_{page-1}",
            icon_custom_emoji_id="5893057118545646106"
        ))
    if end_idx < len(users):
        nav_buttons.append(InlineKeyboardButton(
            text="Вперед",
            callback_data=f"admin_users_page_{page+1}",
            icon_custom_emoji_id="5893057118545646106"
        ))

    if nav_buttons:
        buttons.append(nav_buttons)

    buttons.append([InlineKeyboardButton(
        text="В меню админа",
        callback_data="admin_back",
        icon_custom_emoji_id="5893057118545646106"
    )])

    return InlineKeyboardMarkup(inline_keyboard=buttons)

def edit_user_kb(user_id: int):
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(
            text="Пополнить баланс",
            callback_data=f"admin_edit_balance_{user_id}",
            icon_custom_emoji_id="5904462880941545555"
        )],
        [InlineKeyboardButton(
            text="История ставок",
            callback_data=f"admin_user_history_{user_id}",
            icon_custom_emoji_id="5870930636742595124"
        )],
        [InlineKeyboardButton(
            text="Назад к списку",
            callback_data="admin_users_back",
            icon_custom_emoji_id="5893057118545646106"
        )],
    ])


@dp.message(CommandStart())
async def cmd_start(msg: Message, state: FSMContext):
    bal = get_or_create_user(msg.from_user.id, msg.from_user.username or "игрок")
    await state.clear()
    text = (
        f'<tg-emoji emoji-id="5258882890059091157">🎰</tg-emoji> <b>Добро пожаловать в Казино!</b>\n\n'
        f'<tg-emoji emoji-id="5904462880941545555">🪙</tg-emoji> Ваш баланс: <b>{bal} монет</b>\n\n'
        f'<tg-emoji emoji-id="5778672437122045013">📦</tg-emoji> Нажми на:\n'
        f'<blockquote>"Рулетка" или "Орёл или Решка" или "Ракета" или "Сапер" чтобы играть</blockquote>'
    )
    await msg.answer(text, parse_mode="HTML", reply_markup=main_menu_kb())


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


@dp.callback_query(F.data == "back_main")
async def back_main(cq: CallbackQuery, state: FSMContext):
    await state.clear()
    bal = get_balance(cq.from_user.id)
    text = (
        f'<tg-emoji emoji-id="5258882890059091157">🎰</tg-emoji> <b>Казино</b>\n'
        f'<tg-emoji emoji-id="5904462880941545555">🪙</tg-emoji> Баланс: <b>{bal} монет</b>'
    )
    await cq.message.edit_text(text, parse_mode="HTML", reply_markup=main_menu_kb())


@dp.callback_query(F.data == "balance")
async def show_balance(cq: CallbackQuery):
    bal = get_balance(cq.from_user.id)
    await cq.answer(f'Ваш баланс: {bal} монет', show_alert=True)


@dp.callback_query(F.data == "stats")
async def show_stats(cq: CallbackQuery):
    row = get_user(cq.from_user.id)
    if not row:
        await cq.answer("Сначала запустите /start", show_alert=True); return
    _, username, balance, wins, losses = row
    total = wins + losses
    rate = round(wins / total * 100, 1) if total else 0
    text = (
        f'<tg-emoji emoji-id="5870921681735781843">📊</tg-emoji> <b>Статистика</b>\n\n'
        f'<tg-emoji emoji-id="5870994129244131212">👤</tg-emoji> Игрок: <b>{username or "Неизвестно"}</b>\n'
        f'<tg-emoji emoji-id="5904462880941545555">🪙</tg-emoji> Баланс: <b>{balance}</b> монет\n'
        f'<tg-emoji emoji-id="5870633910337015697">✅</tg-emoji> Побед: <b>{wins}</b>\n'
        f'<tg-emoji emoji-id="5870657884844462243">❌</tg-emoji> Поражений: <b>{losses}</b>\n'
        f'<tg-emoji emoji-id="5870930636742595124">📊</tg-emoji> Процент побед: <b>{rate}%</b>'
    )
    await cq.message.edit_text(text, parse_mode="HTML", reply_markup=stats_menu_kb())


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
            if entry['game_type'] == "coin":
                game_name = "Монета"
            elif entry['game_type'] == "rocket":
                game_name = "Ракета"
            elif entry['game_type'] == "minesweeper":
                game_name = "Сапер"
            elif entry['game_type'] == "admin_topup":
                game_name = "Пополнение"
            else:
                game_name = "Рулетка"
            sign = '+' if entry['is_win'] else '-'
            text += f'{status_emoji} {sign}{entry["amount"]} - {game_name}\n'
    stats = get_user_history_stats(user_id)
    if stats and (stats['win_count'] or stats['lose_count']):
        total_won = stats['total_won'] or 0
        total_lost = stats['total_lost'] or 0
        text += f'<tg-emoji emoji-id="5870633910337015697">✅</tg-emoji> Выигрыши: <b>+{total_won}</b>\n'
        text += f'<tg-emoji emoji-id="5870657884844462243">❌</tg-emoji> Проигрыши: <b>-{total_lost}</b>\n'
        net = total_won - total_lost
        if net > 0:
            text += f'<tg-emoji emoji-id="5870633910337015697">✅</tg-emoji> Итог: <b>+{net}</b>'
        elif net < 0:
            text += f'<tg-emoji emoji-id="5870657884844462243">❌</tg-emoji> Итог: <b>{net}</b>'
        else:
            text += f'<tg-emoji emoji-id="5904462880941545555">🪙</tg-emoji> Итог: <b>0</b>'

    await cq.message.edit_text(text, parse_mode="HTML", reply_markup=InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(
            text="Назад в статистику",
            callback_data="stats",
            icon_custom_emoji_id="5893057118545646106"
        )],
    ]))


@dp.callback_query(F.data == "donate")
async def open_donate(cq: CallbackQuery):
    text = (
        '<tg-emoji emoji-id="6032644646587338669">🎁</tg-emoji> <b>Накормить автора</b>\n\n'
        'Выберите количество звезд для поддержки разработки\n'
        '(звезды идут разработчику, спасибо! 💙)'
    )
    await cq.message.edit_text(text, parse_mode="HTML", reply_markup=donate_kb())


@dp.callback_query(F.data.startswith("donate_") & (F.data != "donate_custom"))
async def process_donation(cq: CallbackQuery):
    amount_str = cq.data.split("_")[1]
    try:
        amount = int(amount_str)
    except:
        await cq.answer(
            '<tg-emoji emoji-id="5870657884844462243">❌</tg-emoji> Некорректная сумма',
            show_alert=True
        )
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
    await cq.message.edit_text(
        '<tg-emoji emoji-id="5870676941614354370">🖋</tg-emoji> <b>Введите сумму в звездах (⭐)</b>\n\n'
        'Минимум: 1 ⭐, максимум: 10000 ⭐',
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(
                text="Назад",
                callback_data="donate",
                icon_custom_emoji_id="5893057118545646106"
            )]
        ])
    )


@dp.message(DonateState.entering_custom_amount)
async def process_custom_donate_amount(msg: Message, state: FSMContext):
    try:
        amount = int(msg.text.strip())
        if amount < 1 or amount > 10000:
            await msg.answer(
                '<tg-emoji emoji-id="5870657884844462243">❌</tg-emoji> <b>Ошибка!</b>\n\n'
                'Сумма должна быть от 1 до 10000 ⭐',
                parse_mode="HTML"
            )
            return
    except:
        await msg.answer(
            '<tg-emoji emoji-id="5870657884844462243">❌</tg-emoji> <b>Ошибка!</b>\n\n'
            'Введите целое число от 1 до 10000',
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
    await cq.answer('Баланс сброшен до 1000 монет!', show_alert=True)
    await cq.message.edit_text(
        f'<tg-emoji emoji-id="5345906554510012647">🔄</tg-emoji> <b>Баланс сброшен!</b>\n'
        f'<tg-emoji emoji-id="5904462880941545555">🪙</tg-emoji> Новый баланс: <b>1000 монет</b>',
        parse_mode="HTML",
        reply_markup=main_menu_kb()
    )


@dp.pre_checkout_query()
async def process_pre_checkout_query(pre_checkout_query: PreCheckoutQuery):
    await bot.answer_pre_checkout_query(pre_checkout_query.id, ok=True)


@dp.message(lambda msg: msg.successful_payment is not None)
async def process_successful_payment(msg: Message):
    payment = msg.successful_payment
    payload = payment.invoice_payload
    if not payload.startswith("donate_"):
        return

    amount_str = payload.split("_")[1]
    amount = int(amount_str)

    text = (
        f'<tg-emoji emoji-id="6041731551845159060">🎉</tg-emoji> <b>Спасибо за {amount} звезд!</b>\n\n'
        f'Ваша поддержка очень важна для развития бота! 💙'
    )
    await msg.answer(text, parse_mode="HTML", reply_markup=main_menu_kb())


@dp.callback_query(F.data == "open_roulette")
async def open_roulette(cq: CallbackQuery, state: FSMContext):
    bal = get_balance(cq.from_user.id)
    if bal <= 0:
        await cq.answer(
            'У вас нет монет! Сбросьте баланс.',
            show_alert=True
        )
        return
    await state.set_state(BetState.choosing_bet_type)
    text = (
        f'<tg-emoji emoji-id="5258882890059091157">🎰</tg-emoji> <b>Европейская Рулетка</b>\n'
        f'<tg-emoji emoji-id="5904462880941545555">🪙</tg-emoji> Баланс: <b>{bal} монет</b>\n\n'
        f'<b>Выберите тип ставки:</b>'
    )
    await cq.message.edit_text(text, parse_mode="HTML", reply_markup=bet_type_kb())


@dp.callback_query(BetState.choosing_bet_type, F.data.startswith("bet_"))
async def choose_bet_type(cq: CallbackQuery, state: FSMContext):
    raw = cq.data[4:]

    if raw == "number":
        await state.update_data(bet_type="pending_number")
        await state.set_state(BetState.choosing_amount)
        await cq.message.edit_text(
            '<tg-emoji emoji-id="5771851822897566479">🔡</tg-emoji> <b>Введите число от 0 до 36:</b>',
            parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(
                    text="Назад",
                    callback_data="back_bet_type",
                    icon_custom_emoji_id="5893057118545646106"
                )]
            ])
        )
        return

    await state.update_data(bet_type=raw)
    await state.set_state(BetState.choosing_amount)
    bal = get_balance(cq.from_user.id)
    label = BET_LABELS.get(raw, raw)
    text = (
        f'<tg-emoji emoji-id="5258882890059091157">🎰</tg-emoji> <b>Ставка: {label}</b>\n'
        f'<tg-emoji emoji-id="5904462880941545555">🪙</tg-emoji> Баланс: <b>{bal} монет</b>\n\n'
        f'<b>Выберите сумму ставки:</b>'
    )
    await cq.message.edit_text(text, parse_mode="HTML", reply_markup=bet_amount_kb(bal))


@dp.callback_query(F.data == "back_bet_type")
async def back_bet_type(cq: CallbackQuery, state: FSMContext):
    await state.set_state(BetState.choosing_bet_type)
    bal = get_balance(cq.from_user.id)
    text = (
        f'<tg-emoji emoji-id="5258882890059091157">🎰</tg-emoji> <b>Европейская Рулетка</b>\n'
        f'<tg-emoji emoji-id="5904462880941545555">🪙</tg-emoji> Баланс: <b>{bal} монет</b>\n\n'
        f'<b>Выберите тип ставки:</b>'
    )
    await cq.message.edit_text(text, parse_mode="HTML", reply_markup=bet_type_kb())


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
        won  = check_bet(bet_type, result)
        mult = payout_multiplier(bet_type)
        color = number_color(result)
        if won:
            profit = amount * mult
            update_balance(msg.from_user.id, profit, win=True, game_type="roulette")
            outcome_text = (
                f'<tg-emoji emoji-id="6041731551845159060">🎉</tg-emoji> <b>ПОБЕДА!</b>\n'
                f'<tg-emoji emoji-id="5890848474563352982">🪙</tg-emoji> +{profit} монет (x{mult})'
            )
        else:
            update_balance(msg.from_user.id, -amount, win=False, game_type="roulette")
            outcome_text = (
                f'<tg-emoji emoji-id="5870657884844462243">❌</tg-emoji> <b>Поражение.</b>\n'
                f'<tg-emoji emoji-id="5904462880941545555">🪙</tg-emoji> -{amount} монет'
            )
        new_bal = get_balance(msg.from_user.id)
        label = BET_LABELS.get(bet_type, bet_type.replace("num_", "число "))
        text = (
            f'<tg-emoji emoji-id="5258882890059091157">🎰</tg-emoji> <b>Шарик остановился на:</b> {color} <b>{result}</b>\n\n'
            f'🎲 Ваша ставка: <b>{label}</b> — <b>{amount}</b> монет\n'
            f'{outcome_text}\n\n'
            f'<tg-emoji emoji-id="5904462880941545555">🪙</tg-emoji> Новый баланс: <b>{new_bal} монет</b>'
        )
        await state.clear()
        if new_bal <= 0:
            text += (
                f'\n\n<tg-emoji emoji-id="5870657884844462243">❌</tg-emoji> '
                f'<b>Вы банкрот!</b> Нажмите «Сбросить баланс».'
            )
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
        f'<tg-emoji emoji-id="5904462880941545555">🪙</tg-emoji> Баланс: <b>{bal} монет</b>\n\n<b>Выберите сумму ставки:</b>',
        parse_mode="HTML",
        reply_markup=bet_amount_kb(bal)
    )


@dp.callback_query(BetState.choosing_amount, F.data == "amount_custom")
async def ask_custom_amount(cq: CallbackQuery, state: FSMContext):
    await state.update_data(waiting_custom=True)
    bal = get_balance(cq.from_user.id)
    await cq.message.edit_text(
        f'<tg-emoji emoji-id="5870676941614354370">🖋</tg-emoji> <b>Введите сумму ставки:</b>\n'
        f'<tg-emoji emoji-id="5904462880941545555">🪙</tg-emoji> Баланс: <b>{bal} монет</b>',
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(
                text="Назад",
                callback_data="back_bet_type",
                icon_custom_emoji_id="5893057118545646106"
            )]
        ])
    )

@dp.callback_query(BetState.choosing_amount, F.data.startswith("amount_"))
async def place_bet(cq: CallbackQuery, state: FSMContext):
    amount = int(cq.data.split("_")[1])
    data   = await state.get_data()
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
            f'<tg-emoji emoji-id="5890848474563352982">🪙</tg-emoji> +{profit} монет (x{mult})'
        )
    else:
        update_balance(cq.from_user.id, -amount, win=False, game_type="roulette")
        outcome_text = (
            f'<tg-emoji emoji-id="5870657884844462243">❌</tg-emoji> <b>Поражение.</b>\n'
            f'<tg-emoji emoji-id="5904462880941545555">🪙</tg-emoji> -{amount} монет'
        )

    new_bal = get_balance(cq.from_user.id)
    label   = BET_LABELS.get(bet_type, bet_type.replace("num_", "число "))

    text = (
        f'<tg-emoji emoji-id="5258882890059091157">🎰</tg-emoji> <b>Шарик остановился на:</b> {color} <b>{result}</b>\n\n'
        f'🎲 Ваша ставка: <b>{label}</b> — <b>{amount}</b> монет\n'
        f'{outcome_text}\n\n'
        f'<tg-emoji emoji-id="5904462880941545555">🪙</tg-emoji> Новый баланс: <b>{new_bal} монет</b>'
    )

    await state.clear()

    if new_bal <= 0:
        text += (
            f'\n\n<tg-emoji emoji-id="5870657884844462243">❌</tg-emoji> '
            f'<b>Вы банкрот!</b> Нажмите «Сбросить баланс».'
        )

    await cq.message.edit_text(text, parse_mode="HTML", reply_markup=game_result_kb("roulette"))


@dp.callback_query(F.data == "open_coin")
async def open_coin(cq: CallbackQuery, state: FSMContext):
    bal = get_balance(cq.from_user.id)
    if bal <= 0:
        await cq.answer('У вас нет монет! Сбросьте баланс.', show_alert=True)
        return
    await state.set_state(CoinState.choosing_side)
    text = (
        f'<tg-emoji emoji-id="5774585885154131652">🪙</tg-emoji> <b>Орёл или Решка</b>\n'
        f'<tg-emoji emoji-id="5904462880941545555">🪙</tg-emoji> Баланс: <b>{bal} монет</b>\n\n'
        f'<b>Выберите сторону монеты:</b>'
    )
    await cq.message.edit_text(text, parse_mode="HTML", reply_markup=coin_side_kb())


@dp.callback_query(CoinState.choosing_side, F.data.startswith("coin_"))
async def choose_coin_side(cq: CallbackQuery, state: FSMContext):
    side_raw = cq.data.split("_")[1]
    side_label = "Орёл" if side_raw == "heads" else "Решка"

    await state.update_data(coin_choice=side_raw)
    await state.set_state(CoinState.choosing_amount)
    bal = get_balance(cq.from_user.id)
    text = (
        f'<tg-emoji emoji-id="5774585885154131652">🪙</tg-emoji> <b>Ставка: {side_label}</b>\n'
        f'<tg-emoji emoji-id="5904462880941545555">🪙</tg-emoji> Баланс: <b>{bal} монет</b>\n\n'
        f'<b>Выберите сумму ставки:</b>'
    )
    await cq.message.edit_text(text, parse_mode="HTML", reply_markup=bet_amount_kb(bal))


@dp.callback_query(CoinState.choosing_amount, F.data == "amount_custom")
async def ask_custom_coin_amount(cq: CallbackQuery, state: FSMContext):
    await state.update_data(waiting_custom=True)
    bal = get_balance(cq.from_user.id)
    await cq.message.edit_text(
        f'<tg-emoji emoji-id="5870676941614354370">🖋</tg-emoji> <b>Введите сумму ставки:</b>\n'
        f'<tg-emoji emoji-id="5904462880941545555">🪙</tg-emoji> Баланс: <b>{bal} монет</b>',
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(
                text="Назад",
                callback_data="back_main",
                icon_custom_emoji_id="5893057118545646106"
            )]
        ])
    )


@dp.message(CoinState.choosing_amount)
async def handle_coin_amount_input(msg: Message, state: FSMContext):
    data = await state.get_data()
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
        won = check_coin_bet(coin_choice, result)

        side_label   = "Орёл" if coin_choice == "heads" else "Решка"
        result_label = "Орёл" if result == "heads" else "Решка"

        if won:
            profit = amount * 2
            update_balance(msg.from_user.id, profit, win=True, game_type="coin")
            outcome_text = (
                f'<tg-emoji emoji-id="6041731551845159060">🎉</tg-emoji> <b>ПОБЕДА!</b>\n'
                f'<tg-emoji emoji-id="5890848474563352982">🪙</tg-emoji> +{profit} монет (x2)'
            )
        else:
            update_balance(msg.from_user.id, -amount, win=False, game_type="coin")
            outcome_text = (
                f'<tg-emoji emoji-id="5870657884844462243">❌</tg-emoji> <b>Поражение.</b>\n'
                f'<tg-emoji emoji-id="5904462880941545555">🪙</tg-emoji> -{amount} монет'
            )

        new_bal = get_balance(msg.from_user.id)
        text = (
            f'<tg-emoji emoji-id="5774585885154131652">🪙</tg-emoji> <b>Результат:</b> {result_label}\n\n'
            f'🎲 Ваша ставка: <b>{side_label}</b> — <b>{amount}</b> монет\n'
            f'{outcome_text}\n\n'
            f'<tg-emoji emoji-id="5904462880941545555">🪙</tg-emoji> Новый баланс: <b>{new_bal} монет</b>'
        )

        await state.clear()

        if new_bal <= 0:
            text += (
                f'\n\n<tg-emoji emoji-id="5870657884844462243">❌</tg-emoji> '
                f'<b>Вы банкрот!</b> Нажмите «Сбросить баланс».'
            )

        await msg.answer(text, parse_mode="HTML", reply_markup=game_result_kb("coin"))
        return


@dp.callback_query(CoinState.choosing_amount, F.data.startswith("amount_"))
async def place_coin_bet(cq: CallbackQuery, state: FSMContext):
    amount = int(cq.data.split("_")[1])
    data = await state.get_data()
    coin_choice = data.get("coin_choice", "")

    if not coin_choice:
        await cq.answer('Сначала выберите сторону монеты.', show_alert=True)
        return

    bal = get_balance(cq.from_user.id)
    if amount > bal:
        await cq.answer('Недостаточно средств!', show_alert=True)
        return

    last_bets[cq.from_user.id] = {'game': 'coin', 'choice': coin_choice, 'amount': amount}

    result = flip_coin()
    won = check_coin_bet(coin_choice, result)

    side_label   = "Орёл" if coin_choice == "heads" else "Решка"
    result_label = "Орёл" if result == "heads" else "Решка"

    if won:
        profit = amount * 2
        update_balance(cq.from_user.id, profit, win=True, game_type="coin")
        outcome_text = (
            f'<tg-emoji emoji-id="6041731551845159060">🎉</tg-emoji> <b>ПОБЕДА!</b>\n'
            f'<tg-emoji emoji-id="5890848474563352982">🪙</tg-emoji> +{profit} монет (x2)'
        )
    else:
        update_balance(cq.from_user.id, -amount, win=False, game_type="coin")
        outcome_text = (
            f'<tg-emoji emoji-id="5870657884844462243">❌</tg-emoji> <b>Поражение.</b>\n'
            f'<tg-emoji emoji-id="5904462880941545555">🪙</tg-emoji> -{amount} монет'
        )

    new_bal = get_balance(cq.from_user.id)
    text = (
        f'<tg-emoji emoji-id="5774585885154131652">🪙</tg-emoji> <b>Результат:</b> {result_label}\n\n'
        f'🎲 Ваша ставка: <b>{side_label}</b> — <b>{amount}</b> монет\n'
        f'{outcome_text}\n\n'
        f'<tg-emoji emoji-id="5904462880941545555">🪙</tg-emoji> Новый баланс: <b>{new_bal} монет</b>'
    )

    await state.clear()

    if new_bal <= 0:
        text += (
            f'\n\n<tg-emoji emoji-id="5870657884844462243">❌</tg-emoji> '
            f'<b>Вы банкрот!</b> Нажмите «Сбросить баланс».'
        )

    await cq.message.edit_text(text, parse_mode="HTML", reply_markup=game_result_kb("coin"))


@dp.callback_query(F.data == "repeat_roulette")
async def repeat_roulette(cq: CallbackQuery, state: FSMContext):
    user_id = cq.from_user.id

    if user_id not in last_bets or last_bets[user_id].get('game') != 'roulette':
        await cq.answer("Нет сохраненной ставки, выберите новую.", show_alert=False)
        return

    bet_data = last_bets[user_id]
    bet_type = bet_data.get('bet_type')
    amount = bet_data.get('amount')

    bal = get_balance(user_id)
    if amount > bal:
        await cq.answer(
            f'Недостаточно монет! Нужно {amount}, а у вас {bal}.',
            show_alert=True
        )
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
            f'<tg-emoji emoji-id="5890848474563352982">🪙</tg-emoji> +{profit} монет (x{mult})'
        )
    else:
        update_balance(user_id, -amount, win=False, game_type="roulette")
        outcome_text = (
            f'<tg-emoji emoji-id="5870657884844462243">❌</tg-emoji> <b>Поражение.</b>\n'
            f'<tg-emoji emoji-id="5904462880941545555">🪙</tg-emoji> -{amount} монет'
        )

    new_bal = get_balance(user_id)
    label   = BET_LABELS.get(bet_type, bet_type.replace("num_", "число "))

    text = (
        f'<tg-emoji emoji-id="5258882890059091157">🎰</tg-emoji> <b>Шарик остановился на:</b> {color} <b>{result}</b>\n\n'
        f'🎲 Ваша ставка: <b>{label}</b> — <b>{amount}</b> монет\n'
        f'{outcome_text}\n\n'
        f'<tg-emoji emoji-id="5904462880941545555">🪙</tg-emoji> Новый баланс: <b>{new_bal} монет</b>'
    )

    if new_bal <= 0:
        text += (
            f'\n\n<tg-emoji emoji-id="5870657884844462243">❌</tg-emoji> '
            f'<b>Вы банкрот!</b> Нажмите «Сбросить баланс».'
        )

    await cq.message.edit_text(text, parse_mode="HTML", reply_markup=game_result_kb("roulette"))


@dp.callback_query(F.data == "repeat_coin")
async def repeat_coin(cq: CallbackQuery, state: FSMContext):
    user_id = cq.from_user.id

    if user_id not in last_bets or last_bets[user_id].get('game') != 'coin':
        await cq.answer("Нет сохраненной ставки, выберите новую.", show_alert=False)
        return

    bet_data = last_bets[user_id]
    coin_choice = bet_data.get('choice')
    amount = bet_data.get('amount')

    bal = get_balance(user_id)
    if amount > bal:
        await cq.answer(
            f'Недостаточно монет! Нужно {amount}, а у вас {bal}.',
            show_alert=True
        )
        return

    result = flip_coin()
    won = check_coin_bet(coin_choice, result)

    side_label   = "Орёл" if coin_choice == "heads" else "Решка"
    result_label = "Орёл" if result == "heads" else "Решка"

    if won:
        profit = amount * 2
        update_balance(user_id, profit, win=True, game_type="coin")
        outcome_text = (
            f'<tg-emoji emoji-id="6041731551845159060">🎉</tg-emoji> <b>ПОБЕДА!</b>\n'
            f'<tg-emoji emoji-id="5890848474563352982">🪙</tg-emoji> +{profit} монет (x2)'
        )
    else:
        update_balance(user_id, -amount, win=False, game_type="coin")
        outcome_text = (
            f'<tg-emoji emoji-id="5870657884844462243">❌</tg-emoji> <b>Поражение.</b>\n'
            f'<tg-emoji emoji-id="5904462880941545555">🪙</tg-emoji> -{amount} монет'
        )

    new_bal = get_balance(user_id)
    text = (
        f'<tg-emoji emoji-id="5774585885154131652">🪙</tg-emoji> <b>Результат:</b> {result_label}\n\n'
        f'🎲 Ваша ставка: <b>{side_label}</b> — <b>{amount}</b> монет\n'
        f'{outcome_text}\n\n'
        f'<tg-emoji emoji-id="5904462880941545555">🪙</tg-emoji> Новый баланс: <b>{new_bal} монет</b>'
    )

    if new_bal <= 0:
        text += (
            f'\n\n<tg-emoji emoji-id="5870657884844462243">❌</tg-emoji> '
            f'<b>Вы банкрот!</b> Нажмите «Сбросить баланс».'
        )

    await cq.message.edit_text(text, parse_mode="HTML", reply_markup=game_result_kb("coin"))


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
    await cq.message.edit_text(text, parse_mode="HTML", reply_markup=users_list_kb(users, 0))


@dp.callback_query(F.data.startswith("admin_users_page_"))
async def paginate_users(cq: CallbackQuery, state: FSMContext):
    if cq.from_user.id != ADMIN_ID:
        return

    page = int(cq.data.split("_")[-1])
    users = get_all_users_full()

    text = (
        f'<tg-emoji emoji-id="5870772616305839506">👥</tg-emoji> <b>Список пользователей:</b>\n\n'
        f'Всего: {len(users)} пользователей\n\n'
        f'Выберите пользователя для редактирования:'
    )
    await cq.message.edit_text(text, parse_mode="HTML", reply_markup=users_list_kb(users, page))


@dp.callback_query(F.data.startswith("admin_edit_user_"))
async def edit_user_menu(cq: CallbackQuery, state: FSMContext):
    if cq.from_user.id != ADMIN_ID:
        return

    user_id = int(cq.data.split("_")[-1])
    row = get_user(user_id)

    if not row:
        await cq.answer('Пользователь не найден', show_alert=True)
        return

    _, username, balance, wins, losses = row
    text = (
        f'<tg-emoji emoji-id="5870994129244131212">👤</tg-emoji> <b>Профиль пользователя:</b>\n\n'
        f'ID: <code>{user_id}</code>\n'
        f'Имя: <b>{username or "Неизвестно"}</b>\n'
        f'<tg-emoji emoji-id="5904462880941545555">🪙</tg-emoji> Баланс: <b>{balance}</b> монет\n'
        f'<tg-emoji emoji-id="5870633910337015697">✅</tg-emoji> Побед: <b>{wins}</b>\n'
        f'<tg-emoji emoji-id="5870657884844462243">❌</tg-emoji> Поражений: <b>{losses}</b>\n\n'
        f'Выберите действие:'
    )

    await state.update_data(admin_user_id=user_id)
    await cq.message.edit_text(text, parse_mode="HTML", reply_markup=edit_user_kb(user_id))


@dp.callback_query(F.data.startswith("admin_user_history_"))
async def admin_show_user_history(cq: CallbackQuery):
    if cq.from_user.id != ADMIN_ID:
        return

    user_id = int(cq.data.split("_")[-1])
    row = get_user(user_id)

    if not row:
        await cq.answer('Пользователь не найден', show_alert=True)
        return

    username = row[1]
    history = get_history(user_id, limit=15)

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
            if entry['game_type'] == "coin":
                game_name = "Монета"
            elif entry['game_type'] == "rocket":
                game_name = "Ракета"
            elif entry['game_type'] == "minesweeper":
                game_name = "Сапер"
            elif entry['game_type'] == "admin_topup":
                game_name = "Пополнение от админа"
            else:
                game_name = "Рулетка"
            sign = '+' if entry['is_win'] else '-'
            text += f'{status_emoji} {sign}{entry["amount"]} - {game_name}\n'
    stats = get_user_history_stats(user_id)
    if stats and (stats['win_count'] or stats['lose_count']):
        total_won = stats['total_won'] or 0
        total_lost = stats['total_lost'] or 0
        text += f'<tg-emoji emoji-id="5870633910337015697">✅</tg-emoji> Выигрыши: <b>+{total_won}</b>\n'
        text += f'<tg-emoji emoji-id="5870657884844462243">❌</tg-emoji> Проигрыши: <b>-{total_lost}</b>\n'
        net = total_won - total_lost
        if net > 0:
            text += f'<tg-emoji emoji-id="5870633910337015697">✅</tg-emoji> Итог: <b>+{net}</b>'
        elif net < 0:
            text += f'<tg-emoji emoji-id="5870657884844462243">❌</tg-emoji> Итог: <b>{net}</b>'
        else:
            text += f'<tg-emoji emoji-id="5904462880941545555">🪙</tg-emoji> Итог: <b>0</b>'

    await cq.message.edit_text(text, parse_mode="HTML", reply_markup=InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(
            text="Назад к пользователю",
            callback_data=f"admin_edit_user_{user_id}",
            icon_custom_emoji_id="5893057118545646106"
        )],
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
        f'<tg-emoji emoji-id="5904462880941545555">🪙</tg-emoji> Текущий баланс: <b>{current_balance}</b> монет\n\n'
        f'Введите сумму которую хотите <b>добавить</b>:\n'
    )

    await cq.message.edit_text(text, parse_mode="HTML", reply_markup=InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(
            text="Назад",
            callback_data="admin_users_back",
            icon_custom_emoji_id="5893057118545646106"
        )]
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
 
    data = await state.get_data()
    user_id = data.get("admin_user_id")
 
    old_balance = get_balance(user_id)
    new_total = old_balance + new_balance
    set_balance(user_id, new_total)
    add_history(user_id, new_balance, True, game_type="admin_topup")
 
    user = get_user(user_id)
    _, username, _, _, _ = user
 
    text = (
        f'<tg-emoji emoji-id="5870633910337015697">✅</tg-emoji> <b>Баланс обновлён!</b>\n\n'
        f'<tg-emoji emoji-id="5870994129244131212">👤</tg-emoji> Пользователь: <b>{username or "Неизвестно"}</b> (ID: {user_id})\n'
        f'<tg-emoji emoji-id="5904462880941545555">🪙</tg-emoji> Было: <b>{old_balance}</b> монет\n'
        f'<tg-emoji emoji-id="5890848474563352982">🪙</tg-emoji> Добавлено: <b>+{new_balance}</b> монет\n'
        f'<tg-emoji emoji-id="5870633910337015697">✅</tg-emoji> Итого: <b>{new_total}</b> монет'
    )
 
    await state.clear()
    await msg.answer(text, parse_mode="HTML", reply_markup=admin_menu_kb())
 
    try:
        await bot.send_message(
            user_id,
            f'<tg-emoji emoji-id="5904462880941545555">🪙</tg-emoji> <b>Баланс пополнен!</b>\n\n'
            f'<tg-emoji emoji-id="5890848474563352982">🪙</tg-emoji> Добавлено: <b>+{new_balance}</b> монет\n'
            f'<tg-emoji emoji-id="5870633910337015697">✅</tg-emoji> Текущий баланс: <b>{new_total}</b> монет',
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
    await cq.message.edit_text(text, parse_mode="HTML", reply_markup=InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(
            text="Отмена",
            callback_data="admin_back",
            icon_custom_emoji_id="5870657884844462243"
        )]
    ]))


@dp.message(AdminState.sending_broadcast)
async def process_broadcast(msg: Message, state: FSMContext):
    if msg.from_user.id != ADMIN_ID:
        return

    broadcast_text = msg.text
    users = get_all_users_full()

    success_count = 0
    fail_count = 0

    for user in users:
        user_id = user['user_id']
        try:
            await bot.send_message(
                user_id,
                broadcast_text,
                parse_mode="HTML"
            )
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
    await cq.message.edit_text(text, parse_mode="HTML", reply_markup=admin_menu_kb())


@dp.callback_query(F.data == "admin_clear_history")
async def admin_clear_history(cq: CallbackQuery):
    if cq.from_user.id != ADMIN_ID:
        return

    clear_all_history()
    text = (
        '✅ <b>История ставок очищена!</b>\n\n'
        'Все записи из таблицы history удалены.'
    )
    await cq.message.edit_text(text, parse_mode="HTML", reply_markup=InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(
            text="Назад в админ-панель",
            callback_data="admin_back",
            icon_custom_emoji_id="5893057118545646106"
        )],
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
    await cq.message.edit_text(text, parse_mode="HTML", reply_markup=users_list_kb(users, 0))


async def daily_bonus_task():
    msk = pytz.timezone("Europe/Moscow")
    while True:
        now = datetime.now(msk)
        target = now.replace(hour=12, minute=0, second=0, microsecond=0)
        if now >= target:
            target += timedelta(days=1)
        wait_seconds = (target - now).total_seconds()
        await asyncio.sleep(wait_seconds)

        users = get_all_users()
        for user_id in users:
            add_daily_bonus(user_id)
            new_bal = get_balance(user_id)
            try:
                await bot.send_message(
                    user_id,
                    f'<tg-emoji emoji-id="6032644646587338669">🎁</tg-emoji> <b>Ежедневный бонус!</b>\n\n'
                    f'<tg-emoji emoji-id="5890848474563352982">🪙</tg-emoji> Вам начислено <b>+500 монет</b>\n'
                    f'<tg-emoji emoji-id="5904462880941545555">🪙</tg-emoji> Текущий баланс: <b>{new_bal} монет</b>\n\n'
                    f'<i>Удачной игры!</i>',
                    parse_mode="HTML"
                )
            except Exception:
                pass

        await asyncio.sleep(60)

def generate_crash_point() -> float:
    """Генерирует точку краша. Чем выше множитель — тем реже."""
    r = random.random()
    if r < 0.40:
        return round(random.uniform(1.0, 1.5), 2)   # 40% — краш до 1.5x
    elif r < 0.65:
        return round(random.uniform(1.5, 2.5), 2)   # 25% — краш 1.5–2.5x
    elif r < 0.80:
        return round(random.uniform(2.5, 4.0), 2)   # 15% — краш 2.5–4x
    elif r < 0.92:
        return round(random.uniform(4.0, 8.0), 2)   # 12% — краш 4–8x
    else:
        return round(random.uniform(8.0, 20.0), 2)  # 8%  — краш 8–20x

def next_multiplier(current: float) -> float:
    """Следующий шаг множителя — случайный прирост."""
    step = round(random.uniform(0.1, 0.6), 2)
    return round(current + step, 2)


def generate_minesweeper_field(size: int = 5, mines: int = 5):
    """Генерирует поле Сапера. Возвращает поле (True=миния, False=пусто)."""
    field = [[False for _ in range(size)] for _ in range(size)]
    placed = 0
    while placed < mines:
        r, c = random.randint(0, size-1), random.randint(0, size-1)
        if not field[r][c]:
            field[r][c] = True
            placed += 1
    return field


def check_minesweeper_cell(field: list, row: int, col: int) -> tuple:
    """Проверяет ячейку. Возвращает (is_mine, count_nearby_mines)"""
    size = len(field)
    if field[row][col]:
        return True, 0
    
    count = 0
    for dr in [-1, 0, 1]:
        for dc in [-1, 0, 1]:
            if dr == 0 and dc == 0:
                continue
            nr, nc = row + dr, col + dc
            if 0 <= nr < size and 0 <= nc < size and field[nr][nc]:
                count += 1
    return False, count


@dp.callback_query(F.data == "open_minesweeper")
async def open_minesweeper(cq: CallbackQuery, state: FSMContext):
    user_id = cq.from_user.id
    bal = get_balance(user_id)
    if bal <= 0:
        await cq.answer("У вас нет монет! Сбросьте баланс.", show_alert=True)
        return
    await state.set_state(MinesweeperState.choosing_amount)
    text = (
        f"💣 <b>Сапер</b>\n"
        f"💰 Баланс: <b>{bal} монет</b>\n\n"
        f"Открывай ячейки и не попадись на мину!\n"
        f"Чем больше ячеек откроешь — тем выше множитель.\n\n"
        f"<b>Выбери сумму ставки:</b>"
    )
    await cq.message.edit_text(text, parse_mode="HTML", reply_markup=bet_amount_kb(bal))


@dp.callback_query(MinesweeperState.choosing_amount, F.data.startswith("amount_"))
async def minesweeper_set_amount(cq: CallbackQuery, state: FSMContext):
    user_id = cq.from_user.id
    raw = cq.data.replace("amount_", "")
    amount = int(raw)
    bal = get_balance(user_id)
    if amount > bal:
        await cq.answer("Недостаточно средств!", show_alert=True)
        return
    
    field = generate_minesweeper_field(size=5, mines=5)
    revealed = [[False for _ in range(5)] for _ in range(5)]
    
    await state.set_state(MinesweeperState.in_game)
    await state.update_data(
        minesweeper_amount=amount,
        minesweeper_field=field,
        minesweeper_revealed=revealed,
        minesweeper_opened=0
    )
    
    text = _minesweeper_text(amount, field, revealed)
    await cq.message.edit_text(text, parse_mode="HTML", reply_markup=_minesweeper_kb(revealed))
    await cq.answer()


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
    
    field = generate_minesweeper_field(size=5, mines=5)
    revealed = [[False for _ in range(5)] for _ in range(5)]
    
    await state.set_state(MinesweeperState.in_game)
    await state.update_data(
        minesweeper_amount=amount,
        minesweeper_field=field,
        minesweeper_revealed=revealed,
        minesweeper_opened=0,
        waiting_custom=False
    )
    
    text = _minesweeper_text(amount, field, revealed)
    await msg.answer(text, parse_mode="HTML", reply_markup=_minesweeper_kb(revealed))


def _minesweeper_text(amount: int, field: list, revealed: list) -> str:
    multiplier = 1.0 + (sum(sum(row) for row in revealed) * 0.3)
    potential = int(amount * multiplier)
    text = (
        f"💣 <b>Сапер</b>\n\n"
        f"💸 Ставка: <b>{amount} монет</b>\n"
        f"📈 Множитель: <b>x{multiplier:.2f}</b>\n"
        f"💰 Можно забрать: <b>{potential} монет</b>\n\n"
        f"<b>Откроется ячейки:</b>\n"
    )
    
    for row_idx, row in enumerate(revealed):
        for col_idx, is_revealed in enumerate(row):
            if is_revealed:
                if field[row_idx][col_idx]:
                    text += "💥"
                else:
                    text += "✅"
            else:
                text += "🟫"
        text += "\n"
    
    return text


def _minesweeper_kb(revealed: list) -> InlineKeyboardMarkup:
    buttons = []
    for row_idx in range(5):
        row_buttons = []
        for col_idx in range(5):
            if revealed[row_idx][col_idx]:
                row_buttons.append(InlineKeyboardButton(text="✅", callback_data=f"ms_cell_{row_idx}_{col_idx}"))
            else:
                row_buttons.append(InlineKeyboardButton(text="🟫", callback_data=f"ms_cell_{row_idx}_{col_idx}"))
        buttons.append(row_buttons)
    
    buttons.append([InlineKeyboardButton(
        text="💰 Забрать",
        callback_data="ms_cashout",
        icon_custom_emoji_id="5870633910337015697"
    )])
    buttons.append([InlineKeyboardButton(
        text="В меню",
        callback_data="back_main",
        icon_custom_emoji_id="5893057118545646106"
    )])
    
    return InlineKeyboardMarkup(inline_keyboard=buttons)


@dp.callback_query(MinesweeperState.in_game, F.data.startswith("ms_cell_"))
async def minesweeper_open_cell(cq: CallbackQuery, state: FSMContext):
    user_id = cq.from_user.id
    parts = cq.data.split("_")
    row, col = int(parts[2]), int(parts[3])
    
    data = await state.get_data()
    field = data.get("minesweeper_field", [])
    revealed = data.get("minesweeper_revealed", [])
    amount = data.get("minesweeper_amount", 0)
    
    if (row < 0 or row >= 5 or col < 0 or col >= 5 or revealed[row][col]):
        await cq.answer("Эта ячейка уже открыта!", show_alert=False)
        return
    
    revealed[row][col] = True
    
    if field[row][col]:
        update_balance(user_id, -amount, win=False, game_type="minesweeper")
        await state.clear()
        text = (
            f"💥 <b>МИНА!</b>\n\n"
            f"💸 Ставка: <b>{amount} монет</b>\n"
            f"❌ Вы потеряли: <b>-{amount} монет</b>\n\n"
            f"💰 Новый баланс: <b>{get_balance(user_id)} монет</b>"
        )
        await cq.message.edit_text(text, parse_mode="HTML", reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="💣 Сыграть снова", callback_data="open_minesweeper")],
            [InlineKeyboardButton(text="В меню", callback_data="back_main",
                                  icon_custom_emoji_id="5893057118545646106")],
        ]))
    else:
        await state.update_data(minesweeper_revealed=revealed)
        text = _minesweeper_text(amount, field, revealed)
        await cq.message.edit_text(text, parse_mode="HTML", reply_markup=_minesweeper_kb(revealed))
    
    await cq.answer()


@dp.callback_query(MinesweeperState.in_game, F.data == "ms_cashout")
async def minesweeper_cashout(cq: CallbackQuery, state: FSMContext):
    user_id = cq.from_user.id
    data = await state.get_data()
    
    amount = data.get("minesweeper_amount", 0)
    revealed = data.get("minesweeper_revealed", [])
    opened_count = sum(sum(row) for row in revealed)
    multiplier = 1.0 + (opened_count * 0.3)
    
    total_payout = int(amount * multiplier)
    net_profit = total_payout - amount
    
    update_balance(user_id, net_profit, win=True, game_type="minesweeper")
    await state.clear()
    
    text = (
        f"✅ <b>Вы забрали выигрыш!</b>\n\n"
        f"📈 Открыто ячеек: <b>{opened_count}</b>\n"
        f"📈 Множитель: <b>x{multiplier:.2f}</b>\n"
        f"💸 Ставка: <b>{amount} монет</b>\n"
        f"🎉 Выигрыш: <b>+{net_profit} монет</b>\n\n"
        f"💰 Новый баланс: <b>{get_balance(user_id)} монет</b>"
    )
    await cq.message.edit_text(text, parse_mode="HTML", reply_markup=InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="💣 Сыграть снова", callback_data="open_minesweeper")],
        [InlineKeyboardButton(text="В меню", callback_data="back_main",
                              icon_custom_emoji_id="5893057118545646106")],
    ]))
    await cq.answer()


@dp.callback_query(F.data == "open_rocket")
async def open_rocket(cq: CallbackQuery, state: FSMContext):
    user_id = cq.from_user.id
    bal = get_balance(user_id)
    if bal <= 0:
        await cq.answer("У вас нет монет! Сбросьте баланс.", show_alert=True)
        return
    await state.set_state(RocketState.choosing_amount)
    text = (
        f"🚀 <b>Ракета</b>\n"
        f"💰 Баланс: <b>{bal} монет</b>\n\n"
        f"Ракета взлетает и множитель растёт.\n"
        f"Нажми <b>«Дальше»</b> чтобы продолжить лететь,\n"
        f"или <b>«Забрать»</b> чтобы зафиксировать выигрыш.\n"
        f"Если ракета взорвётся — ставка сгорает!\n\n"
        f"<b>Выбери сумму ставки:</b>"
    )
    await cq.message.edit_text(text, parse_mode="HTML", reply_markup=rocket_amount_kb(bal))


@dp.callback_query(RocketState.choosing_amount, F.data == "rocket_amount_custom")
async def rocket_amount_custom_cb(cq: CallbackQuery, state: FSMContext):
    await state.update_data(waiting_custom=True)
    bal = get_balance(cq.from_user.id)
    await cq.message.edit_text(
        f"✏️ <b>Введите сумму ставки:</b>\n💰 Баланс: <b>{bal} монет</b>",
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="Назад", callback_data="open_rocket",
                                  icon_custom_emoji_id="5893057118545646106")]
        ])
    )
    await cq.answer()


@dp.callback_query(RocketState.choosing_amount, F.data.startswith("rocket_amount_"))
async def rocket_set_amount(cq: CallbackQuery, state: FSMContext):
    user_id = cq.from_user.id
    raw = cq.data.replace("rocket_amount_", "")
    amount = int(raw)
    bal = get_balance(user_id)
    if amount > bal:
        await cq.answer("Недостаточно средств!", show_alert=True)
        return
    await _start_rocket_game(cq.message, state, user_id, amount)
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


async def _start_rocket_game(message, state: FSMContext, user_id: int, amount: int, is_message: bool = False):
    """Запустить новую игру в ракету."""
    crash_point = generate_crash_point()
    start_multiplier = 1.0

    await state.set_state(RocketState.in_game)
    await state.update_data(
        rocket_amount=amount,
        rocket_multiplier=start_multiplier,
        rocket_crash=crash_point,
        waiting_custom=False,
    )

    text = _rocket_text(amount, start_multiplier)

    if is_message:
        await message.answer(text, parse_mode="HTML", reply_markup=rocket_game_kb())
    else:
        await message.edit_text(text, parse_mode="HTML", reply_markup=rocket_game_kb())


def _rocket_text(amount: int, multiplier: float) -> str:
    potential = int(amount * multiplier)
    stars = "⭐" * min(int(multiplier), 10)
    return (
        f"🚀 <b>Ракета летит!</b>\n\n"
        f"💸 Ставка: <b>{amount} монет</b>\n"
        f"📈 Множитель: <b>x{multiplier:.2f}</b>  {stars}\n"
        f"💰 Можно забрать: <b>{potential} монет</b>\n\n"
        f"Нажми <b>«Дальше»</b> чтобы лететь выше\n"
        f"или <b>«Забрать»</b> чтобы зафиксировать!"
    )


@dp.callback_query(RocketState.in_game, F.data == "rocket_next")
async def rocket_next(cq: CallbackQuery, state: FSMContext):
    """Игрок нажал Дальше — проверяем краш или двигаемся вперёд."""
    user_id = cq.from_user.id
    data = await state.get_data()

    amount = data.get("rocket_amount", 0)
    multiplier = data.get("rocket_multiplier", 1.0)
    crash_point = data.get("rocket_crash", 1.0)

    new_multiplier = next_multiplier(multiplier)

    if new_multiplier >= crash_point:
        # 💥 КРАШ
        update_balance(user_id, -amount, win=False, game_type="rocket")
        await state.clear()
        text = (
            f"💥 <b>РАКЕТА ВЗОРВАЛАСЬ!</b>\n\n"
            f"📈 Множитель дошёл до: <b>x{crash_point:.2f}</b>\n"
            f"💸 Ставка: <b>{amount} монет</b>\n"
            f"❌ Вы потеряли: <b>-{amount} монет</b>\n\n"
            f"💰 Новый баланс: <b>{get_balance(user_id)} монет</b>"
        )
        await cq.message.edit_text(text, parse_mode="HTML", reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🚀 Сыграть снова", callback_data="open_rocket")],
            [InlineKeyboardButton(text="В меню", callback_data="back_main",
                                  icon_custom_emoji_id="5893057118545646106")],
        ]))
    else:
        # ✅ Продолжаем лететь
        await state.update_data(rocket_multiplier=new_multiplier)
        text = _rocket_text(amount, new_multiplier)
        try:
            await cq.message.edit_text(text, parse_mode="HTML", reply_markup=rocket_game_kb())
        except Exception:
            pass

    await cq.answer()


@dp.callback_query(RocketState.in_game, F.data == "rocket_cashout")
async def rocket_cashout(cq: CallbackQuery, state: FSMContext):
    """Игрок нажал Забрать — фиксируем выигрыш."""
    user_id = cq.from_user.id
    data = await state.get_data()

    amount = data.get("rocket_amount", 0)
    multiplier = data.get("rocket_multiplier", 1.0)

    total_payout = int(amount * multiplier)
    net_profit = total_payout - amount
    update_balance(user_id, net_profit, win=True, game_type="rocket")
    await state.clear()

    text = (
        f"✅ <b>Вы забрали выигрыш!</b>\n\n"
        f"📈 Множитель: <b>x{multiplier:.2f}</b>\n"
        f"💸 Ставка: <b>{amount} монет</b>\n"
        f"🎉 Выигрыш: <b>+{net_profit} монет</b>\n\n"
        f"💰 Новый баланс: <b>{get_balance(user_id)} монет</b>"
    )
    await cq.message.edit_text(text, parse_mode="HTML", reply_markup=InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🚀 Сыграть снова", callback_data="open_rocket")],
        [InlineKeyboardButton(text="В меню", callback_data="back_main",
                              icon_custom_emoji_id="5893057118545646106")],
    ]))
    await cq.answer()


async def main():
    init_db()
    print("🎰 Roulette bot started!")
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