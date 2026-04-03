import asyncio
import random
import os
from datetime import datetime, timedelta
import pytz
from dotenv import load_dotenv
import psycopg2
from psycopg2.extras import RealDictCursor
from aiogram import Bot, Dispatcher, F
from aiogram.types import Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton, LabeledPrice, PreCheckoutQuery, ShippingQuery
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

# Глобальное хранилище последних ставок для повтора
last_bets = {}  # {user_id: {'game': 'coin'/'roulette', 'choice'/'bet_type': ..., 'amount': ...}}

# ──────────────────────────────────────────────
# DATABASE
# ──────────────────────────────────────────────
def db_connect():
    return psycopg2.connect(DATABASE_URL)

def init_db():
    with db_connect() as conn:
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

def get_user(user_id: int):
    with db_connect() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM users WHERE user_id=%s", (user_id,))
            return cur.fetchone()

def create_user(user_id: int, username: str):
    with db_connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO users (user_id, username, balance) VALUES (%s,%s,%s) ON CONFLICT DO NOTHING",
                (user_id, username, STARTING_BALANCE)
            )

def update_balance(user_id: int, delta: int, win: bool, game_type: str = "roulette"):
    col = "wins" if win else "losses"
    with db_connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"UPDATE users SET balance = balance + %s, {col} = {col} + 1 WHERE user_id=%s",
                (delta, user_id)
            )
    # Добавляем запись в историю
    add_history(user_id, abs(delta), win, game_type)

def get_balance(user_id: int) -> int:
    with db_connect() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT balance FROM users WHERE user_id=%s", (user_id,))
            row = cur.fetchone()
    return row[0] if row else 0

def get_all_users():
    with db_connect() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT user_id FROM users")
            return [r[0] for r in cur.fetchall()]

def add_daily_bonus(user_id: int):
    with db_connect() as conn:
        with conn.cursor() as cur:
            cur.execute("UPDATE users SET balance = balance + 500 WHERE user_id=%s", (user_id,))

def reset_balance(user_id: int):
    # Сбрасываем только баланс, wins/losses остаются
    with db_connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE users SET balance = %s WHERE user_id=%s",
                (STARTING_BALANCE, user_id)
            )

def get_all_users_full():
    """Получить всех пользователей с полной информацией"""
    with db_connect() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT * FROM users ORDER BY user_id")
            return cur.fetchall()

def set_balance(user_id: int, amount: int):
    """Установить баланс пользователю (админ функция)"""
    with db_connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE users SET balance = %s WHERE user_id=%s",
                (amount, user_id)
            )

def add_history(user_id: int, amount: int, is_win: bool, game_type: str = "roulette"):
    """Добавить запись в историю ставок"""
    with db_connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """INSERT INTO history (user_id, amount, is_win, game_type)
                   VALUES (%s, %s, %s, %s)""",
                (user_id, amount, is_win, game_type)
            )

def get_history(user_id: int, limit: int = 10):
    """Получить последние ставки пользователя"""
    with db_connect() as conn:
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

def get_user_history_stats(user_id: int):
    """Получить статистику по истории (выигрыши/проигрыши)"""
    with db_connect() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """SELECT 
                   SUM(CASE WHEN is_win THEN amount ELSE 0 END) as total_won,
                   SUM(CASE WHEN NOT is_win THEN amount ELSE 0 END) as total_lost,
                   COUNT(CASE WHEN is_win THEN 1 END) as win_count,
                   COUNT(CASE WHEN NOT is_win THEN 1 END) as lose_count
                   FROM history
                   WHERE user_id=%s""",
                (user_id,)
            )
            return cur.fetchone()


# ──────────────────────────────────────────────
# FSM
# ──────────────────────────────────────────────
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

# ──────────────────────────────────────────────
# ROULETTE LOGIC
# ──────────────────────────────────────────────
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

# ──────────────────────────────────────────────
# COIN (ОРЕЛ И РЕШКА)
# ──────────────────────────────────────────────
def flip_coin() -> str:
    return random.choice(["heads", "tails"])

def check_coin_bet(choice: str, result: str) -> bool:
    return choice == result

# ──────────────────────────────────────────────
# KEYBOARDS
# ──────────────────────────────────────────────
def main_menu_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(
            text="Рулетка",
            callback_data="open_roulette",
            icon_custom_emoji_id="5258882890059091157"   # 🎰
        )],
        [InlineKeyboardButton(
            text="Орёл и Решка",
            callback_data="open_coin",
            icon_custom_emoji_id="5774585885154131652"   # 🪙
        )],
        [
            InlineKeyboardButton(
                text="Статистика",
                callback_data="stats",
                icon_custom_emoji_id="5870921681735781843"   # 📊
            ),
        ],
        [InlineKeyboardButton(
            text="Получить звезды ⭐",
            callback_data="donate",
            icon_custom_emoji_id="5904462880941545555"  # 💫
        )],
        [InlineKeyboardButton(
            text="Сбросить баланс",
            callback_data="reset",
            icon_custom_emoji_id="5345906554510012647"   # 🔄
        )],
    ])

def bet_type_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="🟥 Красное",  callback_data="bet_red"),
            InlineKeyboardButton(text="⬛ Чёрное",   callback_data="bet_black"),
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
            icon_custom_emoji_id="5771851822897566479"   # 🔡
        )],
        [InlineKeyboardButton(
            text="Назад",
            callback_data="back_main",
            icon_custom_emoji_id="5893057118545646106"   # ◁
        )],
    ])

def coin_side_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="🦅 Орёл", callback_data="coin_heads"),
            InlineKeyboardButton(text="🪙 Решка", callback_data="coin_tails"),
        ],
        [InlineKeyboardButton(
            text="Назад",
            callback_data="back_main",
            icon_custom_emoji_id="5893057118545646106"   # ◁
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
                icon_custom_emoji_id="6041731551845159060"   # 🎉
            ),
        ],
        [InlineKeyboardButton(
            text="Ввести вручную",
            callback_data="amount_custom",
            icon_custom_emoji_id="5870676941614354370"   # 🖋
        )],
        [InlineKeyboardButton(
            text="Назад",
            callback_data="back_bet_type",
            icon_custom_emoji_id="5893057118545646106"   # ◁
        )],
    ]
    return InlineKeyboardMarkup(inline_keyboard=buttons)

# ──────────────────────────────────────────────
# BET LABELS
# ──────────────────────────────────────────────
BET_LABELS = {
    "red":    "🟥 Красное",
    "black":  "⬛ Чёрное",
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

# ──────────────────────────────────────────────
# ADMIN KEYBOARDS
# ──────────────────────────────────────────────
ADMIN_ID = int(os.getenv("ADMIN_ID", "0"))  # Добавь ADMIN_ID в .env

def admin_menu_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(
            text="Список пользователей",
            callback_data="admin_users",
            icon_custom_emoji_id="5870772616305839506"  # 👥
        )],
        [InlineKeyboardButton(
            text="Рассылка сообщения",
            callback_data="admin_broadcast",
            icon_custom_emoji_id="6039422865189638057"  # 📣
        )],
        [InlineKeyboardButton(
            text="Вернуться в меню",
            callback_data="back_main",
            icon_custom_emoji_id="5893057118545646106"  # 📰
        )],
    ])

def stats_menu_kb():
    """Клавиатура меню статистики"""
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(
            text="История ставок",
            callback_data="stats_history",
            icon_custom_emoji_id="5870930636742595124"  # 📊
        )],
        [InlineKeyboardButton(
            text="Назад в меню",
            callback_data="back_main",
            icon_custom_emoji_id="5893057118545646106"  # 📰
        )],
    ])

def game_result_kb(game_type: str):
    """Клавиатура результатов игры"""
    if game_type == "coin":
        repeat_data = "repeat_coin"
    else:
        repeat_data = "repeat_roulette"
    
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(
            text="Повторить",
            callback_data=repeat_data,
            icon_custom_emoji_id="5345906554510012647"  # 🔄
        )],
        [InlineKeyboardButton(
            text="В меню",
            callback_data="back_main",
            icon_custom_emoji_id="5893057118545646106"  # 📰
        )],
    ])

def donate_kb():
    """Клавиатура с вариантами донатов"""
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="⭐ 50", callback_data="donate_50")],
        [InlineKeyboardButton(text="⭐ 100", callback_data="donate_100")],
        [InlineKeyboardButton(text="⭐ 250", callback_data="donate_250")],
        [InlineKeyboardButton(text="⭐ 500", callback_data="donate_500")],
        [InlineKeyboardButton(text="⭐ 1000", callback_data="donate_1000")],
        [InlineKeyboardButton(
            text="Назад в меню",
            callback_data="back_main",
            icon_custom_emoji_id="5893057118545646106"
        )],
    ])

def users_list_kb(users: list, page: int = 0):
    """Создать клавиатуру со списком пользователей (постраничная)"""
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
            icon_custom_emoji_id="5870994129244131212"  # 👤
        )])
    
    # Навигация
    nav_buttons = []
    if page > 0:
        nav_buttons.append(InlineKeyboardButton(
            text="Назад",
            callback_data=f"admin_users_page_{page-1}",
            icon_custom_emoji_id="5893057118545646106"  # 📰
        ))
    if end_idx < len(users):
        nav_buttons.append(InlineKeyboardButton(
            text="Вперед",
            callback_data=f"admin_users_page_{page+1}",
            icon_custom_emoji_id="5893057118545646106"  # 📰
        ))
    
    if nav_buttons:
        buttons.append(nav_buttons)
    
    buttons.append([InlineKeyboardButton(
        text="В меню админа",
        callback_data="admin_back",
        icon_custom_emoji_id="5893057118545646106"  # 📰
    )])
    
    return InlineKeyboardMarkup(inline_keyboard=buttons)

def edit_user_kb(user_id: int):
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(
            text="Изменить баланс",
            callback_data=f"admin_edit_balance_{user_id}",
            icon_custom_emoji_id="5904462880941545555"  # 🪙
        )],
        [InlineKeyboardButton(
            text="История ставок",
            callback_data=f"admin_user_history_{user_id}",
            icon_custom_emoji_id="5870930636742595124"  # 📊
        )],
        [InlineKeyboardButton(
            text="Назад к списку",
            callback_data="admin_users_back",
            icon_custom_emoji_id="5893057118545646106"  # 📰
        )],
    ])

# ──────────────────────────────────────────────
# HANDLERS
# ──────────────────────────────────────────────
@dp.message(CommandStart())
async def cmd_start(msg: Message, state: FSMContext):
    create_user(msg.from_user.id, msg.from_user.username or "игрок")
    bal = get_balance(msg.from_user.id)
    await state.clear()
    text = (
        f'<tg-emoji emoji-id="5258882890059091157">🎰</tg-emoji> <b>Добро пожаловать в Казино!</b>\n\n'
        f'<tg-emoji emoji-id="5904462880941545555">🪙</tg-emoji> Ваш  баланс: <b>{bal} монет</b>\n\n'
        f'<i>Нажмите «Рулетка» или "Орёл и Решка", чтобы начать игру.</i>'
    )
    await msg.answer(text, parse_mode="HTML", reply_markup=main_menu_kb())


@dp.callback_query(F.data == "back_main")
async def back_main(cq: CallbackQuery, state: FSMContext):
    await state.clear()
    bal = get_balance(cq.from_user.id)
    text = (
        f'<tg-emoji emoji-id="5258882890059091157">🎰</tg-emoji> <b>Европейская Рулетка</b>\n'
        f'<tg-emoji emoji-id="5904462880941545555">🪙</tg-emoji> Баланс: <b>{bal} монет</b>'
    )
    await cq.message.edit_text(text, parse_mode="HTML", reply_markup=main_menu_kb())


@dp.callback_query(F.data == "balance")
async def show_balance(cq: CallbackQuery):
    bal = get_balance(cq.from_user.id)
    await cq.answer(f'<tg-emoji emoji-id="5904462880941545555">🪙</tg-emoji> Ваш баланс: {bal} монет', show_alert=True)


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
    """Показать историю ставок пользователя"""
    user_id = cq.from_user.id
    history = get_history(user_id, limit=15)
    
    if not history:
        text = f'<tg-emoji emoji-id="5870930636742595124">📊</tg-emoji> <b>Нет истории ставок</b>'
    else:
        text = f'<tg-emoji emoji-id="5870930636742595124">📊</tg-emoji> <b>История ставок (последние 15):</b>\n\n'
        for entry in history:
            status_emoji = f'<tg-emoji emoji-id="5870633910337015697">✅</tg-emoji>' if entry['is_win'] else f'<tg-emoji emoji-id="5870657884844462243">❌</tg-emoji>'
            game_name = "Монета" if entry['game_type'] == "coin" else "Рулетка"
            sign = '+' if entry['is_win'] else '-'
            text += f'{status_emoji} {sign}{entry["amount"]} - {game_name}\n'
    
    text += f'\n<tg-emoji emoji-id="5870921681735781843">📊</tg-emoji> <b>Статистика по истории:</b>\n'
    stats = get_user_history_stats(user_id)
    if stats and stats['win_count']:
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
            icon_custom_emoji_id="5893057118545646106"  # 📰
        )],
    ]))


@dp.callback_query(F.data == "donate")
async def open_donate(cq: CallbackQuery):
    """Открыть меню доната"""
    text = (
        '<tg-emoji emoji-id="5258882890059091157">💫</tg-emoji> <b>Получите звезды!</b>\n\n'
        'Выберите количество звезд для пополнения баланса:\n'
        '• 50 ⭐ → +100 монет\n'
        '• 100 ⭐ → +250 монет\n'
        '• 250 ⭐ → +700 монет\n'
        '• 500 ⭐ → +1500 монет\n'
        '• 1000 ⭐ → +3500 монет'
    )
    await cq.message.edit_text(text, parse_mode="HTML", reply_markup=donate_kb())


@dp.callback_query(F.data.startswith("donate_"))
async def process_donation(cq: CallbackQuery):
    """Обработать выбор суммы для доната"""
    amount_str = cq.data.split("_")[1]
    amount = int(amount_str)
    
    # Соответствие звезд к монетам
    star_to_coins = {
        50: 100,
        100: 250,
        250: 700,
        500: 1500,
        1000: 3500,
    }
    
    coins = star_to_coins.get(amount, 0)
    
    if coins == 0:
        await cq.answer("❌ Некорректная сумма", show_alert=True)
        return
    
    # Отправить инвойс для оплаты звездами
    await bot.send_invoice(
        chat_id=cq.from_user.id,
        title="Пополнение баланса",
        description=f"Получите {coins} монет за {amount} ⭐",
        payload=f"donate_{amount}",  # Уникальный payload для отслеживания
        currency="XTR",  # XTR - Telegram Stars
        prices=[LabeledPrice(label=f"Звезды ({amount})", amount=amount)],
        provider_token="",  # Для Telegram Stars provider_token должен быть пустым
    )


@dp.callback_query(F.data == "reset")
async def reset_handler(cq: CallbackQuery, state: FSMContext):
    reset_balance(cq.from_user.id)   # wins/losses не трогаем
    await state.clear()
    await cq.answer(
        f'✅ Баланс сброшен до 1000 монет!',
        show_alert=True
    )
    await cq.message.edit_text(
        f'<tg-emoji emoji-id="5345906554510012647">🔄</tg-emoji> <b>Баланс сброшен!</b>\n'
        f'<tg-emoji emoji-id="5904462880941545555">🪙</tg-emoji> Новый баланс: <b>1000 монет</b>',
        parse_mode="HTML",
        reply_markup=main_menu_kb()
    )


# ──────────────────────────────────────────────
# PAYMENT HANDLERS
# ──────────────────────────────────────────────
@dp.pre_checkout_query()
async def process_pre_checkout_query(pre_checkout_query: PreCheckoutQuery):
    """Обработать pre-checkout запрос (подтверждение перед платежом)"""
    await bot.answer_pre_checkout_query(pre_checkout_query.id, ok=True)


@dp.message(lambda msg: msg.successful_payment is not None)
async def process_successful_payment(msg: Message):
    """Обработать успешный платеж"""
    payment = msg.successful_payment
    
    # Извлечь сумму звезд из payload
    payload = payment.invoice_payload
    if not payload.startswith("donate_"):
        return
    
    amount_str = payload.split("_")[1]
    amount = int(amount_str)
    
    # Соответствие звезд к монетам
    star_to_coins = {
        50: 100,
        100: 250,
        250: 700,
        500: 1500,
        1000: 3500,
    }
    
    coins = star_to_coins.get(amount, 0)
    
    if coins > 0:
        # Добавить монеты пользователю
        update_balance(msg.from_user.id, coins, win=True, game_type="donation")
        new_bal = get_balance(msg.from_user.id)
        
        text = (
            f'<tg-emoji emoji-id="6041731551845159060">🎉</tg-emoji> <b>Спасибо за поддержку!</b>\n\n'
            f'<tg-emoji emoji-id="5904462880941545555">🪙</tg-emoji> Вы получили: <b>+{coins} монет</b>\n'
            f'<tg-emoji emoji-id="5904462880941545555">🪙</tg-emoji> Новый баланс: <b>{new_bal} монет</b>'
        )
        await msg.answer(text, parse_mode="HTML", reply_markup=main_menu_kb())
    else:
        await msg.answer("❌ Ошибка при обработке платежа", parse_mode="HTML")


@dp.callback_query(F.data == "open_roulette")
async def open_roulette(cq: CallbackQuery, state: FSMContext):
    bal = get_balance(cq.from_user.id)
    if bal <= 0:
        await cq.answer(
            f'<tg-emoji emoji-id="5870657884844462243">❌</tg-emoji> У вас нет монет! Сбросьте баланс.',
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
        
        # Сохранить информацию о ставке для функции повтора
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
        await cq.answer(
            f'<tg-emoji emoji-id="5870657884844462243">❌</tg-emoji> Сначала выберите тип ставки.',
            show_alert=True
        )
        return

    bal = get_balance(cq.from_user.id)
    if amount > bal:
        await cq.answer(
            f'<tg-emoji emoji-id="5870657884844462243">❌</tg-emoji> Недостаточно средств!',
            show_alert=True
        )
        return

    # Сохранить информацию о ставке для функции повтора
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
        text += f'\n\n<tg-emoji emoji-id="5870657884844462243">❌</tg-emoji> <b>Вы банкрот!</b> Нажмите «Сбросить баланс».'

    await cq.message.edit_text(text, parse_mode="HTML", reply_markup=game_result_kb("roulette"))


# ──────────────────────────────────────────────
# COIN FLIP GAME
# ──────────────────────────────────────────────
@dp.callback_query(F.data == "open_coin")
async def open_coin(cq: CallbackQuery, state: FSMContext):
    bal = get_balance(cq.from_user.id)
    if bal <= 0:
        await cq.answer(
            f'<tg-emoji emoji-id="5870657884844462243">❌</tg-emoji> У вас нет монет! Сбросьте баланс.',
            show_alert=True
        )
        return
    await state.set_state(CoinState.choosing_side)
    text = (
        f'<tg-emoji emoji-id="5774585885154131652">🪙</tg-emoji> <b>Орёл и Решка</b>\n'
        f'<tg-emoji emoji-id="5904462880941545555">🪙</tg-emoji> Баланс: <b>{bal} монет</b>\n\n'
        f'<b>Выберите сторону монеты:</b>'
    )
    await cq.message.edit_text(text, parse_mode="HTML", reply_markup=coin_side_kb())


@dp.callback_query(CoinState.choosing_side, F.data.startswith("coin_"))
async def choose_coin_side(cq: CallbackQuery, state: FSMContext):
    side_raw = cq.data.split("_")[1]  # "heads" или "tails"
    side_label = "🦅 Орёл" if side_raw == "heads" else "🪙 Решка"
    
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

        # Сохранить информацию о ставке для функции повтора
        last_bets[msg.from_user.id] = {'game': 'coin', 'choice': coin_choice, 'amount': amount}

        result = flip_coin()
        won = check_coin_bet(coin_choice, result)

        side_label = "🦅 Орёл" if coin_choice == "heads" else "🪙 Решка"
        result_label = "🦅 Орёл" if result == "heads" else "🪙 Решка"

        if won:
            profit = amount * 2  # 2x payout for coin flip
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
            text += f'\n\n<tg-emoji emoji-id="5870657884844462243">❌</tg-emoji> <b>Вы банкрот!</b> Нажмите «Сбросить баланс».'

        await msg.answer(text, parse_mode="HTML", reply_markup=game_result_kb("coin"))
        return


@dp.callback_query(CoinState.choosing_amount, F.data.startswith("amount_"))
async def place_coin_bet(cq: CallbackQuery, state: FSMContext):
    amount = int(cq.data.split("_")[1])
    data = await state.get_data()
    coin_choice = data.get("coin_choice", "")

    if not coin_choice:
        await cq.answer(
            f'<tg-emoji emoji-id="5870657884844462243">❌</tg-emoji> Сначала выберите сторону монеты.',
            show_alert=True
        )
        return

    bal = get_balance(cq.from_user.id)
    if amount > bal:
        await cq.answer(
            f'<tg-emoji emoji-id="5870657884844462243">❌</tg-emoji> Недостаточно средств!',
            show_alert=True
        )
        return

    # Сохранить информацию о ставке для функции повтора
    last_bets[cq.from_user.id] = {'game': 'coin', 'choice': coin_choice, 'amount': amount}

    result = flip_coin()
    won = check_coin_bet(coin_choice, result)

    side_label = "🦅 Орёл" if coin_choice == "heads" else "🪙 Решка"
    result_label = "🦅 Орёл" if result == "heads" else "🪙 Решка"

    if won:
        profit = amount * 2  # 2x payout for coin flip
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
        text += f'\n\n<tg-emoji emoji-id="5870657884844462243">❌</tg-emoji> <b>Вы банкрот!</b> Нажмите «Сбросить баланс».'

    await cq.message.edit_text(text, parse_mode="HTML", reply_markup=game_result_kb("coin"))


# ──────────────────────────────────────────────
# REPEAT GAME HANDLERS
# ──────────────────────────────────────────────
@dp.callback_query(F.data == "repeat_roulette")
async def repeat_roulette(cq: CallbackQuery, state: FSMContext):
    """Повторить игру в рулетку с той же ставкой"""
    user_id = cq.from_user.id
    
    # Получить сохраненную информацию о последней ставке
    if user_id not in last_bets or last_bets[user_id].get('game') != 'roulette':
        await cq.answer("ℹ️ Нет сохраненной ставки, выберите новую.", show_alert=False)
        return
    
    bet_data = last_bets[user_id]
    bet_type = bet_data.get('bet_type')
    amount = bet_data.get('amount')
    
    # Проверить баланс
    bal = get_balance(user_id)
    if amount > bal:
        await cq.answer(
            f'<tg-emoji emoji-id="5870657884844462243">❌</tg-emoji> Недостаточно монет! Нужно {amount}, а у вас {bal}.',
            show_alert=True
        )
        return
    
    # Выполнить ставку с теми же параметрами
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
        text += f'\n\n<tg-emoji emoji-id="5870657884844462243">❌</tg-emoji> <b>Вы банкрот!</b> Нажмите «Сбросить баланс».'

    await cq.message.edit_text(text, parse_mode="HTML", reply_markup=game_result_kb("roulette"))


@dp.callback_query(F.data == "repeat_coin")
async def repeat_coin(cq: CallbackQuery, state: FSMContext):
    """Повторить игру в орла и решку с той же ставкой"""
    user_id = cq.from_user.id
    
    # Получить сохраненную информацию о последней ставке
    if user_id not in last_bets or last_bets[user_id].get('game') != 'coin':
        await cq.answer("ℹ️ Нет сохраненной ставки, выберите новую.", show_alert=False)
        return
    
    bet_data = last_bets[user_id]
    coin_choice = bet_data.get('choice')
    amount = bet_data.get('amount')
    
    # Проверить баланс
    bal = get_balance(user_id)
    if amount > bal:
        await cq.answer(
            f'<tg-emoji emoji-id="5870657884844462243">❌</tg-emoji> Недостаточно монет! Нужно {amount}, а у вас {bal}.',
            show_alert=True
        )
        return
    
    # Выполнить ставку с теми же параметрами
    result = flip_coin()
    won = check_coin_bet(coin_choice, result)

    side_label = "🦅 Орёл" if coin_choice == "heads" else "🪙 Решка"
    result_label = "🦅 Орёл" if result == "heads" else "🪙 Решка"

    if won:
        profit = amount * 2  # 2x payout for coin flip
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
        text += f'\n\n<tg-emoji emoji-id="5870657884844462243">❌</tg-emoji> <b>Вы банкрот!</b> Нажмите «Сбросить баланс».'

    await cq.message.edit_text(text, parse_mode="HTML", reply_markup=game_result_kb("coin"))


# ──────────────────────────────────────────────
# ADMIN PANEL
# ──────────────────────────────────────────────
@dp.message(Command("admin"))
async def cmd_admin(msg: Message, state: FSMContext):
    """Команда /admin - доступна только администратору"""
    if msg.from_user.id != ADMIN_ID:
        await msg.answer("❌ У вас нет доступа к админ-панели")
        return
    
    await state.set_state(AdminState.choosing_action)
    text = "<b>🔧 Админ-панель</b>\n\nВыберите действие:"
    await msg.answer(text, parse_mode="HTML", reply_markup=admin_menu_kb())


@dp.callback_query(F.data == "admin_users")
async def show_users_list(cq: CallbackQuery, state: FSMContext):
    """Показать список пользователей"""
    if cq.from_user.id != ADMIN_ID:
        await cq.answer(
            f'<tg-emoji emoji-id="5870657884844462243">❌</tg-emoji> Только админ может это делать',
            show_alert=True
        )
        return
    
    await state.set_state(AdminState.choosing_user)
    users = get_all_users_full()
    
    text = f"<b>👥 Список пользователей:</b>\n\nВсего: {len(users)} пользователей\n\n"
    text += "Выберите пользователя для редактирования:"
    
    await cq.message.edit_text(text, parse_mode="HTML", reply_markup=users_list_kb(users, 0))


@dp.callback_query(F.data.startswith("admin_users_page_"))
async def paginate_users(cq: CallbackQuery, state: FSMContext):
    """Навигация по странам пользователей"""
    if cq.from_user.id != ADMIN_ID:
        return
    
    page = int(cq.data.split("_")[-1])
    users = get_all_users_full()
    
    text = f"<b>👥 Список пользователей:</b>\n\nВсего: {len(users)} пользователей\n\n"
    text += "Выберите пользователя для редактирования:"
    
    await cq.message.edit_text(text, parse_mode="HTML", reply_markup=users_list_kb(users, page))


@dp.callback_query(F.data.startswith("admin_edit_user_"))
async def edit_user_menu(cq: CallbackQuery, state: FSMContext):
    """Меню редактирования пользователя"""
    if cq.from_user.id != ADMIN_ID:
        return
    
    user_id = int(cq.data.split("_")[-1])
    row = get_user(user_id)
    
    if not row:
        await cq.answer(
            f'<tg-emoji emoji-id="5870657884844462243">❌</tg-emoji> Пользователь не найден',
            show_alert=True
        )
        return
    
    _, username, balance, wins, losses = row
    text = (
        f"<b>👤 Профиль пользователя:</b>\n\n"
        f"ID: <code>{user_id}</code>\n"
        f"Имя: <b>{username or 'Неизвестно'}</b>\n"
        f"💰 Баланс: <b>{balance}</b> монет\n"
        f"✅ Побед: <b>{wins}</b>\n"
        f"❌ Поражений: <b>{losses}</b>\n\n"
        f"Выберите действие:"
    )
    
    await state.update_data(admin_user_id=user_id)
    await cq.message.edit_text(text, parse_mode="HTML", reply_markup=edit_user_kb(user_id))


@dp.callback_query(F.data.startswith("admin_user_history_"))
async def admin_show_user_history(cq: CallbackQuery):
    """Показать историю ставок пользователя для админа"""
    if cq.from_user.id != ADMIN_ID:
        return
    
    user_id = int(cq.data.split("_")[-1])
    row = get_user(user_id)
    
    if not row:
        await cq.answer(
            f'<tg-emoji emoji-id="5870657884844462243">❌</tg-emoji> Пользователь не найден',
            show_alert=True
        )
        return
    
    username = row[1]
    history = get_history(user_id, limit=15)
    
    if not history:
        text = f'<tg-emoji emoji-id="5870930636742595124">📊</tg-emoji> <b>История ставок {username or f"ID {user_id}"}:</b>\n\nНет истории ставок'
    else:
        text = f'<tg-emoji emoji-id="5870930636742595124">📊</tg-emoji> <b>История ставок {username or f"ID {user_id}"} (последние 15):</b>\n\n'
        for entry in history:
            status_emoji = f'<tg-emoji emoji-id="5870633910337015697">✅</tg-emoji>' if entry['is_win'] else f'<tg-emoji emoji-id="5870657884844462243">❌</tg-emoji>'
            game_name = "Монета" if entry['game_type'] == "coin" else "Рулетка"
            sign = '+' if entry['is_win'] else '-'
            text += f'{status_emoji} {sign}{entry["amount"]} - {game_name}\n'
    
    text += f'\n<tg-emoji emoji-id="5870921681735781843">📊</tg-emoji> <b>Статистика:</b>\n'
    stats = get_user_history_stats(user_id)
    if stats and stats['win_count']:
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
            icon_custom_emoji_id="5893057118545646106"  # 📰
        )],
    ]))


@dp.callback_query(F.data.startswith("admin_edit_balance_"))
async def ask_new_balance(cq: CallbackQuery, state: FSMContext):
    """Запросить новый баланс"""
    if cq.from_user.id != ADMIN_ID:
        return
    
    user_id = int(cq.data.split("_")[-1])
    await state.update_data(admin_user_id=user_id)
    await state.set_state(AdminState.editing_balance)
    
    current_balance = get_balance(user_id)
    text = (
        f"💰 Введите новый баланс для пользователя (текущий: <b>{current_balance}</b>):\n\n"
        f"<i>Только число, пожалуйста</i>"
    )
    
    await cq.message.edit_text(text, parse_mode="HTML", reply_markup=InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔙 Назад", callback_data="admin_users_back")]
    ]))


@dp.message(AdminState.editing_balance)
async def process_new_balance(msg: Message, state: FSMContext):
    """Обработать ввод нового баланса"""
    if msg.from_user.id != ADMIN_ID:
        return
    
    try:
        new_balance = int(msg.text.strip())
        assert new_balance >= 0
    except:
        await msg.answer("❌ Введите корректное число (больше или равно 0)")
        return
    
    data = await state.get_data()
    user_id = data.get("admin_user_id")
    
    set_balance(user_id, new_balance)
    
    user = get_user(user_id)
    _, username, _, _, _ = user
    
    text = (
        f"✅ <b>Баланс обновлён!</b>\n\n"
        f"Пользователь: <b>{username or 'Неизвестно'}</b> (ID: {user_id})\n"
        f"Новый баланс: <b>{new_balance}</b> монет"
    )
    
    await state.clear()
    await msg.answer(text, parse_mode="HTML", reply_markup=admin_menu_kb())


@dp.callback_query(F.data == "admin_broadcast")
async def broadcast_menu(cq: CallbackQuery, state: FSMContext):
    """Меню рассылки сообщений"""
    if cq.from_user.id != ADMIN_ID:
        await cq.answer(
            f'<tg-emoji emoji-id="5870657884844462243">❌</tg-emoji> Только админ может это делать',
            show_alert=True
        )
        return
    
    users = get_all_users_full()
    text = (
        f"📢 <b>Рассылка сообщений</b>\n\n"
        f"Адресатов: {len(users)} пользователей\n\n"
        f"Введите сообщение, которое нужно отправить всем пользователям:\n\n"
        f"<i>Поддерживает HTML форматирование</i>"
    )
    
    await state.set_state(AdminState.sending_broadcast)
    await cq.message.edit_text(text, parse_mode="HTML", reply_markup=InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔙 Отмена", callback_data="admin_back")]
    ]))


@dp.message(AdminState.sending_broadcast)
async def process_broadcast(msg: Message, state: FSMContext):
    """Отправить сообщение всем пользователям"""
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
                f"📢 <b>Сообщение от администратора:</b>\n\n{broadcast_text}",
                parse_mode="HTML"
            )
            success_count += 1
        except Exception as e:
            fail_count += 1
    
    text = (
        f"✅ <b>Рассылка завершена!</b>\n\n"
        f"Успешно отправлено: <b>{success_count}</b>\n"
        f"Ошибок: <b>{fail_count}</b>"
    )
    
    await state.clear()
    await msg.answer(text, parse_mode="HTML", reply_markup=admin_menu_kb())


@dp.callback_query(F.data == "admin_back")
async def admin_back_to_menu(cq: CallbackQuery, state: FSMContext):
    """Вернуться в меню админа"""
    if cq.from_user.id != ADMIN_ID:
        return
    
    await state.clear()
    await state.set_state(AdminState.choosing_action)
    text = "<b>🔧 Админ-панель</b>\n\nВыберите действие:"
    await cq.message.edit_text(text, parse_mode="HTML", reply_markup=admin_menu_kb())


@dp.callback_query(F.data == "admin_users_back")
async def admin_back_to_users(cq: CallbackQuery, state: FSMContext):
    """Вернуться в список пользователей"""
    if cq.from_user.id != ADMIN_ID:
        return
    
    await state.set_state(AdminState.choosing_user)
    users = get_all_users_full()
    
    text = f"<b>👥 Список пользователей:</b>\n\nВсего: {len(users)} пользователей\n\n"
    text += "Выберите пользователя для редактирования:"
    
    await cq.message.edit_text(text, parse_mode="HTML", reply_markup=users_list_kb(users, 0))


# ──────────────────────────────────────────────
# DAILY BONUS
# ──────────────────────────────────────────────
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
                    f'<i>Удачной игры! 🎰</i>',
                    parse_mode="HTML"
                )
            except Exception:
                pass
 
        await asyncio.sleep(60)

# ──────────────────────────────────────────────
# MAIN
# ──────────────────────────────────────────────
async def main():
    init_db()
    print("🎰 Roulette bot started!")
    asyncio.create_task(daily_bonus_task())
    await dp.start_polling(bot)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("🛑 Бот остановлен.")