import asyncio
import random
import os
from datetime import datetime, timedelta
import pytz
from dotenv import load_dotenv
import psycopg2
from aiogram import Bot, Dispatcher, F
from aiogram.types import Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.filters import CommandStart
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage

# ──────────────────────────────────────────────
# CONFIG
# ──────────────────────────────────────────────
load_dotenv()
TOKEN = os.getenv("BOT_TOKEN")
DATABASE_URL = os.getenv("DATABASE_URL")
STARTING_BALANCE = 1000

bot = Bot(token=TOKEN)
dp = Dispatcher(storage=MemoryStorage())

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
                    user_id BIGINT PRIMARY KEY,
                    username TEXT,
                    balance INTEGER DEFAULT 1000,
                    wins INTEGER DEFAULT 0,
                    losses INTEGER DEFAULT 0
                )
            """)

def create_user(user_id, username):
    with db_connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO users VALUES (%s,%s,%s,0,0) ON CONFLICT DO NOTHING",
                (user_id, username, STARTING_BALANCE)
            )

def get_balance(user_id):
    with db_connect() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT balance FROM users WHERE user_id=%s", (user_id,))
            row = cur.fetchone()
    return row[0] if row else 0

def update_balance(user_id, delta, win):
    col = "wins" if win else "losses"
    with db_connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"UPDATE users SET balance=balance+%s, {col}={col}+1 WHERE user_id=%s",
                (delta, user_id)
            )

# ──────────────────────────────────────────────
# STATES
# ──────────────────────────────────────────────
class GameState(StatesGroup):
    choosing_game = State()
    choosing_bet = State()
    choosing_amount = State()

# ──────────────────────────────────────────────
# LOGIC
# ──────────────────────────────────────────────
RED = {1,3,5,7,9,12,14,16,18,19,21,23,25,27,30,32,34,36}

def spin():
    return random.randint(0,36)

def flip():
    return random.choice(["heads","tails"])

# ──────────────────────────────────────────────
# KEYBOARDS
# ──────────────────────────────────────────────
def main_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🎰 Рулетка", callback_data="roulette")],
        [InlineKeyboardButton(text="🪙 Орёл/Решка", callback_data="coin")],
        [InlineKeyboardButton(text="💰 Баланс", callback_data="balance")]
    ])

def roulette_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="🔴 Красное", callback_data="bet_red"),
            InlineKeyboardButton(text="⚫ Чёрное", callback_data="bet_black"),
        ],
        [InlineKeyboardButton(text="Назад", callback_data="back")]
    ])

def coin_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="🦅 Орёл", callback_data="coin_heads"),
            InlineKeyboardButton(text="🪙 Решка", callback_data="coin_tails"),
        ],
        [InlineKeyboardButton(text="Назад", callback_data="back")]
    ])

def amount_kb(balance):
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="25%", callback_data=f"amt_{balance//4}"),
            InlineKeyboardButton(text="50%", callback_data=f"amt_{balance//2}")
        ],
        [
            InlineKeyboardButton(text="Ва-банк", callback_data=f"amt_{balance}")
        ],
        [InlineKeyboardButton(text="Назад", callback_data="back")]
    ])

# ──────────────────────────────────────────────
# START
# ──────────────────────────────────────────────
@dp.message(CommandStart())
async def start(msg: Message):
    create_user(msg.from_user.id, msg.from_user.username or "игрок")
    bal = get_balance(msg.from_user.id)

    await msg.answer(
        f"🎮 <b>Казино бот</b>\n\nБаланс: <b>{bal}</b>",
        parse_mode="HTML",
        reply_markup=main_kb()
    )

# ──────────────────────────────────────────────
# NAVIGATION
# ──────────────────────────────────────────────
@dp.callback_query(F.data == "back")
async def back(cq: CallbackQuery, state: FSMContext):
    await state.clear()
    bal = get_balance(cq.from_user.id)
    await cq.message.edit_text(
        f"Меню\nБаланс: {bal}",
        reply_markup=main_kb()
    )

# ──────────────────────────────────────────────
# BALANCE
# ──────────────────────────────────────────────
@dp.callback_query(F.data == "balance")
async def balance(cq: CallbackQuery):
    bal = get_balance(cq.from_user.id)
    await cq.answer(f"💰 {bal}", show_alert=True)

# ──────────────────────────────────────────────
# ROULETTE
# ──────────────────────────────────────────────
@dp.callback_query(F.data == "roulette")
async def roulette(cq: CallbackQuery, state: FSMContext):
    await state.set_state(GameState.choosing_bet)
    await state.update_data(game="roulette")

    await cq.message.edit_text(
        "🎰 Выбери ставку",
        reply_markup=roulette_kb()
    )

# ──────────────────────────────────────────────
# COIN
# ──────────────────────────────────────────────
@dp.callback_query(F.data == "coin")
async def coin(cq: CallbackQuery, state: FSMContext):
    await state.set_state(GameState.choosing_bet)
    await state.update_data(game="coin")

    await cq.message.edit_text(
        "🪙 Выбери сторону",
        reply_markup=coin_kb()
    )

# ──────────────────────────────────────────────
# BET CHOICE
# ──────────────────────────────────────────────
@dp.callback_query(GameState.choosing_bet)
async def choose_bet(cq: CallbackQuery, state: FSMContext):
    await state.update_data(choice=cq.data)
    await state.set_state(GameState.choosing_amount)

    bal = get_balance(cq.from_user.id)

    await cq.message.edit_text(
        f"Баланс: {bal}\nВыбери сумму",
        reply_markup=amount_kb(bal)
    )

# ──────────────────────────────────────────────
# GAME EXECUTION
# ──────────────────────────────────────────────
@dp.callback_query(GameState.choosing_amount, F.data.startswith("amt_"))
async def play(cq: CallbackQuery, state: FSMContext):
    amount = int(cq.data.split("_")[1])
    data = await state.get_data()

    game = data["game"]
    choice = data["choice"]

    if game == "roulette":
        result = spin()
        color = "red" if result in RED else "black"

        win = (choice == "bet_red" and color == "red") or \
              (choice == "bet_black" and color == "black")

        text_result = f"{'🔴' if color=='red' else '⚫'} {result}"

    else:
        result = flip()
        win = choice == f"coin_{result}"
        text_result = "🦅 Орёл" if result == "heads" else "🪙 Решка"

    if win:
        update_balance(cq.from_user.id, amount, True)
        res = f"🎉 Победа +{amount}"
    else:
        update_balance(cq.from_user.id, -amount, False)
        res = f"❌ Проигрыш -{amount}"

    bal = get_balance(cq.from_user.id)

    await state.clear()

    await cq.message.edit_text(
        f"Результат: {text_result}\n{res}\nБаланс: {bal}",
        reply_markup=main_kb()
    )

# ──────────────────────────────────────────────
# MAIN
# ──────────────────────────────────────────────
async def main():
    init_db()
    print("STARTED")
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())