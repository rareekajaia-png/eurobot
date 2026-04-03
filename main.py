import asyncio
import random
import os
from datetime import datetime, timedelta
import pytz
from dotenv import load_dotenv
import psycopg2
from psycopg2.extras import RealDictCursor
from aiogram import Bot, Dispatcher, F
from aiogram.types import Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.filters import CommandStart
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage

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
                    user_id    BIGINT PRIMARY KEY,
                    username   TEXT,
                    balance    INTEGER DEFAULT 1000,
                    wins       INTEGER DEFAULT 0,
                    losses     INTEGER DEFAULT 0
                )
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

def update_balance(user_id: int, delta: int, win: bool):
    col = "wins" if win else "losses"
    with db_connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"UPDATE users SET balance = balance + %s, {col} = {col} + 1 WHERE user_id=%s",
                (delta, user_id)
            )

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
    with db_connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE users SET balance=%s, wins=0, losses=0 WHERE user_id=%s",
                (STARTING_BALANCE, user_id)
            )

# ──────────────────────────────────────────────
# FSM
# ──────────────────────────────────────────────
class BetState(StatesGroup):
    choosing_bet_type = State()
    choosing_amount   = State()

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
# KEYBOARDS
# ──────────────────────────────────────────────
def main_menu_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🎰 Рулетка", callback_data="open_roulette")],
        [InlineKeyboardButton(text="💰 Баланс",  callback_data="balance"),
         InlineKeyboardButton(text="📊 Статистика", callback_data="stats")],
        [InlineKeyboardButton(text="🔄 Сбросить баланс", callback_data="reset")],
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
            InlineKeyboardButton(text="ЧЁТНОЕ",  callback_data="bet_even"),
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
        [InlineKeyboardButton(text="🔢 Конкретное число (x35)", callback_data="bet_number")],
        [InlineKeyboardButton(text="◀️ Назад", callback_data="back_main")],
    ])

def bet_amount_kb(balance: int):
    chips = [1, 2, 10, 20, 50, 100]
    buttons = []
    row = []
    for chip in chips:
        if chip <= balance:
            row.append(InlineKeyboardButton(text=f"💰 {chip}", callback_data=f"amount_{chip}"))
        if len(row) == 3:
            buttons.append(row); row = []
    if row: buttons.append(row)
    buttons.append([InlineKeyboardButton(text="✏️ Ввести вручную", callback_data="amount_custom")])
    buttons.append([InlineKeyboardButton(text="◀️ Назад", callback_data="back_bet_type")])
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
# HANDLERS
# ──────────────────────────────────────────────
@dp.message(CommandStart())
async def cmd_start(msg: Message, state: FSMContext):
    create_user(msg.from_user.id, msg.from_user.username or "игрок")
    bal = get_balance(msg.from_user.id)
    await state.clear()
    text = (
        f"🎰 <b>Добро пожаловать в Европейскую Рулетку!</b>\n\n"
        f"💰 Ваш стартовый баланс: <b>{bal} монет</b>\n\n"
        f"<i>Нажмите «Рулетка», чтобы начать игру.</i>"
    )
    await msg.answer(text, parse_mode="HTML", reply_markup=main_menu_kb())


@dp.callback_query(F.data == "back_main")
async def back_main(cq: CallbackQuery, state: FSMContext):
    await state.clear()
    bal = get_balance(cq.from_user.id)
    text = (
        f"🎰 <b>Европейская Рулетка</b>\n"
        f"💰 Баланс: <b>{bal} монет</b>"
    )
    await cq.message.edit_text(text, parse_mode="HTML", reply_markup=main_menu_kb())


@dp.callback_query(F.data == "balance")
async def show_balance(cq: CallbackQuery):
    bal = get_balance(cq.from_user.id)
    await cq.answer(f"💰 Ваш баланс: {bal} монет", show_alert=True)


@dp.callback_query(F.data == "stats")
async def show_stats(cq: CallbackQuery):
    row = get_user(cq.from_user.id)
    if not row:
        await cq.answer("Сначала запустите /start", show_alert=True); return
    _, username, balance, wins, losses = row
    total = wins + losses
    rate = round(wins / total * 100, 1) if total else 0
    text = (
        f"📊 <b>Статистика</b>\n\n"
        f"👤 Игрок: <b>{username or 'Неизвестно'}</b>\n"
        f"💰 Баланс: <b>{balance}</b> монет\n"
        f"✅ Побед:  <b>{wins}</b>\n"
        f"❌ Поражений: <b>{losses}</b>\n"
        f"🎯 Процент побед: <b>{rate}%</b>"
    )
    await cq.message.edit_text(text, parse_mode="HTML", reply_markup=main_menu_kb())


@dp.callback_query(F.data == "reset")
async def reset_handler(cq: CallbackQuery, state: FSMContext):
    reset_balance(cq.from_user.id)
    await state.clear()
    await cq.answer("✅ Баланс сброшен до 1000 монет!", show_alert=True)
    await cq.message.edit_text(
        "🔄 <b>Баланс сброшен!</b>\n💰 Новый баланс: <b>1000 монет</b>",
        parse_mode="HTML", reply_markup=main_menu_kb()
    )


@dp.callback_query(F.data == "open_roulette")
async def open_roulette(cq: CallbackQuery, state: FSMContext):
    bal = get_balance(cq.from_user.id)
    if bal <= 0:
        await cq.answer("❌ У вас нет монет! Сбросьте баланс.", show_alert=True); return
    await state.set_state(BetState.choosing_bet_type)
    text = (
        f"🎰 <b>Европейская Рулетка</b>\n"
        f"💰 Баланс: <b>{bal} монет</b>\n\n"
        f"<b>Выберите тип ставки:</b>"
    )
    await cq.message.edit_text(text, parse_mode="HTML", reply_markup=bet_type_kb())


@dp.callback_query(BetState.choosing_bet_type, F.data.startswith("bet_"))
async def choose_bet_type(cq: CallbackQuery, state: FSMContext):
    raw = cq.data[4:]

    if raw == "number":
        await state.update_data(bet_type="pending_number")
        await state.set_state(BetState.choosing_amount)
        await cq.message.edit_text(
            "🔢 <b>Введите число от 0 до 36:</b>",
            parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="◀️ Назад", callback_data="back_bet_type")]
            ])
        )
        return

    await state.update_data(bet_type=raw)
    await state.set_state(BetState.choosing_amount)
    bal = get_balance(cq.from_user.id)
    label = BET_LABELS.get(raw, raw)
    text = (
        f"🎰 <b>Ставка: {label}</b>\n"
        f"💰 Баланс: <b>{bal} монет</b>\n\n"
        f"<b>Выберите сумму ставки:</b>"
    )
    await cq.message.edit_text(text, parse_mode="HTML", reply_markup=bet_amount_kb(bal))


@dp.callback_query(F.data == "back_bet_type")
async def back_bet_type(cq: CallbackQuery, state: FSMContext):
    await state.set_state(BetState.choosing_bet_type)
    bal = get_balance(cq.from_user.id)
    text = (
        f"🎰 <b>Европейская Рулетка</b>\n"
        f"💰 Баланс: <b>{bal} монет</b>\n\n"
        f"<b>Выберите тип ставки:</b>"
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
        result = spin_wheel()
        won  = check_bet(bet_type, result)
        mult = payout_multiplier(bet_type)
        color = number_color(result)
        if won:
            profit = amount * mult
            update_balance(msg.from_user.id, profit, win=True)
            outcome_text = f"🎉 <b>ПОБЕДА!</b>\n💵 +{profit} монет (x{mult})"
        else:
            update_balance(msg.from_user.id, -amount, win=False)
            outcome_text = f"😔 <b>Поражение.</b>\n💸 -{amount} монет"
        new_bal = get_balance(msg.from_user.id)
        label = BET_LABELS.get(bet_type, bet_type.replace("num_", "число "))
        text = (
            f"🎰 <b>Шарик остановился на:</b> {color} <b>{result}</b>\n\n"
            f"🎲 Ваша ставка: <b>{label}</b> — <b>{amount}</b> монет\n"
            f"{outcome_text}\n\n"
            f"💰 Новый баланс: <b>{new_bal} монет</b>"
        )
        await state.clear()
        if new_bal <= 0:
            text += "\n\n❌ <b>Вы банкрот!</b> Нажмите «Сбросить баланс»."
        await msg.answer(text, parse_mode="HTML", reply_markup=main_menu_kb())
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
        f"🔢 <b>Ставка на число {n}</b> (выплата x35)\n"
        f"💰 Баланс: <b>{bal} монет</b>\n\n<b>Выберите сумму ставки:</b>",
        parse_mode="HTML",
        reply_markup=bet_amount_kb(bal)
    )


@dp.callback_query(BetState.choosing_amount, F.data == "amount_custom")
async def ask_custom_amount(cq: CallbackQuery, state: FSMContext):
    await state.update_data(waiting_custom=True)
    bal = get_balance(cq.from_user.id)
    await cq.message.edit_text(
        f"✏️ <b>Введите сумму ставки:</b>\n💰 Баланс: <b>{bal} монет</b>",
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="◀️ Назад", callback_data="back_bet_type")]
        ])
    )

@dp.callback_query(BetState.choosing_amount, F.data.startswith("amount_"))
async def place_bet(cq: CallbackQuery, state: FSMContext):
    amount = int(cq.data.split("_")[1])
    data   = await state.get_data()
    bet_type = data.get("bet_type", "")

    if not bet_type or bet_type == "pending_number":
        await cq.answer("Сначала выберите тип ставки.", show_alert=True); return

    bal = get_balance(cq.from_user.id)
    if amount > bal:
        await cq.answer("❌ Недостаточно средств!", show_alert=True); return

    result = spin_wheel()
    color  = number_color(result)
    won    = check_bet(bet_type, result)
    mult   = payout_multiplier(bet_type)

    if won:
        profit = amount * mult
        update_balance(cq.from_user.id, profit, win=True)
        outcome_text = (
            f"🎉 <b>ПОБЕДА!</b>\n"
            f"💵 +{profit} монет (x{mult})"
        )
    else:
        update_balance(cq.from_user.id, -amount, win=False)
        outcome_text = f"😔 <b>Поражение.</b>\n💸 -{amount} монет"

    new_bal = get_balance(cq.from_user.id)
    label   = BET_LABELS.get(bet_type, bet_type.replace("num_", "число "))

    text = (
        f"🎰 <b>Шарик остановился на:</b> {color} <b>{result}</b>\n\n"
        f"🎲 Ваша ставка: <b>{label}</b> — <b>{amount}</b> монет\n"
        f"{outcome_text}\n\n"
        f"💰 Новый баланс: <b>{new_bal} монет</b>"
    )

    await state.clear()

    if new_bal <= 0:
        text += "\n\n❌ <b>Вы банкрот!</b> Нажмите «Сбросить баланс»."

    await cq.message.edit_text(text, parse_mode="HTML", reply_markup=main_menu_kb())


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
                    f"🎁 <b>Ежедневный бонус!</b>\n\n"
                    f"💰 Вам начислено <b>+500 монет</b>\n"
                    f"💳 Текущий баланс: <b>{new_bal} монет</b>\n\n"
                    f"<i>Удачной игры! 🎰</i>",
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