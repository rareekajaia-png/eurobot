import asyncio
import logging
import os
import random
from datetime import datetime, timedelta

import asyncpg
from dotenv import load_dotenv
from aiogram import Bot, Dispatcher, F, types
from aiogram.filters import CommandStart, Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.exceptions import TelegramBadRequest

# --- Настройки и конфиг ---
load_dotenv()
TOKEN = os.getenv("BOT_TOKEN")
DATABASE_URL = os.getenv("DATABASE_URL")
STARTING_BALANCE = 1000

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# --- Класс базы данных (Asyncpg) ---
class Database:
    def __init__(self, url):
        self.url = url
        self.pool = None

    async def connect(self):
        """Создание пула соединений и инициализация таблиц"""
        self.pool = await asyncpg.create_pool(self.url)
        async with self.pool.acquire() as conn:
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS users (
                    user_id BIGINT PRIMARY KEY,
                    balance BIGINT DEFAULT $1,
                    wins INTEGER DEFAULT 0,
                    losses INTEGER DEFAULT 0,
                    last_farm TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
                );
            ''', STARTING_BALANCE)

    async def get_user(self, user_id: int):
        """Получение или регистрация пользователя"""
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow("SELECT * FROM users WHERE user_id = $1", user_id)
            if not row:
                await conn.execute("INSERT INTO users (user_id) VALUES ($1)", user_id)
                row = await conn.fetchrow("SELECT * FROM users WHERE user_id = $1", user_id)
            return dict(row)

    async def update_balance(self, user_id: int, amount: int, stats_col: str = None):
        """Универсальное обновление баланса и статистики"""
        async with self.pool.acquire() as conn:
            if stats_col in ['wins', 'losses']:
                query = f"UPDATE users SET balance = balance + $1, {stats_col} = {stats_col} + 1 WHERE user_id = $2"
            else:
                query = "UPDATE users SET balance = balance + $1 WHERE user_id = $2"
            await conn.execute(query, amount, user_id)

    async def reset_farm(self, user_id: int):
        """Сброс времени фермы"""
        async with self.pool.acquire() as conn:
            await conn.execute("UPDATE users SET last_farm = CURRENT_TIMESTAMP WHERE user_id = $1", user_id)

    async def get_top_players(self, limit=10):
        """Получение списка богатейших игроков"""
        async with self.pool.acquire() as conn:
            return await conn.fetch("SELECT user_id, balance FROM users ORDER BY balance DESC LIMIT $1", limit)

    async def get_all_user_ids(self):
        """Для массовых рассылок/бонусов"""
        async with self.pool.acquire() as conn:
            rows = await conn.fetch("SELECT user_id FROM users")
            return [r['user_id'] for r in rows]

db = Database(DATABASE_URL)

# --- Клавиатуры ---
def main_reply_kb():
    return types.ReplyKeyboardMarkup(
        keyboard=[
            [types.KeyboardButton(text="🚀 Ракета"), types.KeyboardButton(text="🌾 Ферма")],
            [types.KeyboardButton(text="💰 Профиль"), types.KeyboardButton(text="🏆 Топ")]
        ],
        resize_keyboard=True
    )

def rocket_kb():
    return types.InlineKeyboardMarkup(inline_keyboard=[
        [types.InlineKeyboardButton(text="Поставить 100 🪙", callback_data="bet_100")],
        [types.InlineKeyboardButton(text="Поставить 500 🪙", callback_data="bet_500")],
        [types.InlineKeyboardButton(text="Поставить 1000 🪙", callback_data="bet_1000")]
    ])

# --- Утилиты ---
def format_chips(amount: int) -> str:
    return f"{amount:,}".replace(",", " ")

# --- Инициализация бота ---
bot = Bot(token=TOKEN)
dp = Dispatcher(storage=MemoryStorage())

# --- Обработчики (Handlers) ---

@dp.message(CommandStart())
async def cmd_start(message: types.Message, state: FSMContext):
    await state.clear()
    await db.get_user(message.from_user.id)
    await message.answer(
        f"🎰 <b>Добро пожаловать, {message.from_user.first_name}!</b>\n\n"
        f"Вам начислено {format_chips(STARTING_BALANCE)} стартовых фишек.\n"
        "Используйте меню ниже, чтобы начать игру.",
        reply_markup=main_reply_kb(),
        parse_mode="HTML"
    )

@dp.message(F.text == "💰 Профиль")
async def profile_handler(message: types.Message):
    u = await db.get_user(message.from_user.id)
    text = (
        f"👤 <b>Профиль: {message.from_user.full_name}</b>\n"
        f"🆔 ID: <code>{message.from_user.id}</code>\n\n"
        f"🪙 Баланс: <b>{format_chips(u['balance'])}</b> фишек\n"
        f"✅ Побед: {u['wins']}\n"
        f"❌ Поражений: {u['losses']}"
    )
    await message.answer(text, parse_mode="HTML")

@dp.message(F.text == "🌾 Ферма")
async def farm_handler(message: types.Message):
    u = await db.get_user(message.from_user.id)
    # Расчет накоплений
    last_farm = u['last_farm']
    now = datetime.now(last_farm.tzinfo)
    diff = now - last_farm
    
    hours = min(diff.total_seconds() / 3600, 24) # Максимум за 24 часа
    pending = int(hours * 50) # 50 фишек в час
    
    if pending < 1:
        await message.answer("🐥 Ваша ферма еще не принесла дохода. Приходите позже!")
    else:
        await db.update_balance(message.from_user.id, pending)
        await db.reset_farm(message.from_user.id)
        await message.answer(
            f"🚜 <b>Урожай собран!</b>\n"
            f"Вы получили: <b>+{pending}</b> фишек.\n"
            f"Следующий сбор доступен через час.",
            parse_mode="HTML"
        )

@dp.message(F.text == "🚀 Ракета")
async def rocket_menu(message: types.Message):
    await message.answer(
        "🚀 <b>Добро пожаловать в игру Ракета!</b>\n\n"
        "Правила просты: если ракета взлетает — ваш баланс удваивается.\n"
        "Если взрывается — ставка сгорает.\n\n"
        "Выберите сумму ставки:",
        reply_markup=rocket_kb(),
        parse_mode="HTML"
    )

@dp.callback_query(F.data.startswith("bet_"))
async def rocket_game(callback: types.CallbackQuery):
    bet = int(callback.data.split("_")[1])
    u = await db.get_user(callback.from_user.id)
    
    if u['balance'] < bet:
        return await callback.answer("❌ Недостаточно фишек на балансе!", show_alert=True)

    # Визуализация полета
    try:
        msg = await callback.message.edit_text("🚀 Ракета на старте... 3...")
        await asyncio.sleep(0.7)
        await msg.edit_text("🚀 Ракета на старте... 2...")
        await asyncio.sleep(0.7)
        await msg.edit_text("🚀 Ракета на старте... 1...")
        await asyncio.sleep(0.7)
        await msg.edit_text("🔥 ПОЕХАЛИ!")
        await asyncio.sleep(1)
    except TelegramBadRequest:
        pass

    if random.random() > 0.5:
        await db.update_balance(callback.from_user.id, bet, "wins")
        await callback.message.edit_text(
            f"📈 <b>Ракета успешно вышла на орбиту!</b>\n"
            f"Выигрыш: <b>+{format_chips(bet)}</b> (всего {format_chips(bet*2)})\n\n"
            "Сыграем еще?", reply_markup=rocket_kb(), parse_mode="HTML"
        )
    else:
        await db.update_balance(callback.from_user.id, -bet, "losses")
        await callback.message.edit_text(
            f"💥 <b>Ракета взорвалась в стратосфере...</b>\n"
            f"Потеряно: <b>{format_chips(bet)}</b> фишек.\n\n"
            "Не повезло сейчас — повезет потом!", reply_markup=rocket_kb(), parse_mode="HTML"
        )

@dp.message(F.text == "🏆 Топ")
async def top_handler(message: types.Message):
    top = await db.get_top_players(10)
    text = "🏆 <b>ТОП-10 Богатейших Игроков:</b>\n\n"
    for i, row in enumerate(top, 1):
        text += f"{i}. 🆔 <code>{row['user_id']}</code> — <b>{format_chips(row['balance'])}</b>\n"
    await message.answer(text, parse_mode="HTML")

# --- Фоновые задачи ---
async def daily_bonus_loop():
    """Рассылка бонусов раз в сутки в 12:00"""
    while True:
        now = datetime.now()
        target = now.replace(hour=12, minute=0, second=0, microsecond=0)
        if now >= target:
            target += timedelta(days=1)
        
        wait_time = (target - now).total_seconds()
        logger.info(f"До следующего бонуса: {wait_time/3600:.2f} ч.")
        await asyncio.sleep(wait_time)

        ids = await db.get_all_user_ids()
        for u_id in ids:
            try:
                await db.update_balance(u_id, 500)
                await bot.send_message(
                    u_id, 
                    "🎁 <b>Ежедневный бонус!</b>\nВам начислено 500 фишек. Удачи!",
                    parse_mode="HTML"
                )
            except Exception:
                continue
        await asyncio.sleep(60) # Защита от повторного срабатывания в ту же секунду

# --- Главная функция запуска ---
async def main():
    # Подключаемся к БД
    await db.connect()
    
    # Запускаем фоновую задачу бонусов
    asyncio.create_task(daily_bonus_loop())
    
    # Запуск бота
    logger.info("Бот успешно запущен!")
    await dp.start_polling(bot)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Бот остановлен")