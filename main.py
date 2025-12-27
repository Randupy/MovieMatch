import asyncio
import random
import aiosqlite
import datetime
from urllib.parse import quote
from aiogram import Bot, Dispatcher, F, types, html
from aiogram.filters import Command
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.context import FSMContext
from aiogram.utils.keyboard import InlineKeyboardBuilder
from aiogram.types import BotCommand, BotCommandScopeDefault
from aiogram.exceptions import TelegramBadRequest
from typing import Union, Optional # Optional тоже полезен для типов с None
from aiohttp_socks import ProxyConnector
from aiogram.client.session.aiohttp import AiohttpSession
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
import os
import aiohttp
from pathlib import Path
from dotenv import load_dotenv


# Находим путь к папке, где лежит текущий файл main.py
current_dir = Path(__file__).resolve().parent
# Указываем точное имя твоего файла
env_path = current_dir / '.env.txt'

# Загружаем файл по конкретному пути
load_dotenv(dotenv_path=env_path)

# --- КОНФИГУРАЦИЯ ---
# .strip() удалит случайные пробелы или переносы строк, которые ломают прокси
TELEGRAM_TOKEN = os.getenv('TELEGRAM_TOKEN', '').strip()
TMDB_API_KEY = os.getenv('TMDB_API_KEY', '').strip()
PROXY_URL = os.getenv('PROXY_URL', '').strip()

# Для ID важен тип int
raw_admin_id = os.getenv('SUPER_ADMIN_ID', '0').strip()
SUPER_ADMIN_ID = int(raw_admin_id) if raw_admin_id.isdigit() else 0

DB_PATH = os.getenv('DB_PATH', 'movies_bot.db').strip()
MAIN_MENU_IMAGE = 'https://i.pinimg.com/736x/d5/93/bb/d593bb09053d11c90156aff633ebf2a2.jpg'

# Глобальные переменные
http_client: aiohttp.ClientSession = None
db: aiosqlite.Connection = None # Глобальная переменная для БД
bot: Bot = None
dp = Dispatcher()



# --- ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ---
def get_now():
    # Текущее время для всех записей
    return datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")


# --- СОСТОЯНИЯ ---
class AdminStates(StatesGroup):
    waiting_for_broadcast_content = State()
    waiting_for_broadcast_time = State()  # Новое: ожидание ввода времени
    confirm_broadcast = State()  # Новое: подтверждение
    waiting_for_broadcast_target = State()
    waiting_for_blacklist_id = State()
    waiting_for_profile_view = State()
    waiting_for_new_admin_id = State()
    waiting_for_remove_admin_id = State()
    waiting_for_ticket_reply = State()


class UserStates(StatesGroup):
    waiting_for_ticket = State()


class MovieStates(StatesGroup):
    waiting_for_room_code = State()


# --- ПАМЯТЬ ---
rooms = {}
user_to_room = {}
active_broadcasts = {}  # {task_id: {"task": Task, "data": dict, "admin_id": int}}

GENRES = {
    "28": "💥 Боевик", "12": "🤠 Приключения", "16": "🧸 Мультфильм",
    "35": "🤡 Комедия", "80": "🔪 Криминал", "18": "🎭 Драма",
    "27": "😱 Ужасы", "878": "🚀 Фантастика", "53": "😰 Триллер"
}

ENABLED_GENRES = list(GENRES.keys())


# --- РАБОТА С БАЗОЙ ДАННЫХ ---

async def init_db():

    # 1. Создаем таблицу пользователей (базовая структура)
    await db.execute('''CREATE TABLE IF NOT EXISTS users 
                        (user_id INTEGER PRIMARY KEY, username TEXT, first_name TEXT, 
                         joined_date TIMESTAMP,
                         last_active TIMESTAMP,
                         is_blocked INTEGER DEFAULT 0,
                         blocked_at TIMESTAMP)''')

    # 2. Создаем таблицу тикетов (базовая структура)
    await db.execute('''CREATE TABLE IF NOT EXISTS tickets 
                        (id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER, 
                         message TEXT, status TEXT DEFAULT 'open', created_at TIMESTAMP)''')

    # 3. Проверка структуры таблицы USERS (добавляем недостающие колонки)
    cursor = await db.execute("PRAGMA table_info(users)")
    user_columns = [row[1] for row in await cursor.fetchall()]

    if 'language_code' not in user_columns:
        await db.execute("ALTER TABLE users ADD COLUMN language_code TEXT DEFAULT 'unknown'")
    if 'is_blocked' not in user_columns:
        await db.execute("ALTER TABLE users ADD COLUMN is_blocked INTEGER DEFAULT 0")
    if 'blocked_at' not in user_columns:
        await db.execute("ALTER TABLE users ADD COLUMN blocked_at TIMESTAMP")

    # 4. Проверка структуры таблицы TICKETS (исправление вашей ошибки)
    cursor = await db.execute("PRAGMA table_info(tickets)")
    ticket_columns = [row[1] for row in await cursor.fetchall()]

    if 'created_at' not in ticket_columns:
        await db.execute("ALTER TABLE tickets ADD COLUMN created_at TIMESTAMP")
    if 'status' not in ticket_columns:
        await db.execute("ALTER TABLE tickets ADD COLUMN status TEXT DEFAULT 'open'")

    # 5. Создание остальных таблиц
    await db.execute('''CREATE TABLE IF NOT EXISTS admins 
                        (user_id INTEGER PRIMARY KEY, added_at TIMESTAMP)''')

    await db.execute('''CREATE TABLE IF NOT EXISTS admin_logs 
                        (id INTEGER PRIMARY KEY AUTOINCREMENT, admin_id INTEGER, 
                         action TEXT, details TEXT, timestamp TIMESTAMP)''')

    # Обновленная структура user_votes с movie_title
    await db.execute('''CREATE TABLE IF NOT EXISTS user_votes 
                        (user_id INTEGER, movie_id TEXT, movie_title TEXT, is_like INTEGER)''')

    await db.execute('''CREATE TABLE IF NOT EXISTS logs 
                        (id INTEGER PRIMARY KEY AUTOINCREMENT, error TEXT, time TIMESTAMP)''')

    cursor = await db.execute("PRAGMA table_info(user_votes)")
    vote_columns = [row[1] for row in await cursor.fetchall()]

    if 'added_at' not in vote_columns:
        await db.execute("ALTER TABLE user_votes ADD COLUMN added_at TIMESTAMP")

    # --- ИСПРАВЛЕНИЕ ОШИБКИ: Проверка колонки movie_title ---
    if 'movie_title' not in vote_columns:
        try:
            await db.execute("ALTER TABLE user_votes ADD COLUMN movie_title TEXT")
        except:
            pass

    # Добавляем создателя
    await db.execute("INSERT OR IGNORE INTO admins (user_id, added_at) VALUES (?, ?)", (SUPER_ADMIN_ID, get_now()))
    await db.commit()

    await db.execute("CREATE INDEX IF NOT EXISTS idx_user_votes_lookup ON user_votes (user_id, movie_id)")
    await db.commit()


async def get_user_stats(user_id):

    async with db.execute("SELECT COUNT(*) FROM user_votes WHERE user_id = ?", (user_id,)) as c:
        total_votes = (await c.fetchone())[0]

    if total_votes == 0:
        return None

    async with db.execute("SELECT COUNT(*) FROM user_votes WHERE user_id = ? AND is_like = 1", (user_id,)) as c:
        likes = (await c.fetchone())[0]

    async with db.execute("SELECT joined_date FROM users WHERE user_id = ?", (user_id,)) as c:
        user_data = await c.fetchone()

    # --- РАСЧЕТ КИНО-СТАТУСА ---
    ratio = round((likes / total_votes) * 100, 1)
    if ratio > 75:
        kino_status = "Кино-оптимист 😍"
        mood_text = "Вы любите почти всё! Бот в восторге от вашей доброты."
    elif ratio < 30:
        kino_status = "Строгий критик 🧐"
        mood_text = "Вас трудно впечатлить. Вы выбираете только лучшее."
    else:
        kino_status = "Ценитель баланса ⚖️"
        mood_text = "У вас отличный вкус и здоровое чувство критики."

    # --- ОПРЕДЕЛЕНИЕ ЭПОХИ (Заглушка/Логика) ---
    epoch = "Современность (2010-2024)" if total_votes < 100 else "Золотая эра (90-е и 00-е)"

    # Ранги и прогресс
    if total_votes < 50:
        rank, next_val, next_rank = "Новичок 👶", 50, "Киноман 🍿"
    elif total_votes < 200:
        rank, next_val, next_rank = "Киноман 🍿", 200, "Кинокритик 🧐"
    else:
        rank, next_val, next_rank = "Кинокритик 🧐", 500, "Легенда Голливуда 🌟"

    # Шкала прогресса
    bar = ""
    if next_val:
        percent = min(total_votes / next_val, 1.0)
        bar = "🟩" * int(percent * 10) + "⬜" * (10 - int(percent * 10))

    return {
        "total": total_votes,
        "likes": likes,
        "dislikes": total_votes - likes,
        "ratio": ratio,
        "joined": user_data[0][:10] if user_data else "Неизвестно",
        "rank": rank,
        "next_rank": next_rank,
        "bar": bar,
        "kino_status": kino_status,
        "mood_text": mood_text,
        "epoch": epoch
    }


# --- Остальные функции без изменений ---

async def log_admin_action(admin_id, action, details=""):

    await db.execute("INSERT INTO admin_logs (admin_id, action, details, timestamp) VALUES (?, ?, ?, ?)",
                     (admin_id, action, details, get_now()))
    await db.commit()


async def is_admin(user_id):

    async with db.execute("SELECT 1 FROM admins WHERE user_id = ?", (user_id,)) as cursor:
        return await cursor.fetchone() is not None


async def log_error(error_text):
    try:
        await db.execute("INSERT INTO logs (error, time) VALUES (?, ?)", (str(error_text), get_now()))
        await db.commit()
    except:
        pass


async def is_user_blocked(user_id):
    try:

        async with db.execute("SELECT is_blocked FROM users WHERE user_id = ?", (user_id,)) as cursor:
            row = await cursor.fetchone()
            return row[0] == 1 if row else False
    except:
        return False


async def update_user_activity(user_id):

    await db.execute("UPDATE users SET last_active = ? WHERE user_id = ?", (get_now(), user_id))
    await db.commit()


async def register_user(user_id, username, first_name, lang_code):
    now = get_now()

    await db.execute(
        """INSERT INTO users (user_id, username, first_name, joined_date, last_active, language_code) 
           VALUES (?, ?, ?, ?, ?, ?)
           ON CONFLICT(user_id) DO UPDATE SET 
           last_active = excluded.last_active,
           username = excluded.username,
           language_code = excluded.language_code""",
        (user_id, username, first_name, now, now, lang_code)
    )
    await db.commit()


async def add_vote(user_id, movie_id, title, is_like):

    # Обновлено использование movie_title вместо title для соответствия запросу статистики
    await db.execute(
        "INSERT INTO user_votes (user_id, movie_id, movie_title, is_like, added_at) VALUES (?, ?, ?, ?, ?)",
        (user_id, str(movie_id), title, is_like, get_now())
    )
    await db.commit()


async def get_user_seen_ids(user_id):

    async with db.execute("SELECT movie_id FROM user_votes WHERE user_id = ?", (user_id,)) as cursor:
        rows = await cursor.fetchall()
        return {row[0] for row in rows}


async def get_full_likes(user_id, limit=None):

    # Добавляем условие movie_title IS NOT NULL и проверку на пустую строку
    query = """
        SELECT movie_id, movie_title 
        FROM user_votes 
        WHERE user_id = ? 
          AND is_like = 1 
          AND movie_title IS NOT NULL 
          AND movie_title != ''
    """
    if limit:
        query += f" ORDER BY rowid DESC LIMIT {limit}"

    async with db.execute(query, (user_id,)) as cursor:
        return await cursor.fetchall()


async def delete_like(user_id, movie_id):

    await db.execute(
        "UPDATE user_votes SET is_like = 0 WHERE user_id = ? AND movie_id = ?",
        (user_id, str(movie_id))
    )
    await db.commit()


async def get_global_top():

    # Добавлен фильтр по movie_title
    query = """
        SELECT movie_title, COUNT(*) as count 
        FROM user_votes 
        WHERE is_like = 1 
          AND movie_title IS NOT NULL 
          AND movie_title != ''
        GROUP BY movie_id 
        ORDER BY count DESC LIMIT 10
    """
    async with db.execute(query) as cursor:
        return await cursor.fetchall()


async def get_targeted_user_ids(target_type):

    if target_type == "all":
        query = "SELECT user_id FROM users WHERE is_blocked = 0"
    elif target_type == "new":
        query = "SELECT user_id FROM users WHERE is_blocked = 0 AND joined_date > datetime('now', '-1 day', 'localtime')"
    elif target_type == "active":
        query = "SELECT user_id FROM users WHERE is_blocked = 0 AND last_active > datetime('now', '-1 day', 'localtime')"

    async with db.execute(query) as cursor:
        rows = await cursor.fetchall()
        return [row[0] for row in rows]


# --- ФУНКЦИИ TMDB ---

# Важно: http_client должен быть определен глобально и инициализирован в main()
async def fetch_movies_page(page=1, genre_id=None):
    global http_client  # <--- ОБЯЗАТЕЛЬНО ДОБАВИТЬ ЭТУ СТРОКУ

    url = f"https://api.themoviedb.org/3/discover/movie?api_key={TMDB_API_KEY}&language=ru-RU&sort_by=popularity.desc&page={page}"
    if genre_id:
        url += f"&with_genres={genre_id}"

    try:
        # Теперь Python поймет, что http_client — это наша общая сессия
        async with http_client.get(url, timeout=10) as response:
            if response.status == 200:
                data = await response.json()
                return data.get('results', [])
            return []
    except Exception as e:
        print(f"Ошибка TMDB: {e}")
        return []

async def filter_seen_movies(user_id, movies_list):
    """Оставляет только те фильмы, которые пользователь еще не оценивал"""

    # Получаем все ID фильмов, которые юзер уже свайпал
    async with db.execute("SELECT movie_id FROM user_votes WHERE user_id = ?", (user_id,)) as c:
        seen_ids = [str(r[0]) for r in await c.fetchall()]

    # Возвращаем только те фильмы, ID которых нет в списке просмотренных
    return [m for m in movies_list if str(m['id']) not in seen_ids]


async def get_trailer_url(movie_id):
    global http_client

    url = f"https://api.themoviedb.org/3/movie/{movie_id}/videos?api_key={TMDB_API_KEY}&language=ru-RU"

    # Используем глобальный http_client, инициализированный в main()
    try:
        async with http_client.get(url, timeout=5) as response:
            if response.status == 200:
                data = await response.json()
                for video in data.get('results', []):
                    if video['site'] == 'YouTube' and video['type'] in ['Trailer', 'Teaser']:
                        return f"https://www.youtube.com/watch?v={video['key']}"

            # Если на русском нет, пробуем на английском
            url_en = f"https://api.themoviedb.org/3/movie/{movie_id}/videos?api_key={TMDB_API_KEY}&language=en-US"
            async with http_client.get(url_en, timeout=5) as response_en:
                if response_en.status == 200:
                    data_en = await response_en.json()
                    for video in data_en.get('results', []):
                        if video['site'] == 'YouTube' and video['type'] in ['Trailer', 'Teaser']:
                            return f"https://www.youtube.com/watch?v={video['key']}"
            return None
    except Exception as e:
        print(f"Ошибка get_trailer_url: {e}")
        return None


# --- КЛАВИАТУРЫ ---

def get_main_menu_kb():
    builder = InlineKeyboardBuilder()
    builder.button(text="🙋‍♂️ Один", callback_data="solo_filters")
    builder.button(text="👥 Вдвоем", callback_data="duo_main")
    builder.button(text="❤️ Лайки", callback_data="show_my_likes")
    builder.button(text="🔥 Топ-10", callback_data="show_top_10")
    builder.button(text="📩 Поддержка", callback_data="user_support")
    builder.button(text="👤 Личный кабинет", callback_data="user_profile")
    builder.adjust(2, 2, 1)
    return builder.as_markup()


def get_admin_kb(is_super):
    builder = InlineKeyboardBuilder()
    builder.button(text="📊 Аналитика", callback_data="admin_stats")
    builder.button(text="🎭 Жанры (Вкл/Выкл)", callback_data="admin_content")
    builder.button(text="📩 Тикеты", callback_data="admin_tickets")
    builder.button(text="📢 Рассылка", callback_data="admin_broadcast_start")
    builder.button(text="👤 Юзеры / Профили", callback_data="admin_list_users")
    builder.button(text="🏠 Активные комнаты", callback_data="admin_active_rooms")
    builder.button(text="🚫 Чёрный список", callback_data="admin_blacklist_menu")
    builder.button(text="📜 Логи действий", callback_data="admin_logs_actions")
    builder.button(text="⚠️ Логи ошибок", callback_data="admin_logs_errors")
    builder.button(text="🗑 Очистка мусора", callback_data="admin_cleanup_menu")
    if is_super:
        builder.button(text="👑 Управление составом", callback_data="super_admin_menu")
    builder.adjust(2)
    return builder.as_markup()


# --- МИДДЛВАРЬ ---
@dp.message.outer_middleware()
@dp.callback_query.outer_middleware()
async def blacklist_middleware(handler, event, data):
    user = data.get('event_from_user')
    if user and await is_user_blocked(user.id):
        if isinstance(event, types.Message):
            await event.answer("🚫 Вы заблокированы.")
        else:
            await event.answer("🚫 Доступ запрещен.", show_alert=True)
        return
    return await handler(event, data)


async def send_next_movie(uid):
    await update_user_activity(uid)
    rid = user_to_room.get(uid)
    if not rid: return
    room = rooms[rid]
    u_data = room["users"][uid]
    seen_ids = await get_user_seen_ids(uid)

    while True:
        idx = u_data["idx"]
        if idx >= len(room["movies"]):
            room["last_page"] += 1
            # Исправление №1: Добавили await
            new_m = await fetch_movies_page(room["last_page"], room["genre_id"])
            if not new_m:
                return await bot.send_message(uid, "Фильмы закончились!")
            room["movies"].extend(new_m)

        movie = room["movies"][idx]
        if str(movie['id']) in seen_ids:
            u_data["idx"] += 1
            continue
        break

    m_title = html.quote(movie.get('title', ''))
    m_desc = html.quote(movie.get('overview', ''))[:350] + "..."

    # Проверка постера
    poster_path = movie.get('poster_path')
    if poster_path:
        poster = f"https://image.tmdb.org/t/p/w500{poster_path}"
    else:
        poster = "https://via.placeholder.com/500x750.png?text=No+Poster"

    builder = InlineKeyboardBuilder()
    builder.button(text="❤️", callback_data=f"like_{movie['id']}")
    builder.button(text="❌", callback_data=f"dislike_{movie['id']}")

    # Исправление №2: Добавили await для получения трейлера
    trailer = await get_trailer_url(movie['id'])

    if trailer:
        builder.button(text="📺 Трейлер", url=trailer)

    builder.button(text="🍿 Смотреть", url=f"https://yandex.ru/search?text={quote(m_title + ' смотреть онлайн')}")

    # --- ДОБАВЛЕНА ЛОГИКА КНОПОК ВЫХОДА ---
    if room.get("is_solo"):
        builder.button(text="⏹ Стоп", callback_data="exit_to_menu")
    else:
        # Кнопка для режима "Вдвоем"
        builder.button(text="🚪 Выйти из комнаты", callback_data="exit_room")

    # Исправление №3: Логика отрисовки (adjust)
    # 2 (лайк/дизлайк), 2 (трейлер/смотреть если есть трейлер), 1 (выход)
    if trailer:
        builder.adjust(2, 2, 1)
    else:
        builder.adjust(2, 1, 1)

    caption_text = f"🎬 <b>{m_title}</b>\n⭐ {movie.get('vote_average')}\n\n{m_desc}"

    try:
        await bot.send_photo(
            uid,
            poster,
            caption=caption_text,
            reply_markup=builder.as_markup(),
            parse_mode="HTML"
        )
    except Exception as e:
        print(f"Ошибка фото ({movie['id']}): {e}")
        try:
            await bot.send_message(
                uid,
                text=f"🖼 <i>(Постер недоступен)</i>\n\n{caption_text}",
                reply_markup=builder.as_markup(),
                parse_mode="HTML"
            )
        except Exception as e2:
            print(f"Критическая ошибка отправки: {e2}")



# --- ОБРАБОТЧИКИ ---

@dp.message(Command("start"))
async def start(message: types.Message, state: FSMContext):
    await state.clear()

    # Получаем код языка из данных телеграма
    user_lang = message.from_user.language_code or "unknown"

    # Передаем его в функцию регистрации
    await register_user(
        message.from_user.id,
        message.from_user.username,
        message.from_user.first_name,
        user_lang
    )

    # Текст с описанием функций бота
    menu_text = (
        "🍿 <b>Movie Match</b>\n"
        "━━━━━━━━━━━━━━━━━━\n"
        "Твой личный помощник в выборе кино. Свайпай карточки: ❤️ (нравится) или ❌ (нет), чтобы составить свой список.\n\n"
        "👥 <b>Режим «Вдвоем»:</b>\n"
        "Не можете решить, что посмотреть? Создайте комнату, отправьте код партнеру и бот найдет фильмы, которые понравились вам обоим!"
    )

    # Дальше ваш обычный код отправки фото или текста...
    try:
        await message.answer_photo(
            photo=MAIN_MENU_IMAGE,
            caption=menu_text,
            reply_markup=get_main_menu_kb(),
            parse_mode="HTML"
        )
    except:
        await message.answer(
            menu_text,
            reply_markup=get_main_menu_kb(),
            parse_mode="HTML"
        )


@dp.callback_query(F.data == "exit_to_menu")
async def exit_to_main_menu(callback: types.CallbackQuery, state: FSMContext):
    uid = callback.from_user.id

    # Если пользователь нажал "Назад", удаляем его из всех активных сессий
    if uid in user_to_room:
        rid = user_to_room[uid]
        if rid in rooms:
            # Если он был один в комнате (соло или ждал партнера) - удаляем комнату совсем
            if len(rooms[rid]["users"]) <= 1:
                del rooms[rid]
        del user_to_room[uid]

    await state.clear()

    # Текст и клавиатура твоего главного меню
    menu_text = (
         "🍿 <b>Movie Match</b>\n"
        "━━━━━━━━━━━━━━━━━━\n"
        "Твой личный помощник в выборе кино. Свайпай карточки: ❤️ (нравится) или ❌ (нет), чтобы составить свой список.\n\n"
        "👥 <b>Режим «Вдвоем»:</b>\n"
        "Не можете решить, что посмотреть? Создайте комнату, отправьте код партнеру и бот найдет фильмы, которые понравились вам обоим!"
    )

    # Если в главном меню должна быть картинка:
    try:
        await callback.message.delete()  # Удаляем старое меню выбора жанров
        await callback.message.answer_photo(
            photo=MAIN_MENU_IMAGE,
            caption=menu_text,
            reply_markup=get_main_menu_kb(),  # Твоя функция основной клавиатуры
            parse_mode="HTML"
        )
    except:
        await callback.message.edit_text(
            menu_text,
            reply_markup=get_main_menu_kb(),
            parse_mode="HTML"
        )
    await callback.answer()


@dp.callback_query(F.data == "user_support")
async def user_support_start(callback: types.CallbackQuery, state: FSMContext):
    await callback.message.answer("📝 Напишите вашу проблему одним сообщением:")
    await state.set_state(UserStates.waiting_for_ticket)


@dp.message(UserStates.waiting_for_ticket)
async def user_support_send(message: types.Message, state: FSMContext):
    current_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M")

    # ИСПРАВЛЕНИЕ: берем текст или описание фото, если текста нет
    ticket_content = message.text or message.caption or "[Изображение без текста]"

    await db.execute(
        "INSERT INTO tickets (user_id, message, status, created_at) VALUES (?, ?, ?, ?)",
        (message.from_user.id, ticket_content, "open", current_time)
    )
    await db.commit()

    await message.answer("✅ Ваше обращение отправлено администрации! Мы ответим вам в ближайшее время.")
    await state.clear()


# 1. Вызывается при нажатии кнопки "Ответить"
@dp.callback_query(F.data.startswith("reply_ticket_"))
async def reply_ticket_start(callback: types.CallbackQuery, state: FSMContext):
    ticket_id = int(callback.data.split("_")[2])
    await state.update_data(reply_ticket_id=ticket_id)

    await callback.message.answer(f"✍️ Введите ответ на тикет №{ticket_id}:")
    await state.set_state(AdminStates.waiting_for_ticket_reply)
    await callback.answer()


# 2. Обработка самого текста ответа
@dp.message(AdminStates.waiting_for_ticket_reply)
async def reply_ticket_send(message: types.Message, state: FSMContext):
    data = await state.get_data()
    ticket_id = data.get("reply_ticket_id")
    admin_reply = message.text

    # Получаем ID пользователя, которому отвечаем
    async with db.execute("SELECT user_id, message FROM tickets WHERE id = ?", (ticket_id,)) as c:
        ticket = await c.fetchone()

    if ticket:
        user_id, user_msg = ticket
        try:
            # Отправляем ответ пользователю
            text_to_user = (
                f"📩 <b>Ответ от поддержки по вашему тикету №{ticket_id}</b>\n"
                f"━━━━━━━━━━━━━━\n"
                f"<b>Ваш вопрос:</b> <i>{user_msg}</i>\n\n"
                f"<b>Ответ:</b> {admin_reply}"
            )
            await bot.send_message(user_id, text_to_user, parse_mode="HTML")

            # Закрываем тикет после ответа
            await db.execute("UPDATE tickets SET status = 'closed' WHERE id = ?", (ticket_id,))
            await db.commit()

            await message.answer(f"✅ Ответ отправлен пользователю {user_id}, тикет закрыт.")
            await log_admin_action(message.from_user.id, "REPLY_TICKET", f"Ticket ID: {ticket_id}")
        except Exception as e:
            await message.answer(
                f"❌ Не удалось отправить сообщение пользователю (возможно, бот в блоке). Ошибка: {e}")
    else:
        await message.answer("❌ Тикет не найден в базе.")

    await state.clear()


# --- ВДВОЕМ ---

@dp.callback_query(F.data == "duo_main")
async def duo_main(callback: types.CallbackQuery):
    builder = InlineKeyboardBuilder()
    builder.button(text="➕ Создать комнату", callback_data="duo_create")
    builder.button(text="🔑 Войти по коду", callback_data="duo_join")
    builder.button(text="🔙 Назад", callback_data="exit_to_menu")
    builder.adjust(1)
    # Если сообщение с картинкой, мы его удаляем и шлем текстовое
    if callback.message.photo:
        await callback.message.delete()
        await callback.message.answer("👥 Режим для двоих:", reply_markup=builder.as_markup())
    else:
        await callback.message.edit_text("👥 Режим для двоих:", reply_markup=builder.as_markup())


@dp.callback_query(F.data == "duo_create")
async def duo_create(callback: types.CallbackQuery):
    uid = callback.from_user.id

    # ПРОВЕРКА: Нет ли у пользователя уже активной комнаты
    if uid in user_to_room:
        # Можно вывести уведомление алертом (всплывающим окном)
        return await callback.answer(
            "⚠️ У вас уже есть активная комната!\nСначала завершите текущую сессию или дождитесь её закрытия.",
            show_alert=True
        )

    builder = InlineKeyboardBuilder()
    for g_id, g_name in GENRES.items():
        if g_id in ENABLED_GENRES:
            builder.button(text=g_name, callback_data=f"duogenre_{g_id}")

    builder.button(text="🍿 Любые", callback_data="duogenre_all").adjust(2)
    builder.button(text="🔙 Назад", callback_data="duo_main")  # Добавил кнопку назад для удобства

    await callback.message.edit_text("Выберите жанр для совместного поиска:", reply_markup=builder.as_markup())


async def auto_close_room(rid, creator_id):
    await asyncio.sleep(300)  # Ждем 300 секунд (5 минут)

    # Проверяем, существует ли еще комната и зашел ли в нее кто-то второй
    if rid in rooms:
        if len(rooms[rid]["users"]) < 2:
            # Если в комнате все еще только 1 человек — удаляем
            del rooms[rid]
            # Убираем привязку пользователя к этой комнате
            if creator_id in user_to_room and user_to_room[creator_id] == rid:
                del user_to_room[creator_id]

            try:
                await bot.send_message(
                    creator_id,
                    "⏰ <b>Время вышло!</b>\nНикто не подключился к комнате за 5 минут, она автоматически закрыта.",
                    parse_mode="HTML"
                )
            except:
                pass


async def watch_room_inactivity(rid):
    """Фоновая проверка активности комнаты"""
    while rid in rooms:
        await asyncio.sleep(60)  # Проверяем раз в минуту

        if rid not in rooms: break

        now = datetime.datetime.now()
        # Считаем сколько секунд прошло с последнего room["last_action"]
        diff = (now - rooms[rid]["last_action"]).total_seconds()

        if diff >= 600:  # 600 секунд = 10 минут
            uids = list(rooms[rid]["users"].keys())

            # Удаляем данные
            if rid in rooms: del rooms[rid]
            for u in uids:
                if u in user_to_room: del user_to_room[u]
                try:
                    await bot.send_message(u, "🔔 <b>Комната закрыта!</b>\nВы бездействовали более 10 минут.",
                                           parse_mode="HTML")
                except:
                    pass
            break



async def generate_unique_room_id():
    """Генерирует ID, которого точно нет в базе"""
    while True:
        new_id = random.randint(1000, 9999)
        async with db.execute("SELECT 1 FROM rooms WHERE room_id = ?", (new_id,)) as cursor:
            if not await cursor.fetchone():
                return new_id


@dp.callback_query(F.data.startswith("duogenre_"))
async def finish_create(callback: types.CallbackQuery):
    uid = callback.from_user.id

    # --- ИСПРАВЛЕНИЕ: Безопасный выход из старой комнаты перед созданием новой ---
    if uid in user_to_room:
        old_rid = user_to_room[uid]
        if old_rid in rooms:
            # Убираем только этого пользователя из списка участников
            if uid in rooms[old_rid]["users"]:
                del rooms[old_rid]["users"][uid]

            # Если в старой комнате никого не осталось — удаляем её совсем
            if not rooms[old_rid]["users"]:
                del rooms[old_rid]
            else:
                # Если кто-то остался, уведомляем его
                for partner_id in rooms[old_rid]["users"]:
                    try:
                        await bot.send_message(partner_id, "🚪 Ваш партнер покинул комнату. Сессия завершена.")
                        # Очищаем привязку партнера, так как вдвоем играть больше нельзя
                        if partner_id in user_to_room:
                            del user_to_room[partner_id]
                    except:
                        pass
                # После уведомления удаляем комнату, так как это режим для ДВОИХ
                if old_rid in rooms:
                    del rooms[old_rid]

        # Убираем привязку самого создателя
        del user_to_room[uid]
    # ----------------------------------------------------------------

    gid = callback.data.split("_")[1]
    gid = None if gid == "all" else gid

    # --- ИСПРАВЛЕНИЕ: Генерация УНИКАЛЬНОГО кода комнаты (защита от коллизий) ---
    while True:
        rid = str(random.randint(100000, 999999))
        if rid not in rooms:
            break
    # ---------------------------------------------------------------------------

    # Логика фильтрации (оставляем твою рабочую версию)
    # Используем глобальную db, которую открыли в main()
    async with db.execute("SELECT movie_id FROM user_votes WHERE user_id = ?", (uid,)) as c:
        rows = await c.fetchall()
        seen_ids = {str(r[0]) for r in rows}

    final_movies = []
    current_page = 1
    while len(final_movies) < 15 and current_page <= 5:
        movies_list = await fetch_movies_page(current_page, gid)
        if not movies_list: break
        filtered = [m for m in movies_list if str(m['id']) not in seen_ids]
        final_movies.extend(filtered)
        current_page += 1

    rooms[rid] = {
        "movies": final_movies,
        "users": {uid: {"idx": 0}},
        "is_solo": False,
        "last_page": current_page - 1,
        "genre_id": gid,
        "last_action": datetime.datetime.now()
    }
    user_to_room[uid] = rid

    asyncio.create_task(watch_room_inactivity(rid))

    kb = InlineKeyboardBuilder()
    kb.button(text="❌ Отменить и выйти", callback_data="exit_to_menu")

    await callback.message.edit_text(
        f"✅ Код: <code>{rid}</code>\nЖдем партнера...\n\n"
        f"<i>Передайте этот код другу. У вас есть 5 минут.</i>",
        parse_mode="HTML",
        reply_markup=kb.as_markup()
    )


@dp.callback_query(F.data == "duo_join")
async def duo_join(callback: types.CallbackQuery, state: FSMContext):
    await callback.message.edit_text("Введите код комнаты:")
    await state.set_state(MovieStates.waiting_for_room_code)


@dp.message(MovieStates.waiting_for_room_code)
async def process_code(message: types.Message, state: FSMContext):
    rid_raw = message.text.strip()
    uid = message.from_user.id

    # --- ПРОВЕРКА НА ЧИСЛО (Защита от текста) ---
    if not rid_raw.isdigit():
        return await message.answer("⚠️ Код комнаты должен состоять только из цифр. Попробуй еще раз:")

    rid = rid_raw  # Код прошел проверку, работаем дальше

    # Проверка существования комнаты в БД (или словаре, если ты еще не перенес все в БД)
    # Если ты используешь словарь rooms:
    if rid not in rooms:
        return await message.answer("❌ Код не найден.")

    # ПРОВЕРКА 1: Нельзя зайти в свою же комнату (уже добавлен в список участников)
    if uid in rooms[rid]["users"]:
        return await message.answer("❌ Вы уже являетесь участником этой комнаты.")

    # ПРОВЕРКА 2: Комната заполнена
    if len(rooms[rid]["users"]) >= 2:
        await state.clear()
        return await message.answer(
            "🚫 <b>Эта комната уже заполнена.</b>\nВ режиме для двоих может быть только 2 участника.",
            parse_mode="HTML"
        )

    # Если проверки пройдены, добавляем пользователя
    rooms[rid]["users"][uid] = {"idx": 0}
    user_to_room[uid] = rid
    await state.clear()

    await message.answer("✅ Подключено!")

    # Уведомляем всех участников о начале
    for user_id in rooms[rid]["users"]:
        await send_next_movie(user_id)


@dp.callback_query(F.data == "exit_room")
async def exit_room_handler(callback: types.CallbackQuery):
    uid = callback.from_user.id
    rid = user_to_room.get(uid)
    if not rid or rid not in rooms:
        return await callback.answer("Комната не найдена.")

    all_users = list(rooms[rid]["users"].keys())
    # Удаляем комнату сразу для всех
    if rid in rooms: del rooms[rid]

    for u in all_users:
        if u in user_to_room: del user_to_room[u]
        try:
            if u == uid:
                await bot.send_message(u, "🚪 Вы вышли из комнаты. Сессия завершена.")
            else:
                await bot.send_message(u, "🚪 Ваш партнер покинул комнату. Сессия завершена.")
        except:
            pass

    try:
        await callback.message.delete()
    except:
        pass


@dp.callback_query(F.data == "team_rem")
async def ad_team_rem_start(callback: types.CallbackQuery, state: FSMContext):
    await callback.message.answer("Введите ID админа, которого нужно снять:")
    await state.set_state(AdminStates.waiting_for_remove_admin_id)


@dp.message(AdminStates.waiting_for_remove_admin_id)
async def ad_team_rem_proc(message: types.Message, state: FSMContext):
    try:
        tid = int(message.text)

        # ЗАЩИТА: Нельзя снять права с главного админа
        if tid == SUPER_ADMIN_ID:
            return await message.answer("❌ Нельзя снять права с создателя бота.")

        await db.execute("DELETE FROM admins WHERE user_id = ?", (tid,))
        await db.commit()

        # УБИРАЕМ КНОПКУ /admin из его меню
        await refresh_admin_commands(tid, is_adding=False)

        await message.answer(f"✅ Юзер {tid} снят с поста админа. Кнопка меню удалена.")
    except ValueError:
        await message.answer("❌ Ошибка в ID.")
    await state.clear()

# --- ВСПОМОГАТЕЛЬНАЯ ФУНКЦИЯ ДЛЯ ОБНОВЛЕНИЯ МЕНЮ (добавь её в код) ---
async def refresh_admin_commands(user_id, is_adding=True):
    # Общие команды для всех
    base_cmds = [
        types.BotCommand(command="start", description="🏠 Главное меню"),
        types.BotCommand(command="profile", description="👤 Профиль")
    ]

    if is_adding:
        # Админ получает базу + команду admin
        admin_cmds = base_cmds + [types.BotCommand(command="admin", description="⚙️ Админка")]
        await bot.set_my_commands(admin_cmds, scope=types.BotCommandScopeChat(chat_id=user_id))
    else:
        # Обычный юзер получает только базу
        await bot.set_my_commands(base_cmds, scope=types.BotCommandScopeChat(chat_id=user_id))
# --- ЛАЙКИ ---

# 1. ОСНОВНАЯ ЛОГИКА ОТРИСОВКИ (вызываем из других функций)
async def render_likes_page(callback, movies, page, total_pages):
    # movies - это список кортежей (movie_id, movie_title)

    # Функция для параллельной загрузки постера
    async def fetch_poster(session, m_id):
        url = f"https://api.themoviedb.org/3/movie/{m_id}?api_key={TMDB_API_KEY}&language=ru-RU"
        try:
            async with session.get(url, timeout=2) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    path = data.get('poster_path')
                    return f"https://image.tmdb.org/t/p/w200{path}" if path else None
        except:
            return None
        return None

    # Запускаем все запросы одновременно (это ускоряет загрузку в 5 раз)
    async with aiohttp.ClientSession() as session:
        tasks = [fetch_poster(session, m[0]) for m in movies]
        await asyncio.gather(*tasks)  # Прогреваем кеш или проверяем доступность (опционально)

    text = f"❤️ <b>Ваши лайки (Страница {page}/{total_pages}):</b>\n\n"
    text += "<i>Нажмите на кнопку с названием, чтобы открыть описание и трейлер</i>\n\n"

    kb = InlineKeyboardBuilder()
    for i, (m_id, m_title) in enumerate(movies):
        num = (page - 1) * 5 + i + 1
        text += f"{num}. 🎬 <b>{m_title}</b>\n"
        # callback_data="info_{m_id}" теперь будет открывать карточку
        kb.button(text=f"🎥 {m_title}", callback_data=f"info_{m_id}_{page}")

    nav_btns = []
    if page > 1:
        nav_btns.append(types.InlineKeyboardButton(text="⬅️", callback_data=f"likes_page_{page - 1}"))
    if page < total_pages:
        nav_btns.append(types.InlineKeyboardButton(text="➡️", callback_data=f"likes_page_{page + 1}"))

    if nav_btns:
        kb.row(*nav_btns)

    kb.button(text="🔙 В главное меню", callback_data="exit_to_menu")
    kb.adjust(1)

    try:
        await callback.message.edit_text(text, reply_markup=kb.as_markup(), parse_mode="HTML")
    except:
        await callback.message.answer(text, reply_markup=kb.as_markup(), parse_mode="HTML")
        await callback.message.delete()



@dp.callback_query(F.data.startswith("info_"))
async def movie_info_handler(callback: types.CallbackQuery):
    parts = callback.data.split("_")
    movie_id = parts[1]
    from_page = parts[2] if len(parts) > 2 else 1  # Запоминаем страницу, чтобы вернуться

    url = f"https://api.themoviedb.org/3/movie/{movie_id}?api_key={TMDB_API_KEY}&language=ru-RU"

    conn = ProxyConnector.from_url(PROXY_URL)
    async with aiohttp.ClientSession(connector=conn) as session:
        async with session.get(url) as resp:
            if resp.status != 200:
                return await callback.answer("Ошибка TMDB")
            movie = await resp.json()

    m_title = movie.get('title', 'Без названия')
    caption = (
        f"🎬 <b>{m_title}</b>\n"
        f"⭐️ Рейтинг: {movie.get('vote_average', 0)}\n"
        f"📅 Дата выхода: {movie.get('release_date', 'Неизвестно')}\n\n"
        f"{movie.get('overview', 'Описание отсутствует.')[:400]}..."
    )

    kb = InlineKeyboardBuilder()
    trailer = await get_trailer_url(movie_id)
    if trailer:
        kb.button(text="📺 Смотреть трейлер", url=trailer)

    kb.button(text="🗑 Удалить из лайков", callback_data=f"confirm_del_{movie_id}_{from_page}")
    kb.button(text="🔙 Назад к списку", callback_data=f"likes_page_{from_page}")
    kb.adjust(1)

    poster_path = movie.get('poster_path')
    if poster_path:
        await callback.message.answer_photo(
            f"https://image.tmdb.org/t/p/w500{poster_path}",
            caption=caption,
            reply_markup=kb.as_markup(),
            parse_mode="HTML"
        )
        await callback.message.delete()
    else:
        await callback.message.edit_text(caption, reply_markup=kb.as_markup(), parse_mode="HTML")


# 2. ОБРАБОТЧИК КНОПКИ "МОИ ЛАЙКИ" И СТРЕЛОК
@dp.callback_query(F.data.startswith("show_my_likes"))
async def show_likes_handler(callback: types.CallbackQuery):
    uid = callback.from_user.id
    # Получаем все лайки (используем твою существующую функцию)
    all_likes = await get_full_likes(uid)

    if not all_likes:
        return await callback.answer("У вас пока нет лайков!", show_alert=True)

    items_per_page = 5
    total_pages = (len(all_likes) + items_per_page - 1) // items_per_page
    current_page = 1 # Всегда начинаем с первой страницы

    # Берем первые 5 фильмов
    page_movies = all_likes[0:items_per_page]

    # Передаем всё в функцию отрисовки
    await render_likes_page(callback, page_movies, current_page, total_pages)


# 3. ОБРАБОТЧИК УДАЛЕНИЯ
@dp.callback_query(F.data.startswith("confirm_del_"))
async def delete_like_handler(callback: types.CallbackQuery):
    data = callback.data.split("_")
    movie_id = data[2]
    current_page = int(data[3])  # Предположим, тут хранится номер страницы

    await delete_like(callback.from_user.id, movie_id)
    await callback.answer("Удалено 🗑")

    all_likes = await get_full_likes(callback.from_user.id)
    if not all_likes:
        try:
            await callback.message.delete()
        except:
            pass
        return await callback.message.answer("Список лайков теперь пуст!")

    # Пересчитываем страницы
    items_per_page = 5
    total_pages = (len(all_likes) + items_per_page - 1) // items_per_page

    if current_page > total_pages:
        current_page = total_pages

    start_idx = (current_page - 1) * items_per_page
    page_movies = all_likes[start_idx: start_idx + items_per_page]

    # ВЫЗОВ: передаем обновленные данные
    await render_likes_page(callback, page_movies, current_page, total_pages)

@dp.callback_query(F.data.startswith("likes_page_"))
async def likes_pagination_handler(callback: types.CallbackQuery):
    page = int(callback.data.split("_")[2])
    uid = callback.from_user.id
    all_likes = await get_full_likes(uid)

    items_per_page = 5
    total_pages = (len(all_likes) + items_per_page - 1) // items_per_page

    start_idx = (page - 1) * items_per_page
    page_movies = all_likes[start_idx: start_idx + items_per_page]

    await render_likes_page(callback, page_movies, page, total_pages)

@dp.callback_query(F.data.startswith("del_"))
async def handle_delete(callback: types.CallbackQuery):
    await delete_like(callback.from_user.id, callback.data.split("_")[1])
    await callback.message.edit_text("🗑 Удалено")


@dp.callback_query(F.data == "show_top_10")
async def show_top(callback: types.CallbackQuery):
    top = await get_global_top()
    if not top: return await callback.answer("Топ пуст!")
    text = "🔥 Топ-10:\n\n" + "\n".join([f"{i + 1}. {t} ({c})" for i, (t, c) in enumerate(top)])
    await callback.message.answer(text)


# --- СВАЙПЫ ---

@dp.callback_query(F.data.startswith("like_") | F.data.startswith("dislike_"))
async def handle_vote(callback: types.CallbackQuery):
    act, mid = callback.data.split("_")
    uid = callback.from_user.id
    rid = user_to_room.get(uid)

    if not rid or rid not in rooms:
        return await callback.answer("Сессия завершена или комната не найдена.")

    room = rooms[rid]

    # --- ОБНОВЛЯЕМ ВРЕМЯ АКТИВНОСТИ (ДЛЯ ТАЙМЕРА 10 МИНУТ) ---
    room["last_action"] = datetime.datetime.now()

    movie = next((m for m in room["movies"] if str(m['id']) == mid), None)
    if movie:
        # Записываем голос в БД
        await add_vote(uid, mid, movie['title'], 1 if act == "like" else 0)

        # Логика мэтча (только для дуо)
        if act == "like" and not room["is_solo"]:
            for o_uid in room["users"]:
                if o_uid != uid:
                    async with db.execute(
                            "SELECT 1 FROM user_votes WHERE user_id=? AND movie_id=? AND is_like=1",
                            (o_uid, mid)
                    ) as c:
                        if await c.fetchone():
                            # Уведомляем обоих участников
                            for u in room["users"]:
                                await bot.send_message(u, f"🥳 <b>МЭТЧ: {movie['title']}!</b>", parse_mode="HTML")

        # Переходим к следующему фильму
        room["users"][uid]["idx"] += 1

        # --- СИНХРОНИЗАЦИЯ ЛЕНТЫ (ДОБАВЛЕНО) ---
        # Если текущий пользователь дошел до конца подгруженного списка
        if room["users"][uid]["idx"] >= len(room["movies"]):
            next_page = room.get("last_page", 1) + 1
            # Подгружаем следующую страницу API
            new_movies = await fetch_movies_page(next_page, room.get("genre_id"))

            if new_movies:
                # Собираем ID фильмов, которые УЖЕ есть в этой комнате, чтобы не было дублей
                existing_ids = {str(m['id']) for m in room["movies"]}
                # Добавляем в общий список только уникальные новые фильмы
                for m in new_movies:
                    if str(m['id']) not in existing_ids:
                        room["movies"].append(m)

                room["last_page"] = next_page
        # ---------------------------------------

        try:
            await callback.message.delete()
        except:
            pass

        await send_next_movie(uid)


# --- АДМИН ПАНЕЛЬ ---

@dp.message(Command("admin"))
async def admin_panel(message: types.Message):
    if not await is_admin(message.from_user.id): return
    await message.answer("🛠 Админ-панель", reply_markup=get_admin_kb(message.from_user.id == SUPER_ADMIN_ID))


@dp.callback_query(F.data == "back_to_admin")
async def back_to_admin(callback: types.CallbackQuery, state: FSMContext):
    await state.clear()
    if callback.message.photo:
        await callback.message.delete()
        await callback.message.answer("🛠 Админ-панель",
                                      reply_markup=get_admin_kb(callback.from_user.id == SUPER_ADMIN_ID))
    else:
        await callback.message.edit_text("🛠 Админ-панель",
                                         reply_markup=get_admin_kb(callback.from_user.id == SUPER_ADMIN_ID))


# --- УПРАВЛЕНИЕ АДМИНАМИ ---

@dp.callback_query(F.data == "super_admin_menu")
async def super_menu(callback: types.CallbackQuery):
    if callback.from_user.id != SUPER_ADMIN_ID: return
    kb = InlineKeyboardBuilder()
    kb.button(text="➕ Добавить админа", callback_data="adm_add")
    kb.button(text="➖ Снять админа", callback_data="adm_rem")
    kb.button(text="📜 Список админов", callback_data="adm_list")
    kb.button(text="🔙 Назад", callback_data="back_to_admin")
    kb.adjust(1)
    await callback.message.edit_text("👑 Управление доступом:", reply_markup=kb.as_markup())


@dp.callback_query(F.data == "adm_list")
async def adm_list_show(callback: types.CallbackQuery):
    async with db.execute("SELECT user_id, added_at FROM admins") as c: rows = await c.fetchall()
    text = "📜 Админы:\n\n" + "\n".join([f"• <code>{r[0]}</code> ({r[1]})" for r in rows])
    await callback.message.edit_text(text, parse_mode="HTML", reply_markup=InlineKeyboardBuilder().button(text="🔙",
                                                                                                          callback_data="super_admin_menu").as_markup())


@dp.callback_query(F.data == "adm_add")
async def adm_add_start(callback: types.CallbackQuery, state: FSMContext):
    await callback.message.answer("Введите ID нового админа:")
    await state.set_state(AdminStates.waiting_for_new_admin_id)


@dp.message(AdminStates.waiting_for_new_admin_id)
async def ad_team_add_proc(message: types.Message, state: FSMContext):
    try:
        tid = int(message.text)
        await db.execute("INSERT OR IGNORE INTO admins VALUES (?,?)", (tid, get_now()))
        await db.commit()

        # ОБНОВЛЯЕМ МЕНЮ (теперь у него появится кнопка /admin)
        await refresh_admin_commands(tid, is_adding=True)

        try:
            await bot.send_message(tid,
                                   "👑 <b>Вам выданы права администратора!</b>\nИспользуйте команду /admin для доступа к панели.",
                                   parse_mode="HTML")
        except:
            pass

        await message.answer(f"✅ Юзер {tid} теперь админ. Кнопка меню обновлена.")
    except ValueError:
        await message.answer("❌ Ошибка в ID.")
    await state.clear()


@dp.callback_query(F.data == "adm_rem")
async def adm_rem_start(callback: types.CallbackQuery, state: FSMContext):
    await callback.message.answer("Введите ID для снятия прав:")
    await state.set_state(AdminStates.waiting_for_remove_admin_id)




# --- ЧЕРНЫЙ СПИСОК ---

@dp.callback_query(F.data == "admin_blacklist_menu")
async def bl_menu(callback: types.CallbackQuery):
    kb = InlineKeyboardBuilder()
    kb.button(text="🚫 Забанить пользователя", callback_data="bl_add")
    kb.button(text="✅ Разбанить пользователя", callback_data="bl_rem")
    kb.button(text="📋 Список ЧС", callback_data="bl_list")
    kb.button(text="🔙 Назад", callback_data="back_to_admin")
    kb.adjust(1)
    await callback.message.edit_text("Черный список:", reply_markup=kb.as_markup())


@dp.callback_query(F.data == "bl_list")
async def bl_list_show(callback: types.CallbackQuery):
    async with db.execute(
            "SELECT user_id, blocked_at FROM users WHERE is_blocked = 1") as c: rows = await c.fetchall()
    text = "🚫 Забанены:\n\n" + "\n".join([f"• <code>{r[0]}</code> ({r[1]})" for r in rows])
    await callback.message.edit_text(text or "ЧС пуст", parse_mode="HTML",
                                     reply_markup=InlineKeyboardBuilder().button(text="🔙",
                                                                                 callback_data="admin_blacklist_menu").as_markup())


@dp.callback_query(F.data.startswith("bl_add") | F.data.startswith("bl_rem"))
async def bl_action(callback: types.CallbackQuery, state: FSMContext):
    await callback.message.answer("Введите ID пользователя:")
    await state.update_data(act=callback.data)
    await state.set_state(AdminStates.waiting_for_blacklist_id)


@dp.message(AdminStates.waiting_for_blacklist_id)
async def ad_bl_proc(message: types.Message, state: FSMContext):
    try:
        tid = int(message.text)

        # ЗАЩИТА: Иммунитет супер-админа от бана
        if tid == SUPER_ADMIN_ID:
            return await message.answer("🛡️ Этот пользователь имеет иммунитет. Его невозможно забанить.")

        async with db.execute("SELECT is_blocked FROM users WHERE user_id=?", (tid,)) as c:
            r = await c.fetchone()

        new_s = 0 if r and r[0] == 1 else 1
        await db.execute("UPDATE users SET is_blocked=? WHERE user_id=?", (new_s, tid))
        await db.commit()

        # --- УВЕДОМЛЕНИЕ ПОЛЬЗОВАТЕЛЯ ---
        try:
            if new_s == 1:
                await bot.send_message(tid, "🚫 <b>Администрация ограничила вам доступ к боту.</b>", parse_mode="HTML")
            else:
                await bot.send_message(tid, "✅ <b>Ваш доступ к боту восстановлен!</b>", parse_mode="HTML")
        except:
            pass

        await message.answer(f"✅ Статус юзера {tid} изменен на {'БАН' if new_s else 'АКТИВЕН'}")
    except ValueError:
        await message.answer("❌ Ошибка: Введите корректный числовой ID.")
    await state.clear()


# --- ДЕТАЛЬНАЯ СТАТИСТИКА / ПРОФИЛЬ ---

@dp.callback_query(F.data == "admin_list_users")
async def list_users_admin(callback: types.CallbackQuery, state: FSMContext):
    async with db.execute(
            "SELECT user_id, first_name FROM users ORDER BY rowid DESC LIMIT 10") as c: users = await c.fetchall()
    text = "👤 Последние пользователи:\n\n" + "\n".join([f"• <code>{u[0]}</code> - {u[1]}" for u in users])
    text += "\n\nВведите ID для детальной статистики:"
    await state.set_state(AdminStates.waiting_for_profile_view)
    await callback.message.edit_text(text, parse_mode="HTML", reply_markup=InlineKeyboardBuilder().button(text="🔙",
                                                                                                          callback_data="back_to_admin").as_markup())


@dp.message(AdminStates.waiting_for_profile_view)
async def view_profile(message: types.Message, state: FSMContext):
    try:
        uid = int(message.text.strip())

        async with db.execute("SELECT * FROM users WHERE user_id = ?", (uid,)) as c: user = await c.fetchone()
        if not user: return await message.answer("Пользователь не найден.")

        async with db.execute("SELECT COUNT(*), SUM(is_like) FROM user_votes WHERE user_id = ?", (uid,)) as c:
            res = await c.fetchone()
            total_v, likes_v = res[0], res[1] or 0

        recent = await get_full_likes(uid, 10)
        likes_str = "\n".join([f"  └ {l[1]}" for l in recent]) if recent else "  (нет)"

        text = (f"👤 <b>Профиль:</b> {user[1]}\n"
                f"🆔 ID: <code>{user[0]}</code>\n"
                f"📅 Регистрация: {user[3]}\n"
                f"🕒 Активность: {user[4]}\n\n"
                f"📊 <b>Действия:</b>\n"
                f"├ Свайпов: {total_v}\n"
                f"└ Лайков: {likes_v}\n\n"
                f"❤️ <b>Лайкнутые фильмы (последние 10):</b>\n{likes_str}")
        await message.answer(text, parse_mode="HTML")
    except:
        await message.answer("Ошибка.")
    await state.clear()


# --- ЛОГИ ---

@dp.callback_query(F.data == "admin_logs_actions")
async def show_act_logs(callback: types.CallbackQuery):
    async with db.execute(
            "SELECT admin_id, action, details, timestamp FROM admin_logs ORDER BY id DESC LIMIT 15") as c: logs = await c.fetchall()
    text = "📜 <b>Логи действий админов:</b>\n\n" + "\n".join([f"🕒 {l[3]}\n👤 {l[0]}: {l[1]} ({l[2]})\n" for l in logs])
    await callback.message.edit_text(text or "Логи пусты", parse_mode="HTML",
                                     reply_markup=InlineKeyboardBuilder().button(text="🔙",
                                                                                 callback_data="back_to_admin").as_markup())


@dp.callback_query(F.data == "admin_logs_errors")
async def show_err_logs(callback: types.CallbackQuery):
    async with db.execute("SELECT error, time FROM logs ORDER BY id DESC LIMIT 10") as c: logs = await c.fetchall()
    text = "⚠️ <b>Ошибки системы:</b>\n\n" + "\n".join([f"🕒 {l[1]}\n❌ {l[0][:100]}\n" for l in logs])
    await callback.message.edit_text(text or "Ошибок нет", parse_mode="HTML",
                                     reply_markup=InlineKeyboardBuilder().button(text="🔙",
                                                                                 callback_data="back_to_admin").as_markup())



# --- РАССЫЛКА ---
@dp.callback_query(
    F.data == "admin_broadcast_start")  # Оставляем старый callback, чтобы не менять кнопки в главном меню
async def broadcast_manage_menu(callback: types.CallbackQuery):
    kb = InlineKeyboardBuilder()
    kb.button(text="➕ Создать рассылку", callback_data="br_create_new")
    kb.button(text="📋 Очередь рассылок", callback_data="view_active_broadcasts")
    kb.button(text="🔙 Назад", callback_data="back_to_admin")
    kb.adjust(1)

    await callback.message.edit_text(
        "📢 <b>Управление рассылками</b>\n\nВыберите нужное действие:",
        reply_markup=kb.as_markup(),
        parse_mode="HTML"
    )


@dp.callback_query(F.data == "admin_broadcast_start")
async def broadcast_manage_menu(callback: types.CallbackQuery):
    # Это меню с 2 кнопками, о которых ты просил
    kb = InlineKeyboardBuilder()
    kb.button(text="➕ Создать рассылку", callback_data="br_create_new")
    kb.button(text="📋 Очередь рассылок", callback_data="view_active_broadcasts")
    kb.button(text="🔙 Назад", callback_data="back_to_admin")
    kb.adjust(1)

    await callback.message.edit_text(
        "📢 <b>Управление рассылками</b>\n\nВыберите нужное действие:",
        reply_markup=kb.as_markup(),
        parse_mode="HTML"
    )


@dp.callback_query(F.data == "br_create_new")
async def br_start_process(callback: types.CallbackQuery, state: FSMContext):
    await state.clear()
    await callback.message.answer("📝 <b>Введите сообщение для рассылки:</b>\n(Текст, фото, видео или кружочек)", parse_mode="HTML")
    await state.set_state(AdminStates.waiting_for_broadcast_content)


@dp.message(AdminStates.waiting_for_broadcast_content)
async def br_content(message: types.Message, state: FSMContext):
    await state.update_data(mid=message.message_id, cid=message.chat.id)
    await message.answer(
        "⏳ <b>Введите время рассылки</b> в формате <code>ЧЧ:ММ</code>\n"
        "Например: <code>15:30</code> или <code>09:00</code>\n\n"
        "<i>Если нужно отправить прямо сейчас — введите 0.</i>",
        parse_mode="HTML"
    )
    await state.set_state(AdminStates.waiting_for_broadcast_time)


@dp.message(AdminStates.waiting_for_broadcast_time)
async def br_time(message: types.Message, state: FSMContext):
    time_input = message.text.strip()
    if time_input == "0":
        await state.update_data(br_time="now")
    else:
        try:
            datetime.datetime.strptime(time_input, "%H:%M")
            await state.update_data(br_time=time_input)
        except ValueError:
            return await message.answer("❌ Формат ЧЧ:ММ!")

    kb = InlineKeyboardBuilder()
    kb.button(text="👥 Всем", callback_data="trg_all")
    kb.button(text="⚡ Активным", callback_data="trg_active")
    kb.adjust(1)
    await message.answer("Кому отправить?", reply_markup=kb.as_markup())
    await state.set_state(AdminStates.waiting_for_broadcast_target)

@dp.callback_query(AdminStates.waiting_for_broadcast_target)
async def br_preview(callback: types.CallbackQuery, state: FSMContext):
    target = "all" if "all" in callback.data else "active"
    await state.update_data(target=target)
    data = await state.get_data()

    kb = InlineKeyboardBuilder()
    kb.button(text="✅ ПОДТВЕРДИТЬ", callback_data="br_confirm")
    kb.button(text="❌ ОТМЕНИТЬ", callback_data="back_to_admin")
    kb.adjust(1)

    await callback.message.answer(f"👀 Предпросмотр (Цель: {target}, Время: {data['br_time']}):")
    # Копируем сообщение, чтобы админ видел, ЧТО он отправляет
    await bot.copy_message(callback.message.chat.id, data['cid'], data['mid'], reply_markup=kb.as_markup())
    await state.set_state(AdminStates.confirm_broadcast)


@dp.callback_query(F.data == "br_confirm", AdminStates.confirm_broadcast)
async def br_exec(callback: types.CallbackQuery, state: FSMContext):
    data = await state.get_data()
    await state.clear()

    # Удаляем предпросмотр, чтобы не путаться
    try:
        await callback.message.delete()
    except:
        pass

    # РАСЧЕТ ВРЕМЕНИ ТОЛЬКО ЗДЕСЬ
    delay = 0
    if data['br_time'] != "now":
        now = datetime.datetime.now()
        target_time = datetime.datetime.strptime(data['br_time'], "%H:%M").replace(
            year=now.year, month=now.month, day=now.day, second=0, microsecond=0
        )
        if target_time < now:
            target_time += datetime.timedelta(days=1)
        delay = (target_time - now).total_seconds()

    if delay > 0:
        task_id = f"br_{random.randint(100, 999)}"
        # Создаем задачу только после подтверждения
        task = asyncio.create_task(
            run_delayed_broadcast(task_id, delay, data, callback.from_user.id, data['cid'], data['mid'])
        )
        active_broadcasts[task_id] = {
            "task": task, "time": data['br_time'], "target": data['target']
        }
        await callback.message.answer(f"⏳ Запланировано (ID: {task_id}) на {data['br_time']}.")
    else:
        await callback.message.answer("🚀 Рассылка запущена прямо сейчас!")
        await run_delayed_broadcast(None, 0, data, callback.from_user.id, data['cid'], data['mid'])


async def run_delayed_broadcast(task_id, delay, data, admin_id, from_chat, msg_id):
    if delay > 0:
        await asyncio.sleep(delay)

    uids = await get_targeted_user_ids(data['target'])
    s, f = 0, 0
    for u in uids:
        try:
            await bot.copy_message(u, from_chat, msg_id)
            s += 1
            await asyncio.sleep(0.05)
        except:
            f += 1

    await bot.send_message(admin_id, f"📢 Рассылка {task_id or ''} завершена!\n✅ Успешно: {s}\n❌ Ошибок: {f}")
    if task_id in active_broadcasts:
        del active_broadcasts[task_id]


# --- УПРАВЛЕНИЕ ОЧЕРЕДЬЮ (ВЫНЕСЕНО НАРУЖУ) ---

@dp.callback_query(F.data == "view_active_broadcasts")
@dp.message(Command("admin_broadcasts"))
async def list_broadcasts(message: Union[types.Message, types.CallbackQuery]):
    uid = message.from_user.id
    if not await is_admin(uid): return

    if not active_broadcasts:
        text = "📭 Нет запланированных рассылок."
        if isinstance(message, types.CallbackQuery):
            return await message.answer(text)
        return await message.answer(text)

    text = "📋 <b>Запланированные рассылки:</b>\n\n"
    kb = InlineKeyboardBuilder()
    for tid, info in active_broadcasts.items():
        text += f"🆔 <code>{tid}</code> | ⏰ {info['time']} | 👥 {info['target']}\n"
        kb.button(text=f"❌ Отменить {tid}", callback_data=f"cancel_br_{tid}")

    kb.adjust(1)
    kb.row(types.InlineKeyboardButton(text="🔙 Назад", callback_data="admin_broadcast_start"))

    if isinstance(message, types.CallbackQuery):
        await message.message.edit_text(text, reply_markup=kb.as_markup(), parse_mode="HTML")
    else:
        await message.answer(text, reply_markup=kb.as_markup(), parse_mode="HTML")


@dp.callback_query(F.data.startswith("cancel_br_"))
async def cancel_broadcast_handler(callback: types.CallbackQuery):
    tid = callback.data.replace("cancel_br_", "")
    if tid in active_broadcasts:
        active_broadcasts[tid]["task"].cancel()
        del active_broadcasts[tid]
        await callback.answer("✅ Рассылка отменена", show_alert=True)
        await list_broadcasts(callback)
    else:
        await callback.answer("❌ Рассылка не найдена")


@dp.callback_query(F.data == "admin_stats")
async def admin_stats_pro(callback: types.CallbackQuery):

    # 1. Общие данные аудитории
    async with db.execute("SELECT COUNT(*) FROM users") as c: total = (await c.fetchone())[0]
    async with db.execute("SELECT COUNT(*) FROM users WHERE joined_date > datetime('now', '-1 day')") as c: new_24 = \
    (await c.fetchone())[0]
    async with db.execute("SELECT COUNT(*) FROM users WHERE last_active > datetime('now', '-1 day')") as c: act_24 = \
    (await c.fetchone())[0]

    # 2. Хит дня (фильм с наибольшим кол-вом лайков сегодня)
    async with db.execute("""
        SELECT movie_title, COUNT(*) as count 
        FROM user_votes 
        WHERE is_like = 1 
          AND movie_title IS NOT NULL 
          AND movie_title != ''
          AND added_at > datetime('now', 'start of day')
        GROUP BY movie_title 
        ORDER BY count DESC LIMIT 1
    """) as c: best_movie = await c.fetchone()

    # 3. Активные сессии (Соло / Дуо)
    current_solo = len([r for r in rooms.values() if r.get('is_solo') is True])
    current_duo = len([r for r in rooms.values() if r.get('is_solo') is False])

    # 4. ТОП ЖАНРОВ СЕГОДНЯ (на основе активных сессий)
    # Маппинг ID жанров TMDB в читаемые названия
    genre_mapping = {
        '28': 'Боевик', '12': 'Приключения', '16': 'Мультфильм', '35': 'Комедия',
        '80': 'Криминал', '99': 'Документальный', '18': 'Драма', '10751': 'Семейный',
        '14': 'Фэнтези', '36': 'История', '27': 'Ужасы', '10402': 'Музыка',
        '9648': 'Детектив', '10749': 'Мелодрама', '878': 'Фантастика', '10770': 'ТВ фильм',
        '53': 'Триллер', '10752': 'Военный', '37': 'Вестерн', None: 'Все жанры'
    }

    from collections import Counter
    # Собираем все genre_id из всех активных комнат
    all_active_genres = [str(r.get('genre_id')) if r.get('genre_id') else None for r in rooms.values()]
    genre_counts = Counter(all_active_genres).most_common(3)

    genres_top_text = ""
    for g_id, count in genre_counts:
        g_name = genre_mapping.get(g_id, "Неизвестно")
        genres_top_text += f"   • {g_name}: <b>{count}</b> сессий\n"

    # 5. Уникальные свайперы за сегодня
    async with db.execute(
            "SELECT COUNT(DISTINCT user_id) FROM user_votes WHERE added_at > datetime('now', 'start of day')") as c:
        unique_users_today = (await c.fetchone())[0]

    # 6. Топ фанатов
    async with db.execute("""
        SELECT user_id, COUNT(*) as cnt 
        FROM user_votes 
        GROUP BY user_id 
        ORDER BY cnt DESC LIMIT 3
    """) as c: top_fans = await c.fetchall()

    # 7. Тикеты
    async with db.execute("SELECT COUNT(*) FROM tickets WHERE status = 'open'") as c: open_tickets = \
    (await c.fetchone())[0]

    fans_text = ""
    for i, (fid, fcnt) in enumerate(top_fans, 1):
        fans_text += f"   {i}. <code>{fid}</code> — <b>{fcnt}</b>\n"

    movie_display = f"«{best_movie[0]}»" if best_movie else "Нет данных"
    likes_display = f"(👍 {best_movie[1]})" if best_movie else ""

    text = (
        f"📊 <b>ПРОФЕССИОНАЛЬНАЯ АНАЛИТИКА</b>\n"
        f"━━━━━━━━━━━━━━\n"
        f"👥 <b>Аудитория:</b>\n"
        f"├ Всего: <code>{total}</code>\n"
        f"├ Новых (24ч): <code>{new_24}</code>\n"
        f"└ Активных (24ч): <code>{act_24}</code>\n\n"
        f"🎮 <b>Сессии сейчас:</b>\n"
        f"├ 👤 Соло: <b>{current_solo}</b> | 👥 Вдвоем: <b>{current_duo}</b>\n"
        f"└ Уник. юзеров сегодня: <code>{unique_users_today}</code>\n\n"
        f"🔝 <b>Популярные жанры:</b>\n"
        f"{genres_top_text or '   (активных сессий нет)'}\n"
        f"🔥 <b>Хит дня:</b>\n"
        f"└ {movie_display} {likes_display}\n\n"
        f"🏆 <b>Топ по свайпам:</b>\n"
        f"{fans_text or '   (данных пока нет)'}\n"
        f"📩 <b>Поддержка:</b>\n"
        f"└ Открытых тикетов: <b>{open_tickets}</b>\n"
        f"━━━━━━━━━━━━━━\n"
        f"🕒 <i>Обновлено: {datetime.datetime.now().strftime('%H:%M:%S')}</i>"
    )

    kb = InlineKeyboardBuilder()
    if open_tickets > 0:
        kb.button(text="📩 К тикетам", callback_data="admin_tickets")
    kb.button(text="🔄 Обновить", callback_data="admin_stats")
    kb.button(text="🔙 Назад", callback_data="back_to_admin")

    try:
        await callback.message.edit_text(text, reply_markup=kb.adjust(1).as_markup(), parse_mode="HTML")
    except Exception as e:
        if "message is not modified" not in str(e):
            await callback.answer("❌ Ошибка обновления")


@dp.callback_query(F.data == "admin_content")
async def admin_content(callback: types.CallbackQuery):
    builder = InlineKeyboardBuilder()
    for gid, name in GENRES.items():
        st = "✅" if gid in ENABLED_GENRES else "❌"
        builder.button(text=f"{st} {name}", callback_data=f"tgl_{gid}")
    builder.adjust(2).row(types.InlineKeyboardButton(text="🔙 Назад", callback_data="back_to_admin"))
    await callback.message.edit_text("🎭 Управление жанрами:", reply_markup=builder.as_markup())


@dp.callback_query(F.data.startswith("tgl_"))
async def toggle_genre(callback: types.CallbackQuery):
    gid = callback.data.split("_")[1]
    if gid in ENABLED_GENRES:
        ENABLED_GENRES.remove(gid)
    else:
        ENABLED_GENRES.append(gid)
    await admin_content(callback)


@dp.callback_query(F.data == "admin_tickets")
async def view_tickets_list(callback: types.CallbackQuery):
    async with db.execute(
            "SELECT id, user_id, message FROM tickets WHERE status = 'open' ORDER BY created_at DESC"
    ) as c:
        tickets = await c.fetchall()

    kb = InlineKeyboardBuilder()

    if not tickets:
        text = "📩 <b>Новых обращений нет.</b>\nВы можете проверить архив закрытых тикетов:"
    else:
        text = f"📩 <b>Список открытых обращений ({len(tickets)}):</b>\nВыберите тикет для работы:"
        for t_id, u_id, t_message in tickets:
            # ИСПРАВЛЕНИЕ: если в базе по какой-то причине None, подменяем на строку
            safe_msg = t_message if t_message is not None else "[Медиа-файл]"

            # Теперь len() не вызовет ошибку
            short_msg = (safe_msg[:20] + '..') if len(safe_msg) > 20 else safe_msg
            kb.button(text=f"№{t_id} | {short_msg}", callback_data=f"open_ticket_{t_id}")

    # Кнопки управления (вне цикла!)
    kb.button(text="📜 Архив (закрытые)", callback_data="admin_tickets_history")
    kb.button(text="🔙 Назад в админку", callback_data="back_to_admin")

    await callback.message.edit_text(text, reply_markup=kb.adjust(1).as_markup(), parse_mode="HTML")


# ЭТА ФУНКЦИЯ ДОЛЖНА БЫТЬ СНАРУЖИ (на одном уровне с остальными)
@dp.callback_query(F.data.startswith("open_ticket_"))
async def show_specific_ticket(callback: types.CallbackQuery):
    ticket_id = int(callback.data.split("_")[2])

    async with db.execute(
            "SELECT id, user_id, message, created_at, status FROM tickets WHERE id = ?", (ticket_id,)
    ) as c:
        ticket = await c.fetchone()

    if not ticket:
        return await callback.answer("Тикет не найден")

    t_id, u_id, t_msg, t_time, t_status = ticket

    status_emoji = "🟢 Открыт" if t_status == "open" else "🔴 Закрыт (Архив)"

    text = (
        f"📋 <b>Тикет №{t_id}</b> ({status_emoji})\n"
        f"👤 От пользователя: <code>{u_id}</code>\n"
        f"⏰ Создан: <code>{t_time}</code>\n"
        f"━━━━━━━━━━━━━━\n"
        f"💬 Сообщение:\n{t_msg or '[Без текста]'}"
    )

    kb = InlineKeyboardBuilder()

    if t_status == "open":
        # Если тикет открыт — показываем кнопки управления
        kb.button(text="✍️ Ответить", callback_data=f"reply_ticket_{t_id}")
        kb.button(text="✅ Закрыть без ответа", callback_data=f"close_ticket_{t_id}")

    # Кнопка возврата в зависимости от того, откуда мы пришли
    back_target = "admin_tickets_history" if t_status == "closed" else "admin_tickets"
    kb.button(text="🔙 Назад", callback_data=back_target)

    await callback.message.edit_text(text, reply_markup=kb.adjust(1).as_markup(), parse_mode="HTML")


@dp.callback_query(F.data.startswith("close_ticket_"))
async def close_ticket_no_reply(callback: types.CallbackQuery):
    # Извлекаем ID тикета из callback_data (close_ticket_ID)
    ticket_id = int(callback.data.split("_")[2])

    # Проверяем, существует ли тикет
    async with db.execute("SELECT user_id FROM tickets WHERE id = ?", (ticket_id,)) as c:
        ticket = await c.fetchone()

    if not ticket:
        await callback.answer("Ошибка: Тикет не найден в базе.")
        return

    # Обновляем статус на 'closed'
    await db.execute(
        "UPDATE tickets SET status = 'closed' WHERE id = ?",
        (ticket_id,)
    )
    await db.commit()

    # Логируем действие админа
    await log_admin_action(callback.from_user.id, "CLOSE_TICKET", f"Ticket #{ticket_id} closed without reply")

    await callback.answer(f"✅ Тикет №{ticket_id} закрыт", show_alert=False)

    # Возвращаем админа к списку открытых тикетов
    await view_tickets_list(callback)


# И хендлер истории тоже должен быть здесь
@dp.callback_query(F.data == "admin_tickets_history")
async def view_tickets_history(callback: types.CallbackQuery):

    # Берем последние 15 закрытых тикетов
    async with db.execute(
            "SELECT id, user_id, message FROM tickets WHERE status = 'closed' ORDER BY created_at DESC LIMIT 15"
    ) as c:
        tickets = await c.fetchall()

    kb = InlineKeyboardBuilder()

    if not tickets:
        text = "📜 <b>Архив пуст.</b>\nЗакрытых обращений пока нет."
    else:
        text = "📜 <b>Последние 15 закрытых тикетов:</b>\nНажмите, чтобы прочитать полностью:"
        for t_id, u_id, t_message in tickets:
            # Наша "безопасная" проверка текста (чтобы не было ошибки None)
            safe_msg = t_message if t_message is not None else "[Медиа/Фото]"
            short_msg = (safe_msg[:20] + '..') if len(safe_msg) > 20 else safe_msg
            kb.button(text=f"✅ №{t_id} | {short_msg}", callback_data=f"open_ticket_{t_id}")

    kb.button(text="🔙 Назад к активным", callback_data="admin_tickets")
    kb.adjust(1)

    await callback.message.edit_text(text, reply_markup=kb.as_markup(), parse_mode="HTML")


@dp.callback_query(F.data == "admin_active_rooms")
async def show_active_rooms(callback: types.CallbackQuery):
    # 1. Проверка прав (используем твой SUPER_ADMIN_ID)
    if callback.from_user.id != SUPER_ADMIN_ID:
        return await callback.answer("Доступ запрещен", show_alert=True)

    # 2. ФИЛЬТРАЦИЯ: Оставляем только те комнаты, где is_solo: False
    # Это уберет одиночек из списка ожидания
    active_duo_rooms = {
        rid: data for rid, data in rooms.items()
        if not data.get("is_solo", False)
    }

    # 3. Формирование текста
    if not active_duo_rooms:
        text = "📭 <b>Сейчас нет активных совместных комнат.</b>\n"
    else:
        text = f"🏠 <b>Активные пары ({len(active_duo_rooms)}):</b>\n\n"
        for rid, data in active_duo_rooms.items():
            users_count = len(data['users'])
            # Если 1 юзер - ждет, если 2 - уже мэтчат
            status = "👥 В процессе" if users_count > 1 else "⏳ Ожидание партнера"

            # Получаем название жанра из твоего словаря GENRES
            genre_id = data.get('genre_id')
            genre_name = GENRES.get(genre_id, "Любые") if genre_id else "Любые"

            text += (f"🔹 <b>ID:</b> <code>{rid}</code>\n"
                     f"├ Статус: {status}\n"
                     f"├ Юзеров: {users_count}/2\n"
                     f"└ Жанр: {genre_name}\n\n")

    # 4. Добавляем время (чтобы избежать ошибки TelegramBadRequest про отсутствие изменений)
    import datetime
    now = datetime.datetime.now().strftime("%H:%M:%S")
    text += f"<i>Последнее обновление: {now}</i>"

    # 5. Создание клавиатуры
    kb = InlineKeyboardBuilder()
    kb.button(text="🔄 Обновить", callback_data="admin_active_rooms")
    kb.button(text="🔙 Назад", callback_data="back_to_admin")
    kb.adjust(1)

    # 6. Безопасное редактирование сообщения
    try:
        await callback.message.edit_text(
            text,
            parse_mode="HTML",
            reply_markup=kb.as_markup()
        )
    except TelegramBadRequest as e:
        if "message is not modified" in str(e):
            # Если данные совсем не изменились (даже секунды), просто мигнем алертом
            await callback.answer("Данные актуальны")
        else:
            # Если возникла другая ошибка - выводим её
            raise e


@dp.callback_query(F.data == "admin_cleanup_menu")
async def cleanup_menu(callback: types.CallbackQuery):
    if callback.from_user.id != SUPER_ADMIN_ID:
        return await callback.answer("Доступ запрещен")

    text = (
        "🗑 <b>Инструменты очистки</b>\n\n"
        "• <b>Комнаты:</b> удалит все текущие сессии (дуо-режим).\n"
        "• <b>Тикеты:</b> удалит из базы все закрытые обращения.\n"
    )

    kb = InlineKeyboardBuilder()
    kb.button(text="🏠 Сбросить комнаты", callback_data="clean_rooms")
    kb.button(text="🎫 Удалить архив тикетов", callback_data="clean_tickets")
    kb.button(text="🔙 Назад", callback_data="back_to_admin")
    kb.adjust(1)

    try:
        await callback.message.edit_text(text, reply_markup=kb.as_markup(), parse_mode="HTML")
    except TelegramBadRequest as e:
        if "message is not modified" in str(e):
            # Если текст тот же самый, просто ничего не делаем
            await callback.answer()
        else:
            raise e


# --- Очистка комнат ---
@dp.callback_query(F.data == "clean_rooms")
async def clean_rooms_proc(callback: types.CallbackQuery):
    global rooms, user_to_room
    count = len(rooms)

    # Полная очистка словарей в памяти
    rooms.clear()
    user_to_room.clear()

    await callback.answer(f"✅ Удалено комнат: {count}", show_alert=True)
    await cleanup_menu(callback)  # Возвращаемся в меню


# --- Очистка архива тикетов из БД ---
@dp.callback_query(F.data == "clean_tickets")
async def clean_tickets_proc(callback: types.CallbackQuery):

    # Считаем, сколько закроем
    async with db.execute("SELECT COUNT(*) FROM tickets WHERE status='closed'") as c:
        count = (await c.fetchone())[0]

    # Удаляем
    await db.execute("DELETE FROM tickets WHERE status='closed'")
    await db.commit()

    await callback.answer(f"🗑 Удалено из архива: {count} тикетов", show_alert=True)
    await cleanup_menu(callback)

# --- ЗАПУСК ---

@dp.callback_query(F.data == "solo_filters")
async def solo_filters(callback: types.CallbackQuery):
    uid = callback.from_user.id

    # --- ЛОГИКА ЗАКРЫТИЯ КОМНАТЫ ПРИ УХОДЕ В СОЛО ---
    if uid in user_to_room:
        rid = user_to_room[uid]
        if rid in rooms:
            # Уведомляем других участников комнаты (если они есть)
            other_users = [u for u in rooms[rid]["users"] if u != uid]
            for other_id in other_users:
                try:
                    await bot.send_message(
                        other_id,
                        "🚪 Ваш партнер ушел в соло-режим. Комната закрыта."
                    )
                    # Убираем связь с комнатой для партнера
                    if other_id in user_to_room:
                        del user_to_room[other_id]
                except:
                    pass

            # Удаляем саму комнату из памяти
            del rooms[rid]

        # Убираем связь с комнатой для текущего пользователя
        del user_to_room[uid]
        await callback.answer("Вы вышли из комнаты", show_alert=False)
    # -----------------------------------------------

    builder = InlineKeyboardBuilder()
    for g_id, g_name in GENRES.items():
        if g_id in ENABLED_GENRES:
            builder.button(text=g_name, callback_data=f"genre_{g_id}")

    builder.button(text="🍿 Любые", callback_data="genre_all")
    # Добавляем кнопку возврата в самый конец
    builder.button(text="🔙 Назад", callback_data="exit_to_menu")

    # adjust(2) сделает жанры по парам, а кнопка "Назад" встанет внизу
    builder.adjust(2)

    if callback.message.photo:
        await callback.message.delete()
        await callback.message.answer("Выберите жанр для поиска:", reply_markup=builder.as_markup())
    else:
        await callback.message.edit_text("Выберите жанр для поиска:", reply_markup=builder.as_markup())


# Обработчик для команды /profile из меню
@dp.message(Command("profile"))
async def profile_command(message: types.Message):
    # Просто перенаправляем логику на основную функцию,
    # передавая message вместо callback
    await show_profile(message)

@dp.callback_query(F.data == "user_profile")
@dp.message(Command("profile"))
async def show_profile(event: Union[types.Message, types.CallbackQuery]):
    # Определяем, откуда пришел запрос
    if isinstance(event, types.CallbackQuery):
        uid = event.from_user.id
        user_name = event.from_user.first_name
        message = event.message
    else:
        uid = event.from_user.id
        user_name = event.from_user.first_name
        message = event

    stats = await get_user_stats(uid)

    if not stats:
        text = "У вас еще нет статистики. Начните свайпать!"
        if isinstance(event, types.CallbackQuery):
            return await event.answer(text, show_alert=True)
        return await message.answer(text)

    text = (
        f"👤 <b>ЛИЧНЫЙ КАБИНЕТ: {user_name}</b>\n"
        f"━━━━━━━━━━━━━━\n"
        f"🏅 Ранг: <b>{stats['rank']}</b>\n"
        f"🎭 Статус: <b>{stats['kino_status']}</b>\n"
        f"⏳ Эпоха: <b>{stats['epoch']}</b>\n"
        f"━━━━━━━━━━━━━━\n\n"
        f"📈 <b>Прогресс до {stats['next_rank']}:</b>\n"
        f"{stats['bar']} {stats['total']} свайпов\n\n"
        f"📊 <b>Детальная статистика:</b>\n"
        f"├ Всего просмотрено: <code>{stats['total']}</code>\n"
        f"├ Процент одобрения: <code>{stats['ratio']}%</code>\n"
        f"└ Регистрация: <code>{stats['joined']}</code>\n\n"
        f"💬 <i>{stats['mood_text']}</i>"
    )

    kb = InlineKeyboardBuilder()
    kb.button(text="❤️ Мои лайки", callback_data="show_my_likes")
    kb.button(text="🔙 В меню", callback_data="exit_to_menu")
    kb.adjust(1)

    if isinstance(event, types.CallbackQuery):
        if message.photo:
            await message.delete()
            await message.answer(text, reply_markup=kb.as_markup(), parse_mode="HTML")
        else:
            await message.edit_text(text, reply_markup=kb.as_markup(), parse_mode="HTML")
    else:
        await message.answer(text, reply_markup=kb.as_markup(), parse_mode="HTML")


@dp.callback_query(F.data.startswith("genre_"))
async def start_solo(callback: types.CallbackQuery):
    uid = callback.from_user.id

    # --- ИСПРАВЛЕНИЕ: Полная зачистка перед входом в соло ---
    if uid in user_to_room:
        old_rid = user_to_room[uid]
        if old_rid in rooms:
            del rooms[old_rid]
        del user_to_room[uid]
    # ------------------------------------------------------

    gid = callback.data.split("_")[1]
    gid = None if gid == "all" else gid

    # ВАЖНО: Добавляем await перед вызовом функции
    movies_list = await fetch_movies_page(1, gid)

    # Создаем новую соло-комнату
    rooms[f"s_{uid}"] = {
        "movies": movies_list, # Теперь тут список фильмов, а не задача
        "users": {uid: {"idx": 0}},
        "is_solo": True,
        "last_page": 1,
        "genre_id": gid
    }
    user_to_room[uid] = f"s_{uid}"

    try:
        await callback.message.delete()
    except:
        pass
    await send_next_movie(uid)


async def main():
    global bot, http_client, db  # db теперь инициализируется один раз здесь

    # 1. Инициализируем соединение с БД (открываем "трубу")
    db = await aiosqlite.connect(DB_PATH)
    # Это позволит доставать данные по именам колонок: row["user_id"]
    db.row_factory = aiosqlite.Row

    # Настройка прокси для aiohttp
    connector = ProxyConnector.from_url(PROXY_URL)

    # Инициализируем одну общую сессию для HTTP запросов (TMDB)
    http_client = aiohttp.ClientSession(connector=connector)

    # Настройка сессии самого бота (aiogram)
    session = AiohttpSession(proxy=PROXY_URL)
    bot = Bot(
        token=TELEGRAM_TOKEN,
        session=session,
        default=DefaultBotProperties(parse_mode=ParseMode.HTML)
    )

    # ХАК для SOCKS5 в aiogram 3.x:
    bot.session._connector = connector

    # Передаем управление в init_db (она теперь должна использовать глобальную db)
    await init_db()

    # Установка общих команд меню для всех пользователей
    await bot.set_my_commands(
        [BotCommand(command='/start', description='🏠 Главное меню')],
        scope=BotCommandScopeDefault()
    )

    # --- СИНХРОНИЗАЦИЯ АДМИН-КОМАНД ---
    # Обновляем меню для создателя
    await refresh_admin_commands(SUPER_ADMIN_ID, is_adding=True)

    # Теперь используем уже открытое соединение db вместо async with aiosqlite.connect
    async with db.execute("SELECT user_id FROM admins") as cursor:
        rows = await cursor.fetchall()
        for row in rows:
            try:
                # row[0] сработает, так как fetchall вернет список строк
                if row[0] != SUPER_ADMIN_ID:
                    await refresh_admin_commands(row[0], is_adding=True)
            except Exception as e:
                print(f"Ошибка обновления меню для админа {row[0]}: {e}")
    # -------------------------------------------------------

    try:
        print("Бот запущен через прокси...")
        await dp.start_polling(bot, skip_updates=True)
    finally:
        # ЗАКРЫВАЕМ ВСЕ РЕСУРСЫ
        await bot.session.close()
        if http_client:
            await http_client.close()
        if db:
            await db.close()  # Закрываем соединение с БД только при выключении
        print("Все сессии и база данных закрыты.")


if __name__ == '__main__':
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        # Ловим ручную остановку и просто выводим сообщение вместо ошибки
        print("\nБот остановлен пользователем")
    except Exception as e:
        # Ловим все остальные критические ошибки, если они будут
        print(f"\nКритическая ошибка: {e}")