import asyncio
import sqlite3
import os
from datetime import datetime, timedelta
import pytz
from telethon import TelegramClient, events
from telethon.sessions import StringSession
import logging
import re
import hashlib

# ===== КОНФИГУРАЦИЯ =====
API_ID = 24826804
API_HASH = '048e59c243cce6ff788a7da214bf8119'
SESSION_STRING = "1ApWapzMBuyYVL4A-V5WkBkaQm1u79hOAUMNuUgzJQ47-Rr9cv-ahgjpYKLeO5_XIKcar2tfqamYFk7QFUE0PhAkNH0l36kLkUKLxcVbTHKLF9eRg02bnbWFrYsWJWEV1VNsYDhTJ8-ruHVKX58LqzZ3YuufJZ0CK81HlRrGuFgT3sWLLf31TVwUa-L1wIqRRfbwPW3MSK_CmhCUWB7EjMEEb2aAnJa4Ek0-cz_JOwaQwxVvWD22BUHO9RQSSuYFTv2IkO6gEpr6M7mm6_TymMhmIrkg5qGo-Fh05a2wO5d0xavPGdzg_4cjemdXWjvepFL0P3o_5SO8MvGAjnVYdTGVKekXwsRA="
BOT_TOKEN = '8306634056:AAEXAd3P6TnH7OgpVoYCoI1FezacXtJuei8'

CHANNELS = [
    'gubernator_46', 'kursk_info46', 'Alekhin_Telega', 'rian_ru',
    'kursk_ak46', 'zhest_kursk_146', 'novosti_efir', 'kursk_tipich',
    'seymkursk', 'kursk_smi', 'kursk_russia', 'belgorod01', 'kurskadm',
    'incident46', 'kurskbomond', 'prigranichie_radar1', 'grohot_pgr',
    'kursk_nasv', 'mchs_46', 'patriot046', 'kursk_now', 'Hinshtein',
    'incidentkursk', 'zhest_belgorod', 'RVvoenkor', 'pb_032',
    'tipicl32', 'bryansk_smi', 'Ria_novosti_rossiya','criminalru','bra_32','br_gorod','br_zhest', 'pravdas', 'wargonzo', 'ploschadmedia', 
    'belgorod_smi','ssigny','rucriminalinfo','kurskiy_harakter','dva_majors','ENews112','mash','NewsRussias7',
]

SUBSCRIBERS_FILE = 'subscribers.txt'

# Фильтр спама
SPAM_PHRASES = [
    'get free', 'бесплатно', 'получите бесплатно', 'закажите сейчас',
    'скидка', 'акция', 'промокод', 'купить', 'продать', 'заказать',
    'перейдите по ссылке', 'нажмите здесь', 'подпишитесь', 'кликните',
    'диплом', 'курсовая', 'накрутка', 'подписчиков', 'лайков',
    'заработок', 'инвестиции', 'криптовалюта', 'бинарные опционы',
    'гарантия', 'результат', 'быстро', 'легко', 'выгодно', 
    'ракетная опасность', 'отбой', 'ракетной опасности', 
    'ОПАСНОСТЬ АТАКИ БПЛА', 'опасность атаки БПЛА', 'опасность атаки', 
    'отбой ракетной опасности', 'отбой опасности атаки БПЛА', 
    'ОТБОЙ опасности атаки БПЛА', 'доброе утро', 'спокойной ночи', 
    'ночной чат', 'устренний чат', 'ржать', 'угарные', 'ракетную опасность',
    'отзыв', 'родительский чат', 'чат',
]

# Важные ключевые слова
IMPORTANT_KEYWORDS = [
    'беспилотник', 'сбил', 'уничтожил', 'разминировал', 'задержал', 
    'арест', 'террорист', 'экстремист', 'минобороны', 'военкор', 
    'спецоперация', 'губернатор', 'президент', 'правительство', 
    'министр', 'встреча', 'переговоры', 'заявление', 'пожар', 
    'сгорел', 'мчс', 'полиция', 'суд', 'обстрел', 'катастрофа',
    'авария', 'взрыв', 'нападение', 'жертв', 'пострадал', 
    'наступление', 'оборона', 'военные', 'силовики', 'уголовное дело', 
    'возбудили', 'запрещено', 'санкции', 'убийство', 'ограбление',
    'заложники', 'похищение', 'теракт', 'диверсия', 'шпион'
]

MAX_MESSAGE_AGE_HOURS = 6

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# ===== ФУНКЦИИ ДЛЯ ПОДПИСЧИКОВ =====
def load_subscribers():
    try:
        with open(SUBSCRIBERS_FILE, 'r') as f:
            return [int(line.strip()) for line in f if line.strip()]
    except FileNotFoundError:
        return []

def save_subscribers(subscribers):
    with open(SUBSCRIBERS_FILE, 'w') as f:
        for user_id in subscribers:
            f.write(f"{user_id}\n")

def add_subscriber(user_id):
    subscribers = load_subscribers()
    if user_id not in subscribers:
        subscribers.append(user_id)
        save_subscribers(subscribers)
        logger.info(f"Новый подписчик: {user_id}")
    return subscribers

def remove_subscriber(user_id):
    subscribers = load_subscribers()
    if user_id in subscribers:
        subscribers.remove(user_id)
        save_subscribers(subscribers)
        logger.info(f"Отписался: {user_id}")
    return subscribers

# ===== ФУНКЦИИ ФИЛЬТРАЦИИ =====
def clean_text(text):
    text = re.sub(r'http\S+|@\w+|#\w+', '', text)
    text = re.sub(r'[^\w\s]', ' ', text)
    text = re.sub(r'\s+', ' ', text).strip()
    return text.lower()

def is_important_news(text):
    text_lower = text.lower()
    return any(keyword in text_lower for keyword in IMPORTANT_KEYWORDS)

def is_spam_message(text):
    text_lower = text.lower()
    
    if is_important_news(text):
        return False
    
    for phrase in SPAM_PHRASES:
        if phrase in text_lower:
            return True
    
    return False

def is_recent_message(message_date):
    utc_now = datetime.utcnow().replace(tzinfo=pytz.utc)
    message_age = utc_now - message_date
    return message_age <= timedelta(hours=MAX_MESSAGE_AGE_HOURS)

# ===== БАЗА ДАННЫХ =====
def init_db():
    conn = sqlite3.connect('telegram_parser.db')
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS sent_messages (
            message_hash TEXT PRIMARY KEY,
            channel TEXT,
            message_text TEXT,
            message_id INTEGER,
            sent_time DATETIME DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    conn.commit()
    return conn

def generate_message_hash(channel_name, message_text):
    text_to_hash = f"{channel_name}_{clean_text(message_text)}"
    return hashlib.md5(text_to_hash.encode()).hexdigest()

def is_message_sent(conn, message_hash):
    cursor = conn.cursor()
    cursor.execute("SELECT message_hash FROM sent_messages WHERE message_hash = ?", (message_hash,))
    return cursor.fetchone() is not None

def mark_message_as_sent(conn, message_hash, channel_name, message_text, message_id):
    cursor = conn.cursor()
    cursor.execute(
        "INSERT OR IGNORE INTO sent_messages (message_hash, channel, message_text, message_id) VALUES (?, ?, ?, ?)",
        (message_hash, channel_name, message_text[:500], message_id)
    )
    conn.commit()

# ===== ФУНКЦИИ ДЛЯ ПАРСЕРИНГА =====
def format_channel_name(channel_name):
    name_map = {
        'gubernator_46': 'Оперштаб Курской области',
        'kursk_info46': 'Твой Курский край',
        'Alekhin_Telega': 'Роман Алехин',
        'rian_ru': 'РИА Новости',
        'kursk_ak46': 'Актуальный Курск',
        'zhest_kursk_146': 'Жесть Курск',
        'novosti_efir': 'Прямой Эфир',
        'kursk_tipich': 'Типичный Курск',
        'seymkursk': 'Сейм: новости Курской области',
        'kursk_smi': 'Новости Курска и Области',
        'kursk_russia': 'Курск №1',
        'belgorod01': 'Белгород №1',
        'kurskadm': 'Курская область',
        'incident46': 'Инцидент Курск',
        'kurskbomond': 'Курский Бомонд',
        'prigranichie_radar1': 'Приграничный Радар',
        'grohot_pgr': 'Грохот приграничья',
        'kursk_nasv': 'Курск на связи',
        'mchs_46': 'МЧС Курской области',
        'patriot046': 'Патриот Курск',
        'kursk_now': 'Курск сейчас',
        'Hinshtein': 'Александр Хинштейн',
        'incidentkursk': 'ЧП Курское приграничье',
        'zhest_belgorod': 'Жесть Белгород',
        'RVvoenkor': 'Военкоры Русской Весны',
        'pb_032': 'Подслушано Брянск',
        'tipicl32': 'Типичный Брянск',
        'bryansk_smi': 'Новости Брянска и Области',
        'Ria_novosti_rossiya': 'Россия сейчас',
        'criminalru': 'Компромат Групп',
        'bra_32': 'Новости Брянска',
        'br_gorod': 'Город Брянск',
        'br_zhest': 'Жесть Брянск',
        'pravdas': 'ПС-Расследования',
        'wargonzo': 'WarGonzo',
        'ploschadmedia': 'Площадь',
        'belgorod_smi': 'Новости Белгорода и Области',
        'ssigny': 'Сигнал',
        'rucriminalinfo': 'ВЧК-ОГПУ',
        'kurskiy_harakter': 'Курский характер',
        'dva_majors': 'Два майора',
        'ENews112': '112',
        'mash': 'Mash',
        'NewsRussias7': 'Новости России'
    }
    return name_map.get(channel_name, channel_name)

def format_message_text(text):
    text = re.sub(r'\n\s*\n', '\n\n', text.strip())
    if len(text) > 3800:
        text = text[:3800] + "..."
    return text

def generate_message_url(channel_username, message_id):
    return f"https://t.me/{channel_username}/{message_id}"

def generate_channel_url(channel_username):
    return f"https://t.me/{channel_username}"

async def check_channel_for_new_messages(user_client, bot_client, db_conn, channel_name):
    try:
        messages = await user_client.get_messages(channel_name, limit=5)
        
        for message in messages:
            if not message.text or not message.text.strip():
                continue
            
            message_text = message.text.strip()
            
            if is_spam_message(message_text):
                logger.debug(f"Пропущен спам из {channel_name}")
                continue
            
            if not is_recent_message(message.date):
                logger.debug(f"Пропущено старое сообщение из {channel_name}")
                continue
            
            message_hash = generate_message_hash(channel_name, message_text)
            if is_message_sent(db_conn, message_hash):
                logger.debug(f"Сообщение уже отправлено из {channel_name}")
                continue
            
            # Форматируем сообщение
            formatted_text = format_message_text(message_text)
            message_url = generate_message_url(channel_name, message.id)
            channel_url = generate_channel_url(channel_name)
            formatted_channel = format_channel_name(channel_name)
            message_time = message.date.astimezone(pytz.timezone('Europe/Moscow')).strftime('%H:%M %d.%m.%Y')
            
            formatted_post = (
                f"📰 **{formatted_channel}**\n"
                f"🕒 {message_time}\n"
                f"━━━━━━━━━━━━━━━━━━━━\n"
                f"{formatted_text}\n"
                f"━━━━━━━━━━━━━━━━━━━━\n"
                f"🔗 [Открыть сообщение]({message_url}) | 📢 [Перейти в канал]({channel_url})"
            )
            
            # Отправляем подписчикам
            subscribers = load_subscribers()
            success_count = 0
            
            for user_id in subscribers:
                try:
                    await bot_client.send_message(
                        user_id, 
                        formatted_post, 
                        parse_mode='md',
                        link_preview=False
                    )
                    success_count += 1
                    await asyncio.sleep(0.5)
                except Exception as e:
                    logger.error(f"Ошибка отправки {user_id}: {e}")
            
            if success_count > 0:
                mark_message_as_sent(db_conn, message_hash, channel_name, message_text, message.id)
                logger.info(f"✅ Отправлена новость из {channel_name} для {success_count} подписчиков")
            
            break  # Отправляем только одно сообщение за проверку
        
    except Exception as e:
        logger.error(f"❌ Ошибка проверки канала {channel_name}: {e}")

async def continuous_parsing(user_client, bot_client):
    db_conn = init_db()
    logger.info("🔄 Парсер запущен!")
    
    while True:
        try:
            logger.info("🔍 Начинаем проверку каналов...")
            
            for channel in CHANNELS:
                try:
                    await check_channel_for_new_messages(user_client, bot_client, db_conn, channel)
                    await asyncio.sleep(2)  # Задержка между каналами
                except Exception as e:
                    logger.error(f"❌ Ошибка при проверке {channel}: {e}")
            
            logger.info("✅ Проверка завершена, ждем 30 секунд")
            await asyncio.sleep(30)
            
        except Exception as e:
            logger.error(f"💥 Критическая ошибка в основном цикле: {e}")
            await asyncio.sleep(60)

# ===== ОСНОВНАЯ ФУНКЦИЯ =====
async def main():
    # User client для парсинга (использует сессию пользователя)
    user_client = TelegramClient(StringSession(SESSION_STRING), API_ID, API_HASH)
    
    # Bot client только для отправки сообщений
    bot_client = TelegramClient('bot', API_ID, API_HASH)
    
    # Обработчики команд для бота
    @bot_client.on(events.NewMessage(pattern='/start'))
    async def start_handler(event):
        user_id = event.chat_id
        add_subscriber(user_id)
        await event.reply(
            "🎉 **Добро пожаловать в парсер новостей!**\n\n"
            "✅ Вы успешно подписались на рассылку\n"
            "🔄 **РЕЖИМ РАБОТЫ:** непрерывный парсеринг\n"
            "⏱ **ПРОВЕРКА:** каждые 30 секунд\n"
            f"📰 **ОТСЛЕЖИВАЕМ:** {len(CHANNELS)} каналов\n\n"
            "✨ Команды:\n"
            "/stats - статистика\n"
            "/stop - отписаться",
            parse_mode='md',
            link_preview=False
        )
    
    @bot_client.on(events.NewMessage(pattern='/stop'))
    async def stop_handler(event):
        user_id = event.chat_id
        remove_subscriber(user_id)
        await event.reply(
            "❌ **Вы отписались от рассылки**\n\n"
            "Если передумаете - просто напишите /start",
            parse_mode='md',
            link_preview=False
        )
    
    @bot_client.on(events.NewMessage(pattern='/stats'))
    async def stats_handler(event):
        subscribers = load_subscribers()
        await event.reply(
            f"📊 **СТАТИСТИКА СИСТЕМЫ**\n\n"
            f"👥 *Подписчиков:* {len(subscribers)}\n"
            f"📰 *Отслеживаемых каналов:* {len(CHANNELS)}\n"
            f"🔄 *Режим работы:* непрерывный парсеринг\n"
            f"⏱ *Частота проверки:* каждые 30 секунд",
            parse_mode='md',
            link_preview=False
        )
    
    try:
        # Запускаем user client (должен работать без ввода пароля если сессия валидна)
        await user_client.start()
        logger.info("✅ User client запущен для парсинга")
        
        # Запускаем bot client с токеном
        await bot_client.start(bot_token=BOT_TOKEN)
        logger.info("✅ Bot client запущен для отправки сообщений")
        
        logger.info(f"📊 Каналов для парсинга: {len(CHANNELS)}")
        logger.info("🚀 Запускаем непрерывный парсеринг...")
        
        # Запускаем парсеринг
        await continuous_parsing(user_client, bot_client)
            
    except Exception as e:
        logger.error(f"💥 Критическая ошибка: {e}")
    finally:
        await user_client.disconnect()
        await bot_client.disconnect()

if __name__ == '__main__':
    asyncio.run(main())
