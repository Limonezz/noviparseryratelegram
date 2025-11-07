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

# ===== ПАРСЕРИНГ =====
async def check_channel_for_new_messages(user_client, bot_client, db_conn, channel_name):
    try:
        messages = await user_client.get_messages(channel_name, limit=5)
        
        for message in messages:
            if not message.text or not message.text.strip():
                continue
            
            message_text = message.text.strip()
            
            if is_spam_message(message_text):
                continue
            
            if not is_recent_message(message.date):
                continue
            
            message_hash = generate_message_hash(channel_name, message_text)
            if is_message_sent(db_conn, message_hash):
                continue
            
            # Форматируем сообщение
            message_url = f"https://t.me/{channel_name}/{message.id}"
            message_time = message.date.astimezone(pytz.timezone('Europe/Moscow')).strftime('%H:%M %d.%m.%Y')
            
            formatted_post = (
                f"📰 **{channel_name}**\n"
                f"🕒 {message_time}\n"
                f"{message_text}\n"
                f"🔗 [Источник]({message_url})"
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
                    await asyncio.sleep(0.3)
                except Exception as e:
                    logger.error(f"Ошибка отправки {user_id}: {e}")
            
            if success_count > 0:
                mark_message_as_sent(db_conn, message_hash, channel_name, message_text, message.id)
                logger.info(f"Отправлена новость из {channel_name} для {success_count} подписчиков")
            
            break
        
    except Exception as e:
        logger.error(f"Ошибка проверки канала {channel_name}: {e}")

async def continuous_parsing(user_client, bot_client):
    db_conn = init_db()
    logger.info("Парсер запущен!")
    
    while True:
        try:
            for channel in CHANNELS:
                try:
                    await check_channel_for_new_messages(user_client, bot_client, db_conn, channel)
                    await asyncio.sleep(3)
                except Exception as e:
                    logger.error(f"Ошибка при проверке {channel}: {e}")
            
            logger.info("Цикл проверки завершен, ждем 30 секунд")
            await asyncio.sleep(30)
            
        except Exception as e:
            logger.error(f"Ошибка в основном цикле: {e}")
            await asyncio.sleep(60)

# ===== ОСНОВНАЯ ФУНКЦИЯ =====
async def main():
    # User client для парсинга (использует сессию пользователя)
    user_client = TelegramClient(StringSession(SESSION_STRING), API_ID, API_HASH)
    
    # Bot client только для отправки сообщений
    bot_client = TelegramClient('bot', API_ID, API_HASH)
    
    @bot_client.on(events.NewMessage(pattern='/start'))
    async def start_handler(event):
        user_id = event.chat_id
        add_subscriber(user_id)
        await event.reply(
            "✅ Вы подписались на новостной парсер\n\n"
            f"📊 Отслеживаем каналов: {len(CHANNELS)}\n"
            "🔄 Проверка каждые 30 секунд\n\n"
            "Команды:\n"
            "/stats - статистика\n"
            "/stop - отписаться"
        )
    
    @bot_client.on(events.NewMessage(pattern='/stop'))
    async def stop_handler(event):
        user_id = event.chat_id
        remove_subscriber(user_id)
        await event.reply("❌ Вы отписались от рассылки")
    
    @bot_client.on(events.NewMessage(pattern='/stats'))
    async def stats_handler(event):
        subscribers = load_subscribers()
        await event.reply(
            f"📊 Статистика:\n\n"
            f"👥 Подписчиков: {len(subscribers)}\n"
            f"📰 Каналов: {len(CHANNELS)}\n"
            f"🔄 Режим: непрерывный парсинг"
        )
    
    try:
        # Запускаем user client - ОЧЕНЬ ВАЖНО: без вызова input()!
        await user_client.start()
        logger.info("User client запущен для парсинга")
        
        # Запускаем bot client с токеном
        await bot_client.start(bot_token=BOT_TOKEN)
        logger.info("Bot client запущен для отправки сообщений")
        
        logger.info(f"Каналов для парсинга: {len(CHANNELS)}")
        
        # Запускаем парсеринг
        await continuous_parsing(user_client, bot_client)
            
    except Exception as e:
        logger.error(f"Ошибка: {e}")
    finally:
        await user_client.disconnect()
        await bot_client.disconnect()

if __name__ == '__main__':
    asyncio.run(main())
