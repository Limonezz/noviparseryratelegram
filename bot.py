import asyncio
import sqlite3
import os
from datetime import datetime, timedelta
import pytz
from telethon import TelegramClient, events
from telethon.sessions import StringSession
import logging
import re
from collections import Counter 
import html
import hashlib

# ===== КОНФИГУРАЦИЯ =====
# Для Railway - проверяем переменные окружения
if os.environ.get('RAILWAY_ENVIRONMENT'):
    API_ID = os.environ.get('API_ID', '24826804')
    API_HASH = os.environ.get('API_HASH', '048e59c243cce6ff788a7da214bf8119')
    SESSION_STRING = os.environ.get('SESSION_STRING', "1ApWapzMBuy-exPfF7z634N4Gos8qEwxZ92Nj1r4PWBEd55yqbaP_jcaTT6RiRwd5N4k2snlw_NaVLZ_2C4AvxvB_UG_exIrWgIOj6wsZrHlvBKt92xsGsEbZeo3l95d_6Vr5KKgWaxw531DwOrtWH-lerhkJ7XlDWtt_c225I7W0lIAk8P_k6gzm5oGvRFXqe0ivHxU7q4sJz6V61Ca0jyA_Sv-74OxB9l07HmIbOAC66oCtekxj4G5MTKKudofzmu2IqjqTgfFHwnKzE6hA3qik1SqSWdtWvmXHGb_44qPSk2dWGdW7vsN8inFuByDQLCF1_VLdGe0aFohbN0TXKKi7k0C8g2I=")
    BOT_TOKEN = os.environ.get('BOT_TOKEN', '7597923417:AAEyZvTyyrPFQDz1o1qURDeCEoBFc0fMWaY')
else:
    API_ID = '24826804'
    API_HASH = '048e59c243cce6ff788a7da214bf8119'
    SESSION_STRING = "1ApWapzMBuy-exPfF7z634N4Gos8qEwxZ92Nj1r4PWBEd55yqbaP_jcaTT6RiRwd5N4k2snlw_NaVLZ_2C4AvxvB_UG_exIrWgIOj6wsZrHlvBKt92xsGsEbZeo3l95d_6Vr5KKgWaxw531DwOrtWH-lerhkJ7XlDWtt_c225I7W0lIAk8P_k6gzm5oGvRFXqe0ivHxU7q4sJz6V61Ca0jyA_Sv-74OxB9l07HmIbOAC66oCtekxj4G5MTKKudofzmu2IqjqTgfFHwnKzE6hA3qik1SqSWdtWvmXHGb_44qPSk2dWGdW7vsN8inFuByDQLCF1_VLdGe0aFohbN0TXKKi7k0C8g2I="
    BOT_TOKEN = '7597923417:AAEyZvTyyrPFQDz1o1qURDeCEoBFc0fMWaY'

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

# Фильтр спама и скама
SPAM_DOMAINS = [
    'ordershunter.ru', 'premium_gift', 'telegram-premium', 'free-telegram',
    'nakrutka', 'followers', 'likes', 'diplom', 'kursovaya', 'zarabotok'
]

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
    # Бытовые и местные новости
    'изрисовали', 'неизвестные возле', 'элеваторы для оплаты',
    'автобусах появились', 'техосмотр', 'как выбрать генератор',
    'инструкция по выбору', 'утренняя зарядка', 'рецидивист',
    'сразу видно рецидивист', 'обсуждают уличных музыкантов',
    'утренняя зарядка в vk', 'vk.com/video',
    
    # Рекламные призывы
    'подписаться на канал', 'подписаться на нас', 'подписаться на риа',
    'курсовая программа', 'бесплатная программа', 'заявки принимаются',
    'количество мест ограничено', 'шапка', 'маркетплейс',
    'утренняя зарядка', 'ссылка на видео', 'платим за ваш эксклюзив',
    'реклама', 'коммерция', 'озон', 'wildberries', 'накрутка',
    
    # Развлекательный контент
    'трамп вернулся в tiktok', 'утренняя зарядка',
    
    # Коммерческие предложения
    'платим за ваш эксклюзив', 'реклама', 'коммерция',
    'маркетплейс', 'озон', 'wildberries',
    
    # Убраны упоминания опасностей
    'авиационная опасность', 'воздушная опасность', 'авиационная', 
    'воздушная', 'бпла опасность', 'опасность бпла', 'оперштаб',
    'сирена', 'тревога', 'воздушная тревога', 'отбой тревоги'
]

# Важные ключевые слова, которые должны ПРОПУСКАТЬСЯ
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

SPAM_URL_THRESHOLD = 3
UNIQUE_WORDS_THRESHOLD = 5
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
        logger.info(f"✅ Новый подписчик: {user_id}")
    return subscribers

def remove_subscriber(user_id):
    subscribers = load_subscribers()
    if user_id in subscribers:
        subscribers.remove(user_id)
        save_subscribers(subscribers)
        logger.info(f"❌ Отписался: {user_id}")
    return subscribers

# ===== ФУНКЦИИ ФИЛЬТРАЦИИ =====
def clean_text(text):
    """Очистка текста"""
    text = re.sub(r'http\S+|@\w+|#\w+', '', text)
    text = re.sub(r'[^\w\s]', ' ', text)
    text = re.sub(r'\s+', ' ', text).strip()
    return text.lower()

def get_text_words(text):
    """Получаем значимые слова"""
    cleaned = clean_text(text)
    words = cleaned.split()
    stop_words = {'и', 'в', 'на', 'с', 'по', 'за', 'к', 'у', 'о', 'от', 'для', 'это', 'как', 'что', 'из', 'не'}
    return {word for word in words if len(word) > 2 and word not in stop_words}

def is_important_news(text):
    """Проверяет, содержит ли текст важные ключевые слова"""
    text_lower = text.lower()
    return any(keyword in text_lower for keyword in IMPORTANT_KEYWORDS)

def is_spam_message(text):
    """Проверка на спам и скам"""
    text_lower = text.lower()
    
    # ВАЖНО: сообщения с важными ключевыми словами всегда пропускаем
    if is_important_news(text):
        return False
    
    # Проверка спам-фраз
    for phrase in SPAM_PHRASES:
        if phrase in text_lower:
            return True
    
    # Проверка спам-доменов
    for domain in SPAM_DOMAINS:
        if domain in text_lower:
            return True
    
    # Проверка количества ссылок (кроме telegram ссылок)
    url_pattern = r'http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\\(\\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+'
    urls = re.findall(url_pattern, text)
    
    # Фильтруем telegram ссылки
    non_telegram_urls = [url for url in urls if 't.me' not in url and 'telegram' not in url]
    
    if len(non_telegram_urls) > SPAM_URL_THRESHOLD:
        return True
    
    # Проверка призывов к действию с нетематическими ссылками
    action_words = ['перейдите', 'нажмите', 'кликните', 'закажите', 'купить']
    has_action = any(word in text_lower for word in action_words)
    has_non_telegram_url = len(non_telegram_urls) > 0
    
    if has_action and has_non_telegram_url:
        return True
    
    return False

def is_relevant_topic(text):
    """Проверка тематики"""
    return True, ['новости']

def is_recent_message(message_date):
    """Проверка свежести сообщения"""
    utc_now = datetime.utcnow().replace(tzinfo=pytz.utc)
    message_age = utc_now - message_date
    
    if message_age > timedelta(hours=MAX_MESSAGE_AGE_HOURS):
        return False
    
    return True

# ===== БАЗА ДАННЫХ ДЛЯ ОТСЛЕЖИВАНИЯ ОТПРАВЛЕННЫХ СООБЩЕНИЙ =====
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
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS last_checked (
            channel TEXT PRIMARY KEY,
            last_message_id INTEGER
        )
    ''')
    conn.commit()
    return conn

def generate_message_hash(channel_name, message_text):
    """Генерируем хеш для уникальной идентификации сообщения"""
    text_to_hash = f"{channel_name}_{clean_text(message_text)}"
    return hashlib.md5(text_to_hash.encode()).hexdigest()

def is_message_sent(conn, message_hash):
    """Проверяем, было ли сообщение уже отправлено"""
    cursor = conn.cursor()
    cursor.execute("SELECT message_hash FROM sent_messages WHERE message_hash = ?", (message_hash,))
    return cursor.fetchone() is not None

def mark_message_as_sent(conn, message_hash, channel_name, message_text, message_id):
    """Отмечаем сообщение как отправленное"""
    cursor = conn.cursor()
    cursor.execute(
        "INSERT OR IGNORE INTO sent_messages (message_hash, channel, message_text, message_id) VALUES (?, ?, ?, ?)",
        (message_hash, channel_name, message_text[:500], message_id)
    )
    conn.commit()

def get_last_message_id(conn, channel_name):
    """Получаем ID последнего проверенного сообщения для канала"""
    cursor = conn.cursor()
    cursor.execute("SELECT last_message_id FROM last_checked WHERE channel = ?", (channel_name,))
    result = cursor.fetchone()
    return result[0] if result else None

def update_last_message_id(conn, channel_name, message_id):
    """Обновляем ID последнего проверенного сообщения"""
    cursor = conn.cursor()
    cursor.execute(
        "INSERT OR REPLACE INTO last_checked (channel, last_message_id) VALUES (?, ?)",
        (channel_name, message_id)
    )
    conn.commit()

# ===== ФУНКЦИИ ДЛЯ ПАРСЕРИНГА =====
def format_channel_name(channel_name):
    """Форматирование названия канала"""
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
    """Форматирование текста сообщения"""
    # Очищаем от лишних пробелов и переносов
    text = re.sub(r'\n\s*\n', '\n\n', text.strip())
    
    # Обрезаем слишком длинные сообщения
    if len(text) > 3800:
        text = text[:3800] + "..."
    
    return text

def generate_message_url(channel_username, message_id):
    """Генерация ссылки на оригинальное сообщение"""
    return f"https://t.me/{channel_username}/{message_id}"

def generate_channel_url(channel_username):
    """Генерация ссылки на канал"""
    return f"https://t.me/{channel_username}"

async def check_channel_for_new_messages(user_client, bot_client, db_conn, channel_name):
    """Проверяет канал на наличие новых сообщений"""
    try:
        # Получаем последнее сообщение из канала
        messages = await user_client.get_messages(channel_name, limit=1)
        
        if not messages or not messages[0].text:
            return
        
        last_message = messages[0]
        last_message_id = last_message.id
        message_text = last_message.text.strip()
        
        # Получаем ID последнего проверенного сообщения
        last_checked_id = get_last_message_id(db_conn, channel_name)
        
        # Если это новое сообщение (или первая проверка)
        if last_checked_id is None or last_message_id > last_checked_id:
            
            # Фильтр спама
            if is_spam_message(message_text):
                # Все равно обновляем last_message_id чтобы не проверять повторно
                update_last_message_id(db_conn, channel_name, last_message_id)
                return
            
            # Фильтр свежести
            if not is_recent_message(last_message.date):
                update_last_message_id(db_conn, channel_name, last_message_id)
                return
            
            # Фильтр тематики
            is_relevant, categories = is_relevant_topic(message_text)
            if not is_relevant:
                update_last_message_id(db_conn, channel_name, last_message_id)
                return
            
            # Проверяем, не отправляли ли мы уже это сообщение
            message_hash = generate_message_hash(channel_name, message_text)
            if is_message_sent(db_conn, message_hash):
                update_last_message_id(db_conn, channel_name, last_message_id)
                return
            
            # Форматируем текст
            formatted_text = format_message_text(message_text)
            
            # Генерируем ссылки
            message_url = generate_message_url(channel_name, last_message_id)
            channel_url = generate_channel_url(channel_name)
            
            # Красивое оформление сообщения
            formatted_channel = format_channel_name(channel_name)
            message_time = last_message.date.astimezone(pytz.timezone('Europe/Moscow')).strftime('%H:%M %d.%m.%Y')
            
            formatted_post = (
                f"📰 **{formatted_channel}**\n"
                f"🕒 {message_time}\n"
                f"{formatted_text}\n"
                f"🔗 [Источник]({message_url})"
            )
            
            # Отправляем всем подписчикам
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
                mark_message_as_sent(db_conn, message_hash, channel_name, message_text, last_message_id)
                update_last_message_id(db_conn, channel_name, last_message_id)
                logger.info(f"Отправлена новость из {channel_name} для {success_count} подписчиков")
            else:
                update_last_message_id(db_conn, channel_name, last_message_id)
        
    except Exception as e:
        logger.error(f"Ошибка проверки канала {channel_name}: {e}")

async def continuous_parsing(user_client, bot_client):
    """Непрерывный парсеринг каналов"""
    db_conn = init_db()
    
    # Инициализируем last_message_id для всех каналов при первом запуске
    for channel in CHANNELS:
        try:
            if get_last_message_id(db_conn, channel) is None:
                messages = await user_client.get_messages(channel, limit=1)
                if messages:
                    update_last_message_id(db_conn, channel, messages[0].id)
        except Exception as e:
            logger.error(f"Ошибка инициализации канала {channel}: {e}")
    
    logger.info("Парсер запущен!")
    
    while True:
        try:
            for channel in CHANNELS:
                try:
                    await check_channel_for_new_messages(user_client, bot_client, db_conn, channel)
                    await asyncio.sleep(2)
                except Exception as e:
                    logger.error(f"Ошибка при проверке {channel}: {e}")
            
            await asyncio.sleep(30)
            
        except Exception as e:
            logger.error(f"Ошибка в основном цикле: {e}")
            await asyncio.sleep(60)

# ===== ОСНОВНАЯ ФУНКЦИЯ =====
async def main():
    user_client = TelegramClient(StringSession(SESSION_STRING), API_ID, API_HASH)
    bot_client = TelegramClient('bot_session', API_ID, API_HASH)
    
    @bot_client.on(events.NewMessage(pattern='/start'))
    async def start_handler(event):
        if event.message.out:
            return
            
        user_id = event.chat_id
        add_subscriber(user_id)
        await event.reply(
            "✅ Вы подписались на новостной парсер\n\n"
            f"📊 Отслеживаем каналов: {len(CHANNELS)}\n"
            "🔄 Проверка каждые 30 секунд\n\n"
            "Команды:\n"
            "/stats - статистика\n"
            "/stop - отписаться",
            parse_mode='md'
        )
    
    @bot_client.on(events.NewMessage(pattern='/stop'))
    async def stop_handler(event):
        if event.message.out:
            return
            
        user_id = event.chat_id
        remove_subscriber(user_id)
        await event.reply(
            "❌ Вы отписались от рассылки\n\n"
            "Чтобы снова подписаться - /start",
            parse_mode='md'
        )
    
    @bot_client.on(events.NewMessage(pattern='/stats'))
    async def stats_handler(event):
        if event.message.out:
            return
            
        subscribers = load_subscribers()
        await event.reply(
            f"📊 Статистика:\n\n"
            f"👥 Подписчиков: {len(subscribers)}\n"
            f"📰 Каналов: {len(CHANNELS)}\n"
            f"🔄 Режим: непрерывный парсинг",
            parse_mode='md'
        )
    
    try:
        await user_client.start()
        await bot_client.start(bot_token=BOT_TOKEN)
        
        logger.info("Бот запущен!")
        logger.info(f"Каналов для парсинга: {len(CHANNELS)}")
        
        # Запускаем непрерывный парсеринг
        await continuous_parsing(user_client, bot_client)
            
    except Exception as e:
        logger.error(f"Ошибка: {e}")
    finally:
        await user_client.disconnect()
        await bot_client.disconnect()

if __name__ == '__main__':
    asyncio.run(main())
