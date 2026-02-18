import os
import logging
import traceback
import sys
import re
from datetime import datetime, timedelta, time as dtime
import pytz
from logging.handlers import RotatingFileHandler

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, ReplyKeyboardMarkup, KeyboardButton
from telegram.error import BadRequest, Forbidden, TelegramError
from telegram.ext import (
    ApplicationBuilder, CommandHandler, ContextTypes, CallbackQueryHandler,
    MessageHandler, filters, ConversationHandler, JobQueue
)
from telegram.ext import PicklePersistence
from telegram.request import HTTPXRequest

import config
from translations import T
import db

# ────────────────────────────────────────────────
# Loggingni DARHOL va juda batafsil sozlaymiz
# ────────────────────────────────────────────────

logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s | %(levelname)-7s | %(name)s:%(lineno)d | %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

# Add file handler for errors so tracebacks are persisted to disk for later debugging
try:
    # Ensure log directory exists
    log_dir = getattr(config, 'LOG_DIR', '.') or '.'
    os.makedirs(log_dir, exist_ok=True)
    log_path = os.path.join(log_dir, 'errors.log')
    # Use RotatingFileHandler to prevent unbounded growth of errors.log
    # Keeps max 5 backup files, each up to 5MB
    error_file_handler = RotatingFileHandler(log_path, maxBytes=5*1024*1024, backupCount=5)
    error_file_handler.setLevel(logging.ERROR)
    formatter = logging.Formatter('%(asctime)s | %(levelname)-7s | %(name)s:%(lineno)d | %(message)s')
    error_file_handler.setFormatter(formatter)
    logger.addHandler(error_file_handler)
    logger.debug(f'Error log handler attached at {log_path}')
except Exception:
    logger.exception('Failed to attach errors.log file handler')

# Global exception hook — hech qayerda ushlanmagan xatolarni ham ushlaydi
def global_exception_handler(exc_type, exc_value, exc_traceback):
    logger.critical("!!! GLOBAL UNCAUGHT EXCEPTION !!!", exc_info=(exc_type, exc_value, exc_traceback))

sys.excepthook = global_exception_handler

logger.info("BOT STARTED — logging fully enabled")

# Reduce noisy HTTP client debug logs in normal operation
logging.getLogger('httpx').setLevel(logging.WARNING)
logging.getLogger('httpcore').setLevel(logging.WARNING)

TZ = pytz.timezone(config.TIMEZONE)

# Weekday names in different languages
WEEKDAYS = {
    'en': ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday'],
    'uz': ['Dushanba', 'Seshanba', 'Chorshanba', 'Payshanba', 'Juma', 'Shanba', 'Yakshanba'],
    'ru': ['Понедельник', 'Вторник', 'Среда', 'Четверг', 'Пятница', 'Суббота', 'Воскресенье']
}

def format_date_with_weekday(date_str: str, lang: str = 'en') -> str:
    """
    Format date string (YYYY-MM-DD) with day of week.
    Returns: 'Monday, 2024-01-15' or equivalent in the chosen language
    """
    try:
        d = datetime.strptime(date_str, '%Y-%m-%d').date()
        weekday_name = WEEKDAYS.get(lang, WEEKDAYS['en'])[d.weekday()]
        return f"{weekday_name}, {date_str}"
    except Exception as e:
        logger.debug(f"Error formatting date {date_str}: {e}")
        return date_str


def _split_text_chunks(text: str, limit: int = 3900):
    """Split text into chunks not exceeding `limit` characters, splitting on line boundaries when possible."""
    if len(text) <= limit:
        return [text]
    lines = text.split('\n')
    chunks = []
    cur = []
    cur_len = 0
    for ln in lines:
        add_len = len(ln) + 1
        if cur_len + add_len > limit and cur:
            chunks.append('\n'.join(cur))
            cur = [ln]
            cur_len = len(ln) + 1
        else:
            cur.append(ln)
            cur_len += add_len
    if cur:
        chunks.append('\n'.join(cur))
    return chunks


def build_calendar_buttons(dates, branch_key):
    """Build a simple calendar-style inline keyboard for the given date objects.
    Dates is a list of date objects; we arrange them in rows of 2 (2 columns).
    Callback payload is 'date:{branch}|{iso}'"""
    buttons = []
    week = []
    for d in dates:
        text = d.strftime('%d %b')
        week.append(InlineKeyboardButton(text, callback_data=f'date:{branch_key}|{d.isoformat()}'))
        if len(week) == 2:
            buttons.append(week)
            week = []
    if week:
        buttons.append(week)
    return buttons


def build_numbered_booking_list(bookings, callback_prefix, context=None, page=1, items_per_page=10):
    """Build numbered inline list of bookings with pagination.
    Returns (text, buttons, total_pages)"""
    if not bookings:
        return "No bookings", [], 1
    
    total_bookings = len(bookings)
    total_pages = (total_bookings + items_per_page - 1) // items_per_page
    start_idx = (page - 1) * items_per_page
    end_idx = start_idx + items_per_page
    page_bookings = bookings[start_idx:end_idx]
    
    lines = []
    # Use per-page numbering (1..items_per_page) for display
    for display_idx, bb in enumerate(page_bookings, start=1):
        u = get_user_cached(context, bb['user_id']) if context else None
        student = ''
        if u:
            if u.get('username'):
                student = f"@{u.get('username')}"
            else:
                student = u.get('first_name', str(bb['user_id']))
        else:
            student = str(bb['user_id'])
        
        formatted_date = format_date_with_weekday(bb['date'], 'en')
        branch_short = bb.get('branch', 'branch_1').replace('branch_', 'Br.')
        purpose = bb.get('purpose', '-')[:20]  # Truncate long purpose
        
        lines.append(f"<b>{display_idx}.</b> {formatted_date} | {bb['time']} | {branch_short} | {purpose} | {student}")
    
    text = '\n'.join(lines)
    
    # Number buttons (1-10)
    number_buttons = []
    row = []
    for offset in range(len(page_bookings)):
        # Button labels should be 1..N within the page
        btn_label = str(offset + 1)
        bid = page_bookings[offset]['id']
        row.append(InlineKeyboardButton(btn_label, callback_data=f'{callback_prefix}:{bid}'))
        if len(row) == 5:
            number_buttons.append(row)
            row = []
    if row:
        number_buttons.append(row)
    
    # Pagination buttons
    pagination_row = []
    if page > 1:
        pagination_row.append(InlineKeyboardButton('◀ Previous', callback_data=f'{callback_prefix}:page:{page-1}'))
    if page < total_pages:
        pagination_row.append(InlineKeyboardButton('Next ▶', callback_data=f'{callback_prefix}:page:{page+1}'))
    
    buttons = number_buttons
    if pagination_row:
        buttons.append(pagination_row)
    buttons.append([InlineKeyboardButton('⬅️ Back', callback_data='back:menu')])
    
    return text, buttons, total_pages


async def _safe_edit_or_send(callback_q, context, text, parse_mode=None, reply_markup=None):
    """Edit callback message if possible; if text too long, split into chunks and send subsequent messages."""
    try:
        chunks = _split_text_chunks(text)
        # Edit first chunk
        await callback_q.edit_message_text(chunks[0], parse_mode=parse_mode, reply_markup=reply_markup)
        # Send remaining chunks as separate messages
        for c in chunks[1:]:
            await context.bot.send_message(chat_id=callback_q.from_user.id, text=c, parse_mode=parse_mode)
    except Exception as e:
        logger.debug(f"_safe_edit_or_send: edit failed ({e}), attempting to send chunks as new messages")
        try:
            for c in _split_text_chunks(text):
                await context.bot.send_message(chat_id=callback_q.from_user.id, text=c, parse_mode=parse_mode)
        except Exception:
            logger.exception("Failed to deliver long admin message")

async def _safe_edit_message(callback_q, text, parse_mode=None, reply_markup=None):
    """Safely edit callback message, ignoring 'Message is not modified' errors."""
    try:
        await callback_q.edit_message_text(text, parse_mode=parse_mode, reply_markup=reply_markup)
    except Exception as e:
        # Silently ignore "Message is not modified" errors (user clicked same button twice)
        if "Message is not modified" not in str(e):
            logger.debug(f"_safe_edit_message: {e}")

def safe_handler(func):
    async def wrapper(*args, **kwargs):
        update = kwargs.get('update') if 'update' in kwargs else (args[0] if args else None)
        context = kwargs.get('context') if 'context' in kwargs else (args[1] if len(args) > 1 else None)

        try:
            logger.debug(f"→ Handler called: {func.__name__}")
            return await func(*args, **kwargs)
        except Exception as e:
            tb = traceback.format_exc()
            user_id = "unknown"
            chat_id = "unknown"
            data_info = ""
            user_input = ""
            user_info = None

            # Safely extract user info from update
            try:
                if update is not None:
                    user = update.effective_user
                    user_id = user.id if user else "no_user"
                    chat_id = update.effective_chat.id if update.effective_chat else "no_chat"
                    
                    # Get user info for profile link
                    if user_id != "no_user":
                        try:
                            user_info = get_user_cached(context, user_id) if context else db.get_user(user_id)
                        except Exception:
                            user_info = None

                    if update.callback_query:
                        data_info = f"callback_data: {update.callback_query.data}"
                        user_input = f"Callback: {update.callback_query.data}"
                    elif update.message and update.message.text:
                        data_info = f"message_text: {update.message.text[:100]}"
                        user_input = f"Input: {update.message.text[:200]}"
            except:
                pass

            logger.error(
                f"HANDLER CRASHED | func={func.__name__} | user={user_id} | chat={chat_id} | {data_info}\n"
                f"Exception: {type(e).__name__}: {str(e)}\n"
                f"{tb}"
            )

            # Notify admins with detailed error report and user profile link
            for aid in config.ADMIN_IDS:
                try:
                    # Ensure context and bot are available before sending
                    if context and hasattr(context, 'bot') and context.bot:
                        user_link = "Unknown"
                        if user_id != "unknown" and user_id != "no_user":
                            if user_info:
                                first_name = user_info.get('first_name', str(user_id))
                                username = user_info.get('username')
                                if username:
                                    user_link = f'<a href="tg://user?id={user_id}">@{username}</a>'
                                else:
                                    user_link = f'<a href="tg://user?id={user_id}">{first_name}</a>'
                            else:
                                user_link = f'<a href="tg://user?id={user_id}">{user_id}</a>'
                        
                        admin_username = config.ADMIN_USERNAMES.get(aid, "")
                        admin_mention = f" ({admin_username})" if admin_username else ""
                        
                        error_message = (
                            f"🚨 <b>BOT ERROR</b>{admin_mention}\n\n"
                            f"👤 <b>User:</b> {user_link}\n"
                            f"🔧 <b>Handler:</b> {func.__name__}\n"
                            f"❌ <b>Error:</b> {type(e).__name__}: {str(e)}\n"
                            f"📝 <b>User Action:</b>\n{user_input}\n\n"
                            f"<b>Traceback:</b>\n"
                            f"<pre>{tb[:2000]}</pre>"
                        )
                        
                        await context.bot.send_message(
                            aid,
                            error_message,
                            parse_mode='HTML'
                        )
                    else:
                        logger.error(f"Cannot notify admin {aid}: context or context.bot not available")
                except Exception as notify_err:
                    logger.error(f"Failed to notify admin {aid}: {notify_err}")

            # Notify user and ask them to contact admin with problem description
            try:
                lang = 'en'
                if user_id != "unknown" and user_id != "no_user":
                    try:
                        u = get_user_cached(context, user_id) if context else db.get_user(user_id)
                    except Exception:
                        u = db.get_user(user_id)
                    if u:
                        lang = u.get('lang', 'en')
                
                # Only send user notification if we have both context and update with effective_user
                if context and hasattr(context, 'bot') and context.bot and update and hasattr(update, 'effective_user') and update.effective_user:
                    # Build admin contact message
                    admin_contacts = []
                    for admin_id in config.ADMIN_IDS:
                        admin_username = config.ADMIN_USERNAMES.get(admin_id, f"Admin {admin_id}")
                        admin_contacts.append(admin_username)
                    
                    admin_list = " yoki ".join(admin_contacts) if lang == 'uz' else " or ".join(admin_contacts)
                    
                    error_messages = {
                        'en': f"❌ An error occurred. Please contact admin: {admin_list}\n\n💬 <b>Describe what happened and what you were trying to do.</b>",
                        'uz': f"❌ Xatolik yuz berdi. Iltimos admin bilan bog'laning: {admin_list}\n\n💬 <b>Qanday muammo bo'lganini va nima qilmoqchi ekanligingizni yozing.</b>",
                        'ru': f"❌ Произошла ошибка. Пожалуйста, свяжитесь с администратором: {admin_list}\n\n💬 <b>Опишите что произошло и что вы пытались сделать.</b>"
                    }
                    
                    error_text = error_messages.get(lang, error_messages['en'])
                    
                    await context.bot.send_message(
                        chat_id=update.effective_user.id,
                        text=error_text,
                        parse_mode='HTML'
                    )
            except Exception as user_notify_err:
                logger.error(f"Failed to notify user: {user_notify_err}")

    return wrapper


async def safe_answer(q):
    try:
        await q.answer()
    except BadRequest as e:
        logger.debug(f"Callback answer failed (likely expired): {e}")


def get_user_cached(context, user_id: int):
    """
    Safe user fetcher.
    Uses context.user_data cache only when available (interactive handlers).
    Falls back to DB for background jobs (JobQueue).
    """
    # Interactive handlers only
    if context and hasattr(context, 'user_data') and context.user_data is not None:
        cache = context.user_data.setdefault('_user_cache', {})
        if user_id in cache:
            return cache[user_id]

        u = db.get_user(user_id)
        cache[user_id] = u
        return u

    # Background jobs / startup / polling
    return db.get_user(user_id)


# Conversation states
DELAY_NEW = 2
BROADCAST_MSGS = 3

def tr(lang, key, **kwargs):
    lang = lang if lang in T else 'en'
    txt = T[lang].get(key, T['en'].get(key, key))
    return txt.format(**kwargs) if kwargs else txt

def main_menu(lang='en', user_id: int = None):
    book_label = tr(lang, 'book')
    my_bookings_label = tr(lang, 'my_bookings')
    lang_label = tr(lang, 'select_language')
    logger.debug(f'main_menu: lang={lang}, book={repr(book_label)}, my_bookings={repr(my_bookings_label)}, lang_sel={repr(lang_label)}')
    kb = [[KeyboardButton(book_label)], [KeyboardButton(my_bookings_label)], [KeyboardButton(lang_label)]]
    # show admin panel button only to admins
    try:
        if user_id is not None and user_id in config.ADMIN_IDS:
            kb.append([KeyboardButton(tr(lang, 'admin_panel'))])
    except Exception:
        pass
    return ReplyKeyboardMarkup(kb, resize_keyboard=True)

@safe_handler
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    u = get_user_cached(context, user.id)
    if not u:
        # ask language
        kb = [[InlineKeyboardButton('English', callback_data='lang:en'), InlineKeyboardButton('Русский', callback_data='lang:ru')],
              [InlineKeyboardButton("O'zbek", callback_data='lang:uz')]]
        await update.message.reply_text('Please select language / Пожалуйста выберите язык / Iltimos tilni tanlang', reply_markup=InlineKeyboardMarkup(kb))
        db.create_user(user.id, 'en', user.first_name or '', user.username or '')
        return
    lang = u.get('lang', 'en')
    kb = main_menu(lang, user.id)
    text = tr(lang, 'start')
    await update.message.reply_text(text, reply_markup=kb)

@safe_handler
async def lang_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await safe_answer(q)
    data = q.data
    if data and data.startswith('lang:'):
        lang = data.split(':', 1)[1]
        user = update.effective_user
        db.set_user_lang(user.id, lang)
        # Clear user cache so next handler fetches fresh language preference
        context.user_data.pop('_user_cache', {}).pop(user.id, None)
        kb = main_menu(lang)
        # can't attach a ReplyKeyboardMarkup to an edited inline message — edit the inline message text
        # then send a new message with the reply keyboard for the user
        await q.edit_message_text(tr(lang, 'start'))
        try:
            await q.message.reply_text(tr(lang, 'start'), reply_markup=main_menu(lang, user.id))
        except Exception:
            # fallback: send without keyboard
            await q.message.reply_text(tr(lang, 'start'))

@safe_handler
async def back_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    try:
        await q.answer()
    except BadRequest as e:
        # Callback query is too old or invalid; ignore and continue handling
        logger.debug(f"Callback answer failed (likely expired): {e}")
    data = q.data
    user = update.effective_user
    u = get_user_cached(context, user.id) or {}
    lang = u.get('lang','en')
    if data == 'back:menu':
        # edit the current inline message to show the main menu (don't delete it)
        try:
            await q.edit_message_text(tr(lang, 'start'), reply_markup=main_menu(lang, user.id))
            return
        except BadRequest:
            # if editing fails (expired/old message), fallback to sending a new main menu message
            try:
                await context.bot.send_message(chat_id=update.effective_user.id, text=tr(lang, 'start'), reply_markup=main_menu(lang, user.id))
                return
            except Exception:
                logger.exception('Failed to show main menu on back:menu')
    elif data.startswith('back:dates'):
        # data may be 'back:dates' or 'back:dates:branch_key'
        parts = data.split(':')
        branch_key = parts[2] if len(parts) > 2 else None
        dates = next_14_dates()
        today = datetime.now(TZ).date()
        today_wd = today.weekday()
        buttons = []
        row = []
        if branch_key:
            # filter dates by weekday according to branch
            # Check if today has slots for this branch
            today_has_slots = False
            if branch_key == 'branch_2' and today_wd in (0, 2, 4):
                today_has_slots = True
            elif branch_key == 'branch_1' and today_wd in (1, 3, 5):
                today_has_slots = True
            
            if today_has_slots:
                text = today.strftime('%a %d %b')
                row.append(InlineKeyboardButton(text, callback_data=f'date:{branch_key}|{today.isoformat()}'))
                if len(row) == 2:
                    buttons.append(row)
                    row = []
            
            for d in dates:
                if d == today:  # Skip today - already added
                    continue
                wd = d.weekday()
                if branch_key == 'branch_2' and wd in (0, 2, 4):
                    text = d.strftime('%a %d %b')
                    row.append(InlineKeyboardButton(text, callback_data=f'date:{branch_key}|{d.isoformat()}'))
                    if len(row) == 2:
                        buttons.append(row)
                        row = []
                elif branch_key == 'branch_1' and wd in (1, 3, 5):
                    text = d.strftime('%a %d %b')
                    row.append(InlineKeyboardButton(text, callback_data=f'date:{branch_key}|{d.isoformat()}'))
                    if len(row) == 2:
                        buttons.append(row)
                        row = []
        else:
            for d in dates:
                text = d.strftime('%a %d %b')
                row.append(InlineKeyboardButton(text, callback_data=f'date:{d.isoformat()}'))
                if len(row) == 2:
                    buttons.append(row)
                    row = []
        if row:
            buttons.append(row)
        buttons.append([InlineKeyboardButton(tr(lang,'back'), callback_data='back:menu')])
        if branch_key:
            await q.edit_message_text(f"{tr(lang, 'choose_date')}\n{tr(lang, branch_key)}", reply_markup=InlineKeyboardMarkup(buttons))
        else:
            await q.edit_message_text(tr(lang, 'choose_date'), reply_markup=InlineKeyboardMarkup(buttons))

def next_14_dates():
    today = datetime.now(TZ).date()
    dates = []
    # Include today if it's not Sunday and has available slots
    if today.weekday() != 6:
        dates.append(today)
    # Add next 14 days
    for i in range(1, 15):
        d = today + timedelta(days=i)
        if d.weekday() == 6:  # Sunday skip
            continue
        dates.append(d)
    return dates

def weekday_slots(d: datetime.date):
    # returns list of times (HH:MM strings) and branch
    wd = d.weekday()
    if wd in (0, 2, 4):  # Mon Wed Fri
        times = ['14:00','14:30','15:00','15:30','16:00','16:30','17:00','17:30','18:00']
        branch = 'branch_2'
    else:
        times = ['14:00','14:30','15:00','15:30','16:00','16:30','17:00','17:30','18:00']
        branch = 'branch_1'
    return times, branch


def branch_slots_for_date(branch_key: str, d: datetime.date):
    # branch_2 has shorter schedule, branch_1 has longer schedule
    if branch_key == 'branch_2':
        times = ['14:00','14:30','15:00','15:30','16:00','16:30','17:00','17:30','18:00']
    else:
        times = ['14:00','14:30','15:00','15:30','16:00','16:30','17:00','17:30','18:00']
    return times

@safe_handler
async def book_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.message
    user = update.effective_user
    u = get_user_cached(context, user.id) or {}
    lang = u.get('lang','en')
    # Ask which branch the user wants
    buttons = [
        [InlineKeyboardButton(tr(lang, 'branch_1'), callback_data='branch:branch_1')],
        [InlineKeyboardButton(tr(lang, 'branch_2'), callback_data='branch:branch_2')],
        [InlineKeyboardButton(tr(lang,'back'), callback_data='back:menu')]
    ]
    await msg.reply_text('Choose branch / Filialni tanlang', reply_markup=InlineKeyboardMarkup(buttons))


@safe_handler
async def branch_selected(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await safe_answer(q)
    data = q.data
    if not data.startswith('branch:'):
        return
    branch_key = data.split(':',1)[1]
    user = update.effective_user
    u = get_user_cached(context, user.id) or {}
    lang = u.get('lang','en')

    # show dates for selected branch based on weekday
    # branch_2: Monday, Wednesday, Friday (0,2,4)
    # branch_1: Tuesday, Thursday, Saturday (1,3,5)
    dates = next_14_dates()
    filtered = []
    today = datetime.now(TZ).date()
    today_wd = today.weekday()
    
    # Check if today has available slots for this branch
    today_has_slots = False
    if branch_key == 'branch_2' and today_wd in (0, 2, 4):
        today_has_slots = True
    elif branch_key == 'branch_1' and today_wd in (1, 3, 5):
        today_has_slots = True
    
    # Only include today if it's not Sunday
    if today_has_slots:
        filtered.append(today)
    
    for d in dates:
        if d == today:  # Skip today - already added
            continue
        wd = d.weekday()
        if branch_key == 'branch_2' and wd in (0, 2, 4):
            filtered.append(d)
        elif branch_key == 'branch_1' and wd in (1, 3, 5):
            filtered.append(d)

    # Use calendar-style keyboard for better UX
    buttons = build_calendar_buttons(filtered, branch_key)
    # Add back button
    buttons.append([InlineKeyboardButton(tr(lang,'back'), callback_data='back:menu')])

    await q.edit_message_text(f"{tr(lang, 'choose_date')}\n{tr(lang, branch_key)}", reply_markup=InlineKeyboardMarkup(buttons))

@safe_handler
async def date_selected(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await safe_answer(q)
    data = q.data
    user = update.effective_user
    u = get_user_cached(context, user.id) or {}
    lang = u.get('lang','en')
    if not data.startswith('date:'):
        return
    payload = data.split(':',1)[1]
    if '|' in payload:
        branch_key, date_str = payload.split('|',1)
    else:
        # backward compatibility
        date_str = payload
        d = datetime.fromisoformat(date_str).date()
        # pick branch based on parity of day
        branch_key = 'branch_1' if d.day % 2 == 0 else 'branch_2'
    d = datetime.fromisoformat(date_str).date()
    # Do not allow booking on closed dates
    try:
        if db.is_date_closed(date_str):
            reason = db.get_closed_date_reason(date_str)
            if reason:
                msg = f"🔒 {tr(lang, 'date_closed')}\n\n📝 <b>Sabab:</b> {reason}"
            else:
                msg = f"🔒 {tr(lang, 'date_closed')}"
            from telegram.constants import ParseMode
            await _safe_edit_message(q, msg, parse_mode=ParseMode.HTML)
            return
    except Exception:
        logger.exception('Failed to check closed date')
    times = branch_slots_for_date(branch_key, d)
    buttons = []
    row = []
    now_tz = datetime.now(TZ)
    today_tz = now_tz.date()
    # arrange time buttons horizontally (3 per row)
    for t in times:
        # compute start_ts in UTC
        hh, mm = map(int, t.split(':'))
        local_dt = TZ.localize(datetime.combine(d, dtime(hh, mm)))
        start_ts = local_dt.astimezone(pytz.utc).isoformat()
        
        # Check if this is today and time has passed
        is_today = d == today_tz
        slot_passed = is_today and local_dt <= now_tz
        
        if slot_passed:
            # Time has passed - disable this slot
            cb = InlineKeyboardButton('⏰', callback_data='slot_passed')
        elif db.is_slot_free(start_ts):
            cb = InlineKeyboardButton(t, callback_data=f'slot:{date_str}|{t}|{branch_key}')
        else:
            cb = InlineKeyboardButton('⛔', callback_data='slot_taken')
        row.append(cb)
        if len(row) == 3:
            buttons.append(row)
            row = []
    if row:
        buttons.append(row)
    # add back button to go back to dates (preserve branch_key)
    buttons.append([InlineKeyboardButton(tr(lang,'back'), callback_data=f'back:dates:{branch_key}')])
    # show branch information before time selection (localized)
    branch_label = tr(lang, branch_key)
    msg_text = f"{tr(lang, 'choose_slot')}\n{branch_label}"
    await _safe_edit_message(q, msg_text, reply_markup=InlineKeyboardMarkup(buttons))

@safe_handler
async def slot_selected(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await safe_answer(q)
    data = q.data
    user = update.effective_user
    u = get_user_cached(context, user.id) or {}
    lang = u.get('lang','en')
    if data == 'slot_taken':
        await q.answer(tr(lang, 'slot_taken'), show_alert=True)
        return
    if data == 'slot_passed':
        await q.answer(tr(lang, 'slot_passed'), show_alert=True)
        return
    if not data.startswith('slot:'):
        return
    payload = data.split(':',1)[1]
    parts = payload.split('|')
    if len(parts) == 3:
        date_str, time_str, branch_key = parts
    else:
        date_str, time_str = parts[0], parts[1]
        branch_key = weekday_slots(datetime.fromisoformat(date_str).date())[1]
    d = datetime.fromisoformat(date_str).date()
    hh, mm = map(int, time_str.split(':'))
    local_dt = TZ.localize(datetime.combine(d, dtime(hh, mm)))
    start_ts = local_dt.astimezone(pytz.utc).isoformat()

    # Admin reschedule flow: if admin initiated reschedule, capture chosen slot and ask for reason
    if user.id in config.ADMIN_IDS and 'admin_reschedule_bid' in context.user_data:
        bid_to = context.user_data.pop('admin_reschedule_bid')
        if not db.is_slot_free(start_ts):
            await q.edit_message_text(tr(lang, 'slot_taken'))
            return
        context.user_data['admin_reschedule_pending'] = {'bid': bid_to, 'date': date_str, 'time': time_str, 'start_ts': start_ts, 'branch': branch_key}
        await q.edit_message_text('📝 Please enter reason for rescheduling this lesson:')
        return

    # weekly limit check
    # define week range (Mon-Sun) in UTC
    week_start = (local_dt - timedelta(days=local_dt.weekday())).replace(hour=0, minute=0, second=0, microsecond=0)
    week_end = week_start + timedelta(days=7)
    week_start_utc = week_start.astimezone(pytz.utc).isoformat()
    week_end_utc = week_end.astimezone(pytz.utc).isoformat()
    cnt = db.count_user_bookings_in_week(user.id, week_start_utc, week_end_utc)
    if cnt >= 50:
        await q.edit_message_text(tr(lang, 'max_weekly'))
        return

    if not db.is_slot_free(start_ts):
        await q.edit_message_text(tr(lang, 'slot_taken'))
        return

    # Final validation: check one more time to prevent race conditions
    # where another user booked the slot while this user was viewing the menu
    if not db.is_slot_free(start_ts):
        await q.edit_message_text(tr(lang, 'slot_taken'))
        return

    # store selection in user_data (store branch key too)
    context.user_data['pending'] = {'date': date_str, 'time': time_str, 'start_ts': start_ts, 'branch': branch_key}
    
    # Show purpose selection buttons instead of asking for text
    purpose_buttons = [
        [InlineKeyboardButton(tr(lang, 'purpose_grammar'), callback_data='purpose:Grammar'),
         InlineKeyboardButton(tr(lang, 'purpose_speaking'), callback_data='purpose:Speaking')],
        [InlineKeyboardButton(tr(lang, 'purpose_writing'), callback_data='purpose:Writing'),
         InlineKeyboardButton(tr(lang, 'purpose_reading'), callback_data='purpose:Reading')],
        [InlineKeyboardButton(tr(lang, 'purpose_listening'), callback_data='purpose:Listening'),
         InlineKeyboardButton(tr(lang, 'purpose_all'), callback_data='purpose:All')],
        [InlineKeyboardButton(tr(lang, 'back'), callback_data='back:menu')]
    ]
    await q.edit_message_text(tr(lang, 'choose_purpose'), reply_markup=InlineKeyboardMarkup(purpose_buttons))

@safe_handler
async def purpose_selected(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await safe_answer(q)
    data = q.data
    if not data.startswith('purpose:'):
        return
    
    purpose = data.split(':',1)[1]
    user = update.effective_user
    u = get_user_cached(context, user.id) or {}
    lang = u.get('lang','en')
    pending = context.user_data.get('pending')
    
    if not pending:
        logger.debug(f'purpose_selected called but no pending for user {user.id}')
        await q.edit_message_text('❌ Session expired')
        return
    
    try:
        # Booking details
        d = datetime.fromisoformat(pending['date']).date()
        timestr = pending['time']
        hh, mm = map(int, timestr.split(':'))
        local_dt = TZ.localize(datetime.combine(d, dtime(hh, mm)))
        branch_key = pending.get('branch') or weekday_slots(d)[1]
        branch_label = tr(lang, branch_key)
        
        # re-check slot
        if not db.is_slot_free(pending['start_ts']):
            logger.warning(f"🚫 Slot {pending['start_ts']} is no longer free for user {user.id}")
            await q.edit_message_text(tr(lang, 'slot_taken'))
            return
        
        # Add booking with the selected purpose
        logger.info(f"➕ Creating booking for user {user.id}: {pending['date']} {timestr} - Purpose: {purpose}")
        bid = db.add_booking(user.id, pending['date'], timestr, pending['start_ts'], branch_key, purpose)
        logger.info(f"✅ Booking {bid} created successfully")
        
        # ... rest of reminder/notification code remains the same (copied from purpose_received) ...
        
        # schedule reminders
        dt_local = local_dt
        now_tz = datetime.now(TZ)
        remind1 = dt_local - timedelta(hours=4)
        remind2 = dt_local - timedelta(minutes=30)
        
        logger.info(f"📅 BOOKING {bid} - REMINDER SETUP: {d} {timestr}")
        logger.info(f"   4h reminder: {remind1} (in future: {remind1 > now_tz})")
        logger.info(f"   30m reminder: {remind2} (in future: {remind2 > now_tz})")
        
        # Save reminders to database for persistence
        if remind1 > now_tz:
            try:
                rid = db.save_reminder(bid, user.id, None, 'student', '4h', remind1.astimezone(pytz.utc).isoformat())
                when_utc = remind1.astimezone(pytz.utc).replace(tzinfo=None)
                context.application.job_queue.run_once(send_reminder_student, when=when_utc, data={'user_id': user.id, 'purpose': purpose, 'datetime': dt_local, 'branch': branch_label, 'reminder_id': rid}, name=f"reminder_{rid}")
                logger.info(f"✅ 4h student reminder scheduled for booking {bid} (reminder_id={rid})")
            except Exception as e:
                logger.error(f"❌ Failed to schedule 4h reminder for booking {bid}: {e}")
        else:
            logger.warning(f"⏭️ 4h reminder is in the past (not scheduled)")
        
        if remind2 > now_tz:
            try:
                rid = db.save_reminder(bid, user.id, None, 'student', '30m', remind2.astimezone(pytz.utc).isoformat())
                when_utc = remind2.astimezone(pytz.utc).replace(tzinfo=None)
                context.application.job_queue.run_once(send_reminder_student, when=when_utc, data={'user_id': user.id, 'purpose': purpose, 'datetime': dt_local, 'branch': branch_label, 'reminder_id': rid}, name=f"reminder_{rid}")
                logger.info(f"✅ 30m student reminder scheduled for booking {bid} (reminder_id={rid})")
            except Exception as e:
                logger.error(f"❌ Failed to schedule 30m reminder for booking {bid}: {e}")
        else:
            logger.warning(f"⏭️ 30m reminder is in the past (not scheduled)")

        # teacher reminders - 10 minutes before
        teacher_remind = dt_local - timedelta(minutes=10)
        u = get_user_cached(context, user.id) or {}
        # Use profile link for student in teacher reminder
        if u.get('username'):
            student_mention = f'<a href="tg://user?id={user.id}">@{u.get("username")}</a>'
            student_display = f'@{u.get("username")}'
        else:
            student_mention = f'<a href="tg://user?id={user.id}">{u.get("first_name") or user.id}</a>'
            student_display = u.get("first_name") or str(user.id)
        
        if teacher_remind > now_tz:
            try:
                rid = db.save_reminder(bid, user.id, None, 'teacher', '10m', teacher_remind.astimezone(pytz.utc).isoformat())
                for admin_id in config.ADMIN_IDS:
                    when_utc = teacher_remind.astimezone(pytz.utc).replace(tzinfo=None)
                    context.application.job_queue.run_once(send_reminder_teacher, when=when_utc, data={'admin_id': admin_id, 'student_id': user.id, 'student_display': student_display, 'purpose': purpose, 'datetime': dt_local, 'branch': branch_label, 'reminder_id': rid}, name=f"reminder_{rid}")
                logger.info(f"✅ 10m teacher reminder scheduled for booking {bid} (reminder_id={rid})")
            except Exception as e:
                logger.error(f"❌ Failed to schedule 10m teacher reminder for booking {bid}: {e}")
        else:
            logger.warning(f"⏭️ 10m teacher reminder is in the past")

        # Notify admins about the new booking
        for aid in config.ADMIN_IDS:
            try:
                formatted_date = format_date_with_weekday(pending['date'], 'en')
                notify_text = f"""
✅ <b>New Booking</b>

👤 Student: {student_mention}
📅 Date: <b>{formatted_date}</b>
🕐 Time: <b>{timestr}</b>
📍 Branch: <b>{branch_label}</b>
📝 Purpose: <b>{purpose}</b>
🎫 Booking ID: <b>#{bid}</b>
"""
                await context.bot.send_message(aid, notify_text, parse_mode='HTML')
            except Exception:
                logger.exception(f'Failed to notify admin {aid} about booking')
        
        # Clear pending and show confirmation
        context.user_data.pop('pending', None)
        formatted_date = format_date_with_weekday(pending['date'], lang)
        confirmation_text = f"""
✅ <b>{tr(lang, 'booking_confirmed')}</b>

📅 <b>Date:</b> {formatted_date}
🕐 <b>Time:</b> {timestr}
📍 <b>Branch:</b> {branch_label}
📝 <b>Purpose:</b> {purpose}
"""
        # Make timezone explicit to avoid confusion
        confirmation_text = confirmation_text + "\n⏰ (Tashkent Time)"
        await q.edit_message_text(confirmation_text, parse_mode='HTML')
        
    except Exception as e:
        logger.exception(f'Error in purpose_selected: {e}')
        await q.edit_message_text('❌ An error occurred while creating your booking')

@safe_handler
async def send_reminder_student(context: ContextTypes.DEFAULT_TYPE):
    # Guard: ensure bot is available
    if not context.bot:
        logger.error("send_reminder_student: bot not available")
        return
    
    data = context.job.data
    uid = data['user_id']
    purpose = data['purpose']
    dt = data['datetime']
    branch = data['branch']
    reminder_id = data.get('reminder_id')
    
    u = get_user_cached(context, uid) or {}
    lang = u.get('lang','en')
    txt = tr(lang,'reminder_student', purpose=purpose, datetime=dt.strftime('%A, %d %B %Y %H:%M'), branch=branch)
    try:
        await context.bot.send_message(chat_id=uid, text=txt)
        logger.info(f"✅ Student reminder sent to user {uid}")
        # Mark reminder as sent in database
        if reminder_id:
            db.mark_reminder_sent(reminder_id)
    except Exception as e:
        logger.exception(f'Failed to send student reminder to {uid}: {e}')

@safe_handler
async def send_reminder_teacher(context: ContextTypes.DEFAULT_TYPE):
    from telegram.constants import ParseMode
    
    # Guard: ensure bot is available
    if not context.bot:
        logger.error("send_reminder_teacher: bot not available")
        return
    
    data = context.job.data
    admin_id = data.get('admin_id')
    
    # Validate admin_id before sending
    if not admin_id or not isinstance(admin_id, (int, str)):
        logger.warning(f"send_reminder_teacher: invalid admin_id={admin_id}, skipping")
        return
    
    student_mention = data.get('student_mention')
    student_id = data.get('student_id')
    purpose = data.get('purpose', '')
    dt = data.get('datetime')
    branch_key = data.get('branch', 'branch_1')
    reminder_id = data.get('reminder_id')
    
    # If no student_mention, create one from student_id
    if not student_mention and student_id:
        u = db.get_user(student_id)
        if u:
            if u.get('username'):
                student_mention = f'<a href="tg://user?id={student_id}">@{u.get("username")}</a>'
            else:
                student_mention = f'<a href="tg://user?id={student_id}">{u.get("first_name") or student_id}</a>'
        else:
            student_mention = str(student_id)
    
    branch_label = tr('en', branch_key)
    
    # Format beautiful reminder for teacher
    teacher_reminder_msg = f"""
🔔 <b>Upcoming Lesson</b>

👤 Student: {student_mention}
📅 Date: <b>{dt.strftime('%A, %d %B %Y')}</b>
🕐 Time: <b>{dt.strftime('%H:%M')}</b>
📍 Branch: <b>{branch_label}</b>
📝 Purpose: <b>{purpose}</b>
"""
    
    try:
        await context.bot.send_message(chat_id=admin_id, text=teacher_reminder_msg, parse_mode=ParseMode.HTML)
        logger.info(f"✅ Teacher reminder sent to admin {admin_id}")
        # Mark reminder as sent in database
        if reminder_id:
            db.mark_reminder_sent(reminder_id)
    except Exception as e:
        logger.exception(f'Failed to send teacher reminder to admin {admin_id}: {e}')


@safe_handler
async def poll_reminders(context: ContextTypes.DEFAULT_TYPE):
    """Periodic job: check DB for unsent reminders and send any that are due.
    Runs every minute (scheduled in run())."""
    try:
        reminders = db.get_unsent_reminders()
        now_utc = datetime.now(pytz.utc)
        for rem in reminders:
            try:
                scheduled = datetime.fromisoformat(rem['scheduled_time']).replace(tzinfo=pytz.utc)
            except Exception:
                logger.exception(f"Invalid scheduled_time for reminder {rem.get('id')}")
                continue

            if scheduled <= now_utc:
                # send immediately depending on type
                if rem.get('reminder_type') == 'student':
                    try:
                        dt_local = datetime.fromisoformat(rem['start_ts']).astimezone(TZ)
                        text = tr(rem.get('lang') or 'en', 'reminder_student', purpose=rem.get('purpose'), datetime=dt_local.strftime('%A, %d %B %Y %H:%M'), branch=rem.get('branch'))
                        await context.bot.send_message(chat_id=rem.get('user_id'), text=text)
                        db.mark_reminder_sent(rem['id'])
                        logger.info(f"Polled & sent student reminder {rem['id']} to {rem.get('user_id')}")
                    except Exception:
                        logger.exception(f"Failed to poll-send student reminder {rem.get('id')}")
                elif rem.get('reminder_type') == 'teacher':
                    try:
                        # send to all admins (admin_id in reminder row may be NULL)
                        dt_local = datetime.fromisoformat(rem['start_ts']).astimezone(TZ)
                        # build a teacher reminder message
                        student_mention = 'Student'
                        if rem.get('user_id'):
                            u = db.get_user(rem.get('user_id'))
                            if u and u.get('username'):
                                student_mention = f"@{u.get('username')}"
                        text = f"🔔 Upcoming lesson with {student_mention} on {dt_local.strftime('%A, %d %B %Y %H:%M')}"
                        # Send to all admins
                        for admin_id in config.ADMIN_IDS:
                            try:
                                await context.bot.send_message(chat_id=admin_id, text=text)
                            except Exception as e:
                                logger.warning(f"Failed to send teacher reminder {rem['id']} to admin {admin_id}: {e}")
                        db.mark_reminder_sent(rem['id'])
                        logger.info(f"Polled & sent teacher reminder {rem['id']} to {len(config.ADMIN_IDS)} admins")
                    except Exception:
                        logger.exception(f"Failed to poll-send teacher reminder {rem.get('id')}")
    except Exception:
        logger.exception('poll_reminders error')


@safe_handler
async def send_60min_reminders_task(context: ContextTypes.DEFAULT_TYPE):
    """Poll DB for bookings ~60 minutes away and send reminders.

    This job runs every 5 minutes and ensures students/admins receive a 1-hour
    reminder. It records a '60m' reminder row (marked sent) to prevent duplicates.
    """
    try:
        bookings = db.get_bookings_in_exactly_one_hour()
    except Exception:
        logger.exception('send_60min_reminders_task: failed to fetch bookings')
        return

    for b in bookings:
        try:
            bid = b.get('id')
            student_id = b.get('student_id')
            student_username = b.get('student_username') or ''
            admin_id = b.get('admin_id')
            booking_iso = b.get('booking_time')

            # parse booking time
            try:
                dt = datetime.fromisoformat(booking_iso)
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=pytz.utc)
                else:
                    dt = dt.astimezone(pytz.utc)
            except Exception:
                logger.exception('Invalid booking_time for booking %s', bid)
                continue

            time_text = dt.strftime('%H:%M')
            student_msg = f"Sizning darsingiz 1 soatdan keyin boshlanadi! (vaqt: {time_text})"
            admin_msg = f"1 soatdan keyin @{student_username} bilan dars bor!" if student_username else f"1 soatdan keyin user_id={student_id} bilan dars bor!"

            # send student
            try:
                await context.bot.send_message(chat_id=student_id, text=student_msg)
                logger.info('60m reminder sent -> student %s (booking=%s)', student_id, bid)
            except Forbidden:
                logger.warning('Cannot send 60m reminder to student %s: Forbidden', student_id)
            except BadRequest as e:
                logger.warning('BadRequest sending 60m reminder to student %s: %s', student_id, e)
            except TelegramError:
                logger.exception('TelegramError sending 60m reminder to student %s', student_id)

            # send admin
            if admin_id:
                try:
                    await context.bot.send_message(chat_id=admin_id, text=admin_msg)
                    logger.info('60m reminder sent -> admin %s (booking=%s)', admin_id, bid)
                except Forbidden:
                    logger.warning('Cannot send 60m reminder to admin %s: Forbidden', admin_id)
                except BadRequest as e:
                    logger.warning('BadRequest sending 60m reminder to admin %s: %s', admin_id, e)
                except TelegramError:
                    logger.exception('TelegramError sending 60m reminder to admin %s', admin_id)

            # record a sent reminder to avoid duplicates
            try:
                db.mark_reminder_sent_for_booking(bid)
            except Exception:
                logger.exception('Failed to mark 60m reminder sent for booking %s', bid)

        except Exception:
            logger.exception('send_60min_reminders_task: error processing booking %s', b.get('id'))


@safe_handler
async def send_reminders_task(context: ContextTypes.DEFAULT_TYPE):
    """Run every minute: fetch due reminders and deliver them, marking as sent.

    This complements the one-off scheduled jobs restored on startup and
    acts as a safety net in case jobs were missed.
    """
    try:
        due = db.get_due_reminders()
    except Exception:
        logger.exception('send_reminders_task: failed to fetch due reminders')
        return

    for rem in due:
        try:
            rid = rem.get('id')
            rtype = rem.get('reminder_type')
            if rtype == 'student':
                try:
                    dt_local = datetime.fromisoformat(rem.get('start_ts')).astimezone(TZ)
                except Exception:
                    dt_local = None
                lang = rem.get('lang') or 'en'
                text = tr(lang, 'reminder_student', purpose=rem.get('purpose'), datetime=(dt_local.strftime('%A, %d %B %Y %H:%M') if dt_local else 'soon'), branch=rem.get('branch'))
                try:
                    await context.bot.send_message(chat_id=rem.get('user_id'), text=text, parse_mode='HTML')
                    try:
                        db.mark_reminder_sent(rid)
                    except Exception:
                        logger.exception(f"Failed to mark reminder {rid} as sent in DB")
                    logger.info(f"send_reminders_task: sent student reminder {rid} to {rem.get('user_id')}")
                except Forbidden:
                    # User blocked the bot -> mark reminder as sent to avoid repeated failures
                    logger.warning(f"send_reminders_task: Forbidden sending student reminder {rid} to {rem.get('user_id')} (bot blocked)")
                    try:
                        db.mark_reminder_sent(rid)
                    except Exception:
                        logger.exception(f"Failed to mark reminder {rid} as sent after Forbidden")
                except BadRequest as e:
                    # Likely invalid chat or message parameters; mark and continue
                    logger.warning(f"send_reminders_task: BadRequest sending student reminder {rid} to {rem.get('user_id')}: {e}")
                    try:
                        db.mark_reminder_sent(rid)
                    except Exception:
                        logger.exception(f"Failed to mark reminder {rid} as sent after BadRequest")
                except TelegramError as e:
                    logger.exception(f"send_reminders_task: TelegramError sending student reminder {rid} to {rem.get('user_id')}: {e}")
                except Exception:
                    logger.exception(f"send_reminders_task: failed sending student reminder {rid}")
            elif rtype == 'teacher':
                try:
                    dt_local = datetime.fromisoformat(rem.get('start_ts')).astimezone(TZ)
                except Exception:
                    dt_local = None
                admin_id = rem.get('admin_id')
                if admin_id:
                    text = f"🔔 Upcoming lesson on {dt_local.strftime('%A, %d %B %Y %H:%M') if dt_local else 'soon'}"
                    try:
                        await context.bot.send_message(chat_id=admin_id, text=text)
                        try:
                            db.mark_reminder_sent(rid)
                        except Exception:
                            logger.exception(f"Failed to mark teacher reminder {rid} as sent in DB")
                        logger.info(f"send_reminders_task: sent teacher reminder {rid} to {admin_id}")
                    except Forbidden:
                        logger.warning(f"send_reminders_task: Forbidden sending teacher reminder {rid} to {admin_id} (bot blocked or no access)")
                        try:
                            db.mark_reminder_sent(rid)
                        except Exception:
                            logger.exception(f"Failed to mark teacher reminder {rid} as sent after Forbidden")
                    except BadRequest as e:
                        logger.warning(f"send_reminders_task: BadRequest sending teacher reminder {rid} to {admin_id}: {e}")
                        try:
                            db.mark_reminder_sent(rid)
                        except Exception:
                            logger.exception(f"Failed to mark teacher reminder {rid} as sent after BadRequest")
                    except TelegramError as e:
                        logger.exception(f"send_reminders_task: TelegramError sending teacher reminder {rid} to {admin_id}: {e}")
                    except Exception:
                        logger.exception(f"send_reminders_task: failed sending teacher reminder {rid}")
        except Exception:
            logger.exception(f"send_reminders_task: error processing reminder {rem.get('id')}")

@safe_handler
async def my_bookings_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.message
    user = update.effective_user
    u = get_user_cached(context, user.id) or {}
    lang = u.get('lang','en')
    bookings = db.list_user_bookings(user.id)
    if not bookings:
        await msg.reply_text(tr(lang, 'no_bookings'))
        return
    
    # Pagination
    page = 1
    items_per_page = 10
    total_bookings = len(bookings)
    total_pages = (total_bookings + items_per_page - 1) // items_per_page
    start_idx = (page - 1) * items_per_page
    end_idx = start_idx + items_per_page
    page_bookings = bookings[start_idx:end_idx]
    
    lines = []
    for b in page_bookings:
        formatted_date = format_date_with_weekday(b['date'], lang)
        branch_label = tr(lang, b.get('branch', 'branch_1'))
        lines.append(f"<b>#{b['id']}</b>\n📅 {formatted_date}\n🕐 {b['time']}\n📍 {branch_label}")
    
    text = '\n\n'.join(lines)
    page_info = tr(lang, 'page_info', current=page, total=total_pages)
    full_text = f"<b>{tr(lang, 'my_bookings')}</b> | {page_info}\n\n{text}"
    
    buttons = []
    # Booking buttons
    for b in page_bookings:
        formatted_date = format_date_with_weekday(b['date'], lang)
        display = f"❌ #{b['id']} - {formatted_date} {b['time']}"
        buttons.append([InlineKeyboardButton(display, callback_data=f'cancel:{b["id"]}')])
    
    # Pagination buttons
    if total_pages > 1:
        row = []
        if page > 1:
            row.append(InlineKeyboardButton('◀', callback_data=f'my_bookings:p:{page-1}'))
        if page < total_pages:
            row.append(InlineKeyboardButton('▶', callback_data=f'my_bookings:p:{page+1}'))
        if row:
            buttons.append(row)
    
    from telegram.constants import ParseMode
    await msg.reply_text(full_text, parse_mode=ParseMode.HTML, reply_markup=InlineKeyboardMarkup(buttons) if buttons else None)

@safe_handler
async def cancel_selected(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await safe_answer(q)
    data = q.data
    if not data.startswith('cancel:'):
        return
    bid = int(data.split(':',1)[1])
    b = db.get_booking(bid)
    if not b:
        await q.edit_message_text('Not found')
        return
    user = update.effective_user
    u = get_user_cached(context, user.id) or {}
    lang = u.get('lang','en')
    
    # Format booking details beautifully
    from telegram.constants import ParseMode
    formatted_date = format_date_with_weekday(b['date'], lang)
    booking_details = f"""
📚 <b>Booking Details</b>

📅 <b>Date:</b> {formatted_date}
🕐 <b>Time:</b> {b['time']}
📍 <b>Branch:</b> {b['branch']}
📝 <b>Purpose:</b> {b.get('purpose', '-')}

❓ <b>{tr(lang, 'cancel_confirm')}</b>
"""
    
    # Confirm and cancel buttons
    kb = [
        [InlineKeyboardButton('✅ ' + tr(lang,'confirm'), callback_data=f'confirm_cancel:{bid}'),
         InlineKeyboardButton('❌ ' + tr(lang,'back'), callback_data='back:menu')]
    ]
    await q.edit_message_text(booking_details, parse_mode=ParseMode.HTML, reply_markup=InlineKeyboardMarkup(kb))

@safe_handler
async def my_bookings_pagination(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await safe_answer(q)
    data = q.data
    if not data.startswith('my_bookings:p:'):
        return
    
    page = int(data.split(':')[-1])
    user = update.effective_user
    u = get_user_cached(context, user.id) or {}
    lang = u.get('lang','en')
    bookings = db.list_user_bookings(user.id)
    
    if not bookings:
        await q.edit_message_text(tr(lang, 'no_bookings'))
        return
    
    # Pagination
    items_per_page = 10
    total_bookings = len(bookings)
    total_pages = (total_bookings + items_per_page - 1) // items_per_page
    
    if page < 1 or page > total_pages:
        page = 1
    
    start_idx = (page - 1) * items_per_page
    end_idx = start_idx + items_per_page
    page_bookings = bookings[start_idx:end_idx]
    
    lines = []
    for b in page_bookings:
        formatted_date = format_date_with_weekday(b['date'], lang)
        branch_label = tr(lang, b.get('branch', 'branch_1'))
        lines.append(f"<b>#{b['id']}</b>\n📅 {formatted_date}\n🕐 {b['time']}\n📍 {branch_label}")
    
    text = '\n\n'.join(lines)
    page_info = tr(lang, 'page_info', current=page, total=total_pages)
    full_text = f"<b>{tr(lang, 'my_bookings')}</b> | {page_info}\n\n{text}"
    
    buttons = []
    # Booking buttons
    for b in page_bookings:
        formatted_date = format_date_with_weekday(b['date'], lang)
        display = f"❌ #{b['id']} - {formatted_date} {b['time']}"
        buttons.append([InlineKeyboardButton(display, callback_data=f'cancel:{b["id"]}')])
    
    # Pagination buttons
    if total_pages > 1:
        row = []
        if page > 1:
            row.append(InlineKeyboardButton('◀', callback_data=f'my_bookings:p:{page-1}'))
        if page < total_pages:
            row.append(InlineKeyboardButton('▶', callback_data=f'my_bookings:p:{page+1}'))
        if row:
            buttons.append(row)
    
    from telegram.constants import ParseMode
    await q.edit_message_text(full_text, parse_mode=ParseMode.HTML, reply_markup=InlineKeyboardMarkup(buttons) if buttons else None)

@safe_handler
async def confirm_cancel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await safe_answer(q)
    data = q.data
    if not data.startswith('confirm_cancel:'):
        return
    bid = int(data.split(':',1)[1])
    b = db.get_booking(bid)
    if not b:
        await q.edit_message_text('Not found')
        return
    
    from telegram.constants import ParseMode
    
    # Cancel any scheduled jobs for this booking's reminders
    try:
        reminders = db.get_reminders_for_booking(bid)
        for r in reminders:
            rid = r.get('id')
            try:
                jobs = context.application.job_queue.get_jobs_by_name(f"reminder_{rid}")
                for job in jobs:
                    try:
                        job.schedule_removal()
                    except Exception:
                        logger.exception(f'Failed to remove job for reminder {rid}')
            except Exception:
                logger.exception(f'Failed to lookup jobs for reminder {rid}')
    except Exception:
        logger.exception('Failed while attempting to cancel scheduled reminder jobs')

    db.cancel_booking(bid)
    db.delete_reminders_for_booking(bid)  # Delete associated reminders
    # notify teachers/admins with profile
    from telegram.constants import ParseMode
    usr = get_user_cached(context, b['user_id'])
    if usr:
        student_display = usr.get('username') and ('@' + usr.get('username')) or f'<a href="tg://user?id={b["user_id"]}">{usr.get("first_name") or b["user_id"]}</a>'
    else:
        student_display = str(b['user_id'])
    for aid in config.ADMIN_IDS:
        try:
            # Format beautiful notification for admin
            formatted_date = format_date_with_weekday(b['date'], 'en')
            cancel_msg = f"""
❌ <b>Booking Canceled</b>

👤 Student: {student_display}
📅 Date: <b>{formatted_date}</b>
🕐 Time: <b>{b['time']}</b>
📍 Branch: <b>{tr('en', b['branch'])}</b>
📝 Purpose: <b>{b.get('purpose','')}</b>
"""
            await context.bot.send_message(aid, cancel_msg, parse_mode=ParseMode.HTML)
        except Exception:
            logger.exception('Failed to notify admin of cancellation')
    
    # Notify student of their cancellation
    user_lang = get_user_cached(context, b['user_id']).get('lang', 'en') if get_user_cached(context, b['user_id']) else 'en'
    try:
        formatted_date = format_date_with_weekday(b['date'], user_lang)
        student_cancel_msg = f"""
✅ <b>Booking Canceled</b>

Your lesson booking has been successfully canceled.

📅 <b>Date:</b> {formatted_date}
🕐 <b>Time:</b> {b['time']}
📍 <b>Branch:</b> {tr(user_lang, b.get('branch', 'branch_1'))}
📝 <b>Purpose:</b> {b.get('purpose', '')}

You can book another time from the main menu.
"""
        await context.bot.send_message(b['user_id'], student_cancel_msg, parse_mode=ParseMode.HTML)
    except Exception:
        logger.exception('Failed to notify student of cancellation')
    
    await q.edit_message_text(tr(user_lang, 'canceled'))

@safe_handler
async def broadcast_message_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle any message type during broadcast flow (voice, video, text, etc.)"""
    user = update.effective_user
    u = get_user_cached(context, user.id) or {}
    lang = u.get('lang','en')
    
    is_admin = user.id in config.ADMIN_IDS
    if not (is_admin and 'broadcast' in context.user_data):
        return  # Not in broadcast flow
    
    bc = context.user_data['broadcast']
    msg_type = bc.get('msg_type')
    
    if not msg_type or 'message_content' in bc:
        return  # Already have content or no type selected
    
    file_id = None
    caption = None
    
    # Try to extract file_id based on message type
    if msg_type == 'text' and update.message and update.message.text:
        file_id = update.message.text
    elif msg_type == 'voice' and update.message and update.message.voice:
        file_id = update.message.voice.file_id
        caption = update.message.caption
    elif msg_type == 'video' and update.message and update.message.video:
        file_id = update.message.video.file_id
        caption = update.message.caption
    elif msg_type == 'video_file' and update.message and update.message.video:
        file_id = update.message.video.file_id
        caption = update.message.caption
    elif msg_type == 'animation' and update.message and update.message.animation:
        file_id = update.message.animation.file_id
        caption = update.message.caption
    elif msg_type == 'document' and update.message and update.message.document:
        file_id = update.message.document.file_id
        caption = update.message.caption
    elif msg_type == 'photo' and update.message and update.message.photo:
        file_id = update.message.photo[-1].file_id
        caption = update.message.caption
    elif msg_type == 'audio' and update.message and update.message.audio:
        file_id = update.message.audio.file_id
        caption = update.message.caption
    
    if file_id:
        bc['message_content'] = file_id
        bc['caption'] = caption
        context.user_data['broadcast'] = bc
        
        confirm_text = f'📢 Broadcasting {msg_type.replace("_", " ")} message'
        if caption:
            confirm_text += f':\n\n{caption}'
        buttons = [[InlineKeyboardButton('✅ To all users', callback_data='broadcast:confirm:all')], 
                  [InlineKeyboardButton('✅ To booked students', callback_data='broadcast:confirm:booked')],
                  [InlineKeyboardButton('❌ Cancel', callback_data='back:menu')]]
        await update.message.reply_text(confirm_text + '\n\nChoose target:', reply_markup=InlineKeyboardMarkup(buttons))
    else:
        await update.message.reply_text(f'❌ Please send a valid {msg_type} message for this type')

@safe_handler
async def unknown_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # Guard: ensure we have a message with text
    if not update.message or not update.message.text:
        logger.warning(f"unknown_text: no message or text from user {update.effective_user.id}")
        return
    
    text = update.message.text
    user = update.effective_user
    u = get_user_cached(context, user.id) or {}
    lang = u.get('lang','en')
    
    from telegram.constants import ParseMode
    
    # Check if admin is deleting a booking and providing reason
    is_admin = user.id in config.ADMIN_IDS
    if is_admin and 'admin_delete_bid' in context.user_data:
        # Admin is providing reason for deletion
        bid = context.user_data.pop('admin_delete_bid')
        reason = text.strip()
        
        b = db.get_booking(bid)
        if not b:
            await update.message.reply_text('❌ Booking not found')
            return
        
        try:
            # preserve booking info for notifications
            usr = get_user_cached(context, b['user_id'])
            if usr:
                student_display = usr.get('username') and ('@' + usr.get('username')) or f'<a href="tg://user?id={b["user_id"]}">{usr.get("first_name") or b["user_id"]}</a>'
            else:
                student_display = str(b['user_id'])

            db.delete_booking(bid)
            
            # notify admins
            for aid in config.ADMIN_IDS:
                try:
                    delete_msg = f"""
❌ <b>Booking Deleted</b>

👤 Student: {student_display}
📅 Date: <b>{b['date']}</b>
🕐 Time: <b>{b['time']}</b>
📍 Branch: <b>{tr('en', b['branch'])}</b>
📝 Purpose: <b>{b.get('purpose','')}</b>
🗑️ Reason: <b>{reason}</b>
"""
                    await context.bot.send_message(aid, delete_msg, parse_mode=ParseMode.HTML)
                except Exception:
                    logger.exception('Failed to notify admin of deletion')

            # notify student with reason
            try:
                user_lang = usr.get('lang','en') if usr else 'en'
                deletion_msg = f"""
❌ <b>Your lesson has been canceled</b>

📅 <b>Date:</b> {b['date']}
🕐 <b>Time:</b> {b['time']}
📝 <b>Purpose:</b> {b.get('purpose','')}

📌 <b>Reason:</b> {reason}

💬 If you have any questions, please contact admin:
{' or '.join([config.ADMIN_USERNAMES.get(aid, f'Admin {aid}') for aid in config.ADMIN_IDS])}
"""
                await context.bot.send_message(b['user_id'], deletion_msg, parse_mode=ParseMode.HTML)
            except Exception:
                logger.exception('Failed to notify student of deletion')

            await update.message.reply_text('✅ Booking deleted and student notified')
        except Exception:
            logger.exception('Error deleting booking')
            await update.message.reply_text('❌ Error deleting booking')
        return
    
    # Check if admin is in an active admin flow (delay or broadcast)
    is_admin = user.id in config.ADMIN_IDS
    if is_admin and 'admin_delay' in context.user_data:
        # Admin is rescheduling a lesson
        bid = context.user_data.pop('admin_delay')
        try:
            s = text.strip()
            dt = datetime.strptime(s, '%Y-%m-%d %H:%M')
            # assume local TZ
            local_dt = TZ.localize(dt)
            start_ts = local_dt.astimezone(pytz.utc).isoformat()
            date = dt.date().isoformat()
            time_str = dt.time().strftime('%H:%M')
            # check free
            if not db.is_slot_free(start_ts):
                await update.message.reply_text('Slot not free')
                return
            db.update_booking_time(bid, date, time_str, start_ts)
            b = db.get_booking(bid)
            # notify student
            try:
                u = get_user_cached(context, b['user_id']) or {}
                lang = u.get('lang', 'en')
                formatted_date = format_date_with_weekday(date, lang)
                await context.bot.send_message(b['user_id'], tr(lang, 'rescheduled', date=formatted_date, time=time_str))
            except Exception:
                pass
            await update.message.reply_text('Rescheduled')
        except Exception:
            await update.message.reply_text('Invalid format')
        return

    # Admin manage flows: capture reason after date/action selected (via text input)
    if is_admin and 'admin_manage_date' in context.user_data:
        date_str = context.user_data.pop('admin_manage_date')
        action = context.user_data.pop('admin_manage_action', None)
        
        # Safety check: if action is missing, user may have just sent random text
        if not action:
            logger.warning(f"Admin {user.id} sent text but admin_manage_action is missing, ignoring")
            return
        
        reason = text.strip()
        logger.info(f"Admin {user.id} manage action={action}, date={date_str} (format: {type(date_str).__name__}), reason={reason}")
        logger.debug(f"Date string details: len={len(date_str)}, repr={repr(date_str)}, isoformat check")

        # Normalize date string to YYYY-MM-DD to ensure DB lookups match
        norm_date = None
        from datetime import datetime as _dt
        for _fmt in ('%Y-%m-%d', '%d-%m-%Y', '%d.%m.%Y', '%d/%m/%Y'):
            try:
                norm_date = _dt.strptime(date_str, _fmt).date().isoformat()
                break
            except Exception:
                pass
        if not norm_date:
            try:
                norm_date = _dt.fromisoformat(date_str).date().isoformat()
            except Exception:
                # fallback to original string (may still work with DB functions that normalize)
                norm_date = date_str
        logger.debug(f'Normalized date for admin action: {norm_date}')
        try:
            if action == 'cancel':
                # First, cancel any scheduled jobs for reminders on that date
                try:
                    rems = db.get_reminders_for_date(norm_date)
                    for r in rems:
                        rid = r.get('id')
                        try:
                            jobs = context.application.job_queue.get_jobs_by_name(f"reminder_{rid}")
                            for job in jobs:
                                try:
                                    job.schedule_removal()
                                except Exception:
                                    logger.exception(f'Failed to remove job for reminder {rid}')
                        except Exception:
                            logger.exception(f'Failed to lookup jobs for reminder {rid}')
                except Exception:
                    logger.exception('Failed while attempting to cancel scheduled reminder jobs for date')

                deleted = db.cancel_all_bookings_on_date(norm_date)
                logger.info(f"cancel_all_bookings_on_date returned {len(deleted)} bookings for date {norm_date}")
                # Extra safety: ensure any previously discovered reminder jobs are removed
                try:
                    if rems:
                        for r in rems:
                            rid = r.get('id')
                            try:
                                jobs = context.application.job_queue.get_jobs_by_name(f"reminder_{rid}")
                                for job in jobs:
                                    try:
                                        job.schedule_removal()
                                    except Exception:
                                        logger.exception(f'Failed to remove job for reminder {rid} on safety pass')
                            except Exception:
                                logger.exception(f'Failed to lookup jobs for reminder {rid} on safety pass')
                except Exception:
                    logger.exception('Safety pass: failed while ensuring reminder jobs removed')
                for aid in config.ADMIN_IDS:
                    try:
                        formatted_date = format_date_with_weekday(date_str, 'en')
                        await context.bot.send_message(aid, f"❌ Bookings deleted on {formatted_date}. Count: {len(deleted)}\nReason: {reason}")
                    except Exception:
                        logger.exception('Failed to notify admin about bulk delete')
                for b in deleted:
                    try:
                        u = get_user_cached(context, b['user_id']) or {}
                        u_lang = u.get('lang', 'en')
                        formatted_date = format_date_with_weekday(b['date'], u_lang)
                        msg = f"❌ <b>Your lesson on {formatted_date} at {b['time']} has been canceled</b>\n\n📌 <b>Reason:</b> {reason}\n\n{' '.join([config.ADMIN_USERNAMES.get(a,'Admin') for a in config.ADMIN_IDS])}"
                        await context.bot.send_message(b['user_id'], msg, parse_mode=ParseMode.HTML)
                    except Exception:
                        logger.exception('Failed to notify student for bulk delete')
                await update.message.reply_text(f'✅ Deleted {len(deleted)} bookings on {format_date_with_weekday(norm_date)}')
            else:
                # Close date action: delete existing bookings and prevent new bookings
                # First, cancel any scheduled jobs for reminders on that date
                try:
                    rems = db.get_reminders_for_date(norm_date)
                    for r in rems:
                        rid = r.get('id')
                        try:
                            jobs = context.application.job_queue.get_jobs_by_name(f"reminder_{rid}")
                            for job in jobs:
                                try:
                                    job.schedule_removal()
                                except Exception:
                                    logger.exception(f'Failed to remove job for reminder {rid}')
                        except Exception:
                            logger.exception(f'Failed to lookup jobs for reminder {rid}')
                except Exception:
                    logger.exception('Failed while attempting to cancel scheduled reminder jobs for date')

                deleted = db.cancel_all_bookings_on_date(norm_date)
                db.add_closed_date(norm_date, reason)
                # Extra safety: ensure any previously discovered reminder jobs are removed
                try:
                    if rems:
                        for r in rems:
                            rid = r.get('id')
                            try:
                                jobs = context.application.job_queue.get_jobs_by_name(f"reminder_{rid}")
                                for job in jobs:
                                    try:
                                        job.schedule_removal()
                                    except Exception:
                                        logger.exception(f'Failed to remove job for reminder {rid} on safety pass')
                            except Exception:
                                logger.exception(f'Failed to lookup jobs for reminder {rid} on safety pass')
                except Exception:
                    logger.exception('Safety pass: failed while ensuring reminder jobs removed')
                for aid in config.ADMIN_IDS:
                    try:
                        formatted_date = format_date_with_weekday(norm_date, 'en')
                        await context.bot.send_message(aid, f"🔒 Date {formatted_date} closed for bookings. Deleted {len(deleted)} bookings.\nReason: {reason}")
                    except Exception:
                        logger.exception('Failed to notify admin about close date')
                for b in deleted:
                    try:
                        u = get_user_cached(context, b['user_id']) or {}
                        u_lang = u.get('lang', 'en')
                        formatted_date = format_date_with_weekday(b['date'], u_lang)
                        msg = f"🔒 <b>This date has been closed for lessons</b>\n\n📅 <b>Canceled date:</b> {formatted_date}\n🕐 <b>Time:</b> {b['time']}\n\n📌 <b>Reason:</b> {reason}\n\nContact admin for more info"
                        await context.bot.send_message(b['user_id'], msg, parse_mode=ParseMode.HTML)
                    except Exception:
                        logger.exception('Failed to notify student about date closure')
                await update.message.reply_text(f'✅ Date {format_date_with_weekday(norm_date)} closed. Deleted {len(deleted)} bookings')
        except Exception:
            logger.exception('Error processing admin manage action')
            await update.message.reply_text('❌ Error processing request')
        return

    # Admin reschedule finalization: admin provided reason after selecting slot
    if is_admin and 'admin_reschedule_slot' in context.user_data:
        slot_info = context.user_data.pop('admin_reschedule_slot')
        bid = context.user_data.pop('admin_reschedule_bid', None)
        date_str = slot_info.get('date')
        time_str = slot_info.get('time')
        reason = text.strip()
        logger.info(f"Admin {user.id} reschedule: bid={bid}, date={date_str}, time={time_str}, reason={reason}")
        try:
            hh, mm = map(int, time_str.split(':'))
            # support short YYMMDD stored from callbacks
            try:
                if len(date_str) == 6 and date_str.isdigit():
                    d = datetime.strptime(date_str, '%y%m%d').date()
                    date_iso = d.isoformat()
                else:
                    d = datetime.fromisoformat(date_str).date()
                    date_iso = date_str
            except Exception:
                d = datetime.fromisoformat(date_str).date()
                date_iso = date_str
            local_dt = TZ.localize(datetime.combine(d, dtime(hh, mm)))
            start_ts = local_dt.astimezone(pytz.utc).isoformat()
            # Cancel any previously scheduled reminder jobs for this booking and delete DB reminders
            try:
                old_rems = db.get_reminders_for_booking(bid)
                for r in old_rems:
                    rid = r.get('id')
                    try:
                        jobs = context.application.job_queue.get_jobs_by_name(f"reminder_{rid}")
                        for job in jobs:
                            try:
                                job.schedule_removal()
                            except Exception:
                                logger.exception(f'Failed to remove job for old reminder {rid}')
                    except Exception:
                        logger.exception(f'Failed to lookup jobs for old reminder {rid}')
                # remove old reminder rows
                db.delete_reminders_for_booking(bid)
            except Exception:
                logger.exception('Failed to cleanup old reminders before reschedule')

            db.update_booking_time(bid, date_iso, time_str, start_ts)
            b = db.get_booking(bid)

            # Create new reminders for the updated booking and schedule jobs
            try:
                now_tz = datetime.now(TZ)
                # student reminders: 4 hours and 30 minutes
                remind1 = local_dt - timedelta(hours=4)
                remind2 = local_dt - timedelta(minutes=30)
                if remind1 > now_tz:
                    try:
                        rid = db.save_reminder(bid, b['user_id'], None, 'student', '4h', remind1.astimezone(pytz.utc).isoformat())
                        when_utc = remind1.astimezone(pytz.utc).replace(tzinfo=None)
                        context.application.job_queue.run_once(send_reminder_student, when=when_utc, data={'user_id': b['user_id'], 'purpose': b.get('purpose'), 'datetime': local_dt, 'branch': tr('en', b.get('branch')), 'reminder_id': rid}, name=f"reminder_{rid}")
                    except Exception:
                        logger.exception('Failed to schedule 4h student reminder during reschedule')
                if remind2 > now_tz:
                    try:
                        rid = db.save_reminder(bid, b['user_id'], None, 'student', '30m', remind2.astimezone(pytz.utc).isoformat())
                        when_utc = remind2.astimezone(pytz.utc).replace(tzinfo=None)
                        context.application.job_queue.run_once(send_reminder_student, when=when_utc, data={'user_id': b['user_id'], 'purpose': b.get('purpose'), 'datetime': local_dt, 'branch': tr('en', b.get('branch')), 'reminder_id': rid}, name=f"reminder_{rid}")
                    except Exception:
                        logger.exception('Failed to schedule 30m student reminder during reschedule')

                # teacher reminder: 10 minutes before
                teacher_remind = local_dt - timedelta(minutes=10)
                if teacher_remind > now_tz:
                    try:
                        rid = db.save_reminder(bid, b['user_id'], None, 'teacher', '10m', teacher_remind.astimezone(pytz.utc).isoformat())
                        for admin_id in config.ADMIN_IDS:
                            when_utc = teacher_remind.astimezone(pytz.utc).replace(tzinfo=None)
                            context.application.job_queue.run_once(send_reminder_teacher, when=when_utc, data={'admin_id': admin_id, 'student_id': b['user_id'], 'student_mention': None, 'purpose': b.get('purpose'), 'datetime': local_dt, 'branch': tr('en', b.get('branch')), 'reminder_id': rid}, name=f"reminder_{rid}")
                    except Exception:
                        logger.exception('Failed to schedule teacher reminder during reschedule')
            except Exception:
                logger.exception('Failed while creating new reminders after reschedule')
            from telegram.constants import ParseMode
            for aid in config.ADMIN_IDS:
                try:
                    formatted_date = format_date_with_weekday(date_str, 'en')
                    await context.bot.send_message(aid, f"🔁 Booking rescheduled: #{bid} -> {formatted_date} {time_str}\nReason: {reason}")
                except Exception:
                    logger.exception('Failed to notify admin of reschedule')
            try:
                u = get_user_cached(context, b['user_id']) or {}
                u_lang = u.get('lang', 'en')
                formatted_date = format_date_with_weekday(date_str, u_lang)
                msg = f"🔁 <b>Your lesson has been rescheduled</b>\n\n📅 <b>Date:</b> {formatted_date}\n🕐 <b>Time:</b> {time_str}\n\n📌 <b>Reason:</b> {reason}\n\nIf questions, contact admin: {' '.join([config.ADMIN_USERNAMES.get(a,'Admin') for a in config.ADMIN_IDS])}"
                await context.bot.send_message(b['user_id'], msg, parse_mode=ParseMode.HTML)
            except Exception:
                logger.exception('Failed to notify student of reschedule')
            await update.message.reply_text('✅ Booking rescheduled and student notified')
        except Exception:
            logger.exception('Error finalizing admin reschedule')
            await update.message.reply_text('❌ Error processing reschedule')
        return
    
    if is_admin and 'broadcast' in context.user_data:
        # Admin is sending a broadcast message (text only in this handler)
        bc = context.user_data['broadcast']
        msg_type = bc.get('msg_type')
        
        # For text messages, capture the text directly
        if msg_type == 'text' and 'message_content' not in bc:
            bc['message_content'] = text
            context.user_data['broadcast'] = bc
            buttons = [[InlineKeyboardButton('✅ To all users', callback_data='broadcast:confirm:all')], 
                      [InlineKeyboardButton('✅ To booked students', callback_data='broadcast:confirm:booked')],
                      [InlineKeyboardButton('❌ Cancel', callback_data='back:menu')]]
            await update.message.reply_text(f'📢 Broadcast message:\n\n{text}\n\nChoose target:', reply_markup=InlineKeyboardMarkup(buttons))
            return
    
    logger.debug(f'unknown_text: user_id={user.id}, lang={lang}, text={repr(text)}, len={len(text)}')
    logger.debug(f'  tr(lang,"book")={repr(tr(lang,"book"))}, match={text == tr(lang,"book")}')
    logger.debug(f'  tr(lang,"cancel")={repr(tr(lang,"cancel"))}, match={text == tr(lang,"cancel")}')
    logger.debug(f'  tr(lang,"select_language")={repr(tr(lang,"select_language"))}, match={text == tr(lang,"select_language")}')
    
    # booking is handled by ConversationHandler entry point; avoid duplicating call here
    if text == tr(lang,'my_bookings'):
        logger.debug('  → Matched my_bookings')
        return await my_bookings_start(update, context)
    if text == tr(lang,'select_language'):
        logger.debug('  → Matched select_language')
        kb = [[InlineKeyboardButton('English', callback_data='lang:en'), InlineKeyboardButton('Русский', callback_data='lang:ru')],
              [InlineKeyboardButton("O'zbek", callback_data='lang:uz')]]
        await update.message.reply_text('Choose language', reply_markup=InlineKeyboardMarkup(kb))
        return
    # admin panel (match localized label or english fallback)
    try:
        is_admin_check = user.id in config.ADMIN_IDS
    except Exception:
        is_admin_check = False
    admin_label_local = tr(lang, 'admin_panel')
    admin_label_en = tr('en', 'admin_panel')
    logger.debug(f'  admin check: is_admin={is_admin_check}, admin_label_local={repr(admin_label_local)}, text match={text == admin_label_local}')
    if is_admin_check and (text == admin_label_local or text == admin_label_en or text.strip().lower() == admin_label_local.lower()):
        logger.debug('  → Matched admin_panel')
        buttons = [
            [InlineKeyboardButton(tr(lang, 'view_users'), callback_data='admin:users')],
            [InlineKeyboardButton(tr(lang, 'manage_bookings'), callback_data='adm:mg')],
            [InlineKeyboardButton(tr(lang, 'send_broadcast'), callback_data='admin:broadcast')]
        ]
        await update.message.reply_text(tr(lang, 'admin_panel'), reply_markup=InlineKeyboardMarkup(buttons))
        return
    
    logger.debug(f'  No match found for text {repr(text)}')

@safe_handler
async def admin_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    data = q.data
    try:
        from telegram.constants import ParseMode
        
        # Pagination logic for bookings
        if data == 'admin:view' or data.startswith('admin:v:p:'):
            bks = db.list_upcoming_bookings()
            if not bks:
                await q.edit_message_text('No bookings')
                return
            
            # Get page number from callback data
            page = 1
            if data.startswith('admin:v:p:'):
                page = int(data.split(':')[-1])
            
            items_per_page = 5  # Reduced page size to fit Telegram message limits
            total_pages = (len(bks) + items_per_page - 1) // items_per_page
            start_idx = (page - 1) * items_per_page
            end_idx = start_idx + items_per_page
            page_items = bks[start_idx:end_idx]
            
            lines = []
            for bb in page_items:
                u = get_user_cached(context, bb['user_id'])
                if u:
                    if u.get('username'):
                        student = f"@{u.get('username')}"
                        student_mention = f'<a href="tg://user?id={bb["user_id"]}">{u.get("first_name") or student}</a>'
                    else:
                        student = u.get('first_name') or str(bb['user_id'])
                        student_mention = f'<a href="tg://user?id={bb["user_id"]}">{student}</a>'
                else:
                    student_mention = str(bb['user_id'])
                branch_label = tr('en', bb['branch']) if bb.get('branch') else ''
                # Compact format to reduce message length, include day of week
                formatted_date = format_date_with_weekday(bb['date'], 'en')
                lines.append(f"<b>#{bb['id']}</b> {formatted_date} {bb['time']} | {student_mention} | {branch_label}")
            
            text = '\n'.join(lines)
            page_info = tr('en', 'page_info', current=page, total=total_pages)
            total_upcoming = len(bks)
            full_text = f"<b>Bookings: {total_upcoming}</b> | {page_info}\n\n{text}"
            
            # Pagination buttons with shortened callback data
            buttons = []
            if total_pages > 1:
                row = []
                if page > 1:
                    row.append(InlineKeyboardButton('◀', callback_data=f'admin:v:p:{page-1}'))
                if page < total_pages:
                    row.append(InlineKeyboardButton('▶', callback_data=f'admin:v:p:{page+1}'))
                if row:
                    buttons.append(row)

            await _safe_edit_or_send(q, context, full_text, parse_mode=ParseMode.HTML, reply_markup=InlineKeyboardMarkup(buttons) if buttons else None)
        
        # Pagination logic for users
        elif data == 'admin:users' or data.startswith('admin:u:p:'):
            users = db.get_all_users()
            if not users:
                await _safe_edit_message(q, 'No users')
                return
            
            # Get page number from callback data
            page = 1
            if data.startswith('admin:u:p:'):
                page = int(data.split(':')[-1])
            
            items_per_page = 5  # Reduced page size
            total_pages = (len(users) + items_per_page - 1) // items_per_page
            start_idx = (page - 1) * items_per_page
            end_idx = start_idx + items_per_page
            page_users = users[start_idx:end_idx]
            
            lines = []
            for u in page_users:
                user_id = u['user_id']
                user_detail = db.get_user(user_id)
                if user_detail:
                    first_name = user_detail.get('first_name', 'Unknown')
                    username = user_detail.get('username')
                    
                    # Show username if available, otherwise first_name
                    if username:
                        display_name = f"@{username}"
                        user_mention = f'<a href="tg://user?id={user_id}">{display_name}</a>'
                    else:
                        display_name = first_name
                        user_mention = f'<a href="tg://user?id={user_id}">{display_name}</a>'
                    
                    lines.append(f"👤 {user_mention}\n")
            
            text = '\n'.join(lines)
            page_info = tr('en', 'page_info', current=page, total=total_pages)
            full_text = f"<b>Total users: {len(users)}</b>\n{page_info}\n\n{text}"
            
            # Pagination buttons
            buttons = []
            if total_pages > 1:
                row = []
                if page > 1:
                    row.append(InlineKeyboardButton('◀', callback_data=f'admin:u:p:{page-1}'))
                if page < total_pages:
                    row.append(InlineKeyboardButton('▶', callback_data=f'admin:u:p:{page+1}'))
                if row:
                    buttons.append(row)

            await _safe_edit_or_send(q, context, full_text, parse_mode=ParseMode.HTML, reply_markup=InlineKeyboardMarkup(buttons) if buttons else None)
        
        # support both long and short manage-view callbacks
        elif data in ('admin:manage:view', 'adm:mg:view') or data.startswith('admin:mv:p:') or data.startswith('adm:mg:mv:'):
            # use paginated DB fetch to avoid loading everything
            page = 1
            if data.startswith('admin:mv:p:') or data.startswith('adm:mg:mv:'):
                page = int(data.split(':')[-1])
            items_per_page = 10
            total = db.count_upcoming_bookings()
            total_pages = (total + items_per_page - 1) // items_per_page
            start_idx = (page - 1) * items_per_page
            bks = db.get_upcoming_bookings_paginated(limit=items_per_page, offset=start_idx)
            if not bks:
                await _safe_edit_message(q, 'No bookings')
                return
            page_items = bks
            
            lines = []
            for bb in page_items:
                u = get_user_cached(context, bb['user_id'])
                if u:
                    if u.get('username'):
                        student = f"@{u.get('username')}"
                        student_mention = f'<a href="tg://user?id={bb["user_id"]}">{u.get("first_name") or student}</a>'
                    else:
                        student = u.get('first_name') or str(bb['user_id'])
                        student_mention = f'<a href="tg://user?id={bb["user_id"]}">{student}</a>'
                else:
                    student_mention = str(bb['user_id'])
                branch_label = tr('en', bb['branch']) if bb.get('branch') else ''
                # Formatted display with emojis (include purpose)
                formatted_date = format_date_with_weekday(bb['date'], 'en')
                purpose = bb.get('purpose') or '-'
                lines.append(f"<b>#{bb['id']}</b>\n👤 {student_mention}\n📅 {formatted_date}\n🕐 {bb['time']}\n📍 {branch_label}\n📝 {purpose}")
            
            text = '\n\n'.join(lines)
            page_info = tr('en', 'page_info', current=page, total=total_pages)
            # Show total upcoming bookings above the list for admin clarity
            full_text = f"<b>📋 All Bookings</b> | {page_info}\n\n<b>Total:</b> {total} bookings\n\n{text}"
            
            buttons = []
            if total_pages > 1:
                row = []
                if page > 1:
                    row.append(InlineKeyboardButton('◀', callback_data=f'adm:mg:mv:{page-1}'))
                if page < total_pages:
                    row.append(InlineKeyboardButton('▶', callback_data=f'adm:mg:mv:{page+1}'))
                if row:
                    buttons.append(row)

            await _safe_edit_or_send(q, context, full_text, parse_mode=ParseMode.HTML, reply_markup=InlineKeyboardMarkup(buttons) if buttons else None)
        elif data in ('admin:manage:delay', 'adm:mg:delay') or data.startswith('adm:mg:delay:page:'):
            bks = db.list_upcoming_bookings()
            if not bks:
                await q.edit_message_text('No bookings')
                return
            # Check if this is a page navigation or initial load
            page = 1
            if data.startswith('adm:mg:delay:page:'):
                page = int(data.split(':')[-1])
            text, buttons, _ = build_numbered_booking_list(bks, 'adm:mg:delay', context=context, page=page)
            from telegram.constants import ParseMode
            await q.edit_message_text(text, parse_mode=ParseMode.HTML, reply_markup=InlineKeyboardMarkup(buttons))
        elif data in ('admin:manage', 'adm:mg'):
            buttons = [
                [InlineKeyboardButton(tr('en', 'cancel_all_on_date'), callback_data='adm:mg:can')],
                [InlineKeyboardButton(tr('en', 'close_bookings_on_date'), callback_data='adm:mg:clo')],
                [InlineKeyboardButton(tr('en', 'view_bookings'), callback_data='adm:mg:view')],
                [InlineKeyboardButton(tr('en', 'delay_lesson'), callback_data='adm:mg:delay')],
                [InlineKeyboardButton(tr('en', 'delete_booking'), callback_data='adm:mg:del')],
                [InlineKeyboardButton('⬅️ Back', callback_data='back:menu')]
            ]
            await _safe_edit_message(q, tr('en', 'manage_bookings'), reply_markup=InlineKeyboardMarkup(buttons))
            return
        elif data in ('admin:manage:cancel', 'admin:manage:close', 'adm:mg:can', 'adm:mg:clo'):
            # support long and short callbacks
            if data in ('admin:manage:cancel', 'adm:mg:can'):
                action = 'cancel'
            else:
                action = 'close'
            context.user_data['admin_manage_action'] = action
            buttons = [
                [InlineKeyboardButton(tr('en', 'branch_1'), callback_data='adm:mg:br:b1')],
                [InlineKeyboardButton(tr('en', 'branch_2'), callback_data='adm:mg:br:b2')],
                [InlineKeyboardButton('⬅️ Back', callback_data='back:menu')]
            ]
            await q.edit_message_text('Select branch for this action', reply_markup=InlineKeyboardMarkup(buttons))
            return
        elif data in ('admin:manage:delete', 'adm:mg:del') or data.startswith('adm:mg:del:page:'):
            bks = db.list_upcoming_bookings()
            if not bks:
                await q.edit_message_text('No bookings')
                return
            # Check if this is a page navigation or initial load
            page = 1
            if data.startswith('adm:mg:del:page:'):
                page = int(data.split(':')[-1])
            text, buttons, _ = build_numbered_booking_list(bks, 'adm:mg:del', context=context, page=page)
            from telegram.constants import ParseMode
            await _safe_edit_message(q, text, parse_mode=ParseMode.HTML, reply_markup=InlineKeyboardMarkup(buttons))
        elif data.startswith('admin:manage:delete:') or (data.startswith('adm:mg:del:') and not data.startswith('adm:mg:del:page:')):
            bid = int(data.rsplit(':',1)[-1])
            b = db.get_booking(bid)
            if not b:
                await q.edit_message_text('Not found')
                return
            from telegram.constants import ParseMode
            bookmark_date = format_date_with_weekday(b['date'], 'en')
            booking_details = f"""
📚 <b>Booking Details</b>

📅 <b>Date:</b> {bookmark_date}
🕐 <b>Time:</b> {b['time']}
📍 <b>Branch:</b> {b['branch']}
📝 <b>Purpose:</b> {b.get('purpose', '-')}

❗ <b>Are you sure you want to permanently delete this booking?</b>
"""
            kb = [
                [InlineKeyboardButton('🗑️ ' + tr('en','confirm'), callback_data=f'admin:manage:ask_delete_reason:{bid}'),
                 InlineKeyboardButton('❌ ' + tr('en','back'), callback_data='back:menu')]
            ]
            await q.edit_message_text(booking_details, parse_mode=ParseMode.HTML, reply_markup=InlineKeyboardMarkup(kb))
        elif data.startswith('admin:manage:branch:') or data.startswith('adm:mg:br:'):
            branch_key = data.rsplit(':',1)[-1]
            # map short branch keys to full keys
            if branch_key == 'b1':
                branch_key = 'branch_1'
            elif branch_key == 'b2':
                branch_key = 'branch_2'
            context.user_data['admin_manage_branch'] = branch_key
            dates = next_14_dates()
            filtered = []
            today = datetime.now(TZ).date()
            today_wd = today.weekday()
            
            if branch_key == 'branch_2' and today_wd in (0, 2, 4):
                filtered.append(today)
            elif branch_key == 'branch_1' and today_wd in (1, 3, 5):
                filtered.append(today)
            
            for d in dates:
                if d == today:
                    continue
                wd = d.weekday()
                if branch_key == 'branch_2' and wd in (0, 2, 4):
                    filtered.append(d)
                elif branch_key == 'branch_1' and wd in (1, 3, 5):
                    filtered.append(d)
            
            buttons = []
            row = []
            for d in filtered:
                text = d.strftime('%a %d %b')
                short = d.strftime('%y%m%d')
                # Show status: 🔒 if closed, 🔓 if open
                is_closed = db.is_date_closed(d.isoformat())
                status = '🔒' if is_closed else '🔓'
                button_text = f"{status} {text}"
                row.append(InlineKeyboardButton(button_text, callback_data=f'adm:mg:dt:{short}'))
                if len(row) == 2:
                    buttons.append(row)
                    row = []
            if row:
                buttons.append(row)
            buttons.append([InlineKeyboardButton('⬅️ Back', callback_data='back:menu')])
            branch_label = tr('en', branch_key)
            await q.edit_message_text(f"📅 Select date to toggle close/open", reply_markup=InlineKeyboardMarkup(buttons))
            return
        elif data.startswith('admin:manage:date:') or data.startswith('adm:mg:dt:'):
            date_str = data.rsplit(':',1)[-1]
            # support short YYMMDD format from callback
            try:
                if len(date_str) == 6 and date_str.isdigit():
                    date_obj = datetime.strptime(date_str, '%y%m%d').date()
                    date_str = date_obj.isoformat()
            except Exception:
                pass
            context.user_data['admin_manage_date'] = date_str
            
            # Check if date is already closed
            is_closed = db.is_date_closed(date_str)
            if is_closed:
                # Show option to open the date
                current_reason = db.get_closed_date_reason(date_str) or 'No reason provided'
                msg = f"🔒 <b>Date is CLOSED</b>\n\n<b>Current reason:</b> {current_reason}\n\n<b>Do you want to OPEN this date for bookings?</b>"
                # Convert to short format for callback
                try:
                    d = datetime.fromisoformat(date_str).date()
                    date_short = d.strftime('%y%m%d')
                except Exception:
                    date_short = date_str
                kb = [
                    [InlineKeyboardButton('✅ Open Date', callback_data=f'adm:mg:open:{date_short}'),
                     InlineKeyboardButton('❌ Cancel', callback_data='back:menu')]
                ]
                from telegram.constants import ParseMode
                await q.edit_message_text(msg, parse_mode=ParseMode.HTML, reply_markup=InlineKeyboardMarkup(kb))
            else:
                # Ask for reason to close
                msg = f"🔓 <b>Date is OPEN</b>\n\n<b>Enter reason to CLOSE this date:</b>"
                from telegram.constants import ParseMode
                await q.edit_message_text(msg, parse_mode=ParseMode.HTML)
            return
        elif data.startswith('admin:ask_delete_reason:'):
            bid = int(data.rsplit(':',1)[-1])
            context.user_data['admin_delete_bid'] = bid
            await q.edit_message_text('📝 Please enter the reason for deleting this booking:')
        elif data.startswith('admin:manage:delay:') or (data.startswith('adm:mg:delay:') and not data.startswith('adm:mg:delay:page:')):
            bid = int(data.rsplit(':',1)[-1])
            context.user_data['admin_reschedule_bid'] = bid
            buttons = [
                [InlineKeyboardButton(tr('en', 'branch_1'), callback_data='adm:mg:rs:br:b1')],
                [InlineKeyboardButton(tr('en', 'branch_2'), callback_data='adm:mg:rs:br:b2')],
                [InlineKeyboardButton(tr('en','back'), callback_data='back:menu')]
            ]
            await q.edit_message_text('Choose branch for rescheduling', reply_markup=InlineKeyboardMarkup(buttons))
            return
        elif data.startswith('admin:manage:reschedule:branch:') or data.startswith('adm:mg:rs:br:'):
            branch_key = data.rsplit(':',1)[-1]
            if branch_key == 'b1':
                branch_key = 'branch_1'
            elif branch_key == 'b2':
                branch_key = 'branch_2'
            context.user_data['admin_reschedule_branch'] = branch_key
            dates = next_14_dates()
            filtered = []
            today = datetime.now(TZ).date()
            today_wd = today.weekday()
            
            if branch_key == 'branch_2' and today_wd in (0, 2, 4):
                filtered.append(today)
            elif branch_key == 'branch_1' and today_wd in (1, 3, 5):
                filtered.append(today)
            
            for d in dates:
                if d == today:
                    continue
                wd = d.weekday()
                if branch_key == 'branch_2' and wd in (0, 2, 4):
                    filtered.append(d)
                elif branch_key == 'branch_1' and wd in (1, 3, 5):
                    filtered.append(d)
            
            buttons = []
            row = []
            for d in filtered:
                text = d.strftime('%a %d %b')
                short = d.strftime('%y%m%d')
                row.append(InlineKeyboardButton(text, callback_data=f'adm:mg:rs:{short}'))
                if len(row) == 2:
                    buttons.append(row)
                    row = []
            if row:
                buttons.append(row)
            buttons.append([InlineKeyboardButton('⬅️ Back', callback_data='back:menu')])
            await q.edit_message_text(f"Select new date for rescheduling", reply_markup=InlineKeyboardMarkup(buttons))
            return
        elif data.startswith('admin:manage:reschedule:date:') or data.startswith('adm:mg:rs:'):
            date_str = data.rsplit(':',1)[-1]
            branch_key = context.user_data.get('admin_reschedule_branch')
            # support short YYMMDD callback dates
            try:
                if len(date_str) == 6 and date_str.isdigit():
                    d = datetime.strptime(date_str, '%y%m%d').date()
                else:
                    d = datetime.fromisoformat(date_str).date()
            except Exception:
                d = datetime.now(TZ).date()
            times = branch_slots_for_date(branch_key, d)
            
            buttons = []
            row = []
            for t in times:
                cb_date = d.strftime('%y%m%d')
                row.append(InlineKeyboardButton(t, callback_data=f'adm:mg:rs:slot:{cb_date}|{t}'))
                if len(row) == 3:
                    buttons.append(row)
                    row = []
            if row:
                buttons.append(row)
            buttons.append([InlineKeyboardButton('⬅️ Back', callback_data='back:menu')])
            await _safe_edit_message(q, f"Select time for {d.strftime('%A, %d %B')}", reply_markup=InlineKeyboardMarkup(buttons))
            return
        elif data.startswith('admin:manage:reschedule:slot:') or data.startswith('adm:mg:rs:slot:'):
            payload = data.rsplit(':',1)[-1]
            date_str, time_str = payload.split('|')
            context.user_data['admin_reschedule_slot'] = {'date': date_str, 'time': time_str}
            await q.edit_message_text('📝 Please enter reason for rescheduling:')
            return
        elif data.startswith('admin:manage:ask_delete_reason:'):
            bid = int(data.rsplit(':',1)[-1])
            context.user_data['admin_delete_bid'] = bid
            await q.edit_message_text('📝 Please enter the reason for deleting this booking:')
        elif data == 'admin:broadcast':
            buttons = [
                [InlineKeyboardButton('📝 Text', callback_data='broadcast:type:text')],
                [InlineKeyboardButton('🎵 Voice Message', callback_data='broadcast:type:voice')],
                [InlineKeyboardButton('🎬 Video Message', callback_data='broadcast:type:video')],
                [InlineKeyboardButton('📹 Video', callback_data='broadcast:type:video_file')],
                [InlineKeyboardButton('🎥 Animation (GIF)', callback_data='broadcast:type:animation')],
                [InlineKeyboardButton('📄 Document', callback_data='broadcast:type:document')],
                [InlineKeyboardButton('🖼️ Photo', callback_data='broadcast:type:photo')],
                [InlineKeyboardButton('🔊 Audio', callback_data='broadcast:type:audio')],
                [InlineKeyboardButton('⬅️ Back', callback_data='back:menu')]
            ]
            await q.edit_message_text('📢 Select message type for broadcast:', reply_markup=InlineKeyboardMarkup(buttons))
            context.user_data['broadcast'] = {'single': True}
            return
    except Exception:
        logger.exception('Error in admin_callback')
        # notify admin user simply
        try:
            u = get_user_cached(context, update.effective_user.id) or {}
            lang = u.get('lang', 'en')
            await q.edit_message_text(tr(lang, 'error'))
        except Exception:
            pass

@safe_handler
@safe_handler
async def open_closed_date(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle opening a previously closed date"""
    q = update.callback_query
    await safe_answer(q)
    data = q.data
    if not data.startswith('adm:mg:open:'):
        return
    
    date_str = data.rsplit(':', 1)[-1]
    # Convert short format back to ISO format if needed
    try:
        if len(date_str) == 6 and date_str.isdigit():
            d = datetime.strptime(date_str, '%y%m%d').date()
            date_str = d.isoformat()
    except Exception:
        pass
    
    try:
        logger.debug(f"Attempting to open closed date: {date_str}")
        # Verify date was actually closed before attempting to open
        was_closed = db.is_date_closed(date_str)
        if not was_closed:
            logger.warning(f"Date {date_str} is not closed, cannot open")
            await q.edit_message_text("ℹ️ This date is already open for bookings.")
            return
        
        db.remove_closed_date(date_str)
        # Verify it was actually removed
        is_still_closed = db.is_date_closed(date_str)
        if is_still_closed:
            await q.edit_message_text("❌ Failed to open date. Please try again.")
            logger.error(f"Date {date_str} still appears as closed after removal attempt - DB issue suspected")
            return
        
        logger.info(f"✅ Successfully opened date {date_str}")
        # Format date nicely for display
        date_obj = datetime.fromisoformat(date_str).date()
        formatted_date = date_obj.strftime('%a %d %b %Y')

        # Rebuild the date-selection view for the current admin branch if available
        branch_key = context.user_data.get('admin_manage_branch', 'branch_1')
        logger.debug(f"Opening date {date_str} for branch {branch_key}")
        dates = next_14_dates()
        filtered = []
        today = datetime.now(TZ).date()
        today_wd = today.weekday()
        if branch_key == 'branch_2' and today_wd in (0, 2, 4):
            filtered.append(today)
        elif branch_key == 'branch_1' and today_wd in (1, 3, 5):
            filtered.append(today)
        for d in dates:
            if d == today:
                continue
            wd = d.weekday()
            if branch_key == 'branch_2' and wd in (0, 2, 4):
                filtered.append(d)
            elif branch_key == 'branch_1' and wd in (1, 3, 5):
                filtered.append(d)

        buttons = []
        row = []
        for d in filtered:
            text = d.strftime('%a %d %b')
            short = d.strftime('%y%m%d')
            # Show status: 🔒 if closed, 🔓 if open
            try:
                is_closed = db.is_date_closed(d.isoformat())
            except Exception as e:
                logger.error(f"Error checking date status for {d.isoformat()}: {e}")
                is_closed = False
            status = '🔒' if is_closed else '🔓'
            button_text = f"{status} {text}"
            row.append(InlineKeyboardButton(button_text, callback_data=f'adm:mg:dt:{short}'))
            if len(row) == 2:
                buttons.append(row)
                row = []
        if row:
            buttons.append(row)
        buttons.append([InlineKeyboardButton('⬅️ Back', callback_data='back:menu')])

        msg = f"✅ <b>Date opened for bookings!</b>\n\n📅 {formatted_date}\n\n🔓 Students can now book for this date."
        from telegram.constants import ParseMode
        
        # Clean up admin manage context to prevent text handler from triggering
        context.user_data.pop('admin_manage_date', None)
        context.user_data.pop('admin_manage_action', None)
        context.user_data.pop('admin_manage_branch', None)
        
        await q.edit_message_text(msg, parse_mode=ParseMode.HTML, reply_markup=InlineKeyboardMarkup(buttons))
        logger.info(f"Date {date_str} opened for bookings - verified in DB and date list refreshed, context cleared")
    except Exception as e:
        logger.error(f"Error opening date: {e}", exc_info=True)
        # Clean up on error too
        context.user_data.pop('admin_manage_date', None)
        context.user_data.pop('admin_manage_action', None)
        context.user_data.pop('admin_manage_branch', None)
        await q.edit_message_text("❌ Error opening date")

@safe_handler
async def broadcast_type_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle broadcast message type selection"""
    q = update.callback_query
    await q.answer()
    data = q.data
    
    if data.startswith('broadcast:type:'):
        msg_type = data.split(':', 2)[2]
        bc = context.user_data.get('broadcast', {})
        bc['msg_type'] = msg_type
        context.user_data['broadcast'] = bc
        
        type_prompts = {
            'text': '📝 Send the text message:',
            'voice': '🎵 Send a voice message:',
            'video': '🎬 Send a video message (recorded with video message feature):',
            'video_file': '📹 Send a video file:',
            'animation': '🎥 Send an animation/GIF:',
            'document': '📄 Send a document:',
            'photo': '🖼️ Send a photo:',
            'audio': '🔊 Send an audio file:'
        }
        
        prompt = type_prompts.get(msg_type, 'Send message:')
        await q.edit_message_text(prompt)
        return

@safe_handler
async def broadcast_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    data = q.data
    bc = context.user_data.pop('broadcast', {})
    if not bc:
        await q.edit_message_text('No broadcast data')
        return

    msg_type = bc.get('msg_type', 'text')
    msg_content = bc.get('message_content')
    caption = bc.get('caption')

    if data == 'broadcast:confirm:all':
        rows = db.get_all_users()
        success_count = 0
        error_count = 0
        
        for r in rows:
            uid = r['user_id']
            try:
                if msg_type == 'text':
                    await context.bot.send_message(uid, msg_content)
                elif msg_type == 'voice':
                    await context.bot.send_voice(uid, msg_content, caption=caption)
                elif msg_type == 'video':
                    await context.bot.send_video_note(uid, msg_content)
                elif msg_type == 'video_file':
                    await context.bot.send_video(uid, msg_content, caption=caption)
                elif msg_type == 'animation':
                    await context.bot.send_animation(uid, msg_content, caption=caption)
                elif msg_type == 'document':
                    await context.bot.send_document(uid, msg_content, caption=caption)
                elif msg_type == 'photo':
                    await context.bot.send_photo(uid, msg_content, caption=caption)
                elif msg_type == 'audio':
                    await context.bot.send_audio(uid, msg_content, caption=caption)
                success_count += 1
            except Exception as e:
                logger.debug(f"Failed to send broadcast to {uid}: {e}")
                error_count += 1
        
        result_text = f'✅ Broadcast sent to all users\n✅ Success: {success_count}\n❌ Failed: {error_count}'
        await q.edit_message_text(result_text)
    
    elif data == 'broadcast:confirm:booked':
        bks = db.list_upcoming_bookings()
        user_ids = {b['user_id'] for b in bks}
        success_count = 0
        error_count = 0
        
        for uid in user_ids:
            try:
                if msg_type == 'text':
                    await context.bot.send_message(uid, msg_content)
                elif msg_type == 'voice':
                    await context.bot.send_voice(uid, msg_content, caption=caption)
                elif msg_type == 'video':
                    await context.bot.send_video_note(uid, msg_content)
                elif msg_type == 'video_file':
                    await context.bot.send_video(uid, msg_content, caption=caption)
                elif msg_type == 'animation':
                    await context.bot.send_animation(uid, msg_content, caption=caption)
                elif msg_type == 'document':
                    await context.bot.send_document(uid, msg_content, caption=caption)
                elif msg_type == 'photo':
                    await context.bot.send_photo(uid, msg_content, caption=caption)
                elif msg_type == 'audio':
                    await context.bot.send_audio(uid, msg_content, caption=caption)
                success_count += 1
            except Exception as e:
                logger.debug(f"Failed to send broadcast to {uid}: {e}")
                error_count += 1
        
        result_text = f'✅ Broadcast sent to booked students\n✅ Success: {success_count}\n❌ Failed: {error_count}'
        await q.edit_message_text(result_text)

def run():
    db.init_db()
    # Run DB migrations to ensure schema is up-to-date
    try:
        from migrations import run_all_migrations
        run_all_migrations()
    except Exception:
        logger.exception('Error running migrations on startup')
    # Ensure BOT_TOKEN is configured; fail fast with clear message if missing
    config.require_bot_token()
    token = config.BOT_TOKEN
    # ensure a JobQueue is provided so context.job_queue is available in handlers
    job_queue = JobQueue()
    # persistence so user_data and conversation state survive restarts
    # use a dedicated filename to make it explicit and stable across deployments
    persistence = PicklePersistence(filepath='bot_persistence.pickle')
    # Configure timeouts to handle slow network/Telegram servers (Asia-Tashkent region)
    # Use HTTPXRequest to set timeouts (v20.x compatibility)
    request = HTTPXRequest(
        connect_timeout=30,
        read_timeout=30,
        write_timeout=30
    )
    app = ApplicationBuilder().token(token).persistence(persistence).job_queue(job_queue).request(request).build()

    async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE):
        """Handle unhandled errors with admin notifications"""
        logger.error('Unhandled error', exc_info=True)
        
        tb = traceback.format_exc()
        user_id = "unknown"
        user_info = None
        
        try:
            if hasattr(update, 'effective_user') and update.effective_user:
                user_id = update.effective_user.id
                try:
                    user_info = get_user_cached(context, user_id) if context else db.get_user(user_id)
                except Exception:
                    user_info = None
        except Exception:
            pass
        
        # Notify admins with detailed error report
        for aid in config.ADMIN_IDS:
            try:
                user_link = "Unknown"
                if user_id != "unknown":
                    if user_info:
                        first_name = user_info.get('first_name', str(user_id))
                        username = user_info.get('username')
                        if username:
                            user_link = f'<a href="tg://user?id={user_id}">@{username}</a>'
                        else:
                            user_link = f'<a href="tg://user?id={user_id}">{first_name}</a>'
                    else:
                        user_link = f'<a href="tg://user?id={user_id}">{user_id}</a>'
                
                admin_username = config.ADMIN_USERNAMES.get(aid, "")
                admin_mention = f" ({admin_username})" if admin_username else ""
                
                error_message = (
                    f"🚨 <b>UNHANDLED BOT ERROR</b>{admin_mention}\n\n"
                    f"👤 <b>User:</b> {user_link}\n"
                    f"<b>Details:</b>\n"
                    f"<pre>{tb[:2500]}</pre>"
                )
                
                await context.bot.send_message(
                    aid,
                    error_message,
                    parse_mode='HTML'
                )
            except Exception as notify_err:
                logger.error(f"Failed to notify admin {aid} of unhandled error: {notify_err}")
        
        # Notify user and ask to contact admin
        try:
            if hasattr(update, 'effective_user') and update.effective_user:
                user = update.effective_user
                u = get_user_cached(context, user.id) or {}
                lang = u.get('lang','en')
                
                # Build admin contact message
                admin_contacts = []
                for admin_id in config.ADMIN_IDS:
                    admin_username = config.ADMIN_USERNAMES.get(admin_id, f"Admin {admin_id}")
                    admin_contacts.append(admin_username)
                
                admin_list = " yoki ".join(admin_contacts) if lang == 'uz' else " or ".join(admin_contacts)
                
                error_messages = {
                    'en': f"❌ An error occurred. Please contact admin: {admin_list}",
                    'uz': f"❌ Xatolik yuz berdi. Iltimos admin bilan bog'laning: {admin_list}",
                    'ru': f"❌ Произошла ошибка. Пожалуйста, свяжитесь с администратором: {admin_list}"
                }
                
                error_text = error_messages.get(lang, error_messages['en'])
                await context.bot.send_message(chat_id=user.id, text=error_text)
        except Exception:
            logger.exception('Failed to send error message to user')

    async def startup(app_context):
        """Restore reminders from database on startup"""
        logger.info("🔄 Starting reminder restoration from database...")
        try:
            # Remove past lessons from the DB before restoring reminders
            try:
                db.delete_past_bookings()
                logger.info('Deleted past bookings from DB')
            except Exception as e:
                logger.exception(f'Failed to delete past bookings: {e}')

            reminders = db.get_unsent_reminders()
            logger.info(f"Found {len(reminders)} unsent reminders to restore")

            for reminder in reminders:
                try:
                    scheduled_time = datetime.fromisoformat(reminder['scheduled_time']).replace(tzinfo=pytz.utc)
                    now_utc = datetime.now(pytz.utc)

                    # Only schedule if reminder time is in the future
                    if scheduled_time > now_utc:
                        when_utc = scheduled_time.replace(tzinfo=None)

                        if reminder.get('reminder_type') == 'student':
                            logger.info(f"📤 Restoring student reminder {reminder['id']} for user {reminder['user_id']} at {scheduled_time}")
                            app_context.job_queue.run_once(
                                send_reminder_student,
                                when=when_utc,
                                data={
                                    'user_id': reminder['user_id'],
                                    'purpose': reminder['purpose'],
                                    'datetime': datetime.fromisoformat(reminder['start_ts']).astimezone(TZ),
                                    'branch': reminder['branch'],
                                    'reminder_id': reminder['id']
                                },
                                name=f"reminder_{reminder['id']}"
                            )

                        elif reminder.get('reminder_type') == 'teacher':
                            logger.info(f"📤 Restoring teacher reminder {reminder['id']} at {scheduled_time}")
                            # build student mention for teacher reminder
                            student_mention = "Unknown"
                            if reminder.get('user_id'):
                                student_user = db.get_user(reminder['user_id'])
                                if student_user:
                                    if student_user.get('username'):
                                        student_mention = f'<a href="tg://user?id={reminder["user_id"]}">@{student_user.get("username")}</a>'
                                    else:
                                        student_mention = f'<a href="tg://user?id={reminder["user_id"]}">{ student_user.get("first_name") or str(reminder["user_id"])}</a>'
                                else:
                                    student_mention = f'<a href="tg://user?id={reminder["user_id"]}">{reminder["user_id"]}</a>'

                            # Create separate job for each admin
                            for admin_id in config.ADMIN_IDS:
                                app_context.job_queue.run_once(
                                    send_reminder_teacher,
                                    when=when_utc,
                                    data={
                                        'admin_id': admin_id,
                                        'student_mention': student_mention,
                                        'student_id': reminder['user_id'],
                                        'purpose': reminder['purpose'],
                                        'datetime': datetime.fromisoformat(reminder['start_ts']).astimezone(TZ),
                                        'branch': reminder['branch'],
                                        'reminder_id': reminder['id']
                                    },
                                    name=f"reminder_{reminder['id']}_{admin_id}"
                                )
                            logger.debug(f"📤 Scheduled teacher reminder {reminder['id']} for {len(config.ADMIN_IDS)} admins")

                except Exception as e:
                    logger.error(f"Failed to restore reminder {reminder.get('id')}: {e}", exc_info=True)

            # Start a repeating poller to ensure reminders get sent even if DB/restore missed some
            try:
                # Use the robust send_reminders_task as the repeating safety net (every 60s)
                app_context.job_queue.run_repeating(send_reminders_task, interval=60, first=10, name="send_reminders_task")
                logger.info('Started send_reminders_task repeating job (every 60s)')
            except Exception:
                logger.exception('Failed to start poll_reminders job')
            
            logger.info("✅ Reminder restoration complete")
        except Exception as e:
            logger.error(f"Failed to restore reminders: {e}", exc_info=True)

    app.add_error_handler(error_handler)
    # Optional DB migration: run only if MIGRATE_DB=1 in environment (manual opt-in)
    try:
        if os.getenv('MIGRATE_DB', '0') == '1':
            logger.info('MIGRATE_DB=1 detected — running booking UNIQUE constraint migration')
            db.migrate_add_unique_constraint_bookings()
    except Exception:
        logger.exception('Automatic DB migration failed')

    async def shutdown(app_context):
        """Graceful shutdown: close DB connections and log final status"""
        try:
            logger.info("🛑 Bot shutting down gracefully...")
            # Stop all scheduled jobs
            app_context.job_queue.stop()
            logger.info("✅ Job queue stopped")
            # Note: SQLite connections are automatically closed when they go out of scope
            # The DB module uses connection pooling with context managers, so no explicit close needed
            logger.info("✅ Bot shutdown complete")
        except Exception as e:
            logger.exception(f"Error during shutdown: {e}")

    app.post_init = startup
    app.post_stop = shutdown

    app.add_handler(CommandHandler('start', start))
    app.add_handler(CallbackQueryHandler(lang_callback, pattern=r'^lang:'))
    
    # ✅ PROFESSIONAL ARCHITECTURE: ConversationHandler for booking flow
    booking_conv_handler = ConversationHandler(
        entry_points=[
            MessageHandler(
                filters.Regex('^' + re.escape(T['en']['book']) + '$') | 
                filters.Regex('^' + re.escape(T['uz']['book']) + '$') | 
                filters.Regex('^' + re.escape(T['ru']['book']) + '$'), 
                book_start
            )
        ],
        states={},
        fallbacks=[],
        per_message=False,
        per_chat=True
    )
    app.add_handler(booking_conv_handler)
    
    # Callbacks for booking flow (outside ConversationHandler)
    app.add_handler(CallbackQueryHandler(branch_selected, pattern=r'^branch:'))
    app.add_handler(CallbackQueryHandler(date_selected, pattern=r'^date:'))
    app.add_handler(CallbackQueryHandler(purpose_selected, pattern=r'^purpose:'))
    app.add_handler(CallbackQueryHandler(back_callback, pattern=r'^back:'))
    app.add_handler(CallbackQueryHandler(slot_selected, pattern=r'^slot:'))

    # Cancel booking flow
    app.add_handler(CallbackQueryHandler(cancel_selected, pattern=r'^cancel:'))
    app.add_handler(CallbackQueryHandler(confirm_cancel, pattern=r'^confirm_cancel:'))
    app.add_handler(CallbackQueryHandler(my_bookings_pagination, pattern=r'^my_bookings:p:'))
    
    # Admin panel - register specific handlers BEFORE general catch-all
    app.add_handler(CallbackQueryHandler(open_closed_date, pattern=r'^adm:mg:open:'))
    app.add_handler(CallbackQueryHandler(broadcast_type_callback, pattern=r'^broadcast:type:'))
    app.add_handler(CallbackQueryHandler(broadcast_callback, pattern=r'^broadcast:confirm:'))
    # General admin handler (support both 'admin:' and 'adm:' prefixes)
    app.add_handler(CallbackQueryHandler(admin_callback, pattern=r'^(admin|adm):'))
    
    # Broadcast message handler (accepts all message types: voice, video, photo, etc.)
    app.add_handler(MessageHandler(~filters.COMMAND, broadcast_message_handler), group=0)
    
    # ✅ unknown_text as GLOBAL FALLBACK (last resort, text only)
    # This runs AFTER all specific handlers and ConversationHandler
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, unknown_text), group=1)

    app.run_polling()

if __name__ == '__main__':
    run()
