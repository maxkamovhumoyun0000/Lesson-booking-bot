import sqlite3
import threading
import logging
from typing import Optional, List, Dict
from datetime import datetime
import pytz
from config import DB_PATH

logger = logging.getLogger(__name__)

_lock = threading.Lock()

def get_conn():
    conn = sqlite3.connect(DB_PATH, timeout=15, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn


def _parse_start_ts(ts: str):
    """Parse various ISO-like timestamp formats into an aware UTC datetime or return None."""
    if not ts:
        return None
    try:
        d = datetime.fromisoformat(ts)
    except Exception:
        try:
            d = datetime.strptime(ts, '%Y-%m-%dT%H:%M:%S')
        except Exception:
            return None
    if d.tzinfo is None:
        d = pytz.utc.localize(d)
    return d

def init_db():
    with _lock:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute('''
        CREATE TABLE IF NOT EXISTS users (
            user_id INTEGER PRIMARY KEY,
            lang TEXT,
            first_name TEXT,
            username TEXT,
            created_at TEXT
        )
        ''')
        cur.execute('''
        CREATE TABLE IF NOT EXISTS bookings (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER,
            date TEXT,
            time TEXT,
            start_ts TEXT,
            branch TEXT,
            purpose TEXT,
            active INTEGER DEFAULT 1,
            created_at TEXT
        )
        ''')
        cur.execute('''
        CREATE TABLE IF NOT EXISTS reminders (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            booking_id INTEGER,
            user_id INTEGER,
            admin_id INTEGER,
            reminder_type TEXT,
            remind_time TEXT,
            scheduled_time TEXT,
            sent INTEGER DEFAULT 0,
            created_at TEXT,
            FOREIGN KEY(booking_id) REFERENCES bookings(id)
        )
        ''')
        cur.execute('''
        CREATE TABLE IF NOT EXISTS closed_dates (
            date TEXT PRIMARY KEY,
            reason TEXT,
            created_at TEXT
        )
        ''')
        # Ensure index on start_ts for faster time-range queries (reminders, upcoming bookings)
        try:
            cur.execute('CREATE INDEX IF NOT EXISTS idx_bookings_start_ts ON bookings(start_ts)')
        except Exception:
            logger.exception('Failed to create index idx_bookings_start_ts')

        conn.commit()
        conn.close()
        logger.debug('DB initialized')

def get_user(user_id: int) -> Optional[Dict]:
    with _lock:
        conn = get_conn()
        try:
            cur = conn.cursor()
            cur.execute('SELECT * FROM users WHERE user_id=?', (user_id,))
            row = cur.fetchone()
            return dict(row) if row else None
        except Exception as e:
            logger.error(f'DB error in get_user: {e}', exc_info=True)
            raise
        finally:
            conn.close()

def create_user(user_id: int, lang: str = 'en', first_name: str = '', username: str = ''):
    with _lock:
        conn = get_conn()
        cur = conn.cursor()
        now = datetime.now(pytz.utc).isoformat()
        cur.execute('INSERT OR REPLACE INTO users(user_id, lang, first_name, username, created_at) VALUES (?,?,?,?,?)',
                    (user_id, lang, first_name, username, now))
        conn.commit()
        conn.close()

def set_user_lang(user_id: int, lang: str):
    with _lock:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute('UPDATE users SET lang=? WHERE user_id=?', (lang, user_id))
        conn.commit()
        conn.close()

def count_user_bookings_in_week(user_id: int, week_start_ts: str, week_end_ts: str) -> int:
    # Be resilient to mixed start_ts formats in the DB: fetch user's active bookings
    # and count by parsing timestamps in Python rather than relying on TEXT comparison.
    with _lock:
        conn = get_conn()
        try:
            cur = conn.cursor()
            cur.execute('SELECT start_ts FROM bookings WHERE user_id=? AND active=1', (user_id,))
            rows = cur.fetchall()
            if not rows:
                return 0
            start = None
            end = None
            try:
                start = datetime.fromisoformat(week_start_ts)
            except Exception:
                start = None
            try:
                end = datetime.fromisoformat(week_end_ts)
            except Exception:
                end = None

            parse_ts = _parse_start_ts

            count = 0
            for r in rows:
                d = parse_ts(r['start_ts'])
                if not d:
                    continue
                if start and d < start:
                    continue
                if end and d >= end:
                    continue
                count += 1
            return count
        except Exception as e:
            logger.error(f'DB error in count_user_bookings_in_week: {e}', exc_info=True)
            raise
        finally:
            conn.close()

def is_slot_free(start_ts: str) -> bool:
    with _lock:
        conn = get_conn()
        try:
            cur = conn.cursor()
            cur.execute('SELECT COUNT(*) as c FROM bookings WHERE start_ts=? AND active=1', (start_ts,))
            r = cur.fetchone()
            return (r['c'] if r else 0) == 0
        except Exception as e:
            logger.error(f'DB error in is_slot_free: {e}', exc_info=True)
            raise
        finally:
            conn.close()

def add_booking(user_id: int, date: str, time: str, start_ts: str, branch: str, purpose: str) -> int:
    # Perform an atomic check-and-insert within a transaction to avoid race conditions
    with _lock:
        conn = get_conn()
        try:
            cur = conn.cursor()
            # Start an immediate transaction so other connections cannot write
            cur.execute('BEGIN IMMEDIATE')
            # Check slot availability inside the same transaction
            cur.execute('SELECT COUNT(*) as c FROM bookings WHERE start_ts=? AND active=1', (start_ts,))
            r = cur.fetchone()
            if (r['c'] if r else 0) > 0:
                raise ValueError('Slot is already taken')

            now = datetime.now(pytz.utc).isoformat()
            cur.execute('INSERT INTO bookings(user_id, date, time, start_ts, branch, purpose, created_at) VALUES (?,?,?,?,?,?,?)',
                        (user_id, date, time, start_ts, branch, purpose, now))
            bid = cur.lastrowid
            conn.commit()
            return bid
        except Exception as e:
            # Ensure any failed transaction is rolled back
            try:
                conn.rollback()
            except Exception:
                pass
            logger.error(f'DB error in add_booking: {e}', exc_info=True)
            raise
        finally:
            conn.close()

def list_user_bookings(user_id: int) -> List[Dict]:
    # Fetch user's active bookings and filter/sort in Python to handle mixed timestamp formats.
    with _lock:
        conn = get_conn()
        try:
            cur = conn.cursor()
            cur.execute('SELECT * FROM bookings WHERE user_id=? AND active=1', (user_id,))
            rows = cur.fetchall()

            parse_ts = _parse_start_ts

            now = datetime.now(pytz.utc)
            items = []
            for r in rows:
                d = parse_ts(r['start_ts'])
                if not d:
                    continue
                if d >= now:
                    item = dict(r)
                    item['_parsed_start'] = d
                    items.append(item)
            # sort by parsed datetime
            items.sort(key=lambda x: x['_parsed_start'])
            for it in items:
                it.pop('_parsed_start', None)
            return items
        except Exception as e:
            logger.error(f'DB error in list_user_bookings: {e}', exc_info=True)
            raise
        finally:
            conn.close()

def get_booking(booking_id: int) -> Optional[Dict]:
    with _lock:
        conn = get_conn()
        try:
            cur = conn.cursor()
            cur.execute('SELECT * FROM bookings WHERE id=?', (booking_id,))
            r = cur.fetchone()
            return dict(r) if r else None
        except Exception as e:
            logger.error(f'DB error in get_booking: {e}', exc_info=True)
            raise
        finally:
            conn.close()

def cancel_booking(booking_id: int):
    with _lock:
        conn = get_conn()
        try:
            cur = conn.cursor()
            cur.execute('UPDATE bookings SET active=0 WHERE id=?', (booking_id,))
            conn.commit()
        except Exception as e:
            logger.error(f'DB error in cancel_booking: {e}', exc_info=True)
            raise
        finally:
            conn.close()

def list_upcoming_bookings() -> List[Dict]:
    # Fetch active bookings and filter/sort in Python to avoid issues with TEXT timestamp comparisons
    with _lock:
        conn = get_conn()
        try:
            cur = conn.cursor()
            cur.execute('SELECT * FROM bookings WHERE active=1')
            rows = cur.fetchall()

            parse_ts = _parse_start_ts

            now = datetime.now(pytz.utc)
            items = []
            for r in rows:
                d = parse_ts(r['start_ts'])
                if not d:
                    continue
                if d >= now:
                    item = dict(r)
                    item['_parsed_start'] = d
                    items.append(item)
            items.sort(key=lambda x: x['_parsed_start'])
            for it in items:
                it.pop('_parsed_start', None)
            return items
        except Exception as e:
            logger.error(f'DB error in list_upcoming_bookings: {e}', exc_info=True)
            raise
        finally:
            conn.close()


def count_upcoming_bookings() -> int:
    """Return total count of active upcoming bookings (start_ts >= now)."""
    with _lock:
        conn = get_conn()
        try:
            cur = conn.cursor()
            now = datetime.now(pytz.utc).isoformat()
            cur.execute('SELECT COUNT(*) as c FROM bookings WHERE active=1 AND start_ts>=?', (now,))
            r = cur.fetchone()
            return int(r['c']) if r else 0
        except Exception as e:
            logger.error(f'DB error in count_upcoming_bookings: {e}', exc_info=True)
            raise
        finally:
            conn.close()

def get_upcoming_bookings_paginated(limit: int, offset: int) -> List[Dict]:
    """Fetch upcoming active bookings ordered by start_ts with LIMIT/OFFSET.

    Returns a list of booking dicts.
    """
    with _lock:
        conn = get_conn()
        try:
            cur = conn.cursor()
            now = datetime.now(pytz.utc).isoformat()
            cur.execute('SELECT * FROM bookings WHERE active=1 AND start_ts>=? ORDER BY start_ts ASC LIMIT ? OFFSET ?', (now, limit, offset))
            rows = cur.fetchall()
            return [dict(r) for r in rows]
        except Exception as e:
            logger.error(f'DB error in get_upcoming_bookings_paginated: {e}', exc_info=True)
            raise
        finally:
            conn.close()

def delete_past_bookings():
    """Delete bookings (and their reminders) that are strictly in the past.
    This permanently removes old lessons from the database.
    """
    with _lock:
        conn = get_conn()
        try:
            cur = conn.cursor()
            now = datetime.now(pytz.utc).isoformat()
            # collect ids of bookings to delete
            cur.execute('SELECT id FROM bookings WHERE start_ts<?', (now,))
            rows = cur.fetchall()
            ids = [r['id'] for r in rows]
            if ids:
                placeholders = ','.join(['?'] * len(ids))
                # delete reminders linked to those bookings
                cur.execute(f'DELETE FROM reminders WHERE booking_id IN ({placeholders})', ids)
                cur.execute(f'DELETE FROM bookings WHERE id IN ({placeholders})', ids)
                conn.commit()
                logger.info(f'Removed {len(ids)} past bookings')
        except Exception as e:
            logger.error(f'DB error in delete_past_bookings: {e}', exc_info=True)
            raise
        finally:
            conn.close()


def add_closed_date(date: str, reason: str):
    with _lock:
        conn = get_conn()
        try:
            cur = conn.cursor()
            now = datetime.now(pytz.utc).isoformat()
            cur.execute('INSERT OR REPLACE INTO closed_dates(date, reason, created_at) VALUES (?,?,?)', (date, reason, now))
            conn.commit()
        except Exception as e:
            logger.error(f'DB error in add_closed_date: {e}', exc_info=True)
            raise
        finally:
            conn.close()


    def delete_bookings_older_than(days: int = 30) -> int:
        """Delete bookings (and their reminders) older than `days` days from now.

        Returns the number of bookings deleted.
        """
        with _lock:
            conn = get_conn()
            try:
                cur = conn.cursor()
                from datetime import datetime as dt, timedelta
                cutoff = (dt.utcnow() - timedelta(days=days)).isoformat()
                # find booking ids older than cutoff
                cur.execute('SELECT id FROM bookings WHERE start_ts<?', (cutoff,))
                rows = cur.fetchall()
                ids = [r['id'] for r in rows]
                if not ids:
                    logger.debug(f'delete_bookings_older_than({days}) -> 0')
                    return 0
                placeholders = ','.join(['?'] * len(ids))
                cur.execute(f'DELETE FROM reminders WHERE booking_id IN ({placeholders})', ids)
                cur.execute(f'DELETE FROM bookings WHERE id IN ({placeholders})', ids)
                conn.commit()
                logger.info(f'delete_bookings_older_than -> removed {len(ids)} bookings older than {days} days')
                return len(ids)
            except Exception:
                logger.exception('DB error in delete_bookings_older_than')
                conn.rollback()
                raise
            finally:
                conn.close()


    def get_bookings_in_exactly_one_hour(window_seconds: int = 120) -> list:
        """Return bookings starting ~60 minutes from now (within +/- window_seconds).

        This function is robust to timestamp formats by parsing in Python. It
        excludes bookings that already have a '60m' reminder recorded in the
        `reminders` table to avoid duplicate notifications.
        """
        from datetime import datetime, timedelta
        with _lock:
            conn = get_conn()
            try:
                cur = conn.cursor()
                cur.execute('SELECT * FROM bookings WHERE active=1')
                rows = cur.fetchall()
                now = datetime.now(pytz.utc)
                target = now + timedelta(minutes=60)
                results = []
                for r in rows:
                    rec = dict(r)
                    ts = rec.get('start_ts') or rec.get('booking_time')
                    if not ts:
                        continue
                    try:
                        dt = datetime.fromisoformat(ts)
                    except Exception:
                        try:
                            dt = datetime.strptime(ts, '%Y-%m-%d %H:%M:%S')
                        except Exception:
                            dt = None
                    if not dt:
                        continue
                    if dt.tzinfo is None:
                        dt = pytz.utc.localize(dt)
                    else:
                        dt = dt.astimezone(pytz.utc)

                    if abs((dt - target).total_seconds()) <= window_seconds:
                        # ensure we haven't already recorded a 60m reminder for this booking
                        try:
                            cur2 = conn.cursor()
                            cur2.execute('SELECT 1 FROM reminders WHERE booking_id=? AND remind_time=? LIMIT 1', (rec['id'], '60m'))
                            if cur2.fetchone():
                                continue
                        except Exception:
                            logger.exception('Failed to check existing reminders for booking %s', rec.get('id'))
                        results.append({
                            'id': rec.get('id'),
                            'student_id': rec.get('user_id') or rec.get('student_id'),
                            'student_username': rec.get('student_username') or rec.get('username') or '',
                            'admin_id': rec.get('admin_id'),
                            'booking_time': dt.isoformat(),
                        })
                return results
            except Exception:
                logger.exception('DB error in get_bookings_in_exactly_one_hour')
                raise
            finally:
                conn.close()


    def mark_reminder_sent_for_booking(booking_id: int, reminder_type: str = 'student', remind_time: str = '60m') -> int:
        """Create a reminder row for the booking and mark it as sent immediately.

        Returns the reminder id.
        """
        with _lock:
            conn = get_conn()
            try:
                cur = conn.cursor()
                # fetch booking to compute scheduled_time
                cur.execute('SELECT start_ts, user_id, admin_id FROM bookings WHERE id=?', (booking_id,))
                b = cur.fetchone()
                if not b:
                    raise ValueError('Booking not found')
                start_ts = b['start_ts']
                user_id = b['user_id']
                admin_id = b.get('admin_id')

                # compute scheduled_time = start_ts - 60 minutes
                try:
                    from datetime import datetime, timedelta
                    dt = datetime.fromisoformat(start_ts)
                    if dt.tzinfo is None:
                        dt = pytz.utc.localize(dt)
                    scheduled = (dt - timedelta(minutes=60)).astimezone(pytz.utc).isoformat()
                except Exception:
                    scheduled = None

                now = datetime.now(pytz.utc).isoformat()
                cur.execute('''
                    INSERT INTO reminders(booking_id, user_id, admin_id, reminder_type, remind_time, scheduled_time, sent, created_at)
                    VALUES (?,?,?,?,?,?,1,?)
                ''', (booking_id, user_id, admin_id, reminder_type, remind_time, scheduled, now))
                rid = cur.lastrowid
                conn.commit()
                logger.debug('mark_reminder_sent_for_booking -> id=%s for booking=%s', rid, booking_id)
                return rid
            except Exception:
                logger.exception('DB error in mark_reminder_sent_for_booking')
                conn.rollback()
                raise
            finally:
                conn.close()


def is_date_closed(date: str) -> bool:
    with _lock:
        conn = get_conn()
        try:
            cur = conn.cursor()
            cur.execute('SELECT 1 FROM closed_dates WHERE date=?', (date,))
            r = cur.fetchone()
            return bool(r)
        except Exception as e:
            logger.error(f'DB error in is_date_closed: {e}', exc_info=True)
            raise
        finally:
            conn.close()


def get_closed_date_reason(date: str) -> Optional[str]:
    """Get the reason why a date is closed, or None if date is open."""
    with _lock:
        conn = get_conn()
        try:
            cur = conn.cursor()
            cur.execute('SELECT reason FROM closed_dates WHERE date=?', (date,))
            r = cur.fetchone()
            return r['reason'] if r else None
        except Exception as e:
            logger.error(f'DB error in get_closed_date_reason: {e}', exc_info=True)
            return None
        finally:
            conn.close()


def remove_closed_date(date: str):
    """Open a previously closed date for bookings."""
    with _lock:
        conn = get_conn()
        try:
            cur = conn.cursor()
            cur.execute('DELETE FROM closed_dates WHERE date=?', (date,))
            conn.commit()
        except Exception as e:
            logger.error(f'DB error in remove_closed_date: {e}', exc_info=True)
            raise
        finally:
            conn.close()


def delete_bookings_on_date(date: str) -> list:
    """Delete all bookings on the given date (date is stored as YYYY-MM-DD). Returns list of booking dicts that were deleted."""
    with _lock:
        conn = get_conn()
        try:
            logger.debug(f"delete_bookings_on_date: input date={repr(date)}, format validation...")
            # Validate date format
            try:
                from datetime import datetime as dt
                dt.strptime(date, '%Y-%m-%d')
                logger.debug(f"Date format validated: {date}")
            except ValueError as fe:
                logger.warning(f"Date format issue: {date} - {fe}")
                raise ValueError(f"Invalid date format. Expected YYYY-MM-DD, got: {date}") from fe
            
            cur = conn.cursor()
            cur.execute('SELECT * FROM bookings WHERE date=? AND active=1', (date,))
            rows = cur.fetchall()
            bookings = [dict(r) for r in rows]
            ids = [r['id'] for r in rows]
            logger.debug(f"Found {len(bookings)} active bookings for date {date}")
            
            if ids:
                placeholders = ','.join(['?'] * len(ids))
                cur.execute(f'DELETE FROM reminders WHERE booking_id IN ({placeholders})', ids)
                reminders_deleted = cur.rowcount
                cur.execute(f'DELETE FROM bookings WHERE id IN ({placeholders})', ids)
                bookings_deleted = cur.rowcount
                conn.commit()
                logger.info(f"Deleted {bookings_deleted} bookings and {reminders_deleted} reminders for date {date}")
            else:
                logger.debug(f"No active bookings found for date {date}")
            return bookings
        except Exception as e:
            logger.error(f'DB error in delete_bookings_on_date (date={repr(date)}): {e}', exc_info=True)
            raise
        finally:
            conn.close()

def update_booking_time(booking_id: int, date: str, time: str, start_ts: str):
    with _lock:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute('UPDATE bookings SET date=?, time=?, start_ts=? WHERE id=?', (date, time, start_ts, booking_id))
        conn.commit()
        conn.close()

def get_all_users() -> List[Dict]:
    with _lock:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute('SELECT user_id, lang FROM users')
        rows = cur.fetchall()
        conn.close()
    return [dict(r) for r in rows]
def save_reminder(booking_id: int, user_id: int, admin_id: Optional[int], reminder_type: str, remind_time: str, scheduled_time: str):
    """Save reminder to database for persistence"""
    with _lock:
        conn = get_conn()
        try:
            cur = conn.cursor()
            now = datetime.now(pytz.utc).isoformat()
            cur.execute('''
                INSERT INTO reminders(booking_id, user_id, admin_id, reminder_type, remind_time, scheduled_time, created_at)
                VALUES (?,?,?,?,?,?,?)
            ''', (booking_id, user_id, admin_id, reminder_type, remind_time, scheduled_time, now))
            rid = cur.lastrowid
            conn.commit()
            logger.debug(f'save_reminder -> id={rid} for booking={booking_id}')
            return rid
        except Exception as e:
            logger.error(f'DB error in save_reminder: {e}', exc_info=True)
            raise
        finally:
            conn.close()

def get_unsent_reminders() -> List[Dict]:
    """Get all unsent reminders that should be scheduled"""
    with _lock:
        conn = get_conn()
        try:
            cur = conn.cursor()
            cur.execute('''
                SELECT r.*, b.start_ts, b.purpose, b.branch, u.lang
                FROM reminders r
                JOIN bookings b ON r.booking_id = b.id
                LEFT JOIN users u ON r.user_id = u.user_id
                WHERE r.sent = 0 AND b.active = 1
                ORDER BY r.scheduled_time ASC
            ''')
            rows = cur.fetchall()
            logger.debug(f'get_unsent_reminders -> {len(rows)} reminders')
            return [dict(r) for r in rows]
        except Exception as e:
            logger.error(f'DB error in get_unsent_reminders: {e}', exc_info=True)
            raise
        finally:
            conn.close()


def get_due_reminders(now_iso: Optional[str] = None) -> List[Dict]:
    """Return reminders whose scheduled_time <= now (unsent only).

    If `now_iso` is not provided, use current UTC time. This function
    reuses `get_unsent_reminders()` and filters in Python to avoid
    brittle SQL timestamp comparisons across formats.
    """
    if now_iso:
        try:
            now = datetime.fromisoformat(now_iso)
            if now.tzinfo is None:
                now = pytz.utc.localize(now)
        except Exception:
            now = datetime.now(pytz.utc)
    else:
        now = datetime.now(pytz.utc)

    due = []
    rows = get_unsent_reminders()
    for r in rows:
        try:
            sched = _parse_start_ts(r.get('scheduled_time'))
            if not sched:
                continue
            if sched <= now:
                due.append(r)
        except Exception:
            logger.exception(f'Failed to parse scheduled_time for reminder {r.get("id")}')
            continue
    logger.debug(f'get_due_reminders -> found {len(due)} due reminders')
    return due


def migrate_add_unique_constraint_bookings():
    """Perform a safe migration to add UNIQUE(date, time, branch) constraint.

    This creates a new table with the UNIQUE constraint, copies data,
    drops the old table, and renames the new one. It does NOT run
    automatically; call this manually after backing up your DB.
    """
    with _lock:
        conn = get_conn()
        try:
            cur = conn.cursor()
            logger.info('Starting migration: add UNIQUE(date,time,branch) to bookings')
            # disable foreign keys during migration
            cur.execute('PRAGMA foreign_keys=OFF')

            cur.execute('''
                CREATE TABLE IF NOT EXISTS bookings_new (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER,
                    date TEXT,
                    time TEXT,
                    start_ts TEXT,
                    branch TEXT,
                    purpose TEXT,
                    active INTEGER DEFAULT 1,
                    created_at TEXT,
                    UNIQUE(date, time, branch)
                )
            ''')

            # copy existing data (may fail if duplicates exist)
            cur.execute('INSERT OR IGNORE INTO bookings_new(id, user_id, date, time, start_ts, branch, purpose, active, created_at) SELECT id, user_id, date, time, start_ts, branch, purpose, active, created_at FROM bookings')

            # Drop old table and rename new
            cur.execute('DROP TABLE IF EXISTS bookings')
            cur.execute('ALTER TABLE bookings_new RENAME TO bookings')

            cur.execute('PRAGMA foreign_keys=ON')
            conn.commit()
            logger.info('Migration complete: UNIQUE constraint added (duplicates were ignored)')
        except Exception:
            logger.exception('Migration failed; rolling back')
            conn.rollback()
            raise
        finally:
            conn.close()

def get_reminders_for_booking(booking_id: int) -> List[Dict]:
    """Return reminders for a specific booking."""
    with _lock:
        conn = get_conn()
        try:
            cur = conn.cursor()
            cur.execute('SELECT * FROM reminders WHERE booking_id=?', (booking_id,))
            rows = cur.fetchall()
            return [dict(r) for r in rows]
        except Exception as e:
            logger.error(f'DB error in get_reminders_for_booking: {e}', exc_info=True)
            raise
        finally:
            conn.close()


def get_reminders_for_date(date_str: str) -> List[Dict]:
    """Return reminders for all bookings on a given date (date normalization supported)."""
    with _lock:
        conn = get_conn()
        try:
            # Normalize date similar to cancel_all_bookings_on_date
            from datetime import datetime as dt
            norm_date = None
            for fmt in ('%Y-%m-%d', '%d-%m-%Y', '%d.%m.%Y', '%d/%m/%Y'):
                try:
                    norm_date = dt.strptime(date_str, fmt).date().isoformat()
                    break
                except Exception:
                    pass
            if not norm_date:
                try:
                    norm_date = dt.fromisoformat(date_str).date().isoformat()
                except Exception:
                    logger.error(f'get_reminders_for_date: invalid date format: {date_str}')
                    raise ValueError(f'Invalid date format: {date_str}')

            cur = conn.cursor()
            cur.execute('''
                SELECT r.*
                FROM reminders r
                JOIN bookings b ON r.booking_id = b.id
                WHERE b.date = ? AND r.sent = 0
            ''', (norm_date,))
            rows = cur.fetchall()
            logger.debug(f'get_reminders_for_date -> found {len(rows)} reminders for date {norm_date}')
            return [dict(r) for r in rows]
        except Exception as e:
            logger.error(f'DB error in get_reminders_for_date: {e}', exc_info=True)
            raise
        finally:
            conn.close()


def mark_reminder_sent(reminder_id: int):
    """Mark reminder as sent"""
    with _lock:
        conn = get_conn()
        try:
            cur = conn.cursor()
            cur.execute('UPDATE reminders SET sent=1 WHERE id=?', (reminder_id,))
            conn.commit()
            logger.debug(f'mark_reminder_sent -> id={reminder_id}')
        except Exception as e:
            logger.error(f'DB error in mark_reminder_sent: {e}', exc_info=True)
            raise
        finally:
            conn.close()

def delete_reminders_for_booking(booking_id: int):
    """Delete all reminders for a booking (when booking is canceled)"""
    with _lock:
        conn = get_conn()
        try:
            cur = conn.cursor()
            cur.execute('DELETE FROM reminders WHERE booking_id=?', (booking_id,))
            conn.commit()
            logger.debug(f'delete_reminders_for_booking -> booking_id={booking_id}')
        except Exception as e:
            logger.error(f'DB error in delete_reminders_for_booking: {e}', exc_info=True)
            raise
        finally:
            conn.close()

def delete_booking(booking_id: int):
    """Permanently delete a booking and its reminders."""
    with _lock:
        conn = get_conn()
        try:
            cur = conn.cursor()
            # delete reminders first
            cur.execute('DELETE FROM reminders WHERE booking_id=?', (booking_id,))
            # delete booking
            cur.execute('DELETE FROM bookings WHERE id=?', (booking_id,))
            conn.commit()
            logger.debug(f'delete_booking -> id={booking_id}')
        except Exception as e:
            logger.error(f'DB error in delete_booking: {e}', exc_info=True)
            raise
        finally:
            conn.close()


def cancel_all_bookings_on_date(date_str: str) -> list:
    """Cancel (remove) all bookings on a given date.

    This function normalizes the provided date into YYYY-MM-DD format, finds all active
    bookings on that date, deletes their reminders and the bookings themselves, and
    returns the list of deleted booking dicts.
    """
    with _lock:
        conn = get_conn()
        try:
            # Normalize incoming date to YYYY-MM-DD
            norm_date = None
            from datetime import datetime as dt
            tried = []
            for fmt in ('%Y-%m-%d', '%d-%m-%Y', '%d.%m.%Y', '%d/%m/%Y'):
                try:
                    norm_date = dt.strptime(date_str, fmt).date().isoformat()
                    break
                except Exception as e:
                    tried.append(fmt)
            if not norm_date:
                # try ISO parse as fallback
                try:
                    norm_date = dt.fromisoformat(date_str).date().isoformat()
                except Exception:
                    logger.error(f'cancel_all_bookings_on_date: invalid date format: {date_str} (tried {tried})')
                    raise ValueError(f'Invalid date format: {date_str}')

            cur = conn.cursor()
            logger.debug(f"cancel_all_bookings_on_date: normalized date={norm_date}")
            cur.execute('SELECT * FROM bookings WHERE date=? AND active=1', (norm_date,))
            rows = cur.fetchall()
            bookings = [dict(r) for r in rows]
            ids = [r['id'] for r in rows]
            logger.debug(f"Found {len(bookings)} active bookings for date {norm_date}")

            if ids:
                placeholders = ','.join(['?'] * len(ids))
                sql_rem = f'DELETE FROM reminders WHERE booking_id IN ({placeholders})'
                logger.debug(f"Executing SQL: {sql_rem} with ids={ids}")
                cur.execute(sql_rem, ids)
                sql_del = f'DELETE FROM bookings WHERE id IN ({placeholders})'
                logger.debug(f"Executing SQL: {sql_del} with ids={ids}")
                cur.execute(sql_del, ids)
                conn.commit()
                logger.info(f"cancel_all_bookings_on_date -> deleted {len(ids)} bookings and their reminders for date {norm_date}")
            else:
                logger.debug(f'No active bookings found to cancel for date {norm_date}')

            return bookings
        except Exception as e:
            logger.error(f'DB error in cancel_all_bookings_on_date (date={repr(date_str)}): {e}', exc_info=True)
            raise
        finally:
            conn.close()