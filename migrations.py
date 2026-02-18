"""
Database Migrations System

This module manages schema changes and data migrations for the bot's SQLite database.
Run migrations manually when you need to update the schema (add columns, create new tables, etc.)
without having to delete your entire database.

Usage:
    python -c "from migrations import run_all_migrations; run_all_migrations()"

Or in your code:
    from migrations import run_all_migrations
    run_all_migrations()
"""

import sqlite3
import logging
from typing import Callable, List, Tuple
from config import DB_PATH

logger = logging.getLogger(__name__)

# Global list of migrations in order
MIGRATIONS: List[Tuple[str, Callable]] = []


def register_migration(name: str, migrate_func: Callable):
    """Register a migration function.
    
    Args:
        name: Unique migration identifier (e.g., '001_add_booking_status')
        migrate_func: Callable that performs the migration (synchronous function).
    """
    MIGRATIONS.append((name, migrate_func))


def get_applied_migrations() -> List[str]:
    """Get list of applied migration names from database."""
    conn = sqlite3.connect(DB_PATH)
    try:
        cur = conn.cursor()
        # Check if migrations table exists
        cur.execute('''
            SELECT name FROM sqlite_master 
            WHERE type='table' AND name='_migrations'
        ''')
        if not cur.fetchone():
            # Create migrations table if it doesn't exist
            cur.execute('''
                CREATE TABLE IF NOT EXISTS _migrations (
                    name TEXT PRIMARY KEY,
                    applied_at TEXT DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            conn.commit()
            return []
        
        # Get list of applied migrations
        cur.execute('SELECT name FROM _migrations ORDER BY applied_at')
        return [row[0] for row in cur.fetchall()]
    finally:
        conn.close()


def mark_migration_applied(name: str) -> None:
    """Mark a migration as applied in the database."""
    conn = sqlite3.connect(DB_PATH)
    try:
        cur = conn.cursor()
        cur.execute(
            'INSERT OR IGNORE INTO _migrations(name) VALUES (?)',
            (name,)
        )
        conn.commit()
        logger.info(f'✅ Migration "{name}" marked as applied')
    finally:
        conn.close()


def run_all_migrations() -> None:
    """Run all pending migrations in order."""
    applied = get_applied_migrations()
    pending = [(name, func) for name, func in MIGRATIONS if name not in applied]
    
    if not pending:
        logger.info('✅ All migrations already applied')
        return
    
    logger.info(f'🔄 Running {len(pending)} pending migration(s)...')
    
    for name, migrate_func in pending:
        try:
            logger.info(f'📝 Running migration: {name}')
            migrate_func()
            mark_migration_applied(name)
            logger.info(f'✅ Migration "{name}" completed successfully')
        except Exception as e:
            logger.error(f'❌ Migration "{name}" FAILED: {e}', exc_info=True)
            raise


# ============================================================================
# MIGRATION DEFINITIONS - Add your migrations below
# ============================================================================

def migration_001_add_booking_notes():
    """Example migration: Add 'notes' column to bookings table."""
    conn = sqlite3.connect(DB_PATH)
    try:
        cur = conn.cursor()
        
        # Check if column already exists (idempotent)
        cur.execute("PRAGMA table_info(bookings)")
        columns = [row[1] for row in cur.fetchall()]
        
        if 'notes' not in columns:
            logger.info('Adding "notes" column to bookings table')
            cur.execute('ALTER TABLE bookings ADD COLUMN notes TEXT DEFAULT NULL')
            conn.commit()
            logger.info('✅ Column "notes" added successfully')
        else:
            logger.info('Column "notes" already exists, skipping')
    finally:
        conn.close()


def migration_002_add_booking_delay_column():
    """Example migration: Add 'delay_minutes' column to bookings for rescheduling."""
    conn = sqlite3.connect(DB_PATH)
    try:
        cur = conn.cursor()
        
        # Check if column already exists
        cur.execute("PRAGMA table_info(bookings)")
        columns = [row[1] for row in cur.fetchall()]
        
        if 'delay_minutes' not in columns:
            logger.info('Adding "delay_minutes" column to bookings table')
            cur.execute('ALTER TABLE bookings ADD COLUMN delay_minutes INTEGER DEFAULT 0')
            conn.commit()
            logger.info('✅ Column "delay_minutes" added successfully')
        else:
            logger.info('Column "delay_minutes" already exists, skipping')
    finally:
        conn.close()


def migration_003_add_user_preferences():
    """Example migration: Add timezone preference to users table."""
    conn = sqlite3.connect(DB_PATH)
    try:
        cur = conn.cursor()
        
        # Check if column already exists
        cur.execute("PRAGMA table_info(users)")
        columns = [row[1] for row in cur.fetchall()]
        
        if 'timezone_pref' not in columns:
            logger.info('Adding "timezone_pref" column to users table')
            cur.execute('ALTER TABLE users ADD COLUMN timezone_pref TEXT DEFAULT "Asia/Tashkent"')
            conn.commit()
            logger.info('✅ Column "timezone_pref" added successfully')
        else:
            logger.info('Column "timezone_pref" already exists, skipping')
    finally:
        conn.close()


# Register migrations in order
register_migration('001_add_booking_notes', migration_001_add_booking_notes)
register_migration('002_add_booking_delay_column', migration_002_add_booking_delay_column)
register_migration('003_add_user_preferences', migration_003_add_user_preferences)


def migration_004_add_index_bookings_start_ts():
    """Add index on bookings(start_ts) to speed up time-based queries.

    Creates `idx_bookings_start_ts` if it doesn't already exist.
    """
    conn = sqlite3.connect(DB_PATH)
    try:
        cur = conn.cursor()
        logger.info('Adding index idx_bookings_start_ts on bookings(start_ts)')
        cur.execute('CREATE INDEX IF NOT EXISTS idx_bookings_start_ts ON bookings(start_ts)')
        conn.commit()
        logger.info('✅ Index idx_bookings_start_ts ensured')
    finally:
        conn.close()


register_migration('004_add_index_bookings_start_ts', migration_004_add_index_bookings_start_ts)


def migration_005_add_additional_indices():
    """Add indices for commonly used query patterns.
    
    Improves performance for:
    - Queries filtering by user_id
    - Queries filtering active bookings by start_ts
    - Queries joining reminders to bookings
    """
    conn = sqlite3.connect(DB_PATH)
    try:
        cur = conn.cursor()
        logger.info('Adding performance indices')
        cur.execute('CREATE INDEX IF NOT EXISTS idx_bookings_user_id ON bookings(user_id)')
        cur.execute('CREATE INDEX IF NOT EXISTS idx_bookings_active_start_ts ON bookings(active, start_ts)')
        cur.execute('CREATE INDEX IF NOT EXISTS idx_reminders_booking_id ON reminders(booking_id)')
        conn.commit()
        logger.info('✅ Performance indices ensured')
    finally:
        conn.close()


register_migration('005_add_additional_indices', migration_005_add_additional_indices)


def migration_006_add_unique_constraint_start_ts():
    """Add UNIQUE constraint on bookings(start_ts) to prevent concurrent bookings.
    
    This is a database-level safeguard. If duplicate start_ts values exist,
    this migration will log a warning and skip (idempotent).
    
    Note: SQLite limitations prevent adding UNIQUE constraints to existing tables
    with potential duplicates. If this fails, manually clean up duplicate bookings:
        DELETE FROM bookings WHERE id NOT IN (
            SELECT MIN(id) FROM bookings GROUP BY start_ts
        );
    """
    conn = sqlite3.connect(DB_PATH)
    try:
        cur = conn.cursor()
        
        # Check for existing duplicate start_ts values
        cur.execute('''
            SELECT start_ts, COUNT(*) as cnt 
            FROM bookings 
            WHERE active=1 AND start_ts IS NOT NULL
            GROUP BY start_ts 
            HAVING cnt > 1
        ''')
        duplicates = cur.fetchall()
        
        if duplicates:
            logger.warning(
                f'⚠️  Found {len(duplicates)} duplicate start_ts values in active bookings. '
                f'Cannot add UNIQUE constraint until duplicates are resolved. '
                f'Duplicates: {[d[0] for d in duplicates[:5]]}...'
            )
            logger.info('To resolve, manually remove duplicate bookings keeping only one per start_ts')
            return
        
        # SQLite doesn't support ADD CONSTRAINT on existing tables directly
        # Instead, we create a trigger to enforce uniqueness on INSERT/UPDATE
        cur.execute('''
            CREATE TRIGGER IF NOT EXISTS trigger_prevent_duplicate_bookings
            BEFORE INSERT ON bookings
            FOR EACH ROW
            WHEN (SELECT COUNT(*) FROM bookings 
                  WHERE start_ts = NEW.start_ts AND active = 1) > 0
            BEGIN
                SELECT RAISE(ABORT, 'Slot already booked');
            END
        ''')
        
        cur.execute('''
            CREATE TRIGGER IF NOT EXISTS trigger_prevent_duplicate_bookings_update
            BEFORE UPDATE ON bookings
            FOR EACH ROW
            WHEN NEW.active = 1 
            AND (SELECT COUNT(*) FROM bookings 
                 WHERE start_ts = NEW.start_ts AND id != NEW.id AND active = 1) > 0
            BEGIN
                SELECT RAISE(ABORT, 'Slot already booked');
            END
        ''')
        
        conn.commit()
        logger.info('✅ Triggers for booking slot uniqueness ensured')
    except Exception as e:
        logger.error(f'Migration 006 failed: {e}. This is expected if duplicates exist.')
        logger.info('You can manually resolve by deleting old duplicates and re-running migration')
    finally:
        conn.close()


register_migration('006_add_unique_constraint_start_ts', migration_006_add_unique_constraint_start_ts)


if __name__ == '__main__':
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s | %(levelname)-7s | %(name)s:%(lineno)d | %(message)s'
    )
    run_all_migrations()
    print('\n✅ All migrations completed successfully!')
