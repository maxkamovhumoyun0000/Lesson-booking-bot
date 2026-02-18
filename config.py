import os
from dotenv import load_dotenv

# Load .env (if present) so environment variables can be provided from a file
load_dotenv()

# Bot token must be provided via environment variable in production
# For local development you can set it in a .env file or rely on an existing env var
BOT_TOKEN = os.getenv("BOT_TOKEN")


def require_bot_token():
    """Raise a clear error if BOT_TOKEN is not configured.

    This helper is intentionally a function (not executed at import time)
    so other utility scripts (migrations, maintenance) can import `config`
    without requiring the bot token to be present.
    Call `require_bot_token()` from the bot startup path before building the
    `Application` to fail fast when running the bot.
    """
    if not BOT_TOKEN:
        raise RuntimeError("BOT_TOKEN is not set. Please set the BOT_TOKEN environment variable or add it to .env")

# default admin IDs (can be overridden by ADMIN_IDS env var)
ADMIN_IDS = [841456706, 5130327830]
if os.getenv("ADMIN_IDS"):
    try:
        ADMIN_IDS = [int(i.strip()) for i in os.getenv("ADMIN_IDS").split(",") if i.strip()]
    except Exception:
        pass

# Admin usernames for user contact messages
ADMIN_USERNAMES = {
    841456706: "@Makhsutov1ch",    # Admin 1
    5130327830: "@Jus1_Bea1s"      # Admin 2
}

# Timezone for scheduling
TIMEZONE = os.getenv("TIMEZONE", "Asia/Tashkent")

# SQLite DB path
DB_PATH = os.getenv("DB_PATH", "lesson_bot.db")

# Directory for runtime logs (errors.log will be created here). Defaults to current working directory.
LOG_DIR = os.getenv("LOG_DIR", ".")
