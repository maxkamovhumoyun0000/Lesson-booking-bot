Lesson Booking Bot
A multi-language Telegram bot designed for scheduling educational lessons across different branches. It features an automated booking system, reminders, and a robust admin panel.

🚀 Features
Multi-language Support: Fully localized in English, Uzbek, and Russian.

Lesson Booking: Streamlined process to select a branch, date, and time slot for various subjects (Grammar, Speaking, etc.).

Booking Management: Users can view and cancel their active bookings directly through the bot.

Admin Panel: Specialized tools for administrators to manage bookings, view users, delay lessons, and send broadcast messages.

Automated Reminders: Sends notifications to both students and teachers before scheduled lessons.

Database Migrations: Includes a built-in system to manage SQLite schema changes without losing data.

🛠 Tech Stack
Language: Python 3.x

Library: python-telegram-bot (v20.5)

Database: SQLite with sqlite3

Timezone Management: pytz

Environment Management: python-dotenv

📋 Prerequisites
Before starting, ensure you have:

Python 3.8+ installed.

A Telegram Bot Token from @BotFather.

The following environment variables configured (or placed in a .env file):

BOT_TOKEN: Your unique Telegram bot token.

ADMIN_IDS: A comma-separated list of Telegram User IDs for admin access.

TIMEZONE: (Optional) Default is Asia/Tashkent.

⚙️ Installation & Setup
Clone the repository:

Bash

git clone https://github.com/maxkamovhumoyun0000/Lesson-booking-bot.git
cd Lesson-booking-bot
Install dependencies:

Bash

pip install -r requirements.txt
Initialize the Database:
Run migrations to set up the SQLite tables:

Bash

python migrations.py
Configure Environment:
Create a .env file in the root directory:

Фрагмент кода

BOT_TOKEN=your_token_here
ADMIN_IDS=841456706,5130327830
TIMEZONE=Asia/Tashkent
Run the Bot:

Bash

python bot.py
📂 Project Structure
bot.py: The main entry point containing bot handlers and core logic.

db.py: Database interface for users, bookings, and reminders.

config.py: Configuration loader for environment variables and global settings.

translations.py: Contains all text strings for English, Uzbek, and Russian.

migrations.py: Handles database schema updates and versioning.

requirements.txt: List of necessary Python packages.

🛡 Security & Logging
The bot includes detailed logging with a RotatingFileHandler that saves errors to errors.log, ensuring tracebacks are preserved for debugging while preventing files from growing indefinitely. Critical errors are automatically reported to administrators via Telegram.

Development
To contribute to this project:

Create a feature branch
Make your changes
Run tests to ensure nothing breaks
Submit a pull request
Support & Contact
Developer: Maxkamov Xumoyun
Email:
📧 mahkamovhumoyun121@gmail.com

For issues, questions, or feature requests, please contact the developer.
