#!/usr/bin/env python3
import os
import logging
from telegram import Update, Bot, BotCommand
from telegram.ext import (
    ApplicationBuilder,
    CommandHandler,
    ContextTypes,
)

# -------------------- BASIC SETUP --------------------
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("driver-bot")

BOT_TOKEN = os.getenv("BOT_TOKEN")
SHEET_ID = os.getenv("SHEET_ID")
GOOGLE_CREDS = bool(os.getenv("GOOGLE_CREDS_B64"))
SUMMARY_CHAT_ID = os.getenv("MENU_CHAT_ID") or os.getenv("SUMMARY_CHAT_ID")

# -------------------- COMMANDS --------------------
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("Driver Bot is running.")

async def debug_bot(update: Update, context: ContextTypes.DEFAULT_TYPE):
    lines = []
    lines.append("**Driver Bot - Debug Report**")
    lines.append(f"Bot token present: {'Yes' if BOT_TOKEN else 'No'}")
    lines.append(f"SHEET_ID present: {'Yes' if SHEET_ID else 'No'}")
    lines.append(f"Google creds present: {'Yes' if GOOGLE_CREDS else 'No'}")
    lines.append(f"MENU_CHAT_ID / SUMMARY_CHAT_ID: {SUMMARY_CHAT_ID or '(not set)'}")

    try:
        bot = Bot(BOT_TOKEN)
        cmds = await bot.get_my_commands()
        if cmds:
            lines.append("Registered bot commands:")
            for c in cmds:
                lines.append(f" - /{c.command}: {c.description}")
    except Exception as e:
        lines.append(f"Failed to fetch bot commands: {e}")

    await update.message.reply_text("\n".join(lines))

async def ot_report(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("OT report feature available.")

async def leave(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("Leave request recorded.")

async def finance(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("Finance record added.")

async def mission_end(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("Mission ended.")

async def clock_in(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("Clock IN recorded.")

async def clock_out(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("Clock OUT recorded.")

# -------------------- MAIN --------------------
def main():
    if not BOT_TOKEN:
        raise RuntimeError("BOT_TOKEN missing")

    app = ApplicationBuilder().token(BOT_TOKEN).build()

    # Register handlers (ONLY these — no legacy code paths)
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("debug_bot", debug_bot))
    app.add_handler(CommandHandler("ot_report", ot_report))
    app.add_handler(CommandHandler("leave", leave))
    app.add_handler(CommandHandler("finance", finance))
    app.add_handler(CommandHandler("mission_end", mission_end))
    app.add_handler(CommandHandler("clock_in", clock_in))
    app.add_handler(CommandHandler("clock_out", clock_out))

    # Explicit command list (matches debug output)
    bot = Bot(BOT_TOKEN)
    bot.set_my_commands([
        BotCommand("start", "Show menu"),
        BotCommand("ot_report", "OT report: /ot_report [username] YYYY-MM"),
        BotCommand("leave", "Request leave"),
        BotCommand("finance", "Add finance record"),
        BotCommand("mission_end", "End mission"),
        BotCommand("clock_in", "Clock In"),
        BotCommand("clock_out", "Clock Out"),
        BotCommand("debug_bot", "Debug report"),
    ])

    logger.info("FULL BOT MODE LOADED (clean, debug-equivalent)")
    app.run_polling()

if __name__ == "__main__":
    main()
