#!/usr/bin/env python3
"""
driver-bot final version (paired-record mode for Google Sheet)

Features:
- English UI (commands and inline buttons)
- Auto menu in groups (keyword triggers)
- /start_trip and /end_trip with plate selection
- Writes paired records to Google Sheet:
  Columns expected in the sheet (first row header):
  | Date | Driver | Plate No. | Start date&time | End date&time | Duration |
- Reads credentials from GOOGLE_CREDS_BASE64 environment variable
- Reads BOT_TOKEN, PLATE_LIST, GOOGLE_SHEET_NAME, GOOGLE_SHEET_TAB
"""
import os
import json
import base64
import logging
from datetime import datetime
from typing import Optional

import gspread
from oauth2client.service_account import ServiceAccountCredentials

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, BotCommand, Update
from telegram.constants import ChatAction
from telegram.ext import (
    ApplicationBuilder,
    CommandHandler,
    CallbackQueryHandler,
    MessageHandler,
    filters,
    ContextTypes,
)

# Logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("driver-bot")

# Environment / defaults
BOT_TOKEN = os.getenv("BOT_TOKEN")
GOOGLE_CREDS_BASE64 = os.getenv("GOOGLE_CREDS_BASE64")
PLATE_LIST = os.getenv(
    "PLATE_LIST",
    "2BB-3071,2BB-0809,2CI-8066,2CK-8066,2CJ-8066,3H-8066,2AV-6527,2AZ-6828,2AX-4635,2BV-8320",
)
GOOGLE_SHEET_NAME = os.getenv("GOOGLE_SHEET_NAME", "Driver_Log")
GOOGLE_SHEET_TAB = os.getenv("GOOGLE_SHEET_TAB", "")  # optional tab name

PLATES = [p.strip() for p in PLATE_LIST.split(",") if p.strip()]

# Column mapping (1-indexed for gspread)
COL_DATE = 1
COL_DRIVER = 2
COL_PLATE = 3
COL_START = 4
COL_END = 5
COL_DURATION = 6

# Time formats
TS_FMT = "%Y-%m-%d %H:%M:%S"
DATE_FMT = "%Y-%m-%d"

# Google scopes
SCOPES = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]


def get_gspread_client():
    """Decode GOOGLE_CREDS_BASE64 and return a gspread client."""
    if not GOOGLE_CREDS_BASE64:
        raise RuntimeError("GOOGLE_CREDS_BASE64 environment variable is missing")
    try:
        decoded = base64.b64decode(GOOGLE_CREDS_BASE64)
        creds_json = json.loads(decoded)
    except Exception:
        logger.exception("Failed to decode GOOGLE_CREDS_BASE64")
        raise

    creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_json, SCOPES)
    client = gspread.authorize(creds)
    return client


def open_worksheet():
    """Open the worksheet object (first sheet or by tab name)."""
    gc = get_gspread_client()
    sh = gc.open(GOOGLE_SHEET_NAME)
    if GOOGLE_SHEET_TAB:
        try:
            ws = sh.worksheet(GOOGLE_SHEET_TAB)
        except Exception:
            ws = sh.sheet1
    else:
        ws = sh.sheet1
    return ws


def now_str():
    return datetime.now().strftime(TS_FMT)


def today_date_str():
    return datetime.now().strftime(DATE_FMT)


def compute_duration(start_ts: str, end_ts: str) -> str:
    try:
        s = datetime.strptime(start_ts, TS_FMT)
        e = datetime.strptime(end_ts, TS_FMT)
        delta = e - s
        total_minutes = int(delta.total_seconds() // 60)
        hours = total_minutes // 60
        minutes = total_minutes % 60
        return f"{hours}h{minutes}m"
    except Exception:
        return ""


def record_start_trip(driver: str, plate: str) -> dict:
    """
    Append a new row with start time. Return a dict with status and message.
    """
    ws = open_worksheet()
    start_ts = now_str()
    row = [today_date_str(), driver, plate, start_ts, "", ""]
    try:
        ws.append_row(row, value_input_option="USER_ENTERED")
        logger.info("Recorded start trip: %s %s", driver, plate)
        return {"ok": True, "message": f"Start time recorded for {plate} at {start_ts}"}
    except Exception as e:
        logger.exception("Failed to append start trip row")
        return {"ok": False, "message": "Failed to write start trip to sheet: " + str(e)}


def record_end_trip(driver: str, plate: str) -> dict:
    """
    Find the last row for this plate with empty End date&time and fill end time + duration.
    Return status dict.
    """
    ws = open_worksheet()
    try:
        records = ws.get_all_records()
        # iterate from bottom to top to find the last start without end
        for idx in range(len(records) - 1, -1, -1):
            rec = records[idx]
            # support multiple header name variants
            rec_plate = str(rec.get("Plate No.", rec.get("Plate", rec.get("Plate No", "")))).strip()
            end_val = str(rec.get("End date&time", rec.get("End", ""))).strip()
            start_val = str(rec.get("Start date&time", rec.get("Start", ""))).strip()
            if rec_plate == plate and (end_val == "" or end_val is None):
                # found row to update; compute row index (gspread is 1-indexed and header is row 1)
                row_number = idx + 2  # +1 for zero-index -> 1-index, +1 for header row
                end_ts = now_str()
                duration_text = compute_duration(start_val, end_ts) if start_val else ""
                ws.update_cell(row_number, COL_END, end_ts)
                ws.update_cell(row_number, COL_DURATION, duration_text)
                logger.info("Recorded end trip for %s row %d", plate, row_number)
                return {"ok": True, "message": f"End time recorded for {plate} at {end_ts} (duration {duration_text})"}
        # if not found, append a separate end-only row (fallback)
        end_ts = now_str()
        row = [today_date_str(), driver, plate, "", end_ts, ""]
        ws.append_row(row, value_input_option="USER_ENTERED")
        logger.info("No open start found; appended end-only row for %s", plate)
        return {"ok": True, "message": f"End time recorded (no matching start found) for {plate} at {end_ts}"}
    except Exception as e:
        logger.exception("Failed to update end trip")
        return {"ok": False, "message": "Failed to write end trip to sheet: " + str(e)}


# ----- Telegram UI helpers -----
def build_plate_keyboard(prefix: str):
    buttons = []
    row = []
    for i, plate in enumerate(PLATES, 1):
        row.append(InlineKeyboardButton(plate, callback_data=f"{prefix}|{plate}"))
        if i % 3 == 0:
            buttons.append(row)
            row = []
    if row:
        buttons.append(row)
    return InlineKeyboardMarkup(buttons)


async def menu_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = "Choose an action or select a plate below:\n\n• Start trip — record departure\n• End trip — record return"
    keyboard = [
        [InlineKeyboardButton("Start trip", callback_data="show_start")],
        [InlineKeyboardButton("End trip", callback_data="show_end")],
    ]
    # use context.bot.send_chat_action (ChatAction) to show typing
    try:
        await context.bot.send_chat_action(chat_id=update.effective_chat.id, action=ChatAction.TYPING)
    except Exception:
        pass
    await update.effective_chat.send_message(text=text, reply_markup=InlineKeyboardMarkup(keyboard))


async def start_trip_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        await context.bot.send_chat_action(chat_id=update.effective_chat.id, action=ChatAction.TYPING)
    except Exception:
        pass
    await update.effective_chat.send_message("Please choose the vehicle plate to START trip:", reply_markup=build_plate_keyboard("start"))


async def end_trip_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        await context.bot.send_chat_action(chat_id=update.effective_chat.id, action=ChatAction.TYPING)
    except Exception:
        pass
    await update.effective_chat.send_message("Please choose the vehicle plate to END trip:", reply_markup=build_plate_keyboard("end"))


async def plate_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    data = query.data
    if data == "show_start":
        await query.edit_message_text("Please choose the vehicle plate to START trip:", reply_markup=build_plate_keyboard("start"))
        return
    if data == "show_end":
        await query.edit_message_text("Please choose the vehicle plate to END trip:", reply_markup=build_plate_keyboard("end"))
        return
    try:
        action, plate = data.split("|", 1)
    except Exception:
        await query.edit_message_text("Invalid selection.")
        return
    user = query.from_user
    username = user.username or f"{user.first_name or ''} {user.last_name or ''}".strip()
    # call record functions
    if action == "start":
        res = record_start_trip(username, plate)
        if res["ok"]:
            await query.edit_message_text(f"✅ Started trip for {plate} (driver: {username}). {res['message']}")
        else:
            await query.edit_message_text("❌ " + res["message"])
    elif action == "end":
        res = record_end_trip(username, plate)
        if res["ok"]:
            await query.edit_message_text(f"✅ Ended trip for {plate} (driver: {username}). {res['message']}")
        else:
            await query.edit_message_text("❌ " + res["message"])
    else:
        await query.edit_message_text("Unknown action.")


# Auto keyword listener (group)
AUTO_KEYWORD_PATTERN = r'(?i)\b(start|menu|start trip|end trip|trip|出车|还车|返程)\b'


async def auto_menu_listener(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_chat and update.effective_chat.type in ("group", "supergroup"):
        text = (update.effective_message.text or "").strip()
        if not text:
            return
        # don't respond to commands (slash) to avoid duplicate messages
        if text.startswith("/"):
            return
        keyboard = [
            [InlineKeyboardButton("Start trip", callback_data="show_start"), InlineKeyboardButton("End trip", callback_data="show_end")],
            [InlineKeyboardButton("Open full menu", callback_data="menu_full")],
        ]
        await update.effective_chat.send_message("Need to record a trip? Tap a button below:", reply_markup=InlineKeyboardMarkup(keyboard))


async def register_ui_handlers(application):
    # set the bot's visible commands (English)
    await application.bot.set_my_commands(
        [
            BotCommand("start_trip", "Start a trip (select plate)"),
            BotCommand("end_trip", "End a trip (select plate)"),
            BotCommand("menu", "Open trip menu"),
        ]
    )

    application.add_handler(CommandHandler("menu", menu_command))
    application.add_handler(CommandHandler("start_trip", start_trip_command))
    application.add_handler(CommandHandler("end_trip", end_trip_command))

    # callback for inline buttons
    application.add_handler(CallbackQueryHandler(plate_callback))

    # auto listener in groups (optional)
    application.add_handler(MessageHandler(filters.Regex(AUTO_KEYWORD_PATTERN) & filters.ChatType.GROUPS, auto_menu_listener))


# ----- main -----
def ensure_env():
    if not BOT_TOKEN:
        raise RuntimeError("Please set BOT_TOKEN environment variable")
    if not GOOGLE_CREDS_BASE64:
        raise RuntimeError("Please set GOOGLE_CREDS_BASE64 environment variable")


def main():
    ensure_env()
    application = ApplicationBuilder().token(BOT_TOKEN).build()

    # Register async handlers BEFORE starting polling.
    # Some PTB versions don't have Application.run_async, so call the async register
    # function synchronously using asyncio.run()
    try:
        asyncio.run(register_ui_handlers(application))
    except Exception as e:
        # If registration fails, log and raise to make the container crash so you can see error
        logger.exception("Failed to register handlers")
        raise

    # start polling (polling fine for Railway if container runs continuously)
    logger.info("Starting bot polling...")
    application.run_polling()


if __name__ == "__main__":
    main()
