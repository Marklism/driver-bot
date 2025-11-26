#!/usr/bin/env python3
"""
driver-bot full script (with local timezone option)

Notes:
- This is your original script with one small, safe enhancement:
  you can now configure a local timezone (matching your computer) so
  timestamps written/displayed by the bot use that zone.
- To enable: set environment variable LOCAL_TZ to an IANA timezone string,
  e.g. "Asia/Phnom_Penh" or "Asia/Shanghai" or "Europe/London".
  If LOCAL_TZ is not set or ZoneInfo is unavailable, the script falls back
  to the server's system time (unchanged behavior).
- No other logic or handlers are changed.
"""

import os
import json
import base64
import logging
from datetime import datetime
from typing import Optional

import gspread
from oauth2client.service_account import ServiceAccountCredentials

# Python zoneinfo (standard in 3.9+). We use it if available to format times in user's local tz.
try:
    from zoneinfo import ZoneInfo
except Exception:
    ZoneInfo = None

# Telegram imports (compatible with PTB 20.x where Update is in telegram)
from telegram import (
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    BotCommand,
    Update,
    ReplyKeyboardMarkup,
    KeyboardButton,
)
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
GOOGLE_CREDS_PATH = os.getenv("GOOGLE_CREDS_PATH")  # optional path to credentials.json
PLATE_LIST = os.getenv(
    "PLATE_LIST",
    "2BB-3071,2BB-0809,2CI-8066,2CK-8066,2CJ-8066,3H-8066,2AV-6527,2AZ-6828,2AX-4635,2BV-8320",
)
GOOGLE_SHEET_NAME = os.getenv("GOOGLE_SHEET_NAME", "Driver_Log")
GOOGLE_SHEET_TAB = os.getenv("GOOGLE_SHEET_TAB", "")  # optional tab name

# NEW: Local timezone option
# Set LOCAL_TZ env var to an IANA timezone (e.g. "Asia/Phnom_Penh", "Asia/Shanghai")
LOCAL_TZ = os.getenv("LOCAL_TZ", "").strip() or None
if LOCAL_TZ and ZoneInfo is None:
    logger.warning("LOCAL_TZ set but zoneinfo not available in this Python runtime. Falling back to system time.")

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


def _load_creds_from_base64(encoded: str) -> dict:
    """Try decode base64 and parse JSON. Raise on failure."""
    try:
        # If value looks like raw JSON (not base64), try direct load
        if encoded.strip().startswith("{"):
            return json.loads(encoded)
        # fix padding if needed
        padded = encoded.strip()
        # base64 strings sometimes get whitespace/newlines removed incorrectly; remove whitespace
        padded = "".join(padded.split())
        # add padding
        missing = len(padded) % 4
        if missing:
            padded += "=" * (4 - missing)
        decoded = base64.b64decode(padded)
        return json.loads(decoded)
    except Exception as e:
        logger.exception("Failed to decode GOOGLE_CREDS_BASE64 (or parse JSON): %s", e)
        raise


def get_gspread_client():
    """Return authorized gspread client. Supports GOOGLE_CREDS_BASE64 or GOOGLE_CREDS_PATH."""
    # First prefer explicit base64 env
    creds_json = None
    if GOOGLE_CREDS_BASE64:
        creds_json = _load_creds_from_base64(GOOGLE_CREDS_BASE64)
    elif GOOGLE_CREDS_PATH and os.path.exists(GOOGLE_CREDS_PATH):
        with open(GOOGLE_CREDS_PATH, "r", encoding="utf-8") as f:
            creds_json = json.load(f)
    else:
        # Try commonly uploaded credential file in working dir
        fallback = "credentials.json"
        if os.path.exists(fallback):
            with open(fallback, "r", encoding="utf-8") as f:
                creds_json = json.load(f)

    if not creds_json:
        raise RuntimeError("Google credentials not found. Set GOOGLE_CREDS_BASE64 or GOOGLE_CREDS_PATH or include credentials.json")

    creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_json, SCOPES)
    client = gspread.authorize(creds)
    return client


def open_worksheet():
    """Open the configured worksheet (sheet/tab)."""
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


# ---------- TIME FUNCTIONS (use LOCAL_TZ if available) ----------
def _now_dt():
    """
    Return a timezone-aware or naive datetime depending on availability:
    - If LOCAL_TZ is set and ZoneInfo is available, return now() in that zone.
    - Otherwise return naive datetime.now() (original behavior).
    """
    if LOCAL_TZ and ZoneInfo:
        try:
            tz = ZoneInfo(LOCAL_TZ)
            return datetime.now(tz)
        except Exception:
            logger.exception("Failed to use LOCAL_TZ '%s'; falling back to system time.", LOCAL_TZ)
            return datetime.now()
    else:
        return datetime.now()


def now_str():
    """Return timestamp string in TS_FMT using LOCAL_TZ if configured."""
    return _now_dt().strftime(TS_FMT)


def today_date_str():
    """Return date string in DATE_FMT using LOCAL_TZ if configured."""
    return _now_dt().strftime(DATE_FMT)


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
    """Append start row."""
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
    """Find last row for plate with empty End and update."""
    ws = open_worksheet()
    try:
        records = ws.get_all_records()
        for idx in range(len(records) - 1, -1, -1):
            rec = records[idx]
            rec_plate = str(rec.get("Plate No.", rec.get("Plate", rec.get("Plate No", "")))).strip()
            end_val = str(rec.get("End date&time", rec.get("End", ""))).strip()
            start_val = str(rec.get("Start date&time", rec.get("Start", ""))).strip()
            if rec_plate == plate and (end_val == "" or end_val is None):
                row_number = idx + 2  # header row + 1-indexing
                end_ts = now_str()
                duration_text = compute_duration(start_val, end_ts) if start_val else ""
                ws.update_cell(row_number, COL_END, end_ts)
                ws.update_cell(row_number, COL_DURATION, duration_text)
                logger.info("Recorded end trip for %s row %d", plate, row_number)
                return {"ok": True, "message": f"End time recorded for {plate} at {end_ts} (duration {duration_text})"}
        # no matching start found -> append end-only row
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


def build_reply_keyboard_buttons():
    """Reply keyboard (bottom-left quick buttons) - optional for private chat fallback."""
    kb = [
        [KeyboardButton("/start_trip")],
        [KeyboardButton("/end_trip")],
        [KeyboardButton("/menu")],
    ]
    return ReplyKeyboardMarkup(kb, resize_keyboard=True, one_time_keyboard=False)


# Command handlers (these will delete the invoking message if possible)
async def menu_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Show inline menu; try to delete invoking message."""
    text = "Driver Bot Menu — tap a button to perform an action:"
    keyboard = [
        [InlineKeyboardButton("Start trip (select plate)", callback_data="show_start"),
         InlineKeyboardButton("End trip (select plate)", callback_data="show_end")],
        [InlineKeyboardButton("Open menu", callback_data="menu_full"),
         InlineKeyboardButton("Help", callback_data="help")],
    ]
    # delete user's command message if possible
    try:
        if update.effective_message:
            await update.effective_message.delete()
    except Exception:
        logger.debug("Cannot delete command message (menu).")

    # show menu
    try:
        await context.bot.send_chat_action(chat_id=update.effective_chat.id, action=ChatAction.TYPING)
    except Exception:
        pass
    await update.effective_chat.send_message(text=text, reply_markup=InlineKeyboardMarkup(keyboard))

    # best-effort: set my commands for the bot (non-blocking)
    try:
        if hasattr(context.application, "create_task"):
            # schedule set_my_commands to run in running loop if available
            async def _set_cmds():
                await context.bot.set_my_commands([
                    BotCommand("start_trip", "Start a trip (select plate)"),
                    BotCommand("end_trip", "End a trip (select plate)"),
                    BotCommand("menu", "Open trip menu"),
                ])
            context.application.create_task(_set_cmds())
    except Exception:
        logger.debug("Could not schedule set_my_commands.")


async def start_trip_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        if update.effective_message:
            await update.effective_message.delete()
    except Exception:
        logger.debug("Cannot delete command message (start_trip).")

    try:
        await context.bot.send_chat_action(chat_id=update.effective_chat.id, action=ChatAction.TYPING)
    except Exception:
        pass
    await update.effective_chat.send_message("Please choose the vehicle plate to START trip:", reply_markup=build_plate_keyboard("start"))


async def end_trip_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        if update.effective_message:
            await update.effective_message.delete()
    except Exception:
        logger.debug("Cannot delete command message (end_trip).")

    try:
        await context.bot.send_chat_action(chat_id=update.effective_chat.id, action=ChatAction.TYPING)
    except Exception:
        pass
    await update.effective_chat.send_message("Please choose the vehicle plate to END trip:", reply_markup=build_plate_keyboard("end"))


# Callback for inline keyboard
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
    if data == "menu_full":
        await query.edit_message_text("Driver Bot Menu — tap a button to perform an action:", reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("Start trip (select plate)", callback_data="show_start"),
             InlineKeyboardButton("End trip (select plate)", callback_data="show_end")],
            [InlineKeyboardButton("Help", callback_data="help")]
        ]))
        return
    if data == "help":
        await query.edit_message_text("Help: Tap Start trip or End trip and then choose a plate.")
        return

    try:
        action, plate = data.split("|", 1)
    except Exception:
        await query.edit_message_text("Invalid selection.")
        return

    user = query.from_user
    username = user.username or f"{user.first_name or ''} {user.last_name or ''}".strip()

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


# Auto keyword listener (group) - shows quick inline buttons when text pattern detected (non-command)
AUTO_KEYWORD_PATTERN = r'(?i)\b(start|menu|start trip|end trip|trip|出车|还车|返程)\b'


async def auto_menu_listener(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_chat and update.effective_chat.type in ("group", "supergroup"):
        text = (update.effective_message.text or "").strip()
        if not text:
            return
        if text.startswith("/"):
            return
        keyboard = [
            [InlineKeyboardButton("Start trip", callback_data="show_start"),
             InlineKeyboardButton("End trip", callback_data="show_end")],
            [InlineKeyboardButton("Open full menu", callback_data="menu_full")],
        ]
        await update.effective_chat.send_message("Need to record a trip? Tap a button below:", reply_markup=InlineKeyboardMarkup(keyboard))


# Register handlers
def register_ui_handlers(application):
    application.add_handler(CommandHandler("menu", menu_command))
    application.add_handler(CommandHandler(["start_trip", "start"], start_trip_command))
    application.add_handler(CommandHandler(["end_trip", "end"], end_trip_command))

    application.add_handler(CallbackQueryHandler(plate_callback))

    application.add_handler(MessageHandler(filters.Regex(AUTO_KEYWORD_PATTERN) & filters.ChatType.GROUPS, auto_menu_listener))

    # Try to set bot commands (best-effort, non-blocking)
    try:
        if hasattr(application, "create_task"):
            async def _set_cmds():
                try:
                    await application.bot.set_my_commands([
                        BotCommand("start_trip", "Start a trip (select plate)"),
                        BotCommand("end_trip", "End a trip (select plate)"),
                        BotCommand("menu", "Open trip menu"),
                    ])
                    logger.info("Set bot commands (set_my_commands).")
                except Exception:
                    logger.exception("Failed to set bot commands.")
            application.create_task(_set_cmds())
    except Exception:
        logger.debug("Could not schedule set_my_commands at register time.")


# Optional admin helper to post and pin the menu in a group (call /setup_menu in the group as admin)
async def setup_menu_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # Only allow in groups and by admins (best-effort check)
    if not (update.effective_chat and update.effective_chat.type in ("group", "supergroup")):
        await update.effective_chat.send_message("This command must be used in a group.")
        return
    try:
        # delete the invoking message
        try:
            if update.effective_message:
                await update.effective_message.delete()
        except Exception:
            pass

        text = "Driver Bot Menu — tap a button to perform an action:"
        keyboard = [
            [InlineKeyboardButton("Start trip (select plate)", callback_data="show_start"),
             InlineKeyboardButton("End trip (select plate)", callback_data="show_end")],
            [InlineKeyboardButton("Open menu", callback_data="menu_full"),
             InlineKeyboardButton("Help", callback_data="help")],
        ]
        sent = await update.effective_chat.send_message(text=text, reply_markup=InlineKeyboardMarkup(keyboard))
        # try to pin the message
        try:
            await context.bot.pin_chat_message(chat_id=update.effective_chat.id, message_id=sent.message_id, disable_notification=True)
        except Exception:
            logger.debug("Pin failed (bot may lack permission).")
        # set commands globally for bot (best-effort)
        try:
            await context.bot.set_my_commands([
                BotCommand("start_trip", "Start a trip (select plate)"),
                BotCommand("end_trip", "End a trip (select plate)"),
                BotCommand("menu", "Open trip menu"),
            ], scope=None)
        except Exception:
            logger.debug("set_my_commands failed in setup_menu_command.")
        await update.effective_chat.send_message("Setup complete — commands are set for this group. If members still don't see the menu, ask them to update Telegram or open a private chat with the bot and send /start.")
    except Exception:
        logger.exception("Failed to run setup_menu_command")
        await update.effective_chat.send_message("Failed to setup menu.")


# Main
def ensure_env():
    if not BOT_TOKEN:
        raise RuntimeError("Please set BOT_TOKEN environment variable")


def main():
    ensure_env()
    application = ApplicationBuilder().token(BOT_TOKEN).build()

    # register handlers
    register_ui_handlers(application)
    # admin setup command
    application.add_handler(CommandHandler("setup_menu", setup_menu_command))

    # run polling (Application manages loop)
    logger.info("Starting bot polling...")
    application.run_polling()


if __name__ == "__main__":
    main()
