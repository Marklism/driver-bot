#!/usr/bin/env python3
"""
Enhanced driver-bot (group UI + pinned menu + Google Sheet paired records)

Save this file as bot.py (or the name you use) and deploy.
Environment variables required:
- BOT_TOKEN: Telegram bot token
- GOOGLE_CREDS_BASE64: base64-encoded service account JSON (no newlines, or it will be cleaned)
Optional:
- PLATE_LIST: comma-separated plates (default provided)
- GOOGLE_SHEET_NAME: default "Driver_Log"
- GOOGLE_SHEET_TAB: optional worksheet/tab name
"""
import os
import json
import base64
import logging
import re
import time
import requests
from datetime import datetime

import gspread
from oauth2client.service_account import ServiceAccountCredentials

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, BotCommand, Update
from telegram.constants import ChatAction
from telegram.ext import (
    ApplicationBuilder,
    CommandHandler,
    CallbackQueryHandler,
    MessageHandler,
    ChatMemberHandler,
    filters,
    ContextTypes,
)

# ----------------- Configuration & Logging -----------------
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("driver-bot")

BOT_TOKEN = os.getenv("BOT_TOKEN")
GOOGLE_CREDS_BASE64 = os.getenv("GOOGLE_CREDS_BASE64")
PLATE_LIST = os.getenv(
    "PLATE_LIST",
    "2BB-3071,2BB-0809,2CI-8066,2CK-8066,2CJ-8066,3H-8066,2AV-6527,2AZ-6828,2AX-4635,2BV-8320",
)
GOOGLE_SHEET_NAME = os.getenv("GOOGLE_SHEET_NAME", "Driver_Log")
GOOGLE_SHEET_TAB = os.getenv("GOOGLE_SHEET_TAB", "")  # optional

PLATES = [p.strip() for p in PLATE_LIST.split(",") if p.strip()]

# Column mapping (1-indexed)
COL_DATE = 1
COL_DRIVER = 2
COL_PLATE = 3
COL_START = 4
COL_END = 5
COL_DURATION = 6

TS_FMT = "%Y-%m-%d %H:%M:%S"
DATE_FMT = "%Y-%m-%d"

SCOPES = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]

# Auto keyword pattern (triggers in group messages)
AUTO_KEYWORD_PATTERN = r'(?i)\b(start|menu|start trip|end trip|trip|出车|还车|返程)\b'

# In-memory rate-limiter for auto menu postings (per-process)
LAST_POST_BY_CHAT = {}  # chat_id -> timestamp (seconds)
AUTO_POST_COOLDOWN = int(os.getenv("AUTO_POST_COOLDOWN", "60"))  # seconds

# ----------------- Google Sheets helpers -----------------
def get_gspread_client():
    if not GOOGLE_CREDS_BASE64:
        raise RuntimeError("GOOGLE_CREDS_BASE64 environment variable is missing")
    try:
        # normalize: remove whitespace/newlines and pad
        s = "".join(GOOGLE_CREDS_BASE64.split())
        s += "=" * ((4 - len(s) % 4) % 4)
        decoded = base64.b64decode(s)
        creds_json = json.loads(decoded)
    except Exception as e:
        logger.exception("Failed to decode GOOGLE_CREDS_BASE64: %s", e)
        raise

    creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_json, SCOPES)
    client = gspread.authorize(creds)
    return client

def open_worksheet():
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
    ws = open_worksheet()
    start_ts = now_str()
    row = [today_date_str(), driver, plate, start_ts, "", ""]
    try:
        ws.append_row(row, value_input_option="USER_ENTERED")
        logger.info("Recorded start trip: %s %s", driver, plate)
        return {"ok": True, "message": f"Start time recorded for {plate} at {start_ts}"}
    except Exception as e:
        logger.exception("Failed to append start trip row: %s", e)
        return {"ok": False, "message": "Failed to write start trip to sheet: " + str(e)}

def record_end_trip(driver: str, plate: str) -> dict:
    ws = open_worksheet()
    try:
        records = ws.get_all_records()
        for idx in range(len(records) - 1, -1, -1):
            rec = records[idx]
            rec_plate = str(rec.get("Plate No.", rec.get("Plate", rec.get("Plate No", "")))).strip()
            end_val = str(rec.get("End date&time", rec.get("End", ""))).strip()
            start_val = str(rec.get("Start date&time", rec.get("Start", ""))).strip()
            if rec_plate == plate and (end_val == "" or end_val is None):
                row_number = idx + 2
                end_ts = now_str()
                duration_text = compute_duration(start_val, end_ts) if start_val else ""
                ws.update_cell(row_number, COL_END, end_ts)
                ws.update_cell(row_number, COL_DURATION, duration_text)
                logger.info("Recorded end trip for %s row %d", plate, row_number)
                return {"ok": True, "message": f"End time recorded for {plate} at {end_ts} (duration {duration_text})"}
        end_ts = now_str()
        row = [today_date_str(), driver, plate, "", end_ts, ""]
        ws.append_row(row, value_input_option="USER_ENTERED")
        logger.info("No open start found; appended end-only row for %s", plate)
        return {"ok": True, "message": f"End time recorded (no matching start found) for {plate} at {end_ts}"}
    except Exception as e:
        logger.exception("Failed to update end trip: %s", e)
        return {"ok": False, "message": "Failed to write end trip to sheet: " + str(e)}

# ----------------- Telegram UI helpers -----------------
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

def build_command_menu():
    keyboard = [
        [
            InlineKeyboardButton("Start trip (select plate)", callback_data="cmd|start_trip"),
            InlineKeyboardButton("End trip (select plate)", callback_data="cmd|end_trip"),
        ],
        [
            InlineKeyboardButton("Open menu", callback_data="cmd|menu"),
            InlineKeyboardButton("Help", callback_data="cmd|help"),
        ],
    ]
    return InlineKeyboardMarkup(keyboard)

async def setup_menu_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat = update.effective_chat
    user = update.effective_user
    if chat.type not in ("group", "supergroup"):
        await update.effective_chat.send_message("This command only works in groups.")
        return
    try:
        member = await context.bot.get_chat_member(chat.id, user.id)
        if member.status not in ("administrator", "creator"):
            await update.effective_chat.send_message("Only group admins can run /setup_menu.")
            return
    except Exception:
        pass

    msg = await update.effective_chat.send_message(
        "Driver Bot Menu — tap a button to perform an action:",
        reply_markup=build_command_menu()
    )
    try:
        await context.bot.pin_chat_message(chat_id=chat.id, message_id=msg.message_id, disable_notification=True)
    except Exception:
        await update.effective_chat.send_message("Menu posted. (Bot couldn't pin — give bot pin/manage message rights to auto-pin.)")

async def menu_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = "Choose an action or select a plate below:\n\n• Start trip — record departure\n• End trip — record return"
    keyboard = [
        [InlineKeyboardButton("Start trip", callback_data="show_start")],
        [InlineKeyboardButton("End trip", callback_data="show_end")],
    ]
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

# Unified handling of plate actions
async def handle_plate_action(action: str, plate: str, query, context: ContextTypes.DEFAULT_TYPE):
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
        await query.edit_message_text("Unknown plate action.")

# Callback query router
async def callback_router(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    data = query.data or ""

    if data.startswith("cmd|"):
        cmd = data.split("|", 1)[1]
        if cmd == "start_trip":
            await query.edit_message_text("Please choose the vehicle plate to START trip:", reply_markup=build_plate_keyboard("start"))
            return
        if cmd == "end_trip":
            await query.edit_message_text("Please choose the vehicle plate to END trip:", reply_markup=build_plate_keyboard("end"))
            return
        if cmd == "menu":
            await query.edit_message_text("Choose an action or select a plate below:", reply_markup=build_command_menu())
            return
        if cmd == "help":
            await query.edit_message_text("Help: use the buttons to Start/End trips. Admins: /setup_menu to pin this panel.")
            return

    if data in ("show_start", "show_end"):
        if data == "show_start":
            await query.edit_message_text("Please choose the vehicle plate to START trip:", reply_markup=build_plate_keyboard("start"))
        else:
            await query.edit_message_text("Please choose the vehicle plate to END trip:", reply_markup=build_plate_keyboard("end"))
        return

    if "|" in data:
        prefix, rest = data.split("|", 1)
        if prefix in ("start", "end"):
            await handle_plate_action(prefix, rest, query, context)
            return

    await query.edit_message_text("Unknown action.")

# Auto keyword listener (group) with rate limiting and bot-check
async def auto_menu_listener(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.effective_chat or update.effective_chat.type not in ("group", "supergroup"):
        return
    text = (update.effective_message.text or "").strip()
    if not text:
        return
    if update.effective_user and update.effective_user.is_bot:
        return
    now_ts = int(time.time())
    last = LAST_POST_BY_CHAT.get(update.effective_chat.id, 0)
    if now_ts - last < AUTO_POST_COOLDOWN:
        return
    LAST_POST_BY_CHAT[update.effective_chat.id] = now_ts
    keyboard = [
        [InlineKeyboardButton("Start trip", callback_data="show_start"), InlineKeyboardButton("End trip", callback_data="show_end")],
        [InlineKeyboardButton("Open full menu", callback_data="menu_full")],
    ]
    try:
        await update.effective_chat.send_message("Need to record a trip? Tap a button below:", reply_markup=InlineKeyboardMarkup(keyboard))
    except Exception:
        logger.exception("Failed to send auto menu")

# Bot's own chat member updates (trigger when bot is added/promoted) -> post & pin menu
async def my_chat_member_update(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        chat = update.effective_chat
        new_status = update.my_chat_member.new_chat_member.status
    except Exception:
        return
    if not chat or chat.type not in ("group", "supergroup"):
        return
    if new_status in ("administrator", "member", "creator"):
        try:
            msg = await context.bot.send_message(chat_id=chat.id, text="Driver Bot Menu — tap a button to perform an action:", reply_markup=build_command_menu())
            try:
                await context.bot.pin_chat_message(chat_id=chat.id, message_id=msg.message_id, disable_notification=True)
            except Exception:
                logger.info("Bot cannot pin in chat %s — maybe no permission", chat.id)
        except Exception:
            logger.exception("Failed to auto-post menu on chat member update")

# Register handlers
def register_ui_handlers(application):
    application.add_handler(CommandHandler("menu", menu_command))
    application.add_handler(CommandHandler("start_trip", start_trip_command))
    application.add_handler(CommandHandler("end_trip", end_trip_command))
    application.add_handler(CommandHandler("setup_menu", setup_menu_command))
    application.add_handler(CallbackQueryHandler(callback_router))
    # Chat member handler: triggers when bot's status changes
    application.add_handler(ChatMemberHandler(my_chat_member_update, ChatMemberHandler.MY_CHAT_MEMBER))
    # Better filter for auto-menu
    try:
        application.add_handler(
            MessageHandler(
                (filters.TEXT & ~filters.COMMAND & filters.Regex(AUTO_KEYWORD_PATTERN, flags=re.IGNORECASE))
                & filters.ChatType.GROUPS,
                auto_menu_listener
            )
        )
    except Exception:
        # fallback: register simpler regex-only handler
        application.add_handler(MessageHandler(filters.Regex(AUTO_KEYWORD_PATTERN) & filters.ChatType.GROUPS, auto_menu_listener))

    # Best-effort set_my_commands (non-blocking)
    try:
        res = application.bot.set_my_commands([
            BotCommand("start_trip", "Start a trip (select plate)"),
            BotCommand("end_trip", "End a trip (select plate)"),
            BotCommand("menu", "Open trip menu"),
            BotCommand("setup_menu", "Post & pin the command menu (admin only)"),
        ])
        # If this returns a coroutine in your PTB version it may not execute here; it's fine.
    except Exception:
        logger.exception("Failed to set_my_commands (non-fatal)")

# ----------------- Main -----------------
def ensure_env():
    if not BOT_TOKEN:
        raise RuntimeError("Please set BOT_TOKEN environment variable")
    if not GOOGLE_CREDS_BASE64:
        raise RuntimeError("Please set GOOGLE_CREDS_BASE64 environment variable")

def main():
    ensure_env()
    application = ApplicationBuilder().token(BOT_TOKEN).build()
    register_ui_handlers(application)

    # Ensure no webhook active to avoid getUpdates conflict
    try:
        resp = requests.post(f"https://api.telegram.org/bot{BOT_TOKEN}/deleteWebhook")
        logger.info("deleteWebhook response: %s", resp.text)
    except Exception:
        logger.exception("Failed to call deleteWebhook (non-fatal)")

    logger.info("Starting bot polling...")
    application.run_polling()

if __name__ == "__main__":
    main()
