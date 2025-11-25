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

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, BotCommand, ChatAction, Update
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
PLATE_LIST = os.getenv("PLATE_LIST", "2BB-3071,2BB-0809,2CI-8066,2CK-8066,2CJ-8066,3H-8066,2AV-6527,2AZ-6828,2AX-4635,2BV-8320")
GOOGLE_SHEET_NAME = os.getenv("GOOGLE_SHEET_NAME", "Driver_Log")
GOOGLE_SHEET_TAB = os.getenv("GOOGLE_SHEET_TAB", "")

PLATES = [p.strip() for p in PLATE_LIST.split(",") if p.strip()]

# Column mapping
COL_DATE = 1
COL_DRIVER = 2
COL_PLATE = 3
COL_START = 4
COL_END = 5
COL_DURATION = 6

TS_FMT = "%Y-%m-%d %H:%M:%S"
DATE_FMT = "%Y-%m-%d"

SCOPES = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]

def get_gspread_client():
    if not GOOGLE_CREDS_BASE64:
        raise RuntimeError("GOOGLE_CREDS_BASE64 environment variable is missing")
    try:
        decoded = base64.b64decode(GOOGLE_CREDS_BASE64)
        creds_json = json.loads(decoded)
    except Exception:
        logger.exception("Failed to decode GOOGLE_CREDS_BASE64")
        raise

    creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_json, SCOPES)
    return gspread.authorize(creds)

def open_worksheet():
    gc = get_gspread_client()
    sh = gc.open(GOOGLE_SHEET_NAME)
    if GOOGLE_SHEET_TAB:
        try:
            return sh.worksheet(GOOGLE_SHEET_TAB)
        except Exception:
            return sh.sheet1
    return sh.sheet1

def now_str(): return datetime.now().strftime(TS_FMT)
def today_date_str(): return datetime.now().strftime(DATE_FMT)

def compute_duration(start_ts, end_ts):
    try:
        s = datetime.strptime(start_ts, TS_FMT)
        e = datetime.strptime(end_ts, TS_FMT)
        mins = int((e - s).total_seconds() // 60)
        return f"{mins//60}h{mins%60}m"
    except:
        return ""

def record_start_trip(driver, plate):
    ws = open_worksheet()
    start_ts = now_str()
    row = [today_date_str(), driver, plate, start_ts, "", ""]
    try:
        ws.append_row(row, value_input_option="USER_ENTERED")
        return {"ok": True, "message": f"Start time recorded for {plate} at {start_ts}"}
    except Exception as e:
        return {"ok": False, "message": str(e)}

def record_end_trip(driver, plate):
    ws = open_worksheet()
    try:
        records = ws.get_all_records()
        # find last row matching plate with empty end time
        for idx in range(len(records)-1, -1, -1):
            rec = records[idx]
            rec_plate = str(rec.get("Plate No.", rec.get("Plate", ""))).strip()
            end_val = str(rec.get("End date&time", "")).strip()
            start_val = str(rec.get("Start date&time", "")).strip()

            if rec_plate == plate and end_val == "":
                row_num = idx + 2
                end_ts = now_str()
                duration = compute_duration(start_val, end_ts)
                ws.update_cell(row_num, COL_END, end_ts)
                ws.update_cell(row_num, COL_DURATION, duration)
                return {"ok": True, "message": f"End recorded at {end_ts} (duration {duration})"}

        # no open start → append fallback row
        end_ts = now_str()
        row = [today_date_str(), driver, plate, "", end_ts, ""]
        ws.append_row(row, value_input_option="USER_ENTERED")
        return {"ok": True, "message": f"End recorded at {end_ts} (no matching start)"}

    except Exception as e:
        return {"ok": False, "message": str(e)}

# -------- Telegram interface ----------

def build_plate_keyboard(prefix):
    rows, row = [], []
    for i, plate in enumerate(PLATES, 1):
        row.append(InlineKeyboardButton(plate, callback_data=f"{prefix}|{plate}"))
        if i % 3 == 0:
            rows.append(row); row = []
    if row: rows.append(row)
    return InlineKeyboardMarkup(rows)

async def menu_command(update, ctx):
    txt = "Choose an action:\n• Start trip\n• End trip"
    kb = [
        [InlineKeyboardButton("Start trip", callback_data="show_start")],
        [InlineKeyboardButton("End trip", callback_data="show_end")],
    ]
    await update.effective_chat.send_message(txt, reply_markup=InlineKeyboardMarkup(kb))

async def start_trip_command(update, ctx):
    await update.effective_chat.send_message("Choose vehicle plate to START trip:", reply_markup=build_plate_keyboard("start"))

async def end_trip_command(update, ctx):
    await update.effective_chat.send_message("Choose vehicle plate to END trip:", reply_markup=build_plate_keyboard("end"))

async def plate_callback(update, ctx):
    q = update.callback_query
    await q.answer()
    data = q.data

    if data == "show_start":
        await q.edit_message_text("Choose plate to START trip:", reply_markup=build_plate_keyboard("start"))
        return
    if data == "show_end":
        await q.edit_message_text("Choose plate to END trip:", reply_markup=build_plate_keyboard("end"))
        return

    try:
        action, plate = data.split("|", 1)
    except:
        await q.edit_message_text("Invalid selection.")
        return

    user = q.from_user
    driver = user.username or (user.first_name or "") + " " + (user.last_name or "")

    if action == "start":
        res = record_start_trip(driver, plate)
    else:
        res = record_end_trip(driver, plate)

    msg = "✅ " + res["message"] if res["ok"] else "❌ Failed: " + res["message"]
    await q.edit_message_text(msg)

AUTO_KEYWORD_PATTERN = r"(?i)\b(start|menu|start trip|end trip|trip|出车|还车|返程)\b"

async def auto_menu_listener(update, ctx):
    chat = update.effective_chat
    if chat.type not in ("group", "supergroup"):
        return
    txt = update.effective_message.text or ""
    if not txt or txt.startswith("/"):
        return
    kb = [
        [InlineKeyboardButton("Start trip", callback_data="show_start"),
         InlineKeyboardButton("End trip", callback_data="show_end")],
        [InlineKeyboardButton("Open full menu", callback_data="show_start")],
    ]
    await chat.send_message("Need to record a trip?", reply_markup=InlineKeyboardMarkup(kb))

async def register_handlers(app):
    await app.bot.set_my_commands([
        BotCommand("start_trip", "Start a trip"),
        BotCommand("end_trip", "End a trip"),
        BotCommand("menu", "Open menu"),
    ])

    app.add_handler(CommandHandler("menu", menu_command))
    app.add_handler(CommandHandler("start_trip", start_trip_command))
    app.add_handler(CommandHandler("end_trip", end_trip_command))
    app.add_handler(CallbackQueryHandler(plate_callback))
    app.add_handler(MessageHandler(filters.Regex(AUTO_KEYWORD_PATTERN) & filters.ChatType.GROUPS, auto_menu_listener))

def ensure_env():
    if not BOT_TOKEN:
        raise RuntimeError("BOT_TOKEN not set")
    if not GOOGLE_CREDS_BASE64:
        raise RuntimeError("GOOGLE_CREDS_BASE64 not set")

def main():
    ensure_env()
    app = ApplicationBuilder().token(BOT_TOKEN).build()
    app.run_async(register_handlers(app))
    app.run_polling()

if __name__ == "__main__":
    main()
