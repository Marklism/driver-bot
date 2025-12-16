
# === AUTO-PATCH: Missing symbols & runtime guards (V9 Frozen) ===
from datetime import time as dtime

def count_trips_for_day(username, nowdt):
    # Safe fallback: return 0 if detailed stats unavailable
    try:
        ws = open_worksheet(RECORDS_TAB)
        rows = ws.get_all_values()
        if not rows or len(rows) < 2:
            return 0
        date_str = nowdt.strftime("%Y-%m-%d")
        cnt = 0
        for r in rows[1:]:
            try:
                if r[1] == username and r[0] == date_str:
                    cnt += 1
            except Exception:
                pass
        return cnt
    except Exception:
        return 0

# Guard undefined logger
try:
    logger
except NameError:
    import logging
    logger = logging.getLogger("driver-bot")

# === END AUTO-PATCH ===

from datetime import datetime, timedelta, time as time


def determine_ot_rate(dt: datetime, is_holiday: bool = False) -> str:
    """
    OT rate rules:
    - Holiday: always 200%
    - Friday:
        before 23:59:59 -> 150%
        from Saturday 00:00 -> 200%
    - Saturday/Sunday: 200%
    - Weekday: 150%
    """
    if is_holiday:
        return "200%"

    weekday = dt.weekday()  # Mon=0 ... Sun=6

    # Saturday or Sunday
    if weekday >= 5:
        return "200%"

    # Friday special cut-off
    if weekday == 4:  # Friday
        if dt.time() <= time(23, 59, 59):
            return "150%"
        return "200%"

    return "150%"


import io
# === /ot_report rewritten to DRIVER BUTTON MODE ===
# Old parameter-based logic removed
# New flow: /ot_report -> private driver selection -> callback generates CSV
# === VERSION A: DRIVER BUTTON REPORTS & CSV SPECS APPLIED ===
# ===============================
# DRIVER BOT — LTS FROZEN VERSION
# ===============================
# This is the LONG-TERM SUPPORT version.
# - Behavior identical to current running bot
# - No __future__ imports
# - Structure frozen for stability
#
# Allowed edits (ONLY):
#   - HOLIDAYS
#   - VEHICLE_PLATES
#   - Language text dictionaries
#   - ADMIN / permission lists
#
# Do NOT modify logic below.
# ===== FIXED ORDER: OT REPORT DRIVER BUTTON =====
from telegram import InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import CallbackQueryHandler

from telegram import InlineKeyboardButton, InlineKeyboardMarkup

async def reply_private(update, context, text, reply_markup=None):
    await context.bot.send_message(
        chat_id=update.effective_user.id,
        text=text,
        reply_markup=reply_markup,
    )

async def ot_report_entry(update, context):
    driver_map = get_driver_map()   # ✅ 正确的数据入口
    drivers = sorted(driver_map.keys())

    if not drivers:
        await reply_private(update, context, "❌ No drivers found.")
        return

    keyboard = []
    for d in drivers:
        keyboard.append([
            InlineKeyboardButton(
                d,
                callback_data=f"OTR_DRIVER:{d}"
            )
        ])

    await reply_private(
        update,
        context,
        "Select driver:",
        reply_markup=InlineKeyboardMarkup(keyboard)
    )
# ===== END helper =====


    if not drivers:
        await reply_private(update, context, "No drivers found.")
        return

    keyboard = [
        [InlineKeyboardButton(d, callback_data=f"OTR_DRIVER:{d}")]
        for d in drivers
    ]
    await reply_private(
        update,
        context,
        "Select driver:",
        reply_markup=InlineKeyboardMarkup(keyboard),
    )

def _calc_hours_fallback(r, idx_morning, idx_evening, idx_start, idx_end):
    try:
        m = float(r[idx_morning] or 0)
        ev = float(r[idx_evening] or 0)
        if m + ev > 0:
            return round(m + ev, 2)
        s = datetime.fromisoformat(r[idx_start])
        e2 = datetime.fromisoformat(r[idx_end])
        return round((e2 - s).total_seconds() / 3600, 2)
    except Exception:
        return 0

async def ot_report_driver_callback(update, context):
    query = update.callback_query
    await query.answer()

    driver = query.data.split(":", 1)[1]

    ws = open_worksheet("OT Record")
    rows = ws.get_all_values()

    if len(rows) < 2:
        await context.bot.send_message(
            chat_id=query.from_user.id,
            text="❌ OT Record is empty."
        )
        return

    header = rows[0]
    data = rows[1:]

    idx_name = header.index("Name")
    idx_type = header.index("Type")
    idx_start = header.index("Start Date")
    idx_end = header.index("End Date")
    idx_morning = header.index("Morning OT")
    idx_evening = header.index("Evening OT")

    ot_150, ot_200 = [], []

    for r in data:
        if r[idx_name].strip() != driver:
            continue

        typ = r[idx_type].strip()

        # ===== 核心修复点 =====
        try:
            m = float(r[idx_morning] or 0)
            e = float(r[idx_evening] or 0)
            hours = m + e

            if hours == 0:
                s = datetime.fromisoformat(r[idx_start])
                en = datetime.fromisoformat(r[idx_end])
                hours = round((en - s).total_seconds() / 3600, 2)
        except Exception as ex:
            # 直接跳过坏行，但不炸整个 driver
            continue

        row = [r[idx_start], r[idx_end], f"{hours:.2f}"]

        if typ == "150%":
            ot_150.append(row)
        elif typ == "200%":
            ot_200.append(row)

    if not ot_150 and not ot_200:
        await context.bot.send_message(
            chat_id=query.from_user.id,
            text=f"❌ No OT data for {driver}"
        )
        return

    import io, csv
    output = io.StringIO()
    writer = csv.writer(output)

    writer.writerow(["Driver", driver])
    writer.writerow([])

    if ot_150:
        writer.writerow(["150% OT"])
        writer.writerow(["Start", "End", "Hours"])
        total = 0
        for r in ot_150:
            writer.writerow(r)
            total += float(r[2])
        writer.writerow(["TOTAL", "", f"{total:.2f}"])
        writer.writerow([])

    if ot_200:
        writer.writerow(["200% OT"])
        writer.writerow(["Start", "End", "Hours"])
        total = 0
        for r in ot_200:
            writer.writerow(r)
            total += float(r[2])
        writer.writerow(["TOTAL", "", f"{total:.2f}"])

    bio = io.BytesIO(output.getvalue().encode("utf-8"))
    bio.name = f"OT_Report_{driver}.csv"

    await context.bot.send_document(
        chat_id=query.from_user.id,
        document=bio,
        caption=f"OT report for {driver}"
    )


# ===== END FIX =====

# ===============================


from telegram import Update
from telegram.ext import ContextTypes
import os
from telegram import Bot, BotCommand
"""
Merged Driver Bot — usage notes (auto-inserted)


# === BEGIN: Group-silent private reply helper ===
async def reply_privately(update, context, text):
    chat = update.effective_chat
    user = update.effective_user

    # If triggered in group, reply via private chat
    if chat and chat.type in ("group", "supergroup"):
        await context.bot.send_message(chat_id=user.id, text=text)
    else:
        await update.effective_message.reply_text(text)
# === END: Group-silent private reply helper ===
Before running this script, set these environment variables (examples):

BOT_TOKEN — Telegram bot token, e.g. export BOT_TOKEN="123:ABC..."
SHEET_ID — Google Sheets ID, e.g. export SHEET_ID="1aBcD..." (required if using Google Sheets)
GOOGLE_CREDS_B64 — base64 of service-account JSON (export GOOGLE_CREDS_B64="$(base64 -w0 creds.json)") (required if using Google Sheets)

Optional tab names (if you customized them): DRIVERS_TAB, LEAVE_TAB, FINANCE_TAB, DRIVER_OT_TAB, DRIVER_OT_TAB

Notes:
- This file was auto-merged. I tried to avoid changing existing behavior.
- If you hit runtime errors (ImportError, NameError, KeyError), copy the full error text and send it back — I'll repair it.
"""

def check_deployment_requirements():
    """
    Deployment check: prints warnings about missing environment variables and missing optional imports.
    This runs at startup (inside main) to give clearer logs on Railway.
    """
    required_env = ["BOT_TOKEN", "SHEET_ID", "GOOGLE_CREDS_B64"]
    missing = [v for v in required_env if not os.getenv(v)]
    if missing:
        print("=== DEPLOYMENT CHECK WARNING ===")
        print("Missing required environment variables:", missing)
        print("Please set them in your Railway project variables.")
    else:
        print("Deployment check: required env vars present (BOT_TOKEN, SHEET_ID, GOOGLE_CREDS_B64).")
    # Try importing some optional modules to give clearer messages
    optional_checks = ["gspread", "oauth2client", "zoneinfo", "httpx"]
    for mod in optional_checks:
        try:
            __import__(mod)
        except Exception as e:
            print(f"Note: optional module import failed: {mod} -> {e}")
    print("=== DEPLOYMENT CHECK COMPLETE ===")

try:
    from zoneinfo import ZoneInfo
except Exception:
    ZoneInfo = None

from typing import Optional, Dict, List, Any

# --- BEGIN: Inserted OT & Clock functionality (from Bot(包含OT和打卡).txt) ---
# Added OT Table headers
OT_HEADERS = ["Date", "Driver", "Action", "Timestamp", "ClockType", "Note"]

# OT per-shift summary tab for calculated OT
OT_RECORD_TAB = os.getenv("OT_RECORD_TAB", "OT Record")
OT_RECORD_HEADERS = ["Name", "Type", "Start Date", "End Date", "Day", "Morning OT", "Evening OT", "Note"]

# OT holidays configuration: default includes 2025-12-29; extend via OT_HOLIDAYS or HOLIDAYS env vars
OT_HOLIDAYS = {"2025-12-29"}
_env_h = os.getenv("OT_HOLIDAYS") or os.getenv("HOLIDAYS", "")
for _h in _env_h.split(","):
    _h = _h.strip()
    if _h:
        OT_HOLIDAYS.add(_h)

def _is_holiday(dt: datetime) -> bool:
    return dt.strftime("%Y-%m-%d") in OT_HOLIDAYS

# Various column indices
M_IDX_ID = 0
M_IDX_GID = 1
M_IDX_DRIVER = 2
M_IDX_PLATE = 3
M_IDX_DEPART = 4
M_IDX_FROM = 5
M_IDX_TO = 6
M_IDX_START = 7
M_IDX_END = 8
M_IDX_ROUNDTRIP = 9
M_IDX_NOTE = 10

# Leave sheet
L_IDX_DRIVER = 0
L_IDX_TYPE = 1
L_IDX_START = 2
L_IDX_END = 3
L_IDX_STATUS = 4
L_IDX_NOTE = 5

# Finance sheet
F_IDX_DRIVER = 0
F_IDX_CAT = 1
F_IDX_AMOUNT = 2
F_IDX_DATE = 3
F_IDX_NOTE = 4

# OT Clock sheet (new)
O_IDX_DATE = 0
O_IDX_DRIVER = 1
O_IDX_ACTION = 2
O_IDX_TIME = 3
O_IDX_TYPE = 4
O_IDX_NOTE = 5

# ---------------------------------------------------------
#  OT SECTION — Clock In/Out + OT Calculation
# ---------------------------------------------------------

def record_clock_entry(driver: str, action: str, note: str = ""):
    dt = _now_dt()
    ws = open_worksheet(OT_TAB)

    # Ensure headers exist
    try:
        ensure_sheet_headers_match(ws, OT_HEADERS)
    except Exception:
        try:
            logger.exception("Failed to ensure/update OT_TAB headers")
        except Exception:
            pass

    row = [
        dt.strftime("%Y-%m-%d"),
        driver,
        action,
        dt.strftime("%Y-%m-%d %H:%M:%S"),
        "IN" if action == "IN" else "OUT",
        note,
    ]
    ws.append_row(row)
    return row

def get_last_clock_entry(driver: str):
    ws = open_worksheet(OT_TAB)
    vals = ws.get_all_values()
    if len(vals) <= 1:
        return None
    # vals[0] is header
    for row in reversed(vals[1:]):
        if row[O_IDX_DRIVER] == driver:
            return row
    return None

def _is_weekend(dt: datetime) -> bool:
    return dt.weekday() >= 5  # 5=Sat,6=Sun

def compute_ot_for_shift(start_dt: datetime, end_dt: datetime, is_holiday: bool = False):
    """Return total OT hours for one shift, possibly crossing midnight."""
    if end_dt < start_dt:
        end_dt = end_dt + timedelta(days=1)

    total_ot = 0.0

    dt = start_dt
    while dt < end_dt:
        next_day = (dt + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
        segment_end = min(next_day, end_dt)

        seg_is_weekend = _is_weekend(dt)
        seg_is_holiday = is_holiday

        if seg_is_weekend or seg_is_holiday:
            hours = (segment_end - dt).total_seconds() / 3600
            total_ot += hours
        else:
            t7 = dt.replace(hour=7, minute=0, second=0, microsecond=0)
            if dt < t7:
                ot_morning = (min(segment_end, t7) - dt).total_seconds() / 3600
                total_ot += max(ot_morning, 0)

            t18 = dt.replace(hour=18, minute=0, second=0, microsecond=0)
            t1830 = dt.replace(hour=18, minute=30, second=0, microsecond=0)

            if segment_end > t1830:
                ot_evening = (segment_end - t18).total_seconds() / 3600
                total_ot += max(ot_evening, 0)

        dt = segment_end

    return round(total_ot, 2)

async def clock_callback_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Unified Clock In/Out + OT calculation handler.

    Rules (Mon–Fri, non-holiday):
      1) If action=OUT and 00:00 <= ts < 04:00 → OT = ts - 00:00 (same day)  (morning OT)
      2) If action=IN and 04:00 < ts < 07:00 → OT = 08:00 - ts              (morning OT)
      3) If action=IN and ts >= 07:00      → no OT
      4) If action=OUT and ts < 18:30      → no OT
      5) If action=OUT and ts >= 18:30     → OT = ts - 18:00                (evening OT)

    Weekend (Sat 00:00 – Sun 23:59) and holidays:
      6) For a shift (IN → OUT) fully on weekend/holiday → OT = end - start (200%)
         (Clock IN: no message; Clock OUT: record & notify "OT today: X hour(s)")
    """
    query = update.callback_query
    await query.answer()

    user = update.effective_user
    driver = user.username or user.first_name
    chat = query.message.chat if query.message else None

    # previous entry for this driver
    last = get_last_clock_entry(driver)
    now_in = last is None or (len(last) > O_IDX_ACTION and last[O_IDX_ACTION] == "OUT")
    action = "IN" if now_in else "OUT"

    # record raw clock
    rec = record_clock_entry(driver, action)

    # parse timestamp
    try:
        ts_dt = datetime.strptime(rec[O_IDX_TIME], "%Y-%m-%d %H:%M:%S")
    except Exception:
        ts_dt = _now_dt()

    is_weekend = _is_weekend(ts_dt)
    is_holiday = _is_holiday(ts_dt)
    is_normal_weekday = (not is_weekend) and (not is_holiday)

    morning_hours = 0.0
    evening_hours = 0.0
    total_ot = 0.0
    ot_type = ""
    note = ""
    should_notify = False
    weekday_msg = True  # False → use "OT today: X" wording

    # Helper: append one OT record row
    def append_ot_record(start_dt, end_dt, morning_h, evening_h, ot_type_str, note_str):
        try:
            tab_name = OT_RECORD_TAB
            ws = open_worksheet(tab_name)
            try:
                ensure_sheet_headers_match(ws, OT_RECORD_HEADERS)
            except Exception:
                pass
            day_str = (start_dt or end_dt).strftime("%Y-%m-%d") if (start_dt or end_dt) else ""
            row = [
                driver,                      # Name
                ot_type_str,                 # Type 150% / 200%
                start_dt.strftime("%Y-%m-%d %H:%M:%S") if start_dt else "",
                end_dt.strftime("%Y-%m-%d %H:%M:%S") if end_dt else "",
                day_str,
                f"{morning_h:.2f}" if morning_h > 0 else "",
                f"{evening_h:.2f}" if evening_h > 0 else "",
                note_str,
            ]
            try:
                ws.append_row(row, value_input_option="USER_ENTERED")
            except Exception:
                ws.append_row(row)
        except Exception:
            logger.exception("Failed to append OT record row for %s", driver)

    # --- Normal weekdays OT rules ---
    if is_normal_weekday:
        if action == "IN":
            # Rule 2: IN between (04:00, 07:00)
            t4 = ts_dt.replace(hour=4, minute=0, second=0, microsecond=0)
            t7 = ts_dt.replace(hour=7, minute=0, second=0, microsecond=0)
            if t4 < ts_dt < t7:
                end_morning = ts_dt.replace(hour=8, minute=0, second=0, microsecond=0)
                morning_hours = max((end_morning - ts_dt).total_seconds() / 3600.0, 0)
                total_ot = round(morning_hours, 2)
                if total_ot > 0:
                    ot_type = "150%"
                    note = "Weekday morning OT (Clock In)"
                    append_ot_record(ts_dt, end_morning, total_ot, 0.0, ot_type, note)
                    should_notify = True
        else:
            # action == OUT
            h = ts_dt.hour + ts_dt.minute / 60.0
            # Rule 1: OUT between [00:00, 04:00)
            if 0 <= h < 4:
                start_dt = ts_dt.replace(hour=0, minute=0, second=0, microsecond=0)
                morning_hours = max((ts_dt - start_dt).total_seconds() / 3600.0, 0)
                total_ot = round(morning_hours, 2)
                if total_ot > 0:
                    ot_type = "150%"
                    note = "Weekday early-morning OT (after midnight)"
                    append_ot_record(start_dt, ts_dt, total_ot, 0.0, ot_type, note)
                    should_notify = True
            # Rule 5: OUT >= 18:30
            elif ts_dt.hour > 18 or (ts_dt.hour == 18 and ts_dt.minute >= 30):
                start_dt = ts_dt.replace(hour=18, minute=0, second=0, microsecond=0)
                evening_hours = max((ts_dt - start_dt).total_seconds() / 3600.0, 0)
                total_ot = round(evening_hours, 2)
                if total_ot > 0:
                    ot_type = "150%"
                    note = "Weekday evening OT"
                    append_ot_record(start_dt, ts_dt, 0.0, total_ot, ot_type, note)
                    should_notify = True

    # --- Weekend / Holiday OT rules ---
    else:
        # Only act on OUT; IN just records time
        if action == "OUT":
            start_dt = None
            if last and len(last) > O_IDX_ACTION and last[O_IDX_ACTION] == "IN":
                try:
                    start_dt = datetime.strptime(last[O_IDX_TIME], "%Y-%m-%d %H:%M:%S")
                except Exception:
                    start_dt = None
            if start_dt is not None:
                # Full shift as OT
                if ts_dt < start_dt:
                    ts_dt_adj = ts_dt + timedelta(days=1)
                else:
                    ts_dt_adj = ts_dt
                dur = max((ts_dt_adj - start_dt).total_seconds() / 3600.0, 0)
                total_ot = round(dur, 2)
                if total_ot > 0:
                    ot_type = "200%"
                    note = "Weekend/Holiday full-shift OT"
                    append_ot_record(start_dt, ts_dt_adj, 0.0, total_ot, ot_type, note)
                    should_notify = True
                    weekday_msg = False  # use 'OT today' wording

    # --- Notifications & user feedback ---
    if should_notify and total_ot > 0 and chat is not None:
        try:
            if weekday_msg:
                msg = f"Driver {driver}: OT: {total_ot:.2f} hour(s)."
            else:
                msg = f"Driver {driver}: OT today: {total_ot:.2f} hour(s)."
            await context.bot.send_message(chat_id=chat.id, text=msg)
        except Exception:
            logger.exception("Failed to send OT notification")

    # Edit the inline-button message as a confirmation
    try:
        if total_ot > 0:
            await query.edit_message_text(
                f"Recorded {action} for {driver} at {ts_dt.strftime('%Y-%m-%d %H:%M:%S')}. OT: {total_ot:.2f} hour(s)."
            )
        else:
            await query.edit_message_text(
                f"Recorded {action} for {driver} at {ts_dt.strftime('%Y-%m-%d %H:%M:%S')}."
            )
    except Exception:
        # Fallback: ignore edit errors
        pass
# ---------------------------------------------------------
# Driver / Mission / Leave / Finance Helpers

# Register OT handlers (inserted)
try:
    # These handlers implement Clock In/Out toggle and OT reporting
    application.add_handler(CallbackQueryHandler(clock_callback_handler, pattern=r"^clock_toggle$"))
    application.add_handler(CommandHandler("ot_report", ot_report_entry))

except Exception:
    # If application not available at import time, registration will be attempted in register_ui_handlers
    pass

# --- END: Inserted OT & Clock functionality ---
#!/usr/bin/env python3
import json
import base64
import logging
import uuid
import re
from typing import Optional, Dict, List, Any
import urllib.request

import gspread
from oauth2client.service_account import ServiceAccountCredentials

try:
    from zoneinfo import ZoneInfo
except Exception:
    ZoneInfo = None

from telegram import (
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    BotCommand,
    Update,
    ReplyKeyboardMarkup,
    KeyboardButton,
    ForceReply,
)
from telegram.constants import ChatAction
from telegram.ext import (
    ApplicationBuilder,
    CommandHandler,
    CallbackQueryHandler,
    MessageHandler,
    filters,
    ContextTypes,
    PicklePersistence,
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("driver-bot")

BOT_TOKEN = os.getenv("BOT_TOKEN")
GOOGLE_CREDS_BASE64 = os.getenv("GOOGLE_CREDS_BASE64")
GOOGLE_CREDS_PATH = os.getenv("GOOGLE_CREDS_PATH")

PLATE_LIST = os.getenv(
    "PLATE_LIST",
    "2BB-3071,2BB-0809,2CI-8066,2CK-8066,2CJ-8066,3H-8066,2AV-6527,2AZ-6828,2AX-4635,2BV-8320",
)
GOOGLE_SHEET_NAME = os.getenv("GOOGLE_SHEET_NAME", "Driver_Log")
GOOGLE_SHEET_TAB = os.getenv("GOOGLE_SHEET_TAB", "")

_env_tz = os.getenv("LOCAL_TZ")
if _env_tz is None:
    LOCAL_TZ = "Asia/Phnom_Penh"
else:
    LOCAL_TZ = _env_tz.strip() or None

PLATES = [p.strip() for p in PLATE_LIST.split(",") if p.strip()]
DRIVER_PLATE_MAP_JSON = os.getenv("DRIVER_PLATE_MAP", "").strip() or None

SUMMARY_CHAT_ID = os.getenv("SUMMARY_CHAT_ID")
SUMMARY_HOUR = int(os.getenv("SUMMARY_HOUR", "20"))
SUMMARY_TZ = os.getenv("SUMMARY_TZ", LOCAL_TZ or "Asia/Phnom_Penh")

DEFAULT_LANG = os.getenv("LANG", "en").lower()

# --- Added: Holiday list from environment variable ---
# Format: HOLIDAYS="2025-12-25,2025-12-31"
try:
    _raw_holidays = os.getenv("HOLIDAYS", "") or ""
    HOLIDAYS = {d.strip() for d in _raw_holidays.split(",") if d.strip()}
except Exception:
    HOLIDAYS = set()

SUPPORTED_LANGS = ("en", "km")

RECORDS_TAB = os.getenv("RECORDS_TAB", "Driver_Log")
DRIVERS_TAB = os.getenv("DRIVERS_TAB", "Drivers")
SUMMARY_TAB = os.getenv("SUMMARY_TAB", "Summary")
MISSIONS_TAB = os.getenv("MISSIONS_TAB", "Missions")
LEAVE_TAB = os.getenv("LEAVE_TAB", "Driver_Leave")

# OT tab name (created if missing)
OT_TAB = os.getenv('OT_TAB', 'Driver_OT')
OT_HEADERS = ['Date', 'Driver', 'Action', 'Timestamp', 'ClockType', 'Note']

MAINT_TAB = os.getenv("MAINT_TAB", "Vehicle_Maintenance")
EXPENSE_TAB = os.getenv("EXPENSE_TAB", "Trip_Expenses")

# new separate finance tabs
FUEL_TAB = os.getenv("FUEL_TAB", "Fuel")
PARKING_TAB = os.getenv("PARKING_TAB", "Parking")
WASH_TAB = os.getenv("WASH_TAB", "Wash")
REPAIR_TAB = os.getenv("REPAIR_TAB", "Repair")
ODO_TAB = os.getenv("ODO_TAB", "Odo")

BOT_ADMINS_DEFAULT = "markpeng1,kmnyy,ClaireRin777"

M_IDX_GUID = 0
M_IDX_NO = 1
M_IDX_NAME = 2
M_IDX_PLATE = 3
M_IDX_START = 4
M_IDX_END = 5
M_IDX_DEPART = 6
M_IDX_ARRIVAL = 7
M_IDX_STAFF = 8
M_IDX_ROUNDTRIP = 9
M_IDX_RETURN_START = 10
M_IDX_RETURN_END = 11
M_MANDATORY_COLS = 12

COL_DATE = 1
COL_DRIVER = 2
COL_PLATE = 3
COL_START = 4
COL_END = 5
COL_DURATION = 6

TS_FMT = "%Y-%m-%d %H:%M:%S"
DATE_FMT = "%Y-%m-%d"

ROUNDTRIP_WINDOW_HOURS = int(os.getenv("ROUNDTRIP_WINDOW_HOURS", "24"))

# --- BEGIN: Google Sheets API queue, caching and Worksheet proxy helpers ---
import threading
import queue
import time
from typing import Callable, Any, Optional, Dict, Tuple

# --- BEGIN: Bot state persistence to Google Sheets (mission_cycle) ---
import base64, json, io
from google.oauth2 import service_account
import gspread

# --- BEGIN: ENV NAMES NORMALIZATION & Bot-state persistence helpers ---
import base64, json
from google.oauth2 import service_account
import gspread
if not os.getenv('GOOGLE_CREDS_B64') and os.getenv('GOOGLE_CREDS_BASE64'):
    os.environ['GOOGLE_CREDS_B64'] = os.getenv('GOOGLE_CREDS_BASE64')
_GSPREAD_SCOPES = ['https://www.googleapis.com/auth/spreadsheets','https://www.googleapis.com/auth/drive']
_LOADED_MISSION_CYCLES = {}
def _get_gspread_client():
    b64 = os.getenv('GOOGLE_CREDS_B64') or os.getenv('GOOGLE_CREDS_BASE64')
    if not b64:
        raise RuntimeError('Google credentials not provided (GOOGLE_CREDS_B64 / GOOGLE_CREDS_BASE64)')
    info = json.loads(base64.b64decode(b64))
    try:
        creds = service_account.Credentials.from_service_account_info(info, scopes=_GSPREAD_SCOPES)
    except Exception:
        creds = service_account.Credentials.from_service_account_info(info)
    return gspread.authorize(creds)
def open_bot_state_worksheet():
    gc = _get_gspread_client()
    sheet_name = os.getenv('GOOGLE_SHEET_NAME'); sheet_id = os.getenv('SHEET_ID')
    if sheet_name: sh = gc.open(sheet_name)
    elif sheet_id: sh = gc.open_by_key(sheet_id)
    else: raise RuntimeError('Provide GOOGLE_SHEET_NAME or SHEET_ID')
    tab = os.getenv('BOT_STATE_TAB') or 'Bot_State'
    try: ws = sh.worksheet(tab)
    except Exception:
        ws = sh.add_worksheet(tab, rows=100, cols=10); ws.update('A1:B1',[['Key','Value']])
    return ws
def load_mission_cycles_from_sheet():
    global _LOADED_MISSION_CYCLES
    try:
        ws = open_bot_state_worksheet(); records = ws.get_all_records()
        for r in records:
            k = r.get('Key') or r.get('key'); v = r.get('Value') or r.get('value')
            if k == 'mission_cycle' and v:
                _LOADED_MISSION_CYCLES = json.loads(v); return _LOADED_MISSION_CYCLES
    except Exception:
        pass
    _LOADED_MISSION_CYCLES = {}; return _LOADED_MISSION_CYCLES
def save_mission_cycles_to_sheet(mdict):
    try:
        ws = open_bot_state_worksheet(); records = ws.get_all_records(); found=None
        for idx,r in enumerate(records,start=2):
            k = r.get('Key') or r.get('key')
            if k == 'mission_cycle': found=idx; break
        j=json.dumps(mdict, ensure_ascii=False)
        if found: ws.update(f'B{found}', j)
        else: ws.append_row(['mission_cycle', j])
    except Exception:
        try: logger.exception('Failed to save mission cycles to sheet')
        except Exception: pass
# --- END: ENV NAMES NORMALIZATION & Bot-state persistence helpers ---

# Top-level OT writer (moved out of nested scope to avoid indentation issues)
def _write_ot_rows(rows):
    logger.info("Entering _write_ot_rows")
    try:
        # Prefer the configured OT_RECORD_TAB; fall back to legacy OT_SUM_TAB or default "OT record"
        tab_name = OT_RECORD_TAB or os.getenv("OT_SUM_TAB") or "OT record"
        ws = open_worksheet(tab_name)
        headers = OT_RECORD_HEADERS
        try:
            ensure_sheet_headers_match(ws, headers)
        except Exception:
            try:
                logger.exception("Failed to ensure/update OT record headers")
            except Exception:
                pass
        for r in rows:
            try:
                ws.append_row(r, value_input_option='USER_ENTERED')
            except Exception:
                try:
                    ws.append_row(r)
                except Exception:
                    logger.exception("Failed to append OT calc row %s", r)
    except Exception:
        logger.exception("Failed writing OT calc rows")

_LOADED_MISSION_CYCLES = {}

def _get_gspread_client():
    # Accept either GOOGLE_CREDS_BASE64 or GOOGLE_CREDS_B64 env var
    b64 = os.getenv("GOOGLE_CREDS_BASE64") or os.getenv("GOOGLE_CREDS_B64")
    if not b64:
        raise RuntimeError("Google credentials not provided in environment (GOOGLE_CREDS_BASE64 / GOOGLE_CREDS_B64)")
    cred_json = base64.b64decode(b64)
    creds = service_account.Credentials.from_service_account_info(json.loads(cred_json))
    return gspread.authorize(creds)

def open_bot_state_worksheet():
    gc = _get_gspread_client()
    # prefer GOOGLE_SHEET_NAME, else fall back to SHEET_ID
    sheet_name = os.getenv("GOOGLE_SHEET_NAME")
    sheet_id = os.getenv("SHEET_ID")
    if sheet_name:
        sh = gc.open(sheet_name)
    elif sheet_id:
        sh = gc.open_by_key(sheet_id)
    else:
        raise RuntimeError("Neither GOOGLE_SHEET_NAME nor SHEET_ID provided")
    tab = os.getenv("BOT_STATE_TAB") or "Bot_State"
    try:
        ws = sh.worksheet(tab)
    except Exception:
        # create worksheet if missing
        ws = sh.add_worksheet(tab, rows=100, cols=10)
        # set headers
        ws.update("A1:B1", [["Key","Value"]])
    return ws

def load_mission_cycles_from_sheet():
    global _LOADED_MISSION_CYCLES
    try:
        ws = open_bot_state_worksheet()
        records = ws.get_all_records()
        for r in records:
            k = r.get("Key") or r.get("key")
            v = r.get("Value") or r.get("value")
            if k and v:
                if k == "mission_cycle":
                    try:
                        _LOADED_MISSION_CYCLES = json.loads(v)
                    except Exception:
                        _LOADED_MISSION_CYCLES = {}
                    return _LOADED_MISSION_CYCLES
        # if not found, keep empty dict
        _LOADED_MISSION_CYCLES = {}
        return _LOADED_MISSION_CYCLES
    except Exception:
        # don't crash startup; leave empty
        _LOADED_MISSION_CYCLES = {}
        return _LOADED_MISSION_CYCLES

def save_mission_cycles_to_sheet(mdict):
    try:
        ws = open_bot_state_worksheet()
        records = ws.get_all_records()
        found_row = None
        for idx, r in enumerate(records, start=2):
            k = r.get("Key") or r.get("key")
            if k == "mission_cycle":
                found_row = idx
                break
        json_val = json.dumps(mdict, ensure_ascii=False)
        if found_row:
            ws.update(f"B{found_row}", json_val)
        else:
            ws.append_row(["mission_cycle", json_val])
    except Exception as e:
        logger.exception("Failed to save mission cycles to sheet: %s", e)
# --- END: Bot state persistence ---

# Simple thread-based serial executor to avoid 429s.
class GoogleApiQueue:
    def __init__(self, min_interval_sec: float = 1.0, max_retries: int = 5, backoff_factor: float = 1.5):
        self._q: "queue.Queue[Tuple[Callable, tuple, dict, queue.Queue]]" = queue.Queue()
        self._min_interval = min_interval_sec
        self._last_time = 0.0
        self._thread = threading.Thread(target=self._worker, daemon=True)
        self._stop = threading.Event()
        self._max_retries = max_retries
        self._backoff = backoff_factor
        self._thread.start()

    def _worker(self):
        while not self._stop.is_set():
            try:
                func, args, kwargs, resp_q = self._q.get(timeout=0.1)
            except queue.Empty:
                continue
            # Ensure minimum interval between requests
            now = time.time()
            since = now - self._last_time
            if since < self._min_interval:
                time.sleep(self._min_interval - since)
            attempt = 0
            while True:
                try:
                    result = func(*args, **kwargs)
                    self._last_time = time.time()
                    resp_q.put((True, result))
                    break
                except Exception as e:
                    attempt += 1
                    # If likely a rate-limit / transient error, retry with backoff up to max_retries;
                    # otherwise return error after retries.
                    if attempt > self._max_retries:
                        resp_q.put((False, e))
                        break
                    # backoff sleep
                    time.sleep(self._backoff * attempt)
            self._q.task_done()

    def submit(self, func: Callable, *args, **kwargs) -> Tuple[bool, Any]:
        resp_q: "queue.Queue" = queue.Queue()
        self._q.put((func, args, kwargs, resp_q))
        ok, res = resp_q.get()
        return ok, res

    def stop(self):
        self._stop.set()
        try:
            self._thread.join(timeout=1.0)
        except Exception:
            pass

# Singleton queue instance (used by WorksheetProxy)
_api_queue = GoogleApiQueue(min_interval_sec=1.0, max_retries=6, backoff_factor=1.2)

# Aggressive in-memory read cache for sheet values (per worksheet title)
_sheets_read_cache: Dict[str, Tuple[float, Any]] = {}
_READ_CACHE_TTL = 10.0  # seconds (aggressive caching)

class WorksheetProxy:
    """
    Wraps a gspread Worksheet object and routes calls through the _api_queue.
    Also provides a read cache for get_all_values/get_all_records to reduce read QPS.
    """
    def __init__(self, ws):
        self._ws = ws
        # use title as cache key when available
        self._key = getattr(ws, "title", None) or str(id(ws))

    # Helper to submit to the queue and propagate exceptions
    def _submit(self, fn_name: str, *args, **kwargs):
        func = getattr(self._ws, fn_name)
        ok, res = _api_queue.submit(func, *args, **kwargs)
        if not ok:
            # raise original exception
            raise res
        return res

    def get_all_values(self, *args, **kwargs):
        now = time.time()
        cache = _sheets_read_cache.get(self._key)
        if cache and (now - cache[0]) < _READ_CACHE_TTL:
            return cache[1]
        # call
        vals = self._submit("get_all_values", *args, **kwargs)
        _sheets_read_cache[self._key] = (time.time(), vals)
        return vals

    def get_all_records(self, *args, **kwargs):
        # gspread internally calls get_all_values so use cache by asking for values then convert.
        vals = self.get_all_values(*args, **kwargs)
        # Attempt to emulate gspread.Worksheet.get_all_records behavior
        if not vals:
            return []
        headers = vals[0]
        out = []
        for row in vals[1:]:
            obj = {}
            for i, h in enumerate(headers):
                obj[h] = row[i] if i < len(row) else ""
            out.append(obj)
        return out

    def row_values(self, *args, **kwargs):
        return self._submit("row_values", *args, **kwargs)

    def append_row(self, *args, **kwargs):
        # Invalidate read cache on writes
        res = self._submit("append_row", *args, **kwargs)
        _sheets_read_cache.pop(self._key, None)
        return res

    def update_cell(self, *args, **kwargs):
        res = self._submit("update_cell", *args, **kwargs)
        _sheets_read_cache.pop(self._key, None)
        return res

    def update(self, *args, **kwargs):
        res = self._submit("update", *args, **kwargs)
        _sheets_read_cache.pop(self._key, None)
        return res

    def delete_rows(self, *args, **kwargs):
        # gspread newer method name; support both delete_rows and delete_row
        if hasattr(self._ws, "delete_rows"):
            res = self._submit("delete_rows", *args, **kwargs)
        else:
            res = self._submit("delete_row", *args, **kwargs)
        _sheets_read_cache.pop(self._key, None)
        return res

    def delete_row(self, *args, **kwargs):
        return self.delete_rows(*args, **kwargs)

    def insert_row(self, *args, **kwargs):
        res = self._submit("insert_row", *args, **kwargs)
        _sheets_read_cache.pop(self._key, None)
        return res

    def worksheet(self, *args, **kwargs):
        # Delegate to internal spreadsheet if called
        return getattr(self._ws, "worksheet")(*args, **kwargs)

    def __getattr__(self, name):
        # Fallback for other attributes/methods: call directly but queued
        if hasattr(self._ws, name) and callable(getattr(self._ws, name)):
            def _callable(*a, **k):
                ok, res = _api_queue.submit(getattr(self._ws, name), *a, **k)
                if not ok:
                    raise res
                # Invalidate cache on any write-like operations heuristically
                if name.startswith(("append", "update", "delete", "insert")):
                    _sheets_read_cache.pop(self._key, None)
                return res
            return _callable
        return getattr(self._ws, name)
# --- END: Google Sheets API queue, caching and Worksheet proxy helpers ---

SCOPES = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]

HEADERS_BY_TAB: Dict[str, List[str]] = {
    RECORDS_TAB: ["Date", "Driver", "Plate", "Start DateTime", "End DateTime", "Duration"],
    MISSIONS_TAB: ["GUID", "No.", "Name", "Plate", "Start Date", "End Date", "Departure", "Arrival", "Staff Name", "Roundtrip", "Return Start", "Return End"],
    SUMMARY_TAB: ["Date", "PeriodType", "TotalsJSON", "HumanSummary"],
    DRIVERS_TAB: ["Username", "Plates"],
    LEAVE_TAB: ["Driver", "Start Date", "End Date", "Leave Days", "Reason", "Notes"],
    MAINT_TAB: ["Plate", "Mileage", "Maintenance Item", "Cost", "Date", "Workshop", "Notes"],
    EXPENSE_TAB: ["Plate", "Driver", "DateTime", "Mileage", "Delta KM", "Fuel Cost", "Parking Fee", "Other Fee", "Invoice", "DriverPaid"],
    FUEL_TAB: ["Plate", "Driver", "DateTime", "Mileage", "Delta KM", "Fuel Cost", "Invoice", "DriverPaid"],
    PARKING_TAB: ["Plate", "Driver", "DateTime", "Amount", "Notes"],
    WASH_TAB: ["Plate", "Driver", "DateTime", "Amount", "Notes"],
    REPAIR_TAB: ["Plate", "Driver", "DateTime", "Amount", "Notes"],
    ODO_TAB: ["Plate", "Driver", "DateTime", "Mileage", "Notes"],
}

# Ensure OT-related tabs have canonical headers
HEADERS_BY_TAB.setdefault(OT_TAB, OT_HEADERS)
HEADERS_BY_TAB.setdefault(OT_RECORD_TAB, OT_RECORD_HEADERS)

TR = {
    "en": {
        "menu": "Driver Bot Menu — tap a button:",
        "choose_start": "Choose vehicle plate to START trip:",
        "choose_end": "Choose vehicle plate to END trip:",
        "start_ok": "Driver {driver} {plate} starts trip at {ts}.",
        "end_ok": "Driver {driver} {plate} ends trip at {ts}.",
        "trip_summary": "Driver {driver} completed {n_today} trip(s) today and {n_month} trip(s) in {month} and {n_year} trip(s) in {year}.\n{plate} completed {p_today} trip(s) today and {p_month} trip(s) in {month} and {p_year} trip(s) in {year}.",
        "not_allowed": "❌ You are not allowed to operate plate: {plate}.",
        "invalid_sel": "Invalid selection.",
        "help": "Help: Use /start_trip or /end_trip and select a plate.",
        "mission_start_prompt_plate": "Choose plate to start mission:",
        "mission_start_prompt_depart": "Select departure city:",
        "mission_end_prompt_plate": "Choose plate to end mission:",
        "mission_start_ok": "Driver {driver} {plate} departures from {dep} at {ts}.",
        "mission_end_ok": "Driver {driver} {plate} arrives at {arr} at {ts}.",
        "mission_no_open": "No open mission found for {plate}.",
        "roundtrip_merged_notify": "✅ Driver {driver} completed {d_month} mission(s) in {month} and {d_year} mission(s) in {year}. {plate} completed {p_month} mission(s) in {month} and {p_year} mission(s) in {year}.",
        "lang_set": "Language set to {lang}.",
        "invalid_amount": "Invalid amount — please send a numeric value like `23.5`.",
        "invalid_odo": "Invalid odometer — please send numeric KM like `12345` or `12345KM`.",
        "confirm_recorded": "{typ} recorded for {plate}: {amount}",
        "leave_prompt": "Reply to this message: <driver_username> <-DD> <-DD> <reason> [notes]\nExample: markpeng1 2025-12-01 2025-12-05 annual_leave",
        "leave_confirm": "Leave recorded for {driver}: {start} to {end} ({reason})",
        "enter_odo_km": "Enter odometer reading (KM) for {plate}:",
        "enter_fuel_cost": "Enter fuel cost in $ for {plate}: (optionally add `inv:INV123 paid:yes`)",
    },
        "km": {
        "menu": "ម៉ឺនុយបុរសបើក — ចុចប៊ូតុងមួយ:",
        "choose_start": "ជ្រើសលេខឡាន ដើម្បីចាប់ផ្តើមដំណើរ:",
        "choose_end": "ជ្រើសលេខឡាន ដើម្បីបញ្ចប់ដំណើរ:",
        "start_ok": "អ្នកបើក {driver} លេខ {plate} បានចាប់ផ្តើមដំណើរ​នៅ {ts}។",
        "end_ok": "អ្នកបើក {driver} លេខ {plate} បានបញ្ចប់ដំណើរ​នៅ {ts}។",
        "trip_summary": "អ្នកបើក {driver} បានបញ្ចប់ {n_today} ដំណើរ នៅថ្ងៃនេះ និង {n_month} ក្នុង {month} និង {n_year} ក្នុង {year}។\n{plate} បានបញ្ចប់ {p_today} ដំណើរ នៅថ្ងៃនេះ និង {p_month} ក្នុង {month} និង {p_year} ក្នុង {year}។",
        "not_allowed": "❌ អ្នកមិនមានសិទ្ធិបើកឡាននេះ: {plate}។",
        "invalid_sel": "ការជ្រើសមិនត្រឹមត្រូវ។",
        "help": "ជំនួយ៖ ប្រើ /start_trip ឬ /end_trip ហើយជ្រើសលេខឡាន។",
        "mission_start_prompt_plate": "ជ្រើសលេខឡាន ដើម្បីចាប់ផ្តើមបេសកកម្ម:",
        "mission_start_prompt_depart": "ជ្រើសទីក្រុងចេញដំណើរ:",
        "mission_end_prompt_plate": "ជ្រើសលេខឡាន ដើម្បីបញ្ចប់បេសកកម្ម:",
        "mission_start_ok": "អ្នកបើក {driver} លេខ {plate} បានចេញពី {dep} នៅ {ts}។",
        "mission_end_ok": "អ្នកបើក {driver} លេខ {plate} បានមកដល់ {arr} នៅ {ts}។",
        "mission_no_open": "មិនមានបេសកកម្មបើកសម្រាប់ {plate} ទេ។",
        "roundtrip_merged_notify": "✅{driver} បានបញ្ចប់ {d_month} បេសកកម្ម ក្នុង {month} និង {d_year} ក្នុង {year}។\n✅{driver} មាន {md_today} ថ្ងៃបេសកកម្ម (ថ្ងៃនេះ), {md_month} ថ្ងៃក្នុង {month} {year}។\n✅{plate} បានបញ្ចប់ {p_month} បេសកកម្ម ក្នុង {month} និង {p_year} ក្នុង {year}។",
        "lang_set": "បានកំណត់ភាសាទៅ {lang}។",
        "invalid_amount": "ចំនួនមិនត្រឹមត្រូវ — សូមផ្ញើលេខដូចជា `23.5`។",
        "invalid_odo": "Odometer មិនត្រឹមត្រូវ — សូមផ្ញើលេខ KM ដូចជា `12345` ឬ `12345KM`។",
        "confirm_recorded": "{typ} ត្រូវបានកត់ត្រាសម្រាប់ {plate}: {amount}",
        "leave_prompt": "ឆ្លើយតបទៅសារ​នេះ៖ <driver_username> <-DD> <-DD> <មូលហេតុ> [កំណត់សំគាល់]\nឧទាហរណ៍: markpeng1 2025-12-01 2025-12-05 annual_leave",
        "leave_confirm": "ការសុំច្បាប់ត្រូវបានកត់ត្រាសម្រាប់ {driver}: {start} ដល់ {end} ({reason})",
        "enter_odo_km": "សូមបញ្ចូល Odometer (KM) សម្រាប់ {plate}:",
        "enter_fuel_cost": "សូមបញ្ចូលថ្លៃប្រេង (USD) សម្រាប់ {plate}: (អាចបញ្ចូល `inv:INV123 paid:yes`)",
    },
}

def t(user_lang: Optional[str], key: str, **kwargs) -> str:
    lang = (user_lang or DEFAULT_LANG or "en").lower()
    if lang not in SUPPORTED_LANGS:
        lang = "en"
    return TR.get(lang, TR["en"]).get(key, TR["en"].get(key, "")).format(**kwargs)

def _load_creds_from_base64(encoded: str) -> dict:
    try:
        if encoded.strip().startswith("{"):
            return json.loads(encoded)
        padded = "".join(encoded.split())
        missing = len(padded) % 4
        if missing:
            padded += "=" * (4 - missing)
        decoded = base64.b64decode(padded)
        return json.loads(decoded)
    except Exception:
        logger.exception("Failed to decode GOOGLE_CREDS_BASE64")
        raise

def get_gspread_client():
    creds_json = None
    if GOOGLE_CREDS_BASE64:
        creds_json = _load_creds_from_base64(GOOGLE_CREDS_BASE64)
    elif GOOGLE_CREDS_PATH and os.path.exists(GOOGLE_CREDS_PATH):
        with open(GOOGLE_CREDS_PATH, "r", encoding="utf-8") as f:
            creds_json = json.load(f)
    else:
        fallback = "credentials.json"
        if os.path.exists(fallback):
            with open(fallback, "r", encoding="utf-8") as f:
                creds_json = json.load(f)
    if not creds_json:
        raise RuntimeError("Google credentials not found.")
    creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_json, SCOPES)
    client = gspread.authorize(creds)
    return client

def ensure_sheet_has_headers_conservative(ws, headers: List[str]):
    try:
        values = ws.get_all_values()
        if not values:
            ws.insert_row(headers, index=1)
    except Exception:
        logger.exception("Failed to ensure headers on %s", getattr(ws, "title", "<ws>"))

def ensure_sheet_headers_match(ws, headers: List[str]):
    try:
        values = ws.get_all_values()
        if not values:
            ws.insert_row(headers, index=1)
            return
        first_row = values[0]
        norm_first = [str(c).strip() for c in first_row]
        norm_headers = [str(c).strip() for c in headers]
        if norm_first != norm_headers:
            rng = f"A1:{chr(ord('A') + len(headers) - 1)}1"
            ws.update(rng, [headers], value_input_option="USER_ENTERED")
            logger.info("Updated header row on %s", getattr(ws, "title", "<ws>"))
    except Exception:
        logger.exception("Failed to ensure/update headers on %s", getattr(ws, "title", "<ws>"))

_UUID_RE = re.compile(r'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$', re.I)

def _missions_header_fix_if_needed(ws):
    try:
        values = ws.get_all_values()
        if not values:
            return
        first_row = values[0]
        header_like_keywords = {"no", "no.", "name", "plate", "start", "end", "departure", "arrival", "staff", "roundtrip"}
        is_header_like = any(str(c).strip().lower() in header_like_keywords for c in first_row if c)
        if not is_header_like:
            return
        if len(values) < 2:
            return
        second_row = values[1]
        first_cell = str(second_row[0]).strip() if len(second_row) > 0 else ""
        if first_cell and _UUID_RE.match(first_cell):
            header_first = str(first_row[0]).strip().lower() if len(first_row) > 0 else ""
            if header_first != "guid":
                headers = HEADERS_BY_TAB.get(MISSIONS_TAB, [])
                if not headers:
                    return
                try:
                    h = list(headers)
                    while len(h) < M_MANDATORY_COLS:
                        h.append("")
                    col_letter_end = chr(ord('A') + M_MANDATORY_COLS - 1)
                    rng = f"A1:{col_letter_end}1"
                    ws.update(rng, [h], value_input_option="USER_ENTERED")
                    logger.info("Fixed MISSIONS header row to canonical headers due to GUID detected.")
                except Exception:
                    logger.exception("Failed to update header row in MISSIONS sheet.")
    except Exception:
        logger.exception("Error checking/fixing missions header.")

def open_worksheet(tab: str = ""):
    """Open a worksheet with minimal header enforcement and wrap it in WorksheetProxy.

    This central helper applies:
    - GoogleApiQueue for all sheet operations
    - Lightweight header checks/creation using HEADERS_BY_TAB
    """

    def _wrap_ws(ws):
        try:
            return WorksheetProxy(ws)
        except Exception:
            # If proxying somehow fails, fall back to raw worksheet
            return ws

    gc = get_gspread_client()
    sh = gc.open(GOOGLE_SHEET_NAME)

    def _create_tab(name: str, headers: Optional[List[str]] = None):
        try:
            cols = max(12, len(headers) if headers else 12)
            ws_new = sh.add_worksheet(title=name, rows="2000", cols=str(cols))
            if headers:
                # Header row – queued via proxy, but it's a one‑time write anyway
                ws_new.insert_row(headers, index=1)
            return _wrap_ws(ws_new)
        except Exception:
            # If sheet already exists or another error, just get existing
            try:
                return _wrap_ws(sh.worksheet(name))
            except Exception:
                raise

    if tab:
        try:
            ws = sh.worksheet(tab)
            template = HEADERS_BY_TAB.get(tab)
            if template:
                ensure_sheet_has_headers_conservative(ws, template)
                ensure_sheet_headers_match(ws, template)
            if tab == MISSIONS_TAB:
                _missions_header_fix_if_needed(ws)
            return _wrap_ws(ws)
        except Exception:
            headers = HEADERS_BY_TAB.get(tab)
            return _create_tab(tab, headers=headers)
    else:
        if GOOGLE_SHEET_TAB:
            try:
                ws = sh.worksheet(GOOGLE_SHEET_TAB)
                if GOOGLE_SHEET_TAB in HEADERS_BY_TAB:
                    ensure_sheet_has_headers_conservative(ws, HEADERS_BY_TAB[GOOGLE_SHEET_TAB])
                    ensure_sheet_headers_match(ws, HEADERS_BY_TAB[GOOGLE_SHEET_TAB])
                return _wrap_ws(ws)
            except Exception:
                return _create_tab(GOOGLE_SHEET_TAB, headers=None)
        # Default to first sheet, wrapped
        return _wrap_ws(sh.sheet1)







async def process_leave_entry(ws, driver, start, end, reason, notes, update, context, pending_leave, user):
    """Helper to append leave row with Leave Days, check duplicates and exclude weekends/holidays."""
    try:
        sd_dt = datetime.strptime(start, "%Y-%m-%d")
        ed_dt = datetime.strptime(end, "%Y-%m-%d")
    except Exception:
        sd_dt = None
        ed_dt = None

    try:
        records = ws.get_all_records()
    except Exception:
        records = []

    # check overlaps
    if sd_dt and ed_dt:
        for r in records:
            try:
                r_driver = next((r[k] for k in ("Driver","driver","Username","Name") if k in r and str(r.get(k,"")).strip()), "")
                if r_driver != driver:
                    continue
                r_start = next((r[k] for k in ("Start","Start Date","Start DateTime","StartDate") if k in r and str(r.get(k,"")).strip()), None)
                r_end = next((r[k] for k in ("End","End Date","End DateTime","EndDate") if k in r and str(r.get(k,"")).strip()), None)
                if not r_start or not r_end:
                    continue
                r_s = str(r_start).split()[0]
                r_e = str(r_end).split()[0]
                r_sd = datetime.strptime(r_s, "%Y-%m-%d")
                r_ed = datetime.strptime(r_e, "%Y-%m-%d")
                if not (ed_dt < r_sd or sd_dt > r_ed):
                    # overlap
                    msg = f"This date has already been applied for leave ({r_s} to {r_e}), please choose different dates."
                    try:
                        await context.bot.send_message(chat_id=user.id, text=msg)
                    except Exception:
                        pass
                    try:
                        await safe_delete_message(context.bot, pending_leave.get("prompt_chat"), pending_leave.get("prompt_msg_id"))
                    except Exception:
                        pass
                    context.user_data.pop("pending_leave", None)
                    return False
            except Exception:
                continue

    # compute leave days excluding weekends and HOLIDAYS
    leave_days = 0
    if sd_dt and ed_dt and sd_dt <= ed_dt:
        cur = sd_dt
        while cur <= ed_dt:
            try:
                is_hol = cur.strftime("%Y-%m-%d") in HOLIDAYS
            except Exception:
                is_hol = False
            if cur.weekday() < 5 and not is_hol:
                leave_days += 1
            cur += timedelta(days=1)

    row = [driver, start, end, str(leave_days), reason, notes]
    try:
        ws.append_row(row, value_input_option="USER_ENTERED")
    except Exception:
        try:
            ws.append_row(row)
        except Exception:
            logger.exception("Failed to append leave row")
    # success
    return True

def load_driver_map_from_env() -> Dict[str, List[str]]:
    if not DRIVER_PLATE_MAP_JSON:
        return {}
    try:
        obj = json.loads(DRIVER_PLATE_MAP_JSON)
        normalized = {}
        for k, v in obj.items():
            if isinstance(v, str):
                plates = [p.strip() for p in v.split(",") if p.strip()]
            elif isinstance(v, list):
                plates = [str(p).strip() for p in v]
            else:
                plates = []
            normalized[str(k).strip()] = plates
        return normalized
    except Exception:
        logger.exception("Failed to parse DRIVER_PLATE_MAP env JSON.")
        return {}

def load_driver_map_from_sheet() -> Dict[str, List[str]]:
    try:
        ws = open_worksheet(DRIVERS_TAB)
        rows = ws.get_all_records()
        mapping = {}
        for r in rows:
            user = str(r.get("Username", r.get("username", r.get("User", "")))).strip()
            plates_raw = str(r.get("Plates", r.get("plates", r.get("Plate", "")))).strip()
            if user:
                mapping[user] = [p.strip() for p in plates_raw.split(",") if p.strip()]
        return mapping
    except Exception:
        logger.exception("Failed to load drivers tab.")
        return {}

def get_driver_map() -> Dict[str, List[str]]:
    env_map = load_driver_map_from_env()
    if env_map:
        return env_map
    sheet_map = load_driver_map_from_sheet()
    return sheet_map

def _now_dt() -> datetime:
    if LOCAL_TZ and ZoneInfo:
        try:
            tz = ZoneInfo(LOCAL_TZ)
            return datetime.now(tz)
        except Exception:
            logger.exception("Failed to use LOCAL_TZ; falling back to system time.")
            return datetime.now()
    else:
        return datetime.now()

def now_str() -> str:
    return _now_dt().strftime(TS_FMT)

def today_date_str() -> str:
    return _now_dt().strftime(DATE_FMT)

def parse_ts(ts: str) -> Optional[datetime]:
    try:
        return datetime.strptime(ts, TS_FMT)
    except Exception:
        return None

def compute_duration(start_ts: str, end_ts: str) -> str:
    try:
        s = parse_ts(start_ts)
        e = parse_ts(end_ts)
        if s is None or e is None:
            return ""
        delta = e - s
        total_minutes = int(delta.total_seconds() // 60)
        if total_minutes < 0:
            return ""
        hours = total_minutes // 60
        minutes = total_minutes % 60
        return f"{hours}h{minutes}m"
    except Exception:
        return ""

def record_start_trip(driver: str, plate: str) -> dict:
    ws = open_worksheet(RECORDS_TAB)
    start_ts = now_str()
    row = [today_date_str(), driver, plate, start_ts, "", ""]
    try:
        ws.append_row(row, value_input_option="USER_ENTERED")
        logger.info("Recorded start trip: %s %s %s", driver, plate, start_ts)
        return {"ok": True, "message": f"Start time recorded for {plate} at {start_ts}", "ts": start_ts}
    except Exception as e:
        logger.exception("Failed to append start trip")
        return {"ok": False, "message": "Failed to write start trip to sheet: " + str(e)}

def record_end_trip(driver: str, plate: str) -> dict:
    ws = open_worksheet(RECORDS_TAB)
    try:
        rows = ws.get_all_values()
        start_idx = 1 if rows and any("date" in c.lower() for c in rows[0] if c) else 0
        for idx in range(len(rows) - 1, start_idx - 1, -1):
            rec = rows[idx]
            rec_plate = rec[2] if len(rec) > 2 else ""
            rec_end = rec[4] if len(rec) > 4 else ""
            rec_start = rec[3] if len(rec) > 3 else ""
            if str(rec_plate).strip() == plate and (not rec_end):
                row_number = idx + 1
                end_ts = now_str()
                duration_text = compute_duration(rec_start, end_ts) if rec_start else ""
                try:
                    ws.update_cell(row_number, COL_END, end_ts)
                    ws.update_cell(row_number, COL_DURATION, duration_text)
                except Exception:
                    existing = ws.row_values(row_number)
                    while len(existing) < COL_DURATION:
                        existing.append("")
                    existing[COL_END - 1] = end_ts
                    existing[COL_DURATION - 1] = duration_text
                    try:
                        ws.delete_rows(row_number)
                    except Exception:
                        logger.exception("Failed to delete row for fallback replacement at %d", row_number)
                    try:
                        ws.insert_row(existing, row_number)
                    except Exception:
                        logger.exception("Failed to insert fallback row at %d", row_number)
                logger.info("Recorded end trip for %s row %d", plate, row_number)
                return {"ok": True, "message": f"End time recorded for {plate} at {end_ts} (duration {duration_text})", "ts": end_ts, "duration": duration_text}
        end_ts = now_str()
        row = [today_date_str(), driver, plate, "", end_ts, ""]
        ws.append_row(row, value_input_option="USER_ENTERED")
        logger.info("No open start found; appended end-only row for %s", plate)
        return {"ok": True, "message": f"End time recorded (no matching start found) for {plate} at {end_ts}", "ts": end_ts, "duration": ""}
    except Exception as e:
        logger.exception("Failed to update end trip")
        return {"ok": False, "message": "Failed to write end trip to sheet: " + str(e)}

def _missions_get_values_and_data_rows(ws):
    values = ws.get_all_values()
    if not values:
        return [], 0
    first_row = values[0]
    header_like_keywords = {"guid", "no", "name", "plate", "start", "end", "departure", "arrival", "staff", "roundtrip"}
    if any(str(c).strip().lower() in header_like_keywords for c in first_row if c):
        return values, 1
    return values, 0

def _missions_next_no(ws) -> int:
    vals, start_idx = _missions_get_values_and_data_rows(ws)
    return max(1, len(vals) - start_idx + 1)

def _ensure_row_length(row: List[Any], length: int) -> List[Any]:
    r = list(row)
    while len(r) < length:
        r.append("")
    return r

def start_mission_record(driver: str, plate: str, departure: str) -> dict:
    ws = open_worksheet(MISSIONS_TAB)
    start_ts = now_str()
    try:
        next_no = _missions_next_no(ws)
        guid = str(uuid.uuid4())
        row = [""] * M_MANDATORY_COLS
        row[M_IDX_GUID] = guid
        row[M_IDX_NO] = next_no
        row[M_IDX_NAME] = driver
        row[M_IDX_PLATE] = plate
        row[M_IDX_START] = start_ts
        row[M_IDX_END] = ""
        row[M_IDX_DEPART] = departure
        row[M_IDX_ARRIVAL] = ""
        row[M_IDX_STAFF] = ""
        row[M_IDX_ROUNDTRIP] = ""
        row[M_IDX_RETURN_START] = ""
        row[M_IDX_RETURN_END] = ""
        ws.append_row(row, value_input_option="USER_ENTERED")
        logger.info("Mission start recorded GUID=%s no=%s driver=%s plate=%s dep=%s", guid, next_no, driver, plate, departure)
        return {"ok": True, "guid": guid, "no": next_no, "start_ts": start_ts}
    except Exception as e:
        logger.exception("Failed to append mission start")
        return {"ok": False, "message": "Failed to write mission start to sheet: " + str(e)}

def end_mission_record(driver: str, plate: str, arrival: str) -> dict:
    try:
        ws = open_worksheet(MISSIONS_TAB)
    except Exception as e:
        logger.exception("Failed to open MISSIONS_TAB: %s", e)
        return {"ok": False, "message": "Could not open missions sheet: " + str(e)}

    try:
        vals, start_idx = _missions_get_values_and_data_rows(ws)
        for i in range(len(vals) - 1, start_idx - 1, -1):
            row = _ensure_row_length(vals[i], M_MANDATORY_COLS)
            rec_plate = str(row[M_IDX_PLATE]).strip()
            rec_name = str(row[M_IDX_NAME]).strip()
            rec_end = str(row[M_IDX_END]).strip()
            rec_start = str(row[M_IDX_START]).strip()
            rec_dep = str(row[M_IDX_DEPART]).strip()
            if rec_plate == plate and rec_name == driver and not rec_end:
                row_number = i + 1
                end_ts = now_str()
                try:
                    ws.update_cell(row_number, M_IDX_END + 1, end_ts)
                    ws.update_cell(row_number, M_IDX_ARRIVAL + 1, arrival)
                except Exception:
                    try:
                        existing = ws.row_values(row_number)
                    except Exception:
                        existing = []
                    existing = _ensure_row_length(existing, M_MANDATORY_COLS)
                    existing[M_IDX_END] = end_ts
                    existing[M_IDX_ARRIVAL] = arrival
                    try:
                        ws.delete_rows(row_number)
                    except Exception:
                        logger.exception("Failed to delete row for fallback replacement at %d", row_number)
                    try:
                        ws.insert_row(existing, row_number)
                    except Exception:
                        logger.exception("Failed to insert fallback row at %d", row_number)

                logger.info("Updated mission end for row %d plate=%s driver=%s", row_number, plate, driver)

                s_dt = parse_ts(rec_start) if rec_start else None
                if not s_dt:
                    return {"ok": True, "message": f"Mission end recorded for {plate} at {end_ts}", "merged": False, "end_ts": end_ts}

                window_start = s_dt - timedelta(hours=ROUNDTRIP_WINDOW_HOURS)
                window_end = s_dt + timedelta(hours=ROUNDTRIP_WINDOW_HOURS)

                vals2, start_idx2 = _missions_get_values_and_data_rows(ws)
                candidates = []
                for j in range(start_idx2, len(vals2)):
                    if j == i:
                        continue
                    r2 = _ensure_row_length(vals2[j], M_MANDATORY_COLS)
                    rn = str(r2[M_IDX_NAME]).strip()
                    rp = str(r2[M_IDX_PLATE]).strip()
                    rstart = str(r2[M_IDX_START]).strip()
                    rend = str(r2[M_IDX_END]).strip()
                    dep = str(r2[M_IDX_DEPART]).strip()
                    arr = str(r2[M_IDX_ARRIVAL]).strip()
                    if rn != driver or rp != plate:
                        continue
                    if not rstart or not rend:
                        continue
                    r_s_dt = parse_ts(rstart)
                    if not r_s_dt:
                        continue
                    if not (window_start <= r_s_dt <= window_end):
                        continue
                    candidates.append({"idx": j, "start": r_s_dt, "end": parse_ts(rend), "dep": dep, "arr": arr, "rstart": rstart, "rend": rend})

                found_pair = None
                cur_dep = rec_dep
                cur_arr = arrival
                for comp in candidates:
                    if (cur_dep == "PP" and cur_arr == "SHV" and comp["dep"] == "SHV" and comp["arr"] == "PP") or \
                       (cur_dep == "SHV" and cur_arr == "PP" and comp["dep"] == "PP" and comp["arr"] == "SHV"):
                        found_pair = comp
                        break

                if not found_pair:
                    for comp in candidates:
                        if comp["dep"] == cur_arr and comp["arr"] == cur_dep:
                            found_pair = comp
                            break

                if not found_pair and candidates:
                    candidates.sort(key=lambda x: abs((x["start"] - s_dt).total_seconds()))
                    found_pair = candidates[0]

                if not found_pair:
                    return {"ok": True, "message": f"Mission end recorded for {plate} at {end_ts}", "merged": False, "end_ts": end_ts}

                other_idx = found_pair["idx"]
                other_start = found_pair["start"]
                primary_idx = i if s_dt <= other_start else other_idx
                secondary_idx = other_idx if primary_idx == i else i

                primary_row_number = primary_idx + 1
                secondary_row_number = secondary_idx + 1

                if primary_idx == i:
                    return_start = found_pair["rstart"]
                    return_end = found_pair["rend"] if found_pair["rend"] else (found_pair["end"].strftime(TS_FMT) if found_pair["end"] else "")
                else:
                    return_start = rec_start
                    return_end = end_ts

                try:
                    ws.update_cell(primary_row_number, M_IDX_ROUNDTRIP + 1, "Yes")
                    ws.update_cell(primary_row_number, M_IDX_RETURN_START + 1, return_start)
                    ws.update_cell(primary_row_number, M_IDX_RETURN_END + 1, return_end)
                except Exception:
                    try:
                        existing = ws.row_values(primary_row_number)
                    except Exception:
                        existing = []
                    existing = _ensure_row_length(existing, M_MANDATORY_COLS)
                    existing[M_IDX_ROUNDTRIP] = "Yes"
                    existing[M_IDX_RETURN_START] = return_start
                    existing[M_IDX_RETURN_END] = return_end
                    try:
                        ws.delete_rows(primary_row_number)
                    except Exception:
                        logger.exception("Failed to delete primary row for fallback replacement at %d", primary_row_number)
                    try:
                        ws.insert_row(existing, primary_row_number)
                    except Exception:
                        logger.exception("Failed to insert fallback primary row at %d", primary_row_number)

                try:
                    sec_vals = _ensure_row_length(vals2[secondary_idx], M_MANDATORY_COLS) if secondary_idx < len(vals2) else None
                    sec_guid = sec_vals[M_IDX_GUID] if sec_vals else None
                    deleted_secondary = False
                    if sec_guid:
                        all_vals_post, start_idx_post = _missions_get_values_and_data_rows(ws)
                        for k in range(start_idx_post, len(all_vals_post)):
                            r_k = _ensure_row_length(all_vals_post[k], M_MANDATORY_COLS)
                            if str(r_k[M_IDX_GUID]).strip() == str(sec_guid).strip():
                                try:
                                    ws.delete_rows(k + 1)
                                    deleted_secondary = True
                                    break
                                except Exception:
                                    try:
                                        ws.update_cell(k + 1, M_IDX_ROUNDTRIP + 1, "Merged")
                                    except Exception:
                                        logger.exception("Failed to delete or mark secondary merged row.")
                                    # deleted_secondary remains False
                                    break
                    else:
                        try:
                            ws.delete_rows(secondary_row_number)
                        except Exception:
                            try:
                                ws.update_cell(secondary_row_number, M_IDX_ROUNDTRIP + 1, "Merged")
                            except Exception:
                                logger.exception("Failed to delete or mark secondary merged row.")
                except Exception:
                    logger.exception("Failed cleaning up secondary mission row after merge.")

                # Only treat as merged (and notify) when we actually deleted the secondary row
                                # Only treat as merged (and notify) when we actually deleted the secondary row
                # and the primary row has return start and return end recorded (i.e. full roundtrip completed).
                has_return_info = bool(return_start and return_end)
                merged_flag = True if (found_pair or deleted_secondary) and has_return_info else False

                return {"ok": True, "message": f"Mission end recorded and merged for {plate} at {end_ts}", "merged": merged_flag, "driver": driver, "plate": plate, "end_ts": end_ts}
        return {"ok": False, "message": "No open mission found"}
    except Exception as e:
        logger.exception("Failed to update mission end: %s", e)
        return {"ok": False, "message": "Failed to write mission end to sheet: " + str(e)}


AMOUNT_RE = re.compile(r'^\s*(\d+(?:\.\d+)?)\s*$', re.I)
ODO_RE = re.compile(r'^\s*(\d+)(?:\s*km)?\s*$', re.I)
FIN_TYPES = {"odo", "fuel", "parking", "wash", "repair"}

FIN_TYPE_ALIASES = {
    "odo": "odo", "km": "odo", "odometer": "odo",
    "fuel": "fuel", "fu": "fuel", "gas": "fuel", "diesel": "fuel",
    "parking": "parking", "park": "parking", "pk": "parking",
    "wash": "wash", "carwash": "wash",
    "repair": "repair", "rep": "repair", "service": "repair", "maint": "repair",
}

INV_RE = re.compile(r'(?i)\binv[:#\s]*([^\s,;]+)')
PAID_RE = re.compile(r'(?i)\bpaid[:\s]*(yes|y|no|n)\b')

def normalize_fin_type(typ: str) -> Optional[str]:
    if not typ:
        return None
    typ = typ.strip().lower()
    if typ in FIN_TYPES:
        return typ
    if typ in FIN_TYPE_ALIASES:
        return FIN_TYPE_ALIASES[typ]
    for k, v in FIN_TYPE_ALIASES.items():
        if typ.startswith(k):
            return v
    return None

def _find_last_mileage_for_plate(plate: str) -> Optional[int]:
    try:
        ws = open_worksheet(FUEL_TAB)
        vals = ws.get_all_values()
        if not vals:
            return None
        start_idx = 1 if any("plate" in c.lower() for c in vals[0] if c) else 0
        for r in reversed(vals[start_idx:]):
            if len(r) >= 4:
                rp = str(r[0]).strip() if len(r) > 0 else ""
                mileage_cell = str(r[3]).strip() if len(r) > 3 else ""
                if rp == plate and mileage_cell:
                    m = re.search(r'(\d+)', mileage_cell)
                    if m:
                        return int(m.group(1))
        return None
    except Exception:
        logger.exception("Failed to find last mileage for plate")
        return None

def record_finance_odo_fuel(plate: str, mileage: str, fuel_cost: str, by_user: str = "", invoice: str = "", driver_paid: str = "") -> dict:
    try:
        ws = open_worksheet(FUEL_TAB)
        prev_m = _find_last_mileage_for_plate(plate)
        m_int = None
        try:
            m_int = int(re.search(r'(\d+)', str(mileage)).group(1))
        except Exception:
            m_int = None
        delta = ""
        if prev_m is not None and m_int is not None:
            try:
                delta_val = m_int - prev_m
                delta = str(delta_val)
            except Exception:
                delta = ""
        dt = now_str()
        row = [plate, by_user or "Unknown", dt, str(m_int) if m_int is not None else str(mileage), delta, str(fuel_cost), invoice or "", driver_paid or ""]
        ws.append_row(row, value_input_option="USER_ENTERED")
        logger.info("Recorded combined ODO+Fuel: plate=%s mileage=%s delta=%s fuel=%s invoice=%s paid=%s", plate, m_int, delta, fuel_cost, invoice, driver_paid)
        return {"ok": True, "delta": delta, "mileage": m_int, "fuel": fuel_cost}
    except Exception as e:
        logger.exception("Failed to append combined odo+fuel row: %s", e)
        return {"ok": False, "message": str(e)}

def record_parking(plate: str, amount: str, by_user: str = "", notes: str = "") -> dict:
    try:
        ws = open_worksheet(PARKING_TAB)
        dt = now_str()
        row = [plate, by_user or "Unknown", dt, str(amount), notes or ""]
        ws.append_row(row, value_input_option="USER_ENTERED")
        return {"ok": True}
    except Exception as e:
        logger.exception("Failed to record parking: %s", e)
        return {"ok": False, "message": str(e)}

def record_wash(plate: str, amount: str, by_user: str = "", notes: str = "") -> dict:
    try:
        ws = open_worksheet(WASH_TAB)
        dt = now_str()
        row = [plate, by_user or "Unknown", dt, str(amount), notes or ""]
        ws.append_row(row, value_input_option="USER_ENTERED")
        return {"ok": True}
    except Exception as e:
        logger.exception("Failed to record wash: %s", e)
        return {"ok": False, "message": str(e)}

def record_repair(plate: str, amount: str, by_user: str = "", notes: str = "") -> dict:
    try:
        ws = open_worksheet(REPAIR_TAB)
        dt = now_str()
        row = [plate, by_user or "Unknown", dt, str(amount), notes or ""]
        ws.append_row(row, value_input_option="USER_ENTERED")
        return {"ok": True}
    except Exception as e:
        logger.exception("Failed to record repair: %s", e)
        return {"ok": False, "message": str(e)}

BOT_ADMINS = set([u.strip() for u in os.getenv("BOT_ADMINS", BOT_ADMINS_DEFAULT).split(",") if u.strip()])
BOT_ADMINS.add("markpeng1,kmnyy,ClaireRin777")

def build_plate_keyboard(prefix: str, allowed_plates: Optional[List[str]] = None):
    buttons = []
    row = []
    plates = allowed_plates if allowed_plates is not None else PLATES
    for i, plate in enumerate(plates, 1):
        row.append(InlineKeyboardButton(plate, callback_data=f"{prefix}|{plate}"))
        if i % 3 == 0:
            buttons.append(row)
            row = []
    if row:
        buttons.append(row)
    return InlineKeyboardMarkup(buttons)

async def safe_delete_message(bot, chat_id, message_id):
    try:
        await bot.delete_message(chat_id=chat_id, message_id=message_id)
    except Exception:
        pass

async def menu_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        if update.effective_message:
            await update.effective_message.delete()
    except Exception:
        pass
    user_lang = context.user_data.get("lang", DEFAULT_LANG)
    text = t(user_lang, "menu")
    keyboard = [
        [InlineKeyboardButton("Clock In", callback_data="clock_in"), InlineKeyboardButton("Clock Out", callback_data="clock_out")],
        [InlineKeyboardButton("Start trip (select plate)", callback_data="show_start"),
         InlineKeyboardButton("End trip (select plate)", callback_data="show_end")],
        [InlineKeyboardButton("Mission start", callback_data="show_mission_start"),
         InlineKeyboardButton("Mission end", callback_data="show_mission_end")],
        [InlineKeyboardButton("Admin Finance", callback_data="admin_finance"),
         InlineKeyboardButton("Leave", callback_data="leave_menu")],
    ]
    try:
        await context.bot.send_chat_action(chat_id=update.effective_chat.id, action=ChatAction.TYPING)
    except Exception:
        pass
    await update.effective_chat.send_message(text=text, reply_markup=InlineKeyboardMarkup(keyboard))

async def start_trip_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        if update.effective_message:
            await update.effective_message.delete()
    except Exception:
        pass
    user = update.effective_user
    driver_map = get_driver_map()
    allowed = None
    if user and user.username and driver_map.get(user.username):
        allowed = driver_map.get(user.username)
    await update.effective_chat.send_message(t(context.user_data.get("lang", DEFAULT_LANG), "choose_start"), reply_markup=build_plate_keyboard("start", allowed_plates=allowed))

async def end_trip_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        if update.effective_message:
            await update.effective_message.delete()
    except Exception:
        pass
    user = update.effective_user
    driver_map = get_driver_map()
    allowed = None
    if user and user.username and driver_map.get(user.username):
        allowed = driver_map.get(user.username)
    await update.effective_chat.send_message(t(context.user_data.get("lang", DEFAULT_LANG), "choose_end"), reply_markup=build_plate_keyboard("end", allowed_plates=allowed))

async def mission_start_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        if update.effective_message:
            await update.effective_message.delete()
    except Exception:
        pass
    user = update.effective_user
    driver_map = get_driver_map()
    allowed = None
    if user and user.username and driver_map.get(user.username):
        allowed = driver_map.get(user.username)
    await update.effective_chat.send_message(t(context.user_data.get("lang", DEFAULT_LANG), "mission_start_prompt_plate"), reply_markup=build_plate_keyboard("mission_start_plate", allowed_plates=allowed))

async def mission_end_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        if update.effective_message:
            await update.effective_message.delete()
    except Exception:
        pass
    user = update.effective_user
    driver_map = get_driver_map()
    allowed = None
    if user and user.username and driver_map.get(user.username):
        allowed = driver_map.get(user.username)
    await update.effective_chat.send_message(t(context.user_data.get("lang", DEFAULT_LANG), "mission_end_prompt_plate"), reply_markup=build_plate_keyboard("mission_end_plate", allowed_plates=allowed))

async def leave_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        if update.effective_message:
            await update.effective_message.delete()
    except Exception:
        pass
    # Make leave a pending entry but DO NOT send prompt message to avoid duplicates.
    try:
        # Record pending_leave with no external prompt message; callback handlers can edit the UI message instead.
        context.user_data['pending_leave'] = {'prompt_chat': None, 'prompt_msg_id': None, 'origin': {'chat': update.effective_chat.id, 'msg_id': None}}
    except Exception:
        logger.exception('Failed to set pending leave state.')
    return

async def admin_finance_callback_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    user = query.from_user
    username = user.username or (user.first_name or "")
    if username not in BOT_ADMINS:
        try:
            await query.edit_message_text("❌ You are not an admin.")
        except Exception:
            pass
        return
    kb = [
        [InlineKeyboardButton("ODO+Fuel", callback_data="fin_type|odo_fuel"), InlineKeyboardButton("Fuel (solo)", callback_data="fin_type|fuel")],
        [InlineKeyboardButton("Parking", callback_data="fin_type|parking"), InlineKeyboardButton("Wash", callback_data="fin_type|wash")],
        [InlineKeyboardButton("Repair", callback_data="fin_type|repair")],
    ]
    try:
        await query.edit_message_text("Select finance type:", reply_markup=InlineKeyboardMarkup(kb))
    except Exception:
        logger.exception("Failed to prompt finance options.")
        try:
            await query.edit_message_text("Failed to prompt for finance entry.")
        except Exception:
            pass

async def admin_fin_type_selected(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    data = query.data
    parts = data.split("|", 1)
    if len(parts) != 2:
        try:
            await query.edit_message_text("Invalid selection.")
        except Exception:
            pass
        return
    _, typ = parts
    user = query.from_user
    username = user.username or (user.first_name or "")
    if username not in BOT_ADMINS:
        try:
            await query.edit_message_text("❌ Not admin.")
        except Exception:
            pass
        return
    try:
        await query.edit_message_text("Choose plate:", reply_markup=build_plate_keyboard(f"fin_plate|{typ}"))
    except Exception:
        logger.exception("Failed to present plate selection for finance.")

async def process_force_reply(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    text = update.effective_message.text.strip() if update.effective_message and update.effective_message.text else ""
    if not text:
        return

    pending_multi = context.user_data.get("pending_fin_multi")
    if pending_multi:
        ptype = pending_multi.get("type")
        plate = pending_multi.get("plate")
        step = pending_multi.get("step")
        origin = pending_multi.get("origin")
        if ptype == "odo_fuel":
            if step == "km":
                m = ODO_RE.match(text)
                if not m:
                    m2 = re.search(r'(\d+)', text)
                    if m2:
                        km = m2.group(1)
                    else:
                        try:
                            await update.effective_message.delete()
                        except Exception:
                            pass
                        try:
                            await context.bot.send_message(chat_id=user.id, text=t(context.user_data.get("lang", DEFAULT_LANG), "invalid_odo"))
                        except Exception:
                            pass
                        try:
                            if origin:
                                await safe_delete_message(context.bot, origin.get("chat"), origin.get("msg_id"))
                        except Exception:
                            pass
                        context.user_data.pop("pending_fin_multi", None)
                        return
                else:
                    km = m.group(1)
                # We no longer send an "Enter fuel cost" ForceReply message here.
                # Just advance the state; the user should next send fuel amount in chat.
                pending_multi["km"] = km
                pending_multi["step"] = "fuel"
                context.user_data["pending_fin_multi"] = pending_multi
                try:
                    await update.effective_message.delete()
                except Exception:
                    pass
                # Do NOT send a ForceReply prompt; user will provide fuel amount directly.
                return
            elif step == "fuel":
                raw = text
                inv_m = INV_RE.search(raw)
                paid_m = PAID_RE.search(raw)
                invoice = inv_m.group(1) if inv_m else ""
                driver_paid = ""
                if paid_m:
                    v = paid_m.group(1).lower()
                    driver_paid = "yes" if v.startswith("y") else "no"
                am = AMOUNT_RE.match(raw)
                if not am:
                    m2 = re.search(r'(\d+(?:\.\d+)?)', raw)
                    if m2:
                        fuel_amt = m2.group(1)
                    else:
                        try:
                            await update.effective_message.delete()
                        except Exception:
                            pass
                        try:
                            await context.bot.send_message(chat_id=user.id, text=t(context.user_data.get("lang", DEFAULT_LANG), "invalid_amount"))
                        except Exception:
                            pass
                        try:
                            if origin:
                                await safe_delete_message(context.bot, origin.get("chat"), origin.get("msg_id"))
                        except Exception:
                            pass
                        context.user_data.pop("pending_fin_multi", None)
                        return
                else:
                    fuel_amt = am.group(1)
                km = pending_multi.get("km", "")
                try:
                    res = record_finance_odo_fuel(plate, km, fuel_amt, by_user=user.username or "", invoice=invoice, driver_paid=driver_paid)
                except Exception:
                    res = {"ok": False}
                try:
                    await update.effective_message.delete()
                except Exception:
                    pass
                try:
                    pchat = pending_multi.get("prompt_chat")
                    pmsg = pending_multi.get("prompt_msg_id")
                    if pchat and pmsg:
                        await safe_delete_message(context.bot, pchat, pmsg)
                except Exception:
                    pass
                try:
                    if origin:
                        await safe_delete_message(context.bot, origin.get("chat"), origin.get("msg_id"))
                except Exception:
                    pass
                try:
                    delta_txt = res.get("delta", "")
                    m_val = res.get("mileage", km)
                    fuel_val = res.get("fuel", fuel_amt)
                    nowd = _now_dt().strftime(DATE_FMT)
                    # 公共群通知固定显示 "paid by Mark"
                    msg = f"{plate} @ {m_val} km + ${fuel_val} fuel on {nowd} paid by Mark. difference from previous odo is {delta_txt} km."
                    await update.effective_chat.send_message(msg)
                except Exception:
                    logger.exception("Failed to send group notification for odo+fuel")
                try:
                    await context.bot.send_message(chat_id=user.id, text=f"Recorded {plate}: {km}KM and ${fuel_amt} fuel. Delta {delta_txt} km. Invoice={invoice} Paid={driver_paid}")
                except Exception:
                    pass
                context.user_data.pop("pending_fin_multi", None)
                return

    pending_simple = context.user_data.get("pending_fin_simple")
    if pending_simple:
        typ = pending_simple.get("type")
        plate = pending_simple.get("plate")
        origin = pending_simple.get("origin")
        raw = text
        if typ == "odo":
            m = ODO_RE.match(raw)
            if not m:
                m2 = re.search(r'(\d+)', raw)
                if m2:
                    km = m2.group(1)
                else:
                    try:
                        await update.effective_message.delete()
                    except Exception:
                        pass
                    try:
                        await context.bot.send_message(chat_id=user.id, text=t(context.user_data.get("lang", DEFAULT_LANG), "invalid_odo"))
                    except Exception:
                        pass
                    try:
                        if origin:
                            await safe_delete_message(context.bot, origin.get("chat"), origin.get("msg_id"))
                    except Exception:
                        pass
                    context.user_data.pop("pending_fin_simple", None)
                    return
            else:
                km = m.group(1)
            try:
                # odo simple used record_parking by previous mistake in older code; keep behavior unchanged.
                res = record_parking(plate, "", by_user=user.username or "")
            except Exception:
                res = {"ok": False}
            try:
                await update.effective_message.delete()
            except Exception:
                pass
            try:
                if origin:
                    await safe_delete_message(context.bot, origin.get("chat"), origin.get("msg_id"))
            except Exception:
                pass
            try:
                await context.bot.send_message(chat_id=user.id, text=f"Recorded ODO {km}KM for {plate}.")
            except Exception:
                pass
            context.user_data.pop("pending_fin_simple", None)
            return
        else:
            inv_m = INV_RE.search(raw)
            paid_m = PAID_RE.search(raw)
            invoice = inv_m.group(1) if inv_m else ""
            driver_paid = ""
            if paid_m:
                v = paid_m.group(1).lower()
                driver_paid = "yes" if v.startswith("y") else "no"
            am = AMOUNT_RE.match(raw)
            if not am:
                m2 = re.search(r'(\d+(?:\.\d+)?)', raw)
                if m2:
                    amt = m2.group(1)
                else:
                    try:
                        await update.effective_message.delete()
                    except Exception:
                        pass
                    try:
                        await context.bot.send_message(chat_id=user.id, text=t(context.user_data.get("lang", DEFAULT_LANG), "invalid_amount"))
                    except Exception:
                        pass
                    try:
                        if origin:
                            await safe_delete_message(context.bot, origin.get("chat"), origin.get("msg_id"))
                    except Exception:
                        pass
                    context.user_data.pop("pending_fin_simple", None)
                    return
            else:
                amt = am.group(1)
            res = {"ok": False}
            if typ == "parking":
                res = record_parking(plate, amt, by_user=user.username or "")
                # 公共群通知固定显示 "paid by Mark"
                msg_pub = f"{plate} parking fee ${amt} on {today_date_str()} paid by Mark."
            elif typ == "wash":
                res = record_wash(plate, amt, by_user=user.username or "")
                msg_pub = f"{plate} wash fee ${amt} on {today_date_str()} paid by Mark."
            elif typ == "repair":
                res = record_repair(plate, amt, by_user=user.username or "")
                msg_pub = f"{plate} repair fee ${amt} on {today_date_str()} paid by Mark."
            else:
                msg_pub = f"{plate} {typ} recorded ${amt}."
            try:
                await update.effective_message.delete()
            except Exception:
                pass
            try:
                if origin:
                    await safe_delete_message(context.bot, origin.get("chat"), origin.get("msg_id"))
            except Exception:
                pass
            try:
                await update.effective_chat.send_message(msg_pub)
            except Exception:
                logger.exception("Failed to publish finance short message.")
            try:
                await context.bot.send_message(chat_id=user.id, text=f"Recorded {typ} ${amt} for {plate}. Invoice={invoice} Paid={driver_paid}")
            except Exception:
                pass
            context.user_data.pop("pending_fin_simple", None)
            return

    pending_leave = context.user_data.get("pending_leave")
    if pending_leave:
        parts = text.split()
        if len(parts) < 4:
            try:
                await update.effective_message.delete()
            except Exception:
                pass
            try:
                await context.bot.send_message(chat_id=user.id, text="Invalid leave format. Please send: <driver> <-DD> <-DD> <reason> [notes]")
            except Exception:
                pass
            try:
                await safe_delete_message(context.bot, pending_leave.get("prompt_chat"), pending_leave.get("prompt_msg_id"))
            except Exception:
                pass
            context.user_data.pop("pending_leave", None)
            return
        driver = parts[0]
        start = parts[1]
        end = parts[2]
        reason = parts[3]
        notes = " ".join(parts[4:]) if len(parts) > 4 else ""
        try:
            sd = datetime.strptime(start, "%Y-%m-%d")
            ed = datetime.strptime(end, "%Y-%m-%d")
        except Exception:
            try:
                await update.effective_message.delete()
            except Exception:
                pass
            try:
                await context.bot.send_message(chat_id=user.id, text="Invalid dates. Use -DD.")
            except Exception:
                pass
            try:
                await safe_delete_message(context.bot, pending_leave.get("prompt_chat"), pending_leave.get("prompt_msg_id"))
            except Exception:
                pass
            context.user_data.pop("pending_leave", None)
            return
        try:
            ws = open_worksheet(LEAVE_TAB)
            success = await process_leave_entry(ws, driver, start, end, reason, notes, update, context, pending_leave, user)
            if not success:
                return
            try:
                await update.effective_message.delete()
            except Exception:
                pass
            try:
                await safe_delete_message(context.bot, pending_leave.get("prompt_chat"), pending_leave.get("prompt_msg_id"))
            except Exception:
                pass
            # Send confirmation plus a short leave summary for this driver (count of leave entries)
            try:
                records = ws.get_all_records()
                # compute month/year totals by summing existing leave rows for this driver (inclusive) + this entry
                month_total = 0
                year_total = 0
                START_KEYS = ("Start", "Start Date", "Start DateTime", "StartDate")
                END_KEYS = ("End", "End Date", "End DateTime", "EndDate")
                DRIVER_KEYS = ("Driver", "driver", "Username", "Name")
                for r in records:
                    try:
                        drv = None
                        for k in DRIVER_KEYS:
                            if k in r and str(r.get(k, "")).strip():
                                drv = str(r.get(k, "")).strip()
                                break
                        if drv != driver:
                            continue
                        s_val = None
                        e_val = None
                        for k in START_KEYS:
                            if k in r and str(r.get(k, "")).strip():
                                s_val = str(r.get(k, "")).strip()
                                break
                        for k in END_KEYS:
                            if k in r and str(r.get(k, "")).strip():
                                e_val = str(r.get(k, "")).strip()
                                break
                        if not s_val or not e_val:
                            continue
                        s_val = s_val.split()[0]
                        e_val = e_val.split()[0]
                        s2 = datetime.strptime(s_val, "%Y-%m-%d")
                        e2 = datetime.strptime(e_val, "%Y-%m-%d")
                    except Exception:
                        continue
                    try:
                        ld_raw = r.get('Leave Days', r.get('LeaveDays', ''))
                        this_days = int(str(ld_raw).strip()) if str(ld_raw).strip() and str(ld_raw).strip().isdigit() else None
                    except Exception:
                        this_days = None
                    if this_days is None:
                        # fallback: compute excluding weekends and HOLIDAYS
                        this_days = 0
                        curd = s2
                        while curd <= e2:
                            try:
                                is_hol = curd.strftime('%Y-%m-%d') in HOLIDAYS
                            except Exception:
                                is_hol = False
                            if curd.weekday() < 5 and not is_hol:
                                this_days += 1
                            curd += timedelta(days=1)
                    if s2.year == sd.year and s2.month == sd.month:
                        month_total += this_days
                    if s2.year == sd.year:
                        year_total += this_days
                try:
                    # compute leave days for current entry excluding weekends and HOLIDAYS
                    days_this = 0
                    curd = sd
                    while curd <= ed:
                        try:
                            is_hol = curd.strftime('%Y-%m-%d') in HOLIDAYS
                        except Exception:
                            is_hol = False
                        if curd.weekday() < 5 and not is_hol:
                            days_this += 1
                        curd += timedelta(days=1)
                except Exception:
                    days_this = 0
                found_exact = False
                for r in records:
                    try:
                        s_val = next((r[k] for k in START_KEYS if k in r and str(r.get(k, "")).strip()), None)
                        e_val = next((r[k] for k in END_KEYS if k in r and str(r.get(k, "")).strip()), None)
                        dval = next((r[k] for k in DRIVER_KEYS if k in r and str(r.get(k, "")).strip()), None)
                        if dval == driver and s_val.split()[0] == start and e_val.split()[0] == end:
                            found_exact = True
                            break
                    except Exception:
                        continue
                if not found_exact:
                    month_total += days_this
                    year_total += days_this
                month_name = sd.strftime('%B') if isinstance(sd, datetime) else ''
                msg = (
                    f"Driver {driver} {start} to {end} {reason} ({days_this} days)\n"
                    f"Total leave days for {driver}: {month_total} days in {month_name} and {year_total} days in {sd.strftime('%Y')}."
                )
                await context.bot.send_message(chat_id=update.effective_chat.id, text=msg)
            except Exception:
                # fallback: simple confirmation if any error computing totals
                try:
                    await context.bot.send_message(chat_id=update.effective_chat.id, text=f"Driver {driver} {start} to {end} {reason}.")
                except Exception:
                    pass
        except Exception:
            logger.exception("Failed to record leave")
            try:
                await context.bot.send_message(chat_id=user.id, text="Failed to record leave (sheet error).")
            except Exception:
                pass
        context.user_data.pop("pending_leave", None)
        return

    if pending_leave:
        parts = text.split()
        if len(parts) < 4:
            try:
                await update.effective_message.delete()
            except Exception:
                pass
            try:
                await context.bot.send_message(chat_id=user.id, text="Invalid leave format. See prompt.")
            except Exception:
                pass
            try:
                await safe_delete_message(context.bot, pending_leave.get("prompt_chat"), pending_leave.get("prompt_msg_id"))
            except Exception:
                pass
            context.user_data.pop("pending_leave", None)
            return
        driver = parts[0]
        start = parts[1]
        end = parts[2]
        reason = parts[3]
        notes = " ".join(parts[4:]) if len(parts) > 4 else ""
        try:
            sd = datetime.strptime(start, "%Y-%m-%d")
            ed = datetime.strptime(end, "%Y-%m-%d")
        except Exception:
            try:
                await update.effective_message.delete()
            except Exception:
                pass
            try:
                await context.bot.send_message(chat_id=user.id, text="Invalid dates. Use -DD.")
            except Exception:
                pass
            try:
                await safe_delete_message(context.bot, pending_leave.get("prompt_chat"), pending_leave.get("prompt_msg_id"))
            except Exception:
                pass
            context.user_data.pop("pending_leave", None)
            return
        try:
            ws = open_worksheet(LEAVE_TAB)
            success = await process_leave_entry(ws, driver, start, end, reason, notes, update, context, pending_leave, user)
            if not success:
                return
            try:
                await update.effective_message.delete()
            except Exception:
                pass
            try:
                await safe_delete_message(context.bot, pending_leave.get("prompt_chat"), pending_leave.get("prompt_msg_id"))
            except Exception:
                pass
                # Build and send aggregated leave summary (robust fallback path)
                try:
                    records = ws.get_all_records()
                except Exception:
                    records = []
                month_total = 0
                year_total = 0
                START_KEYS = ("Start", "Start Date", "Start DateTime", "StartDate")
                END_KEYS = ("End", "End Date", "End DateTime", "EndDate")
                DRIVER_KEYS = ("Driver", "driver", "Username", "Name")
                for r in records:
                    try:
                        drv = None
                        for k in DRIVER_KEYS:
                            if k in r and str(r.get(k, "")).strip():
                                drv = str(r.get(k, "")).strip()
                                break
                        if drv != driver:
                            continue
                        s_val = next((r[k] for k in START_KEYS if k in r and str(r.get(k, "")).strip()), None)
                        e_val = next((r[k] for k in END_KEYS if k in r and str(r.get(k, "")).strip()), None)
                        if not s_val or not e_val:
                            continue
                        s_val = s_val.split()[0]
                        e_val = e_val.split()[0]
                        s2 = datetime.strptime(s_val, "%Y-%m-%d")
                        e2 = datetime.strptime(e_val, "%Y-%m-%d")
                    except Exception:
                        continue
                    try:
                        ld_raw = r.get('Leave Days', r.get('LeaveDays', ''))
                        this_days = int(str(ld_raw).strip()) if str(ld_raw).strip() and str(ld_raw).strip().isdigit() else None
                    except Exception:
                        this_days = None
                    if this_days is None:
                        # fallback: compute excluding weekends and HOLIDAYS
                        this_days = 0
                        curd = s2
                        while curd <= e2:
                            try:
                                is_hol = curd.strftime('%Y-%m-%d') in HOLIDAYS
                            except Exception:
                                is_hol = False
                            if curd.weekday() < 5 and not is_hol:
                                this_days += 1
                            curd += timedelta(days=1)
                    if s2.year == sd.year and s2.month == sd.month:
                        month_total += this_days
                    if s2.year == sd.year:
                        year_total += this_days
                try:
                    # compute leave days for current entry excluding weekends and HOLIDAYS
                    days_this = 0
                    curd = sd
                    while curd <= ed:
                        try:
                            is_hol = curd.strftime('%Y-%m-%d') in HOLIDAYS
                        except Exception:
                            is_hol = False
                        if curd.weekday() < 5 and not is_hol:
                            days_this += 1
                        curd += timedelta(days=1)
                except Exception:
                    days_this = 0
                # if current entry not in sheet records yet, add it
                found_exact = False
                for r in records:
                    try:
                        s_val = next((r[k] for k in START_KEYS if k in r and str(r.get(k, "")).strip()), None)
                        e_val = next((r[k] for k in END_KEYS if k in r and str(r.get(k, "")).strip()), None)
                        dval = next((r[k] for k in DRIVER_KEYS if k in r and str(r.get(k, "")).strip()), None)
                        if dval == driver and s_val.split()[0] == start and e_val.split()[0] == end:
                            found_exact = True
                            break
                    except Exception:
                        continue
                if not found_exact:
                    month_total += days_this
                    year_total += days_this
                month_name = sd.strftime('%B') if isinstance(sd, datetime) else ''
                msg = (
                    f"Driver {driver} {start} to {end} {reason} ({days_this} days)\n"
                    f"Total leave days for {driver}: {month_total} days in {month_name} and {year_total} days in {sd.strftime('%Y')}."
                )
                await context.bot.send_message(chat_id=update.effective_chat.id, text=msg)
        except Exception:
            logger.exception("Failed to record leave")
            try:
                await context.bot.send_message(chat_id=user.id, text="Failed to record leave (sheet error).")
            except Exception:
                pass
        context.user_data.pop("pending_leave", None)
        return

async def location_or_staff(update: Update, context: ContextTypes.DEFAULT_TYPE):
    return await process_force_reply(update, context)

async def plate_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    # If this callback is for clock buttons, delegate to the clock handler immediately.
    try:
        data_check = (q.data or "").strip()
    except Exception:
        data_check = ""
    if data_check.startswith("clock_"):
        # call dedicated handler to avoid being handled as invalid selection by plate_callback
        return await handle_clock_button(update, context)

    await q.answer()
    data = q.data
    user = q.from_user
    username = user.username or f"{user.first_name or ''} {user.last_name or ''}".strip()
    user_lang = context.user_data.get("lang", DEFAULT_LANG)

    if data == "show_start":
        await q.edit_message_text(t(user_lang, "choose_start"), reply_markup=build_plate_keyboard("start"))
        return
    if data == "show_end":
        await q.edit_message_text(t(user_lang, "choose_end"), reply_markup=build_plate_keyboard("end"))
        return
    if data == "show_mission_start":
        await q.edit_message_text(t(user_lang, "mission_start_prompt_plate"), reply_markup=build_plate_keyboard("mission_start_plate"))
        return
    if data == "show_mission_end":
        await q.edit_message_text(t(user_lang, "mission_end_prompt_plate"), reply_markup=build_plate_keyboard("mission_end_plate"))
        return
    if data == "help":
        await q.edit_message_text(t(user_lang, "help"))
        return

    if data == "admin_finance":
        if (q.from_user.username or "") not in BOT_ADMINS:
            await q.edit_message_text("❌ Admins only.")
            return
        return await admin_finance_callback_handler(update, context)
    if data.startswith("fin_type|"):
        return await admin_fin_type_selected(update, context)

    if data.startswith("fin_plate|"):
        parts = data.split("|", 2)
        if len(parts) < 3:
            await q.edit_message_text("Invalid selection.")
            return
        _, typ, plate = parts
        if (q.from_user.username or "") not in BOT_ADMINS:
            await q.edit_message_text("❌ Admins only.")
            return
        origin_info = {"chat": q.message.chat.id, "msg_id": q.message.message_id, "typ": typ}
        if typ == "odo_fuel":
            # Set pending state but DO NOT send a separate "Enter odometer..." ForceReply message.
            context.user_data["pending_fin_multi"] = {"type": "odo_fuel", "plate": plate, "step": "km", "origin": origin_info}
            try:
                # Edit the callback message minimally to reflect pending state; do not send a new ForceReply prompt.
                await q.edit_message_text(f"Pending ODO+Fuel entry for {plate}. Please send odometer (KM) in chat.")
            except Exception:
                logger.exception("Failed to edit message for pending odo_fuel entry.")
            return
        if typ in ("parking", "wash", "repair", "fuel"):
            # Set pending simple state but DO NOT send a separate "Enter amount..." ForceReply message.
            context.user_data["pending_fin_simple"] = {"type": typ, "plate": plate, "origin": origin_info}
            try:
                await q.edit_message_text(f"Pending {typ} entry for {plate}. Please send amount in chat.")
            except Exception:
                logger.exception("Failed to edit message for pending simple finance entry.")
            return

    if data == "leave_menu":
        # Mark leave pending and edit the callback message to a short prompt (avoid duplicate long messages)
        try:
            context.user_data["pending_leave"] = {"prompt_chat": q.message.chat.id, "prompt_msg_id": q.message.message_id, "origin": {"chat": q.message.chat.id, "msg_id": q.message.message_id}}
            try:
                await q.edit_message_text("Leave entry pending. Please reply in chat with: <driver_username> <-DD> <-DD> <reason> [notes]")
            except Exception:
                pass
        except Exception:
            logger.exception("Failed to prompt leave.")
        return

    # ---------- mission-related handlers ----------
    if data.startswith("mission_start_plate|"):
        parts = data.split("|", 1)
        if len(parts) < 2:
            logger.warning("mission_start_plate callback missing plate: %s", data)
            return
        _, plate = parts
        # show departure choices
        context.user_data["pending_mission"] = {"action": "start", "plate": plate, "driver": username}
        kb = [[InlineKeyboardButton("PP", callback_data=f"mission_depart|PP|{plate}"),
               InlineKeyboardButton("SHV", callback_data=f"mission_depart|SHV|{plate}")]]
        await q.edit_message_text(t(user_lang, "mission_start_prompt_depart"), reply_markup=InlineKeyboardMarkup(kb))
        return

        # Legacy mission end callback from old menus: "mission_end|{plate}"
    if data.startswith("mission_end|") and not data.startswith("mission_end_plate|"):
        try:
            _, legacy_plate = data.split("|", 1)
        except Exception:
            logger.warning("legacy mission_end callback invalid: %s", data)
            return
        # Normalize to new-style callback so existing handler works
        data = f"mission_end_now|{legacy_plate}"

    if data.startswith("mission_end_plate|"):
        parts = data.split("|", 1)
        if len(parts) < 2:
            logger.warning("mission_end_plate callback missing plate: %s", data)
            return
        _, plate = parts
        context.user_data["pending_mission"] = {"action": "end", "plate": plate, "driver": username}
        # allow immediate end (auto arrival) button; callback includes plate for robustness
        kb = [[InlineKeyboardButton("End mission now (auto arrival)", callback_data=f"mission_end_now|{plate}")]]
        await q.edit_message_text(t(user_lang, "mission_end_prompt_plate"), reply_markup=InlineKeyboardMarkup(kb))
        return

    if data.startswith("mission_depart|"):
        parts = data.split("|")
        if len(parts) < 3:
            logger.warning("mission_depart callback missing fields: %s", data)
            return
        _, dep, plate = parts
        context.user_data["pending_mission"] = {"action": "start", "plate": plate, "departure": dep, "driver": username}
        res = start_mission_record(username, plate, dep)
        if res.get("ok"):
            # mission_start_ok template already adjusted to not show the word "plate"
            await q.edit_message_text(t(user_lang, "mission_start_ok", driver=username, plate=plate, dep=dep, ts=res.get("start_ts")))
        else:
            await q.edit_message_text("❌ " + res.get("message", ""))
        return

    # support both "mission_end_now|{plate}" and "mission_end_now"
    if data.startswith("mission_end_now|") or data == "mission_end_now":
        if data == "mission_end_now":
            # try to get plate from pending_mission
            pending = context.user_data.get("pending_mission") or {}
            plate = pending.get("plate")
            if not plate:
                logger.warning("mission_end_now callback without plate and no pending_mission: %s", data)
                return
        else:
            _, plate = data.split("|", 1)

        # permission check
        driver_map = get_driver_map()
        allowed = driver_map.get(username, []) if username else []
        if allowed and plate not in allowed:
            await q.edit_message_text(t(user_lang, "not_allowed", plate=plate))
            return
        try:
            # find last open mission for this driver+plate
            ws = open_worksheet(MISSIONS_TAB)
            vals, start_idx = _missions_get_values_and_data_rows(ws)
            found_idx = None
            found_dep = None
            for i in range(len(vals) - 1, start_idx - 1, -1):
                r = _ensure_row_length(vals[i], M_MANDATORY_COLS)
                rn = str(r[M_IDX_NAME]).strip()
                rp = str(r[M_IDX_PLATE]).strip()
                rend = str(r[M_IDX_END]).strip()
                dep = str(r[M_IDX_DEPART]).strip()
                if rn == username and rp == plate and not rend:
                    found_idx = i
                    found_dep = dep
                    break
            if found_idx is None:
                await q.edit_message_text(t(user_lang, "mission_no_open", plate=plate))
                return

            # arrival automatically opposite of departure
            arrival = "SHV" if found_dep == "PP" else "PP"
            res = end_mission_record(username, plate, arrival)

            if not res.get("ok"):
                await q.edit_message_text("❌ " + res.get("message", ""))
                return

            # Show standardized arrival message
            end_ts = res.get("end_ts") or ""
            try:
                await q.edit_message_text(t(user_lang, "mission_end_ok", driver=username, plate=plate, arr=arrival, ts=end_ts))
            except Exception:
                try:
                    await q.message.chat.send_message(t(user_lang, "mission_end_ok", driver=username, plate=plate, arr=arrival, ts=end_ts))
                    await safe_delete_message(context.bot, q.message.chat.id, q.message.message_id)
                except Exception:
                    pass

            # If merged roundtrip, send summary (uses roundtrip_merged_notify template)
            if res.get("merged"):
                # ==== merged roundtrip handling (clean replacement) ====
                # Ensure mission_cycle loaded
                try:
                    _ensure_mission_cycle_loaded(context.chat_data)
                except Exception:
                    pass
                key_cycle = f"mission_cycle|{username}|{plate}"
                cur_cycle = context.chat_data.get("mission_cycle", {}).get(key_cycle, 0) + 1
                context.chat_data.setdefault("mission_cycle", {})[key_cycle] = cur_cycle
                logger.info("Mission cycle for %s now %d", key_cycle, cur_cycle)
                # persist immediately (best-effort)
                try:
                    save_mission_cycles_to_sheet(context.chat_data.get("mission_cycle", {}))
                except Exception:
                    try:
                        logger.exception("Failed to persist mission_cycle after update")
                    except Exception:
                        pass
                # A merged roundtrip was just detected -> compute and send summary immediately
# roundtrip is complete (outbound + return)
                try:
                    nowdt = _now_dt()
                    month_start = datetime(nowdt.year, nowdt.month, 1)
                    if nowdt.month == 12:
                        month_end = datetime(nowdt.year + 1, 1, 1)
                    else:
                        month_end = datetime(nowdt.year, nowdt.month + 1, 1)
                    counts = count_roundtrips_per_driver_month(month_start, month_end)
                    d_month = counts.get(username, 0)
                    year_start = datetime(nowdt.year, 1, 1)
                    counts_year = count_roundtrips_per_driver_month(year_start, datetime(nowdt.year + 1, 1, 1))
                    d_year = counts_year.get(username, 0)
                    plate_counts_month = 0
                    plate_counts_year = 0
                    try:
                        vals_all, sidx = _missions_get_values_and_data_rows(open_worksheet(MISSIONS_TAB))
                        target_plate = str(plate).strip()
                        year_end = datetime(nowdt.year + 1, 1, 1)
                        for r in vals_all[sidx:]:
                            r = _ensure_row_length(r, M_MANDATORY_COLS)
                            rpl = str(r[M_IDX_PLATE]).strip() if len(r) > M_IDX_PLATE else ""
                            rrt = str(r[M_IDX_ROUNDTRIP]).strip().lower() if len(r) > M_IDX_ROUNDTRIP else ""
                            rstart = str(r[M_IDX_START]).strip() if len(r) > M_IDX_START else ""
                            if not rpl or rpl != target_plate or rrt != "yes":
                                continue
                            sdt = parse_ts(rstart)
                            if not sdt:
                                continue
                            if month_start <= sdt < month_end:
                                plate_counts_month += 1
                            if year_start <= sdt < year_end:
                                plate_counts_year += 1
                    except Exception:
                        try:
                            logger.exception("Failed to compute plate roundtrip counts")
                        except Exception:
                            pass
                    month_label = month_start.strftime("%B")
                    msg = t(user_lang, "roundtrip_merged_notify", driver=username, d_month=d_month, month=month_label, d_year=d_year, year=nowdt.year, plate=plate, p_month=plate_counts_month, p_year=plate_counts_year)
                    
                    try:
                        md_month = 0
                        md_today = 0
                        today_dt = nowdt.date()
                        try:
                            vals_all, sidx = _missions_get_values_and_data_rows(open_worksheet(MISSIONS_TAB))
                            for r in vals_all[sidx:]:
                                r = _ensure_row_length(r, M_MANDATORY_COLS)
                                ruser = str(r[M_IDX_NAME]).strip() if len(r) > M_IDX_NAME else ''
                                if not ruser or ruser != username:
                                    continue
                                rstart = parse_ts(str(r[M_IDX_START]).strip()) if len(r) > M_IDX_START else None
                                rend = parse_ts(str(r[M_IDX_END]).strip()) if len(r) > M_IDX_END else None
                                if not rstart or not rend:
                                    continue
                                m_start = max(rstart.date(), month_start.date())
                                m_end = min(rend.date(), (month_end - timedelta(days=1)).date())
                                if m_start <= m_end:
                                    md_month += (m_end - m_start).days + 1
                                t_start = max(rstart.date(), today_dt)
                                t_end = min(rend.date(), today_dt)
                                if t_start <= t_end:
                                    md_today += (t_end - t_start).days + 1
                        except Exception:
                            try:
                                logger.exception('Failed to compute mission days for notification (safe)')
                            except Exception:
                                pass
                        month_label = month_start.strftime('%B')
                        line1 = t(user_lang, 'roundtrip_merged_notify', driver=username, d_month=d_month, month=month_label, d_year=d_year, year=nowdt.year, plate=plate, p_month=plate_counts_month, p_year=plate_counts_year)
                        # Build line2 and line3 explicitly
                        line2 = f"✅Driver {username} has {md_today} mission day(s) (today), {md_month} mission day(s) in {month_label} {nowdt.year}."
                        line3 = f"✅{plate} completed {plate_counts_month} mission(s) in {month_label} and {plate_counts_year} mission(s) in {nowdt.year}."
                        await q.message.chat.send_message(line1)
                        await q.message.chat.send_message(line2)
                        await q.message.chat.send_message(line3)
                    except Exception:
                        try:
                            logger.exception('Failed to send enhanced merged roundtrip summary (safe)')
                        except Exception:
                            pass
        # record sent time and reset cycle counter
                        try:
                            last_map = context.chat_data.get("last_merge_sent", {})
                            last_map[f"{username}|{plate}"] = nowdt.isoformat()
                            context.chat_data["last_merge_sent"] = last_map
                            context.chat_data["mission_cycle"][key_cycle] = 0
                            try:
                                save_mission_cycles_to_sheet(context.chat_data.get("mission_cycle", {}))
                            except Exception:
                                try:
                                    logger.exception("Failed to persist mission_cycle after reset")
                                except Exception:
                                    pass
                        except Exception:
                            try:
                                logger.exception("Failed to persist last_merge_sent timestamp or reset cycle")
                            except Exception:
                                pass
                    except Exception:
                        try:
                            logger.exception("Failed to send merged roundtrip summary.")
                        except Exception:
                            pass
                except Exception:
                    try:
                        logger.exception("Failed preparing merged roundtrip summary.")
                    except Exception:
                        pass

    # ---------- end mission-related handlers ----------

        except Exception:
            try:
                logger.exception("Closed missing except for mission handler")
            except Exception:
                pass
            pass
    if data.startswith("start|") or data.startswith("end|"):
        try:
            action, plate = data.split("|", 1)
        except Exception:
            await q.edit_message_text("Invalid selection.")
            return
        driver_map = get_driver_map()
        allowed = driver_map.get(username, []) if username else []
        if allowed and plate not in allowed:
            await q.edit_message_text(t(user_lang, "not_allowed", plate=plate))
            return
        if action == "start":
            res = record_start_trip(username, plate)
            if res.get("ok"):
                try:
                    await q.edit_message_text(t(user_lang, "start_ok", driver=username, plate=plate, ts=res.get("ts")))
                except Exception:
                    try:
                        await q.message.chat.send_message(t(user_lang, "start_ok", driver=username, plate=plate, ts=res.get("ts")))
                        await safe_delete_message(context.bot, q.message.chat.id, q.message.message_id)
                    except Exception:
                        pass
            else:
                try:
                    await q.edit_message_text("❌ " + res.get("message", ""))
                except Exception:
                    pass
            return
        elif action == "end":
            res = record_end_trip(username, plate)
            if res.get("ok"):
                ts = res.get("ts")
                dur = res.get("duration") or ""
                nowdt = _now_dt()
                n_today = count_trips_for_day(username, nowdt)
                month_start = datetime(nowdt.year, nowdt.month, 1)
                if nowdt.month == 12:
                    month_end = datetime(nowdt.year + 1, 1, 1)
                else:
                    month_end = datetime(nowdt.year, nowdt.month + 1, 1)
                n_month = count_trips_for_month(username, month_start, month_end)
                # year counts
                year_start = datetime(nowdt.year, 1, 1)
                year_end = datetime(nowdt.year + 1, 1, 1)
                n_year = count_trips_for_month(username, year_start, year_end)
                # plate counts
                p_today = 0
                p_month = 0
                p_year = 0
                try:
                    ws = open_worksheet(RECORDS_TAB)
                    vals = ws.get_all_values()
                    if vals:
                        start_idx = 1 if any("date" in c.lower() for c in vals[0] if c) else 0
                        for r in vals[start_idx:]:
                            if len(r) < COL_START:
                                continue
                            dr = r[1] if len(r) > 1 else ""
                            pl = r[2] if len(r) > 2 else ""
                            s_ts = r[3] if len(r) > 3 else ""
                            e_ts = r[4] if len(r) > 4 else ""
                            if pl != plate:
                                continue
                            if not s_ts or not e_ts:
                                continue
                            sdt = parse_ts(s_ts)
                            if not sdt:
                                continue
                            if sdt.date() == nowdt.date():
                                p_today += 1
                            if month_start <= sdt < month_end:
                                p_month += 1
                            if year_start <= sdt < year_end:
                                p_year += 1
                except Exception:
                    logger.exception("Failed to compute plate trip counts")
                try:
                    await q.edit_message_text(t(user_lang, "end_ok", driver=username, plate=plate, ts=ts))
                except Exception:
                    try:
                        await q.message.chat.send_message(t(user_lang, "end_ok", driver=username, plate=plate, ts=ts))
                        await safe_delete_message(context.bot, q.message.chat.id, q.message.message_id)
                    except Exception:
                        pass
                try:
                    month_label = month_start.strftime("%B")
                    await q.message.chat.send_message(t(user_lang, "trip_summary", driver=username, n_today=n_today, n_month=n_month, month=month_label, n_year=n_year, plate=plate, p_today=p_today, p_month=p_month, p_year=p_year, year=nowdt.year))
                except Exception:
                    logger.exception("Failed to send trip summary")
            else:
                try:
                    await q.edit_message_text("❌ " + res.get("message", ""))
                except Exception:
                    pass
            return


    # Prevent spurious "Invalid selection" after mission_end_now handlers
    if data.startswith("mission_end_now|") or data == "mission_end_now":
        return

    await q.edit_message_text(t(user_lang, "invalid_sel"))

async def lang_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.effective_message
    args = context.args or []

    if not args:
        await reply_privately(update, context, "Usage: /lang en | km")
        return

    lang = args[0].lower()
    if lang not in ("en", "km"):
        await reply_privately(update, context, "Unsupported language. Use: en / km")
        return

    context.user_data["lang"] = lang

    if lang == "en":
        await reply_privately(update, context, "Language set to English.")
    else:
        await reply_privately(update, context, "បានកំណត់ភាសាជាភាសាខ្មែរ。")
    try:
        if update.effective_message:
            await update.effective_message.delete()
    except Exception:
        pass
    args = context.args
    if not args or len(args) < 2:
        return
    mode = args[0].lower()
    if mode == "month":
        try:
            y_m = args[1]
            dt = datetime.strptime(y_m + "-01", "%Y-%m-%d")
            start = datetime(dt.year, dt.month, 1)
            if dt.month == 12:
                end = datetime(dt.year + 1, 1, 1)
            else:
                end = datetime(dt.year, dt.month + 1, 1)
            rows = mission_rows_for_period(start, end)
            counts = count_roundtrips_per_driver_month(start, end)
            if ok:
                pass
            else:
                pass
        except Exception:
            pass
    else:
        pass

async def debug_bot_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    /debug_bot - replies with a self-check report including env vars and current bot commands.
    """
    try:
        await update.effective_message.delete()
    except Exception:
        pass
    user = update.effective_user
    bot_token = os.getenv("BOT_TOKEN")
    sheet_id = os.getenv("SHEET_ID") or os.getenv("GOOGLE_SHEET_NAME") or ""
    google_creds = bool(os.getenv("GOOGLE_CREDS_B64") or os.getenv("GOOGLE_CREDS_BASE64") or os.getenv("GOOGLE_CREDS_PATH"))
    menu_chat = os.getenv("MENU_CHAT_ID") or os.getenv("SUMMARY_CHAT_ID") or ""
    lines = []
    lines.append("**Driver Bot - Debug Report**")
    lines.append(f"Bot token present: {'Yes' if bot_token else 'No'}")
    lines.append(f"SHEET_ID present: {'Yes' if sheet_id else 'No'}")
    lines.append(f"Google creds present: {'Yes' if google_creds else 'No'}")
    lines.append(f"MENU_CHAT_ID / SUMMARY_CHAT_ID: {menu_chat or '(not set)'}")
    # Try to fetch current bot commands
    try:
        if bot_token:
            b = Bot(bot_token)
            cmds = await b.get_my_commands()
            if cmds:
                lines.append("Registered bot commands:")
                for c in cmds:
                    lines.append(f" - /{c.command}: {c.description}")
            else:
                lines.append("Registered bot commands: (none)")
    except Exception as e:
        lines.append("Failed to fetch bot commands: " + str(e))
    # Basic feature checks (handlers presence cannot be introspected easily; we'll report config and tabs)
    try:
        tabs = list(HEADERS_BY_TAB.keys()) if 'HEADERS_BY_TAB' in globals() else []
        lines.append("Known sheet tabs: " + (", ".join(tabs) if tabs else "(none)"))
    except Exception:
        pass
    text = "\
".join(lines)
    # Send in chat (split if too long)
    try:
        await update.effective_chat.send_message(text)
    except Exception:
        try:
            await context.bot.send_message(chat_id=user.id, text=text)
        except Exception:
            pass

AUTO_KEYWORD_PATTERN = r'(?i)\b(start|menu|start trip|end trip|trip|出车|还车|返程)\b'

async def auto_menu_listener(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_chat and update.effective_chat.type in ("group", "supergroup"):
        text = (update.effective_message.text or "").strip()
        if not text:
            return
        if text.startswith("/"):
            try:
                await update.effective_message.delete()
            except Exception:
                pass
            return
        user_lang = context.user_data.get("lang", DEFAULT_LANG)
        keyboard = [
            [InlineKeyboardButton("Start trip", callback_data="show_start"), InlineKeyboardButton("End trip", callback_data="show_end")],
            [InlineKeyboardButton("Open full menu", callback_data="menu_full")],
        ]
        await update.effective_chat.send_message(t(user_lang, "menu"), reply_markup=InlineKeyboardMarkup(keyboard))

async def send_daily_summary_job(context: ContextTypes.DEFAULT_TYPE):
    job_data = context.job.data if hasattr(context.job, "data") else {}
    chat_id = job_data.get("chat_id") or SUMMARY_CHAT_ID
    if not chat_id:
        logger.info("SUMMARY_CHAT_ID not set; skipping daily summary.")
        return
    if SUMMARY_TZ and ZoneInfo:
        try:
            tz = ZoneInfo(SUMMARY_TZ)
            now = datetime.now(tz)
        except Exception:
            now = _now_dt()
    else:
        now = _now_dt()
    yesterday = now.date() - timedelta(days=1)
    date_dt = datetime.combine(yesterday, dtime.min)
    try:
        totals = aggregate_for_period(date_dt, date_dt + timedelta(days=1))
        if not totals:
            await context.bot.send_message(chat_id=chat_id, text=f"No records for {date_dt.strftime(DATE_FMT)}")
        else:
            lines = []
            for plate, minutes in sorted(totals.items()):
                h = minutes // 60
                m = minutes % 60
                lines.append(f"{plate}: {h}h{m}m")
            await context.bot.send_message(chat_id=chat_id, text="\n".join(lines))
    except Exception:
        logger.exception("Failed to send daily summary.")

    if now.day == 1:
        try:
            first_of_this_month = datetime(now.year, now.month, 1)
            prev_month_end = first_of_this_month
            prev_month_start = (first_of_this_month - timedelta(days=1)).replace(day=1)
            rows = mission_rows_for_period(prev_month_start, prev_month_end)
            counts = count_roundtrips_per_driver_month(prev_month_start, prev_month_end)
            if ok:
        except Exception:

def aggregate_for_period(start_dt: datetime, end_dt: datetime) -> Dict[str, int]:
    totals: Dict[str, int] = {}
    try:
        ws = open_worksheet(RECORDS_TAB)
        vals = ws.get_all_values()
        if not vals:
            return totals
        start_idx = 1 if any("date" in c.lower() for c in vals[0] if c) else 0
        for r in vals[start_idx:]:
            if len(r) < COL_DURATION:
                continue
            plate = r[COL_PLATE - 1] if len(r) >= COL_PLATE else ""
            start_ts = r[COL_START - 1] if len(r) >= COL_START else ""
            if not start_ts:
                continue
            s_dt = parse_ts(start_ts)
            if not s_dt:
                continue
            if not (start_dt <= s_dt < end_dt):
                continue
            duration_text = r[COL_DURATION - 1] if len(r) >= COL_DURATION else ""
            minutes = 0
            m = re.match(r'(?:(\d+)h)?(?:(\d+)m)?', duration_text)
            if m:
                hours = int(m.group(1)) if m.group(1) else 0
                mins = int(m.group(2)) if m.group(2) else 0
                minutes = hours * 60 + mins
            totals[plate] = totals.get(plate, 0) + minutes
    except Exception:
        logger.exception("Failed to aggregate for period.")
    return totals

async def setup_menu_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    if (user.username or "") not in BOT_ADMINS:
        await update.effective_chat.send_message("❌ Admins only.")
        return
    try:
        user_lang = context.user_data.get("lang", DEFAULT_LANG)
        keyboard = [
            [InlineKeyboardButton("Start trip", callback_data="show_start"), InlineKeyboardButton("End trip", callback_data="show_end")],
            [InlineKeyboardButton("Mission start", callback_data="show_mission_start"), InlineKeyboardButton("Mission end", callback_data="show_mission_end")],
            [InlineKeyboardButton("Admin Finance", callback_data="admin_finance"), InlineKeyboardButton("Leave", callback_data="leave_menu")],
        ]
        sent = await update.effective_chat.send_message(t(user_lang, "menu"), reply_markup=InlineKeyboardMarkup(keyboard))
        # pin removed per user request: do not pin the menu message
    except Exception:
        logger.exception("Failed to setup menu.")

async def delete_command_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        if update.effective_message:
            await update.effective_message.delete()
    except Exception:
        pass

async def handle_clock_button(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Handle Clock In / Clock Out buttons by delegating to clock_callback_handler,
    so OT records and notifications are generated without breaking existing features.
    """
    try:
        await clock_callback_handler(update, context)
    except Exception:
        logger.exception("Error in handle_clock_button")

def register_ui_handlers(application):
    application.add_handler(CommandHandler("menu", menu_command))
    application.add_handler(CommandHandler(["start_trip", "start"], start_trip_command))
    application.add_handler(CommandHandler(["end_trip", "end"], end_trip_command))
    application.add_handler(CommandHandler("mission_start", mission_start_command))
    application.add_handler(CommandHandler("mission_end", mission_end_command))
    application.add_handler(CommandHandler("leave", leave_command))
    application.add_handler(CommandHandler("setup_menu", setup_menu_command))
    application.add_handler(CommandHandler("lang", lang_command))
    application.add_handler(CommandHandler("ot_report", ot_report_entry))

    application.add_handler(CallbackQueryHandler(ot_report_driver_callback, pattern=r"^OTR_DRIVER:"))

    application.add_handler(CallbackQueryHandler(handle_clock_button, pattern=r"^clock_(in|out)$"))
 
    application.add_handler(CallbackQueryHandler(plate_callback))
    # Clock In/Out buttons handler
    application.add_handler(MessageHandler(filters.REPLY & filters.TEXT & (~filters.COMMAND), process_force_reply))
    application.add_handler(MessageHandler(filters.TEXT & (~filters.COMMAND), location_or_staff))
    application.add_handler(MessageHandler(filters.Regex(AUTO_KEYWORD_PATTERN) & filters.ChatType.GROUPS, auto_menu_listener))
    application.add_handler(MessageHandler(filters.COMMAND, delete_command_message), group=1)
    application.add_handler(CommandHandler("help", lambda u, c: u.message.reply_text(t(c.user_data.get("lang", DEFAULT_LANG), "help"))))

    
    # Debug command for runtime self-check
    application.add_handler(CommandHandler('debug_bot', debug_bot_command))
    async def _set_cmds():
        try:
            await application.bot.set_my_commands([
                BotCommand("start_trip", "Start a trip (select plate)"),
                BotCommand("end_trip", "End a trip (select plate)"),
                BotCommand("menu", "Open trip menu"),
            ])
        except Exception:
            logger.exception("Failed to set bot commands.")

    # Schedule _set_cmds safely using the running event loop if available.
    try:
        import asyncio
        loop = None
        try:
            loop = asyncio.get_running_loop()
        except Exception:
            try:
                loop = asyncio.get_event_loop()
            except Exception:
                loop = None
        if loop and hasattr(loop, "create_task"):
            loop.create_task(_set_cmds())
        else:
            # Fallback: try to call application.create_task if provided by library
            try:
                if hasattr(application, "create_task"):
                    application.create_task(_set_cmds())
            except Exception:
                logger.exception("Could not schedule set_my_commands.")
    except Exception:
        logger.exception("Could not schedule set_my_commands.")

def ensure_env():
    if not BOT_TOKEN:
        raise RuntimeError(t(DEFAULT_LANG, "no_bot_token"))

def schedule_daily_summary(application):
    try:
        if SUMMARY_CHAT_ID:
            if ZoneInfo and SUMMARY_TZ:
                tz = ZoneInfo(SUMMARY_TZ)
            else:
                tz = None
            job_time = dtime(hour=SUMMARY_HOUR, minute=0, second=0)
            application.job_queue.run_daily(send_daily_summary_job, time=job_time, context={"chat_id": SUMMARY_CHAT_ID}, name="daily_summary", tz=tz)
            logger.info("Scheduled daily summary at %02d:00 (%s) to %s", SUMMARY_HOUR, SUMMARY_TZ, SUMMARY_CHAT_ID)
        else:
            logger.info("SUMMARY_CHAT_ID not configured; scheduled jobs disabled.")
    except Exception:
        logger.exception("Failed to schedule daily summary.")

def _delete_telegram_webhook(token: str) -> bool:
    try:
        url = f"https://api.telegram.org/bot{token}/deleteWebhook"
        req = urllib.request.Request(url, method="POST")
        with urllib.request.urlopen(req, timeout=10) as resp:
            data = resp.read().decode("utf-8", errors="ignore")
            if '"ok":true' in data or '"ok": true' in data:
                logger.info("deleteWebhook succeeded or webhook not present.")
                return True
            logger.info("deleteWebhook response: %s", data)
            return True
    except Exception as e:
        logger.exception("Failed to call deleteWebhook: %s", e)
        return False

async def _send_startup_debug(application):
    """
    Send startup debug report to MENU_CHAT_ID or SUMMARY_CHAT_ID if configured.
    """
    chat_id = os.getenv("MENU_CHAT_ID") or os.getenv("SUMMARY_CHAT_ID")
    if not chat_id:
        return
    try:
        bot_token = os.getenv("BOT_TOKEN")
        lines = []
        lines.append("Driver Bot startup debug report:")
        lines.append(f"Bot token present: {'Yes' if bot_token else 'No'}")
        lines.append(f"SHEET_ID present: {'Yes' if (os.getenv('SHEET_ID') or os.getenv('GOOGLE_SHEET_NAME')) else 'No'}")
        lines.append(f"Google creds present: {'Yes' if (os.getenv('GOOGLE_CREDS_B64') or os.getenv('GOOGLE_CREDS_BASE64') or os.getenv('GOOGLE_CREDS_PATH')) else 'No'}")
        # list commands
        try:
            if bot_token:
                b = Bot(bot_token)
                cmds = await b.get_my_commands()
                if cmds:
                    lines.append("Registered commands:")
                    for c in cmds:
                        lines.append(f" - /{c.command}: {c.description}")
        except Exception as e:
            lines.append("Failed to fetch commands: " + str(e))
        text = "\
".join(lines)
        await application.bot.send_message(chat_id=chat_id, text=text)
    except Exception:
        pass


# ===============================
# REPORT HANDLER SELF-CHECK (LTS)
# ===============================
def _report_entry_self_check(application):
    try:
        cmds = []
        for h in application.handlers.get(0, []):
            try:
                if hasattr(h, "command"):
                    cmds.extend(h.command)
            except Exception:
                pass
        expected = [
            "ot_report",
            "ot_monthly_report",
        ]
        for c in expected:
            if c in cmds:
                print(f"[REPORT CHECK] /{c} OK")
            else:
                print(f"[REPORT CHECK] /{c} MISSING")
    except Exception as e:
        print("[REPORT CHECK] failed:", e)
# ===============================


def main():
    check_deployment_requirements()
    ensure_env()

    # --- Set Telegram slash commands on startup (uses direct HTTP API to avoid coroutine issues) ---
    try:
        token_tmp = os.getenv("BOT_TOKEN")
        if token_tmp:
            try:
                # Build command list for Telegram API
                cmds_payload = [
                    {"command": "start", "description": "Show menu"},
                    {"command": "ot_report", "description": "OT report: /ot_report [username] "},
                    {"command": "leave", "description": "Request leave"},
                    {"command": "clock_in", "description": "Clock In"},
                    {"command": "clock_out", "description": "Clock Out"}
                ]
                try:
                    import json, urllib.request
                    url = f"https://api.telegram.org/bot{token_tmp}/setMyCommands"
                    data = json.dumps({ "commands": cmds_payload }).encode("utf-8")
                    req = urllib.request.Request(url, data=data, headers={"Content-Type": "application/json"}, method="POST")
                    with urllib.request.urlopen(req, timeout=10) as resp:
                        resp_text = resp.read().decode("utf-8", errors="ignore")
                        print("Set my commands via HTTP API:", resp_text[:200])
                except Exception as e:
                    print("Warning: failed to set Telegram commands via HTTP API:", e)
            except Exception as e:
                print("Warning: could not prepare setting commands:", e)
    except Exception:
        pass
    # --- end set commands ---

    if LOCAL_TZ and ZoneInfo:
        try:
            ZoneInfo(LOCAL_TZ)
            logger.info("Using LOCAL_TZ=%s", LOCAL_TZ)
        except Exception:
            logger.info("LOCAL_TZ=%s but failed to initialize ZoneInfo; using system time.", LOCAL_TZ)
    else:
        logger.info("LOCAL_TZ not set; using system local time.")

    persistence = None
    try:
        persistence = PicklePersistence(filepath="driver_bot_persistence.pkl")
    except Exception:
        persistence = None

    application = ApplicationBuilder().token(BOT_TOKEN).persistence(persistence).build()
    register_ui_handlers(application)

    # Schedule startup debug report (if MENU_CHAT_ID or SUMMARY_CHAT_ID configured)
    try:
        import asyncio
        loop = None
        try:
            loop = asyncio.get_running_loop()
        except Exception:
            try:
                loop = asyncio.get_event_loop()
            except Exception:
                loop = None
        if loop and hasattr(loop, "create_task"):
            loop.create_task(_send_startup_debug(application))
        else:
            try:
                if hasattr(application, "create_task"):
                    application.create_task(_send_startup_debug(application))
            except Exception:
                pass
    except Exception:
        pass
    schedule_daily_summary(application)

    WEBHOOK_URL = os.getenv("WEBHOOK_URL")
    PORT = int(os.getenv("PORT", "8443"))

    if WEBHOOK_URL:
        logger.info("Starting in webhook mode. WEBHOOK_URL=%s", WEBHOOK_URL)
        try:
            application.run_webhook(
                listen="0.0.0.0",
                port=PORT,
                webhook_url=WEBHOOK_URL,
            )
        except Exception:
            logger.exception("Failed to start webhook mode.")
    else:
        try:
            logger.info("No WEBHOOK_URL set — attempting to delete existing webhook (if any) before polling.")
            ok = _delete_telegram_webhook(BOT_TOKEN)
            if not ok:
                logger.warning("deleteWebhook call returned failure or error; proceeding to polling anyway.")
        except Exception:
            logger.exception("Error while attempting deleteWebhook; proceeding to polling.")
        logger.info("Starting driver-bot polling...")
        try:
            application.run_polling()
        except Exception:
            logger.exception("Polling exited with exception.")

if __name__ == "__main__":
    
    main()
main()
# === In-memory override for mission cycle persistence ===
# We deliberately avoid any Google Sheets I/O for mission_cycle state
# to reduce API usage and prevent OAuth scope issues. The mission
# cycle information is kept only in memory for the lifetime of the
# bot process.

_MISSION_CYCLE_STORE = {}


def load_mission_cycles_from_sheet():
    """Return current in-memory mission cycle mapping.

    This overrides the earlier implementation that read from Google
    Sheets. The return value is a shallow copy so callers can't
    accidentally mutate the internal store without calling the
    save helper.
    """
    return dict(_MISSION_CYCLE_STORE)


def save_mission_cycles_to_sheet(mission_cycles):
    """Update the in-memory mission cycle mapping.

    This overrides the earlier implementation that wrote to Google
    Sheets. It simply keeps everything in process memory.
    """
    _MISSION_CYCLE_STORE.clear()
    _MISSION_CYCLE_STORE.update(mission_cycles or {})





# === BEGIN: OT Summary integration (added) ===

def compute_window_for_time(now_dt: Optional[datetime] = None):
    """Compute OT window start/end.
    Window: 16th 00:00 of month M to 15th 23:59:59 of next month.
    If current time < 16th 04:00 of current month, use previous window.
    Returns (window_start, window_end) as naive datetimes in LOCAL_TZ if available.
    """
    if now_dt is None:
        now_dt = _now_dt()
    year = now_dt.year
    month = now_dt.month
    candidate_start = datetime(year, month, 16, 0, 0, 0)
    if now_dt < (candidate_start + timedelta(hours=4)):
        # use previous month
        if month == 1:
            prev_month = 12
            prev_year = year - 1
        else:
            prev_month = month - 1
            prev_year = year
        window_start = datetime(prev_year, prev_month, 16, 0, 0, 0)
    else:
        window_start = candidate_start
    # window_end is 15th of next month 23:59:59
    if window_start.month == 12:
        next_month = 1
        next_year = window_start.year + 1
    else:
        next_month = window_start.month + 1
        next_year = window_start.year
    window_end = datetime(next_year, next_month, 15, 23, 59, 59)
    return window_start, window_end

def _collect_ot_records_in_window(window_start: datetime, window_end: datetime):
    """Read OT_TAB and return list of records (driver, datetime, action) within window."""
    try:
        ws = open_worksheet(OT_TAB)
        vals = ws.get_all_values()
    except Exception:
        try:
            logger.exception("Failed to open OT_TAB for OT summary collection")
        except Exception:
            pass
        return []
    out = []
    if not vals or len(vals) <= 1:
        return out
    for row in vals[1:]:
        try:
            drv = row[O_IDX_DRIVER] if len(row) > O_IDX_DRIVER else ""
            ts_s = row[O_IDX_TIME] if len(row) > O_IDX_TIME else ""
            act = row[O_IDX_ACTION] if len(row) > O_IDX_ACTION else ""
            if not ts_s:
                continue
            try:
                ts = datetime.strptime(ts_s, "%Y-%m-%d %H:%M:%S")
            except Exception:
                continue
            if window_start <= ts <= window_end:
                out.append({"driver": drv, "timestamp": ts, "event": act})
        except Exception:
            continue
    return out

def compute_driver_ot_hours_from_records(records, window_start, window_end):
    """Aggregate simple worked-hours from IN/OUT pairs per driver within window (hours float)."""
    drivers = {}
    per = {}
    for r in records:
        d = r.get("driver") or "Unknown"
        per.setdefault(d, []).append((r.get("timestamp"), r.get("event")))
    totals = {}
    for drv, events in per.items():
        events.sort(key=lambda x: x[0])
        total = timedelta(0)
        in_time = None
        for ts, ev in events:
            if ev and str(ev).upper().strip() == "IN":
                in_time = ts
            elif ev and str(ev).upper().strip() == "OUT":
                if in_time:
                    total += (ts - in_time)
                    in_time = None
                else:
                    # OUT without IN - skip
                    pass
        if in_time:
            total += (window_end - in_time)
        totals[drv] = round(total.total_seconds() / 3600.0, 2)
    return totals

def ensure_ot_summary_sheet_exists(spreadsheet):
    """Ensure a worksheet titled 'OT Summary' exists; create with headers if missing."""
    title = os.getenv("OT_SUMMARY_TAB") or "OT Summary"
    try:
        ws = spreadsheet.worksheet(title)
    except Exception:
        # create sheet
        try:
            ws = spreadsheet.add_worksheet(title=title, rows=1000, cols=10)
            ws.append_row(["Driver", "Total OT Hours", "Window Start", "Window End"])
        except Exception as e:
            raise
    return ws

def update_ot_summary_sheet(driver_totals: Dict[str, float], window_start: datetime, window_end: datetime):
    """Update or create OT Summary tab with totals. Uses existing gspread client helpers."""
    try:
        gc = _get_gspread_client()
        # prefer explicit sheet name env vars
        sheet_name = os.getenv("GOOGLE_SHEET_NAME") or os.getenv("GOOGLE_SHEET_TAB") or None
        sheet_id = os.getenv("SHEET_ID") or os.getenv("SPREADSHEET_ID") or None
        if sheet_name:
            sh = gc.open(sheet_name)
        elif sheet_id:
            sh = gc.open_by_key(sheet_id)
        else:
            sh = gc.open(GOOGLE_SHEET_NAME)
        ws = None
        try:
            ws = sh.worksheet(os.getenv("OT_SUMMARY_TAB") or "OT Summary")
        except Exception:
            ws = ensure_ot_summary_sheet_exists(sh)
        # prepare rows sorted by driver
        rows = []
        for drv in sorted(driver_totals.keys(), key=lambda s: s or ""):
            rows.append([drv, round(driver_totals[drv], 2), window_start.isoformat(), window_end.isoformat()])
        # clear existing body and write
        try:
            # write header if missing
            vals = ws.get_all_values()
            if not vals or len(vals) == 0:
                ws.append_row(["Driver", "Total OT Hours", "Window Start", "Window End"], value_input_option="USER_ENTERED")
            # clear from A2:D1000 (best-effort)
            try:
                ws.batch_clear(["A2:D1000"])
            except Exception:
                pass
            if rows:
                ws.update("A2:D{}".format(len(rows)+1), rows, value_input_option="USER_ENTERED")
        except Exception:
            # fallback: append rows
            for r in rows:
                try:
                    ws.append_row(r, value_input_option="USER_ENTERED")
                except Exception:
                    try:
                        ws.append_row(r)
                    except Exception:
                        pass
        return True
    except Exception as e:
        try:
            logger.exception("Failed to update OT Summary sheet: %s", e)
        except Exception:
            pass
        return False

async def ot_summary_summary_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Telegram command: /ot_summary_summary [at:ISO] - compute OT totals for current window and update OT Summary tab."""
    args = context.args or []
    at = None
    if args:
        try:
            at = datetime.fromisoformat(args[0])
        except Exception:
            at = None
    now_dt = at or _now_dt()
    window_start, window_end = compute_window_for_time(now_dt)
    # collect records from OT_TAB
    recs = _collect_ot_records_in_window(window_start, window_end)
    driver_totals = compute_driver_ot_hours_from_records(recs, window_start, window_end)
    # attempt to update sheet (non-fatal)
    sheet_result = None
    try:
        ok = update_ot_summary_sheet(driver_totals, window_start, window_end)
        sheet_result = "updated" if ok else "failed"
    except Exception as e:
        sheet_result = f"error: {e}"
    # build reply text
    lines = [f"OT Summary {window_start.date()} → {window_end.date()} ({window_start.year})", ""]
    if driver_totals:
        for drv, hrs in sorted(driver_totals.items(), key=lambda x: x[0]):
            lines.append(f"{drv}\t{hrs:.2f}")
    else:
        lines.append("No records found in window.")
    lines.append("")
    lines.append(f"Sheet result: {sheet_result}")
    text = "\
".join(lines)
    try:
        await update.effective_chat.send_message(text)
    except Exception:
        try:
            await update.message.reply_text(text)
        except Exception:
            pass

# Register command handler if application exists
try:
    application.add_handler(CommandHandler("ot_summary_summary", ot_summary_summary_command))
except Exception:
    pass
# === END: OT Summary integration ===




# === BEGIN: lightweight /chatid command (added) ===
async def chatid_command(update, context):
    """Return the current chat's ID. Safe, non-intrusive addition."""
    try:
        chat = None
        # Prefer effective_chat when available
        if hasattr(update, "effective_chat") and update.effective_chat is not None:
            chat = update.effective_chat
        elif hasattr(update, "message") and update.message and update.message.chat:
            chat = update.message.chat
        elif hasattr(update, "callback_query") and update.callback_query and update.callback_query.message and update.callback_query.message.chat:
            chat = update.callback_query.message.chat

        if not chat:
            # best-effort fallback
            try:
                await update.effective_chat.send_message("Could not determine chat id.")
            except Exception:
                try:
                    await update.message.reply_text("Could not determine chat id.")
                except Exception:
                    pass
            return

        cid = getattr(chat, "id", None)
        title = getattr(chat, "title", None) or getattr(chat, "username", None) or "this chat"
        text = f"Chat ID for {title}: {cid}"
        try:
            await update.effective_chat.send_message(text)
        except Exception:
            try:
                await update.message.reply_text(text)
            except Exception:
                pass
    except Exception as e:
        try:
            await update.effective_chat.send_message(f"Error retrieving chat id: {e}")
        except Exception:
            try:
                await update.message.reply_text(f"Error retrieving chat id: {e}")
            except Exception:
                pass

# Register handler if dispatcher/application exists
try:
    application.add_handler(CommandHandler("chatid", chatid_command))
except Exception:
    try:
        # older style: dispatcher
        dispatcher.add_handler(CommandHandler("chatid", chatid_command))
    except Exception:
        pass
# === END: lightweight /chatid command (added) ===




# === BEGIN: lightweight /chatid command (added) ===
async def chatid_command(update, context):
    """Return the current chat's ID. Safe, non-intrusive addition."""
    try:
        chat = None
        # Prefer effective_chat when available
        if hasattr(update, "effective_chat") and update.effective_chat is not None:
            chat = update.effective_chat
        elif hasattr(update, "message") and update.message and update.message.chat:
            chat = update.message.chat
        elif hasattr(update, "callback_query") and update.callback_query and update.callback_query.message and update.callback_query.message.chat:
            chat = update.callback_query.message.chat

        if not chat:
            # best-effort fallback
            try:
                await update.effective_chat.send_message("Could not determine chat id.")
            except Exception:
                try:
                    await update.message.reply_text("Could not determine chat id.")
                except Exception:
                    pass
            return

        cid = getattr(chat, "id", None)
        title = getattr(chat, "title", None) or getattr(chat, "username", None) or "this chat"
        text = f"Chat ID for {title}: {cid}"
        try:
            await update.effective_chat.send_message(text)
        except Exception:
            try:
                await update.message.reply_text(text)
            except Exception:
                pass
    except Exception as e:
        try:
            await update.effective_chat.send_message(f"Error retrieving chat id: {e}")
        except Exception:
            try:
                await update.message.reply_text(f"Error retrieving chat id: {e}")
            except Exception:
                pass

# Register handler if dispatcher/application exists
try:
    application.add_handler(CommandHandler("chatid", chatid_command))
except Exception:
    try:
        # older style: dispatcher
        dispatcher.add_handler(CommandHandler("chatid", chatid_command))
    except Exception:
        pass
# === END: lightweight /chatid command (added) ===




# === BEGIN: MULTILANG EXTENSION (ADDED) ===
# Provides per-user language persistence and admin overrides using the Bot_State worksheet.
# Adds commands: /setlang, /mylang, /forcelang
# Adds a lightweight sync handler that synchronizes context.user_data['lang'] from persisted store.

SUPPORTED_STORE_PREFIX = "lang:user:"
SUPPORTED_OVERRIDE_PREFIX = "lang:override:"

# In-memory cache to reduce sheet requests (best-effort, not authoritative across processes)
_USER_LANG_CACHE = {}
_OVERRIDE_LANG_CACHE = {}

def _kv_get(key: str) -> str:
    """Get a stored value from Bot_State worksheet by Key column. Returns empty string if missing."""
    try:
        ws = open_bot_state_worksheet()
        records = ws.get_all_records()
        for r in records:
            k = str(r.get("Key") or r.get("key") or "").strip()
            if k == key:
                return str(r.get("Value") or r.get("value") or "")
        return ""
    except Exception:
        try:
            logger.exception("Failed kv_get for %s", key)
        except Exception:
            pass
        return ""

def _kv_set(key: str, value: str) -> bool:
    """Set a key/value pair in Bot_State worksheet. Overwrites existing key if present."""
    try:
        ws = open_bot_state_worksheet()
        records = ws.get_all_records()
        found_row = None
        for idx, r in enumerate(records, start=2):
            k = str(r.get("Key") or r.get("key") or "").strip()
            if k == key:
                found_row = idx
                break
        if found_row:
            ws.update_cell(found_row, 2, str(value))
        else:
            ws.append_row([key, str(value)], value_input_option="USER_ENTERED")
        return True
    except Exception as e:
        try:
            logger.exception("Failed kv_set %s -> %s : %s", key, value, e)
        except Exception:
            pass
        return False

def save_user_lang(username: str, lang: str) -> bool:
    if not username or not lang:
        return False
    key = SUPPORTED_STORE_PREFIX + username
    ok = _kv_set(key, lang)
    if ok:
        _USER_LANG_CACHE[username] = lang
    return ok

def get_user_lang_stored(username: str) -> str:
    if not username:
        return ""
    if username in _USER_LANG_CACHE:
        return _USER_LANG_CACHE[username]
    key = SUPPORTED_STORE_PREFIX + username
    v = _kv_get(key)
    if v:
        _USER_LANG_CACHE[username] = v
    return v or ""

def set_admin_override(username: str, lang: str) -> bool:
    if not username:
        return False
    key = SUPPORTED_OVERRIDE_PREFIX + username
    ok = _kv_set(key, lang)
    if ok:
        _OVERRIDE_LANG_CACHE[username] = lang
    return ok

def get_admin_override(username: str) -> str:
    if not username:
        return ""
    if username in _OVERRIDE_LANG_CACHE:
        return _OVERRIDE_LANG_CACHE[username]
    key = SUPPORTED_OVERRIDE_PREFIX + username
    v = _kv_get(key)
    if v:
        _OVERRIDE_LANG_CACHE[username] = v
    return v or ""

def get_effective_lang_for_username(username: str, context=None) -> str:
    """Resolved language for a username: admin override -> user stored -> context.user_data -> DEFAULT_LANG"""
    if not username:
        return DEFAULT_LANG
    ov = get_admin_override(username)
    if ov:
        return ov.lower()
    st = get_user_lang_stored(username)
    if st:
        return st.lower()
    # fallback to context.user_data if provided (useful when username not yet in sheet)
    if context is not None:
        ctx_lang = context.user_data.get("lang") if isinstance(context, type(context)) or hasattr(context, "user_data") else None
        if ctx_lang:
            return ctx_lang.lower()
    return DEFAULT_LANG

# Redefine t to accept either (user_lang, key, ...) OR (update/context, key, ...) in a best-effort manner.
_old_t = globals().get("t")
def t(user_lang_or_update, key: str, **kwargs) -> str:
    # If first argument looks like an Update or has 'effective_user', try to resolve username
    lang = None
    try:
        if hasattr(user_lang_or_update, "effective_user") or hasattr(user_lang_or_update, "message"):
            # it's likely an Update or Context; prefer to resolve via update + context if provided via kwargs
            update = user_lang_or_update
            ctx = kwargs.pop("_context", None)
            username = None
            try:
                username = update.effective_user.username if update and update.effective_user else None
            except Exception:
                username = None
            if username:
                lang = get_effective_lang_for_username(username, context=ctx)
        else:
            # treat as explicit lang string
            if isinstance(user_lang_or_update, str) and len(user_lang_or_update) <= 3:
                lang = user_lang_or_update.lower()
    except Exception:
        lang = None
    if not lang:
        # fallback: try the old t behavior
        try:
            return _old_t(user_lang_or_update if isinstance(user_lang_or_update, str) else None, key, **kwargs)
        except Exception:
            # last resort
            lang = DEFAULT_LANG
    if lang not in SUPPORTED_LANGS:
        lang = "en"
    # Try to fetch translation from TR; if missing, fall back to English string then format
    txt_template = TR.get(lang, TR.get("en", {})).get(key)
    if txt_template is None:
        txt_template = TR.get("en", {}).get(key, "")
    try:
        return txt_template.format(**kwargs)
    except Exception:
        try:
            return str(txt_template)
        except Exception:
            return ""

# Sync handler: ensure context.user_data['lang'] matches persisted/override when user interacts.
async def sync_user_lang(update, context):
    try:
        user = update.effective_user if hasattr(update, "effective_user") else None
        if not user or not getattr(user, "username", None):
            return
        username = user.username
        eff = get_effective_lang_for_username(username, context=context)
        cur = context.user_data.get("lang")
        if cur != eff:
            context.user_data["lang"] = eff
    except Exception:
        try:
            logger.exception("sync_user_lang failed")
        except Exception:
            pass
    # Do not block other handlers; always return without replying.

# Command: /setlang <lang>
async def setlang_command(update, context):
    try:
        if update.effective_message:
            await update.effective_message.delete()
    except Exception:
        pass
    args = context.args or []
    if not args:
        await update.effective_chat.send_message("Usage: /setlang en|km")
        return
    lang = args[0].lower()
    if lang not in SUPPORTED_LANGS:
        await update.effective_chat.send_message("Supported langs: " + ", ".join(SUPPORTED_LANGS))
        return
    user = update.effective_user
    username = user.username if user else None
    if username:
        ok = save_user_lang(username, lang)
        context.user_data["lang"] = lang
        if ok:
            await update.effective_chat.send_message(t(lang, "lang_set", lang=lang))
        else:
            await update.effective_chat.send_message("Failed to persist language setting.")
    else:
        await update.effective_chat.send_message("Could not determine your username; cannot persist language.")

# Command: /mylang
async def mylang_command(update, context):
    user = update.effective_user
    username = user.username if user else None
    if not username:
        await update.effective_chat.send_message("No username found for your account.")
        return
    eff = get_effective_lang_for_username(username, context=context)
    await update.effective_chat.send_message(f"Your language: {eff}")

# Command: /forcelang <username> <lang>  (admin only)
async def forcelang_command(update, context):
    try:
        if update.effective_message:
            await update.effective_message.delete()
    except Exception:
        pass
    user = update.effective_user
    username = user.username if user else None
    if not username or username not in [u.strip() for u in (os.getenv('BOT_ADMINS_DEFAULT') or BOT_ADMINS_DEFAULT).split(",") if u.strip()]:
        await update.effective_chat.send_message("❌ You are not an admin for this operation.")
        return
    args = context.args or []
    if not args or len(args) < 2:
        await update.effective_chat.send_message("Usage: /forcelang <username> <lang>  (e.g. /forcelang markpeng1 km)")
        return
    target = args[0].strip()
    lang = args[1].lower().strip()
    if lang not in SUPPORTED_LANGS:
        await update.effective_chat.send_message("Supported langs: " + ", ".join(SUPPORTED_LANGS))
        return
    ok = set_admin_override(target, lang)
    if ok:
        await update.effective_chat.send_message(f"Set admin override for {target} → {lang}")
    else:
        await update.effective_chat.send_message("Failed to set admin override.")

# Register handlers if application object exists (best-effort, non-invasive)
try:
    application.add_handler(MessageHandler(filters.ALL, sync_user_lang), group=0)
    application.add_handler(CommandHandler("setlang", setlang_command))
    application.add_handler(CommandHandler("mylang", mylang_command))
    application.add_handler(CommandHandler("forcelang", forcelang_command))
except Exception:
    # If 'application' is not yet defined at import time, registration will be attempted in main()
    pass

# Also attempt to register during main() if register hook available
try:
    def register_multilang_handlers(app):
        try:
            app.add_handler(MessageHandler(filters.ALL, sync_user_lang), group=0)
            app.add_handler(CommandHandler("setlang", setlang_command))
            app.add_handler(CommandHandler("mylang", mylang_command))
            app.add_handler(CommandHandler("forcelang", forcelang_command))
        except Exception:
            pass
    globals().setdefault("register_multilang_handlers", register_multilang_handlers)
except Exception:
    pass

# Ensure Khmer entry exists in TR (placeholder copy of English strings) so user can paste full KH translations later.
if "km" not in TR:
    TR["km"] = {}
for k, v in list(TR.get("en", {}).items()):
    if k not in TR.get("km", {}):
        TR["km"][k] = v  # placeholder: copy English (user will replace with full KH translations)

# === END: MULTILANG EXTENSION ===

# Register handlers
try:
    application.add_handler(CommandHandler("ot_report", ot_report_entry))
except Exception:
    # safe fallback: expose register function
    def register_report_handlers(app):
        try:
            app.add_handler(CommandHandler("ot_report", ot_report_entry))
        except Exception:
            pass
    globals().setdefault("register_report_handlers", register_report_handlers)

# Add minimal Khmer phrases into TR["km"] for report-related messages (user should replace with full colloquial translations).
try:
    TR_k = TR.setdefault("km", {})
    TR_k.setdefault("ot_report_no_sheet", "សូមទោស: មិន​មានសន្លឹក OT នៅក្នុង Google Sheets។")
    TR_k.setdefault("ot_report_no_data", "មិនមានទិន្នន័យ OT ទេ។")
    TR_k.setdefault("ot_report_no_records", "មិនមានកំណត់ត្រា OT ក្នុងកាលបរិច្ឆេទ {start} ដល់ {end}។")
    TR_k.setdefault("ot_report_sent_files", "OT reports generated: {count}")
    TR_k.setdefault("ot_report_failed", "បរាជ័យក្នុងការបង្កើត OT report។")
except Exception:
    pass


# === BEGIN: MULTILANG PERSISTENCE & COMMANDS (ADDED) ===
# Provides per-user language choice (en/km) stored in Bot_State worksheet (Key/Value),
# admin override, and commands /setlang, /mylang, /forcelang.
# This extension is non-invasive: it adds handlers and helper functions only.

SUPPORTED_LANGS = ["en", "km"]
LANG_STORE_PREFIX = "lang:user:"
LANG_OVERRIDE_PREFIX = "lang:override:"

_USER_LANG_CACHE = {}
_OVERRIDE_LANG_CACHE = {}

def _open_bot_state_ws():
    # prefer existing helper open_bot_state_worksheet() if available
    try:
        return open_bot_state_worksheet()
    except Exception:
        try:
            return open_worksheet(BOT_STATE_TAB)
        except Exception:
            # best-effort: try open_worksheet_by_name
            try:
                return open_worksheet_by_name("Bot_State")
            except Exception:
                return None

def _kv_get(key: str) -> str:
    try:
        ws = _open_bot_state_ws()
        if not ws:
            return ""
        records = ws.get_all_records()
        for r in records:
            k = str(r.get("Key") or r.get("key") or "").strip()
            if k == key:
                return str(r.get("Value") or r.get("value") or "")
        return ""
    except Exception:
        try:
            logger.exception("kv_get failed for %s", key)
        except Exception:
            pass
        return ""

def _kv_set(key: str, value: str) -> bool:
    try:
        ws = _open_bot_state_ws()
        if not ws:
            return False
        records = ws.get_all_records()
        found_row = None
        for idx, r in enumerate(records, start=2):
            k = str(r.get("Key") or r.get("key") or "").strip()
            if k == key:
                found_row = idx
                break
        if found_row:
            ws.update_cell(found_row, 2, str(value))
        else:
            ws.append_row([key, str(value)], value_input_option="USER_ENTERED")
        return True
    except Exception:
        try:
            logger.exception("kv_set failed for %s", key)
        except Exception:
            pass
        return False

def save_user_lang(username: str, lang: str) -> bool:
    if not username or not lang:
        return False
    lang = lang.lower()
    if lang not in SUPPORTED_LANGS:
        return False
    key = LANG_STORE_PREFIX + username
    ok = _kv_set(key, lang)
    if ok:
        _USER_LANG_CACHE[username] = lang
    return ok

def get_user_lang_stored(username: str) -> str:
    if not username:
        return ""
    if username in _USER_LANG_CACHE:
        return _USER_LANG_CACHE[username]
    key = LANG_STORE_PREFIX + username
    v = _kv_get(key)
    if v:
        _USER_LANG_CACHE[username] = v
    return v or ""

def set_admin_override(username: str, lang: str) -> bool:
    if not username:
        return False
    lang = lang.lower()
    if lang not in SUPPORTED_LANGS:
        return False
    key = LANG_OVERRIDE_PREFIX + username
    ok = _kv_set(key, lang)
    if ok:
        _OVERRIDE_LANG_CACHE[username] = lang
    return ok

def get_admin_override(username: str) -> str:
    if not username:
        return ""
    if username in _OVERRIDE_LANG_CACHE:
        return _OVERRIDE_LANG_CACHE[username]
    key = LANG_OVERRIDE_PREFIX + username
    v = _kv_get(key)
    if v:
        _OVERRIDE_LANG_CACHE[username] = v
    return v or ""

def resolve_effective_lang(username: str, context=None) -> str:
    if not username:
        return DEFAULT_LANG if 'DEFAULT_LANG' in globals() else "en"
    ov = get_admin_override(username)
    if ov:
        return ov.lower()
    st = get_user_lang_stored(username)
    if st:
        return st.lower()
    # fallback to context.user_data if provided
    try:
        if context and hasattr(context, "user_data"):
            ctx_lang = context.user_data.get("lang")
            if ctx_lang:
                return ctx_lang.lower()
    except Exception:
        pass
    return DEFAULT_LANG if 'DEFAULT_LANG' in globals() else "en"

# Wrap existing t() to accept update/context or explicit lang
_old_t = globals().get("t")
def t(user_lang_or_update, key: str, **kwargs) -> str:
    # Determine language
    lang = None
    try:
        if hasattr(user_lang_or_update, "effective_user") or hasattr(user_lang_or_update, "message"):
            update = user_lang_or_update
            ctx = kwargs.pop("_context", None)
            username = None
            try:
                username = update.effective_user.username if update and update.effective_user else None
            except Exception:
                username = None
            if username:
                lang = resolve_effective_lang(username, context=ctx)
        elif isinstance(user_lang_or_update, str) and len(user_lang_or_update) <= 3:
            lang = user_lang_or_update.lower()
        else:
            # fallback to default
            lang = DEFAULT_LANG if 'DEFAULT_LANG' in globals() else "en"
    except Exception:
        lang = DEFAULT_LANG if 'DEFAULT_LANG' in globals() else "en"
    if lang not in SUPPORTED_LANGS:
        lang = "en"
    # Use TR dict if present
    try:
        tr = TR.get(lang, TR.get("en", {}))
        txt_template = tr.get(key, TR.get("en", {}).get(key, ""))
        return txt_template.format(**kwargs)
    except Exception:
        try:
            return str(TR.get("en", {}).get(key, "")).format(**kwargs)
        except Exception:
            return ""

# Sync handler to keep context.user_data['lang'] updated when users interact
async def _sync_user_lang(update, context):
    try:
        user = update.effective_user if hasattr(update, "effective_user") else None
        if not user or not getattr(user, "username", None):
            return
        username = user.username
        eff = resolve_effective_lang(username, context=context)
        cur = context.user_data.get("lang")
        if cur != eff:
            context.user_data["lang"] = eff
    except Exception:
        try:
            logger.exception("sync_user_lang failed")
        except Exception:
            pass
    # do not send messages

# Command handlers
async def cmd_setlang(update, context):
    try:
        if update.effective_message:
            await update.effective_message.delete()
    except Exception:
        pass
    args = context.args or []
    if not args:
        await update.effective_chat.send_message("Usage: /setlang en|km")
        return
    lang = args[0].lower()
    if lang not in SUPPORTED_LANGS:
        await update.effective_chat.send_message("Supported: " + ", ".join(SUPPORTED_LANGS))
        return
    user = update.effective_user
    uname = user.username if user else None
    if not uname:
        await update.effective_chat.send_message("Cannot determine username; cannot persist language.")
        return
    ok = save_user_lang(uname, lang)
    context.user_data["lang"] = lang
    if ok:
        await update.effective_chat.send_message(t(lang, "lang_set", lang=lang))
    else:
        await update.effective_chat.send_message("Failed to persist language setting.")

async def cmd_mylang(update, context):
    user = update.effective_user
    uname = user.username if user else None
    if not uname:
        await update.effective_chat.send_message("No username found.")
        return
    eff = resolve_effective_lang(uname, context=context)
    await update.effective_chat.send_message(f"Your language: {eff}")

async def cmd_forcelang(update, context):
    try:
        if update.effective_message:
            await update.effective_message.delete()
    except Exception:
        pass
    user = update.effective_user
    uname = user.username if user else None
    # check admin: prefer BOT_ADMINS env var then BOT_ADMINS_DEFAULT global
    admins = []
    try:
        if os.getenv("BOT_ADMINS"):
            admins = [x.strip() for x in os.getenv("BOT_ADMINS").split(",") if x.strip()]
        elif 'BOT_ADMINS_DEFAULT' in globals():
            admins = [x.strip() for x in BOT_ADMINS_DEFAULT.split(",") if x.strip()]
    except Exception:
        admins = []
    if not uname or uname not in admins:
        await update.effective_chat.send_message("❌ You are not an admin for this operation.")
        return
    args = context.args or []
    if len(args) < 2:
        await update.effective_chat.send_message("Usage: /forcelang <username> <en|km>")
        return
    target = args[0].strip()
    lang = args[1].lower().strip()
    if lang not in SUPPORTED_LANGS:
        await update.effective_chat.send_message("Supported: " + ", ".join(SUPPORTED_LANGS))
        return
    ok = set_admin_override(target, lang)
    if ok:
        await update.effective_chat.send_message(f"Set admin override for {target} → {lang}")
    else:
        await update.effective_chat.send_message("Failed to set admin override.")

# Register handlers if application object is present
try:
    application.add_handler(CommandHandler("setlang", cmd_setlang))
    application.add_handler(CommandHandler("mylang", cmd_mylang))
    application.add_handler(CommandHandler("forcelang", cmd_forcelang))
    application.add_handler(MessageHandler(filters.ALL, _sync_user_lang), group=0)
except Exception:
    # expose a function to register later
    def register_multilang(app):
        try:
            app.add_handler(CommandHandler("setlang", cmd_setlang))
            app.add_handler(CommandHandler("mylang", cmd_mylang))
            app.add_handler(CommandHandler("forcelang", cmd_forcelang))
            app.add_handler(MessageHandler(filters.ALL, _sync_user_lang), group=0)
        except Exception:
            pass
    globals().setdefault("register_multilang", register_multilang)

# Ensure TR has km entry – if missing, copy en (user can replace with more natural KH later)
try:
    if "TR" in globals() and isinstance(TR, dict):
        if "km" not in TR:
            TR["km"] = {}
        for k, v in TR.get("en", {}).items():
            if k not in TR["km"]:
                # placeholder: copy English; earlier we may have partial translations; do not overwrite existing entries
                if not TR["km"].get(k):
                    TR["km"][k] = v
except Exception:
    pass

# === END MULTILANG EXTENSION ===


# Auto-register bot commands (so they appear in Telegram UI)
def _register_bot_commands(app):
    try:
        from telegram import BotCommand
        cmds = [
            BotCommand("setlang", "Set your language (en/km)"),
            BotCommand("mylang", "Show your current language"),
            BotCommand("forcelang", "Admin: force language for a user"),
        ]
        try:
            # PTB v20: application.bot.set_my_commands exists
            app.bot.set_my_commands(cmds)
        except Exception:
            try:
                app.set_my_commands(cmds)
            except Exception:
                pass
    except Exception:
        try:
            logger.exception("Failed to register bot commands")
        except Exception:
            pass

# If application is present, register commands now
try:
    if 'application' in globals() and application is not None:
        _register_bot_commands(application)
except Exception:
    pass

# Also expose helper for explicit call
globals().setdefault("register_bot_commands", _register_bot_commands)



# ===============================
# === C FINAL SAFE ADDON
# ===============================

from telegram import InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import CommandHandler, CallbackQueryHandler

# === Unified private reply helper (A-approved) ===
async def reply_private(update, context, text, **kwargs):
    user_id = update.effective_user.id
    await context.bot.send_message(
        chat_id=user_id,
        text=text,
        **kwargs
    )


# ---- Language command ----
async def lang_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.effective_message
    args = context.args or []

    if not args:
        await reply_privately(update, context, "Usage: /lang en | km")
        return

    lang = args[0].lower()
    if lang not in ("en", "km"):
        await reply_privately(update, context, "Unsupported language. Use: en / km")
        return

    context.user_data["lang"] = lang

    if lang == "en":
        await reply_privately(update, context, "Language set to English.")
    else:
        await reply_privately(update, context, "បានកំណត់ភាសាជាភាសាខ្មែរ。")
async def reports_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    kb = [
        [InlineKeyboardButton("OT Report", callback_data="rep_ot")],
        [InlineKeyboardButton("OT Monthly Report", callback_data="rep_otm")],
        [
            InlineKeyboardButton("English", callback_data="lang_en"),
            InlineKeyboardButton("Khmer", callback_data="lang_km"),
        ],
    ]
    await update.effective_message.reply_text(
        "Reports & Language",
        reply_markup=InlineKeyboardMarkup(kb)
    )

# ---- Callback handler ----
async def c_safe_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    data = q.data

    if data == "lang_en":
        context.user_data["lang"] = "en"
        await q.edit_message_text("Language set to English")
    elif data == "lang_km":
        context.user_data["lang"] = "km"
        await q.edit_message_text("Language set to Khmer")
    elif data == "rep_ot":
        await q.edit_message_text("Use: /ot_report <username> ")
    elif data == "rep_otm":
        await q.edit_message_text("Use: /ot_monthly_report  <username>")
    elif data == "rep_mm":

# ---- Register handlers ----
try:
    application.add_handler(CommandHandler("lang", lang_command))
    application.add_handler(CommandHandler("reports", reports_menu))
    application.add_handler(CallbackQueryHandler(c_safe_callback, pattern="^(lang_|rep_)"))
except Exception:
    pass

# === END C FINAL SAFE ADDON ===
application.add_handler(CallbackQueryHandler(ot_report_driver_callback, pattern="^OTR_DRIVER:"))



# ======================
# OT REPORT PATCH V7
# ======================
import io, csv

def _calc_hours(row, idx_morning, idx_evening, idx_start, idx_end):
    try:
        m = float(row[idx_morning] or 0)
        e = float(row[idx_evening] or 0)
        if m + e > 0:
            return round(m + e, 2)
        s = datetime.fromisoformat(row[idx_start])
        en = datetime.fromisoformat(row[idx_end])
        return round((en - s).total_seconds() / 3600, 2)
    except Exception:
        return 0.0

async def ot_report_entry(update, context):
    driver_map = get_driver_map()
    drivers = sorted(driver_map.keys())
    if not drivers:
        await reply_private(update, context, "❌ No drivers found.")
        return
    keyboard = [[InlineKeyboardButton(d, callback_data=f"OTR_DRIVER:{d}")] for d in drivers]
    await reply_private(update, context, "Select driver:", reply_markup=InlineKeyboardMarkup(keyboard))

async def ot_report_driver_callback(update, context):
    query = update.callback_query
    await query.answer()
    try:
        await query.edit_message_reply_markup(reply_markup=None)
    except Exception:
        pass

    driver = query.data.split(":", 1)[1]
    ws = open_worksheet(OT_RECORD_TAB)
    rows = ws.get_all_values()
    if len(rows) < 2:
        await context.bot.send_message(query.from_user.id, "❌ No OT records.")
        return

    header, data = rows[0], rows[1:]
    idx_name = header.index("Name")
    idx_type = header.index("Type")
    idx_start = header.index("Start Date")
    idx_end = header.index("End Date")
    idx_morning = header.index("Morning OT")
    idx_evening = header.index("Evening OT")

    now = _now_dt()
    start_window = now.replace(day=16, hour=4, minute=0, second=0, microsecond=0)
    if now < start_window:
        start_window = (start_window - timedelta(days=31)).replace(day=16)
    end_window = (start_window + timedelta(days=31)).replace(day=16, hour=4)

    ot150, ot200 = [], []
    t150 = t200 = 0.0

    for r in data:
        if r[idx_name].strip() != driver:
            continue
        try:
            sdt = datetime.fromisoformat(r[idx_start])
            if not (start_window <= sdt < end_window):
                continue
        except Exception:
            continue

        h = _calc_hours(r, idx_morning, idx_evening, idx_start, idx_end)
        if h <= 0:
            continue

        row = [r[idx_start], r[idx_end], f"{h:.2f}"]
        if r[idx_type] == "150%":
            ot150.append(row); t150 += h
        elif r[idx_type] == "200%":
            ot200.append(row); t200 += h

    ot150.sort(key=lambda x: x[0])
    ot200.sort(key=lambda x: x[0])

    out = io.StringIO()
    w = csv.writer(out)
    w.writerow(["Driver", driver])
    w.writerow(["Period", f"{start_window} → {end_window}"])
    w.writerow([])

    if ot150:
        w.writerow(["150% OT"]); w.writerow(["Start","End","Hours"])
        w.writerows(ot150); w.writerow(["Subtotal","","%.2f"%t150]); w.writerow([])
    if ot200:
        w.writerow(["200% OT"]); w.writerow(["Start","End","Hours"])
        w.writerows(ot200); w.writerow(["Subtotal","","%.2f"%t200]); w.writerow([])

    w.writerow(["GRAND TOTAL","","%.2f"%(t150+t200)])

    bio = io.BytesIO(out.getvalue().encode("utf-8"))
    bio.name = f"OT_Report_{driver}.csv"
    await context.bot.send_document(query.from_user.id, bio, caption=f"OT report for {driver}")

# === CLOCK HANDLER START ===
def _auto_close_previous_in(ws, driver, new_in_time):
    rows = ws.get_all_values()
    if len(rows) < 2:
        return
    header = rows[0]
    data = rows[1:]
    idx_driver = header.index("Name")
    idx_action = header.index("Action")
    idx_time = header.index("Time")
    idx_end = header.index("End Time") if "End Time" in header else None

    for i in range(len(data)-1, -1, -1):
        r = data[i]
        if r[idx_driver] == driver and r[idx_action] == "IN":
            last_in = datetime.strptime(r[idx_time], "%Y-%m-%d %H:%M:%S")
            auto_out = last_in.replace(hour=4, minute=0, second=0)
            if auto_out <= last_in:
                auto_out += timedelta(days=1)
            ws.update_cell(i+2, idx_action+1, "OUT")
            ws.update_cell(i+2, idx_time+1, auto_out.strftime("%Y-%m-%d %H:%M:%S"))
            return

# === CLOCK HANDLER END ===


# =========================
# Mission Report V9 (M-03)
# Period calculation only (Natural Month)
# =========================

def get_mission_period(year: int, month: int):
    """
    Natural month period:
    From: YYYY-MM-01 00:00:00
    To:   YYYY-MM-last 23:59:59
    """
    period_start = datetime(year, month, 1, 0, 0, 0)
    if month == 12:
        next_month = datetime(year + 1, 1, 1, 0, 0, 0)
    else:
        next_month = datetime(year, month + 1, 1, 0, 0, 0)
    period_end = next_month - timedelta(seconds=1)
    return period_start, period_end




# =========================
# Mission Report V9 (M-06)
# Mission Days calculation (PURE FUNCTION)
# =========================

def compute_mission_days(start_dt, end_dt):
    """
    Compute mission days using date-based rule only.

    Rule:
        mission_days = (end_date - start_date) + 1

    Notes:
        - Only dates are considered (ignore time)
        - Same start/end date => 1 day
        - This function is pure and has no side effects
    """
    start_date = start_dt.date()
    end_date = end_dt.date()
    return (end_date - start_date).days + 1


# =========================
# Mission Report V9 (M-07)
# Mission Type determination (PURE FUNCTION)
# =========================

def determine_mission_type(start_city: str, end_city: str):
    """
    Determine mission type based on CLOSED round-trip endpoints only.

    Valid closed loops:
      - SHV -> PP -> SHV  => 'PP Mission'
      - PP  -> SHV -> PP  => 'SHV Mission'

    Input semantics:
      - start_city: round-trip start city
      - end_city:   round-trip end city
      - This function MUST be called only after a round-trip is confirmed.

    Return:
      - 'PP Mission'
      - 'SHV Mission'
      - None (if not a valid closed loop)
    """
    if not start_city or not end_city:
        return None

    sc = start_city.strip().upper()
    ec = end_city.strip().upper()

    if sc == ec == "SHV":
        return "PP Mission"
    if sc == ec == "PP":
        return "SHV Mission"

    return None


# =========================
# Mission Report V9 (M-15)
# Mission data read (READ-ONLY, STRICT)
# =========================

def read_mission_records(period_start, period_end, driver):
    """
    Read mission records from MISSIONS_TAB (READ-ONLY, STRICT).

    M-15 constraints:
      - READ ONLY
      - Real sheet I/O allowed
      - Filter ONLY by driver name
      - NO autofix
      - NO pairing
      - NO datetime parsing / NO interpretation
      - NO data reshaping
      - Return RAW rows (list of lists), header excluded
    """
    ws = open_worksheet(MISSIONS_TAB)
    rows = ws.get_all_values()

    if not rows or len(rows) < 2:
        return []

    header = rows[0]

    # locate driver/name column (strict, no reshaping)
    try:
        idx_name = header.index("Name")
    except ValueError:
        # schema error: return empty but do not transform data
        return []

    results = []

    for r in rows[1:]:
        if len(r) <= idx_name:
            continue
        if r[idx_name].strip() != driver:
            continue

        # return raw row as-is
        results.append(r)

    return results


# =========================
# Mission Report V9 (M-18)
# Mission table schema freeze (HEADER → INDEX)
# =========================

# Canonical Mission sheet headers (ORDER IS FROZEN)
MISSION_HEADERS = [
    "GUID",           # 0
    "No.",            # 1
    "Name",           # 2
    "Plate",          # 3
    "Start Date",     # 4
    "End Date",       # 5
    "Departure",      # 6
    "Arrival",        # 7
    "Staff Name",     # 8
    "Roundtrip",      # 9
    "Return Start",   # 10
    "Return End",     # 11
]

# Header → index mapping (FROZEN)
M_IDX_GUID = 0
M_IDX_NO = 1
M_IDX_NAME = 2
M_IDX_PLATE = 3
M_IDX_START = 4
M_IDX_END = 5
M_IDX_DEPARTURE = 6
M_IDX_ARRIVAL = 7
M_IDX_STAFF = 8
M_IDX_ROUNDTRIP = 9
M_IDX_RETURN_START = 10
M_IDX_RETURN_END = 11

M_MANDATORY_COLS = len(MISSION_HEADERS)


def validate_mission_header(header_row):
    """
    Validate Mission sheet header against frozen schema.

    M-18 constraints:
      - NO mutation
      - NO auto-fix
      - Strict equality check (order + name)
    """
    if not header_row:
        return False
    normalized = [str(c).strip() for c in header_row]
    return normalized[:M_MANDATORY_COLS] == MISSION_HEADERS

# =========================
# Mission Report V9 (M-21)
# Mission Days calculation (REAL, PURE)
# =========================

from datetime import datetime

def apply_mission_days(round_trips):
    """
    Apply mission_days to paired round-trips.

    M-21 constraints (STRICT):
      - Input: output of M-20 (paired round-trips)
      - Do NOT modify input objects
      - Do NOT determine mission type
      - Do NOT adjust start/end times
      - Use date-based rule ONLY:
          mission_days = (end_date - start_date) + 1
      - Ignore hours/minutes/seconds

    Expected keys in round_trips:
      - start_dt (ISO string)
      - end_dt   (ISO string)

    Output:
      - New list with 'mission_days' injected
    """
    if not round_trips:
        return []

    enriched = []

    for rt in round_trips:
        start_raw = rt.get("start_dt")
        end_raw = rt.get("end_dt")

        if not start_raw or not end_raw:
            # skip invalid pair silently
            continue

        try:
            start_date = datetime.fromisoformat(start_raw).date()
            end_date = datetime.fromisoformat(end_raw).date()
        except Exception:
            continue

        mission_days = (end_date - start_date).days + 1

        enriched.append({
            **rt,
            "mission_days": mission_days,
        })

    return enriched

# =========================
# Mission Report V9 (M-22)
# Mission Type determination (REAL, PURE)
# =========================

def apply_mission_type(round_trips):
    """
    Determine mission type for each round-trip.

    M-22 constraints (STRICT):
      - Input: output of M-21 (round-trips with mission_days)
      - Do NOT modify input objects
      - Do NOT recalculate mission_days
      - Do NOT adjust start/end times
      - Do NOT guess based on button clicks

    Rule (FROZEN):
      - Only look at CLOSED round-trip endpoints
      - If start_city == end_city == 'SHV' -> 'PP Mission'
      - If start_city == end_city == 'PP'  -> 'SHV Mission'
      - Else -> None (invalid / skip later)

    Expected keys:
      - start_city
      - end_city

    Output:
      - New list with 'type' injected
    """
    if not round_trips:
        return []

    enriched = []

    for rt in round_trips:
        start_city = rt.get("start_city")
        end_city = rt.get("end_city")

        mission_type = None
        if start_city and end_city:
            sc = str(start_city).strip().upper()
            ec = str(end_city).strip().upper()

            if sc == ec == "SHV":
                mission_type = "PP Mission"
            elif sc == ec == "PP":
                mission_type = "SHV Mission"

        enriched.append({
            **rt,
            "type": mission_type,
        })

    return enriched

# =========================
# Mission Report V9 (M-23)
# CSV rows fill + TOTAL (OT V9 aligned)
# =========================

import io
import csv

def export_mission_csv(driver, period, missions):
    """
    Export Mission Report CSV with rows and TOTAL.

    M-23 constraints (STRICT):
      - Input: output of M-22 (missions with mission_days + type)
      - Do NOT modify mission objects
      - Do NOT sort missions here (sorting upstream)
      - Do NOT send file
      - OT V9 CSV structure alignment REQUIRED

    CSV Structure (FROZEN):
      Driver,<driver>
      Period,<period_start> → <period_end>

      Plate,Start Date,End Date,Mission Day(s),From,To,Type
      <rows...>

      TOTAL MISSION DAY(s),<sum>
    """
    output = io.StringIO()
    writer = csv.writer(output)

    period_start, period_end = period

    # Header block (OT V9 aligned)
    writer.writerow(["Driver", driver])
    writer.writerow(["Period", f"{period_start} → {period_end}"])
    writer.writerow([])

    # Table header
    writer.writerow([
        "Plate",
        "Start Date",
        "End Date",
        "Mission Day(s)",
        "From",
        "To",
        "Type",
    ])

    total_days = 0

    for m in missions:
        days = m.get("mission_days")
        if days:
            total_days += days

        writer.writerow([
            m.get("plate"),
            m.get("start_dt"),
            m.get("end_dt"),
            days,
            m.get("from"),
            m.get("to"),
            m.get("type"),
        ])

    writer.writerow([])
    writer.writerow(["TOTAL MISSION DAY(s)", total_days])

    return output.getvalue()

# =========================
# Mission Report V9 (M-24)
# Callback full wiring (OT V9 aligned)
# =========================

async def mission_report_driver_callback(update, context):
    q = update.callback_query
    await q.answer()

    # OT V9：确保私聊路径
    await reply_private(update, context)

    # OT V9：防抖（按钮点一次即禁用）
    if q.message and q.message.reply_markup:
        await q.message.edit_reply_markup(reply_markup=None)

    # Driver
    driver = q.data.split(":", 1)[1]

    # ---------- Period（M-03） ----------
    period_start, period_end = get_mission_period_from_context(context)
    period = (period_start, period_end)

    # ---------- Read records（M-15） ----------
    records = read_mission_records(period_start, period_end, driver)

    # ---------- Auto-fix（M-19，仅 23:59:59） ----------
    # 使用 report_time 作为唯一时间基准
    report_time = period_end
    records = autofix_mission_records(records, driver, report_time)

    # ---------- Pair round-trips（M-20） ----------
    round_trips = pair_mission_round_trips(records)

    # ---------- Mission Days（M-21） ----------
    with_days = apply_mission_days(round_trips)

    # ---------- Mission Type（M-22） ----------
    missions = apply_mission_type(with_days)

    # ---------- CSV Export（M-23） ----------
    csv_content = export_mission_csv(driver, period, missions)

    # ---------- Send CSV（OT V9 私聊方式） ----------
    bio = io.BytesIO(csv_content.encode("utf-8"))
    bio.name = f"Mission_Report_{driver}_{period_start:%Y-%m}.csv"

    await update.effective_chat.send_document(document=bio, caption=f"Mission report for {driver}")

# =========================
# Mission Report V9 (M-25)
# Clock-out integration (OPTIONAL, SAFE, BACKWARD-COMPATIBLE)
# =========================

from copy import deepcopy
from datetime import datetime, timedelta, time

def autofix_mission_records(records, driver, report_time, clock_map=None):
    """
    Auto-fix mission records — Scenario A with OPTIONAL clock-out (M-25).

    Inputs:
      - records: raw mission rows (list[list])
      - driver: driver name
      - report_time: period_end (唯一时间基准)
      - clock_map: OPTIONAL
          { "YYYY-MM-DD": "YYYY-MM-DD HH:MM:SS" }

    Priority (FROZEN):
      1) clock-out (if provided)
      2) same-day 23:59:59

    Constraints:
      - Do NOT mutate input records
      - Use ONLY M-18 indices
      - Do NOT use return start/end
      - Do NOT use datetime.now()
      - MUST work even if clock_map is None
    """
    if not records:
        return []

    fixed = deepcopy(records)

    for r in fixed:
        if len(r) < M_MANDATORY_COLS:
            continue

        start_raw = r[M_IDX_START]
        end_raw = r[M_IDX_END]

        # Only: has start, missing end
        if not start_raw or end_raw:
            continue

        try:
            start_dt = datetime.fromisoformat(start_raw)
        except Exception:
            continue

        # threshold = next-day 04:00
        threshold = datetime.combine(
            start_dt.date() + timedelta(days=1),
            time(4, 0, 0)
        )
        if report_time < threshold:
            continue

        # Priority 1: clock-out (OPTIONAL)
        clock_out_raw = None
        if clock_map:
            day_key = start_dt.date().isoformat()
            clock_out_raw = clock_map.get(day_key)

        if clock_out_raw:
            r[M_IDX_END] = clock_out_raw
            continue

        # Priority 2: same-day 23:59:59
        r[M_IDX_END] = datetime.combine(
            start_dt.date(),
            time(23, 59, 59)
        ).strftime("%Y-%m-%d %H:%M:%S")

    return fixed

# ===== BEGIN M-27 INTEGRATION =====
# =========================
# Mission Report V9 (M-27)
# Final integration: M-26 Clock schema + M-25 + M-24
# =========================

from copy import deepcopy
from datetime import datetime, timedelta, time
import io
import csv

# ============================================================
# M-18: Mission table schema (FROZEN)
# =========================================================
MISSION_HEADERS = [
    "GUID",
    "No.",
    "Name",
    "Plate",
    "Start Date",
    "End Date",
    "Departure",
    "Arrival",
    "Staff Name",
    "Roundtrip",
    "Return Start",
    "Return End",
]

M_IDX_GUID = 0
M_IDX_NO = 1
M_IDX_NAME = 2
M_IDX_PLATE = 3
M_IDX_START = 4
M_IDX_END = 5
M_IDX_DEPARTURE = 6
M_IDX_ARRIVAL = 7
M_IDX_STAFF = 8
M_IDX_ROUNDTRIP = 9
M_IDX_RETURN_START = 10
M_IDX_RETURN_END = 11

M_MANDATORY_COLS = len(MISSION_HEADERS)

# ============================================================
# M-26: Clock / Driver_OT schema (FROZEN)
# =========================================================
CLOCK_HEADERS = [
    "Date",
    "Driver",
    "Action",
    "Timestamp",
    "ClockType",
]

C_IDX_DATE = 0
C_IDX_DRIVER = 1
C_IDX_ACTION = 2
C_IDX_TIMESTAMP = 3
C_IDX_TYPE = 4

C_MANDATORY_COLS = len(CLOCK_HEADERS)

CLOCK_TAB = "Driver_OT"


def validate_clock_header(header_row):
    if not header_row:
        return False
    normalized = [str(c).strip() for c in header_row]
    return normalized[:C_MANDATORY_COLS] == CLOCK_HEADERS

# ============================================================
# M-25: Clock-out map builder (Driver_OT aligned)
# =========================================================
def build_clock_map(period_start, period_end, driver):
    ws = open_worksheet(CLOCK_TAB)
    rows = ws.get_all_values()

    if not rows or len(rows) < 2:
        return {}

    header = rows[0]
    if not validate_clock_header(header):
        return {}

    clock_map = {}

    for r in rows[1:]:
        if len(r) < C_MANDATORY_COLS:
            continue

        if r[C_IDX_DRIVER].strip() != driver:
            continue

        if str(r[C_IDX_ACTION]).strip().upper() != "OUT":
            continue

        ts_raw = r[C_IDX_TIMESTAMP]
        if not ts_raw:
            continue

        try:
            ts_dt = datetime.fromisoformat(ts_raw)
        except Exception:
            continue

        if not (period_start <= ts_dt <= period_end):
            continue

        day_key = ts_dt.date().isoformat()

        if day_key not in clock_map or ts_dt > datetime.fromisoformat(clock_map[day_key]):
            clock_map[day_key] = ts_raw

    return clock_map

# ============================================================
# M-25: Auto-fix Scenario A (clock-out / 23:59:59)
# =========================================================
def autofix_mission_records(records, driver, report_time, clock_map=None):
    if not records:
        return []

    fixed = deepcopy(records)

    for r in fixed:
        if len(r) < M_MANDATORY_COLS:
            continue

        start_raw = r[M_IDX_START]
        end_raw = r[M_IDX_END]

        if not start_raw or end_raw:
            continue

        try:
            start_dt = datetime.fromisoformat(start_raw)
        except Exception:
            continue

        threshold = datetime.combine(
            start_dt.date() + timedelta(days=1),
            time(4, 0, 0)
        )

        if report_time < threshold:
            continue

        clock_out_raw = None
        if clock_map:
            clock_out_raw = clock_map.get(start_dt.date().isoformat())

        if clock_out_raw:
            r[M_IDX_END] = clock_out_raw
        else:
            r[M_IDX_END] = datetime.combine(
                start_dt.date(),
                time(23, 59, 59)
            ).strftime("%Y-%m-%d %H:%M:%S")

    return fixed

# ============================================================
# M-21: Mission days
# =========================================================
def apply_mission_days(round_trips):
    out = []
    for rt in round_trips:
        try:
            sd = datetime.fromisoformat(rt["start_dt"]).date()
            ed = datetime.fromisoformat(rt["end_dt"]).date()
        except Exception:
            continue

        out.append({
            **rt,
            "mission_days": (ed - sd).days + 1
        })
    return out

# ============================================================
# M-22: Mission type
# =========================================================
def apply_mission_type(round_trips):
    out = []
    for rt in round_trips:
        sc = str(rt.get("start_city", "")).upper()
        ec = str(rt.get("end_city", "")).upper()
        mtype = None
        if sc == ec == "SHV":
            mtype = "PP Mission"
        elif sc == ec == "PP":
            mtype = "SHV Mission"

        out.append({**rt, "type": mtype})
    return out

# ============================================================
# M-23: CSV export
# =========================================================
def export_mission_csv(driver, period, missions):
    output = io.StringIO()
    writer = csv.writer(output)

    period_start, period_end = period

    writer.writerow(["Driver", driver])
    writer.writerow(["Period", f"{period_start} → {period_end}"])
    writer.writerow([])

    writer.writerow([
        "Plate",
        "Start Date",
        "End Date",
        "Mission Day(s)",
        "From",
        "To",
        "Type",
    ])

    total = 0
    for m in missions:
        d = m.get("mission_days") or 0
        total += d
        writer.writerow([
            m.get("plate"),
            m.get("start_dt"),
            m.get("end_dt"),
            d,
            m.get("from"),
            m.get("to"),
            m.get("type"),
        ])

    writer.writerow([])
    writer.writerow(["TOTAL MISSION DAY(s)", total])

    return output.getvalue()

# ============================================================
# M-24: Callback wiring (OT V9 aligned)
# =========================================================
async def mission_report_driver_callback(update, context):
    q = update.callback_query
    await q.answer()

    await reply_private(update, context)

    if q.message and q.message.reply_markup:
        await q.message.edit_reply_markup(reply_markup=None)

    driver = q.data.split(":", 1)[1]

    period_start, period_end = get_mission_period_from_context(context)
    period = (period_start, period_end)

    records = read_mission_records(period_start, period_end, driver)

    clock_map = build_clock_map(period_start, period_end, driver)

    records = autofix_mission_records(
        records,
        driver,
        report_time=period_end,
        clock_map=clock_map
    )

    trips = pair_mission_round_trips(records)
    trips = apply_mission_days(trips)
    trips = apply_mission_type(trips)

    csv_content = export_mission_csv(driver, period, trips)

    bio = io.BytesIO(csv_content.encode("utf-8"))
    bio.name = f"Mission_Report_{driver}_{period_start:%Y-%m}.csv"

    await update.effective_chat.send_document(
        document=bio,
        caption=f"Mission report for {driver}"
    )

# ===== END M-27 INTEGRATION =====


# ===== M27 STATE MACHINE OVERRIDE =====

# =========================
# M27-STATE-MACHINE
# Mission round-trip pairing using OT V9 state machine model
# =========================

from datetime import datetime

def pair_mission_round_trips(records):
    """
    OT V9–equivalent state machine pairing.

    Rules:
      - Single open_trip state
      - Scan records in chronological order
      - New outbound overrides previous open outbound
      - Return closes only the current open outbound
      - No searching, no backtracking
    """
    if not records:
        return []

    # Prepare sortable list
    rows = []
    for r in records:
        if len(r) < M_MANDATORY_COLS:
            continue
        start_raw = r[M_IDX_START]
        if not start_raw:
            continue
        try:
            start_dt = datetime.fromisoformat(start_raw)
        except Exception:
            continue
        rows.append((start_dt, r))

    # Sort by time ascending
    rows.sort(key=lambda x: x[0])

    open_row = None
    trips = []

    for _, r in rows:
        dep = r[M_IDX_DEPARTURE]
        arr = r[M_IDX_ARRIVAL]
        plate = r[M_IDX_PLATE]

        # Outbound opens state
        if open_row is None:
            open_row = r
            continue

        # Check if this row closes the open outbound
        if (
            open_row[M_IDX_PLATE] == plate and
            open_row[M_IDX_DEPARTURE] == arr and
            open_row[M_IDX_ARRIVAL] == dep and
            r[M_IDX_END]
        ):
            trips.append({
                "plate": plate,
                "start_dt": open_row[M_IDX_START],
                "end_dt": r[M_IDX_END],
                "from": open_row[M_IDX_DEPARTURE],
                "to": open_row[M_IDX_ARRIVAL],
                "start_city": open_row[M_IDX_DEPARTURE],
                "end_city": open_row[M_IDX_DEPARTURE],
            })
            open_row = None
        else:
            # OT V9 behavior: override open state
            open_row = r

    return trips
