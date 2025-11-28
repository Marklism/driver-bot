#!/usr/bin/env python3
"""
Driver Bot — consolidated fixed version
- Removes pin/置顶 functionality
- Removes many prompt messages; /fuel flow expects amount+odo as command args, then plate selection keyboard, then one invoice yes/no follow-up.
- ODO persisted to sheet: last ODO read from sheet used to compute delta.
- /leave command: zero prompt, writes single row to Leave sheet.
- Missions recorded in a single merged row when a full roundtrip (d,a,d,a) is observed. Mission summary updated per driver-month and plate-month.
- set_my_commands registered via post_init (no create_task)
- Webhook if WEBHOOK_URL present else polling. No conflicting getUpdates calls.
- Minimal public messages: only short English receipts on success.

Notes:
- Environment variables required: BOT_TOKEN, GSHEET_KEY, GOOGLE_CREDS_JSON (service account JSON content), optionally WEBHOOK_URL and PORT.
- Sheet tabs (defaults): fuel_odo, missions, mission_summary, leave

"""

import os
import sys
import json
import logging
import base64
import re
from datetime import datetime, timedelta, time as dtime
from typing import Optional, Dict, Any, List

import gspread
from oauth2client.service_account import ServiceAccountCredentials

from telegram import (
    Update,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    BotCommand,
)
from telegram.ext import (
    ApplicationBuilder,
    CommandHandler,
    CallbackQueryHandler,
    MessageHandler,
    ContextTypes,
    filters,
)

# -------------------------
# Basic logging
# -------------------------
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("driver-bot-fixed")

# -------------------------
# Environment / configuration
# -------------------------
BOT_TOKEN = os.getenv("BOT_TOKEN")
if not BOT_TOKEN:
    logger.error("BOT_TOKEN not set. Exiting.")
    sys.exit(1)

GSHEET_KEY = os.getenv("GSHEET_KEY")
CREDS_JSON = os.getenv("GOOGLE_CREDS_JSON") or os.getenv("GOOGLE_CREDS_BASE64")
WEBHOOK_URL = os.getenv("WEBHOOK_URL")
PORT = int(os.getenv("PORT", "8443"))

# Sheet names
FUEL_SHEET = os.getenv("FuelSheet", "fuel_odo")
MISSION_SHEET = os.getenv("MissionSheet", "missions")
SUMMARY_SHEET = os.getenv("SummarySheet", "mission_summary")
LEAVE_SHEET = os.getenv("LeaveSheet", "leave")

# UI plates list (can be loaded from env PLATE_LIST CSV)
PLATE_LIST = os.getenv("PLATE_LIST", "2BB-3071,2BB-0809,2CI-8066,2CK-8066,2CJ-8066")
PLATES = [p.strip() for p in PLATE_LIST.split(",") if p.strip()]
if not PLATES:
    PLATES = ["UNKNOWN"]

# Per-diem rate
PER_DIEM_PER_DAY = float(os.getenv("PER_DIEM_PER_DAY", "15"))

# -------------------------
# Google Sheets helpers
# -------------------------
SCOPES = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
_gc = None
_sheet_cache: Dict[str, Any] = {}


def _load_gspread_client() -> Optional[gspread.Client]:
    global _gc
    if _gc:
        return _gc
    if not CREDS_JSON:
        logger.error("No Google credentials provided (GOOGLE_CREDS_JSON or GOOGLE_CREDS_BASE64). Sheets disabled.")
        return None
    try:
        # Accept either raw JSON or base64
        raw = CREDS_JSON.strip()
        if not raw.startswith("{"):
            # assume base64
            padded = "".join(raw.split())
            missing = len(padded) % 4
            if missing:
                padded += "=" * (4 - missing)
            raw = base64.b64decode(padded).decode("utf-8")
        creds_dict = json.loads(raw)
        creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, SCOPES)
        _gc = gspread.authorize(creds)
        return _gc
    except Exception:
        logger.exception("Failed to create gspread client")
        return None


def _ws(name: str):
    """Open worksheet by name and cache it."""
    client = _load_gspread_client()
    if not client or not GSHEET_KEY:
        raise RuntimeError("Google Sheets client or key not configured")
    key = f"{GSHEET_KEY}:{name}"
    if key in _sheet_cache:
        return _sheet_cache[key]
    sh = client.open_by_key(GSHEET_KEY)
    try:
        ws = sh.worksheet(name)
    except Exception:
        ws = sh.add_worksheet(title=name, rows="2000", cols="20")
    _sheet_cache[key] = ws
    return ws


# Ensure canonical headers
FUEL_HEADERS = ["timestamp", "plate", "odo", "fuel_usd", "invoice_received", "odo_diff"]
MISSION_HEADERS = ["driver", "plate", "depart1_city", "depart1_ts", "arrive1_city", "arrive1_ts", "depart2_city", "depart2_ts", "arrive2_city", "arrive2_ts", "start", "end", "duration_minutes"]
SUMMARY_HEADERS = ["driver", "month", "mission_days", "per_diem_amt"]
LEAVE_HEADERS = ["driver", "start_date", "end_date", "type", "notes", "timestamp"]


def _ensure_headers(ws_name: str, headers: List[str]):
    try:
        ws = _ws(ws_name)
        first = ws.row_values(1)
        if first != headers:
            # overwrite header row
            try:
                ws.delete_row(1)
            except Exception:
                pass
            ws.insert_row(headers, index=1)
            logger.info("Set headers for %s", ws_name)
    except Exception:
        logger.exception("Failed to ensure headers for %s", ws_name)


# Initialize sheets and headers at startup
try:
    if _load_gspread_client():
        _ensure_headers(FUEL_SHEET, FUEL_HEADERS)
        _ensure_headers(MISSION_SHEET, MISSION_HEADERS)
        _ensure_headers(SUMMARY_SHEET, SUMMARY_HEADERS)
        _ensure_headers(LEAVE_SHEET, LEAVE_HEADERS)
except Exception:
    logger.exception("Exception while ensuring headers")

# -------------------------
# Utility functions
# -------------------------

def now_ts():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def parse_ts(ts: str) -> Optional[datetime]:
    try:
        return datetime.strptime(ts, "%Y-%m-%d %H:%M:%S")
    except Exception:
        try:
            return datetime.strptime(ts, "%Y-%m-%d %H:%M")
        except Exception:
            return None


# Read last odo for a plate from fuel sheet (bottom-up)
def get_last_odo_from_sheet(plate: str) -> Optional[int]:
    try:
        ws = _ws(FUEL_SHEET)
        vals = ws.get_all_values()
        if not vals or len(vals) <= 1:
            return None
        # find header
        start_idx = 1
        for row in reversed(vals[start_idx:]):
            if len(row) >= 3 and row[1].strip() == plate and row[2].strip():
                m = re.search(r"(\d+)", row[2])
                if m:
                    return int(m.group(1))
        return None
    except Exception:
        logger.exception("Failed to read last odo for %s", plate)
        return None


# Append unified fuel+odo row and compute diff
def record_fuel_odo(plate: str, odo: int, fuel_usd: float, invoice_received: str) -> Dict[str, Any]:
    try:
        ws = _ws(FUEL_SHEET)
        prev = get_last_odo_from_sheet(plate)
        diff = ""
        if prev is not None:
            try:
                diff_val = int(odo) - int(prev)
                diff = str(diff_val)
            except Exception:
                diff = ""
        ts = now_ts()
        row = [ts, plate, str(int(odo)), f"{float(fuel_usd):.2f}", invoice_received, diff]
        ws.append_row(row, value_input_option="USER_ENTERED")
        logger.info("Wrote fuel row for %s odo=%s fuel=%s invoice=%s diff=%s", plate, odo, fuel_usd, invoice_received, diff)
        return {"ok": True, "ts": ts, "odo": odo, "fuel": fuel_usd, "diff": diff}
    except Exception:
        logger.exception("Failed to write fuel row")
        return {"ok": False}


# -------------------------
# /fuel flow
# -------------------------
# Flow design chosen for minimal prompts and no 'enter amount' prompts:
# - User issues: /fuel <fuel_amount> <odo>
# - Bot responds with plate selection keyboard
# - User clicks plate
# - Bot asks one question: "Invoice received? yes/no"
# - User replies with yes/no (plaintext)
# - Bot writes single combined row and sends short receipt (English)


async def cmd_fuel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # Expect two args: fuel_amount and odo. Both required to avoid additional prompts.
    args = context.args or []
    if len(args) < 2:
        await update.effective_chat.send_message("Usage: /fuel <fuel_usd> <odo>")
        return
    try:
        fuel_amt = float(args[0])
        odo_val = int(re.search(r"(\d+)", args[1]).group(1))
    except Exception:
        await update.effective_chat.send_message("Invalid args. Usage: /fuel <fuel_usd> <odo>")
        return
    # store pending values and present plate keyboard
    context.user_data["pending_fuel"] = {"fuel": fuel_amt, "odo": odo_val}
    kb = []
    row = []
    for i, p in enumerate(PLATES, 1):
        row.append(InlineKeyboardButton(p, callback_data=f"fuel_plate|{p}"))
        if i % 3 == 0:
            kb.append(row)
            row = []
    if row:
        kb.append(row)
    try:
        await update.effective_chat.send_message("Select plate:", reply_markup=InlineKeyboardMarkup(kb))
    except Exception:
        logger.exception("Failed to send plate keyboard")


async def _handle_fuel_plate_cb(update: Update, context: ContextTypes.DEFAULT_TYPE, plate: str):
    # user selected plate; now ask invoice yes/no
    query = update.callback_query
    await query.answer()
    pd = context.user_data.get("pending_fuel")
    if not pd:
        try:
            await query.edit_message_text("No pending fuel data. Use /fuel <amount> <odo>")
        except Exception:
            pass
        return
    # bind plate
    pd["plate"] = plate
    context.user_data["pending_fuel"] = pd
    # ask invoice yes/no (single follow up)
    try:
        # edit to a minimal message to avoid clutter
        await query.edit_message_text("Invoice received? yes/no")
    except Exception:
        logger.exception("Failed editing callback message for invoice question")


# Global handler: any non-command message used for invoice yes/no and other plain replies
async def handle_plain_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = (update.effective_message.text or "").strip().lower()
    # handle invoice reply if pending
    pending = context.user_data.get("pending_fuel")
    if pending and "plate" in pending and text in ("yes", "no"):
        invoice = "Yes" if text == "yes" else "No"
        plate = pending.get("plate")
        fuel = pending.get("fuel")
        odo = pending.get("odo")
        res = record_fuel_odo(plate, odo, fuel, invoice)
        # cleanup pending
        context.user_data.pop("pending_fuel", None)
        if res.get("ok"):
            diff_txt = res.get("diff", "")
            ts = res.get("ts")
            # short English receipt
            receipt = f"Plate {plate} @ {odo} km + ${fuel:.2f} fuel on {ts.split()[0]}, Odo difference since last record: {diff_txt} km"
            try:
                await update.effective_chat.send_message(receipt)
            except Exception:
                logger.exception("Failed to send receipt message")
        else:
            try:
                await update.effective_chat.send_message("Failed to record fuel entry.")
            except Exception:
                pass
        return

    # otherwise ignore or handle leave freeform; keep minimal
    return


# -------------------------
# /leave command (zero prompt)
# Usage: /leave <driver> <YYYY-MM-DD> <YYYY-MM-DD> <type: SL|AL> [notes]
# -------------------------
async def cmd_leave(update: Update, context: ContextTypes.DEFAULT_TYPE):
    args = context.args or []
    if len(args) < 4:
        await update.effective_chat.send_message("Usage: /leave <driver> <start> <end> <type> [notes]")
        return
    driver = args[0]
    start = args[1]
    end = args[2]
    leave_type = args[3]
    notes = " ".join(args[4:]) if len(args) > 4 else ""
    ts = now_ts()
    try:
        ws = _ws(LEAVE_SHEET)
        ws.append_row([driver, start, end, leave_type, notes, ts], value_input_option="USER_ENTERED")
        try:
            await update.effective_chat.send_message(f"Leave recorded for {driver} {start} to {end}")
        except Exception:
            pass
    except Exception:
        logger.exception("Failed to write leave row")
        try:
            await update.effective_chat.send_message("Failed to record leave (sheet error)")
        except Exception:
            pass


# -------------------------
# Missions flow
# - mission_start: select plate then departure city
# - mission_end: select plate then arrival city
# - internal per-chat buffer collects legs for each plate; when pattern d,a,d,a occurs, write merged mission row
# - update mission_summary: driver-month mission_days and per diem A-2 rule
# -------------------------

# per-chat mission buffer held in chat_data["mission_buf"] = { plate: [legs...] }
# leg: { 't': 'd'|'a', 'city': 'PP'|'SHV', 'ts': timestamp, 'driver': username }


def _append_leg(ctx_chat_data: Dict, plate: str, typ: str, city: str, driver: str):
    buf = ctx_chat_data.setdefault("mission_buf", {})
    legs = buf.setdefault(plate, [])
    legs.append({"t": typ, "city": city, "ts": now_ts(), "driver": driver})
    buf[plate] = legs
    ctx_chat_data["mission_buf"] = buf


def compute_duration_minutes(start_ts: str, end_ts: str) -> int:
    s = parse_ts(start_ts)
    e = parse_ts(end_ts)
    if not s or not e:
        return 0
    delta = e - s
    return int(delta.total_seconds() // 60)


def compute_mission_days_a2(start_ts: str, end_ts: str) -> int:
    """
    A-2 rule: if start and end same day or end before 12:00 next day counts as 1 day; otherwise count additional days.
    We count inclusive days and apply midday rule on last day boundary (12:00).
    """
    s = parse_ts(start_ts)
    e = parse_ts(end_ts)
    if not s or not e:
        return 0
    # normalize times
    # if same date -> 1
    if s.date() == e.date():
        return 1
    days = (e.date() - s.date()).days + 1
    # if end time is on the last day and before 12:00, possibly reduce
    last_midday = datetime(e.year, e.month, e.day, 12, 0, 0)
    if e < last_midday:
        # subtract one day
        days -= 1
    return max(1, days)


async def cmd_mission_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # present plate keyboard
    kb = []
    row = []
    for i, p in enumerate(PLATES, 1):
        row.append(InlineKeyboardButton(p, callback_data=f"ms_start_plate|{p}"))
        if i % 3 == 0:
            kb.append(row); row = []
    if row:
        kb.append(row)
    try:
        await update.effective_chat.send_message("Select plate for mission start:", reply_markup=InlineKeyboardMarkup(kb))
    except Exception:
        logger.exception("Failed to send ms start keyboard")


async def cmd_mission_end(update: Update, context: ContextTypes.DEFAULT_TYPE):
    kb = []
    row = []
    for i, p in enumerate(PLATES, 1):
        row.append(InlineKeyboardButton(p, callback_data=f"ms_end_plate|{p}"))
        if i % 3 == 0:
            kb.append(row); row = []
    if row:
        kb.append(row)
    try:
        await update.effective_chat.send_message("Select plate for mission end:", reply_markup=InlineKeyboardMarkup(kb))
    except Exception:
        logger.exception("Failed to send ms end keyboard")


async def mission_plate_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    data = q.data or ""
    parts = data.split("|")
    action = parts[0]
    plate = parts[1] if len(parts) > 1 else "UNKNOWN"
    if action == "ms_start_plate":
        # present departure city choices
        kb = [[InlineKeyboardButton("PP", callback_data=f"ms_start_city|{plate}|PP"), InlineKeyboardButton("SHV", callback_data=f"ms_start_city|{plate}|SHV")]]
        try:
            await q.edit_message_text("Select departure:", reply_markup=InlineKeyboardMarkup(kb))
        except Exception:
            logger.exception("Failed to edit msg for ms_start_plate")
        return
    if action == "ms_end_plate":
        kb = [[InlineKeyboardButton("PP", callback_data=f"ms_end_city|{plate}|PP"), InlineKeyboardButton("SHV", callback_data=f"ms_end_city|{plate}|SHV")]]
        try:
            await q.edit_message_text("Select arrival:", reply_markup=InlineKeyboardMarkup(kb))
        except Exception:
            logger.exception("Failed to edit msg for ms_end_plate")
        return
    # handle city callbacks
    if action == "ms_start_city":
        _, plate, city = parts
        driver = q.from_user.username or q.from_user.full_name
        # append departure leg
        _append_leg(context.chat_data, plate, "d", city, driver)
        try:
            await q.edit_message_text(f"Recorded departure for {plate} from {city} at {now_ts()}")
        except Exception:
            pass
        return
    if action == "ms_end_city":
        _, plate, city = parts
        driver = q.from_user.username or q.from_user.full_name
        _append_leg(context.chat_data, plate, "a", city, driver)
        try:
            await q.edit_message_text(f"Recorded arrival for {plate} at {city} {now_ts()}")
        except Exception:
            pass
        # attempt to resolve possible full mission (d,a,d,a)
        try:
            buf = context.chat_data.get("mission_buf", {})
            legs = buf.get(plate, [])
            # search for last pattern d,a,d,a
            if len(legs) >= 4:
                tail = legs[-4:]
                if [x["t"] for x in tail] == ["d", "a", "d", "a"]:
                    d1, a1, d2, a2 = tail
                    drv = d1["driver"]
                    start = d1["ts"]
                    end = a2["ts"]
                    dur_min = compute_duration_minutes(start, end)
                    # write merged mission row
                    ws = _ws(MISSION_SHEET)
                    row = [drv, plate, d1["city"], d1["ts"], a1["city"], a1["ts"], d2["city"], d2["ts"], a2["city"], a2["ts"], start, end, str(dur_min)]
                    ws.append_row(row, value_input_option="USER_ENTERED")
                    # clear buffer for plate
                    buf[plate] = []
                    context.chat_data["mission_buf"] = buf
                    # update mission summary counts (driver and plate)
                    try:
                        _update_mission_summary(drv, start, end)
                    except Exception:
                        logger.exception("Failed updating mission summary")
                    # send short notifications (driver and plate counts)
                    # we compute counts for month and send short messages
                    drv_count, plate_count = _get_monthly_mission_counts(drv, plate, start)
                    try:
                        await q.message.chat.send_message(f"Driver {drv} completed {drv_count} mission(s) in {start[:7]}")
                        await q.message.chat.send_message(f"Plate {plate} completed {plate_count} mission(s) in {start[:7]}")
                    except Exception:
                        pass
        except Exception:
            logger.exception("Error resolving mission pattern")
        return


# -------------------------
# Mission summary helpers
# -------------------------

def _month_key_for_ts(ts: str) -> str:
    dt = parse_ts(ts)
    if not dt:
        return ""
    return dt.strftime("%Y-%m")


def _update_mission_summary(driver: str, start_ts: str, end_ts: str):
    """Update mission_summary sheet per A-2 rule 
    - driver monthly mission_days and per_diem
    - plate monthly mission_days
    """
    try:
        month = _month_key_for_ts(start_ts)
        days = compute_mission_days_a2(start_ts, end_ts)
        # update driver/month row
        ws = _ws(SUMMARY_SHEET)
        # load existing
        rows = ws.get_all_records()
        found = False
        for idx, r in enumerate(rows, start=2):
            if str(r.get("driver")) == driver and str(r.get("month")) == month:
                # update mission_days and per_diem
                try:
                    existing_days = int(r.get("mission_days") or 0)
                except Exception:
                    existing_days = 0
                new_days = existing_days + days
                per_diem = new_days * PER_DIEM_PER_DAY
                ws.update_cell(idx, 3, str(new_days))  # mission_days
                ws.update_cell(idx, 4, f"{per_diem:.2f}")
                found = True
                break
        if not found:
            ws.append_row([driver, month, str(days), f"{days * PER_DIEM_PER_DAY:.2f}"], value_input_option="USER_ENTERED")
    except Exception:
        logger.exception("Failed to update mission summary sheet")


def _get_monthly_mission_counts(driver: str, plate: str, ts_example: str) -> (int, int):
    month = _month_key_for_ts(ts_example)
    d_count = 0
    p_count = 0
    try:
        ws = _ws(MISSION_SHEET)
        rows = ws.get_all_records()
        for r in rows:
            s = str(r.get("start") or "")
            if not s:
                continue
            if not s.startswith(month):
                # compare by parsed month
                sdt = parse_ts(s)
                if not sdt:
                    continue
                if sdt.strftime("%Y-%m") != month:
                    continue
            if str(r.get("driver")) == driver:
                d_count += 1
            if str(r.get("plate")) == plate:
                p_count += 1
    except Exception:
        logger.exception("Failed counting monthly missions")
    return d_count, p_count


# -------------------------
# Command registration (post_init style for set_my_commands)
# -------------------------
async def _post_init(app):
    try:
        cmds = [
            BotCommand("fuel", "Record fuel+odo: /fuel <fuel_usd> <odo> (then select plate)") ,
            BotCommand("leave", "Record leave: /leave <driver> <start> <end> <type> [notes]"),
            BotCommand("mission_start", "Start mission (select plate then departure)"),
            BotCommand("mission_end", "End mission (select plate then arrival)"),
        ]
        await app.bot.set_my_commands(cmds)
        logger.info("Registered bot commands")
    except Exception:
        logger.exception("Failed to set bot commands in post_init")


# -------------------------
# Wiring and main
# -------------------------

def build_application() -> Any:
    app = ApplicationBuilder().token(BOT_TOKEN).post_init(_post_init).build()
    # handlers
    app.add_handler(CommandHandler("fuel", cmd_fuel))
    app.add_handler(CallbackQueryHandler(lambda u, c: _dispatch_callback(u, c)))
    app.add_handler(MessageHandler(filters.TEXT & (~filters.COMMAND), handle_plain_text))
    app.add_handler(CommandHandler("leave", cmd_leave))
    app.add_handler(CommandHandler("mission_start", cmd_mission_start))
    app.add_handler(CommandHandler("mission_end", cmd_mission_end))
    return app


async def _dispatch_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # central callback router for plate & mission & fuel
    q = update.callback_query
    data = (q.data or "")
    if data.startswith("fuel_plate|"):
        _, plate = data.split("|", 1)
        await _handle_fuel_plate_cb(update, context, plate)
        return
    if data.startswith("ms_"):
        await mission_plate_cb(update, context)
        return
    # unknown
    try:
        await q.answer()
    except Exception:
        pass


def run():
    app = build_application()
    if WEBHOOK_URL:
        # run webhook server
        logger.info("Starting webhook at %s (port %s)", WEBHOOK_URL, PORT)
        # webhook path derives from bot token suffix for security
        path = f"/webhook/{BOT_TOKEN.split(":")[-1]}" if ":" in BOT_TOKEN else f"/webhook"
        app.run_webhook(listen="0.0.0.0", port=PORT, url_path=path, webhook_url=WEBHOOK_URL + path)
    else:
        logger.info("Starting polling mode")
        app.run_polling()


if __name__ == "__main__":
    run()
