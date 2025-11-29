#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
driver-bot — merged final
Features:
 - Fuel+ODO flow with plate keyboard, merged write to fuel_odo sheet, ask Invoice yes/no, send short receipt.
 - ODO persisted in sheet; diff calculated vs last record for same plate.
 - Missions recorded in merged rows (d,a,d,a -> one mission).
 - Mission summary (monthly) written to mission_summary tab: driver-month mission_days and per-diem.
 - /leave zero-prompt single-line write to leave tab.
 - Headers enforced (exact match).
 - post_init registration used for set_my_commands; no create_task and no un-awaited coroutines.
 - Removes pin/anchor UI and removes "enter staff" prompts.
"""

import os
import json
import logging
import base64
import re
from datetime import datetime, timedelta, time as dtime
from typing import Optional, List, Dict, Any

import gspread
from oauth2client.service_account import ServiceAccountCredentials

from telegram import (
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    BotCommand,
    Update,
)
from telegram.ext import (
    ApplicationBuilder,
    CommandHandler,
    CallbackQueryHandler,
    MessageHandler,
    ContextTypes,
    filters,
)

# --------------- config & logging ---------------
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("driver-bot-final")

BOT_TOKEN = os.getenv("BOT_TOKEN")
WEBHOOK_URL = os.getenv("WEBHOOK_URL", "").strip()
PORT = int(os.getenv("PORT", "8443"))

# Sheets config
GOOGLE_CREDS_JSON = os.getenv("GOOGLE_CREDS_JSON")  # full json text
GSHEET_KEY = os.getenv("GSHEET_KEY")  # spreadsheet id

# Sheet names (you can change)
FUEL_SHEET = os.getenv("FuelSheet", "fuel_odo")
MISSION_SHEET = os.getenv("MissionSheet", "missions")
SUMMARY_SHEET = os.getenv("SummarySheet", "mission_summary")
LEAVE_SHEET = os.getenv("LeaveSheet", "leave")

# Per-diem config for A-2
PER_DIEM_PER_DAY_USD = float(os.getenv("PER_DIEM_USD", "15.0"))

# Plates list fallback
PLATES = [p.strip() for p in os.getenv("PLATE_LIST", "2BB-3071,2BB-0809,2CI-8066").split(",") if p.strip()]

# --------------- time helpers ---------------
TS_FMT = "%Y-%m-%d %H:%M:%S"
DATE_FMT = "%Y-%m-%d"

def now_str(tsfmt: str = TS_FMT) -> str:
    return datetime.now().strftime(tsfmt)

def parse_ts(ts: str) -> Optional[datetime]:
    try:
        return datetime.strptime(ts, TS_FMT)
    except Exception:
        try:
            return datetime.strptime(ts, DATE_FMT)
        except Exception:
            return None

# --------------- Google Sheets helpers ---------------
SCOPES = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]

GS_CLIENT = None
SHEETS_ENABLED = False

def _init_gs_client():
    global GS_CLIENT, SHEETS_ENABLED
    if not GOOGLE_CREDS_JSON or not GSHEET_KEY:
        logger.warning("Google Sheets not configured: set GOOGLE_CREDS_JSON and GSHEET_KEY if you want sheet writes.")
        SHEETS_ENABLED = False
        return
    try:
        # allow creds either as JSON string or base64
        txt = GOOGLE_CREDS_JSON.strip()
        try:
            if txt.startswith("{"):
                creds_dict = json.loads(txt)
            else:
                # maybe base64
                padded = "".join(txt.split())
                missing = len(padded) % 4
                if missing:
                    padded += "=" * (4 - missing)
                creds_dict = json.loads(base64.b64decode(padded))
        except Exception:
            creds_dict = json.loads(txt)
        creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, SCOPES)
        GS_CLIENT = gspread.authorize(creds)
        SHEETS_ENABLED = True
        logger.info("Google Sheets client initialized.")
    except Exception as e:
        logger.exception("Failed to init Google Sheets client: %s", e)
        GS_CLIENT = None
        SHEETS_ENABLED = False

def _ws(tab_name: str):
    if not SHEETS_ENABLED or GS_CLIENT is None:
        raise RuntimeError("Google Sheets client or key not configured")
    sh = GS_CLIENT.open_by_key(GSHEET_KEY)
    try:
        ws = sh.worksheet(tab_name)
    except Exception:
        # try to create
        ws = sh.add_worksheet(title=tab_name, rows="2000", cols="20")
    return ws

# Header templates (exact match required)
HEADERS = {
    FUEL_SHEET: ["timestamp", "plate", "odo", "fuel_usd", "invoice_received", "odo_diff"],
    MISSION_SHEET: ["driver","plate","depart1_city","depart1_ts","arrive1_city","arrive1_ts","depart2_city","depart2_ts","arrive2_city","arrive2_ts","start","end","duration_minutes"],
    SUMMARY_SHEET: ["driver","month","mission_days","per_diem_usd"],
    LEAVE_SHEET: ["driver","start_date","end_date","type","notes","recorded_at"],
}

def ensure_headers_once():
    if not SHEETS_ENABLED:
        logger.info("Skipping header ensure: Google Sheets not enabled.")
        return
    for name, hdr in HEADERS.items():
        try:
            ws = _ws(name)
            current = ws.row_values(1)
            if current != hdr:
                # overwrite first row exactly
                try:
                    # If sheet is empty or header incorrect, update by range
                    end_col = chr(ord('A') + len(hdr) - 1)
                    rng = f"A1:{end_col}1"
                    ws.update(rng, [hdr], value_input_option="USER_ENTERED")
                except Exception:
                    # fallback: delete row and insert
                    try:
                        ws.delete_row(1)
                    except Exception:
                        pass
                    ws.insert_row(hdr, index=1)
                logger.info("Updated header row on %s", name)
        except Exception as e:
            logger.exception("Failed to ensure headers for %s: %s", name, e)

# --------------- Fuel & Odo functions ---------------

def _last_odo_from_sheet(plate: str) -> Optional[int]:
    """
    Read bottom-up to find last mileage value for plate from FUEL_SHEET (column 'odo').
    """
    if not SHEETS_ENABLED:
        return None
    try:
        ws = _ws(FUEL_SHEET)
        vals = ws.get_all_values()
        if not vals or len(vals) <= 1:
            return None
        # header assumed row 1
        for r in reversed(vals[1:]):
            rp = r[1] if len(r) > 1 else ""
            odo_cell = r[2] if len(r) > 2 else ""
            if str(rp).strip() == plate and odo_cell:
                m = re.search(r'(\d+)', str(odo_cell))
                if m:
                    return int(m.group(1))
        return None
    except Exception:
        logger.exception("Failed to read last odo from sheet")
        return None

def write_fuel_odo_row(plate: str, odo: int, fuel_usd: float, invoice_received: str) -> Dict[str, Any]:
    """
    Write a single row to FUEL_SHEET and return computed diff.
    """
    if not SHEETS_ENABLED:
        logger.info("Sheets disabled, skipping write for fuel_odo.")
        return {"ok": False, "message": "Sheets disabled"}
    try:
        prev = _last_odo_from_sheet(plate)
        diff = ""
        if prev is not None:
            try:
                diff = str(int(odo) - int(prev))
            except Exception:
                diff = ""
        ws = _ws(FUEL_SHEET)
        row = [now_str(), plate, str(odo), f"{float(fuel_usd):.2f}", invoice_received, diff]
        ws.append_row(row, value_input_option="USER_ENTERED")
        logger.info("Wrote fuel_odo for %s odo=%s fuel=%s invoice=%s diff=%s", plate, odo, fuel_usd, invoice_received, diff)
        return {"ok": True, "delta": diff, "mileage": odo}
    except Exception as e:
        logger.exception("Failed to append fuel_odo row: %s", e)
        return {"ok": False, "message": str(e)}

# --------------- Mission functions & in-memory staging ---------------
# We'll stage per-chat mission legs in memory (this avoids prompts); on complete sequence (d,a,d,a) write merged row.

CHAT_MISSION_LEGS: Dict[int, Dict[str, List[Dict[str, Any]]]] = {}  # chat_id -> plate -> legs list

def _append_mission_leg(chat_id: int, plate: str, ttype: str, city: str, driver: str):
    m = CHAT_MISSION_LEGS.setdefault(chat_id, {})
    legs = m.setdefault(plate, [])
    legs.append({"t": ttype, "city": city, "ts": now_str(), "driver": driver})
    return legs

def _try_resolve_and_write_mission(chat_id: int, plate: str):
    """
    If last 4 legs are d,a,d,a (depart,arrive,depart,arrive) for same driver & plate then merge and write.
    returns tuple(ok, message)
    """
    if not SHEETS_ENABLED:
        # still try to maintain in memory but don't write
        logger.info("Sheets disabled: mission write skipped.")
    m = CHAT_MISSION_LEGS.get(chat_id, {})
    legs = m.get(plate, [])
    if len(legs) < 4:
        return False, "not enough legs"
    seq = legs[-4:]
    if [x['t'] for x in seq] != ['d','a','d','a']:
        return False, "sequence not complete"
    d1,a1,d2,a2 = seq
    if d1['driver'] != a1['driver'] or d1['driver'] != d2['driver'] or d1['driver'] != a2['driver']:
        # drivers mismatch, cannot merge
        return False, "driver mismatch"
    driver = d1['driver']
    start_ts = d1['ts']
    end_ts = a2['ts']
    # compute duration in minutes
    try:
        s_dt = parse_ts(start_ts)
        e_dt = parse_ts(end_ts)
        duration_minutes = int((e_dt - s_dt).total_seconds() // 60) if s_dt and e_dt else ""
    except Exception:
        duration_minutes = ""
    # write merged row to sheet
    if SHEETS_ENABLED:
        try:
            ws = _ws(MISSION_SHEET)
            row = [
                driver, plate,
                d1['city'], d1['ts'],
                a1['city'], a1['ts'],
                d2['city'], d2['ts'],
                a2['city'], a2['ts'],
                start_ts, end_ts, str(duration_minutes)
            ]
            ws.append_row(row, value_input_option="USER_ENTERED")
            logger.info("Mission merged for %s %s", driver, plate)
        except Exception:
            logger.exception("Failed to write mission merged row.")
            # proceed, but still treat as success to clear legs
    # clear legs for this plate
    m[plate] = []
    CHAT_MISSION_LEGS[chat_id] = m
    # update summary counts
    try:
        update_mission_summary_for_period(driver, plate, start_ts, end_ts)
    except Exception:
        logger.exception("Failed to update mission summary.")
    return True, "merged"

# --------------- Mission summary (monthly) ---------------
def month_key(dt: datetime) -> str:
    return dt.strftime("%Y-%m")

def update_mission_summary_for_period(driver: str, plate: str, start_ts: str, end_ts: str):
    """
    Add mission_days for driver-month according to A-2 rules:
     - day-count based on start and end timestamps: if end is same day or before next day noon -> count 1 day, else increments by days crossing noon threshold.
    For simplicity, we compute mission days as number of midnights crossed, with noon cutoff per your spec:
      - For each day in [start_date, end_date], we check if the mission covered >12:00 of that day to count additional day. Implementing simplified logic:
        count days = max(1, ceil((end_date + end_time_offset - start_date) / 1 day)) using noon rule.
    We'll implement the noon rule: if end time on a day is after 12:00, we count that day; otherwise not.
    """
    try:
        s_dt = parse_ts(start_ts)
        e_dt = parse_ts(end_ts)
        if not s_dt or not e_dt:
            days = 1
        else:
            # compute naive day count according to your rule:
            # start day always counts
            days = 1
            # iterate each subsequent date between start and end
            cur = s_dt.date()
            last = e_dt.date()
            if cur == last:
                # same date: count 1
                days = 1
            else:
                # for days between start+1 .. end_date inclusive, check end-day noon rule for last day and intermediate days count fully
                # intermediate full days
                delta_days = (e_dt.date() - s_dt.date()).days
                # for days >0, count intermediate full days (delta_days - 1)
                if delta_days > 1:
                    days += (delta_days - 1)
                # for last day, count if end time > 12:00
                if e_dt.time() >= dtime(hour=12):
                    days += 1
            if days < 1:
                days = 1
    except Exception:
        days = 1

    # write/update mission_summary sheet: we will append a line per merged mission, and also optionally rebuild aggregated month report.
    if SHEETS_ENABLED:
        try:
            ws = _ws(SUMMARY_SHEET)
            month = month_key(s_dt if s_dt else datetime.now())
            per_diem = float(days) * PER_DIEM_PER_DAY_USD
            row = [driver, month, str(days), f"{per_diem:.2f}"]
            ws.append_row(row, value_input_option="USER_ENTERED")
            logger.info("Appended mission_summary row for %s %s days=%s", driver, month, days)
        except Exception:
            logger.exception("Failed to append mission_summary row.")

# --------------- Leave handler ---------------
def write_leave_row(driver: str, start_date: str, end_date: str, ltype: str, notes: str = "") -> bool:
    if not SHEETS_ENABLED:
        logger.info("Sheets disabled: skipping leave write.")
        return False
    try:
        ws = _ws(LEAVE_SHEET)
        row = [driver, start_date, end_date, ltype, notes, now_str()]
        ws.append_row(row, value_input_option="USER_ENTERED")
        logger.info("Wrote leave row for %s %s->%s type=%s", driver, start_date, end_date, ltype)
        return True
    except Exception:
        logger.exception("Failed to write leave row.")
        return False

# --------------- UI / Handlers ---------------

# helper: build plate keyboard
def build_plate_keyboard(prefix: str, plates: Optional[List[str]] = None) -> InlineKeyboardMarkup:
    if plates is None:
        plates = PLATES
    buttons = []
    row = []
    for i, p in enumerate(plates, 1):
        row.append(InlineKeyboardButton(p, callback_data=f"{prefix}|{p}"))
        if i % 3 == 0:
            buttons.append(row)
            row = []
    if row:
        buttons.append(row)
    return InlineKeyboardMarkup(buttons)

# ---------- /fuel flow ----------
# Steps:
# 1) /fuel -> show plate inline keyboard
# 2) user taps plate -> bot edits callback message and instructs user to send "<odo> <fuel>" (single minimal message)
# 3) user sends text like "123456 20" -> bot saves temporarily in user_data and asks "Invoice received? yes/no"
# 4) user replies "yes" or "no" -> bot writes to sheet and sends short english receipt.

async def cmd_fuel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # delete original command message to reduce clutter
    try:
        if update.effective_message:
            await update.effective_message.delete()
    except Exception:
        pass
    await update.effective_chat.send_message("Choose plate for fuel/odo entry:", reply_markup=build_plate_keyboard("fuel_plate"))

async def fuel_plate_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    # data format: fuel_plate|{plate}
    parts = q.data.split("|", 1)
    if len(parts) < 2:
        await q.edit_message_text("Invalid selection.")
        return
    plate = parts[1]
    # store pending plate in user_data
    context.user_data["pending_fuel_plate"] = plate
    # minimal instruction to user: send "ODO FUEL" (single message)
    try:
        await q.edit_message_text(f"Selected {plate}. Please send odo and fuel like: `123456 20` (odo km and fuel USD).", parse_mode=None)
    except Exception:
        # fallback send
        try:
            await q.message.chat.send_message(f"Selected {plate}. Please send odo and fuel like: 123456 20")
        except Exception:
            pass

async def handle_text_for_fuel_and_invoice(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    This handler serves two purposes:
     - If user has pending_fuel_plate and hasn't yet provided odo+fuel, treat text as odo+fuel.
     - If user has pending_fuel_invoice True, treat text as yes/no answer.
    Otherwise, it's general text and may be routed to mission/stage handlers.
    """
    user = update.effective_user
    chat = update.effective_chat
    txt = (update.effective_message.text or "").strip()
    if not txt:
        return

    # Invoice answer handling (expect yes/no)
    if context.user_data.get("pending_fuel_invoice"):
        ans = txt.lower()
        if ans not in ("yes", "no", "y", "n"):
            # ignore or send minimal guidance
            try:
                await update.effective_message.reply_text("Please reply: yes or no")
            except Exception:
                pass
            return
        invoice_answer = "Yes" if ans.startswith("y") else "No"
        # get staged values
        staged = context.user_data.pop("pending_fuel_staged", None)
        staged_plate = context.user_data.pop("pending_fuel_plate", None)
        staged_amount = None
        staged_odo = None
        if staged:
            staged_odo = staged.get("odo")
            staged_amount = staged.get("fuel")
        # write to sheet
        if staged_plate and staged_odo is not None and staged_amount is not None:
            res = write_fuel_odo_row(staged_plate, staged_odo, staged_amount, invoice_answer)
            # send short english receipt
            if res.get("ok"):
                delta = res.get("delta", "")
                odo_val = res.get("mileage")
                try:
                    await update.effective_chat.send_message(f"Plate {staged_plate} @ {odo_val} km + {staged_amount}$ fuel on {datetime.now().date()},\nOdo difference since last record: {delta} km")
                except Exception:
                    pass
                try:
                    # also DM operator who sent it
                    await context.bot.send_message(chat_id=user.id, text=f"Recorded {staged_plate}: {staged_odo}KM + ${staged_amount} fuel. Invoice={invoice_answer}. Delta={delta} km")
                except Exception:
                    pass
            else:
                try:
                    await context.bot.send_message(chat_id=user.id, text=f"Failed to record fuel entry: {res.get('message')}")
                except Exception:
                    pass
        else:
            try:
                await context.bot.send_message(chat_id=user.id, text="No staged fuel data found.")
            except Exception:
                pass
        context.user_data.pop("pending_fuel_invoice", None)
        return

    # If user selected plate and now sends odo+fuel
    pending_plate = context.user_data.get("pending_fuel_plate")
    if pending_plate:
        # Expect "123456 20" or "123456,20" etc.
        m = re.match(r'^\s*(\d+)\s*[,\s]\s*(\d+(?:\.\d+)?)\s*$', txt)
        if not m:
            # try more permissive: find two numbers
            nums = re.findall(r'(\d+(?:\.\d+)?)', txt)
            if len(nums) >= 2:
                odo = nums[0]
                fuel = nums[1]
            else:
                # invalid format: give minimal guidance and exit
                try:
                    await update.effective_message.reply_text("Send: <odo_km> <fuel_usd> (e.g. `123456 20`)")
                except Exception:
                    pass
                return
        else:
            odo = m.group(1)
            fuel = m.group(2)
        # stage data and ask invoice yes/no
        try:
            context.user_data["pending_fuel_staged"] = {"odo": int(odo), "fuel": float(fuel)}
        except Exception:
            try:
                context.user_data["pending_fuel_staged"] = {"odo": int(float(odo)), "fuel": float(fuel)}
            except Exception:
                await update.effective_message.reply_text("Invalid numbers.")
                return
        context.user_data["pending_fuel_invoice"] = True
        # ask invoice yes/no (minimal)
        try:
            await update.effective_message.reply_text("Invoice received? yes/no")
        except Exception:
            pass
        return

    # If none of the above, route to mission-free-text handler if relevant
    # (mission text entries are not used; missions use buttons)
    # For any other free text, do nothing (we avoid clutter)
    return

# ---------- /mission flows ----------
# Commands:
# - /mission_start -> show plate keyboard (callback ms_start)
# - after plate selected -> ask depart city (PP/SHV) via inline
# - record legs silently and try to detect merged mission
# - /mission_end -> same flow but arrival buttons (we use ms_end)
# No staff prompts.

async def cmd_mission_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        if update.effective_message:
            await update.effective_message.delete()
    except Exception:
        pass
    await update.effective_chat.send_message("Select plate to START mission:", reply_markup=build_plate_keyboard("ms_start"))

async def cmd_mission_end(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        if update.effective_message:
            await update.effective_message.delete()
    except Exception:
        pass
    await update.effective_chat.send_message("Select plate to END mission:", reply_markup=build_plate_keyboard("ms_end"))

def _kb_city(prefix: str, plate: str) -> InlineKeyboardMarkup:
    kb = [
        [InlineKeyboardButton("PP", callback_data=f"{prefix}|{plate}|PP"),
         InlineKeyboardButton("SHV", callback_data=f"{prefix}|{plate}|SHV")]
    ]
    return InlineKeyboardMarkup(kb)

async def mission_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    parts = q.data.split("|")
    if not parts:
        await q.edit_message_text("Invalid.")
        return
    prefix = parts[0]
    # ms_start -> choose depart city
    if prefix == "ms_start":
        plate = parts[1] if len(parts) > 1 else ""
        try:
            await q.edit_message_text("Select departure city:", reply_markup=_kb_city("ms_start_city", plate))
        except Exception:
            pass
        return
    if prefix == "ms_end":
        plate = parts[1] if len(parts) > 1 else ""
        try:
            await q.edit_message_text("Select arrival city:", reply_markup=_kb_city("ms_end_city", plate))
        except Exception:
            pass
        return

    # ms_start_city|{plate}|{city}
    if prefix == "ms_start_city":
        _, plate, city = parts
        legs = _append_mission_leg(q.message.chat.id, plate, "d", city, q.from_user.username or (q.from_user.first_name or ""))
        # do not send long prompts; just acknowledge briefly
        try:
            await q.edit_message_text(f"Recorded departure for {plate} at {now_str()}")
        except Exception:
            pass
        return

    if prefix == "ms_end_city":
        _, plate, city = parts
        legs = _append_mission_leg(q.message.chat.id, plate, "a", city, q.from_user.username or (q.from_user.first_name or ""))
        try:
            await q.edit_message_text(f"Recorded arrival for {plate} at {now_str()}")
        except Exception:
            pass
        # try to resolve merged mission
        ok, msg = _try_resolve_and_write_mission(q.message.chat.id, plate)
        if ok:
            # on success, we will send minimal notices about completed counts
            try:
                # count this driver's missions in month (quick scan of SUMMARY_SHEET if enabled)
                if SHEETS_ENABLED:
                    # compute month key from now
                    month = month_key(datetime.now())
                    driver = q.from_user.username or (q.from_user.first_name or "")
                    # quick count rows in SUMMARY_SHEET for driver+month
                    try:
                        ws = _ws(SUMMARY_SHEET)
                        rows = ws.get_all_records()
                        driver_count = sum(1 for r in rows if str(r.get("driver", "")).strip() == driver and str(r.get("month", "")).strip() == month)
                        # plate count from MISSION_SHEET: count merged rows for this plate in month
                        mws = _ws(MISSION_SHEET)
                        mvals = mws.get_all_records()
                        plate_count = sum(1 for r in mvals if str(r.get("plate", "")).strip() == plate and parse_ts(str(r.get("start", "")) or ""))
                    except Exception:
                        driver_count = 0
                        plate_count = 0
                    try:
                        await q.message.chat.send_message(f"Driver {driver} completed {driver_count} mission(s) in {month}.")
                        await q.message.chat.send_message(f"Plate {plate} completed {plate_count} mission(s) in {month}.")
                    except Exception:
                        pass
            except Exception:
                logger.exception("Failed to send mission summary messages.")
        return

# ---------- /leave command ----------
async def cmd_leave(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # zero prompt: expect args
    try:
        if update.effective_message:
            await update.effective_message.delete()
    except Exception:
        pass
    args = context.args
    if not args or len(args) < 4:
        await update.effective_chat.send_message("Usage: /leave <driver> <YYYY-MM-DD> <YYYY-MM-DD> <SL|AL> [notes]")
        return
    driver = args[0]
    start = args[1]
    end = args[2]
    ltype = args[3]
    notes = " ".join(args[4:]) if len(args) > 4 else ""
    # basic validation of dates
    try:
        datetime.strptime(start, DATE_FMT)
        datetime.strptime(end, DATE_FMT)
    except Exception:
        await update.effective_chat.send_message("Dates must be YYYY-MM-DD")
        return
    ok = write_leave_row(driver, start, end, ltype, notes)
    if ok:
        await update.effective_chat.send_message(f"Leave recorded for {driver} {start} -> {end}")
    else:
        await update.effective_chat.send_message("Failed to record leave (sheet error).")

# ---------- Start/End trip quick actions (retain simple start/end messages) ----------
# the user requested start/end show "Driver X start trip at YYYY-MM HH:mm" etc.
# We'll implement basic /start_trip and /end_trip commands using plate keyboard like before.

# We will record start/end in a simple RECORDS sheet (separate from mission). If you already have a RECORDS sheet, adapt names accordingly.
RECORDS_SHEET = os.getenv("RECORDS_SHEET", "Driver_Log")
RECORDS_HEADERS = ["date", "driver", "plate", "start_ts", "end_ts", "duration"]

def _append_start_record(driver: str, plate: str):
    if not SHEETS_ENABLED:
        logger.info("Sheets disabled: skipping start record.")
        return None
    try:
        ws = _ws(RECORDS_SHEET)
        # ensure header
        hdr = ws.row_values(1)
        if hdr != RECORDS_HEADERS:
            try:
                end_col = chr(ord('A') + len(RECORDS_HEADERS) - 1)
                ws.update(f"A1:{end_col}1", [RECORDS_HEADERS], value_input_option="USER_ENTERED")
            except Exception:
                try:
                    ws.delete_row(1)
                except Exception:
                    pass
                ws.insert_row(RECORDS_HEADERS, index=1)
        start_ts = now_str()
        row = [start_ts.split(" ")[0], driver, plate, start_ts, "", ""]
        ws.append_row(row, value_input_option="USER_ENTERED")
        return start_ts
    except Exception:
        logger.exception("Failed to append start record.")
        return None

def _record_end_and_compute_duration(driver: str, plate: str):
    if not SHEETS_ENABLED:
        logger.info("Sheets disabled: skipping end record.")
        return {"ts": now_str(), "duration": ""}
    try:
        ws = _ws(RECORDS_SHEET)
        vals = ws.get_all_values()
        if not vals or len(vals) <= 1:
            # no header / rows
            end_ts = now_str()
            row = [end_ts.split(" ")[0], driver, plate, "", end_ts, ""]
            ws.append_row(row, value_input_option="USER_ENTERED")
            return {"ts": end_ts, "duration": ""}
        # find last open start for this plate and driver scanning bottom-up
        start_idx = 1 if any("date" in c.lower() for c in vals[0] if c) else 0
        for i in range(len(vals)-1, start_idx-1, -1):
            r = vals[i]
            r_driver = r[1] if len(r) > 1 else ""
            r_plate = r[2] if len(r) > 2 else ""
            r_start = r[3] if len(r) > 3 else ""
            r_end = r[4] if len(r) > 4 else ""
            if str(r_driver).strip() == driver and str(r_plate).strip() == plate and (not r_end):
                rownum = i + 1
                end_ts = now_str()
                # compute duration
                dur = ""
                try:
                    s_dt = parse_ts(r_start) if r_start else None
                    e_dt = parse_ts(end_ts)
                    if s_dt and e_dt:
                        total_minutes = int((e_dt - s_dt).total_seconds() // 60)
                        if total_minutes >= 0:
                            hours = total_minutes // 60
                            minutes = total_minutes % 60
                            dur = f"{hours}h{minutes}m"
                except Exception:
                    dur = ""
                # update cells
                try:
                    ws.update_cell(rownum, 5, end_ts)  # end_ts in col 5 (1-index)
                    ws.update_cell(rownum, 6, dur)
                except Exception:
                    # fallback: replace row
                    try:
                        existing = ws.row_values(rownum)
                        while len(existing) < 6:
                            existing.append("")
                        existing[4] = end_ts
                        existing[5] = dur
                        ws.delete_rows(rownum)
                        ws.insert_row(existing, rownum)
                    except Exception:
                        logger.exception("Failed fallback update for end record.")
                return {"ts": end_ts, "duration": dur}
        # no open start found
        end_ts = now_str()
        ws.append_row([end_ts.split(" ")[0], driver, plate, "", end_ts, ""])
        return {"ts": end_ts, "duration": ""}
    except Exception:
        logger.exception("Failed to record end trip.")
        return {"ts": now_str(), "duration": ""}

async def start_trip_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        if update.effective_message:
            await update.effective_message.delete()
    except Exception:
        pass
    await update.effective_chat.send_message("Choose plate to START trip:", reply_markup=build_plate_keyboard("start_trip"))

async def end_trip_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        if update.effective_message:
            await update.effective_message.delete()
    except Exception:
        pass
    await update.effective_chat.send_message("Choose plate to END trip:", reply_markup=build_plate_keyboard("end_trip"))

async def start_end_plate_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    parts = q.data.split("|", 1)
    if len(parts) < 2:
        await q.edit_message_text("Invalid selection.")
        return
    prefix = parts[0]
    plate = parts[1]
    username = q.from_user.username or (q.from_user.first_name or "")
    if prefix == "start_trip":
        ts = _append_start_record(username, plate)
        text = f"Driver {username} start trip at {ts or now_str()}"
        try:
            await q.edit_message_text(text)
        except Exception:
            try:
                await q.message.chat.send_message(text)
            except Exception:
                pass
        return
    if prefix == "end_trip":
        res = _record_end_and_compute_duration(username, plate)
        ts = res.get("ts")
        dur = res.get("duration") or ""
        # count trips today and this month
        nowdt = datetime.now()
        n_today = 0
        n_month = 0
        if SHEETS_ENABLED:
            try:
                ws = _ws(RECORDS_SHEET)
                vals = ws.get_all_records()
                for r in vals:
                    if str(r.get("driver","")).strip() != username:
                        continue
                    s_ts = str(r.get("start_ts","")).strip()
                    e_ts = str(r.get("end_ts","")).strip()
                    if not s_ts or not e_ts:
                        continue
                    sdt = parse_ts(s_ts)
                    if not sdt:
                        continue
                    if sdt.date() == nowdt.date():
                        n_today += 1
                    if sdt.year == nowdt.year and sdt.month == nowdt.month:
                        n_month += 1
            except Exception:
                logger.exception("Failed counting trips for stats.")
        # respond
        text = f"Driver {username} end trip at {ts} (duration {dur}).\nDriver {username} completed {n_today} trips today\nDriver {username} completed {n_month} trips this month."
        try:
            await q.edit_message_text(text)
        except Exception:
            try:
                await q.message.chat.send_message(text)
            except Exception:
                pass
        return

# ---------- misc handlers ----------
async def cmd_help(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        if update.effective_message:
            await update.effective_message.delete()
    except Exception:
        pass
    await update.effective_chat.send_message("Help: Use /fuel, /start_trip, /end_trip, /mission_start, /mission_end, /leave")

# --------------- Application setup & main ---------------

def register_handlers(application):
    # commands
    application.add_handler(CommandHandler("help", cmd_help))
    application.add_handler(CommandHandler("fuel", cmd_fuel))
    application.add_handler(CallbackQueryHandler(fuel_plate_callback, pattern=r"^fuel_plate\|"))
    application.add_handler(CommandHandler("mission_start", cmd_mission_start))
    application.add_handler(CommandHandler("mission_end", cmd_mission_end))
    application.add_handler(CallbackQueryHandler(mission_callback, pattern=r"^ms_"))
    application.add_handler(CommandHandler("leave", cmd_leave))
    application.add_handler(CommandHandler("start_trip", start_trip_cmd))
    application.add_handler(CommandHandler("end_trip", end_trip_cmd))
    application.add_handler(CallbackQueryHandler(start_end_plate_callback, pattern=r"^(start_trip|end_trip)\|"))
    # text handler for fuel staged messages and invoice yes/no
    application.add_handler(MessageHandler(filters.TEXT & (~filters.COMMAND), handle_text_for_fuel_and_invoice))
    # no generic auto prompts

def ensure_env():
    if not BOT_TOKEN:
        raise RuntimeError("BOT_TOKEN not set")

def main():
    ensure_env()
    # init google sheets client if creds provided
    _init_gs_client()
    # ensure headers (safe)
    try:
        ensure_headers_once()
    except Exception:
        logger.exception("Header ensure failed at startup.")

    # build application
    application = ApplicationBuilder().token(BOT_TOKEN).build()

    # post-init hook to set commands and ensure webhook deletion if needed
    async def _post_init(app):
        try:
            # delete any existing webhook to avoid getUpdates conflict when polling (if not using webhook)
            try:
                await app.bot.delete_webhook()
                logger.info("Deleted existing webhook (post_init).")
            except Exception:
                logger.debug("delete_webhook no-op or failed.")

            # set commands
            try:
                await app.bot.set_my_commands([
                    BotCommand("fuel", "Record fuel + odo (choose plate then send odo and fuel)"),
                    BotCommand("start_trip", "Start a trip (select plate)"),
                    BotCommand("end_trip", "End a trip (select plate)"),
                    BotCommand("mission_start", "Start a mission (select plate)"),
                    BotCommand("mission_end", "End a mission (select plate)"),
                    BotCommand("leave", "Record leave: /leave <driver> <start> <end> <SL|AL> [notes]"),
                    BotCommand("help", "Show help"),
                ])
                logger.info("Bot commands set (post_init).")
            except Exception:
                logger.exception("Failed to set bot commands in post_init.")
        except Exception:
            logger.exception("post_init tasks failed.")

    # attach post_init in a safe way
    try:
        if getattr(application, "post_init", None) is None:
            application.post_init = []
        application.post_init.append(_post_init)
    except Exception:
        # fallback: run immediately before polling
        logger.exception("Could not append to application.post_init; will run fallback post_init before polling.")
        import asyncio
        try:
            asyncio.get_event_loop().run_until_complete(_post_init(application))
        except Exception:
            logger.exception("Fallback post_init failed to run synchronously.")

    # register handlers
    register_handlers(application)

    # run either webhook or polling
    try:
        if WEBHOOK_URL:
            logger.info("Starting webhook at %s", WEBHOOK_URL)
            application.run_webhook(listen="0.0.0.0", port=PORT, webhook_url=WEBHOOK_URL)
        else:
            logger.info("Starting polling...")
            application.run_polling()
    except Exception:
        logger.exception("Application run failed.")
        raise

if __name__ == "__main__":
    main()
