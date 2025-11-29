#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
driver-bot — final corrected version (post_init bug fixed)

Notes:
 - Fixed: application.post_init must be a coroutine (callable) — assign async fn directly.
 - No use of application.post_init as a list; no append.
 - No create_task nor un-awaited coroutine left.
 - Keeps features: /fuel (plate select -> send "ODO FUEL" -> "Invoice? yes/no" -> write row), missions merged rows, /leave zero-prompt, start/end trip, headers enforcement.
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
logger = logging.getLogger("driver-bot-final-fixed")

BOT_TOKEN = os.getenv("BOT_TOKEN")
WEBHOOK_URL = os.getenv("WEBHOOK_URL", "").strip()
PORT = int(os.getenv("PORT", "8443"))

# Sheets config
GOOGLE_CREDS_JSON = os.getenv("GOOGLE_CREDS_JSON")  # full json text or base64
GSHEET_KEY = os.getenv("GSHEET_KEY")  # spreadsheet id

# Sheet names (env overrides allowed)
FUEL_SHEET = os.getenv("FuelSheet", "fuel_odo")
MISSION_SHEET = os.getenv("MissionSheet", "missions")
SUMMARY_SHEET = os.getenv("SummarySheet", "mission_summary")
LEAVE_SHEET = os.getenv("LeaveSheet", "leave")
RECORDS_SHEET = os.getenv("RECORDS_SHEET", "Driver_Log")

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
        txt = GOOGLE_CREDS_JSON.strip()
        try:
            if txt.startswith("{"):
                creds_dict = json.loads(txt)
            else:
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
        # create if missing
        ws = sh.add_worksheet(title=tab_name, rows="2000", cols="30")
    return ws

# Header templates (exact match)
HEADERS = {
    FUEL_SHEET: ["timestamp", "plate", "odo", "fuel_usd", "invoice_received", "odo_diff"],
    MISSION_SHEET: ["driver","plate","depart1_city","depart1_ts","arrive1_city","arrive1_ts","depart2_city","depart2_ts","arrive2_city","arrive2_ts","start","end","duration_minutes"],
    SUMMARY_SHEET: ["driver","month","mission_days","per_diem_usd"],
    LEAVE_SHEET: ["driver","start_date","end_date","type","notes","recorded_at"],
    RECORDS_SHEET: ["date", "driver", "plate", "start_ts", "end_ts", "duration"],
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
                try:
                    end_col = chr(ord('A') + len(hdr) - 1)
                    rng = f"A1:{end_col}1"
                    ws.update(rng, [hdr], value_input_option="USER_ENTERED")
                except Exception:
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
    if not SHEETS_ENABLED:
        return None
    try:
        ws = _ws(FUEL_SHEET)
        vals = ws.get_all_values()
        if not vals or len(vals) <= 1:
            return None
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
CHAT_MISSION_LEGS: Dict[int, Dict[str, List[Dict[str, Any]]]] = {}

def _append_mission_leg(chat_id: int, plate: str, ttype: str, city: str, driver: str):
    m = CHAT_MISSION_LEGS.setdefault(chat_id, {})
    legs = m.setdefault(plate, [])
    legs.append({"t": ttype, "city": city, "ts": now_str(), "driver": driver})
    return legs

def _try_resolve_and_write_mission(chat_id: int, plate: str):
    m = CHAT_MISSION_LEGS.get(chat_id, {})
    legs = m.get(plate, [])
    if len(legs) < 4:
        return False, "not enough legs"
    seq = legs[-4:]
    if [x['t'] for x in seq] != ['d','a','d','a']:
        return False, "sequence not complete"
    d1,a1,d2,a2 = seq
    if d1['driver'] != a1['driver'] or d1['driver'] != d2['driver'] or d1['driver'] != a2['driver']:
        return False, "driver mismatch"
    driver = d1['driver']
    start_ts = d1['ts']
    end_ts = a2['ts']
    try:
        s_dt = parse_ts(start_ts)
        e_dt = parse_ts(end_ts)
        duration_minutes = int((e_dt - s_dt).total_seconds() // 60) if s_dt and e_dt else ""
    except Exception:
        duration_minutes = ""
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
    m[plate] = []
    CHAT_MISSION_LEGS[chat_id] = m
    try:
        update_mission_summary_for_period(driver, plate, start_ts, end_ts)
    except Exception:
        logger.exception("Failed to update mission summary.")
    return True, "merged"

# --------------- Mission summary (monthly) ---------------
def month_key(dt: datetime) -> str:
    return dt.strftime("%Y-%m")

def update_mission_summary_for_period(driver: str, plate: str, start_ts: str, end_ts: str):
    try:
        s_dt = parse_ts(start_ts)
        e_dt = parse_ts(end_ts)
        if not s_dt or not e_dt:
            days = 1
        else:
            days = 1
            cur = s_dt.date()
            last = e_dt.date()
            if cur == last:
                days = 1
            else:
                delta_days = (e_dt.date() - s_dt.date()).days
                if delta_days > 1:
                    days += (delta_days - 1)
                if e_dt.time() >= dtime(hour=12):
                    days += 1
            if days < 1:
                days = 1
    except Exception:
        days = 1
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

# --------------- Records (start/end trip) ---------------
def _append_start_record(driver: str, plate: str):
    if not SHEETS_ENABLED:
        logger.info("Sheets disabled: skipping start record.")
        return now_str()
    try:
        ws = _ws(RECORDS_SHEET)
        hdr = ws.row_values(1)
        if hdr != HEADERS[RECORDS_SHEET]:
            try:
                end_col = chr(ord('A') + len(HEADERS[RECORDS_SHEET]) - 1)
                ws.update(f"A1:{end_col}1", [HEADERS[RECORDS_SHEET]], value_input_option="USER_ENTERED")
            except Exception:
                try:
                    ws.delete_row(1)
                except Exception:
                    pass
                ws.insert_row(HEADERS[RECORDS_SHEET], index=1)
        start_ts = now_str()
        row = [start_ts.split(" ")[0], driver, plate, start_ts, "", ""]
        ws.append_row(row, value_input_option="USER_ENTERED")
        return start_ts
    except Exception:
        logger.exception("Failed to append start record.")
        return now_str()

def _record_end_and_compute_duration(driver: str, plate: str):
    if not SHEETS_ENABLED:
        return {"ts": now_str(), "duration": ""}
    try:
        ws = _ws(RECORDS_SHEET)
        vals = ws.get_all_values()
        if not vals or len(vals) <= 1:
            end_ts = now_str()
            ws.append_row([end_ts.split(" ")[0], driver, plate, "", end_ts, ""])
            return {"ts": end_ts, "duration": ""}
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
                try:
                    ws.update_cell(rownum, 5, end_ts)
                    ws.update_cell(rownum, 6, dur)
                except Exception:
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
        end_ts = now_str()
        ws.append_row([end_ts.split(" ")[0], driver, plate, "", end_ts, ""])
        return {"ts": end_ts, "duration": ""}
    except Exception:
        logger.exception("Failed to record end trip.")
        return {"ts": now_str(), "duration": ""}

# --------------- UI / Handlers ---------------
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

# /fuel flow
async def cmd_fuel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        if update.effective_message:
            await update.effective_message.delete()
    except Exception:
        pass
    await update.effective_chat.send_message("Choose plate for fuel/odo entry:", reply_markup=build_plate_keyboard("fuel_plate"))

async def fuel_plate_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    parts = q.data.split("|", 1)
    if len(parts) < 2:
        await q.edit_message_text("Invalid selection.")
        return
    plate = parts[1]
    context.user_data["pending_fuel_plate"] = plate
    try:
        await q.edit_message_text(f"Selected {plate}. Please send odo and fuel like: `123456 20` (odo km and fuel USD).")
    except Exception:
        try:
            await q.message.chat.send_message(f"Selected {plate}. Please send odo and fuel like: 123456 20")
        except Exception:
            pass

async def handle_text_for_fuel_and_invoice(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    chat = update.effective_chat
    txt = (update.effective_message.text or "").strip()
    if not txt:
        return
    if context.user_data.get("pending_fuel_invoice"):
        ans = txt.lower()
        if ans not in ("yes", "no", "y", "n"):
            try:
                await update.effective_message.reply_text("Please reply: yes or no")
            except Exception:
                pass
            return
        invoice_answer = "Yes" if ans.startswith("y") else "No"
        staged = context.user_data.pop("pending_fuel_staged", None)
        staged_plate = context.user_data.pop("pending_fuel_plate", None)
        if staged and staged_plate:
            staged_odo = staged.get("odo")
            staged_amount = staged.get("fuel")
            res = write_fuel_odo_row(staged_plate, staged_odo, staged_amount, invoice_answer)
            if res.get("ok"):
                delta = res.get("delta", "")
                odo_val = res.get("mileage")
                try:
                    await update.effective_chat.send_message(f"Plate {staged_plate} @ {odo_val} km + {staged_amount}$ fuel on {datetime.now().date()},\nOdo difference since last record: {delta} km")
                except Exception:
                    pass
                try:
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
    pending_plate = context.user_data.get("pending_fuel_plate")
    if pending_plate:
        m = re.match(r'^\s*(\d+)\s*[,\s]\s*(\d+(?:\.\d+)?)\s*$', txt)
        if not m:
            nums = re.findall(r'(\d+(?:\.\d+)?)', txt)
            if len(nums) >= 2:
                odo = nums[0]
                fuel = nums[1]
            else:
                try:
                    await update.effective_message.reply_text("Send: <odo_km> <fuel_usd> (e.g. `123456 20`)")
                except Exception:
                    pass
                return
        else:
            odo = m.group(1)
            fuel = m.group(2)
        try:
            context.user_data["pending_fuel_staged"] = {"odo": int(odo), "fuel": float(fuel)}
        except Exception:
            try:
                context.user_data["pending_fuel_staged"] = {"odo": int(float(odo)), "fuel": float(fuel)}
            except Exception:
                await update.effective_message.reply_text("Invalid numbers.")
                return
        context.user_data["pending_fuel_invoice"] = True
        try:
            await update.effective_message.reply_text("Invoice received? yes/no")
        except Exception:
            pass
        return
    return

# Mission handlers
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
    if prefix == "ms_start_city":
        _, plate, city = parts
        _append_mission_leg(q.message.chat.id, plate, "d", city, q.from_user.username or (q.from_user.first_name or ""))
        try:
            await q.edit_message_text(f"Recorded departure for {plate} at {now_str()}")
        except Exception:
            pass
        return
    if prefix == "ms_end_city":
        _, plate, city = parts
        _append_mission_leg(q.message.chat.id, plate, "a", city, q.from_user.username or (q.from_user.first_name or ""))
        try:
            await q.edit_message_text(f"Recorded arrival for {plate} at {now_str()}")
        except Exception:
            pass
        ok, msg = _try_resolve_and_write_mission(q.message.chat.id, plate)
        if ok:
            try:
                if SHEETS_ENABLED:
                    month = month_key(datetime.now())
                    driver = q.from_user.username or (q.from_user.first_name or "")
                    try:
                        ws = _ws(SUMMARY_SHEET)
                        rows = ws.get_all_records()
                        driver_count = sum(1 for r in rows if str(r.get("driver", "")).strip() == driver and str(r.get("month", "")).strip() == month)
                    except Exception:
                        driver_count = 0
                    try:
                        await q.message.chat.send_message(f"Driver {driver} completed {driver_count} mission(s) in {month}.")
                    except Exception:
                        pass
            except Exception:
                logger.exception("Failed to send mission summary messages.")
        return

# /leave command
async def cmd_leave(update: Update, context: ContextTypes.DEFAULT_TYPE):
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

# start/end trip commands and callbacks
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
        text = f"Driver {username} end trip at {ts} (duration {dur}).\nDriver {username} completed {n_today} trips today\nDriver {username} completed {n_month} trips this month."
        try:
            await q.edit_message_text(text)
        except Exception:
            try:
                await q.message.chat.send_message(text)
            except Exception:
                pass
        return

async def cmd_help(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        if update.effective_message:
            await update.effective_message.delete()
    except Exception:
        pass
    await update.effective_chat.send_message("Help: Use /fuel, /start_trip, /end_trip, /mission_start, /mission_end, /leave")

# --------------- Application setup & main ---------------

def register_handlers(application):
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
    application.add_handler(CallbackQueryHandler(mission_callback, pattern=r"^ms_"))
    application.add_handler(CallbackQueryHandler(start_end_plate_callback, pattern=r"^(start_trip|end_trip)\|"))
    application.add_handler(CallbackQueryHandler(fuel_plate_callback, pattern=r"^fuel_plate\|"))
    application.add_handler(MessageHandler(filters.TEXT & (~filters.COMMAND), handle_text_for_fuel_and_invoice))

def ensure_env():
    if not BOT_TOKEN:
        raise RuntimeError("BOT_TOKEN not set")

def main():
    ensure_env()
    _init_gs_client()
    try:
        ensure_headers_once()
    except Exception:
        logger.exception("Header ensure failed at startup.")

    application = ApplicationBuilder().token(BOT_TOKEN).build()

    # post-init coroutine (assign directly, not list)
    async def _post_init(app):
        try:
            try:
                await app.bot.delete_webhook()
                logger.info("Deleted existing webhook (post_init).")
            except Exception:
                logger.debug("delete_webhook no-op or failed.")
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

    # assign post_init as coroutine (callable)
    application.post_init = _post_init

    # register handlers
    register_handlers(application)

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
