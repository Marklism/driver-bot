#!/usr/bin/env python3
# bot.py — driver-bot consolidated fixed version
# Features:
#  - start/end trip logging (records, durations, daily/monthly counts)
#  - fuel+odo combined row writing with invoice question
#  - leave command (zero prompt) writing to Leave sheet
#  - mission start/end flow, merged roundtrip detection, mission_summary with per-diem (A-2)
#  - Google Sheets optional (graceful fallback), headers auto-enforced
#  - post_init deleteWebhook + set_my_commands awaited
#  - no create_task usage, no pin / top functionality
#
# Env:
#  BOT_TOKEN - required
#  GOOGLE_CREDS_JSON or GOOGLE_CREDS_BASE64 - optional, if sheet writes desired
#  GSHEET_KEY - required for sheet writes
#  PLATE_LIST - optional comma-separated plates
#  FUEL_SHEET, MISSION_SHEET, SUMMARY_SHEET, LEAVE_SHEET - optional sheet/tab names

import os
import json
import base64
import logging
import re
from datetime import datetime, timedelta, time as dtime
from typing import Optional, List, Dict, Any

import gspread
from oauth2client.service_account import ServiceAccountCredentials

from telegram import (
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    Update,
    BotCommand,
    ForceReply,
)
from telegram.ext import (
    ApplicationBuilder,
    CommandHandler,
    CallbackQueryHandler,
    MessageHandler,
    filters,
    ContextTypes,
    PicklePersistence,
)

# -------- Logging ----------
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("driver-bot-fixed")

# -------- Environment & defaults ----------
BOT_TOKEN = os.getenv("BOT_TOKEN")
GOOGLE_CREDS_JSON = os.getenv("GOOGLE_CREDS_JSON") or os.getenv("GOOGLE_CREDS_BASE64")
GSHEET_KEY = os.getenv("GSHEET_KEY")

PLATE_LIST = os.getenv("PLATE_LIST", "2BB-3071,2BB-0809,2CI-8066,2CK-8066").split(",")
PLATES = [p.strip() for p in PLATE_LIST if p.strip()]

FUEL_SHEET = os.getenv("FUEL_SHEET", "fuel_odo")
MISSION_SHEET = os.getenv("MISSION_SHEET", "missions")
SUMMARY_SHEET = os.getenv("SUMMARY_SHEET", "mission_summary")
LEAVE_SHEET = os.getenv("LEAVE_SHEET", "leave")
RECORDS_SHEET = os.getenv("RECORDS_SHEET", "Driver_Log")

# Per-diem rate for A-2
PER_DIEM_USD = float(os.getenv("PER_DIEM_USD", "15.0"))

# Regular expressions
AMOUNT_RE = re.compile(r'(\d+(?:\.\d+)?)')
ODO_RE = re.compile(r'(\d{3,7})')  # simplistic odometer extraction

# -------- Google Sheets helpers (optional) ----------
GS_ENABLED = False
GCLIENT = None

SCOPES = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]

def _load_creds(json_text: str) -> dict:
    # support raw JSON or base64
    try:
        # detect base64 vs plain
        s = json_text.strip()
        if s.startswith("{"):
            return json.loads(s)
        # try base64
        padded = "".join(s.split())
        missing = len(padded) % 4
        if missing:
            padded += "=" * (4 - missing)
        decoded = base64.b64decode(padded)
        return json.loads(decoded)
    except Exception:
        logger.exception("Failed to parse GOOGLE_CREDS JSON/Base64")
        raise

def gs_init():
    global GS_ENABLED, GCLIENT
    if not GOOGLE_CREDS_JSON or not GSHEET_KEY:
        logger.error("Google Sheets not configured: set GOOGLE_CREDS_JSON and GSHEET_KEY if you want sheet writes.")
        GS_ENABLED = False
        return
    try:
        creds_obj = _load_creds(GOOGLE_CREDS_JSON)
        creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_obj, SCOPES)
        GCLIENT = gspread.authorize(creds)
        GS_ENABLED = True
        logger.info("Google Sheets authorized.")
    except Exception:
        logger.exception("Failed to initialize Google Sheets client.")
        GS_ENABLED = False

def _ws(tab: str):
    if not GS_ENABLED:
        raise RuntimeError("Google Sheets client or key not configured")
    sh = GCLIENT.open_by_key(GSHEET_KEY)
    try:
        return sh.worksheet(tab)
    except Exception:
        # create if missing
        try:
            # default cols sufficient
            ws = sh.add_worksheet(title=tab, rows="2000", cols="20")
            return ws
        except Exception:
            # fall back to open existing
            return sh.worksheet(tab)

# Ensure headers (strict exact match)
HEADERS = {
    FUEL_SHEET: ["timestamp", "driver", "plate", "odo", "odo_diff", "fuel_usd", "invoice_received"],
    MISSION_SHEET: ["driver", "plate", "start_ts", "end_ts", "duration_min", "leg1_dep_city", "leg1_dep_ts", "leg1_arr_city", "leg1_arr_ts", "leg2_dep_city", "leg2_dep_ts", "leg2_arr_city", "leg2_arr_ts"],
    SUMMARY_SHEET: ["driver", "year_month", "mission_days", "per_diem_usd"],
    LEAVE_SHEET: ["driver", "start_date", "end_date", "type", "notes", "record_ts"],
    RECORDS_SHEET: ["date", "driver", "plate", "start_ts", "end_ts", "duration"]
}

def ensure_headers_for(tab_name: str):
    if not GS_ENABLED:
        logger.info("Skipping header ensure: Google Sheets not enabled.")
        return
    try:
        ws = _ws(tab_name)
        cur = ws.row_values(1)
        expected = HEADERS.get(tab_name, [])
        if cur != expected:
            # overwrite first row
            if cur:
                try:
                    ws.delete_row(1)
                except Exception:
                    pass
            if expected:
                try:
                    ws.insert_row(expected, 1)
                except Exception:
                    logger.exception("Failed to insert header for %s", tab_name)
            logger.info("Updated header for %s", tab_name)
    except Exception:
        logger.exception("Failed to ensure headers for %s", tab_name)

# initialize sheets quietly
try:
    gs_init()
    if GS_ENABLED:
        for t in [FUEL_SHEET, MISSION_SHEET, SUMMARY_SHEET, LEAVE_SHEET, RECORDS_SHEET]:
            try:
                ensure_headers_for(t)
            except Exception:
                pass
except Exception:
    # already logged
    pass

# -------- Utilities ----------
def now_str(fmt="%Y-%m-%d %H:%M:%S"):
    return datetime.now().strftime(fmt)

def parse_ts(s: str) -> Optional[datetime]:
    try:
        return datetime.strptime(s, "%Y-%m-%d %H:%M:%S")
    except Exception:
        try:
            return datetime.strptime(s, "%Y-%m-%d %H:%M")
        except Exception:
            return None

def compute_duration_min(start_ts: str, end_ts: str) -> int:
    s = parse_ts(start_ts)
    e = parse_ts(end_ts)
    if not s or not e:
        return 0
    delta = e - s
    return int(delta.total_seconds() // 60)

# read last odo from fuel sheet for plate
def get_last_odo_from_sheet(plate: str) -> Optional[int]:
    if not GS_ENABLED:
        return None
    try:
        ws = _ws(FUEL_SHEET)
        vals = ws.get_all_values()
        if not vals or len(vals) <= 1:
            return None
        # assume header at row 1
        for r in reversed(vals[1:]):
            if len(r) >= 4 and str(r[2]).strip() == plate:
                m = ODO_RE.search(str(r[3]))
                if m:
                    return int(m.group(1))
        return None
    except Exception:
        logger.exception("Failed reading last odo for %s", plate)
        return None

def append_row_sheet(tab: str, row: List[Any]) -> bool:
    if not GS_ENABLED:
        logger.info("GS disabled: not writing to sheet %s: row=%s", tab, row)
        return False
    try:
        ws = _ws(tab)
        ws.append_row(row, value_input_option="USER_ENTERED")
        return True
    except Exception:
        logger.exception("Failed to append row to %s", tab)
        return False

# -------- Bot state caches ----------
# in-memory mission leg tracker per chat_id -> plate -> list of legs
# leg: {"t":"d" or "a", "city": "PP"/"SHV", "ts": "...", "driver": username}
MISSION_CACHE: Dict[int, Dict[str, List[Dict[str, Any]]]] = {}

# simple in-memory odo cache fallback
ODO_CACHE: Dict[str, int] = {}

# pending interactions stored in user_data (context.user_data)
# pending_fin_simple: {"type":"fuel", "plate":plate, "step":"amount"|"invoice", "amount":..., "odo":...}
# pending_mission: {"action":"start"|"end", "plate": plate, "step":..., "departure"/"arrival": ...}

# -------- Keyboard builders ----------
def build_plate_keyboard(prefix: str, allowed_plates: Optional[List[str]] = None):
    plates = allowed_plates if allowed_plates is not None else PLATES
    rows = []
    row = []
    for i, p in enumerate(plates, 1):
        row.append(InlineKeyboardButton(p, callback_data=f"{prefix}|{p}"))
        if i % 3 == 0:
            rows.append(row)
            row = []
    if row:
        rows.append(row)
    return InlineKeyboardMarkup(rows)

def build_city_kb(prefix: str, plate: str):
    return InlineKeyboardMarkup([[InlineKeyboardButton("PP", callback_data=f"{prefix}|{plate}|PP"),
                                  InlineKeyboardButton("SHV", callback_data=f"{prefix}|{plate}|SHV")]])

# -------- Command handlers --------

async def start_trip_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # delete user message to keep chat tidy
    try:
        if update.effective_message:
            await update.effective_message.delete()
    except Exception:
        pass
    # show plate keyboard to pick start
    await update.effective_chat.send_message("Select plate to START:", reply_markup=build_plate_keyboard("start"))

async def end_trip_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        if update.effective_message:
            await update.effective_message.delete()
    except Exception:
        pass
    await update.effective_chat.send_message("Select plate to END:", reply_markup=build_plate_keyboard("end"))

# record start trip
def record_start_trip_sheet(driver: str, plate: str) -> Dict[str, Any]:
    ts = now_str()
    row = [now_str("%Y-%m-%d"), driver, plate, ts, "", ""]
    ok = append_row_sheet(RECORDS_SHEET, row)
    logger.info("record_start_trip: driver=%s plate=%s ok=%s", driver, plate, ok)
    return {"ok": ok, "ts": ts}

# record end trip (search last open start and update, else append end-only row)
def record_end_trip_sheet(driver: str, plate: str) -> Dict[str, Any]:
    ts = now_str()
    try:
        if not GS_ENABLED:
            # local-only fallback: just return
            return {"ok": False, "message": "Sheets disabled", "ts": ts}
        ws = _ws(RECORDS_SHEET)
        vals = ws.get_all_values()
        if not vals:
            # no header or rows: append end-only
            row = [now_str("%Y-%m-%d"), driver, plate, "", ts, ""]
            ws.append_row(row, value_input_option="USER_ENTERED")
            return {"ok": True, "ts": ts, "duration": ""}
        start_idx = 1 if any("date" in c.lower() for c in vals[0] if c) else 0
        for idx in range(len(vals)-1, start_idx-1, -1):
            r = vals[idx]
            rec_plate = r[2] if len(r) > 2 else ""
            rec_end = r[4] if len(r) > 4 else ""
            rec_start = r[3] if len(r) > 3 else ""
            rec_driver = r[1] if len(r) > 1 else ""
            if str(rec_plate).strip() == plate and not str(rec_end).strip() and str(rec_driver).strip() == driver:
                row_number = idx + 1
                duration_min = compute_duration_min(rec_start, ts) if rec_start else 0
                duration_text = f"{duration_min//60}h{duration_min%60}m" if duration_min else ""
                try:
                    ws.update_cell(row_number, 5, ts)  # End column (1-indexed)
                    ws.update_cell(row_number, 6, duration_text)
                except Exception:
                    # fallback replace row
                    existing = ws.row_values(row_number)
                    while len(existing) < 6:
                        existing.append("")
                    existing[4] = ts
                    existing[5] = duration_text
                    try:
                        ws.delete_rows(row_number)
                    except Exception:
                        pass
                    try:
                        ws.insert_row(existing, row_number)
                    except Exception:
                        pass
                # counts
                return {"ok": True, "ts": ts, "duration": duration_text}
        # no open start found
        row = [now_str("%Y-%m-%d"), driver, plate, "", ts, ""]
        ws.append_row(row, value_input_option="USER_ENTERED")
        return {"ok": True, "ts": ts, "duration": ""}
    except Exception:
        logger.exception("Failed record_end_trip_sheet")
        return {"ok": False, "message": "Sheet error", "ts": ts}

def count_trips_for_day(driver: str, date_dt: datetime) -> int:
    try:
        if not GS_ENABLED:
            return 0
        ws = _ws(RECORDS_SHEET)
        vals = ws.get_all_values()
        if not vals:
            return 0
        start_idx = 1 if any("date" in c.lower() for c in vals[0] if c) else 0
        cnt = 0
        for r in vals[start_idx:]:
            dr = r[1] if len(r) > 1 else ""
            s_ts = r[3] if len(r) > 3 else ""
            e_ts = r[4] if len(r) > 4 else ""
            if dr != driver:
                continue
            if not s_ts or not e_ts:
                continue
            sdt = parse_ts(s_ts)
            if not sdt:
                continue
            if sdt.date() == date_dt.date():
                cnt += 1
        return cnt
    except Exception:
        logger.exception("count_trips_for_day")
        return 0

def count_trips_for_month(driver: str, month_start: datetime, month_end: datetime) -> int:
    try:
        if not GS_ENABLED:
            return 0
        ws = _ws(RECORDS_SHEET)
        vals = ws.get_all_values()
        if not vals:
            return 0
        start_idx = 1 if any("date" in c.lower() for c in vals[0] if c) else 0
        cnt = 0
        for r in vals[start_idx:]:
            dr = r[1] if len(r) > 1 else ""
            s_ts = r[3] if len(r) > 3 else ""
            e_ts = r[4] if len(r) > 4 else ""
            if dr != driver:
                continue
            if not s_ts or not e_ts:
                continue
            sdt = parse_ts(s_ts)
            if not sdt:
                continue
            if month_start <= sdt < month_end:
                cnt += 1
        return cnt
    except Exception:
        logger.exception("count_trips_for_month")
        return 0

# -------- Fuel flow & callbacks --------
# Mode:
#  - /fuel -> show plate keyboard
#  - callback fin_plate|fuel|<plate> -> set pending_fin_simple step=amount and ask for amount (ForceReply, minimal)
#  - when user replies with amount [+ optional odo], bot asks "Invoice received? yes/no" (ForceReply)
#  - final reply triggers write row and short receipt to user

async def fuel_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # delete command
    try:
        if update.effective_message:
            await update.effective_message.delete()
    except Exception:
        pass
    await update.effective_chat.send_message("Select plate for fuel:", reply_markup=build_plate_keyboard("fin_plate|fuel"))

async def plate_callback_finance(update: Update, context: ContextTypes.DEFAULT_TYPE, typ: str, plate: str):
    # start pending simple flow
    context.user_data["pending_fin_simple"] = {"type": "fuel", "plate": plate, "step": "amount"}
    fr = ForceReply(selective=False)
    # minimal prompt
    try:
        m = await context.bot.send_message(chat_id=update.effective_chat.id, text="Amount (you may include odo: e.g. 23.5 odo:12345)", reply_markup=fr)
        context.user_data["pending_fin_simple"]["prompt_chat"] = m.chat_id
        context.user_data["pending_fin_simple"]["prompt_msg_id"] = m.message_id
    except Exception:
        logger.exception("Failed to prompt for fuel amount.")
        context.user_data.pop("pending_fin_simple", None)

# central callback for plate actions and mission flows
async def callback_query_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    if not query or not query.data:
        return
    await query.answer()
    data = query.data
    # navigation
    if data.startswith("start|") or data.startswith("end|"):
        # old style start/end inline (if present)
        parts = data.split("|", 1)
        if len(parts) != 2:
            try:
                await query.edit_message_text("Invalid selection.")
            except Exception:
                pass
            return
        action, plate = parts
        username = (query.from_user.username or query.from_user.full_name or "unknown")
        if action == "start":
            res = record_start_trip_sheet(username, plate)
            # acknowledge: short message with start ts
            try:
                await query.edit_message_text(f"Driver {username} start trip at {res.get('ts')}")
            except Exception:
                try:
                    await query.message.chat.send_message(f"Driver {username} start trip at {res.get('ts')}")
                except Exception:
                    pass
            return
        else:
            res = record_end_trip_sheet(username, plate)
            if res.get("ok"):
                ts = res.get("ts")
                dur = res.get("duration", "")
                nowdt = datetime.now()
                n_today = count_trips_for_day(username, nowdt)
                month_start = datetime(nowdt.year, nowdt.month, 1)
                if nowdt.month == 12:
                    month_end = datetime(nowdt.year+1,1,1)
                else:
                    month_end = datetime(nowdt.year, nowdt.month+1,1)
                n_month = count_trips_for_month(username, month_start, month_end)
                msg = f"Driver {username} end trip at {ts} (duration {dur}).\nDriver {username} completed {n_today} trip(s) today and {n_month} this month."
                try:
                    await query.edit_message_text(msg)
                except Exception:
                    try:
                        await query.message.chat.send_message(msg)
                    except Exception:
                        pass
            else:
                try:
                    await query.edit_message_text("❌ Failed to record end trip.")
                except Exception:
                    pass
            return

    # finance plate selection: fin_plate|fuel|<plate>
    if data.startswith("fin_plate|"):
        parts = data.split("|", 2)
        if len(parts) == 3:
            _, typ, plate = parts
            if typ == "fuel":
                return await plate_callback_finance(update, context, typ, plate)
        try:
            await query.edit_message_text("Invalid finance selection.")
        except Exception:
            pass
        return

    # mission flows: prefixes ms_s (mission start plate), ms_e (mission end plate), ms_sd (start depart city), ms_ed (end arrival city)
    if data.startswith("ms_s|") or data.startswith("ms_e|") or data.startswith("ms_sd|") or data.startswith("ms_ed|"):
        parts = data.split("|")
        key = parts[0]
        if key == "ms_s":
            plate = parts[1]
            # ask departure city
            try:
                await query.edit_message_text("Departure:", reply_markup=build_city_kb("ms_sd", plate))
            except Exception:
                pass
            return
        if key == "ms_e":
            plate = parts[1]
            try:
                await query.edit_message_text("Arrival:", reply_markup=build_city_kb("ms_ed", plate))
            except Exception:
                pass
            return
        if key == "ms_sd" and len(parts) >= 3:
            _, plate, city = parts[:3]
            chat_id = query.message.chat.id
            chat_m = MISSION_CACHE.setdefault(chat_id, {})
            legs = chat_m.setdefault(plate, [])
            legs.append({"t":"d","city":city,"ts":now_str(), "driver": (query.from_user.username or query.from_user.full_name)})
            try:
                await query.edit_message_text("Recorded departure.")
            except Exception:
                pass
            return
        if key == "ms_ed" and len(parts) >= 3:
            _, plate, city = parts[:3]
            chat_id = query.message.chat.id
            chat_m = MISSION_CACHE.setdefault(chat_id, {})
            legs = chat_m.setdefault(plate, [])
            legs.append({"t":"a","city":city,"ts":now_str(), "driver": (query.from_user.username or query.from_user.full_name)})
            try:
                await query.edit_message_text("Recorded arrival.")
            except Exception:
                pass
            # attempt to detect a completed roundtrip sequence: d,a,d,a (last 4)
            if len(legs) >= 4:
                seq = legs[-4:]
                seq_types = [x["t"] for x in seq]
                if seq_types == ["d","a","d","a"]:
                    d1,a1,d2,a2 = seq
                    # validate roundtrip pair: PP-SHV-PP or SHV-PP-SHV
                    ok_rt = ((d1["city"]=="PP" and a1["city"]=="SHV" and d2["city"]=="SHV" and a2["city"]=="PP") or
                             (d1["city"]=="SHV" and a1["city"]=="PP" and d2["city"]=="PP" and a2["city"]=="SHV"))
                    if ok_rt:
                        drv = d1["driver"]
                        start_ts = d1["ts"]
                        end_ts = a2["ts"]
                        dur_min = compute_duration_min(start_ts, end_ts)
                        # write merged mission row
                        row = [drv, plate, start_ts, end_ts, str(dur_min),
                               d1["city"], d1["ts"], a1["city"], a1["ts"],
                               d2["city"], d2["ts"], a2["city"], a2["ts"]]
                        append_row_sheet(MISSION_SHEET, row)
                        # clear legs for that plate in this chat
                        chat_m[plate] = []
                        # update mission summary (A-2) for driver month
                        try:
                            update_mission_summary_for_range(drv, start_ts, end_ts)
                        except Exception:
                            logger.exception("Failed updating mission summary.")
                        # notify driver via private message
                        try:
                            await query.message.chat.send_message(f"Driver {drv} completed a mission for plate {plate}.")
                        except Exception:
                            logger.exception("Failed send mission completion notice.")
            return

    # mission quick helpers done
    try:
        await query.edit_message_text("Invalid selection.")
    except Exception:
        pass

# -------- Message handler for ForceReply / pending flows --------

async def process_force_reply(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # This processes:
    #  - pending_fin_simple flow (fuel amount -> invoice question -> write)
    #  - leave command (handled by direct command handler; no ForceReply)
    #  - other free-text fallback (not used heavily)
    user = update.effective_user
    if not user:
        return
    text = (update.effective_message.text or "").strip()
    if not text:
        return

    # --- Fuel pending simple flow ---
    pending = context.user_data.get("pending_fin_simple")
    if pending and pending.get("type") == "fuel":
        step = pending.get("step")
        plate = pending.get("plate")
        if step == "amount":
            # parse amount and optional odo
            m = AMOUNT_RE.search(text)
            if not m:
                # invalid amount - delete and clear
                try:
                    await update.effective_message.delete()
                except Exception:
                    pass
                try:
                    await context.bot.send_message(chat_id=user.id, text="Invalid amount format. Use like: 23.5 or 23.5 odo:12345")
                except Exception:
                    pass
                # clear pending and delete prompt
                try:
                    origin = pending.get("prompt_chat"), pending.get("prompt_msg_id")
                    if origin and origin[0] and origin[1]:
                        await context.bot.delete_message(chat_id=origin[0], message_id=origin[1])
                except Exception:
                    pass
                context.user_data.pop("pending_fin_simple", None)
                return
            amt = m.group(1)
            # try to parse odo
            odo = None
            odo_m = ODO_RE.search(text)
            if odo_m:
                odo = int(odo_m.group(1))
            # save and ask invoice yes/no
            pending["amount"] = amt
            pending["odo"] = odo
            pending["step"] = "invoice"
            context.user_data["pending_fin_simple"] = pending
            # delete user's amount message to reduce clutter
            try:
                await update.effective_message.delete()
            except Exception:
                pass
            # prompt invoice yes/no (ForceReply)
            fr = ForceReply(selective=False)
            try:
                msg = await context.bot.send_message(chat_id=update.effective_chat.id, text="Invoice received? yes/no", reply_markup=fr)
                pending["invoice_prompt_chat"] = msg.chat_id
                pending["invoice_prompt_msg_id"] = msg.message_id
                context.user_data["pending_fin_simple"] = pending
            except Exception:
                logger.exception("Failed to prompt invoice yes/no")
                context.user_data.pop("pending_fin_simple", None)
            return
        elif step == "invoice":
            ans = text.lower().strip()
            if ans not in ("yes","no"):
                try:
                    await update.effective_message.delete()
                except Exception:
                    pass
                try:
                    await context.bot.send_message(chat_id=user.id, text="Reply with yes or no.")
                except Exception:
                    pass
                return
            # we have plate, amount, odo (maybe), invoice ans -> record
            amount = pending.get("amount")
            odo = pending.get("odo")
            invoice = "Yes" if ans.startswith("y") else "No"
            driver = user.username or user.full_name or "unknown"
            # fetch last odo from sheet if needed
            last_odo = get_last_odo_from_sheet(plate)
            if last_odo is None:
                last_odo = ODO_CACHE.get(plate)
            if odo is None:
                # if user didn't supply odo, we set odo same as last_odo (no change) or empty
                odo_val = last_odo if last_odo is not None else ""
            else:
                odo_val = odo
            # compute diff
            diff_val = ""
            try:
                if isinstance(odo_val, int) and last_odo is not None:
                    diff_val = str(odo_val - last_odo)
                elif isinstance(odo_val, int) and last_odo is None:
                    diff_val = ""
                else:
                    diff_val = ""
            except Exception:
                diff_val = ""
            # update ODO cache
            if isinstance(odo_val, int):
                ODO_CACHE[plate] = odo_val
            # write single combined row: timestamp, driver, plate, odo, odo_diff, fuel_usd, invoice_received
            row = [now_str(), driver, plate, str(odo_val) if odo_val != "" else "", diff_val, str(amount), invoice]
            append_row_sheet(FUEL_SHEET, row)
            # delete invoice question prompt and user's reply to keep chat tidy
            try:
                await update.effective_message.delete()
            except Exception:
                pass
            try:
                inv_chat = pending.get("invoice_prompt_chat")
                inv_msg = pending.get("invoice_prompt_msg_id")
                if inv_chat and inv_msg:
                    await context.bot.delete_message(chat_id=inv_chat, message_id=inv_msg)
            except Exception:
                pass
            # delete original amount prompt if exists
            try:
                orig_chat = pending.get("prompt_chat")
                orig_msg = pending.get("prompt_msg_id")
                if orig_chat and orig_msg:
                    await context.bot.delete_message(chat_id=orig_chat, message_id=orig_msg)
            except Exception:
                pass
            # short private receipt to user
            try:
                receipt = f"Plate {plate} @ {odo_val if odo_val!='' else 'N/A'} km + ${amount} fuel on {datetime.now().date()}\nOdo diff since last record: {diff_val or 'N/A'} km\nInvoice: {invoice}"
                await context.bot.send_message(chat_id=user.id, text=receipt)
            except Exception:
                logger.exception("Failed send receipt DM")
            context.user_data.pop("pending_fin_simple", None)
            return

    # no matching pending flow -> ignore or pass
    return

# -------- Leave command (zero prompt) ----------
# Usage: /leave <driver> <YYYY-MM-DD> <YYYY-MM-DD> <SL|AL> [notes...]
async def leave_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # delete command message
    try:
        if update.effective_message:
            await update.effective_message.delete()
    except Exception:
        pass
    args = context.args
    if not args or len(args) < 4:
        try:
            await update.effective_chat.send_message("Usage: /leave <driver> <YYYY-MM-DD> <YYYY-MM-DD> <SL|AL> [notes...]")
        except Exception:
            pass
        return
    driver = args[0]
    start = args[1]
    end = args[2]
    ltype = args[3]
    notes = " ".join(args[4:]) if len(args) > 4 else ""
    # validate dates
    try:
        sd = datetime.strptime(start, "%Y-%m-%d")
        ed = datetime.strptime(end, "%Y-%m-%d")
    except Exception:
        try:
            await update.effective_chat.send_message("Invalid date format. Use YYYY-MM-DD.")
        except Exception:
            pass
        return
    row = [driver, start, end, ltype, notes, now_str()]
    ok = append_row_sheet(LEAVE_SHEET, row)
    try:
        await context.bot.send_message(chat_id=update.effective_user.id, text=f"Leave recorded for {driver}: {start} -> {end} ({ltype})")
    except Exception:
        pass
    return

# -------- Mission summary A-2 computation & update ----------
# A-2: For each driver & month, compute mission_days based on mission start/end range and per-diem rules:
# - If mission is same-day or ends before next day 12:00 -> count 1 day
# - If end is after 12:00 next day -> count additional day(s) accordingly
# This function updates SUMMARY_SHEET per driver/month aggregations.

def mission_days_from_range(start_ts_str: str, end_ts_str: str) -> int:
    start_dt = parse_ts(start_ts_str)
    end_dt = parse_ts(end_ts_str)
    if not start_dt or not end_dt:
        return 0
    # normalize: count days from start date inclusive, with midday cutoff rule on subsequent days
    # We compute days by iterating day-by-day
    days = 0
    cur = start_dt
    while cur.date() <= end_dt.date():
        if cur.date() == start_dt.date():
            days += 1
        else:
            # on subsequent days, check if the end is past midday of that day
            noon = datetime(cur.year, cur.month, cur.day, 12, 0, 0)
            if end_dt >= noon:
                days += 1
        cur = cur + timedelta(days=1)
    return days

def update_mission_summary_for_range(driver: str, start_ts: str, end_ts: str):
    # determines month key as start_ts.year-month
    try:
        sdt = parse_ts(start_ts)
        if not sdt:
            return
        ym = sdt.strftime("%Y-%m")
        days = mission_days_from_range(start_ts, end_ts)
        per_diem = days * PER_DIEM_USD
        # read existing summary rows and update or append
        if not GS_ENABLED:
            logger.info("GS disabled: summary not updated for %s %s", driver, ym)
            return
        ws = _ws(SUMMARY_SHEET)
        vals = ws.get_all_values()
        start_idx = 1 if vals and any("driver" in c.lower() for c in vals[0]) else 0
        # find driver+ym row
        found = False
        for i, r in enumerate(vals[start_idx:], start_idx):
            r_driver = r[0] if len(r) > 0 else ""
            r_ym = r[1] if len(r) > 1 else ""
            if r_driver == driver and r_ym == ym:
                # update counts
                prev_days = int(r[2]) if len(r) > 2 and str(r[2]).isdigit() else 0
                new_days = prev_days + days
                new_per = new_days * PER_DIEM_USD
                try:
                    ws.update_cell(i+1, 3, str(new_days))
                    ws.update_cell(i+1, 4, str(new_per))
                except Exception:
                    # fallback row replace
                    existing = ws.row_values(i+1)
                    while len(existing) < 4:
                        existing.append("")
                    existing[2] = str(new_days)
                    existing[3] = str(new_per)
                    try:
                        ws.delete_rows(i+1)
                        ws.insert_row(existing, i+1)
                    except Exception:
                        pass
                found = True
                break
        if not found:
            # append
            ws.append_row([driver, ym, str(days), str(per_diem)], value_input_option="USER_ENTERED")
    except Exception:
        logger.exception("Failed to update mission summary.")

# -------- Register & main ----------
def register_handlers(application):
    # Commands
    application.add_handler(CommandHandler(["start_trip","start"], start_trip_command))
    application.add_handler(CommandHandler(["end_trip","end"], end_trip_command))
    application.add_handler(CommandHandler("fuel", fuel_command))
    application.add_handler(CommandHandler("leave", leave_command))
    application.add_handler(CommandHandler("mission_start", lambda u,c: u.message.reply_text("Select plate for mission start:", reply_markup=build_plate_keyboard("ms_s"))))
    application.add_handler(CommandHandler("mission_end", lambda u,c: u.message.reply_text("Select plate for mission end:", reply_markup=build_plate_keyboard("ms_e"))))

    # Callback queries for plates and mission flows and finance plate selection
    application.add_handler(CallbackQueryHandler(callback_query_handler))

    # ForceReply message handler: amount, invoice yes/no, etc.
    application.add_handler(MessageHandler(filters.REPLY & filters.TEXT & (~filters.COMMAND), process_force_reply))
    # fallback text handler (non-command simple replies)
    application.add_handler(MessageHandler(filters.TEXT & (~filters.COMMAND), process_force_reply))

    # global command list
    async def _post_init(app):
        # ensure webhook deleted before polling to avoid conflict
        try:
            await app.bot.delete_webhook()
            logger.info("Deleted existing webhook (if any).")
        except Exception:
            logger.exception("Failed to delete webhook (ignored).")
        # set bot commands
        try:
            await app.bot.set_my_commands([
                BotCommand("start_trip", "Start a trip (select plate)"),
                BotCommand("end_trip", "End a trip (select plate)"),
                BotCommand("fuel", "Record fuel (select plate)"),
                BotCommand("mission_start", "Mission start (select plate)"),
                BotCommand("mission_end", "Mission end (select plate)"),
                BotCommand("leave", "Record leave (admin)"),
            ])
            logger.info("Bot commands set.")
        except Exception:
            logger.exception("Failed to set bot commands.")
    # attach post_init
    if hasattr(application, "post_init"):
        application.post_init.append(_post_init)

def main():
    if not BOT_TOKEN:
        raise RuntimeError("BOT_TOKEN env required")
    # persistence optional
    persistence = None
    try:
        persistence = PicklePersistence(filepath="driver_bot_persistence.pkl")
    except Exception:
        persistence = None

    application = ApplicationBuilder().token(BOT_TOKEN).persistence(persistence).build()
    register_handlers(application)

    logger.info("Starting driver-bot polling...")
    application.run_polling()

if __name__ == "__main__":
    main()
