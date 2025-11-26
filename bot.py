#!/usr/bin/env python3
"""
Driver Bot — Extended: includes annual roundtrip stats, SQLite migration,
unfinished mission alerting, admin cost input UI, and prior features.

Usage:
- Set BOT_TOKEN, GOOGLE_CREDS_BASE64 / GOOGLE_CREDS_PATH, GOOGLE_SHEET_NAME
- Optionally set: SUMMARY_CHAT_ID, SUMMARY_HOUR, SUMMARY_TZ, LOCAL_TZ,
  DRIVER_PLATE_MAP (JSON), ADMIN_USERS (comma-separated usernames)
"""

import os
import json
import base64
import logging
import csv
import uuid
import re
import sqlite3
from datetime import datetime, timedelta, time as dtime
from typing import Optional, Dict, List, Any, Tuple

import gspread
from oauth2client.service_account import ServiceAccountCredentials

# zoneinfo
try:
    from zoneinfo import ZoneInfo  # type: ignore
except Exception:
    ZoneInfo = None  # type: ignore

# Telegram
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
    PicklePersistence,
)

# Logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("driver-bot")

# ===== ENV & defaults =====
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

if LOCAL_TZ and ZoneInfo is None:
    logger.warning("LOCAL_TZ set but zoneinfo not available; falling back to system time.")

PLATES = [p.strip() for p in PLATE_LIST.split(",") if p.strip()]

# DRIVER_PLATE_MAP can be JSON mapping username -> [plates]
DRIVER_PLATE_MAP_JSON = os.getenv("DRIVER_PLATE_MAP", "").strip() or None

# Admins (comma separated usernames)
ADMIN_USERS = [u.strip() for u in os.getenv("ADMIN_USERS", "markpeng1,kmnyy").split(",") if u.strip()]

# Scheduling / summary
SUMMARY_CHAT_ID = os.getenv("SUMMARY_CHAT_ID")
SUMMARY_HOUR = int(os.getenv("SUMMARY_HOUR", "20"))
SUMMARY_TZ = os.getenv("SUMMARY_TZ", LOCAL_TZ or "Asia/Phnom_Penh")

# Language
DEFAULT_LANG = os.getenv("LANG", "en").lower()
SUPPORTED_LANGS = ("en", "km")

# Sheet tabs
RECORDS_TAB = os.getenv("RECORDS_TAB", "Driver_Log")
DRIVERS_TAB = os.getenv("DRIVERS_TAB", "Drivers")
SUMMARY_TAB = os.getenv("SUMMARY_TAB", "Summary")
MISSIONS_TAB = os.getenv("MISSIONS_TAB", "Missions")
MISSIONS_REPORT_TAB = os.getenv("MISSIONS_REPORT_TAB", "Missions_Report")
VEHICLE_COSTS_TAB = os.getenv("VEHICLE_COSTS_TAB", "Vehicle_Costs")

# Mission columns mapping (0-based indexes for get_all_values)
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

# Column mapping for trip records (1-indexed used with update_cell)
COL_DATE = 1
COL_DRIVER = 2
COL_PLATE = 3
COL_START = 4
COL_END = 5
COL_DURATION = 6

# Time formats
TS_FMT = "%Y-%m-%d %H:%M:%S"
DATE_FMT = "%Y-%m-%d"

# Roundtrip match window (hours)
ROUNDTRIP_WINDOW_HOURS = int(os.getenv("ROUNDTRIP_WINDOW_HOURS", "24"))

# Google scopes
SCOPES = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]

# SQLite DB path (for migration)
SQLITE_PATH = os.getenv("SQLITE_PATH", "driver_bot.db")

# HEADERS template per tab — conservative: only write headers if sheet empty
HEADERS_BY_TAB: Dict[str, List[str]] = {
    RECORDS_TAB: ["Date", "Driver", "Plate", "Start DateTime", "End DateTime", "Duration"],
    MISSIONS_TAB: ["GUID", "No.", "Name", "Plate", "Start Date", "End Date",
                   "Departure", "Arrival", "Staff Name", "Roundtrip", "Return Start", "Return End"],
    MISSIONS_REPORT_TAB: ["GUID", "No.", "Name", "Plate", "Start Date", "End Date",
                          "Departure", "Arrival", "Staff Name", "Roundtrip", "Return Start", "Return End"],
    SUMMARY_TAB: ["Date", "PeriodType", "TotalsJSON", "HumanSummary"],
    DRIVERS_TAB: ["Username", "Plates"],
    VEHICLE_COSTS_TAB: ["ID", "Plate", "Date", "Odometer_km", "Fuel_l",
                        "Fuel_cost", "Parking_cost", "Notes", "EnteredBy", "EnteredAt"],
}

# Minimal translations
TR = {
    "en": {
        "menu": "Driver Bot Menu — tap a button to perform an action:",
        "choose_start": "Please choose the vehicle plate to START trip:",
        "choose_end": "Please choose the vehicle plate to END trip:",
        "start_ok": "✅ Started trip for {plate} (driver: {driver}). {msg}",
        "end_ok": "✅ Ended trip for {plate} (driver: {driver}). {msg}",
        "not_allowed": "❌ You are not allowed to operate this plate: {plate}.",
        "invalid_sel": "Invalid selection.",
        "help": "Help: Tap Start trip or End trip and then choose a plate.",
        "no_bot_token": "Please set BOT_TOKEN environment variable.",
        "mission_start_prompt_plate": "Please choose the plate for the mission start:",
        "mission_start_prompt_depart": "Select departure city:",
        "mission_start_prompt_staff": "Optional: send staff name accompanying you (or /skip).",
        "mission_start_ok": "✅ Mission start recorded for {plate} at {start_date} departing {dep}.",
        "mission_end_prompt_plate": "Please choose the plate to end the mission (will match your last open mission):",
        "mission_end_prompt_arrival": "Select arrival city:",
        "mission_end_ok": "✅ Mission end recorded for {plate} at {end_date} arriving {arr}.",
        "mission_no_open": "No open mission found for {plate}.",
        "roundtrip_merged_notify": "✅ Roundtrip merged for {driver} on plate {plate}. {count_msg}",
        "roundtrip_monthly_count": "Driver {driver} completed {count} roundtrips this month.",
        "lang_set": "Language set to {lang}.",
    },
    "km": {
        "menu": "មិនីវរ​បូត — សូមចុចប៊ូតុងដើម្បីអនុវត្ត៖",
        "choose_start": "សូមជ្រើស plate សម្រាប់ចាប់ផ្តើមដំណើរ:",
        "choose_end": "សូមជ្រើស plate សម្រាប់បញ្ចប់ដំណើរ:",
        "start_ok": "✅ ចាប់ផ្ដើមដំណើរសម្រាប់ {plate} (អ្នកបើក: {driver}). {msg}",
        "end_ok": "✅ បញ្ចប់ដំណើរសម្រាប់ {plate} (អ្នកបើក: {driver}). {msg}",
        "not_allowed": "❌ អ្នកមិនមានសិទ្ធិប្រើរថយន្តនេះ: {plate}.",
        "invalid_sel": "ជម្រើសមិនត្រឹមត្រូវ។",
        "help": "ជំនួយ៖ ចុច Start trip ឬ End trip ហើយជ្រើស plate.",
        "no_bot_token": "សូមកំណត់ BOT_TOKEN variable.",
        "mission_start_prompt_plate": "សូមជ្រើស plate សម្រាប់ចាប់ផ្តើមដំណើរ:",
        "mission_start_prompt_depart": "ជ្រើសទីក្រុងចេញ (Departure):",
        "mission_start_prompt_staff": "Optional: ផ្ញើឈ្មោះបុគ្គលិក (ឬ /skip).",
        "mission_start_ok": "✅ កត់ត្រាចាប់ផ្តើមដំណើរ {plate} នៅ {start_date} ចេញពី {dep}.",
        "mission_end_prompt_plate": "សូមជ្រើស plate ដើម្បីបញ្ចប់ដំណើរ:",
        "mission_end_prompt_arrival": "ជ្រើសទីក្រុងមកដល់ (Arrival):",
        "mission_end_ok": "✅ កត់ត្រាបញ្ចប់ដំណើរ {plate} នៅ {end_date} មកដល់ {arr}.",
        "mission_no_open": "មិនមានកិច្ចការបើកសម្រាប់ {plate}.",
        "roundtrip_merged_notify": "✅ រួមបញ្ចូល往返សម្រាប់ {driver} លើ plate {plate}. {count_msg}",
        "roundtrip_monthly_count": "អ្នកបើក {driver} បានធ្វើ往返 {count} ដងនៅខែនេះ។",
        "lang_set": "បានកំណត់ភាសា​ជា {lang}.",
    },
}


def t(user_lang: Optional[str], key: str, **kwargs) -> str:
    lang = (user_lang or DEFAULT_LANG or "en").lower()
    if lang not in SUPPORTED_LANGS:
        lang = "en"
    return TR.get(lang, TR["en"]).get(key, TR["en"].get(key, "")).format(**kwargs)


# ===== Google Sheets helpers =====
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
    except Exception as e:
        logger.exception("Failed to decode GOOGLE_CREDS_BASE64: %s", e)
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
        raise RuntimeError(
            "Google credentials not found. "
            "Set GOOGLE_CREDS_BASE64 or GOOGLE_CREDS_PATH or include credentials.json"
        )
    creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_json, SCOPES)
    client = gspread.authorize(creds)
    return client


def ensure_sheet_has_headers_conservative(ws, headers: List[str]):
    try:
        values = ws.get_all_values()
        if not values:
            ws.insert_row(headers, index=1)
    except Exception:
        logger.exception("Failed to ensure headers on worksheet %s", ws.title)


_UUID_RE = re.compile(r'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$', re.I)


def _missions_header_fix_if_needed(ws):
    try:
        values = ws.get_all_values()
        if not values or len(values) < 2:
            return
        first_row = values[0]
        second_row = values[1]
        first_cell = str(second_row[0]).strip() if len(second_row) else ""
        if first_cell and _UUID_RE.match(first_cell):
            header_first = str(first_row[0]).strip().lower() if len(first_row) else ""
            if header_first != "guid":
                headers = HEADERS_BY_TAB.get(MISSIONS_TAB, [])
                if not headers:
                    return
                h = list(headers)
                while len(h) < M_MANDATORY_COLS:
                    h.append("")
                end_col = chr(ord('A') + M_MANDATORY_COLS - 1)
                rng = f"A1:{end_col}1"
                ws.update(rng, [h], value_input_option="USER_ENTERED")
                logger.info("Fixed MISSIONS header row due to GUID detected.")
    except Exception:
        logger.exception("Error fixing missions header.")


def open_worksheet(tab: str = ""):
    gc = get_gspread_client()
    sh = gc.open(GOOGLE_SHEET_NAME)

    def _create_tab(name: str, headers: Optional[List[str]] = None):
        try:
            cols = max(12, len(headers) if headers else 12)
            ws_new = sh.add_worksheet(title=name, rows="2000", cols=str(cols))
            if headers:
                ws_new.insert_row(headers, index=1)
            return ws_new
        except Exception:
            try:
                return sh.worksheet(name)
            except Exception:
                raise

    if tab:
        try:
            ws = sh.worksheet(tab)
            if tab in HEADERS_BY_TAB:
                ensure_sheet_has_headers_conservative(ws, HEADERS_BY_TAB[tab])
            if tab == MISSIONS_TAB:
                _missions_header_fix_if_needed(ws)
            return ws
        except Exception:
            headers = HEADERS_BY_TAB.get(tab)
            return _create_tab(tab, headers=headers)
    else:
        if GOOGLE_SHEET_TAB:
            try:
                ws = sh.worksheet(GOOGLE_SHEET_TAB)
                if GOOGLE_SHEET_TAB in HEADERS_BY_TAB:
                    ensure_sheet_has_headers_conservative(ws, HEADERS_BY_TAB[GOOGLE_SHEET_TAB])
                return ws
            except Exception:
                return _create_tab(GOOGLE_SHEET_TAB, headers=None)
        return sh.sheet1


# ===== Driver map loaders =====
def load_driver_map_from_env() -> Dict[str, List[str]]:
    if not DRIVER_PLATE_MAP_JSON:
        return {}
    try:
        obj = json.loads(DRIVER_PLATE_MAP_JSON)
        out = {}
        for k, v in obj.items():
            if isinstance(v, str):
                plates = [p.strip() for p in v.split(",") if p.strip()]
            elif isinstance(v, list):
                plates = [str(p).strip() for p in v]
            else:
                plates = []
            out[str(k).strip()] = plates
        return out
    except Exception:
        logger.exception("Failed to parse DRIVER_PLATE_MAP JSON")
        return {}


def load_driver_map_from_sheet() -> Dict[str, List[str]]:
    try:
        ws = open_worksheet(DRIVERS_TAB)
        rows = ws.get_all_records()
        mapping = {}
        for r in rows:
            user = str(r.get("Username", "")).strip()
            plates_raw = str(r.get("Plates", "")).strip()
            if user:
                mapping[user] = [p.strip() for p in plates_raw.split(",") if p.strip()]
        return mapping
    except Exception:
        logger.exception("Failed to load drivers sheet")
        return {}


def get_driver_map() -> Dict[str, List[str]]:
    env_map = load_driver_map_from_env()
    if env_map:
        return env_map
    return load_driver_map_from_sheet()
# ===== Time helpers =====
def _now_dt() -> datetime:
    if LOCAL_TZ and ZoneInfo:
        try:
            tz = ZoneInfo(LOCAL_TZ)
            return datetime.now(tz)
        except Exception:
            return datetime.now()
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
        if not s or not e:
            return ""
        delta = e - s
        total_m = int(delta.total_seconds() // 60)
        if total_m < 0:
            return ""
        h = total_m // 60
        m = total_m % 60
        return f"{h}h{m}m"
    except Exception:
        return ""


# ===== Trip record functions =====
def record_start_trip(driver: str, plate: str) -> dict:
    ws = open_worksheet(RECORDS_TAB)
    start_ts = now_str()
    row = [today_date_str(), driver, plate, start_ts, "", ""]
    try:
        ws.append_row(row, value_input_option="USER_ENTERED")
        return {"ok": True, "message": f"Start recorded at {start_ts}"}
    except Exception as e:
        return {"ok": False, "message": str(e)}


def record_end_trip(driver: str, plate: str) -> dict:
    ws = open_worksheet(RECORDS_TAB)
    try:
        rows = ws.get_all_values()
        start_idx = 1 if rows and "date" in rows[0][0].lower() else 0
        for i in range(len(rows) - 1, start_idx - 1, -1):
            rec = rows[i]
            if len(rec) > 2 and rec[2] == plate and not rec[4]:
                rownum = i + 1
                end_ts = now_str()
                duration = compute_duration(rec[3], end_ts)
                ws.update_cell(rownum, COL_END, end_ts)
                ws.update_cell(rownum, COL_DURATION, duration)
                return {"ok": True, "message": f"Ended at {end_ts} (duration {duration})"}
        end_ts = now_str()
        ws.append_row([today_date_str(), driver, plate, "", end_ts, ""], value_input_option="USER_ENTERED")
        return {"ok": True, "message": f"End-only recorded at {end_ts}"}
    except Exception as e:
        return {"ok": False, "message": str(e)}


# ===== Missions =====
def _missions_get_values_and_data_rows(ws):
    vals = ws.get_all_values()
    if not vals:
        return [], 0
    header_like = {"guid", "no.", "no", "name", "plate", "start", "end"}
    first_row = vals[0]
    if any(str(c).strip().lower() in header_like for c in first_row):
        return vals, 1
    return vals, 0


def _ensure_row_length(row: List[Any], length: int) -> List[Any]:
    r = list(row)
    while len(r) < length:
        r.append("")
    return r


def _missions_next_no(ws) -> int:
    vals, start_idx = _missions_get_values_and_data_rows(ws)
    return max(1, len(vals) - start_idx + 1)


def start_mission_record(driver: str, plate: str, departure: str, staff_name: str = "") -> dict:
    ws = open_worksheet(MISSIONS_TAB)
    start_dt = now_str()
    guid = str(uuid.uuid4())
    no = _missions_next_no(ws)
    row = [""] * M_MANDATORY_COLS
    row[M_IDX_GUID] = guid
    row[M_IDX_NO] = no
    row[M_IDX_NAME] = driver
    row[M_IDX_PLATE] = plate
    row[M_IDX_START] = start_dt
    row[M_IDX_DEPART] = departure
    row[M_IDX_STAFF] = staff_name
    ws.append_row(row, value_input_option="USER_ENTERED")
    return {"ok": True, "guid": guid}


def end_mission_record(driver: str, plate: str, arrival: str) -> dict:
    ws = open_worksheet(MISSIONS_TAB)
    vals, start_idx = _missions_get_values_and_data_rows(ws)
    for i in range(len(vals) - 1, start_idx - 1, -1):
        row = _ensure_row_length(vals[i], M_MANDATORY_COLS)
        if row[M_IDX_NAME] == driver and row[M_IDX_PLATE] == plate and not row[M_IDX_END]:
            end_dt = now_str()
            row_num = i + 1
            ws.update_cell(row_num, M_IDX_END + 1, end_dt)
            ws.update_cell(row_num, M_IDX_ARRIVAL + 1, arrival)
            return {"ok": True, "merged": False}
    return {"ok": False, "message": "No open mission found"}


# ===== UI Handlers =====
def build_plate_keyboard(prefix: str, allowed=None):
    plates = allowed if allowed else PLATES
    btns = []
    row = []
    for i, p in enumerate(plates, 1):
        row.append(InlineKeyboardButton(p, callback_data=f"{prefix}|{p}"))
        if i % 3 == 0:
            btns.append(row)
            row = []
    if row:
        btns.append(row)
    return InlineKeyboardMarkup(btns)


async def start_trip_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        await update.effective_message.delete()
    except:
        pass
    driver_map = get_driver_map()
    allowed = driver_map.get(update.effective_user.username, None)
    await update.effective_chat.send_message(
        t(context.user_data.get("lang", "en"), "choose_start"),
        reply_markup=build_plate_keyboard("start", allowed)
    )


async def end_trip_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        await update.effective_message.delete()
    except:
        pass
    driver_map = get_driver_map()
    allowed = driver_map.get(update.effective_user.username, None)
    await update.effective_chat.send_message(
        t(context.user_data.get("lang", "en"), "choose_end"),
        reply_markup=build_plate_keyboard("end", allowed)
    )


async def mission_start_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        await update.effective_message.delete()
    except:
        pass
    driver_map = get_driver_map()
    allowed = driver_map.get(update.effective_user.username, None)
    kb = build_plate_keyboard("mission_start_plate", allowed)
    await update.effective_chat.send_message(
        t(context.user_data.get("lang"), "mission_start_prompt_plate"),
        reply_markup=kb
    )


async def mission_end_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        await update.effective_message.delete()
    except:
        pass
    driver_map = get_driver_map()
    allowed = driver_map.get(update.effective_user.username, None)
    kb = build_plate_keyboard("mission_end_plate", allowed)
    await update.effective_chat.send_message(
        t(context.user_data.get("lang"), "mission_end_prompt_plate"),
        reply_markup=kb
    )


async def plate_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    data = q.data
    user = q.from_user
    name = user.username
    lang = context.user_data.get("lang", "en")

    # --- trip start ---
    if data.startswith("start|"):
        _, plate = data.split("|")
        res = record_start_trip(name, plate)
        await q.edit_message_text(
            t(lang, "start_ok", plate=plate, driver=name, msg=res["message"])
        )
        return

    # --- trip end ---
    if data.startswith("end|"):
        _, plate = data.split("|")
        res = record_end_trip(name, plate)
        await q.edit_message_text(
            t(lang, "end_ok", plate=plate, driver=name, msg=res["message"])
        )
        return

    # --- mission start plate selection ---
    if data.startswith("mission_start_plate|"):
        _, plate = data.split("|")
        context.user_data["pending_mission"] = {"action": "start", "plate": plate}

        kb = InlineKeyboardMarkup([
            [
                InlineKeyboardButton("PP", callback_data="mission_depart|PP"),
                InlineKeyboardButton("SHV", callback_data="mission_depart|SHV")
            ]
        ])
        context.user_data["last_inline_prompt"] = {
            "chat_id": q.message.chat.id,
            "message_id": q.message.message_id
        }
        await q.edit_message_text(
            t(lang, "mission_start_prompt_depart"),
            reply_markup=kb
        )
        return

    # --- mission end plate selection ---
    if data.startswith("mission_end_plate|"):
        _, plate = data.split("|")
        context.user_data["pending_mission"] = {"action": "end", "plate": plate}
        kb = InlineKeyboardMarkup([
            [
                InlineKeyboardButton("PP", callback_data="mission_arrival|PP"),
                InlineKeyboardButton("SHV", callback_data="mission_arrival|SHV")
            ]
        ])
        context.user_data["last_inline_prompt"] = {
            "chat_id": q.message.chat.id,
            "message_id": q.message.message_id
        }
        await q.edit_message_text(
            t(lang, "mission_end_prompt_arrival"),
            reply_markup=kb
        )
        return

    # --- mission_start depart city ---
    if data.startswith("mission_depart|"):
        _, dep = data.split("|")
        pending = context.user_data.get("pending_mission")
        pending["departure"] = dep
        context.user_data["pending_mission"] = pending

        try:
            chat_id = update.effective_chat.id
            msg = await context.bot.send_message(
                chat_id, t(lang, "mission_start_prompt_staff")
            )
            context.user_data["last_bot_prompt"] = {
                "chat_id": msg.chat_id,
                "message_id": msg.message_id
            }
        except:
            pass
        return

    # --- mission_end arrival ---
    if data.startswith("mission_arrival|"):
        _, arr = data.split("|")
        pending = context.user_data.get("pending_mission")
        plate = pending["plate"]

        res = end_mission_record(name, plate, arr)
        await q.edit_message_text(
            t(lang, "mission_end_ok", plate=plate, end_date=now_str(), arr=arr)
        )

        context.user_data.pop("pending_mission", None)
        return


# ===== staff name handler =====
async def staff_name_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    pending = context.user_data.get("pending_mission")
    if not pending or pending["action"] != "start":
        try:
            await update.effective_message.delete()
        except:
            pass
        return

    name = update.effective_user.username
    lang = context.user_data.get("lang", "en")
    text = update.message.text.strip()
    staff = "" if text.lower() == "/skip" else text

    prompt = context.user_data.get("last_bot_prompt")
    if prompt:
        try:
            await context.bot.delete_message(prompt["chat_id"], prompt["message_id"])
        except:
            pass
    inline_prompt = context.user_data.get("last_inline_prompt")
    if inline_prompt:
        try:
            await context.bot.delete_message(inline_prompt["chat_id"], inline_prompt["message_id"])
        except:
            pass

    plate = pending["plate"]
    dep = pending["departure"]

    start_mission_record(name, plate, dep, staff)

    try:
        await update.effective_message.delete()
    except:
        pass

    await update.effective_chat.send_message(
        t(lang, "mission_start_ok", plate=plate, start_date=now_str(), dep=dep)
    )
    context.user_data.pop("pending_mission", None)


# ===== /lang =====
async def lang_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        await update.effective_message.delete()
    except:
        pass
    if not context.args:
        await update.effective_chat.send_message("Usage: /lang en|km")
        return
    lang = context.args[0].lower()
    context.user_data["lang"] = lang
    await update.effective_chat.send_message(
        t(lang, "lang_set", lang=lang)
    )


# ===== Register handlers =====
def register_handlers(app):
    app.add_handler(CommandHandler("start_trip", start_trip_command))
    app.add_handler(CommandHandler("end_trip", end_trip_command))
    app.add_handler(CommandHandler("lang", lang_command))
    app.add_handler(CommandHandler("mission_start", mission_start_command))
    app.add_handler(CommandHandler("mission_end", mission_end_command))

    app.add_handler(CallbackQueryHandler(plate_callback))

    app.add_handler(MessageHandler(
        filters.Regex(r"(?i)^/skip$") |
        (filters.TEXT & (~filters.COMMAND)),
        staff_name_handler
    ))


# ===== Main =====
def main():
    ensure_env()
    persistence = PicklePersistence("driver_state.pkl")
    app = ApplicationBuilder().token(BOT_TOKEN).persistence(persistence).build()
    register_handlers(app)
    app.run_polling()


if __name__ == "__main__":
    main()
