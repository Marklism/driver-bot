#!/usr/bin/env python3
"""
Driver Bot — Stable release (PART 1)
Includes:
- env, google sheets helpers
- time helpers
- trip record functions
- mission helpers
- record_finance_entry (finance writer)
This is PART 1 of multiple parts. After copying this, request PART 2.
"""

import os
import json
import base64
import logging
import csv
import uuid
import re
import asyncio
from datetime import datetime, timedelta, time as dtime
from typing import Optional, Dict, List, Any

import gspread
from oauth2client.service_account import ServiceAccountCredentials

try:
    from zoneinfo import ZoneInfo  # Python 3.9+
except Exception:
    ZoneInfo = None  # type: ignore

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

# ========= ENV & defaults =========
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
DRIVER_PLATE_MAP_JSON = os.getenv("DRIVER_PLATE_MAP", "").strip() or None

SUMMARY_CHAT_ID = os.getenv("SUMMARY_CHAT_ID")
SUMMARY_HOUR = int(os.getenv("SUMMARY_HOUR", "20"))
SUMMARY_TZ = os.getenv("SUMMARY_TZ", LOCAL_TZ or "Asia/Phnom_Penh")

DEFAULT_LANG = os.getenv("LANG", "en").lower()
SUPPORTED_LANGS = ("en", "km")

RECORDS_TAB = os.getenv("RECORDS_TAB", "Driver_Log")
DRIVERS_TAB = os.getenv("DRIVERS_TAB", "Drivers")
SUMMARY_TAB = os.getenv("SUMMARY_TAB", "Summary")
MISSIONS_TAB = os.getenv("MISSIONS_TAB", "Missions")
MISSIONS_REPORT_TAB = os.getenv("MISSIONS_REPORT_TAB", "Missions_Report")
LEAVE_TAB = os.getenv("LEAVE_TAB", "Driver_Leave")
MAINT_TAB = os.getenv("MAINT_TAB", "Vehicle_Maintenance")
EXPENSE_TAB = os.getenv("EXPENSE_TAB", "Trip_Expenses")

# default admin if BOT_ADMINS not set
BOT_ADMINS_DEFAULT = "markpeng1"

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
SCOPES = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]

HEADERS_BY_TAB: Dict[str, List[str]] = {
    RECORDS_TAB: ["Date", "Driver", "Plate", "Start DateTime", "End DateTime", "Duration"],
    MISSIONS_TAB: ["GUID", "No.", "Name", "Plate", "Start Date", "End Date", "Departure", "Arrival", "Staff Name", "Roundtrip", "Return Start", "Return End"],
    MISSIONS_REPORT_TAB: ["GUID", "No.", "Name", "Plate", "Start Date", "End Date", "Departure", "Arrival", "Staff Name", "Roundtrip", "Return Start", "Return End"],
    SUMMARY_TAB: ["Date", "PeriodType", "TotalsJSON", "HumanSummary"],
    DRIVERS_TAB: ["Username", "Plates"],
    LEAVE_TAB: ["Driver", "Start Date", "End Date", "Reason", "Notes"],
    MAINT_TAB: ["Plate", "Mileage", "Maintenance Item", "Cost", "Date", "Workshop", "Notes"],
    EXPENSE_TAB: ["Plate", "Driver", "DateTime", "Mileage", "Fuel Cost", "Parking Fee", "Other Fee"],
}

TR = {
    "en": {
        "menu": "Driver Bot Menu — tap a button:",
        "choose_start": "Choose vehicle plate to START trip:",
        "choose_end": "Choose vehicle plate to END trip:",
        "start_ok": "✅ Started trip for {plate} ({driver}). {msg}",
        "end_ok": "✅ Ended trip for {plate} ({driver}). {msg}",
        "not_allowed": "❌ You are not allowed to operate plate: {plate}.",
        "invalid_sel": "Invalid selection.",
        "help": "Help: Use /start_trip or /end_trip and select a plate.",
        "no_bot_token": "Please set BOT_TOKEN environment variable.",
        "mission_start_prompt_plate": "Choose plate to start mission:",
        "mission_start_prompt_depart": "Select departure city:",
        "mission_start_prompt_staff": "Optional: enter staff name (or /skip).",
        "mission_start_ok": "✅ Mission start for {plate} at {start_date}, from {dep}.",
        "mission_end_prompt_plate": "Choose plate to end mission:",
        "mission_end_prompt_arrival": "Select arrival city:",
        "mission_end_ok": "✅ Mission ended for {plate} at {end_date}, arrived {arr}.",
        "mission_no_open": "No open mission found for {plate}.",
        "roundtrip_merged_notify": "✅ Roundtrip merged for {driver} on {plate}. {count_msg}",
        "roundtrip_monthly_count": "Driver {driver} completed {count} roundtrips this month.",
        "lang_set": "Language set to {lang}.",
        "invalid_amount": "Invalid amount — please send a numeric value like `23.5` (no currency).",
        "invalid_odo": "Invalid odometer — please send numeric KM like `12345` or `12345KM`.",
        "confirm_recorded": "{typ} recorded for {plate}: {amount}",
        "leave_prompt": "Please reply to this message with: <driver_username> <YYYY-MM-DD> <YYYY-MM-DD> <reason> [notes]\nExample: markpeng1 2025-12-01 2025-12-05 annual_leave",
        "leave_confirm": "Leave recorded for {driver}: {start} to {end} ({reason})",
        "fin_inline_prompt": "Inline finance form — reply with single line: <type> <plate> <amount> [notes]\nExample: fuel 2BB-3071 23.5 bought diesel",
    },
    "km": {
        "menu": "ម្ហឺនុយបូត — សូមជ្រើសប៊ូតុង:",
        "choose_start": "ជ្រើស plate ដើម្បីចាប់ផ្តើមដំណើរ:",
        "choose_end": "ជ្រើស plate ដើម្បីបញ្ចប់ដំណើរ:",
        "start_ok": "✅ ចាប់ផ្ដើមដំណើរ {plate} ({driver}). {msg}",
        "end_ok": "✅ បញ្ចប់ដំណើរ {plate} ({driver}). {msg}",
        "not_allowed": "❌ មិនមានសិទ្ធិប្រើ plate: {plate}.",
        "invalid_sel": "ជម្រើសមិនត្រឹមត្រូវ។",
        "help": "ជំនួយ៖ ប្រើ /start_trip ឬ /end_trip ហើយជ្រើស plate.",
        "no_bot_token": "សូមកំណត់ BOT_TOKEN។",
        "mission_start_prompt_plate": "ជ្រើស plate ដើម្បីចាប់ផ្តើម mission:",
        "mission_start_prompt_depart": "ជ្រើសទីក្រុងចេញ:",
        "mission_start_prompt_staff": "បញ្ចូលឈ្មោះបុគ្គលិក (ឬ /skip).",
        "mission_start_ok": "✅ ចាប់ផ្ដើម mission {plate} នៅ {start_date} ចេញពី {dep}.",
        "mission_end_prompt_plate": "ជ្រើស plate ដើម្បីបញ្ចប់ mission:",
        "mission_end_prompt_arrival": "ជ្រើសទីក្រុងមកដល់:",
        "mission_end_ok": "✅ បញ្ចប់ mission {plate} នៅ {end_date} មកដល់ {arr}.",
        "roundtrip_merged_notify": "✅ រួមបញ្ចូល往返 {driver} លើ {plate}. {count_msg}",
        "roundtrip_monthly_count": "អ្នកបើក {driver} បាន往返 {count} ដងខែនេះ.",
        "lang_set": "បានផ្លាស់ប្ដូរភាសាទៅ {lang}.",
        "invalid_amount": "ទឹកប្រាក់ទម្រង់មិនត្រឹមត្រូវ — សូមផ្ញើត្រឹមតែលេខដូចជា `23.5` (គ្មានអក្សរ).",
        "invalid_odo": "Odometer មិនត្រឹមត្រូវ — សូមផ្ញើលេខ KM ដូចជា `12345` ឬ `12345KM`.",
        "confirm_recorded": "{typ} បានកត់ត្រាសម្រាប់ {plate}: {amount}",
        "leave_prompt": "សូមឆ្លើយជាមួយ: <driver_username> <YYYY-MM-DD> <YYYY-MM-DD> <reason> [notes]\nឧទាហរណ៍: markpeng1 2025-12-01 2025-12-05 annual_leave",
        "leave_confirm": "បានកត់ត្រាច្បាប់សម្រាកសម្រាប់ {driver}: {start} ដល់ {end} ({reason})",
        "fin_inline_prompt": "Inline finance form — ឆ្លើយជួរមួយ: <type> <plate> <amount> [notes]\nឧទាហរណ៍: fuel 2BB-3071 23.5 bought diesel",
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
        raise RuntimeError("Google credentials not found. Set GOOGLE_CREDS_BASE64 or GOOGLE_CREDS_PATH or include credentials.json")
    creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_json, SCOPES)
    client = gspread.authorize(creds)
    return client

def ensure_sheet_has_headers_conservative(ws, headers: List[str]):
    try:
        values = ws.get_all_values()
        if not values:
            ws.insert_row(headers, index=1)
    except Exception:
        logger.exception("Failed to ensure headers (conservative) on worksheet %s", getattr(ws, "title", "<ws>"))

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
            template = HEADERS_BY_TAB.get(tab)
            if template:
                ensure_sheet_has_headers_conservative(ws, template)
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

# Driver map
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

# Time helpers
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

# Trip records
def record_start_trip(driver: str, plate: str) -> dict:
    ws = open_worksheet(RECORDS_TAB)
    start_ts = now_str()
    row = [today_date_str(), driver, plate, start_ts, "", ""]
    try:
        ws.append_row(row, value_input_option="USER_ENTERED")
        logger.info("Recorded start trip: %s %s %s", driver, plate, start_ts)
        return {"ok": True, "message": f"Start time recorded for {plate} at {start_ts}"}
    except Exception as e:
        logger.exception("Failed to append start trip row")
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
                    ws.delete_row(row_number)
                    ws.insert_row(existing, row_number)
                logger.info("Recorded end trip for %s row %d", plate, row_number)
                return {"ok": True, "message": f"End time recorded for {plate} at {end_ts} (duration {duration_text})"}
        end_ts = now_str()
        row = [today_date_str(), driver, plate, "", end_ts, ""]
        ws.append_row(row, value_input_option="USER_ENTERED")
        logger.info("No open start found; appended end-only row for %s", plate)
        return {"ok": True, "message": f"End time recorded (no matching start found) for {plate} at {end_ts}"}
    except Exception as e:
        logger.exception("Failed to update end trip")
        return {"ok": False, "message": "Failed to write end trip to sheet: " + str(e)}

# Missions helpers
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

def start_mission_record(driver: str, plate: str, departure: str, staff_name: str = "") -> dict:
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
        row[M_IDX_STAFF] = staff_name
        row[M_IDX_ROUNDTRIP] = ""
        row[M_IDX_RETURN_START] = ""
        row[M_IDX_RETURN_END] = ""
        ws.append_row(row, value_input_option="USER_ENTERED")
        logger.info("Mission start recorded GUID=%s no=%s driver=%s plate=%s dep=%s", guid, next_no, driver, plate, departure)
        return {"ok": True, "guid": guid, "no": next_no}
    except Exception as e:
        logger.exception("Failed to append mission start")
        return {"ok": False, "message": "Failed to write mission start to sheet: " + str(e)}

# ------------------ finance writer (record_finance_entry) ------------------
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

def record_finance_entry(typ: str, plate: str, amount: str, notes: str, by_user: str = "") -> dict:
    """
    Record a finance/odo/expense entry into the appropriate Google Sheet tab.
    typ: normalized type (odo|fuel|parking|wash|repair)
    plate: vehicle plate
    amount: numeric string (for odo it's mileage; for others it's amount)
    notes: optional text
    by_user: username who recorded
    Returns dict with 'ok' boolean and optional message.
    """
    try:
        ntyp = normalize_fin_type(typ) or typ
        plate = str(plate).strip()
        notes = str(notes).strip()
        by_user = str(by_user).strip()

        # ODO and general expenses go to EXPENSE_TAB
        if ntyp in {"odo", "fuel", "parking", "wash"}:
            try:
                ws = open_worksheet(EXPENSE_TAB)
            except Exception as e:
                logger.exception("Failed to open EXPENSE_TAB sheet: %s", e)
                return {"ok": False, "message": "Could not open expenses sheet: " + str(e)}

            dt = now_str()
            mileage = ""
            fuel_cost = ""
            parking_fee = ""
            other_fee = ""
            if ntyp == "odo":
                mileage = str(amount)
            elif ntyp == "fuel":
                fuel_cost = str(amount)
            elif ntyp == "parking":
                parking_fee = str(amount)
            elif ntyp == "wash":
                other_fee = str(amount)
                if notes:
                    notes = "wash: " + notes
                else:
                    notes = "wash"

            row = [plate, by_user or "Unknown", dt, mileage, fuel_cost, parking_fee, other_fee]
            try:
                ws.append_row(row, value_input_option="USER_ENTERED")
                logger.info("Recorded expense entry: %s %s %s", ntyp, plate, amount)
                return {"ok": True}
            except Exception as e:
                logger.exception("Failed to append expense row: %s", e)
                return {"ok": False, "message": "Failed to write expense row: " + str(e)}

        # repair/maintenance goes to MAINT_TAB
        if ntyp in {"repair", "maint"}:
            try:
                ws = open_worksheet(MAINT_TAB)
            except Exception as e:
                logger.exception("Failed to open MAINT_TAB sheet: %s", e)
                return {"ok": False, "message": "Could not open maintenance sheet: " + str(e)}
            mileage = ""
            item = "Repair"
            cost = str(amount)
            date = now_str().split(" ")[0]
            workshop = ""
            notes_field = notes or ""
            row = [plate, mileage, item, cost, date, workshop, notes_field]
            try:
                ws.append_row(row, value_input_option="USER_ENTERED")
                logger.info("Recorded maintenance entry: plate=%s cost=%s by=%s", plate, cost, by_user)
                return {"ok": True}
            except Exception as e:
                logger.exception("Failed to append maintenance row: %s", e)
                return {"ok": False, "message": "Failed to write maintenance row: " + str(e)}

        # fallback: write to EXPENSE_TAB other fee
        try:
            ws = open_worksheet(EXPENSE_TAB)
            dt = now_str()
            row = [plate, by_user or "Unknown", dt, "", "", "", str(amount)]
            try:
                ws.append_row(row, value_input_option="USER_ENTERED")
                logger.info("Recorded generic finance entry for unknown type '%s': %s", typ, plate)
                return {"ok": True}
            except Exception as e:
                logger.exception("Failed to append generic expense row: %s", e)
                return {"ok": False, "message": "Failed to write generic expense row: " + str(e)}
        except Exception as e:
            logger.exception("Failed to open EXPENSE_TAB for generic write: %s", e)
            return {"ok": False, "message": "Failed to record entry: " + str(e)}

    except Exception as e:
        logger.exception("Unexpected error in record_finance_entry: %s", e)
        return {"ok": False, "message": "Unexpected error: " + str(e)}

# End of PART 1
# ------------------ PART 2 (continuation) ------------------
# Handlers, UI, ForceReply processing, admin inline finance form,
# plate callbacks, mission end/start handlers, schedule, main.

# Admins
BOT_ADMINS = set([u.strip() for u in os.getenv("BOT_ADMINS", BOT_ADMINS_DEFAULT).split(",") if u.strip()])
# ensure markpeng1 present per your request
BOT_ADMINS.add("markpeng1")

# Keyboard builders
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

def build_reply_keyboard_buttons():
    kb = [
        [KeyboardButton("/start_trip")],
        [KeyboardButton("/end_trip")],
        [KeyboardButton("/menu")],
    ]
    return ReplyKeyboardMarkup(kb, resize_keyboard=True, one_time_keyboard=False)

# Menu & commands
async def menu_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        if update.effective_message:
            await update.effective_message.delete()
    except Exception:
        pass
    user_lang = context.user_data.get("lang", DEFAULT_LANG)
    text = t(user_lang, "menu")
    keyboard = [
        [InlineKeyboardButton("Start trip (select plate)", callback_data="show_start"),
         InlineKeyboardButton("End trip (select plate)", callback_data="show_end")],
        [InlineKeyboardButton("Mission start", callback_data="show_mission_start"),
         InlineKeyboardButton("Mission end", callback_data="show_mission_end")],
        [InlineKeyboardButton("Admin Finance", callback_data="admin_finance"),
         InlineKeyboardButton("Help", callback_data="help")],
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
    user_lang = context.user_data.get("lang", DEFAULT_LANG)
    driver_map = get_driver_map()
    allowed = None
    if user and user.username and driver_map.get(user.username):
        allowed = driver_map.get(user.username)
    await update.effective_chat.send_message(t(user_lang, "choose_start"), reply_markup=build_plate_keyboard("start", allowed_plates=allowed))

async def end_trip_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        if update.effective_message:
            await update.effective_message.delete()
    except Exception:
        pass
    user = update.effective_user
    user_lang = context.user_data.get("lang", DEFAULT_LANG)
    driver_map = get_driver_map()
    allowed = None
    if user and user.username and driver_map.get(user.username):
        allowed = driver_map.get(user.username)
    await update.effective_chat.send_message(t(user_lang, "choose_end"), reply_markup=build_plate_keyboard("end", allowed_plates=allowed))

async def mission_start_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        if update.effective_message:
            await update.effective_message.delete()
    except Exception:
        pass
    user = update.effective_user
    user_lang = context.user_data.get("lang", DEFAULT_LANG)
    driver_map = get_driver_map()
    allowed = None
    if user and user.username and driver_map.get(user.username):
        allowed = driver_map.get(user.username)
    await update.effective_chat.send_message(t(user_lang, "mission_start_prompt_plate"), reply_markup=build_plate_keyboard("mission_start_plate", allowed_plates=allowed))

async def mission_end_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        if update.effective_message:
            await update.effective_message.delete()
    except Exception:
        pass
    user = update.effective_user
    user_lang = context.user_data.get("lang", DEFAULT_LANG)
    driver_map = get_driver_map()
    allowed = None
    if user and user.username and driver_map.get(user.username):
        allowed = driver_map.get(user.username)
    await update.effective_chat.send_message(t(user_lang, "mission_end_prompt_plate"), reply_markup=build_plate_keyboard("mission_end_plate", allowed_plates=allowed))

# Admin finance inline form: show options and then ask ForceReply
async def admin_finance_callback_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    user = query.from_user
    username = user.username or (user.first_name or "")
    if username not in BOT_ADMINS:
        await query.edit_message_text("❌ You are not an admin.")
        return
    kb = [
        [InlineKeyboardButton("ODO", callback_data="fin_type|odo"), InlineKeyboardButton("Fuel", callback_data="fin_type|fuel")],
        [InlineKeyboardButton("Parking", callback_data="fin_type|parking"), InlineKeyboardButton("Wash", callback_data="fin_type|wash")],
        [InlineKeyboardButton("Repair", callback_data="fin_type|repair")],
    ]
    await query.edit_message_text("Select finance type:", reply_markup=InlineKeyboardMarkup(kb))

# When admin selects type, prompt ForceReply to collect input one-line: <plate> <amount> [notes]
async def admin_fin_type_selected(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    data = query.data
    parts = data.split("|", 1)
    if len(parts) != 2:
        await query.edit_message_text("Invalid selection.")
        return
    _, typ = parts
    user = query.from_user
    username = user.username or (user.first_name or "")
    if username not in BOT_ADMINS:
        await query.edit_message_text("❌ Not admin.")
        return
    prompt = f"Please reply to this message with: <plate> <amount> [notes]\nExample: 2BB-3071 23.5 bought diesel"
    try:
        # send a ForceReply message that will be replied-to by admin
        sent = await query.edit_message_text(f"Enter {typ} record — reply to this message.", reply_markup=None)
        # send separate message with ForceReply to capture reply
        fr = ForceReply(force_reply=True, selective=False)
        m = await context.bot.send_message(chat_id=query.message.chat.id, text=prompt, reply_markup=fr)
        # store pending fin action so that when admin replies we know what to do
        # store by chat_id and message_id of ForceReply prompt
        context.user_data["pending_fin"] = {"type": typ, "prompt_chat": m.chat_id, "prompt_msg_id": m.message_id}
    except Exception:
        await query.edit_message_text("Failed to prompt for finance entry.")

# Process replies to ForceReply prompts (finance and leave)
async def process_force_reply(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # Only interested when user_data has pending_fin or pending_leave
    user = update.effective_user
    text = update.effective_message.text.strip() if update.effective_message and update.effective_message.text else ""
    if not text:
        return
    # FINANCE
    pending_fin = context.user_data.get("pending_fin")
    if pending_fin:
        typ = pending_fin.get("type")
        # validate that the message is a reply to the prompt (some clients may not set reply_to_message)
        # We'll accept replies in same chat
        # expected format: <plate> <amount> [notes...]
        parts = text.split()
        if len(parts) < 2:
            # invalid -> delete user's message and inform admin privately
            try:
                await update.effective_message.delete()
            except Exception:
                pass
            try:
                await context.bot.send_message(chat_id=user.id, text="Invalid input. Format: <plate> <amount> [notes]. Example: 2BB-3071 23.5 bought diesel")
            except Exception:
                pass
            return
        plate = parts[0]
        amount = parts[1]
        notes = " ".join(parts[2:]) if len(parts) > 2 else ""
        # Validate numeric amount for non-odo types, for odo accept integer
        ntyp = normalize_fin_type(typ)
        if ntyp == "odo":
            if not ODO_RE.match(amount):
                # try to strip non-digits
                m = re.search(r'(\d+)', amount)
                if m:
                    amount_val = m.group(1)
                else:
                    try:
                        await update.effective_message.delete()
                    except Exception:
                        pass
                    try:
                        await context.bot.send_message(chat_id=user.id, text=t(context.user_data.get("lang", DEFAULT_LANG), "invalid_odo"))
                    except Exception:
                        pass
                    context.user_data.pop("pending_fin", None)
                    return
                amount = amount_val
        else:
            if not AMOUNT_RE.match(amount):
                # maybe contains currency like "60USD" -> invalid, delete
                try:
                    await update.effective_message.delete()
                except Exception:
                    pass
                try:
                    await context.bot.send_message(chat_id=user.id, text=t(context.user_data.get("lang", DEFAULT_LANG), "invalid_amount"))
                except Exception:
                    pass
                context.user_data.pop("pending_fin", None)
                return
        # record
        res = record_finance_entry(ntyp or typ, plate, amount, notes, by_user=user.username or "")
        # delete the user's message to keep chat clean
        try:
            await update.effective_message.delete()
        except Exception:
            pass
        # confirm to admin privately (or in chat if private not possible)
        try:
            await context.bot.send_message(chat_id=user.id, text=t(context.user_data.get("lang", DEFAULT_LANG), "confirm_recorded", typ=ntyp or typ, plate=plate, amount=amount))
        except Exception:
            try:
                await update.effective_chat.send_message(t(context.user_data.get("lang", DEFAULT_LANG), "confirm_recorded", typ=ntyp or typ, plate=plate, amount=amount))
            except Exception:
                pass
        context.user_data.pop("pending_fin", None)
        return

    # LEAVE: if pending_leave exists and user replied with leave info
    pending_leave = context.user_data.get("pending_leave")
    if pending_leave:
        # expected: <driver_username> <YYYY-MM-DD> <YYYY-MM-DD> <reason> [notes]
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
            context.user_data.pop("pending_leave", None)
            return
        driver = parts[0]
        start = parts[1]
        end = parts[2]
        reason = parts[3]
        notes = " ".join(parts[4:]) if len(parts) > 4 else ""
        # basic date validation
        try:
            sd = datetime.strptime(start, "%Y-%m-%d")
            ed = datetime.strptime(end, "%Y-%m-%d")
        except Exception:
            try:
                await update.effective_message.delete()
            except Exception:
                pass
            try:
                await context.bot.send_message(chat_id=user.id, text="Invalid dates. Use YYYY-MM-DD.")
            except Exception:
                pass
            context.user_data.pop("pending_leave", None)
            return
        # write to LEAVE_TAB
        try:
            ws = open_worksheet(LEAVE_TAB)
            row = [driver, start, end, reason, notes]
            ws.append_row(row, value_input_option="USER_ENTERED")
            # confirm
            try:
                await context.bot.send_message(chat_id=user.id, text=t(context.user_data.get("lang", DEFAULT_LANG), "leave_confirm", driver=driver, start=start, end=end, reason=reason))
            except Exception:
                try:
                    await update.effective_chat.send_message(t(context.user_data.get("lang", DEFAULT_LANG), "leave_confirm", driver=driver, start=start, end=end, reason=reason))
                except Exception:
                    pass
            # delete user's message from chat
            try:
                await update.effective_message.delete()
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

# location_or_skip (handles mission staff / skip messages and cleans prompts)
async def location_or_skip(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        if update.effective_message:
            await update.effective_message.delete()
    except Exception:
        pass

    user = update.effective_user
    user_lang = context.user_data.get("lang", DEFAULT_LANG)

    # cancel last prompts
    last_prompt = context.user_data.get("last_bot_prompt")
    if last_prompt:
        try:
            chat_id = last_prompt.get("chat_id")
            msg_id = last_prompt.get("message_id")
            if chat_id and msg_id:
                await context.bot.delete_message(chat_id=chat_id, message_id=msg_id)
        except Exception:
            pass
        context.user_data.pop("last_bot_prompt", None)

    last_inline = context.user_data.get("last_inline_prompt")
    if last_inline:
        try:
            chat_id = last_inline.get("chat_id")
            msg_id = last_inline.get("message_id")
            if chat_id and msg_id:
                await context.bot.delete_message(chat_id=chat_id, message_id=msg_id)
        except Exception:
            pass
        context.user_data.pop("last_inline_prompt", None)

    pending_mission = context.user_data.get("pending_mission")
    if pending_mission and pending_mission.get("action") == "start":
        text = update.message.text.strip() if update.message and update.message.text else ""
        staff = text if text and text.lower().strip() != "/skip" else ""
        plate = pending_mission.get("plate")
        departure = pending_mission.get("departure")
        username = user.username or user.full_name
        driver_map = get_driver_map()
        allowed = driver_map.get(user.username, []) if user and user.username else []
        if allowed and plate not in allowed:
            await update.effective_chat.send_message(t(user_lang, "not_allowed", plate=plate))
            context.user_data.pop("pending_mission", None)
            return
        res = start_mission_record(username, plate, departure, staff_name=staff)
        if res.get("ok"):
            await update.effective_chat.send_message(t(user_lang, "mission_start_ok", plate=plate, start_date=now_str(), dep=departure))
        else:
            await update.effective_chat.send_message("❌ " + res.get("message", ""))
        context.user_data.pop("pending_mission", None)
        return

    # fallback: nothing to handle
    return

# Plate callback (handles many inline actions)
async def plate_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    data = query.data
    user = query.from_user
    username = user.username or f"{user.first_name or ''} {user.last_name or ''}".strip()
    user_lang = context.user_data.get("lang", DEFAULT_LANG)

    # menu navigation and handlers
    if data == "show_start":
        await query.edit_message_text(t(user_lang, "choose_start"), reply_markup=build_plate_keyboard("start"))
        return
    if data == "show_end":
        await query.edit_message_text(t(user_lang, "choose_end"), reply_markup=build_plate_keyboard("end"))
        return
    if data == "show_mission_start":
        await query.edit_message_text(t(user_lang, "mission_start_prompt_plate"), reply_markup=build_plate_keyboard("mission_start_plate"))
        return
    if data == "show_mission_end":
        await query.edit_message_text(t(user_lang, "mission_end_prompt_plate"), reply_markup=build_plate_keyboard("mission_end_plate"))
        return
    if data == "help":
        await query.edit_message_text(t(user_lang, "help"))
        return

    # admin finance
    if data == "admin_finance":
        # only admins
        if (query.from_user.username or "") not in BOT_ADMINS:
            await query.edit_message_text("❌ Admins only.")
            return
        return await admin_finance_callback_handler(update, context)

    if data.startswith("fin_type|"):
        return await admin_fin_type_selected(update, context)

    # mission start choice
    if data.startswith("mission_start_plate|"):
        _, plate = data.split("|", 1)
        context.user_data["pending_mission"] = {"action": "start", "plate": plate}
        kb = [[InlineKeyboardButton("PP", callback_data="mission_depart|PP"), InlineKeyboardButton("SHV", callback_data="mission_depart|SHV")]]
        await query.edit_message_text(t(user_lang, "mission_start_prompt_depart"), reply_markup=InlineKeyboardMarkup(kb))
        return

    # mission end choice
    if data.startswith("mission_end_plate|"):
        _, plate = data.split("|", 1)
        context.user_data["pending_mission"] = {"action": "end", "plate": plate}
        kb = [[InlineKeyboardButton("PP", callback_data="mission_arrival|PP"), InlineKeyboardButton("SHV", callback_data="mission_arrival|SHV")]]
        await query.edit_message_text(t(user_lang, "mission_end_prompt_arrival"), reply_markup=InlineKeyboardMarkup(kb))
        return

    # after selecting departure for start
    if data.startswith("mission_depart|"):
        _, dep = data.split("|", 1)
        pending = context.user_data.get("pending_mission")
        if not pending or pending.get("action") != "start":
            await query.edit_message_text(t(user_lang, "invalid_sel"))
            return
        pending["departure"] = dep
        try:
            if query.message:
                context.user_data["last_inline_prompt"] = {"chat_id": query.message.chat.id, "message_id": query.message.message_id}
        except Exception:
            context.user_data.pop("last_inline_prompt", None)
        context.user_data["pending_mission"] = pending
        try:
            chat_id = update.effective_chat.id
            staff_prompt_msg = await context.bot.send_message(chat_id=chat_id, text=t(user_lang, "mission_start_prompt_staff"))
            context.user_data["last_bot_prompt"] = {"chat_id": staff_prompt_msg.chat_id, "message_id": staff_prompt_msg.message_id}
        except Exception:
            await query.edit_message_text(t(user_lang, "mission_start_prompt_staff"))
            context.user_data.pop("last_bot_prompt", None)
        return

    # after selecting arrival for end
    if data.startswith("mission_arrival|"):
        _, arr = data.split("|", 1)
        pending = context.user_data.get("pending_mission")
        if not pending or pending.get("action") != "end":
            await query.edit_message_text(t(user_lang, "invalid_sel"))
            return
        pending["arrival"] = arr
        context.user_data["pending_mission"] = pending
        plate = pending.get("plate")
        arrival = pending.get("arrival")
        driver_map = get_driver_map()
        allowed = driver_map.get(username, []) if username else []
        if allowed and plate not in allowed:
            await query.edit_message_text(t(user_lang, "not_allowed", plate=plate))
            context.user_data.pop("pending_mission", None)
            return

        res = end_mission_record(username, plate, arrival)
        if res.get("ok"):
            await query.edit_message_text(t(user_lang, "mission_end_ok", plate=plate, end_date=now_str(), arr=arrival))
            if res.get("merged"):
                try:
                    nowdt = _now_dt()
                    month_start = datetime(nowdt.year, nowdt.month, 1)
                    if nowdt.month == 12:
                        month_end = datetime(nowdt.year + 1, 1, 1)
                    else:
                        month_end = datetime(nowdt.year, nowdt.month + 1, 1)
                    counts = count_roundtrips_per_driver_month(month_start, month_end)
                    cnt = counts.get(username, 0)
                    summary_msg = t(user_lang, "roundtrip_monthly_count", driver=username, count=cnt)
                    try:
                        await update.effective_chat.send_message(t(user_lang, "roundtrip_merged_notify", driver=username, plate=plate, count_msg=summary_msg))
                    except Exception:
                        await update.effective_chat.send_message(summary_msg)
                except Exception:
                    logger.exception("Failed to build/send roundtrip monthly summary.")
        else:
            await query.edit_message_text("❌ " + res.get("message", ""))
        context.user_data.pop("pending_mission", None)
        return

    # start/end quick handlers
    if data.startswith("start|") or data.startswith("end|"):
        try:
            action, plate = data.split("|", 1)
        except Exception:
            await query.edit_message_text("Invalid selection.")
            return
        driver_map = get_driver_map()
        allowed = driver_map.get(username, []) if username else []
        if allowed and plate not in allowed:
            await query.edit_message_text(t(user_lang, "not_allowed", plate=plate))
            return
        if action == "start":
            res = record_start_trip(username, plate)
            if res.get("ok"):
                await query.edit_message_text(t(user_lang, "start_ok", plate=plate, driver=username, msg=res["message"]))
            else:
                await query.edit_message_text("❌ " + res.get("message", ""))
            return
        elif action == "end":
            res = record_end_trip(username, plate)
            if res.get("ok"):
                await query.edit_message_text(t(user_lang, "end_ok", plate=plate, driver=username, msg=res["message"]))
            else:
                await query.edit_message_text("❌ " + res.get("message", ""))
            return

    await query.edit_message_text(t(user_lang, "invalid_sel"))

# lang command
async def lang_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        if update.effective_message:
            await update.effective_message.delete()
    except Exception:
        pass
    args = context.args
    if not args:
        try:
            await update.effective_chat.send_message("Usage: /lang en|km")
        except Exception:
            if update.effective_message:
                await update.effective_message.reply_text("Usage: /lang en|km")
        return
    lang = args[0].lower()
    if lang not in SUPPORTED_LANGS:
        try:
            await update.effective_chat.send_message("Supported langs: en, km")
        except Exception:
            if update.effective_message:
                await update.effective_message.reply_text("Supported langs: en, km")
        return
    context.user_data["lang"] = lang
    try:
        await update.effective_chat.send_message(t(lang, "lang_set", lang=lang))
    except Exception:
        if update.effective_message:
            try:
                await update.effective_message.reply_text(t(lang, "lang_set", lang=lang))
            except Exception:
                pass

# Mission report command
async def mission_report_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        if update.effective_message:
            await update.effective_message.delete()
    except Exception:
        pass
    args = context.args
    user_lang = context.user_data.get("lang", DEFAULT_LANG)
    if not args or len(args) < 2:
        await update.effective_chat.send_message("Usage: /mission_report month YYYY-MM")
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
            ok = write_mission_report_rows(rows, period_label=start.strftime("%Y-%m"))
            counts = count_roundtrips_per_driver_month(start, end)
            tab_name = write_roundtrip_summary_tab(start.strftime("%Y-%m"), counts)
            csv_path = write_roundtrip_summary_csv(start.strftime("%Y-%m"), counts)
            if ok:
                await update.effective_chat.send_message(f"Monthly mission report for {start.strftime('%Y-%m')} created.")
            else:
                await update.effective_chat.send_message("❌ Failed to write mission report.")
            if tab_name:
                await update.effective_chat.send_message(f"Roundtrip summary tab created: {tab_name}")
            if csv_path:
                try:
                    with open(csv_path, "rb") as f:
                        await context.bot.send_document(chat_id=update.effective_chat.id, document=f, filename=os.path.basename(csv_path))
                except Exception:
                    await update.effective_chat.send_message(f"Roundtrip CSV written: {csv_path}")
        except Exception:
            await update.effective_chat.send_message("Invalid command. Usage: /mission_report month YYYY-MM")
    else:
        await update.effective_chat.send_message("Usage: /mission_report month YYYY-MM")

# Auto-menu listener for groups (triggers menu on keywords)
AUTO_KEYWORD_PATTERN = r'(?i)\b(start|menu|start trip|end trip|trip|出车|还车|返程)\b'

async def auto_menu_listener(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_chat and update.effective_chat.type in ("group", "supergroup"):
        text = (update.effective_message.text or "").strip()
        if not text:
            return
        if text.startswith("/"):
            # remove slash commands from appearing (user requested earlier)
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

# Daily summary & scheduled jobs
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
        totals = {}  # placeholder if aggregate_for_period not present
        # compute totals from RECORDS_TAB if aggregate_for_period exists; otherwise minimal message
        try:
            totals = aggregate_for_period(date_dt, date_dt + timedelta(days=1))
        except Exception:
            totals = {}
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

    # If first day of month, auto-generate previous month's mission report
    if now.day == 1:
        try:
            first_of_this_month = datetime(now.year, now.month, 1)
            prev_month_end = first_of_this_month
            prev_month_start = (first_of_this_month - timedelta(days=1)).replace(day=1)
            rows = mission_rows_for_period(prev_month_start, prev_month_end)
            ok = write_mission_report_rows(rows, period_label=prev_month_start.strftime("%Y-%m"))
            counts = count_roundtrips_per_driver_month(prev_month_start, prev_month_end)
            tab_name = write_roundtrip_summary_tab(prev_month_start.strftime("%Y-%m"), counts)
            csv_path = write_roundtrip_summary_csv(prev_month_start.strftime("%Y-%m"), counts)
            if ok:
                await context.bot.send_message(chat_id=chat_id, text=f"Auto-generated mission report for {prev_month_start.strftime('%Y-%m')}.")
            if tab_name:
                await context.bot.send_message(chat_id=chat_id, text=f"Roundtrip summary tab created: {tab_name}")
            if csv_path:
                try:
                    with open(csv_path, "rb") as f:
                        await context.bot.send_document(chat_id=chat_id, document=f, filename=os.path.basename(csv_path))
                except Exception:
                    await context.bot.send_message(chat_id=chat_id, text=f"Roundtrip CSV written: {csv_path}")
        except Exception:
            logger.exception("Failed to auto-generate monthly mission report on day 1.")

# Helper: aggregate_for_period (simple implementation using RECORDS_TAB)
def aggregate_for_period(start_dt: datetime, end_dt: datetime) -> Dict[str, int]:
    """
    Aggregate durations per plate (returns minutes) between start_dt (inclusive) and end_dt (exclusive).
    """
    totals: Dict[str, int] = {}
    try:
        ws = open_worksheet(RECORDS_TAB)
        vals = ws.get_all_values()
        if not vals:
            return totals
        # detect header
        start_idx = 1 if any("date" in c.lower() for c in vals[0] if c) else 0
        for r in vals[start_idx:]:
            if len(r) < COL_DURATION:
                continue
            plate = r[COL_PLATE - 1] if len(r) >= COL_PLATE else ""
            start_ts = r[COL_START - 1] if len(r) >= COL_START else ""
            end_ts = r[COL_END - 1] if len(r) >= COL_END else ""
            if not start_ts:
                continue
            s_dt = parse_ts(start_ts)
            if not s_dt:
                continue
            if not (start_dt <= s_dt < end_dt):
                continue
            # duration parsing like "1h30m"
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

# Setup menu: send and pin in group
async def setup_menu_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # only allow admins to run setup
    user = update.effective_user
    if (user.username or "") not in BOT_ADMINS:
        await update.effective_chat.send_message("❌ Admins only.")
        return
    try:
        # send menu and pin
        user_lang = context.user_data.get("lang", DEFAULT_LANG)
        keyboard = [
            [InlineKeyboardButton("Start trip", callback_data="show_start"), InlineKeyboardButton("End trip", callback_data="show_end")],
            [InlineKeyboardButton("Mission start", callback_data="show_mission_start"), InlineKeyboardButton("Mission end", callback_data="show_mission_end")],
            [InlineKeyboardButton("Admin Finance", callback_data="admin_finance"), InlineKeyboardButton("Help", callback_data="help")],
        ]
        sent = await update.effective_chat.send_message(t(user_lang, "menu"), reply_markup=InlineKeyboardMarkup(keyboard))
        try:
            await context.bot.pin_chat_message(chat_id=update.effective_chat.id, message_id=sent.message_id)
            logger.info("Pinned menu message in chat %s", update.effective_chat.id)
        except Exception:
            logger.exception("Could not pin menu message.")
    except Exception:
        logger.exception("Failed to setup menu.")

# Register handlers
def register_ui_handlers(application):
    # commands
    application.add_handler(CommandHandler("menu", menu_command))
    application.add_handler(CommandHandler(["start_trip", "start"], start_trip_command))
    application.add_handler(CommandHandler(["end_trip", "end"], end_trip_command))
    application.add_handler(CommandHandler("lang", lang_command))
    application.add_handler(CommandHandler("mission_start", mission_start_command))
    application.add_handler(CommandHandler("mission_end", mission_end_command))
    application.add_handler(CommandHandler("mission_report", mission_report_command))
    application.add_handler(CommandHandler("setup_menu", setup_menu_command))

    # callback queries
    application.add_handler(CallbackQueryHandler(plate_callback))

    # ForceReply processing (finance & leave) and general message handling
    # Keep location_or_skip handler to capture replies and skip
    application.add_handler(MessageHandler(filters.Regex(r'(?i)^/skip$') | (filters.TEXT & (~filters.COMMAND)), location_or_skip))
    # ForceReply messages (admin finance & leave) -> process_force_reply
    application.add_handler(MessageHandler(filters.REPLY & filters.TEXT & (~filters.COMMAND), process_force_reply))

    # Auto-menu listener for groups
    application.add_handler(MessageHandler(filters.Regex(AUTO_KEYWORD_PATTERN) & filters.ChatType.GROUPS, auto_menu_listener))

    # help command
    application.add_handler(CommandHandler("help", lambda u, c: u.message.reply_text(t(c.user_data.get("lang", DEFAULT_LANG), "help"))))

    # try to set bot commands (best-effort)
    try:
        async def _set_cmds():
            try:
                await application.bot.set_my_commands([
                    BotCommand("start_trip", "Start a trip (select plate)"),
                    BotCommand("end_trip", "End a trip (select plate)"),
                    BotCommand("menu", "Open trip menu"),
                    BotCommand("lang", "Set language /lang en|km"),
                    BotCommand("mission_start", "Start a driver mission (PP<->SHV)"),
                    BotCommand("mission_end", "End a driver mission"),
                    BotCommand("mission_report", "Generate mission report: /mission_report month YYYY-MM"),
                    BotCommand("setup_menu", "Post and pin the main menu (admins only)"),
                ])
            except Exception:
                logger.exception("Failed to set bot commands.")
        if hasattr(application, "create_task"):
            application.create_task(_set_cmds())
    except Exception:
        logger.debug("Could not schedule set_my_commands.")

# Environment check
def ensure_env():
    if not BOT_TOKEN:
        raise RuntimeError(t(DEFAULT_LANG, "no_bot_token"))

# Scheduling daily summary
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

# Main entrypoint
def main():
    ensure_env()
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
    schedule_daily_summary(application)
    logger.info("Starting driver-bot polling...")
    application.run_polling()

if __name__ == "__main__":
    main()

# End of PART 2
