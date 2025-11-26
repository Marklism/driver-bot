#!/usr/bin/env python3
"""
Driver Bot — Full (with /setup_menu)
- Missions, Reports, Maintenance, Admin Inline Finance Form
- /setup_menu: sends & pins the main menu message in the chat (admin-only)
"""

import os
import json
import base64
import logging
import csv
import uuid
import re
from datetime import datetime, timedelta, time as dtime
from typing import Optional, Dict, List, Any, Tuple

import gspread
from oauth2client.service_account import ServiceAccountCredentials

# zoneinfo (Python 3.9+)
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

# Logging
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

# Driver map env (username -> [plates])
DRIVER_PLATE_MAP_JSON = os.getenv("DRIVER_PLATE_MAP", "").strip() or None

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

# NEW tabs
LEAVE_TAB = os.getenv("LEAVE_TAB", "Driver_Leave")
MAINT_TAB = os.getenv("MAINT_TAB", "Vehicle_Maintenance")
EXPENSE_TAB = os.getenv("EXPENSE_TAB", "Trip_Expenses")   # For km / fuel / parking

# Mission columns (0-based)
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

# Trip record column mapping (1-indexed for update_cell)
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

# HEADERS template per tab — conservative: will only be written if sheet empty
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

# Minimal translations
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
        "weekly_report_generated": "Weekly report generated for {period}.",
        "unfinished_warning": "⚠️ You have unfinished missions: {detail}",
    },
    "km": {
        "menu": "ម៉ឺនុយបូត — សូមជ្រើសប៊ូតុង:",
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
        "weekly_report_generated": "បានបង្កើតរបាយការណ៍ប្រចាំសប្តាហ៍ {period}.",
        "unfinished_warning": "⚠️ មាន mission មិនទាន់បញ្ចប់: {detail}",
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
    """
    Conservative header writer: only write headers if the sheet has no rows at all.
    """
    try:
        values = ws.get_all_values()
        if not values:
            ws.insert_row(headers, index=1)
    except Exception:
        logger.exception("Failed to ensure headers (conservative) on worksheet %s", ws.title)

_UUID_RE = re.compile(r'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$', re.I)

def _missions_header_fix_if_needed(ws):
    """
    Detect common misalignment: when the sheet's first data column contains GUID strings
    but the header row's first cell is not 'GUID'. In that case overwrite only the header row
    with the canonical HEADERS_BY_TAB[MISSIONS_TAB]. This aligns existing rows with columns
    without moving data.
    """
    try:
        values = ws.get_all_values()
        if not values:
            return  # nothing to fix
        first_row = values[0]
        # detect header-like: if first_row contains 'No' or 'No.' or 'Name' etc, treat as header
        header_like_keywords = {"no", "no.", "name", "plate", "start", "end", "departure", "arrival", "staff", "roundtrip"}
        is_header_like = any(str(c).strip().lower() in header_like_keywords for c in first_row if c)
        if not is_header_like:
            return  # first row not header -> nothing to fix here
        # if there's at least one data row
        if len(values) < 2:
            return
        second_row = values[1]
        first_cell = str(second_row[0]).strip() if len(second_row) > 0 else ""
        # If first data cell looks like a GUID but header first cell isn't 'GUID', fix header row
        if first_cell and _UUID_RE.match(first_cell):
            header_first = str(first_row[0]).strip().lower() if len(first_row) > 0 else ""
            if header_first != "guid":
                # Overwrite the header row with canonical headers for MISSIONS_TAB (A..L)
                headers = HEADERS_BY_TAB.get(MISSIONS_TAB, [])
                if not headers:
                    return
                # build A1:L1 range update
                try:
                    # Ensure list length equals M_MANDATORY_COLS
                    h = list(headers)
                    while len(h) < M_MANDATORY_COLS:
                        h.append("")
                    # update only header row cells (A1..L1)
                    col_letter_end = chr(ord('A') + M_MANDATORY_COLS - 1)
                    rng = f"A1:{col_letter_end}1"
                    ws.update(rng, [h], value_input_option="USER_ENTERED")
                    logger.info("Fixed MISSIONS header row to canonical headers due to GUID detected in first data column.")
                except Exception:
                    logger.exception("Failed to update header row in MISSIONS sheet.")
    except Exception:
        logger.exception("Error checking/fixing missions header.")

def open_worksheet(tab: str = ""):
    """
    Open specific worksheet tab by name. If missing, create and insert headers (only when created).
    For MISSIONS tab, also run header-fix check if needed.
    """
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
            # special-fix for MISSIONS tab (detect GUID-in-first-col situation)
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

# driver map loaders (same as before)
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

# ===== Time helpers =====
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

# ===== Trip record functions (GPS removed) =====
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

# ===== Missions helpers =====
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

# Helper to detect complementary trip (enhanced multi-city)
def _is_complementary_trip(dep1: str, arr1: str, dep2: str, arr2: str) -> bool:
    if not dep1 or not arr1 or not dep2 or not arr2:
        return False
    dep1, arr1, dep2, arr2 = dep1.strip().upper(), arr1.strip().upper(), dep2.strip().upper(), arr2.strip().upper()
    # direct reverse
    if (dep1 == "PP" and arr1 == "SHV" and dep2 == "SHV" and arr2 == "PP") or (dep1 == "SHV" and arr1 == "PP" and dep2 == "PP" and arr2 == "SHV"):
        return True
    # reversed legs
    if arr1 == dep2 and arr2 == dep1:
        return True
    # multi-hop: if both start and return to PP via other nodes, allow
    if dep1 == "PP" and arr2 == "PP" and dep2 == arr1:
        return True
    # fallback: if destinations and departures align
    if dep1 == arr2 or arr1 == dep2:
        return True
    return False

def end_mission_record(driver: str, plate: str, arrival: str) -> dict:
    ws = open_worksheet(MISSIONS_TAB)
    try:
        vals, start_idx = _missions_get_values_and_data_rows(ws)
        for i in range(len(vals) - 1, start_idx - 1, -1):
            row = _ensure_row_length(vals[i], M_MANDATORY_COLS)
            rec_plate = str(row[M_IDX_PLATE]).strip()
            rec_name = str(row[M_IDX_NAME]).strip()
            rec_end = str(row[M_IDX_END]).strip()
            rec_start = str(row[M_IDX_START]).strip()
            if rec_plate == plate and rec_name == driver and not rec_end:
                row_number = i + 1
                end_ts = now_str()
                try:
                    ws.update_cell(row_number, M_IDX_END + 1, end_ts)
                    ws.update_cell(row_number, M_IDX_ARRIVAL + 1, arrival)
                except Exception:
                    existing = ws.row_values(row_number)
                    existing = _ensure_row_length(existing, M_MANDATORY_COLS)
                    existing[M_IDX_END] = end_ts
                    existing[M_IDX_ARRIVAL] = arrival
                    ws.delete_row(row_number)
                    ws.insert_row(existing, row_number)
                logger.info("Updated mission end for row %d plate=%s driver=%s", row_number, plate, driver)
                s_dt = parse_ts(rec_start) if rec_start else None
                if not s_dt:
                    return {"ok": True, "message": f"Mission end recorded for {plate} at {end_ts}", "merged": False}
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
                cur_dep = str(row[M_IDX_DEPART]).strip()
                cur_arr = arrival
                for comp in candidates:
                    if _is_complementary_trip(cur_dep, cur_arr, comp["dep"], comp["arr"]):
                        found_pair = comp
                        break
                if not found_pair:
                    return {"ok": True, "message": f"Mission end recorded for {plate} at {end_ts}", "merged": False}
                other_idx = found_pair["idx"]
                other_start = found_pair["start"]
                primary_idx = i if s_dt <= other_start else other_idx
                secondary_idx = other_idx if primary_idx == i else i
                primary_row_number = primary_idx + 1
                secondary_row_number = secondary_idx + 1
                if primary_idx == i:
                    return_start = found_pair["start"].strftime(TS_FMT)
                    return_end = found_pair["end"].strftime(TS_FMT) if found_pair["end"] else ""
                else:
                    return_start = s_dt.strftime(TS_FMT)
                    return_end = end_ts
                try:
                    ws.update_cell(primary_row_number, M_IDX_ROUNDTRIP + 1, "Yes")
                    ws.update_cell(primary_row_number, M_IDX_RETURN_START + 1, return_start)
                    ws.update_cell(primary_row_number, M_IDX_RETURN_END + 1, return_end)
                except Exception:
                    existing = ws.row_values(primary_row_number)
                    existing = _ensure_row_length(existing, M_MANDATORY_COLS)
                    existing[M_IDX_ROUNDTRIP] = "Yes"
                    existing[M_IDX_RETURN_START] = return_start
                    existing[M_IDX_RETURN_END] = return_end
                    ws.delete_row(primary_row_number)
                    ws.insert_row(existing, primary_row_number)
                try:
                    ws.delete_row(secondary_row_number)
                    logger.info("Deleted secondary mission row %d after merging into %d", secondary_row_number, primary_row_number)
                except Exception:
                    try:
                        ws.update_cell(secondary_row_number, M_IDX_ROUNDTRIP + 1, "Merged")
                    except Exception:
                        logger.exception("Failed to delete or mark secondary merged row.")
                return {"ok": True, "message": f"Mission end recorded and merged for {plate} at {end_ts}", "merged": True, "driver": driver, "plate": plate}
        return {"ok": False, "message": "No open mission found"}
    except Exception as e:
        logger.exception("Failed to update mission end: %s", e)
        return {"ok": False, "message": "Failed to write mission end to sheet: " + str(e)}

def mission_rows_for_period(start_date: datetime, end_date: datetime) -> List[List[Any]]:
    ws = open_worksheet(MISSIONS_TAB)
    out = []
    try:
        vals, start_idx = _missions_get_values_and_data_rows(ws)
        for r in vals[start_idx:]:
            r = _ensure_row_length(r, M_MANDATORY_COLS)
            start = str(r[M_IDX_START]).strip()
            if not start:
                continue
            s_dt = parse_ts(start)
            if not s_dt:
                continue
            if start_date <= s_dt < end_date:
                out.append([r[M_IDX_GUID], r[M_IDX_NO], r[M_IDX_NAME], r[M_IDX_PLATE], r[M_IDX_START], r[M_IDX_END], r[M_IDX_DEPART], r[M_IDX_ARRIVAL], r[M_IDX_STAFF], r[M_IDX_ROUNDTRIP], r[M_IDX_RETURN_START], r[M_IDX_RETURN_END]])
        return out
    except Exception:
        logger.exception("Failed to fetch mission rows")
        return []

def write_mission_report_rows(rows: List[List[Any]], period_label: str) -> bool:
    try:
        ws = open_worksheet(MISSIONS_REPORT_TAB)
        ws.append_row([f"Report: {period_label}"], value_input_option="USER_ENTERED")
        ws.append_row(HEADERS_BY_TAB.get(MISSIONS_REPORT_TAB, []), value_input_option="USER_ENTERED")
        for r in rows:
            r = _ensure_row_length(r, M_MANDATORY_COLS)
            ws.append_row(r, value_input_option="USER_ENTERED")
        rt_counts: Dict[str, int] = {}
        for r in rows:
            name = r[2] if len(r) > 2 else ""
            roundtrip = str(r[9]).strip().lower() if len(r) > 9 else ""
            if name and roundtrip == "yes":
                rt_counts[name] = rt_counts.get(name, 0) + 1
        ws.append_row(["Roundtrip Summary by Driver:"], value_input_option="USER_ENTERED")
        if rt_counts:
            ws.append_row(["Driver", "Roundtrip Count"], value_input_option="USER_ENTERED")
            for driver, cnt in sorted(rt_counts.items(), key=lambda x: (-x[1], x[0])):
                ws.append_row([driver, cnt], value_input_option="USER_ENTERED")
        else:
            ws.append_row(["No roundtrips found in this period."], value_input_option="USER_ENTERED")
        return True
    except Exception:
        logger.exception("Failed to write mission report to sheet.")
        return False

def count_roundtrips_per_driver_month(start_date: datetime, end_date: datetime) -> Dict[str, int]:
    counts: Dict[str, int] = {}
    try:
        ws = open_worksheet(MISSIONS_TAB)
        vals, start_idx = _missions_get_values_and_data_rows(ws)
        for r in vals[start_idx:]:
            r = _ensure_row_length(r, M_MANDATORY_COLS)
            start = str(r[M_IDX_START]).strip()
            if not start:
                continue
            s_dt = parse_ts(start)
            if not s_dt or not (start_date <= s_dt < end_date):
                continue
            rt = str(r[M_IDX_ROUNDTRIP]).strip().lower()
            if rt != "yes":
                continue
            name = str(r[M_IDX_NAME]).strip() or "Unknown"
            counts[name] = counts.get(name, 0) + 1
    except Exception:
        logger.exception("Failed to count roundtrips per driver")
    return counts

def count_roundtrips_per_driver_year(year: int) -> Dict[str, int]:
    start = datetime(year, 1, 1)
    end = datetime(year + 1, 1, 1)
    return count_roundtrips_per_driver_month(start, end)

def write_roundtrip_summary_tab(month_label: str, counts: Dict[str, int]) -> Optional[str]:
    tab_name = f"Roundtrip_Summary_{month_label}"
    try:
        gc = get_gspread_client()
        sh = gc.open(GOOGLE_SHEET_NAME)
        try:
            existing = sh.worksheet(tab_name)
            try:
                sh.del_worksheet(existing)
            except Exception:
                existing.clear()
        except Exception:
            pass
        ws = open_worksheet(tab_name)
        ws.append_row(["Driver", "Roundtrip Count (month)"], value_input_option="USER_ENTERED")
        for driver, cnt in sorted(counts.items(), key=lambda x: (-x[1], x[0])):
            ws.append_row([driver, cnt], value_input_option="USER_ENTERED")
        return tab_name
    except Exception:
        logger.exception("Failed to write roundtrip summary tab")
        return None

def write_roundtrip_summary_csv(month_label: str, counts: Dict[str, int]) -> Optional[str]:
    fname = f"roundtrip_summary_{month_label}.csv"
    try:
        local_path = os.path.join(os.getcwd(), fname)
        with open(local_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(["Driver", "Roundtrip Count (month)"])
            for driver, cnt in sorted(counts.items(), key=lambda x: (-x[1], x[0])):
                writer.writerow([driver, cnt])
        return local_path
    except Exception:
        logger.exception("Failed to write roundtrip summary CSV")
        return None

# ===== Driver Leave =====
def add_driver_leave(driver: str, start: str, end: str, reason: str, notes: str = "") -> dict:
    ws = open_worksheet(LEAVE_TAB)
    try:
        ws.append_row([driver, start, end, reason, notes], value_input_option="USER_ENTERED")
        return {"ok": True}
    except Exception as e:
        logger.exception("Failed to add leave record")
        return {"ok": False, "message": str(e)}

def get_driver_leaves(driver: str) -> List[dict]:
    ws = open_worksheet(LEAVE_TAB)
    out = []
    try:
        rows = ws.get_all_records()
        for r in rows:
            if str(r.get("Driver", "")).strip() == driver:
                out.append(r)
        return out
    except Exception:
        logger.exception("Failed to read leaves")
        return []

# ===== Vehicle Maintenance =====
def add_vehicle_maintenance(plate: str, mileage: str, item: str, cost: str, date: str, workshop: str, notes: str = "") -> dict:
    ws = open_worksheet(MAINT_TAB)
    try:
        ws.append_row([plate, mileage, item, cost, date, workshop, notes], value_input_option="USER_ENTERED")
        return {"ok": True}
    except Exception as e:
        logger.exception("Failed to write maintenance row")
        return {"ok": False, "message": str(e)}

def get_vehicle_maintenance(plate: str) -> List[dict]:
    ws = open_worksheet(MAINT_TAB)
    out = []
    try:
        rows = ws.get_all_records()
        for r in rows:
            if str(r.get("Plate", "")).strip() == plate:
                out.append(r)
        return out
    except Exception:
        logger.exception("Failed to read maintenance rows")
        return []

# ===== Trip Expenses (Admin only helper) =====
ADMIN_USERS = set(os.getenv("BOT_ADMINS", "markpeng1,kmnyy").split(",")) if os.getenv("BOT_ADMINS") else set()
BOT_ADMINS_SET = {u.strip() for u in os.getenv("BOT_ADMINS", "").split(",") if u.strip()}

def add_trip_expense(plate: str, driver: str, mileage: str, fuel: str, parking: str, other: str) -> dict:
    ws = open_worksheet(EXPENSE_TAB)
    try:
        ws.append_row([plate, driver, now_str(), mileage, fuel, parking, other], value_input_option="USER_ENTERED")
        return {"ok": True}
    except Exception as e:
        logger.exception("Failed to add trip expense")
        return {"ok": False, "message": str(e)}

# ===== Unfinished Missions =====
def detect_unfinished_missions() -> Dict[str, List[dict]]:
    ws = open_worksheet(MISSIONS_TAB)
    out: Dict[str, List[dict]] = {}
    try:
        vals, start_idx = _missions_get_values_and_data_rows(ws)
        for r in vals[start_idx:]:
            row = _ensure_row_length(r, M_MANDATORY_COLS)
            end = str(row[M_IDX_END]).strip()
            if not end:
                driver = str(row[M_IDX_NAME]).strip()
                entry = {"plate": row[M_IDX_PLATE], "start": row[M_IDX_START], "depart": row[M_IDX_DEPART]}
                out.setdefault(driver, []).append(entry)
        return out
    except Exception:
        logger.exception("Failed to detect unfinished missions")
        return {}

async def notify_unfinished_missions(context: ContextTypes.DEFAULT_TYPE):
    bot = context.bot
    try:
        unfinished = detect_unfinished_missions()
        for driver, items in unfinished.items():
            text = "Unfinished missions:\n" + "\n".join([f"- Plate {i['plate']} from {i['depart']} at {i['start']}" for i in items])
            try:
                if driver:
                    to = driver if driver.startswith("@") else f"@{driver}"
                    await bot.send_message(chat_id=to, text=text)
            except Exception:
                logger.debug("Failed to DM driver %s; skipping.", driver)
    except Exception:
        logger.exception("Failed notify_unfinished_missions")

# ===== Sheet cleanup / auto-repair =====
def cleanup_old_rows(tab: str, max_rows: int = 2000):
    try:
        ws = open_worksheet(tab)
        rows = ws.get_all_values()
        if len(rows) > max_rows + 1:
            header = rows[0]
            keep = rows[-max_rows:]
            ws.clear()
            ws.insert_row(header, index=1)
            for r in keep:
                ws.append_row(r, value_input_option="USER_ENTERED")
            logger.info("Sheet %s cleaned to last %s rows", tab, max_rows)
    except Exception:
        logger.exception("Failed cleanup for %s", tab)

def auto_repair_all_sheets():
    for tab, headers in HEADERS_BY_TAB.items():
        try:
            ws = open_worksheet(tab)
            ensure_sheet_has_headers_conservative(ws, headers)
            if tab == MISSIONS_TAB:
                _missions_header_fix_if_needed(ws)
        except Exception:
            logger.exception("auto_repair failed for tab=%s", tab)

# ===== Telegram UI helpers & handlers =====
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
    kb = [[KeyboardButton("/start_trip")], [KeyboardButton("/end_trip")], [KeyboardButton("/menu")]]
    return ReplyKeyboardMarkup(kb, resize_keyboard=True, one_time_keyboard=False)

async def menu_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        if update.effective_message:
            await update.effective_message.delete()
    except Exception:
        pass
    user_lang = context.user_data.get("lang", DEFAULT_LANG)
    text = t(user_lang, "menu")
    keyboard = [
        [InlineKeyboardButton("Start trip", callback_data="show_start"),
         InlineKeyboardButton("End trip", callback_data="show_end")],
        [InlineKeyboardButton("Mission start", callback_data="show_mission_start"),
         InlineKeyboardButton("Mission end", callback_data="show_mission_end")],
        [InlineKeyboardButton("Admin finance", callback_data="fin|start|odo"),
         InlineKeyboardButton("Help", callback_data="help")],
    ]
    try:
        await context.bot.send_chat_action(chat_id=update.effective_chat.id, action=ChatAction.TYPING)
    except Exception:
        pass
    await update.effective_chat.send_message(text=text, reply_markup=InlineKeyboardMarkup(keyboard))

# ---------- NEW: /setup_menu handler ----------
async def setup_menu_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Admin-only: posts the main menu message into the current chat and pins it.
    """
    user = update.effective_user
    chat = update.effective_chat
    if not chat:
        return
    username = user.username if user else None
    # auth: must be chat admin or in BOT_ADMINS
    is_admin_flag = False
    if chat and user:
        is_admin_flag = await is_chat_admin(context, chat_id=chat.id, user_id=user.id, username=username)
    else:
        if username and username.lstrip("@") in BOT_ADMINS_SET:
            is_admin_flag = True
    if not is_admin_flag:
        try:
            await update.effective_chat.send_message("You are not authorized to run /setup_menu.")
        except Exception:
            pass
        return
    user_lang = context.user_data.get("lang", DEFAULT_LANG)
    text = t(user_lang, "menu")
    keyboard = [
        [InlineKeyboardButton("Start trip", callback_data="show_start"),
         InlineKeyboardButton("End trip", callback_data="show_end")],
        [InlineKeyboardButton("Mission start", callback_data="show_mission_start"),
         InlineKeyboardButton("Mission end", callback_data="show_mission_end")],
        [InlineKeyboardButton("Admin finance", callback_data="fin|start|odo"),
         InlineKeyboardButton("Help", callback_data="help")],
    ]
    try:
        sent = await update.effective_chat.send_message(text=text, reply_markup=InlineKeyboardMarkup(keyboard))
        try:
            # Try to pin the message. Bot must have permission to pin messages in the group.
            await context.bot.pin_chat_message(chat_id=chat.id, message_id=sent.message_id, disable_notification=False)
            logger.info("Pinned menu message in chat %s", chat.id)
        except Exception:
            logger.exception("Failed to pin menu message (missing permission?).")
    except Exception:
        logger.exception("Failed to send menu message for setup_menu.")

# ---------- end /setup_menu ----------

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

# Admin runtime check: prefer chat admin, fallback to BOT_ADMINS env set
async def is_chat_admin(context: ContextTypes.DEFAULT_TYPE, chat_id: int, user_id: int, username: Optional[str] = None) -> bool:
    try:
        member = await context.bot.get_chat_member(chat_id=chat_id, user_id=user_id)
        if member and getattr(member, "status", None) in ("administrator", "creator"):
            return True
    except Exception:
        logger.debug("Could not fetch chat member for admin check (chat_id=%s user_id=%s). Falling back to BOT_ADMINS if provided.", chat_id, user_id)
    if username:
        uname = username.lstrip("@")
        if uname in BOT_ADMINS_SET:
            return True
    return False

# ------------------ Admin finance (complete) ------------------

# helper: build plates inline keyboard (reuse PLATES global)
def _build_plate_buttons_for_fin(typ: str):
    buttons = []
    row = []
    for i, plate in enumerate(PLATES, 1):
        cb = f"fin|select|{typ}|{plate}"
        row.append(InlineKeyboardButton(plate, callback_data=cb))
        if i % 3 == 0:
            buttons.append(row)
            row = []
    if row:
        buttons.append(row)
    buttons.append([InlineKeyboardButton("Cancel", callback_data="fin|cancel")])
    return InlineKeyboardMarkup(buttons)

# helper: build quick-amount inline keyboard
def _build_amount_buttons_for_fin(typ: str, plate: str):
    quick_amounts = os.getenv("FINANCE_QUICK_AMOUNTS", "5,10,20,50,100").split(",")
    quick_amounts = [a.strip() for a in quick_amounts if a.strip()]
    buttons = []
    row = []
    for i, a in enumerate(quick_amounts, 1):
        cb = f"fin|amount|{typ}|{plate}|{a}"
        row.append(InlineKeyboardButton(a, callback_data=cb))
        if i % 3 == 0:
            buttons.append(row)
            row = []
    if row:
        buttons.append(row)
    buttons.append([InlineKeyboardButton("Custom", callback_data=f"fin|amount|{typ}|{plate}|custom"),
                    InlineKeyboardButton("Cancel", callback_data="fin|cancel")])
    return InlineKeyboardMarkup(buttons)

# admin_finance entry command (shows top-level finance types)
async def group_admin_finance_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    username = user.username if user else None
    chat_id = update.effective_chat.id if update.effective_chat else None
    is_admin_flag = False
    if chat_id and user:
        is_admin_flag = await is_chat_admin(context, chat_id=chat_id, user_id=user.id, username=username)
    else:
        if username and username.lstrip("@") in BOT_ADMINS_SET:
            is_admin_flag = True
    if not is_admin_flag:
        await update.effective_chat.send_message("You are not authorized to use admin finance.")
        return
    kb = [
        [InlineKeyboardButton("Report Odometer", callback_data="fin|start|odo"),
         InlineKeyboardButton("Report Fuel", callback_data="fin|start|fuel")],
        [InlineKeyboardButton("Report Parking", callback_data="fin|start|parking")]
    ]
    await update.effective_chat.send_message("Choose finance action:", reply_markup=InlineKeyboardMarkup(kb))

# Unified callback handler for admin finance inline form
async def admin_finance_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    if not query:
        return
    await query.answer()
    data = query.data or ""
    user = query.from_user
    username = user.username if user else None
    chat_id = query.message.chat.id if query.message else None

    # auth check
    is_admin_flag = False
    if chat_id and user:
        is_admin_flag = await is_chat_admin(context, chat_id=chat_id, user_id=user.id, username=username)
    else:
        if username and username.lstrip("@") in BOT_ADMINS_SET:
            is_admin_flag = True
    if not is_admin_flag:
        try:
            await query.edit_message_text("Unauthorized")
        except Exception:
            pass
        return

    # Normalize prefixes: allow finance|... as alias of fin|
    if data.startswith("finance|"):
        data = "fin|" + data.split("|", 1)[1]

    parts = data.split("|")
    # forms:
    # fin|start|<typ>
    # fin|select|<typ>|<plate>
    # fin|amount|<typ>|<plate>|<amount_or_custom>
    # fin|cancel

    if data == "fin|cancel":
        try:
            await query.edit_message_text("Canceled.")
        except Exception:
            pass
        return

    if len(parts) >= 3 and parts[1] == "start":
        typ = parts[2]
        try:
            await query.edit_message_text(f"Select plate for {typ}:", reply_markup=_build_plate_buttons_for_fin(typ))
        except Exception:
            await query.message.reply_text(f"Select plate for {typ}:", reply_markup=_build_plate_buttons_for_fin(typ))
        return

    if len(parts) >= 4 and parts[1] == "select":
        typ = parts[2]
        plate = parts[3]
        try:
            await query.edit_message_text(f"Enter amount for {typ} — plate {plate}:", reply_markup=_build_amount_buttons_for_fin(typ, plate))
        except Exception:
            await query.message.reply_text(f"Enter amount for {typ} — plate {plate}:", reply_markup=_build_amount_buttons_for_fin(typ, plate))
        return

    if len(parts) >= 5 and parts[1] == "amount":
        typ = parts[2]
        plate = parts[3]
        amount = parts[4]
        if amount != "custom":
            driver = username.lstrip("@") if username else ""
            try:
                # write to your maintenance sheet (you can change to add_trip_expense if preferred)
                res = add_vehicle_maintenance(plate, today_date_str(), amount, typ.capitalize(), "", f"Reported by @{driver}")
                if res.get("ok"):
                    await query.edit_message_text(f"{typ.capitalize()} recorded for {plate}: {amount}")
                else:
                    await query.edit_message_text(f"Failed to record {typ} for {plate}.")
            except Exception:
                logger.exception("Failed to record quick amount")
                try:
                    await query.edit_message_text("Failed to record amount (error).")
                except Exception:
                    pass
            return
        else:
            # Custom amount flow: ask user to reply in chat (ForceReply), store pending state
            context.user_data["pending_finance"] = {"typ": typ, "plate": plate, "initiator_id": user.id}
            try:
                prompt = await query.message.reply_text(
                    f"Please reply in this chat with amount and optional notes for {typ} on {plate}.\nFormat: <amount> [notes]\nExample: 23.5 bought diesel",
                    reply_markup=ForceReply(selective=True),
                )
                try:
                    await query.edit_message_text(f"Waiting for custom amount for {plate} — please reply to the prompt.")
                except Exception:
                    pass
            except Exception:
                try:
                    await query.edit_message_text("Please send a message with amount and optional notes.")
                except Exception:
                    pass
            return

    try:
        await query.edit_message_text("Invalid admin finance action.")
    except Exception:
        pass

# Message handler: process custom finance replies (when pending_finance exists)
async def process_finance_reply(update: Update, context: ContextTypes.DEFAULT_TYPE):
    pending = context.user_data.get("pending_finance")
    if not pending:
        return  # not a finance reply; let other handlers process
    user = update.effective_user
    username = user.username if user else None
    initiator = pending.get("initiator_id")
    # Only the initiator may reply to the ForceReply prompt
    if initiator and user and user.id != initiator:
        return
    typ = pending.get("typ")
    plate = pending.get("plate")
    text = (update.message.text or "").strip()
    if not text:
        await update.effective_chat.send_message("Empty input; canceled.")
        context.user_data.pop("pending_finance", None)
        return
    parts = text.split(None, 1)
    amount = parts[0]
    notes = parts[1] if len(parts) > 1 else ""
    driver = username.lstrip("@") if username else ""
    try:
        res = add_vehicle_maintenance(plate, today_date_str(), amount, typ.capitalize(), "", f"{notes} (reported by @{driver})")
        if res.get("ok"):
            await update.effective_chat.send_message(f"{typ.capitalize()} recorded for {plate}: {amount} {notes}")
        else:
            await update.effective_chat.send_message("Failed to record the expense.")
    except Exception:
        logger.exception("Failed to record custom finance input")
        await update.effective_chat.send_message("Failed (exception) to record the expense.")
    context.user_data.pop("pending_finance", None)
    return

# ------------------ End admin finance ------------------

# ------------------ plate_callback (fixed & extended) ------------------
async def plate_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    if not query:
        return
    await query.answer()
    data = query.data or ""
    user = query.from_user
    username = user.username or f"{user.first_name or ''} {user.last_name or ''}".strip()
    user_lang = context.user_data.get("lang", DEFAULT_LANG)

    # Accept both prefixes 'fin|' and 'finance|' - route finance| to admin_finance_callback
    if data.startswith("finance|") or data.startswith("fin|"):
        await admin_finance_callback(update, context)
        return

    # menu navigation and handlers
    if data == "show_start":
        try:
            await query.edit_message_text(t(user_lang, "choose_start"), reply_markup=build_plate_keyboard("start"))
        except Exception:
            try:
                await query.message.reply_text(t(user_lang, "choose_start"), reply_markup=build_plate_keyboard("start"))
            except Exception:
                pass
        return

    if data == "show_end":
        try:
            await query.edit_message_text(t(user_lang, "choose_end"), reply_markup=build_plate_keyboard("end"))
        except Exception:
            try:
                await query.message.reply_text(t(user_lang, "choose_end"), reply_markup=build_plate_keyboard("end"))
            except Exception:
                pass
        return

    if data == "menu_full":
        try:
            await query.edit_message_text(t(user_lang, "menu"), reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("Start trip", callback_data="show_start"), InlineKeyboardButton("End trip", callback_data="show_end")],
                [InlineKeyboardButton("Help", callback_data="help")]
            ]))
        except Exception:
            pass
        return

    if data == "help":
        try:
            await query.edit_message_text(t(user_lang, "help"))
        except Exception:
            pass
        return

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
            try:
                await query.edit_message_text(t(user_lang, "mission_end_ok", plate=plate, end_date=now_str(), arr=arrival))
            except Exception:
                pass
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
            try:
                await query.edit_message_text("❌ " + res.get("message", ""))
            except Exception:
                pass
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

    # unknown -> clear fallback message
    try:
        await query.edit_message_text(t(user_lang, "invalid_sel"))
    except Exception:
        pass

# ------------------ End plate_callback ------------------

# ===== Other handlers: location_or_skip, lang, mission_report etc. =====
async def location_or_skip(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        if update.effective_message:
            await update.effective_message.delete()
    except Exception:
        pass
    user = update.effective_user
    user_lang = context.user_data.get("lang", DEFAULT_LANG)
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
    # Also allow finance custom fallback here if someone replies without ForceReply (rare)
    if context.user_data.get("pending_finance") and update.message and update.message.text:
        # let process_finance_reply handle it (ensure it's called early in handler registration)
        return
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
    return

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

AUTO_KEYWORD_PATTERN = r'(?i)\b(start|menu|start trip|end trip|trip|出车|还车|返程)\b'

async def auto_menu_listener(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_chat and update.effective_chat.type in ("group", "supergroup"):
        text = (update.effective_message.text or "").strip()
        if not text:
            return
        if text.startswith("/"):
            return
        user_lang = context.user_data.get("lang", DEFAULT_LANG)
        keyboard = [
            [InlineKeyboardButton("Start trip", callback_data="show_start"),
             InlineKeyboardButton("End trip", callback_data="show_end")],
            [InlineKeyboardButton("Open menu", callback_data="menu_full")],
        ]
        await update.effective_chat.send_message(t(user_lang, "menu"), reply_markup=InlineKeyboardMarkup(keyboard))

# scheduling daily summary and monthly auto-report
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
    # If first day of month, auto-generate previous month's mission report
    try:
        if now.day == 1:
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

async def send_weekly_summary_job(context: ContextTypes.DEFAULT_TYPE):
    job_data = context.job.data if hasattr(context.job, "data") else {}
    chat_id = job_data.get("chat_id") or SUMMARY_CHAT_ID
    if not chat_id:
        logger.info("SUMMARY_CHAT_ID not set; skipping weekly summary.")
        return
    if SUMMARY_TZ and ZoneInfo:
        try:
            tz = ZoneInfo(SUMMARY_TZ)
            now = datetime.now(tz)
        except Exception:
            now = _now_dt()
    else:
        now = _now_dt()
    start = now - timedelta(days=7)
    try:
        totals = aggregate_for_period(start, now)
        if not totals:
            await context.bot.send_message(chat_id=chat_id, text=f"No records in the past week ({start.strftime(DATE_FMT)} to {now.strftime(DATE_FMT)}).")
        else:
            lines = [f"Weekly summary ({start.strftime(DATE_FMT)} - {now.strftime(DATE_FMT)}):"]
            for plate, minutes in sorted(totals.items()):
                h = minutes // 60
                m = minutes % 60
                lines.append(f"{plate}: {h}h{m}m")
            await context.bot.send_message(chat_id=chat_id, text="\n".join(lines))
    except Exception:
        logger.exception("Failed to send weekly summary.")

async def detect_unfinished_missions_job(context: ContextTypes.DEFAULT_TYPE):
    try:
        ws = open_worksheet(MISSIONS_TAB)
        vals, start_idx = _missions_get_values_and_data_rows(ws)
        threshold_hours = int(os.getenv("UNFINISHED_MISSION_THRESHOLD_HOURS", "24"))
        now = _now_dt()
        notified = 0
        for i in range(start_idx, len(vals)):
            row = _ensure_row_length(vals[i], M_MANDATORY_COLS)
            start_ts = str(row[M_IDX_START]).strip()
            end_ts = str(row[M_IDX_END]).strip()
            driver = str(row[M_IDX_NAME]).strip()
            plate = str(row[M_IDX_PLATE]).strip()
            guid = str(row[M_IDX_GUID]).strip()
            if start_ts and not end_ts:
                s_dt = parse_ts(start_ts)
                if not s_dt:
                    continue
                delta = now - s_dt
                if delta.total_seconds() >= threshold_hours * 3600:
                    try:
                        if driver:
                            msg = f"You have an open mission (GUID: {guid}) started at {start_ts} on plate {plate}. Please /mission_end or contact dispatch."
                            await context.bot.send_message(chat_id=f"@{driver}", text=msg)
                            notified += 1
                    except Exception:
                        logger.debug("Could not DM driver %s about unfinished mission GUID=%s", driver, guid)
        if notified:
            logger.info("Notified %d drivers about unfinished missions", notified)
    except Exception:
        logger.exception("Failed to detect unfinished missions")

def register_ui_handlers(application):
    application.add_handler(CommandHandler("menu", menu_command))
    application.add_handler(CommandHandler(["start_trip", "start"], start_trip_command))
    application.add_handler(CommandHandler(["end_trip", "end"], end_trip_command))
    application.add_handler(CommandHandler("lang", lang_command))
    application.add_handler(CommandHandler("mission_start", mission_start_command))
    application.add_handler(CommandHandler("mission_end", mission_end_command))
    application.add_handler(CommandHandler("mission_report", mission_report_command))

    # Admin finance: command and callback handler
    application.add_handler(CommandHandler("admin_finance", group_admin_finance_command))
    application.add_handler(CallbackQueryHandler(admin_finance_callback, pattern=r'^(fin\||finance\|)'))

    # plate callback (covers start/end/mission selection etc.)
    application.add_handler(CallbackQueryHandler(plate_callback))

    # Message handler for processing custom finance replies — should be early so it captures ForceReply responses
    application.add_handler(MessageHandler(filters.TEXT & (~filters.COMMAND), process_finance_reply), group=0)

    # other handlers
    application.add_handler(MessageHandler(filters.Regex(r'(?i)^/skip$') | (filters.TEXT & (~filters.COMMAND)), location_or_skip))
    application.add_handler(MessageHandler(filters.Regex(AUTO_KEYWORD_PATTERN) & filters.ChatType.GROUPS, auto_menu_listener))
    application.add_handler(CommandHandler("help", lambda u, c: u.message.reply_text(t(c.user_data.get("lang", DEFAULT_LANG), "help"))))

    # /setup_menu (admin-only) - posts & pins the main menu in the chat
    application.add_handler(CommandHandler("setup_menu", setup_menu_command))

    # fallback admin short commands (informational)
    application.add_handler(CommandHandler("report_odo", lambda u, c: u.message.reply_text("Use /admin_finance or the menu")))
    application.add_handler(CommandHandler("report_fuel", lambda u, c: u.message.reply_text("Use /admin_finance or the menu")))
    application.add_handler(CommandHandler("report_parking", lambda u, c: u.message.reply_text("Use /admin_finance or the menu")))

    try:
        async def _set_cmds():
            try:
                await application.bot.set_my_commands([
                    BotCommand("start_trip", "Start a trip (select plate)"),
                    BotCommand("end_trip", "End a trip (select plate)"),
                    BotCommand("menu", "Open trip menu"),
                    BotCommand("setup_menu", "Post & pin the command menu (admin only)"),
                    BotCommand("lang", "Set language /lang en|km"),

                    # Missions
                    BotCommand("mission_start", "Start a driver mission"),
                    BotCommand("mission_end", "End a driver mission"),
                    BotCommand("mission_report", "Generate mission report: /mission_report month YYYY-MM"),

                    # Admin finance
                    BotCommand("admin_finance", "Admin: inline finance form"),
                    BotCommand("report_odo", "Manually report odometer (admin)"),
                    BotCommand("report_fuel", "Manually report fuel (admin)"),
                    BotCommand("report_parking", "Manually report parking (admin)"),

                    # Leave
                    BotCommand("leave_add", "Add driver leave record"),
                    BotCommand("leave_list", "View driver leave records"),

                    # Maintenance
                    BotCommand("maint_add", "Add vehicle maintenance record"),
                    BotCommand("maint_list", "View vehicle maintenance"),

                    # Help
                    BotCommand("help", "Show help page")
                ])
            except Exception:
                logger.exception("Failed to set bot commands.")
        if hasattr(application, "create_task"):
            application.create_task(_set_cmds())
    except Exception:
        logger.debug("Could not schedule set_my_commands.")

def ensure_env():
    if not BOT_TOKEN:
        raise RuntimeError(t(DEFAULT_LANG, "no_bot_token"))

def schedule_jobs(application):
    try:
        if SUMMARY_CHAT_ID:
            if ZoneInfo and SUMMARY_TZ:
                tz = ZoneInfo(SUMMARY_TZ)
            else:
                tz = None
            job_time = dtime(hour=SUMMARY_HOUR, minute=0, second=0)
            application.job_queue.run_daily(send_daily_summary_job, time=job_time, context={"chat_id": SUMMARY_CHAT_ID}, name="daily_summary", tz=tz)
            application.job_queue.run_repeating(send_weekly_summary_job, interval=7 * 24 * 3600, first=10, context={"chat_id": SUMMARY_CHAT_ID}, name="weekly_summary")
            application.job_queue.run_repeating(detect_unfinished_missions_job, interval=6 * 3600, first=30, name="detect_unfinished")
            application.job_queue.run_repeating(notify_unfinished_missions, interval=6 * 3600, first=60, name="notify_unfinished")
            if int(os.getenv("CLEAN_OLDER_THAN_DAYS", "0")) > 0:
                application.job_queue.run_daily(lambda ctx: cleanup_old_rows(), time=dtime(hour=3, minute=0), name="clean_old_rows")
            logger.info("Scheduled jobs.")
        else:
            logger.info("SUMMARY_CHAT_ID not configured; scheduled jobs disabled.")
    except Exception:
        logger.exception("Failed to schedule jobs.")

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
    schedule_jobs(application)
    logger.info("Starting driver-bot polling...")
    application.run_polling()

if __name__ == "__main__":
    main()
