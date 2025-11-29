#!/usr/bin/env python3
"""
Driver Bot — Updated: auto-update sheet headers; finance prompts removed after entry.
- Ensures EXPENSE_TAB header matches canonical header and updates the sheet header row when needed.
- Deletes finance prompt messages (and the edited origin callback message) after user replies.
- Adds mission days/per-diem A-2 rule, leave statistics, improved mission merge logic.
"""
import os
import json
import base64
import logging
import uuid
import re
import math
from datetime import datetime, timedelta, time as dtime
from typing import Optional, Dict, List, Any
import urllib.request

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

BOT_ADMINS_DEFAULT = "markpeng1"

# Missions mapping (0-based)
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

# Records columns (1-indexed for update_cell)
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

# per diem
PER_DIEM = float(os.getenv("PER_DIEM", "15.0"))
ANNUAL_LEAVE_DAYS = int(os.getenv("ANNUAL_LEAVE_DAYS", "12"))

HEADERS_BY_TAB: Dict[str, List[str]] = {
    RECORDS_TAB: ["Date", "Driver", "Plate", "Start DateTime", "End DateTime", "Duration"],
    MISSIONS_TAB: ["GUID", "No.", "Name", "Plate", "Start Date", "End Date", "Departure", "Arrival", "Staff Name", "Roundtrip", "Return Start", "Return End"],
    MISSIONS_REPORT_TAB: ["GUID", "No.", "Name", "Plate", "Start Date", "End Date", "Departure", "Arrival", "Staff Name", "Roundtrip", "Return Start", "Return End"],
    SUMMARY_TAB: ["Date", "PeriodType", "TotalsJSON", "HumanSummary"],
    DRIVERS_TAB: ["Username", "Plates"],
    LEAVE_TAB: ["Driver", "Start Date", "End Date", "Reason", "Notes"],
    MAINT_TAB: ["Plate", "Mileage", "Maintenance Item", "Cost", "Date", "Workshop", "Notes"],
    EXPENSE_TAB: ["Plate", "Driver", "DateTime", "Mileage", "Delta KM", "Fuel Cost", "Parking Fee", "Other Fee", "Invoice", "DriverPaid"],
}

# Translation / templates (English + Khmer skeleton). We add new English entries required.
TR = {
    "en": {
        "menu": "Driver Bot Menu — tap a button:",
        "choose_start": "Choose vehicle plate to START trip:",
        "choose_end": "Choose vehicle plate to END trip:",
        "start_ok": "Driver {driver} start trip at {ts}.",
        "end_ok": "Driver {driver} end trip at {ts} (duration {dur}). Driver {driver} completed {n_today} trips today and {n_month} trips in {month}.",
        "not_allowed": "❌ You are not allowed to operate plate: {plate}.",
        "invalid_sel": "Invalid selection.",
        "help": "Help: Use /start_trip or /end_trip and select a plate.",
        "no_bot_token": "Please set BOT_TOKEN environment variable.",
        "mission_start_prompt_plate": "Choose plate to start mission:",
        "mission_start_prompt_depart": "Select departure city:",
        "mission_start_prompt_staff": "Choose staff input or skip:",
        "mission_start_ok": "✅ Mission start for {plate} at {start_date}, from {dep}.",
        "mission_end_prompt_plate": "Choose plate to end mission:",
        "mission_end_prompt_arrival": "Select arrival city:",
        "mission_end_ok": "✅ Mission ended for {plate} at {end_date}, arrived {arr}.",
        "mission_no_open": "No open mission found for {plate}.",
        "roundtrip_merged_notify": "✅ Roundtrip merged for {driver} on {plate}. {count_msg}",
        "roundtrip_monthly_count": "Driver {driver} completed {count} roundtrips this month.",
        "lang_set": "Language set to {lang}.",
        "invalid_amount": "Invalid amount — please send a numeric value like `23.5`.",
        "invalid_odo": "Invalid odometer — please send numeric KM like `12345` or `12345KM`.",
        "confirm_recorded": "{typ} recorded for {plate}: {amount}",
        "leave_prompt": "Reply to this message: <driver_username> <YYYY-MM-DD> <YYYY-MM-DD> <reason> [notes]\nExample: markpeng1 2025-12-01 2025-12-05 annual_leave",
        "leave_confirm": "Leave recorded for {driver}: {start} to {end} ({reason})",
        "fin_inline_prompt": "Inline finance form — reply: <plate> <amount> [notes]\nExample: 2BB-3071 23.5 bought diesel",
        "enter_odo_km": "Enter odometer reading (KM) for {plate}:",
        "enter_fuel_cost": "Enter fuel cost in $ for {plate}: (optionally add `inv:INV123 paid:yes`)",
        "enter_amount_for": "Enter amount in $ for {typ} for {plate}:",
        # New templates added
        "finance_odo_fuel_receipt": "{plate} @ {odo} km + ${fuel} fuel on {date} paid by {by}, difference from previous odo is {delta} km.",
        "finance_expense_receipt": "{plate} {typ} fee ${amt} on {date} paid by {by}.",
        "leave_record_receipt": "Driver {driver} {start} to {end} ({reason}).",
        "leave_summary_msg": "Driver {driver} has {month_count} leave days in {month} and {year_count} leave days in {year}. Remaining annual entitlement: {remaining} days.",
        "mission_departure": "Driver {driver} (plate {plate}) departures from {dep} at {ts}.",
        "mission_arrival": "Driver {driver} (plate {plate}) arrives at {arr} at {ts}.",
        "mission_completed_notify": "✅ Driver {driver} completed {month_count} mission(s) in {month} and {year_count} in {year}. Mission days: {days}, Per-diem: ${per_diem}.",
        "trip_start_msg": "Driver {driver} (plate {plate}) starts trip at {ts}.",
        "trip_end_msg": "Driver {driver} (plate {plate}) ends trip at {ts}.",
        "trip_summary_notify": "Driver {driver} completed {n_today} trip(s) today and {n_month} trip(s) in {month} and {n_year} trip(s) in {year}. Plate {plate} completed {p_today} today, {p_month} in {month}, {p_year} in {year}.",
    },
    "km": {
        # keep existing km templates minimal (not all new keys translated)
        "menu": "ម្ហឺនុយបូត — សូមជ្រើសប៊ូតុង:",
        "choose_start": "ជ្រើស plate ដើម្បីចាប់ផ្តើមដំណើរ:",
        "choose_end": "ជ្រើស plate ដើម្បីបញ្ចប់ដំណើរ:",
        "start_ok": "Driver {driver} start trip at {ts}.",
        "end_ok": "Driver {driver} end trip at {ts} (duration {dur}). Driver {driver} completed {n_today} trips today and {n_month} trips in {month}.",
        "not_allowed": "❌ មិនមានសិទ្ធិប្រើ plate: {plate}.",
        "invalid_sel": "ជម្រើសមិនត្រឹមត្រូវ។",
        "help": "ជំនួយ៖ ប្រើ /start_trip ឬ /end_trip ហើយជ្រើស plate.",
        "mission_start_prompt_plate": "ជ្រើស plate ដើម្បីចាប់ផ្តើម mission:",
        "mission_start_prompt_depart": "ជ្រើសទីក្រុងចេញ:",
        "mission_start_prompt_staff": "ជ្រើសបញ្ចូលឈ្មោះរឺលោត:",
        "mission_start_ok": "✅ ចាប់ផ្ដើម mission {plate} នៅ {start_date} ចេញពី {dep}.",
        "mission_end_prompt_plate": "ជ្រើស plate ដើម្បីបញ្ចប់ mission:",
        "mission_end_prompt_arrival": "ជ្រើសទីក្រុងមកដល់:",
        "mission_no_open": "មិនមានកិច្ចការបើកសម្រាប់ {plate}.",
        "roundtrip_merged_notify": "✅ រួមបញ្ចូល往返 {driver} លើ {plate}. {count_msg}",
        "roundtrip_monthly_count": "អ្នកបើក {driver} បាន往返 {count} ដងខែនេះ.",
        "lang_set": "បានផ្លាស់ភាសាទៅ {lang}.",
        "invalid_amount": "ទឹកប្រាក់ទម្រង់មិនត្រឹមត្រូវ — ផ្ញើលេខដូចជា `23.5`។",
        "invalid_odo": "Odometer មិនត្រឹមត្រូវ — ផ្ញើលេខ KM ដូចជា `12345` ឬ `12345KM`។",
        "confirm_recorded": "{typ} បានកត់សម្រាប់ {plate}: {amount}",
        "leave_prompt": "ឆ្លើយ: <driver_username> <YYYY-MM-DD> <YYYY-MM-DD> <reason> [notes]",
        "leave_confirm": "បានកត់សម្រាកសម្រាប់ {driver}: {start} ដល់ {end} ({reason})",
        "fin_inline_prompt": "Finance form — ឆ្លើយ: <plate> <amount> [notes]",
        "enter_odo_km": "បញ្ជូលលេខម៉ាស៊ីន (KM) សម្រាប់ {plate}:",
        "enter_fuel_cost": "បញ្ជូលចំណាយប្រេង ($) សម្រាប់ {plate}: (អាចបញ្ចូល `inv:INV123 paid:yes`)",
        "enter_amount_for": "បញ្ជូលចំនួន ($) សម្រាប់ {typ} សម្រាប់ {plate}:",
    },
}

def t(user_lang: Optional[str], key: str, **kwargs) -> str:
    lang = (user_lang or DEFAULT_LANG or "en").lower()
    if lang not in SUPPORTED_LANGS:
        lang = "en"
    return TR.get(lang, TR["en"]).get(key, TR["en"].get(key, "")).format(**kwargs)

# ===== Google helpers =====
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
        raise RuntimeError("Google credentials not found.")
    creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_json, SCOPES)
    client = gspread.authorize(creds)
    return client

def ensure_sheet_has_headers_conservative(ws, headers: List[str]):
    """
    If the sheet is empty, insert headers.
    """
    try:
        values = ws.get_all_values()
        if not values:
            ws.insert_row(headers, index=1)
    except Exception:
        logger.exception("Failed to ensure headers on %s", getattr(ws, "title", "<ws>"))

def ensure_sheet_headers_match(ws, headers: List[str]):
    """
    Ensure first row equals headers. If different, update A1.. to new headers.
    This will overwrite the first row.
    """
    try:
        values = ws.get_all_values()
        if not values:
            ws.insert_row(headers, index=1)
            return
        first_row = values[0]
        # normalize lengths for comparison
        norm_first = [str(c).strip() for c in first_row]
        norm_headers = [str(c).strip() for c in headers]
        if norm_first != norm_headers:
            # write header row
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
    """
    Opens a worksheet; ensures headers match templates if available (auto-update).
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
                # if sheet exists, ensure headers match template (overwrite if needed)
                ensure_sheet_has_headers_conservative(ws, template)
                ensure_sheet_headers_match(ws, template)
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
                    ensure_sheet_headers_match(ws, HEADERS_BY_TAB[GOOGLE_SHEET_TAB])
                return ws
            except Exception:
                return _create_tab(GOOGLE_SHEET_TAB, headers=None)
        return sh.sheet1

# Driver map loaders
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

# Trip record functions
def record_start_trip(driver: str, plate: str) -> dict:
    ws = open_worksheet(RECORDS_TAB)
    start_ts = now_str()
    row = [today_date_str(), driver, plate, start_ts, "", ""]
    try:
        ws.append_row(row, value_input_option="USER_ENTERED")
        logger.info("Recorded start trip: %s %s %s", driver, plate, start_ts)
        return {"ok": True, "message": f"Start time recorded for {plate} at {start_ts}", "ts": start_ts}
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
                    # replace row
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

# Missions helpers (as earlier, with careful merging)
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

# mission_days/per-diem (A-2 rule)
def mission_days_for_per_diem(start_dt: datetime, end_dt: datetime) -> int:
    """
    A-2 rule:
    - If end <= start_date+1 @ 12:00 noon => count 1 day
    - Otherwise: days = 1 + ceil( (end - cutoff) / 24h ), where cutoff = start_date + 1 day at 12:00
    """
    if not start_dt or not end_dt:
        return 0
    if end_dt < start_dt:
        end_dt = start_dt
    next_day = (start_dt.date() + timedelta(days=1))
    cutoff = datetime(next_day.year, next_day.month, next_day.day, 12, 0, 0)
    if end_dt <= cutoff:
        return 1
    rem = (end_dt - cutoff)
    extra_days = math.ceil(rem.total_seconds() / (24 * 3600))
    return 1 + extra_days

def per_diem_amount_for_mission_days(days: int) -> float:
    return round(days * PER_DIEM, 2)

# Strict mission end merge (enhanced)
def end_mission_record(driver: str, plate: str, arrival: str) -> dict:
    """
    Enhanced end mission:
    - mark end & arrival for the latest open mission for driver+plate
    - attempt to find ANY other completed mission for same driver+plate that is complementary (opposite legs)
      or, if depart/arrival missing, will still attempt to match and merge.
    - after merge, compute mission days & per-diem, write a small summary row to MISSIONS_REPORT_TAB.
    """
    try:
        ws = open_worksheet(MISSIONS_TAB)
    except Exception as e:
        logger.exception("Failed to open MISSIONS_TAB: %s", e)
        return {"ok": False, "message": "Could not open missions sheet: " + str(e)}

    try:
        vals, start_idx = _missions_get_values_and_data_rows(ws)
        # find the latest open mission (no end) for same driver+plate
        target_idx = None
        for i in range(len(vals) - 1, start_idx - 1, -1):
            row = _ensure_row_length(vals[i], M_MANDATORY_COLS)
            if str(row[M_IDX_PLATE]).strip() == plate and str(row[M_IDX_NAME]).strip() == driver and not str(row[M_IDX_END]).strip():
                target_idx = i
                break
        if target_idx is None:
            return {"ok": False, "message": "No open mission found"}
        # fill end and arrival
        row = _ensure_row_length(vals[target_idx], M_MANDATORY_COLS)
        row_number = target_idx + 1
        end_ts = now_str()
        try:
            ws.update_cell(row_number, M_IDX_END + 1, end_ts)
            ws.update_cell(row_number, M_IDX_ARRIVAL + 1, arrival)
        except Exception:
            existing = ws.row_values(row_number)
            existing = _ensure_row_length(existing, M_MANDATORY_COLS)
            existing[M_IDX_END] = end_ts
            existing[M_IDX_ARRIVAL] = arrival
            try:
                ws.delete_rows(row_number)
            except Exception:
                logger.exception("Fallback: failed to delete row before insert")
            try:
                ws.insert_row(existing, row_number)
            except Exception:
                logger.exception("Fallback: failed to insert updated row")
        logger.info("Mission end set for row %d (%s)", row_number, plate)

        # Now attempt to find ANY other completed mission for same driver+plate to merge with.
        vals2, start_idx2 = _missions_get_values_and_data_rows(ws)
        completed_candidates = []
        for j in range(start_idx2, len(vals2)):
            if j == target_idx:
                continue
            r2 = _ensure_row_length(vals2[j], M_MANDATORY_COLS)
            rn = str(r2[M_IDX_NAME]).strip()
            rp = str(r2[M_IDX_PLATE]).strip()
            rstart = str(r2[M_IDX_START]).strip()
            rend = str(r2[M_IDX_END]).strip()
            if rn == driver and rp == plate and rstart and rend:
                completed_candidates.append({"idx": j, "r": r2})

        # Try to pick best complementary candidate:
        chosen = None
        for comp in completed_candidates:
            r = comp["r"]
            dep1 = str(r[M_IDX_DEPART]).strip()
            arr1 = str(r[M_IDX_ARRIVAL]).strip()
            dep_target = str(row[M_IDX_DEPART]).strip()
            arr_target = arrival
            if dep1 and arr1 and dep_target and arr_target:
                if (dep1 == "PP" and arr1 == "SHV" and dep_target == "SHV" and arr_target == "PP") or \
                   (dep1 == "SHV" and arr1 == "PP" and dep_target == "PP" and arr_target == "SHV"):
                    chosen = comp
                    break
        # if not found by route then pick the most recent completed candidate (closest start time)
        if not chosen and completed_candidates:
            def try_parse(s):
                return parse_ts(s) or datetime.min
            tstart = try_parse(str(row[M_IDX_START]).strip())
            completed_candidates.sort(key=lambda x: abs((try_parse(str(x["r"][M_IDX_START]).strip()) - tstart).total_seconds()))
            chosen = completed_candidates[0]

        if not chosen:
            # nothing to merge with — return simple end recorded
            return {"ok": True, "message": f"Mission end recorded for {plate} at {end_ts}", "merged": False}

        # perform merge: primary = earlier start; secondary = later start
        other_idx = chosen["idx"]
        other_row = _ensure_row_length(chosen["r"], M_MANDATORY_COLS)
        this_start = parse_ts(str(row[M_IDX_START]).strip()) if str(row[M_IDX_START]).strip() else None
        other_start = parse_ts(str(other_row[M_IDX_START]).strip()) if str(other_row[M_IDX_START]).strip() else None

        if this_start and other_start and this_start <= other_start:
            primary_idx = target_idx
            secondary_idx = other_idx
            primary_row = row
            secondary_row = other_row
        else:
            primary_idx = other_idx
            secondary_idx = target_idx
            primary_row = other_row
            secondary_row = row

        primary_row_number = primary_idx + 1
        secondary_row_number = secondary_idx + 1

        # compute return start/end: choose the secondary's start/end as return
        return_start = str(secondary_row[M_IDX_START]).strip()
        return_end = str(secondary_row[M_IDX_END]).strip()

        # update primary row: mark Roundtrip=Yes, set Return Start/End
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
                logger.exception("Failed to delete primary row for fallback")
            try:
                ws.insert_row(existing, primary_row_number)
            except Exception:
                logger.exception("Failed to insert updated primary row for fallback")

        # attempt to delete secondary row (cleanup)
        try:
            all_vals_post, start_idx_post = _missions_get_values_and_data_rows(ws)
            sec_guid = str(secondary_row[M_IDX_GUID]).strip() if secondary_row and len(secondary_row) > M_IDX_GUID else None
            found = False
            if sec_guid:
                for k in range(start_idx_post, len(all_vals_post)):
                    r_k = _ensure_row_length(all_vals_post[k], M_MANDATORY_COLS)
                    if str(r_k[M_IDX_GUID]).strip() == sec_guid:
                        try:
                            ws.delete_rows(k + 1)
                            found = True
                        except Exception:
                            try:
                                ws.update_cell(k + 1, M_IDX_ROUNDTRIP + 1, "Merged")
                                found = True
                            except Exception:
                                logger.exception("Failed to mark merged secondary row.")
                        break
            if not found:
                try:
                    ws.delete_rows(secondary_row_number)
                except Exception:
                    try:
                        ws.update_cell(secondary_row_number, M_IDX_ROUNDTRIP + 1, "Merged")
                    except Exception:
                        logger.exception("Failed to cleanup secondary row")
        except Exception:
            logger.exception("Failed to clean up secondary row after merge.")

        # compute mission days & per diem (use primary start and return_end)
        try:
            primary_vals_after, _ = _missions_get_values_and_data_rows(ws)
            p_row = _ensure_row_length(primary_vals_after[primary_idx], M_MANDATORY_COLS) if primary_idx < len(primary_vals_after) else primary_row
            p_start_s = str(p_row[M_IDX_START]).strip()
            p_end_s = str(p_row[M_IDX_RETURN_END] or p_row[M_IDX_END]).strip()
            ps = parse_ts(p_start_s)
            pe = parse_ts(p_end_s)
            days = mission_days_for_per_diem(ps, pe) if ps and pe else 0
            per_d = per_diem_amount_for_mission_days(days)
            # write summary row to MISSIONS_REPORT_TAB
            try:
                rpt_ws = open_worksheet(MISSIONS_REPORT_TAB)
                summary_row = [str(uuid.uuid4()), "", driver, plate, p_start_s, p_end_s, p_row[M_IDX_DEPART], p_row[M_IDX_ARRIVAL], p_row[M_IDX_STAFF], "Yes", p_row[M_IDX_RETURN_START], p_row[M_IDX_RETURN_END], days, per_d]
                rpt_ws.append_row(summary_row, value_input_option="USER_ENTERED")
            except Exception:
                logger.exception("Failed to append mission summary row to report tab.")
        except Exception:
            logger.exception("Failed to compute mission days or write mission summary.")

        return {"ok": True, "message": f"Mission end recorded and merged for {plate} at {end_ts}", "merged": True, "driver": driver, "plate": plate}
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

# Roundtrip count per driver month (missions)
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

# Trip count helpers (driver)
def count_trips_for_day(driver: str, date_dt: datetime) -> int:
    cnt = 0
    try:
        ws = open_worksheet(RECORDS_TAB)
        vals = ws.get_all_values()
        if not vals:
            return 0
        start_idx = 1 if any("date" in c.lower() for c in vals[0] if c) else 0
        for r in vals[start_idx:]:
            if len(r) < COL_START:
                continue
            dr = r[1] if len(r) > 1 else ""
            start_ts = r[3] if len(r) > 3 else ""
            end_ts = r[4] if len(r) > 4 else ""
            if dr != driver:
                continue
            if not start_ts or not end_ts:
                continue
            s_dt = parse_ts(start_ts)
            if not s_dt:
                continue
            if s_dt.date() == date_dt.date():
                cnt += 1
    except Exception:
        logger.exception("Failed to count trips for day")
    return cnt

def count_trips_for_month(driver: str, month_start: datetime, month_end: datetime) -> int:
    cnt = 0
    try:
        ws = open_worksheet(RECORDS_TAB)
        vals = ws.get_all_values()
        if not vals:
            return 0
        start_idx = 1 if any("date" in c.lower() for c in vals[0] if c) else 0
        for r in vals[start_idx:]:
            if len(r) < COL_START:
                continue
            dr = r[1] if len(r) > 1 else ""
            start_ts = r[3] if len(r) > 3 else ""
            end_ts = r[4] if len(r) > 4 else ""
            if dr != driver:
                continue
            if not start_ts or not end_ts:
                continue
            s_dt = parse_ts(start_ts)
            if not s_dt:
                continue
            if month_start <= s_dt < month_end:
                cnt += 1
    except Exception:
        logger.exception("Failed to count trips for month")
    return cnt

# Trip count helpers (plate)
def count_trips_for_day_plate(plate: str, date_dt: datetime) -> int:
    cnt = 0
    try:
        ws = open_worksheet(RECORDS_TAB)
        vals = ws.get_all_values()
        if not vals:
            return 0
        start_idx = 1 if any("date" in c.lower() for c in vals[0] if c) else 0
        for r in vals[start_idx:]:
            pl = r[2] if len(r) > 2 else ""
            start_ts = r[3] if len(r) > 3 else ""
            end_ts = r[4] if len(r) > 4 else ""
            if pl != plate:
                continue
            if not start_ts or not end_ts:
                continue
            s_dt = parse_ts(start_ts)
            if not s_dt:
                continue
            if s_dt.date() == date_dt.date():
                cnt += 1
    except Exception:
        logger.exception("Failed to count trips for plate day")
    return cnt

def count_trips_for_month_plate(plate: str, month_start: datetime, month_end: datetime) -> int:
    cnt = 0
    try:
        ws = open_worksheet(RECORDS_TAB)
        vals = ws.get_all_values()
        if not vals:
            return 0
        start_idx = 1 if any("date" in c.lower() for c in vals[0] if c) else 0
        for r in vals[start_idx:]:
            pl = r[2] if len(r) > 2 else ""
            start_ts = r[3] if len(r) > 3 else ""
            end_ts = r[4] if len(r) > 4 else ""
            if pl != plate:
                continue
            if not start_ts or not end_ts:
                continue
            s_dt = parse_ts(start_ts)
            if not s_dt:
                continue
            if month_start <= s_dt < month_end:
                cnt += 1
    except Exception:
        logger.exception("Failed to count trips for plate month")
    return cnt

# Finance handling (regex and helpers)
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
        ws = open_worksheet(EXPENSE_TAB)
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

def record_finance_combined_odo_fuel(plate: str, mileage: str, fuel_cost: str, by_user: str = "", invoice: str = "", driver_paid: str = "") -> dict:
    """Write a single row containing Mileage, Delta KM, Fuel Cost, Invoice, DriverPaid into EXPENSE_TAB"""
    try:
        ws = open_worksheet(EXPENSE_TAB)
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
        row = [plate, by_user or "Unknown", dt, str(m_int) if m_int is not None else str(mileage), delta, str(fuel_cost), "", "", invoice or "", driver_paid or ""]
        ws.append_row(row, value_input_option="USER_ENTERED")
        logger.info("Recorded combined ODO+Fuel: plate=%s mileage=%s delta=%s fuel=%s invoice=%s paid=%s", plate, m_int, delta, fuel_cost, invoice, driver_paid)
        return {"ok": True, "delta": delta, "mileage": m_int, "fuel": fuel_cost}
    except Exception as e:
        logger.exception("Failed to append combined odo+fuel row: %s", e)
        return {"ok": False, "message": str(e)}

def record_finance_entry_single_row(typ: str, plate: str, amount: str, notes: str, by_user: str = "") -> dict:
    """Record parking/wash/repair into EXPENSE_TAB or MAINT_TAB accordingly"""
    try:
        ntyp = normalize_fin_type(typ) or typ
        plate = str(plate).strip()
        notes = str(notes).strip()
        by_user = str(by_user).strip()
        if ntyp in {"parking", "wash"}:
            ws = open_worksheet(EXPENSE_TAB)
            dt = now_str()
            mileage = ""
            fuel_cost = ""
            parking_fee = ""
            other_fee = ""
            if ntyp == "parking":
                parking_fee = str(amount)
            else:
                other_fee = str(amount)
                if notes:
                    notes = f"{ntyp}: {notes}"
                else:
                    notes = ntyp
            row = [plate, by_user or "Unknown", dt, mileage, "", "", parking_fee, other_fee, notes, ""]
            ws.append_row(row, value_input_option="USER_ENTERED")
            logger.info("Recorded expense entry: %s %s %s", ntyp, plate, amount)
            return {"ok": True}
        if ntyp in {"repair", "maint"}:
            ws = open_worksheet(MAINT_TAB)
            mileage = ""
            item = "Repair"
            cost = str(amount)
            date = now_str().split(" ")[0]
            workshop = ""
            notes_field = notes or ""
            row = [plate, mileage, item, cost, date, workshop, notes_field]
            ws.append_row(row, value_input_option="USER_ENTERED")
            logger.info("Recorded maintenance entry: plate=%s cost=%s by=%s", plate, cost, by_user)
            return {"ok": True}
        # fallback: generic
        ws = open_worksheet(EXPENSE_TAB)
        dt = now_str()
        row = [plate, by_user or "Unknown", dt, "", "", "", "", "", str(amount), ""]
        ws.append_row(row, value_input_option="USER_ENTERED")
        logger.info("Recorded generic finance entry for unknown type '%s': %s", typ, plate)
        return {"ok": True}
    except Exception as e:
        logger.exception("Failed to record finance entry: %s", e)
        return {"ok": False, "message": str(e)}

# UI helpers & handlers
BOT_ADMINS = set([u.strip() for u in os.getenv("BOT_ADMINS", BOT_ADMINS_DEFAULT).split(",") if u.strip()])
BOT_ADMINS.add("markpeng1")

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

# helper to safe-delete a message (ignore errors)
async def safe_delete_message(bot, chat_id, message_id):
    try:
        await bot.delete_message(chat_id=chat_id, message_id=message_id)
    except Exception:
        pass

async def menu_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # delete user command
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
         InlineKeyboardButton("Leave", callback_data="leave_menu")],
    ]
    try:
        await context.bot.send_chat_action(chat_id=update.effective_chat.id, action=ChatAction.TYPING)
    except Exception:
        pass
    await update.effective_chat.send_message(text=text, reply_markup=InlineKeyboardMarkup(keyboard))

async def start_trip_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # delete user command
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
    # delete user command
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

async def mission_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await mission_start_command(update, context)

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

# leave command to create a ForceReply leave entry (bot prompt will be deleted after processing)
async def leave_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        if update.effective_message:
            await update.effective_message.delete()
    except Exception:
        pass
    prompt = t(context.user_data.get("lang", DEFAULT_LANG), "leave_prompt")
    fr = ForceReply(selective=False)
    sent = await update.effective_chat.send_message(prompt, reply_markup=fr)
    # store prompt info so we can delete it when finished
    context.user_data["pending_leave"] = {"prompt_chat": sent.chat_id, "prompt_msg_id": sent.message_id}

# Admin finance inline flow (updated to select plate then multi-step ForceReply)
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
        # ask plate selection (we will edit the callback message to the plate keyboard so we can later delete it)
        await query.edit_message_text("Choose plate:", reply_markup=build_plate_keyboard(f"fin_plate|{typ}"))
    except Exception:
        logger.exception("Failed to present plate selection for finance.")

# Process ForceReply replies: finance (multi-step), leave, mission staff entry (when 'enter staff' chosen)
async def process_force_reply(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    text = update.effective_message.text.strip() if update.effective_message and update.effective_message.text else ""
    if not text:
        return

    # Multi-step finance pending (odo+fuel)
    pending_multi = context.user_data.get("pending_fin_multi")
    if pending_multi:
        ptype = pending_multi.get("type")
        plate = pending_multi.get("plate")
        step = pending_multi.get("step")
        origin = pending_multi.get("origin")  # origin callback message info to delete later
        if ptype == "odo_fuel":
            if step == "km":
                # validate odometer
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
                        # clean up origin prompt if any
                        try:
                            if origin:
                                await safe_delete_message(context.bot, origin.get("chat"), origin.get("msg_id"))
                        except Exception:
                            pass
                        context.user_data.pop("pending_fin_multi", None)
                        return
                else:
                    km = m.group(1)
                pending_multi["km"] = km
                pending_multi["step"] = "fuel"
                context.user_data["pending_fin_multi"] = pending_multi
                # delete user's reply
                try:
                    await update.effective_message.delete()
                except Exception:
                    pass
                # prompt for fuel cost with ForceReply (we will delete this prompt after fuel input)
                fr = ForceReply(selective=False)
                try:
                    mmsg = await context.bot.send_message(chat_id=update.effective_chat.id, text=t(context.user_data.get("lang", DEFAULT_LANG), "enter_fuel_cost", plate=plate), reply_markup=fr)
                    pending_multi["prompt_chat"] = mmsg.chat_id
                    pending_multi["prompt_msg_id"] = mmsg.message_id
                    context.user_data["pending_fin_multi"] = pending_multi
                except Exception:
                    logger.exception("Failed to prompt for fuel cost.")
                    try:
                        if origin:
                            await safe_delete_message(context.bot, origin.get("chat"), origin.get("msg_id"))
                    except Exception:
                        pass
                    context.user_data.pop("pending_fin_multi", None)
                return
            elif step == "fuel":
                raw = text  # raw text may contain amount + inv + paid
                # parse invoice and paid tokens
                inv_m = INV_RE.search(raw)
                paid_m = PAID_RE.search(raw)
                invoice = inv_m.group(1) if inv_m else ""
                driver_paid = ""
                if paid_m:
                    v = paid_m.group(1).lower()
                    driver_paid = "yes" if v.startswith("y") else "no"
                # extract fuel numeric
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
                        # cleanup origin prompt
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
                # attempt to record combined row (compute delta inside)
                try:
                    res = record_finance_combined_odo_fuel(plate, km, fuel_amt, by_user=user.username or "", invoice=invoice, driver_paid=driver_paid)
                except Exception:
                    res = {"ok": False}
                # delete user's reply and the bot's prompt(s)
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
                # delete origin callback message if present
                try:
                    if origin:
                        await safe_delete_message(context.bot, origin.get("chat"), origin.get("msg_id"))
                except Exception:
                    pass
                # send group notification as required (short)
                try:
                    delta_txt = res.get("delta", "")
                    m_val = res.get("mileage", km)
                    fuel_val = res.get("fuel", fuel_amt)
                    nowd = _now_dt().strftime("%Y-%m-%d")
                    msg = t(context.user_data.get("lang", DEFAULT_LANG), "finance_odo_fuel_receipt",
                            plate=plate, odo=m_val, fuel=fuel_val, date=nowd, by=user.username or "Unknown", delta=delta_txt)
                    await update.effective_chat.send_message(msg)
                except Exception:
                    logger.exception("Failed to send group notification for odo+fuel")
                # privately DM operator with brief confirmation
                try:
                    await context.bot.send_message(chat_id=user.id, text=f"Recorded {plate}: {km}KM and ${fuel_amt} fuel. Delta {delta_txt} km. Invoice={invoice} Paid={driver_paid}")
                except Exception:
                    pass
                context.user_data.pop("pending_fin_multi", None)
                return

    # Simple finance pending (single-step flows: parking, repair, wash, fuel solo)
    pending_simple = context.user_data.get("pending_fin_simple")
    if pending_simple:
        typ = pending_simple.get("type")
        plate = pending_simple.get("plate")
        origin = pending_simple.get("origin")
        # expect amount in message; also allow inv/paid tags in same text
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
            res = record_finance_entry_single_row("odo", plate, km, "", by_user=user.username or "")
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
            # parse invoice and paid tokens also from raw
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
            res = record_finance_entry_single_row(typ, plate, amt, invoice or "", by_user=user.username or "")
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
                # group receipt message
                nowd = _now_dt().strftime("%Y-%m-%d")
                msg = t(context.user_data.get("lang", DEFAULT_LANG), "finance_expense_receipt",
                        plate=plate, typ=typ, amt=amt, date=nowd, by=user.username or "Unknown")
                await update.effective_chat.send_message(msg)
                # DM operator with brief confirmation
                await context.bot.send_message(chat_id=user.id, text=f"Recorded {typ} ${amt} for {plate}. Invoice={invoice} Paid={driver_paid}")
            except Exception:
                pass
            context.user_data.pop("pending_fin_simple", None)
            return

    # Leave pending
    pending_leave = context.user_data.get("pending_leave")
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
            # delete the leave prompt message
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
                await context.bot.send_message(chat_id=user.id, text="Invalid dates. Use YYYY-MM-DD.")
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
            row = [driver, start, end, reason, notes]
            ws.append_row(row, value_input_option="USER_ENTERED")
            # delete user's reply and the prompt
            try:
                await update.effective_message.delete()
            except Exception:
                pass
            try:
                await safe_delete_message(context.bot, pending_leave.get("prompt_chat"), pending_leave.get("prompt_msg_id"))
            except Exception:
                pass
            # DM confirmation (group silent) and group receipt
            try:
                await context.bot.send_message(chat_id=user.id, text=t(context.user_data.get("lang", DEFAULT_LANG), "leave_confirm", driver=driver, start=start, end=end, reason=reason))
            except Exception:
                pass
            # group summary: compute counts
            try:
                nowdt = _now_dt()
                # month range
                mstart = datetime(nowdt.year, nowdt.month, 1)
                if nowdt.month == 12:
                    mend = datetime(nowdt.year + 1, 1, 1)
                else:
                    mend = datetime(nowdt.year, nowdt.month + 1, 1)
                month_days = count_leave_days_for_driver(driver, mstart, mend)
                ystart = datetime(nowdt.year, 1, 1)
                yend = datetime(nowdt.year + 1, 1, 1)
                year_days = count_leave_days_for_driver(driver, ystart, yend)
                remaining = max(0, ANNUAL_LEAVE_DAYS - year_days)
                await update.effective_chat.send_message(t(context.user_data.get("lang", DEFAULT_LANG), "leave_summary_msg",
                                                          driver=driver, month=mstart.strftime("%Y-%m"), month_count=month_days,
                                                          year=nowdt.year, year_count=year_days, remaining=remaining))
            except Exception:
                logger.exception("Failed to send leave summary message.")
        except Exception:
            logger.exception("Failed to record leave")
            try:
                await context.bot.send_message(chat_id=user.id, text="Failed to record leave (sheet error).")
            except Exception:
                pass
        context.user_data.pop("pending_leave", None)
        return

    # mission staff entry pending (we store pending_mission with "need_staff": "enter")
    pending_mission = context.user_data.get("pending_mission")
    if pending_mission and pending_mission.get("need_staff") == "enter":
        # We removed staff-enter requirement per your request; if this path triggers, we'll treat text as staff for backward compatibility.
        staff = text
        plate = pending_mission.get("plate")
        departure = pending_mission.get("departure")
        username = user.username or user.full_name
        driver_map = get_driver_map()
        allowed = driver_map.get(user.username, []) if user and user.username else []
        if allowed and plate not in allowed:
            await update.effective_chat.send_message(t(context.user_data.get("lang", DEFAULT_LANG), "not_allowed", plate=plate))
            context.user_data.pop("pending_mission", None)
            return
        res = start_mission_record(username, plate, departure, staff_name=staff)
        try:
            await update.effective_message.delete()
        except Exception:
            pass
        if res.get("ok"):
            try:
                await update.effective_chat.send_message(t(context.user_data.get("lang", DEFAULT_LANG), "mission_departure", plate=plate, driver=username, dep=departure, ts=now_str()))
            except Exception:
                pass
        else:
            try:
                await update.effective_chat.send_message("❌ " + res.get("message", ""))
            except Exception:
                pass
        context.user_data.pop("pending_mission", None)
        return

# fallback free-text handler
async def location_or_staff(update: Update, context: ContextTypes.DEFAULT_TYPE):
    return await process_force_reply(update, context)

# Plate callback with finance & mission flows; ensures deletion of prompts/origin messages
async def plate_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    data = query.data
    user = query.from_user
    username = user.username or f"{user.first_name or ''} {user.last_name or ''}".strip()
    user_lang = context.user_data.get("lang", DEFAULT_LANG)

    # navigation
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

    # admin finance menu
    if data == "admin_finance":
        if (query.from_user.username or "") not in BOT_ADMINS:
            await query.edit_message_text("❌ Admins only.")
            return
        return await admin_finance_callback_handler(update, context)
    if data.startswith("fin_type|"):
        return await admin_fin_type_selected(update, context)

    # handle plate selection for finance flows: fin_plate|{typ}|{plate}
    if data.startswith("fin_plate|"):
        parts = data.split("|", 2)
        if len(parts) < 3:
            await query.edit_message_text("Invalid selection.")
            return
        _, typ, plate = parts
        if (query.from_user.username or "") not in BOT_ADMINS:
            await query.edit_message_text("❌ Admins only.")
            return
        origin_info = {"chat": query.message.chat.id, "msg_id": query.message.message_id, "typ": typ}
        # For combined odo+fuel, start multi-step
        if typ == "odo_fuel":
            context.user_data["pending_fin_multi"] = {"type": "odo_fuel", "plate": plate, "step": "km", "origin": origin_info}
            fr = ForceReply(selective=False)
            try:
                # edit to ask KM and send ForceReply prompt; store prompt id so we can delete later
                await query.edit_message_text(t(context.user_data.get("lang", DEFAULT_LANG), "enter_odo_km", plate=plate))
                mmsg = await context.bot.send_message(chat_id=query.message.chat.id, text=t(context.user_data.get("lang", DEFAULT_LANG), "enter_odo_km", plate=plate), reply_markup=fr)
                context.user_data["pending_fin_multi"]["prompt_chat"] = mmsg.chat_id
                context.user_data["pending_fin_multi"]["prompt_msg_id"] = mmsg.message_id
            except Exception:
                logger.exception("Failed to prompt for odo km.")
                context.user_data.pop("pending_fin_multi", None)
            return
        # other types -> ask for amount (single-step)
        if typ in ("parking", "wash", "repair", "fuel"):
            # store origin and prompt info to delete later
            context.user_data["pending_fin_simple"] = {"type": typ, "plate": plate, "origin": origin_info}
            fr = ForceReply(selective=False)
            try:
                await query.edit_message_text(t(context.user_data.get("lang", DEFAULT_LANG), "enter_amount_for", typ=typ, plate=plate))
                mmsg = await context.bot.send_message(chat_id=query.message.chat.id, text=t(context.user_data.get("lang", DEFAULT_LANG), "enter_amount_for", typ=typ, plate=plate), reply_markup=fr)
                context.user_data["pending_fin_simple"]["prompt_chat"] = mmsg.chat_id
                context.user_data["pending_fin_simple"]["prompt_msg_id"] = mmsg.message_id
            except Exception:
                logger.exception("Failed to prompt for amount.")
                context.user_data.pop("pending_fin_simple", None)
            return

    # leave menu quick
    if data == "leave_menu":
        fr = ForceReply(selective=False)
        try:
            await query.edit_message_text(t(context.user_data.get("lang", DEFAULT_LANG), "leave_prompt"))
            m = await context.bot.send_message(chat_id=query.message.chat.id, text=t(context.user_data.get("lang", DEFAULT_LANG), "leave_prompt"), reply_markup=fr)
            context.user_data["pending_leave"] = {"prompt_chat": m.chat_id, "prompt_msg_id": m.message_id}
        except Exception:
            logger.exception("Failed to prompt leave.")
        return

    # mission start choose plate
    if data.startswith("mission_start_plate|"):
        _, plate = data.split("|", 1)
        context.user_data["pending_mission"] = {"action": "start", "plate": plate}
        kb = [[InlineKeyboardButton("No staff", callback_data=f"mission_staff|none|{plate}"), InlineKeyboardButton("Enter staff", callback_data=f"mission_staff|enter|{plate}")]]
        await query.edit_message_text(t(user_lang, "mission_start_prompt_staff"), reply_markup=InlineKeyboardMarkup(kb))
        return

    # mission end choose plate
    if data.startswith("mission_end_plate|"):
        _, plate = data.split("|", 1)
        context.user_data["pending_mission"] = {"action": "end", "plate": plate}
        kb = [[InlineKeyboardButton("PP", callback_data=f"mission_arrival|PP|{plate}"), InlineKeyboardButton("SHV", callback_data=f"mission_arrival|SHV|{plate}")]]
        await query.edit_message_text(t(user_lang, "mission_end_prompt_arrival"), reply_markup=InlineKeyboardMarkup(kb))
        return

    # mission staff choices (we keep options but staff entry is not required)
    if data.startswith("mission_staff|"):
        parts = data.split("|")
        if len(parts) < 3:
            await query.edit_message_text("Invalid selection.")
            return
        _, choice, plate = parts
        pending = context.user_data.get("pending_mission") or {}
        pending["plate"] = plate
        dep_kb = [[InlineKeyboardButton("PP", callback_data=f"mission_depart|PP|{plate}"), InlineKeyboardButton("SHV", callback_data=f"mission_depart|SHV|{plate}")]]
        context.user_data["pending_mission"] = pending
        await query.edit_message_text(t(user_lang, "mission_start_prompt_depart"), reply_markup=InlineKeyboardMarkup(dep_kb))
        return

    # mission depart after choosing staff option
    if data.startswith("mission_depart|"):
        parts = data.split("|")
        if len(parts) < 3:
            await query.edit_message_text("Invalid selection.")
            return
        _, dep, plate = parts
        pending = context.user_data.get("pending_mission") or {}
        pending["departure"] = dep
        pending["plate"] = plate
        context.user_data["pending_mission"] = pending
        kb = [[InlineKeyboardButton("Start now", callback_data=f"mission_start_now|{plate}|{dep}")]]
        await query.edit_message_text("Choose start option:", reply_markup=InlineKeyboardMarkup(kb))
        return

    # mission start now (no staff)
    if data.startswith("mission_start_now|"):
        _, plate, dep = data.split("|", 2)
        username = query.from_user.username or query.from_user.full_name
        res = start_mission_record(username, plate, dep, staff_name="")
        if res.get("ok"):
            # only report departure message
            await query.edit_message_text(t(user_lang, "mission_departure", plate=plate, driver=username, dep=dep, ts=now_str()))
        else:
            await query.edit_message_text("❌ " + res.get("message", ""))
        context.user_data.pop("pending_mission", None)
        return

    # mission arrival for end flow (user selected arrival city)
    if data.startswith("mission_arrival|"):
        parts = data.split("|")
        if len(parts) < 3:
            await query.edit_message_text("Invalid selection.")
            return
        _, arr, plate = parts
        pending = context.user_data.get("pending_mission") or {}
        pending["arrival"] = arr
        pending["plate"] = plate
        context.user_data["pending_mission"] = pending
        driver_map = get_driver_map()
        allowed = driver_map.get(username, []) if username else []
        if allowed and plate not in allowed:
            await query.edit_message_text(t(user_lang, "not_allowed", plate=plate))
            context.user_data.pop("pending_mission", None)
            return
        res = end_mission_record(username, plate, arr)
        if res.get("ok"):
            # arrival message
            await query.edit_message_text(t(user_lang, "mission_arrival", plate=plate, driver=username, arr=arr, ts=now_str()))
            if res.get("merged"):
                try:
                    nowdt = _now_dt()
                    month_start = datetime(nowdt.year, nowdt.month, 1)
                    if nowdt.month == 12:
                        month_end = datetime(nowdt.year + 1, 1, 1)
                    else:
                        month_end = datetime(nowdt.year, nowdt.month + 1, 1)
                    counts = count_roundtrips_per_driver_month(month_start, month_end)
                    cnt_month = counts.get(username, 0)
                    # year count
                    ystart = datetime(nowdt.year, 1, 1)
                    yend = datetime(nowdt.year + 1, 1, 1)
                    counts_year = count_roundtrips_per_driver_month(ystart, yend)
                    cnt_year = counts_year.get(username, 0)
                    # compute mission days & per-diem for last merged mission: we attempt to read the last REPORT row
                    days = 0
                    per_d = 0.0
                    try:
                        rpt_ws = open_worksheet(MISSIONS_REPORT_TAB)
                        vals = rpt_ws.get_all_values()
                        if vals and len(vals) >= 2:
                            last = vals[-1]
                            # if last row matches driver+plate, attempt parse days/per_d
                            if len(last) >= 13 and str(last[2]).strip() == username and str(last[3]).strip() == plate:
                                try:
                                    days = int(last[12])
                                    per_d = float(last[13]) if len(last) > 13 else 0.0
                                except Exception:
                                    days = 0
                                    per_d = 0.0
                    except Exception:
                        pass
                    await query.message.chat.send_message(t(user_lang, "mission_completed_notify",
                                                            driver=username, month=month_start.strftime("%Y-%m"), month_count=cnt_month,
                                                            year=nowdt.year, year_count=cnt_year, days=days, per_diem=per_d))
                except Exception:
                    logger.exception("Failed to send merged missions message.")
        else:
            await query.edit_message_text("❌ " + res.get("message", ""))
        context.user_data.pop("pending_mission", None)
        return

    # start|plate quick action (start trip)
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
                try:
                    await query.edit_message_text(t(user_lang, "trip_start_msg", driver=username, plate=plate, ts=res.get("ts")))
                except Exception:
                    try:
                        await query.message.chat.send_message(t(user_lang, "trip_start_msg", driver=username, plate=plate, ts=res.get("ts")))
                        await safe_delete_message(context.bot, query.message.chat.id, query.message.message_id)
                    except Exception:
                        pass
            else:
                try:
                    await query.edit_message_text("❌ " + res.get("message", ""))
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
                n_year = count_trips_for_month(username, datetime(nowdt.year, 1, 1), datetime(nowdt.year + 1, 1, 1))
                # plate counts
                p_today = count_trips_for_day_plate(plate, nowdt)
                p_month = count_trips_for_month_plate(plate, month_start, month_end)
                p_year = count_trips_for_month_plate(plate, datetime(nowdt.year, 1, 1), datetime(nowdt.year + 1, 1, 1))
                try:
                    await query.edit_message_text(t(user_lang, "trip_end_msg", driver=username, plate=plate, ts=ts))
                except Exception:
                    try:
                        await query.message.chat.send_message(t(user_lang, "trip_end_msg", driver=username, plate=plate, ts=ts))
                        await safe_delete_message(context.bot, query.message.chat.id, query.message.message_id)
                    except Exception:
                        pass
                # send summary
                try:
                    await query.message.chat.send_message(t(user_lang, "trip_summary_notify",
                                                            driver=username, n_today=n_today, n_month=n_month, n_year=n_year,
                                                            month=month_start.strftime("%Y-%m"),
                                                            plate=plate, p_today=p_today, p_month=p_month, p_year=p_year))
                except Exception:
                    logger.exception("Failed to send trip summary.")
            else:
                try:
                    await query.edit_message_text("❌ " + res.get("message", ""))
                except Exception:
                    pass
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
            tab_name = None
            try:
                tab_name = None
            except Exception:
                tab_name = None
            if ok:
                await update.effective_chat.send_message(f"Monthly mission report for {start.strftime('%Y-%m')} created.")
            else:
                await update.effective_chat.send_message("❌ Failed to write mission report.")
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
            # delete user typed bare slash commands to reduce clutter
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

# scheduled daily summary & auto-monthly mission reporting
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
            ok = write_mission_report_rows(rows, period_label=prev_month_start.strftime("%Y-%m"))
            counts = count_roundtrips_per_driver_month(prev_month_start, prev_month_end)
            tab_name = None
            if ok:
                await context.bot.send_message(chat_id=chat_id, text=f"Auto-generated mission report for {prev_month_start.strftime('%Y-%m')}.")
        except Exception:
            logger.exception("Failed to auto-generate monthly mission report on day 1.")

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

# setup_menu command: post & pin main menu in group
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
        try:
            await context.bot.pin_chat_message(chat_id=update.effective_chat.id, message_id=sent.message_id)
            logger.info("Pinned menu message in chat %s", update.effective_chat.id)
        except Exception:
            logger.exception("Could not pin menu message.")
    except Exception:
        logger.exception("Failed to setup menu.")

# Handler to delete command messages (run AFTER command handlers)
async def delete_command_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        if update.effective_message:
            await update.effective_message.delete()
    except Exception:
        pass

# Leave statistics helpers
def leave_days_between(start_date: datetime, end_date: datetime) -> int:
    sd = start_date.date()
    ed = end_date.date()
    return (ed - sd).days + 1 if ed >= sd else 0

def count_leave_days_for_driver(driver: str, period_start: datetime, period_end: datetime) -> int:
    try:
        ws = open_worksheet(LEAVE_TAB)
        rows = ws.get_all_records()
        total = 0
        for r in rows:
            d = str(r.get("Driver", r.get("driver", ""))).strip()
            if d != driver:
                continue
            s = str(r.get("Start Date", r.get("start", r.get("Start", "")))).strip()
            e = str(r.get("End Date", r.get("end", r.get("End", "")))).strip()
            try:
                sd = datetime.strptime(s, "%Y-%m-%d")
                ed = datetime.strptime(e, "%Y-%m-%d")
            except Exception:
                continue
            overlap_start = max(sd, period_start)
            overlap_end = min(ed, period_end - timedelta(seconds=1))
            if overlap_end >= overlap_start:
                total += leave_days_between(overlap_start, overlap_end)
        return total
    except Exception:
        logger.exception("Failed to count leave days")
        return 0

async def leave_balance_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        if update.effective_message:
            await update.effective_message.delete()
    except Exception:
        pass
    args = context.args
    if not args:
        await update.effective_chat.send_message("Usage: /leave_balance <driver> [YYYY-MM|YYYY]")
        return
    driver = args[0]
    nowdt = _now_dt()
    if len(args) >= 2:
        period = args[1]
        try:
            if re.match(r'^\d{4}-\d{2}$', period):
                dt = datetime.strptime(period + "-01", "%Y-%m-%d")
                start = datetime(dt.year, dt.month, 1)
                if dt.month == 12:
                    end = datetime(dt.year + 1, 1, 1)
                else:
                    end = datetime(dt.year, dt.month + 1, 1)
                month_days = count_leave_days_for_driver(driver, start, end)
                ystart = datetime(dt.year, 1, 1)
                yend = datetime(dt.year + 1, 1, 1)
                year_days = count_leave_days_for_driver(driver, ystart, yend)
                remaining = max(0, ANNUAL_LEAVE_DAYS - count_leave_days_for_driver(driver, ystart, yend))
                await update.effective_chat.send_message(
                    t(context.user_data.get("lang", DEFAULT_LANG), "leave_summary_msg",
                      driver=driver, month=period, month_count=month_days, year=dt.year, year_count=year_days, remaining=remaining)
                )
                return
            elif re.match(r'^\d{4}$', period):
                yr = int(period)
                ystart = datetime(yr, 1, 1)
                yend = datetime(yr + 1, 1, 1)
                year_days = count_leave_days_for_driver(driver, ystart, yend)
                remaining = max(0, ANNUAL_LEAVE_DAYS - year_days)
                await update.effective_chat.send_message(
                    t(context.user_data.get("lang", DEFAULT_LANG), "leave_summary_msg",
                      driver=driver, month=period, month_count=0, year=yr, year_count=year_days, remaining=remaining)
                )
                return
        except Exception:
            pass
    # default: year-to-date
    ystart = datetime(nowdt.year, 1, 1)
    yend = datetime(nowdt.year + 1, 1, 1)
    year_days = count_leave_days_for_driver(driver, ystart, yend)
    remaining = max(0, ANNUAL_LEAVE_DAYS - year_days)
    month_label = nowdt.strftime("%Y-%m")
    month_start = datetime(nowdt.year, nowdt.month, 1)
    if nowdt.month == 12:
        month_end = datetime(nowdt.year + 1, 1, 1)
    else:
        month_end = datetime(nowdt.year, nowdt.month + 1, 1)
    month_days = count_leave_days_for_driver(driver, month_start, month_end)
    await update.effective_chat.send_message(
        t(context.user_data.get("lang", DEFAULT_LANG), "leave_summary_msg",
          driver=driver, month=month_label, month_count=month_days, year=nowdt.year, year_count=year_days, remaining=remaining)
    )

# Register handlers
def register_ui_handlers(application):
    application.add_handler(CommandHandler("menu", menu_command))
    application.add_handler(CommandHandler(["start_trip", "start"], start_trip_command))
    application.add_handler(CommandHandler(["end_trip", "end"], end_trip_command))
    application.add_handler(CommandHandler("mission", mission_command))
    application.add_handler(CommandHandler("mission_start", mission_start_command))
    application.add_handler(CommandHandler("mission_end", mission_end_command))
    application.add_handler(CommandHandler("mission_report", mission_report_command))
    application.add_handler(CommandHandler("leave", leave_command))
    application.add_handler(CommandHandler("leave_balance", leave_balance_command))
    application.add_handler(CommandHandler("setup_menu", setup_menu_command))
    application.add_handler(CommandHandler("lang", lang_command))

    application.add_handler(CallbackQueryHandler(plate_callback))

    # ForceReply responses for finance, leave, mission staff
    application.add_handler(MessageHandler(filters.REPLY & filters.TEXT & (~filters.COMMAND), process_force_reply))
    # fallback text handler (used to route some free text entries)
    application.add_handler(MessageHandler(filters.TEXT & (~filters.COMMAND), location_or_staff))

    application.add_handler(MessageHandler(filters.Regex(AUTO_KEYWORD_PATTERN) & filters.ChatType.GROUPS, auto_menu_listener))

    # delete bare slash commands (after handlers)
    application.add_handler(MessageHandler(filters.COMMAND, delete_command_message), group=1)

    application.add_handler(CommandHandler("help", lambda u, c: u.message.reply_text(t(c.user_data.get("lang", DEFAULT_LANG), "help"))))

    try:
        async def _set_cmds():
            try:
                await application.bot.set_my_commands([
                    BotCommand("start_trip", "Start a trip (select plate)"),
                    BotCommand("end_trip", "End a trip (select plate)"),
                    BotCommand("menu", "Open trip menu"),
                    BotCommand("mission", "Quick mission menu"),
                    BotCommand("mission_report", "Generate mission report: /mission_report month YYYY-MM"),
                    BotCommand("leave", "Record leave (admin)"),
                    BotCommand("leave_balance", "Query leave balance / summary"),
                    BotCommand("setup_menu", "Post and pin the main menu (admins only)"),
                ])
            except Exception:
                logger.exception("Failed to set bot commands.")
        # application.create_task is available; use it non-blocking
        if hasattr(application, "create_task"):
            application.create_task(_set_cmds())
    except Exception:
        logger.debug("Could not schedule set_my_commands.")

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
    """
    Attempt to delete any pre-existing webhook for this bot token via direct HTTP call.
    Returns True on success (or if webhook was not set), False on error.
    """
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
