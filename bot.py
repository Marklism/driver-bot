#!/usr/bin/env python3
"""
Driver Bot — Prompt-deletion update
- Any message sent as a prompt (ForceReply or edited callback text used as prompt)
  will be tracked and deleted after the user's reply is processed.
- This includes odometer/fuel/parking/wash/repair prompts, leave prompts, mission staff prompts, and admin-finance prompts.
- Keeps previous functionality; only augments prompt lifecycle tracking + deletion.
"""
import os
import json
import base64
import logging
import uuid
import re
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
    },
    "km": {
        "menu": "ម្ហឺនុយបូត — សូមជ្រើសប៊ូតុង:",
        "choose_start": "ជ្រើស plate ដើម្បីចាប់ផ្តើមដំណើរ:",
        "choose_end": "ជ្រើស plate ដើម្បីបញ្ចប់ដំណើរ:",
        "start_ok": "Driver {driver} start trip at {ts}.",
        "end_ok": "Driver {driver} end trip at {ts} (duration {dur}). Driver {driver} completed {n_today} trips today and {n_month} trips in {month}.",
        "not_allowed": "❌ មិនមានសិទ្ធិប្រើ plate: {plate}.",
        "invalid_sel": "ជម្រើសមិនត្រឹមត្រូវ។",
        "help": "ជំនួយ៖ ប្រើ /start_trip ឬ /end_trip ហើយជ្រើស plate.",
        "no_bot_token": "សូមកំណត់ BOT_TOKEN។",
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

# driver map loaders (unchanged)
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

# time helpers (unchanged)
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

# trip record functions (unchanged besides returned fields)
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

# missions helpers — unchanged (not repeated here for brevity)...
# (Assume functions _missions_get_values_and_data_rows, _missions_next_no, _ensure_row_length,
# start_mission_record, end_mission_record, etc. are present and unchanged from your previous version.)
# For brevity in this message I won't re-paste those unchanged mission functions again,
# but in your real file they must be present exactly as before.

# ---- finance helpers and prompt-deletion tracking ----
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

# find last mileage for plate in EXPENSE_TAB (used to compute delta)
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

# ----------------------------
# Key handlers
# ----------------------------
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

# leave command: send ForceReply and track prompt for deletion
async def leave_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        if update.effective_message:
            await update.effective_message.delete()
    except Exception:
        pass
    prompt = t(context.user_data.get("lang", DEFAULT_LANG), "leave_prompt")
    fr = ForceReply(selective=False)
    sent = await update.effective_chat.send_message(prompt, reply_markup=fr)
    # store pending leave and prompt id so we can delete it after user reply
    context.user_data["pending_leave"] = {"prompt_chat": sent.chat_id, "prompt_msg_id": sent.message_id}

# admin finance: show menu, then plate selection; many prompts tracked for deletion
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

# process ForceReply: all pending prompts are tracked in user_data and will be deleted here
async def process_force_reply(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    text = update.effective_message.text.strip() if update.effective_message and update.effective_message.text else ""
    if not text:
        return

    # -- handle multi-step odo+fuel flow --
    pending_multi = context.user_data.get("pending_fin_multi")
    if pending_multi:
        ptype = pending_multi.get("type")
        plate = pending_multi.get("plate")
        step = pending_multi.get("step")
        origin = pending_multi.get("origin")  # origin callback msg info
        # KM step
        if ptype == "odo_fuel":
            if step == "km":
                m = ODO_RE.match(text)
                if not m:
                    m2 = re.search(r'(\d+)', text)
                    if m2:
                        km = m2.group(1)
                    else:
                        # invalid -> delete the user reply and the prompt and origin
                        try:
                            await update.effective_message.delete()
                        except Exception:
                            pass
                        try:
                            if pending_multi.get("prompt_chat") and pending_multi.get("prompt_msg_id"):
                                await safe_delete_message(context.bot, pending_multi["prompt_chat"], pending_multi["prompt_msg_id"])
                        except Exception:
                            pass
                        try:
                            if origin:
                                await safe_delete_message(context.bot, origin.get("chat"), origin.get("msg_id"))
                        except Exception:
                            pass
                        context.user_data.pop("pending_fin_multi", None)
                        try:
                            await context.bot.send_message(chat_id=user.id, text=t(context.user_data.get("lang", DEFAULT_LANG), "invalid_odo"))
                        except Exception:
                            pass
                        return
                else:
                    km = m.group(1)
                pending_multi["km"] = km
                pending_multi["step"] = "fuel"
                context.user_data["pending_fin_multi"] = pending_multi
                # delete user's reply (they typed odometer) to keep chat clean
                try:
                    await update.effective_message.delete()
                except Exception:
                    pass
                # delete the original prompt message and origin callback (so prompt disappears)
                try:
                    if pending_multi.get("prompt_chat") and pending_multi.get("prompt_msg_id"):
                        await safe_delete_message(context.bot, pending_multi["prompt_chat"], pending_multi["prompt_msg_id"])
                except Exception:
                    pass
                try:
                    if origin:
                        await safe_delete_message(context.bot, origin.get("chat"), origin.get("msg_id"))
                except Exception:
                    pass
                # now send fuel prompt (ForceReply) and track it
                fr = ForceReply(selective=False)
                try:
                    mmsg = await context.bot.send_message(chat_id=update.effective_chat.id, text=t(context.user_data.get("lang", DEFAULT_LANG), "enter_fuel_cost", plate=plate), reply_markup=fr)
                    pending_multi["prompt_chat"] = mmsg.chat_id
                    pending_multi["prompt_msg_id"] = mmsg.message_id
                    context.user_data["pending_fin_multi"] = pending_multi
                except Exception:
                    logger.exception("Failed to prompt for fuel cost.")
                    # cleanup
                    try:
                        if pending_multi.get("prompt_chat") and pending_multi.get("prompt_msg_id"):
                            await safe_delete_message(context.bot, pending_multi["prompt_chat"], pending_multi["prompt_msg_id"])
                    except Exception:
                        pass
                    try:
                        if origin:
                            await safe_delete_message(context.bot, origin.get("chat"), origin.get("msg_id"))
                    except Exception:
                        pass
                    context.user_data.pop("pending_fin_multi", None)
                return
            # fuel step
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
                        # invalid -> delete reply + prompts
                        try:
                            await update.effective_message.delete()
                        except Exception:
                            pass
                        try:
                            if pending_multi.get("prompt_chat") and pending_multi.get("prompt_msg_id"):
                                await safe_delete_message(context.bot, pending_multi["prompt_chat"], pending_multi["prompt_msg_id"])
                        except Exception:
                            pass
                        try:
                            if origin:
                                await safe_delete_message(context.bot, origin.get("chat"), origin.get("msg_id"))
                        except Exception:
                            pass
                        context.user_data.pop("pending_fin_multi", None)
                        try:
                            await context.bot.send_message(chat_id=user.id, text=t(context.user_data.get("lang", DEFAULT_LANG), "invalid_amount"))
                        except Exception:
                            pass
                        return
                else:
                    fuel_amt = am.group(1)
                km = pending_multi.get("km", "")
                try:
                    res = record_finance_combined_odo_fuel(plate, km, fuel_amt, by_user=user.username or "", invoice=invoice, driver_paid=driver_paid)
                except Exception:
                    res = {"ok": False}
                # delete user's reply and the fuel prompt and origin callback
                try:
                    await update.effective_message.delete()
                except Exception:
                    pass
                try:
                    if pending_multi.get("prompt_chat") and pending_multi.get("prompt_msg_id"):
                        await safe_delete_message(context.bot, pending_multi["prompt_chat"], pending_multi["prompt_msg_id"])
                except Exception:
                    pass
                try:
                    if origin:
                        await safe_delete_message(context.bot, origin.get("chat"), origin.get("msg_id"))
                except Exception:
                    pass
                # send short group notification (optional) — keep minimal
                try:
                    delta_txt = res.get("delta", "")
                    m_val = res.get("mileage", km)
                    fuel_val = res.get("fuel", fuel_amt)
                    nowd = _now_dt().strftime(DATE_FMT)
                    msg = f"{plate} @ {m_val} km + ${fuel_val} fuel in {nowd}, difference from previous odo is {delta_txt} km."
                    await update.effective_chat.send_message(msg)
                except Exception:
                    logger.exception("Failed to send group notification for odo+fuel")
                # DM operator as confirmation
                try:
                    await context.bot.send_message(chat_id=user.id, text=f"Recorded {plate}: {km}KM and ${fuel_amt} fuel. Delta {res.get('delta','') } km. Invoice={invoice} Paid={driver_paid}")
                except Exception:
                    pass
                context.user_data.pop("pending_fin_multi", None)
                return

    # -- handle single-step finance prompts (parking/wash/repair/fuel solo/odo solo) --
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
                        if pending_simple.get("prompt_chat") and pending_simple.get("prompt_msg_id"):
                            await safe_delete_message(context.bot, pending_simple.get("prompt_chat"), pending_simple.get("prompt_msg_id"))
                    except Exception:
                        pass
                    try:
                        if origin:
                            await safe_delete_message(context.bot, origin.get("chat"), origin.get("msg_id"))
                    except Exception:
                        pass
                    context.user_data.pop("pending_fin_simple", None)
                    try:
                        await context.bot.send_message(chat_id=user.id, text=t(context.user_data.get("lang", DEFAULT_LANG), "invalid_odo"))
                    except Exception:
                        pass
                    return
            else:
                km = m.group(1)
            res = record_finance_entry_single_row("odo", plate, km, "", by_user=user.username or "")
            try:
                await update.effective_message.delete()
            except Exception:
                pass
            try:
                if pending_simple.get("prompt_chat") and pending_simple.get("prompt_msg_id"):
                    await safe_delete_message(context.bot, pending_simple.get("prompt_chat"), pending_simple.get("prompt_msg_id"))
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
                        if pending_simple.get("prompt_chat") and pending_simple.get("prompt_msg_id"):
                            await safe_delete_message(context.bot, pending_simple.get("prompt_chat"), pending_simple.get("prompt_msg_id"))
                    except Exception:
                        pass
                    try:
                        if origin:
                            await safe_delete_message(context.bot, origin.get("chat"), origin.get("msg_id"))
                    except Exception:
                        pass
                    context.user_data.pop("pending_fin_simple", None)
                    try:
                        await context.bot.send_message(chat_id=user.id, text=t(context.user_data.get("lang", DEFAULT_LANG), "invalid_amount"))
                    except Exception:
                        pass
                    return
            else:
                amt = am.group(1)
            res = record_finance_entry_single_row(typ, plate, amt, invoice or "", by_user=user.username or "")
            try:
                await update.effective_message.delete()
            except Exception:
                pass
            try:
                if pending_simple.get("prompt_chat") and pending_simple.get("prompt_msg_id"):
                    await safe_delete_message(context.bot, pending_simple.get("prompt_chat"), pending_simple.get("prompt_msg_id"))
            except Exception:
                pass
            try:
                if origin:
                    await safe_delete_message(context.bot, origin.get("chat"), origin.get("msg_id"))
            except Exception:
                pass
            try:
                await context.bot.send_message(chat_id=user.id, text=f"Recorded {typ} ${amt} for {plate}. Invoice={invoice} Paid={driver_paid}")
            except Exception:
                pass
            context.user_data.pop("pending_fin_simple", None)
            return

    # -- leave pending --
    pending_leave = context.user_data.get("pending_leave")
    if pending_leave:
        parts = text.split()
        if len(parts) < 4:
            try:
                await update.effective_message.delete()
            except Exception:
                pass
            try:
                await safe_delete_message(context.bot, pending_leave.get("prompt_chat"), pending_leave.get("prompt_msg_id"))
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
        try:
            sd = datetime.strptime(start, "%Y-%m-%d")
            ed = datetime.strptime(end, "%Y-%m-%d")
        except Exception:
            try:
                await update.effective_message.delete()
            except Exception:
                pass
            try:
                await safe_delete_message(context.bot, pending_leave.get("prompt_chat"), pending_leave.get("prompt_msg_id"))
            except Exception:
                pass
            try:
                await context.bot.send_message(chat_id=user.id, text="Invalid dates. Use YYYY-MM-DD.")
            except Exception:
                pass
            context.user_data.pop("pending_leave", None)
            return
        try:
            ws = open_worksheet(LEAVE_TAB)
            row = [driver, start, end, reason, notes]
            ws.append_row(row, value_input_option="USER_ENTERED")
            # delete reply + prompt
            try:
                await update.effective_message.delete()
            except Exception:
                pass
            try:
                await safe_delete_message(context.bot, pending_leave.get("prompt_chat"), pending_leave.get("prompt_msg_id"))
            except Exception:
                pass
            # DM confirmation
            try:
                await context.bot.send_message(chat_id=user.id, text=t(context.user_data.get("lang", DEFAULT_LANG), "leave_confirm", driver=driver, start=start, end=end, reason=reason))
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

    # -- mission staff ForceReply (if used) --
    pending_mission = context.user_data.get("pending_mission")
    if pending_mission and pending_mission.get("need_staff") == "enter":
        staff = text
        plate = pending_mission.get("plate")
        departure = pending_mission.get("departure")
        username = user.username or user.full_name
        driver_map = get_driver_map()
        allowed = driver_map.get(user.username, []) if user and user.username else []
        if allowed and plate not in allowed:
            try:
                await update.effective_message.delete()
            except Exception:
                pass
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
                await update.effective_chat.send_message(t(context.user_data.get("lang", DEFAULT_LANG), "mission_start_ok", plate=plate, start_date=now_str(), dep=departure))
            except Exception:
                pass
        else:
            try:
                await update.effective_chat.send_message("❌ " + res.get("message", ""))
            except Exception:
                pass
        context.user_data.pop("pending_mission", None)
        return

# fallback free-text handler routes to process_force_reply
async def location_or_staff(update: Update, context: ContextTypes.DEFAULT_TYPE):
    return await process_force_reply(update, context)

# Plate callback handler (many flows) — when we create prompts we now store prompt id for deletion later
async def plate_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    data = query.data
    user = query.from_user
    username = user.username or f"{user.first_name or ''} {user.last_name or ''}".strip()
    user_lang = context.user_data.get("lang", DEFAULT_LANG)

    # navigation & simple flows
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

    # admin finance entry
    if data == "admin_finance":
        if (query.from_user.username or "") not in BOT_ADMINS:
            await query.edit_message_text("❌ Admins only.")
            return
        return await admin_finance_callback_handler(update, context)
    if data.startswith("fin_type|"):
        return await admin_fin_type_selected(update, context)

    # handle plate selection for finance flows
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
        # store origin so we can delete it later (the callback message that user clicked)
        if typ == "odo_fuel":
            context.user_data["pending_fin_multi"] = {"type": "odo_fuel", "plate": plate, "step": "km", "origin": origin_info}
            fr = ForceReply(selective=False)
            try:
                # edit and also send a separate ForceReply message; we track the ForceReply message id for deletion
                await query.edit_message_text(t(context.user_data.get("lang", DEFAULT_LANG), "enter_odo_km", plate=plate))
                mmsg = await context.bot.send_message(chat_id=query.message.chat.id, text=t(context.user_data.get("lang", DEFAULT_LANG), "enter_odo_km", plate=plate), reply_markup=fr)
                context.user_data["pending_fin_multi"]["prompt_chat"] = mmsg.chat_id
                context.user_data["pending_fin_multi"]["prompt_msg_id"] = mmsg.message_id
            except Exception:
                logger.exception("Failed to prompt for odo km.")
                context.user_data.pop("pending_fin_multi", None)
            return
        if typ in ("parking", "wash", "repair", "fuel"):
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

    # leave menu quick (ForceReply will be deleted when processed)
    if data == "leave_menu":
        fr = ForceReply(selective=False)
        try:
            await query.edit_message_text(t(context.user_data.get("lang", DEFAULT_LANG), "leave_prompt"))
            m = await context.bot.send_message(chat_id=query.message.chat.id, text=t(context.user_data.get("lang", DEFAULT_LANG), "leave_prompt"), reply_markup=fr)
            context.user_data["pending_leave"] = {"prompt_chat": m.chat_id, "prompt_msg_id": m.message_id}
        except Exception:
            logger.exception("Failed to prompt leave.")
        return

    # mission start plate selection -> staff options
    if data.startswith("mission_start_plate|"):
        _, plate = data.split("|", 1)
        context.user_data["pending_mission"] = {"action": "start", "plate": plate}
        kb = [[InlineKeyboardButton("No staff", callback_data=f"mission_staff|none|{plate}"), InlineKeyboardButton("Enter staff", callback_data=f"mission_staff|enter|{plate}")]]
        await query.edit_message_text(t(user_lang, "mission_start_prompt_staff"), reply_markup=InlineKeyboardMarkup(kb))
        return

    # mission end plate selection
    if data.startswith("mission_end_plate|"):
        _, plate = data.split("|", 1)
        context.user_data["pending_mission"] = {"action": "end", "plate": plate}
        kb = [[InlineKeyboardButton("PP", callback_data=f"mission_arrival|PP|{plate}"), InlineKeyboardButton("SHV", callback_data=f"mission_arrival|SHV|{plate}")]]
        await query.edit_message_text(t(user_lang, "mission_end_prompt_arrival"), reply_markup=InlineKeyboardMarkup(kb))
        return

    # mission staff choice
    if data.startswith("mission_staff|"):
        parts = data.split("|")
        if len(parts) < 3:
            await query.edit_message_text("Invalid selection.")
            return
        _, choice, plate = parts
        pending = context.user_data.get("pending_mission") or {}
        pending["plate"] = plate
        if choice == "none":
            dep_kb = [[InlineKeyboardButton("PP", callback_data=f"mission_depart|PP|{plate}"), InlineKeyboardButton("SHV", callback_data=f"mission_depart|SHV|{plate}")]]
            context.user_data["pending_mission"] = pending
            await query.edit_message_text(t(user_lang, "mission_start_prompt_depart"), reply_markup=InlineKeyboardMarkup(dep_kb))
            return
        elif choice == "enter":
            dep_kb = [[InlineKeyboardButton("PP", callback_data=f"mission_depart|PP|{plate}"), InlineKeyboardButton("SHV", callback_data=f"mission_depart|SHV|{plate}")]]
            context.user_data["pending_mission"] = pending
            await query.edit_message_text(t(user_lang, "mission_start_prompt_depart"), reply_markup=InlineKeyboardMarkup(dep_kb))
            return

    # mission depart after staff choice
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
        kb = [[InlineKeyboardButton("Start with no staff", callback_data=f"mission_start_now|{plate}|{dep}"), InlineKeyboardButton("Enter staff name", callback_data=f"mission_staff_enter|{plate}|{dep}")]]
        await query.edit_message_text("Choose staff option:", reply_markup=InlineKeyboardMarkup(kb))
        return

    # mission staff enter -> ForceReply tracked for deletion
    if data.startswith("mission_staff_enter|"):
        _, plate, dep = data.split("|", 2)
        fr = ForceReply(selective=False)
        try:
            await query.edit_message_text("Please reply with staff name (this message will be removed).")
            m = await context.bot.send_message(chat_id=query.message.chat.id, text="Please reply with staff name.", reply_markup=fr)
            context.user_data["pending_mission"] = {"action": "start", "plate": plate, "departure": dep, "need_staff": "enter", "prompt_chat": m.chat_id, "prompt_msg_id": m.message_id, "origin": {"chat": query.message.chat.id, "msg_id": query.message.message_id}}
        except Exception:
            logger.exception("Failed to prompt staff entry.")
        return

    # mission start now (no staff)
    if data.startswith("mission_start_now|"):
        _, plate, dep = data.split("|", 2)
        username = query.from_user.username or query.from_user.full_name
        res = start_mission_record(username, plate, dep, staff_name="")
        if res.get("ok"):
            await query.edit_message_text(t(user_lang, "mission_start_ok", plate=plate, start_date=now_str(), dep=dep))
        else:
            await query.edit_message_text("❌ " + res.get("message", ""))
        context.user_data.pop("pending_mission", None)
        return

    # mission arrival end flow
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
            await query.edit_message_text(t(user_lang, "mission_end_ok", plate=plate, end_date=now_str(), arr=arr))
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
                    await query.message.chat.send_message(f"✅ Driver {username} completed {cnt} missions in {month_start.strftime('%Y-%m')}.")
                except Exception:
                    logger.exception("Failed to send merged missions message.")
        else:
            await query.edit_message_text("❌ " + res.get("message", ""))
        context.user_data.pop("pending_mission", None)
        return

    # start|end quick actions
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
                    await query.edit_message_text(t(user_lang, "start_ok", driver=username, ts=res.get("ts")))
                except Exception:
                    try:
                        await query.message.chat.send_message(t(user_lang, "start_ok", driver=username, ts=res.get("ts")))
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
                try:
                    await query.edit_message_text(t(user_lang, "end_ok", driver=username, ts=ts, dur=dur, n_today=n_today, n_month=n_month, month=month_start.strftime("%Y-%m")))
                except Exception:
                    try:
                        await query.message.chat.send_message(t(user_lang, "end_ok", driver=username, ts=ts, dur=dur, n_today=n_today, n_month=n_month, month=month_start.strftime("%Y-%m")))
                        await safe_delete_message(context.bot, query.message.chat.id, query.message.message_id)
                    except Exception:
                        pass
            else:
                try:
                    await query.edit_message_text("❌ " + res.get("message", ""))
                except Exception:
                    pass
            return

    await query.edit_message_text(t(user_lang, "invalid_sel"))

# lang/mission_report/auto summary and other functions should be registered as before...
# (omitted here for brevity; they remain the same as in your original file)

# A small helper to delete a previously-sent prompt message (by chat/message id) is used everywhere:
# safe_delete_message(bot, chat, msg_id)

# register handlers
def register_ui_handlers(application):
    application.add_handler(CommandHandler("menu", menu_command))
    application.add_handler(CommandHandler(["start_trip", "start"], start_trip_command))
    application.add_handler(CommandHandler(["end_trip", "end"], end_trip_command))
    application.add_handler(CommandHandler("mission", mission_start_command))
    application.add_handler(CommandHandler("mission_start", mission_start_command))
    application.add_handler(CommandHandler("mission_end", mission_end_command))
    application.add_handler(CommandHandler("mission_report", mission_report_command))
    application.add_handler(CommandHandler("leave", leave_command))
    application.add_handler(CommandHandler("setup_menu", setup_menu_command))
    application.add_handler(CommandHandler("lang", lang_command))

    application.add_handler(CallbackQueryHandler(plate_callback))
    application.add_handler(MessageHandler(filters.REPLY & filters.TEXT & (~filters.COMMAND), process_force_reply))
    application.add_handler(MessageHandler(filters.TEXT & (~filters.COMMAND), location_or_staff))
    application.add_handler(MessageHandler(filters.Regex(r'(?i)\b(start|menu|start trip|end trip|trip|出车|还车|返程)\b') & filters.ChatType.GROUPS, auto_menu_listener))

    # delete raw commands to avoid clutter
    async def delete_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        try:
            if update.effective_message:
                await update.effective_message.delete()
        except Exception:
            pass
    application.add_handler(MessageHandler(filters.COMMAND, delete_cmd), group=1)

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
                    BotCommand("setup_menu", "Post and pin the main menu (admins only)"),
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
