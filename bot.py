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
ADMIN_USERS = [u.strip() for u in os.getenv("markpeng1", "kmnyy").split(",") if u.strip()]

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

# HEADERS template per tab — conservative: will only be written if sheet empty
HEADERS_BY_TAB: Dict[str, List[str]] = {
    RECORDS_TAB: ["Date", "Driver", "Plate", "Start DateTime", "End DateTime", "Duration"],
    MISSIONS_TAB: ["GUID", "No.", "Name", "Plate", "Start Date", "End Date", "Departure", "Arrival", "Staff Name", "Roundtrip", "Return Start", "Return End"],
    MISSIONS_REPORT_TAB: ["GUID", "No.", "Name", "Plate", "Start Date", "End Date", "Departure", "Arrival", "Staff Name", "Roundtrip", "Return Start", "Return End"],
    SUMMARY_TAB: ["Date", "PeriodType", "TotalsJSON", "HumanSummary"],
    DRIVERS_TAB: ["Username", "Plates"],
    VEHICLE_COSTS_TAB: ["ID", "Plate", "Date", "Odometer_km", "Fuel_l", "Fuel_cost", "Parking_cost", "Notes", "EnteredBy", "EnteredAt"],
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
        "not_allowed": "❌ អ្នកមិនមានសិទ្ធิប្រើរថយន្តនេះ: {plate}.",
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
    If first data row's first cell looks like GUID but header row's first cell isn't 'GUID',
    overwrite header row only (safe).
    """
    try:
        values = ws.get_all_values()
        if not values or len(values) < 2:
            return
        first_row = values[0]
        second_row = values[1]
        first_cell = str(second_row[0]).strip() if len(second_row) > 0 else ""
        if first_cell and _UUID_RE.match(first_cell):
            header_first = str(first_row[0]).strip().lower() if len(first_row) > 0 else ""
            if header_first != "guid":
                headers = HEADERS_BY_TAB.get(MISSIONS_TAB, [])
                if not headers:
                    return
                # ensure length
                h = list(headers)
                while len(h) < M_MANDATORY_COLS:
                    h.append("")
                col_letter_end = chr(ord('A') + M_MANDATORY_COLS - 1)
                rng = f"A1:{col_letter_end}1"
                ws.update(rng, [h], value_input_option="USER_ENTERED")
                logger.info("Fixed MISSIONS header row due to GUID detected in first data column.")
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


# ===== Driver map loaders =====
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
                    if (cur_dep == "PP" and cur_arr == "SHV" and comp["dep"] == "SHV" and comp["arr"] == "PP") or \
                       (cur_dep == "SHV" and cur_arr == "PP" and comp["dep"] == "PP" and comp["arr"] == "SHV"):
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


# ===== Roundtrip summary functions =====
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
        ws = None
        try:
            ws = sh.add_worksheet(title=tab_name, rows="500", cols="3")
        except Exception:
            ws = sh.worksheet(tab_name)
        ws.append_row(["Driver", "Roundtrip Count (period)"], value_input_option="USER_ENTERED")
        for driver, cnt in sorted(counts.items(), key=lambda x: (-x[1], x[0])):
            ws.append_row([driver, cnt], value_input_option="USER_ENTERED")
        return tab_name
    except Exception:
        logger.exception("Failed to write roundtrip summary tab")
        return None


def write_roundtrip_summary_csv(label: str, counts: Dict[str, int]) -> Optional[str]:
    fname = f"roundtrip_summary_{label}.csv"
    try:
        local_path = os.path.join(os.getcwd(), fname)
        with open(local_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(["Driver", "Roundtrip Count (period)"])
            for driver, cnt in sorted(counts.items(), key=lambda x: (-x[1], x[0])):
                writer.writerow([driver, cnt])
        return local_path
    except Exception:
        logger.exception("Failed to write roundtrip summary CSV")
        return None


# ===== SQLite migration (sheet -> sqlite) =====
def migrate_missions_sheet_to_sqlite(sqlite_path: str = SQLITE_PATH) -> str:
    """
    Migrate MISSIONS sheet to SQLite database.
    Creates missions table and inserts/updates rows.
    Returns sqlite_path.
    """
    try:
        gc = get_gspread_client()
        sh = gc.open(GOOGLE_SHEET_NAME)
        ws = sh.worksheet(MISSIONS_TAB)
        vals, start_idx = _missions_get_values_and_data_rows(ws)
    except Exception:
        logger.exception("Failed to read MISSIONS sheet for migration.")
        vals, start_idx = [], 0

    conn = sqlite3.connect(sqlite_path)
    cur = conn.cursor()
    cur.execute("""
    CREATE TABLE IF NOT EXISTS missions (
        guid TEXT PRIMARY KEY,
        no INTEGER,
        name TEXT,
        plate TEXT,
        start_dt TEXT,
        end_dt TEXT,
        departure TEXT,
        arrival TEXT,
        staff_name TEXT,
        roundtrip TEXT,
        return_start TEXT,
        return_end TEXT
    )
    """)
    conn.commit()

    for r in vals[start_idx:]:
        r = _ensure_row_length(r, M_MANDATORY_COLS)
        guid = r[M_IDX_GUID] or str(uuid.uuid4())
        cur.execute("""
        INSERT OR REPLACE INTO missions (guid, no, name, plate, start_dt, end_dt, departure, arrival, staff_name, roundtrip, return_start, return_end)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (guid, r[M_IDX_NO], r[M_IDX_NAME], r[M_IDX_PLATE], r[M_IDX_START], r[M_IDX_END], r[M_IDX_DEPART], r[M_IDX_ARRIVAL], r[M_IDX_STAFF], r[M_IDX_ROUNDTRIP], r[M_IDX_RETURN_START], r[M_IDX_RETURN_END]))
    conn.commit()
    conn.close()
    logger.info("MIGRATION: missions sheet migrated to SQLite at %s", sqlite_path)
    return sqlite_path


# ===== Unfinished missions detection and alerting =====
async def check_and_alert_unfinished_missions(context: ContextTypes.DEFAULT_TYPE, threshold_hours: int = 6):
    """
    Scan MISSIONS sheet for missions with Start Date but no End Date older than threshold_hours,
    and notify SUMMARY_CHAT_ID (or admins).
    """
    ws = open_worksheet(MISSIONS_TAB)
    vals, start_idx = _missions_get_values_and_data_rows(ws)
    now = _now_dt()
    alerts = []
    for i in range(start_idx, len(vals)):
        r = _ensure_row_length(vals[i], M_MANDATORY_COLS)
        start = str(r[M_IDX_START]).strip()
        end = str(r[M_IDX_END]).strip()
        driver = str(r[M_IDX_NAME]).strip()
        plate = str(r[M_IDX_PLATE]).strip()
        guid = str(r[M_IDX_GUID]).strip()
        if start and not end:
            s_dt = parse_ts(start)
            if s_dt and (now - s_dt).total_seconds() > threshold_hours * 3600:
                alerts.append((driver, plate, guid, start))
    if not alerts:
        return
    chat_targets = []
    if SUMMARY_CHAT_ID:
        chat_targets.append(SUMMARY_CHAT_ID)
    # fallback: send to admin users by sending to SUMMARY_CHAT_ID if configured
    for (driver, plate, guid, start) in alerts:
        msg = f"⚠️ Unfinished mission detected: driver {driver} plate {plate} start {start} GUID {guid}."
        for chat in chat_targets:
            try:
                await context.bot.send_message(chat_id=chat, text=msg)
            except Exception:
                logger.exception("Failed to send unfinished mission alert.")


# ===== Vehicle cost admin input flow =====
# Flow: /cost PLATE -> inline buttons to choose input type -> prompt admin to send value -> record -> delete prompts
COST_FLOW_PREFIX = "cost"
# We'll store temporary state in context.user_data: 'pending_cost' with {plate, type, chat_id, prompt_msg_id, inline_msg}

async def cost_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # only admin allowed
    user = update.effective_user
    if not user or (user.username not in ADMIN_USERS and user.id and str(user.id) not in ADMIN_USERS):
        await update.effective_chat.send_message("Only admins can use this command.")
        return
    if not context.args:
        await update.effective_chat.send_message("Usage: /cost PLATE")
        return
    plate = context.args[0].strip()
    kb = [
        [InlineKeyboardButton("Enter odometer (km)", callback_data=f"{COST_FLOW_PREFIX}|{plate}|odometer")],
        [InlineKeyboardButton("Enter fuel (liters)", callback_data=f"{COST_FLOW_PREFIX}|{plate}|fuel")],
        [InlineKeyboardButton("Enter fuel cost", callback_data=f"{COST_FLOW_PREFIX}|{plate}|fuel_cost")],
        [InlineKeyboardButton("Enter parking cost", callback_data=f"{COST_FLOW_PREFIX}|{plate}|parking")],
    ]
    msg = await update.effective_chat.send_message(f"Admin: choose cost type for {plate}", reply_markup=InlineKeyboardMarkup(kb))
    # store prompt to delete later if needed
    context.user_data["last_cost_prompt"] = {"chat_id": msg.chat_id, "message_id": msg.message_id}


async def cost_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    data = query.data
    user = query.from_user
    if not user or (user.username not in ADMIN_USERS and user.id and str(user.id) not in ADMIN_USERS):
        try:
            await query.edit_message_text("Only admins can do this.")
        except Exception:
            pass
        return
    try:
        _, plate, ctype = data.split("|", 2)
    except Exception:
        try:
            await query.edit_message_text("Invalid selection.")
        except Exception:
            pass
        return
    # Save pending cost
    context.user_data["pending_cost"] = {"plate": plate, "type": ctype}
    # delete inline keyboard message to keep chat clean
    try:
        if query.message:
            await context.bot.delete_message(chat_id=query.message.chat.id, message_id=query.message.message_id)
    except Exception:
        pass
    # prompt admin to send value
    prompt = await update.effective_chat.send_message(f"Please send value for {ctype} on {plate} (or /cancel).")
    context.user_data["last_cost_input_prompt"] = {"chat_id": prompt.chat_id, "message_id": prompt.message_id}


async def handle_cost_value_input(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # only process if pending_cost present
    pending = context.user_data.get("pending_cost")
    if not pending:
        return  # ignore
    user = update.effective_user
    if not user or (user.username not in ADMIN_USERS and user.id and str(user.id) not in ADMIN_USERS):
        # delete message and inform
        try:
            await update.effective_message.delete()
        except Exception:
            pass
        try:
            await update.effective_chat.send_message("Only admins can input cost.")
        except Exception:
            pass
        context.user_data.pop("pending_cost", None)
        return
    text = (update.message.text or "").strip()
    if text.lower() == "/cancel":
        # delete prompt
        last_prompt = context.user_data.get("last_cost_input_prompt")
        if last_prompt:
            try:
                await context.bot.delete_message(chat_id=last_prompt.get("chat_id"), message_id=last_prompt.get("message_id"))
            except Exception:
                pass
        context.user_data.pop("pending_cost", None)
        context.user_data.pop("last_cost_input_prompt", None)
        try:
            await update.effective_chat.send_message("Cancelled.")
        except Exception:
            pass
        return
    plate = pending.get("plate")
    ctype = pending.get("type")
    # attempt parse numeric where appropriate
    val = text
    odometer = None
    fuel_l = None
    fuel_cost = None
    parking_cost = None
    try:
        if ctype == "odometer":
            odometer = float(text)
        elif ctype == "fuel":
            fuel_l = float(text)
        elif ctype == "fuel_cost":
            fuel_cost = float(text)
        elif ctype == "parking":
            parking_cost = float(text)
    except Exception:
        # non-numeric allowed for notes
        pass

    # write to Vehicle_Costs sheet
    ws = open_worksheet(VEHICLE_COSTS_TAB)
    rec_id = str(uuid.uuid4())
    entered_at = now_str()
    row = [rec_id, plate, today_date_str(), odometer if odometer is not None else "", fuel_l if fuel_l is not None else "", fuel_cost if fuel_cost is not None else "", parking_cost if parking_cost is not None else "", "", user.username or user.full_name, entered_at]
    try:
        ws.append_row(row, value_input_option="USER_ENTERED")
        await update.effective_chat.send_message(f"Recorded {ctype} for {plate}.")
    except Exception:
        logger.exception("Failed to write vehicle cost row.")
        await update.effective_chat.send_message("Failed to record cost.")
    # cleanup prompts
    last_prompt = context.user_data.get("last_cost_input_prompt")
    if last_prompt:
        try:
            await context.bot.delete_message(chat_id=last_prompt.get("chat_id"), message_id=last_prompt.get("message_id"))
        except Exception:
            pass
    context.user_data.pop("pending_cost", None)
    context.user_data.pop("last_cost_input_prompt", None)


# ===== Aggregation & summaries (daily/weekly/monthly) =====
def aggregate_for_period(start_date: datetime, end_date: datetime) -> Dict[str, int]:
    ws = open_worksheet(RECORDS_TAB)
    totals: Dict[str, int] = {}
    try:
        rows = ws.get_all_values()
        start_idx = 1 if rows and any("date" in c.lower() for c in rows[0] if c) else 0
        for rec in rows[start_idx:]:
            plate = rec[2] if len(rec) > 2 else ""
            start = rec[3] if len(rec) > 3 else ""
            end = rec[4] if len(rec) > 4 else ""
            if not plate:
                continue
            s_dt = parse_ts(start) if start else None
            e_dt = parse_ts(end) if end else None
            if s_dt and e_dt:
                actual_start = max(s_dt, start_date)
                actual_end = min(e_dt, end_date)
                if actual_end > actual_start:
                    minutes = int((actual_end - actual_start).total_seconds() // 60)
                    totals[plate] = totals.get(plate, 0) + minutes
        return totals
    except Exception:
        logger.exception("Failed to aggregate records")
        return {}


def minutes_to_h_m(total_minutes: int) -> Tuple[int, int]:
    h = total_minutes // 60
    m = total_minutes % 60
    return h, m


def write_daily_summary(date_dt: datetime) -> str:
    start = datetime.combine(date_dt.date(), dtime.min)
    end = start + timedelta(days=1)
    totals = aggregate_for_period(start, end)
    if not totals:
        return f"No records for {start.strftime(DATE_FMT)}"
    lines = []
    for plate, minutes in sorted(totals.items()):
        h, m = minutes_to_h_m(minutes)
        lines.append(f"{plate}: {h}h{m}m")
    try:
        ws = open_worksheet(SUMMARY_TAB)
        row = [start.strftime(DATE_FMT), "daily", json.dumps(totals), "\n".join(lines)]
        ws.append_row(row, value_input_option="USER_ENTERED")
    except Exception:
        logger.exception("Failed to write daily summary to sheet.")
    return "\n".join(lines)


# ===== Telegram UI helpers & handlers (menu, mission flows, etc.) =====
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
        [InlineKeyboardButton("Open menu", callback_data="menu_full"),
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


async def plate_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    data = query.data
    user = query.from_user
    username = user.username or f"{user.first_name or ''} {user.last_name or ''}".strip()
    user_lang = context.user_data.get("lang", DEFAULT_LANG)

    # handle cost callbacks
    if data and data.startswith(COST_FLOW_PREFIX + "|"):
        await cost_callback(update, context)
        return

    # menu navigation
    if data == "show_start":
        await query.edit_message_text(t(user_lang, "choose_start"), reply_markup=build_plate_keyboard("start"))
        return
    if data == "show_end":
        await query.edit_message_text(t(user_lang, "choose_end"), reply_markup=build_plate_keyboard("end"))
        return
    if data == "menu_full":
        await query.edit_message_text(t(user_lang, "menu"), reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("Start trip", callback_data="show_start"),
             InlineKeyboardButton("End trip", callback_data="show_end")],
            [InlineKeyboardButton("Help", callback_data="help")]
        ]))
        return
    if data == "help":
        await query.edit_message_text(t(user_lang, "help"))
        return

    # mission start selection
    if data.startswith("mission_start_plate|"):
        _, plate = data.split("|", 1)
        context.user_data["pending_mission"] = {"action": "start", "plate": plate}
        kb = [[InlineKeyboardButton("PP", callback_data="mission_depart|PP"), InlineKeyboardButton("SHV", callback_data="mission_depart|SHV")]]
        # save inline prompt for deletion
        try:
            if query.message:
                context.user_data["last_inline_prompt"] = {"chat_id": query.message.chat.id, "message_id": query.message.message_id}
        except Exception:
            context.user_data.pop("last_inline_prompt", None)
        await query.edit_message_text(t(user_lang, "mission_start_prompt_depart"), reply_markup=InlineKeyboardMarkup(kb))
        return

    # mission end selection
    if data.startswith("mission_end_plate|"):
        _, plate = data.split("|", 1)
        context.user_data["pending_mission"] = {"action": "end", "plate": plate}
        kb = [[InlineKeyboardButton("PP", callback_data="mission_arrival|PP"), InlineKeyboardButton("SHV", callback_data="mission_arrival|SHV")]]
        try:
            if query.message:
                context.user_data["last_inline_prompt"] = {"chat_id": query.message.chat.id, "message_id": query.message.message_id}
        except Exception:
            context.user_data.pop("last_inline_prompt", None)
        await query.edit_message_text(t(user_lang, "mission_end_prompt_arrival"), reply_markup=InlineKeyboardMarkup(kb))
        return

    # after selecting departure (PP/SHV) for a mission start
    if data.startswith("mission_depart|"):
        _, dep = data.split("|", 1)
        pending = context.user_data.get("pending_mission")
        if not pending or pending.get("action") != "start":
            await query.edit_message_text(t(user_lang, "invalid_sel"))
            return
        pending["departure"] = dep
        context.user_data["pending_mission"] = pending

        # send staff prompt message and save its id
        try:
            chat_id = update.effective_chat.id
            staff_prompt_msg = await context.bot.send_message(chat_id=chat_id, text=t(user_lang, "mission_start_prompt_staff"))
            context.user_data["last_bot_prompt"] = {"chat_id": staff_prompt_msg.chat_id, "message_id": staff_prompt_msg.message_id}
        except Exception:
            await query.edit_message_text(t(user_lang, "mission_start_prompt_staff"))
            context.user_data.pop("last_bot_prompt", None)
        return

    # after selecting arrival for mission end
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

    # start|end quick handlers
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


# Message handler for staff-name replies (/skip or text).
# Delete user's message, delete bot staff prompt, delete inline departure prompt, then record mission start.
async def location_or_skip(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        if update.effective_message:
            await update.effective_message.delete()
    except Exception:
        pass

    user = update.effective_user
    user_lang = context.user_data.get("lang", DEFAULT_LANG)

    # delete stored bot prompt
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

    # delete inline "Select departure city" prompt if exists
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

    # handle pending mission start
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


# New command: /roundtrip_year 2025
async def roundtrip_year_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        if update.effective_message:
            await update.effective_message.delete()
    except Exception:
        pass
    if not context.args:
        await update.effective_chat.send_message("Usage: /roundtrip_year YYYY")
        return
    try:
        year = int(context.args[0])
    except Exception:
        await update.effective_chat.send_message("Invalid year. Usage: /roundtrip_year YYYY")
        return
    counts = count_roundtrips_per_driver_year(year)
    tab = write_roundtrip_summary_tab(str(year), counts)
    csv_path = write_roundtrip_summary_csv(str(year), counts)
    await update.effective_chat.send_message(f"Roundtrip year {year} summary generated.")
    if tab:
        await update.effective_chat.send_message(f"Tab: {tab}")
    if csv_path:
        try:
            with open(csv_path, "rb") as f:
                await context.bot.send_document(chat_id=update.effective_chat.id, document=f, filename=os.path.basename(csv_path))
        except Exception:
            await update.effective_chat.send_message(f"CSV saved: {csv_path}")


# Unfinished check command (manual trigger)
async def check_unfinished_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        if update.effective_message:
            await update.effective_message.delete()
    except Exception:
        pass
    # reuse job function but call directly with context
    await check_and_alert_unfinished_missions(context, threshold_hours=6)
    await update.effective_chat.send_message("Checked unfinished missions (alerts sent if any).")


# Handler to manually trigger migration
async def migrate_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    if not user or (user.username not in ADMIN_USERS and str(user.id) not in ADMIN_USERS):
        await update.effective_chat.send_message("Only admins can run migration.")
        return
    try:
        sqlite_path = migrate_missions_sheet_to_sqlite(SQLITE_PATH)
        await update.effective_chat.send_message(f"Migration complete: {sqlite_path}")
    except Exception:
        await update.effective_chat.send_message("Migration failed; check logs.")


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
            [InlineKeyboardButton("Open full menu", callback_data="menu_full")],
        ]
        await update.effective_chat.send_message(t(user_lang, "menu"), reply_markup=InlineKeyboardMarkup(keyboard))


# Scheduling: daily summary job; if day==1, also generate previous month's mission report
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
    text = write_daily_summary(date_dt)
    try:
        await context.bot.send_message(chat_id=chat_id, text=text)
    except Exception:
        logger.exception("Failed to send daily summary message.")

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
                try:
                    await context.bot.send_message(chat_id=chat_id, text=f"Auto-generated mission report for {prev_month_start.strftime('%Y-%m')}.")
                except Exception:
                    pass
            if tab_name:
                try:
                    await context.bot.send_message(chat_id=chat_id, text=f"Roundtrip summary tab created: {tab_name}")
                except Exception:
                    pass
            if csv_path:
                try:
                    with open(csv_path, "rb") as f:
                        await context.bot.send_document(chat_id=chat_id, document=f, filename=os.path.basename(csv_path))
                except Exception:
                    try:
                        await context.bot.send_message(chat_id=chat_id, text=f"Roundtrip CSV written: {csv_path}")
                    except Exception:
                        pass
        except Exception:
            logger.exception("Failed to auto-generate monthly mission report on day 1.")


# Weekly report job
async def send_weekly_report_job(context: ContextTypes.DEFAULT_TYPE):
    chat_id = SUMMARY_CHAT_ID
    if not chat_id:
        return
    now = _now_dt()
    # last week Monday-Sunday
    weekday = now.weekday()
    last_monday = (now - timedelta(days=weekday + 7)).replace(hour=0, minute=0, second=0, microsecond=0)
    last_sunday_end = last_monday + timedelta(days=6, hours=23, minutes=59, seconds=59)
    totals = aggregate_for_period(last_monday, last_sunday_end)
    if not totals:
        await context.bot.send_message(chat_id=chat_id, text=f"No usage last week ({last_monday.date()} ~ {last_sunday_end.date()})")
        return
    lines = [f"Weekly summary: {last_monday.date()} ~ {last_sunday_end.date()}"]
    for plate, minutes in sorted(totals.items()):
        h, m = minutes_to_h_m(minutes)
        lines.append(f"{plate}: {h}h{m}m")
    await context.bot.send_message(chat_id=chat_id, text="\n".join(lines))


# Register handlers
def register_ui_handlers(application):
    application.add_handler(CommandHandler("menu", menu_command))
    application.add_handler(CommandHandler(["start_trip", "start"], start_trip_command))
    application.add_handler(CommandHandler(["end_trip", "end"], end_trip_command))
    application.add_handler(CommandHandler("lang", lang_command))
    application.add_handler(CommandHandler("mission_start", mission_start_command))
    application.add_handler(CommandHandler("mission_end", mission_end_command))
    application.add_handler(CommandHandler("mission_report", mission_report_command))
    application.add_handler(CommandHandler("roundtrip_year", roundtrip_year_command))
    application.add_handler(CommandHandler("migrate", migrate_command))
    application.add_handler(CommandHandler("check_unfinished", check_unfinished_command))
    application.add_handler(CommandHandler("cost", cost_command))
    application.add_handler(CallbackQueryHandler(plate_callback))
    application.add_handler(MessageHandler(filters.Regex(r'(?i)^/skip$') | (filters.TEXT & (~filters.COMMAND)), location_or_skip))
    # cost value handler for admins (text)
    application.add_handler(MessageHandler(filters.TEXT & (~filters.COMMAND), handle_cost_value_input))
    application.add_handler(MessageHandler(filters.Regex(AUTO_KEYWORD_PATTERN) & filters.ChatType.GROUPS, auto_menu_listener))
    application.add_handler(CommandHandler("help", lambda u, c: u.message.reply_text(t(c.user_data.get("lang", DEFAULT_LANG), "help"))))

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
                    BotCommand("roundtrip_year", "Generate annual roundtrip summary: /roundtrip_year YYYY"),
                    BotCommand("migrate", "Migrate MISSIONS sheet to SQLite (admin)"),
                    BotCommand("check_unfinished", "Check for unfinished missions (admin)"),
                    BotCommand("cost", "Admin: enter vehicle cost /cost PLATE"),
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
            # weekly report on Monday at 09:00
            application.job_queue.run_daily(send_weekly_report_job, time=dtime(hour=9, minute=0), days=(0,), context={"chat_id": SUMMARY_CHAT_ID}, name="weekly_report", tz=tz)
            # unfinished missions check every 6 hours
            application.job_queue.run_repeating(lambda ctx: check_and_alert_unfinished_missions(ctx, threshold_hours=6), interval=6 * 3600, first=60, name="unfinished_check")
            logger.info("Scheduled daily/weekly/unfinished-check jobs.")
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
