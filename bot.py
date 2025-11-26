#!/usr/bin/env python3
"""
Driver Bot — Extended + Reports + Maintenance
- Missions header auto-repair
- Robust roundtrip merge (PP-SHV-PP / SHV-PP-SHV)
- Multi-city roundtrip support (PP-SHV-KPS-PP, etc.)
- Annual roundtrip statistics
- Weekly report auto-push
- Auto-detect unfinished missions and remind drivers
- Driver leave records (Google Sheet new tab)
- Vehicle maintenance records (Google Sheet new tab)
- Admin-only expense inputs (km, fuel cost, parking cost)
- Sheet auto-cleanup
- Sheet automatic structural repair
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

# ===== Headers for all tabs (auto-repair engine will use these) =====
HEADERS_BY_TAB: Dict[str, List[str]] = {
    RECORDS_TAB: ["Date", "Driver", "Plate", "Start DateTime", "End DateTime", "Duration"],

    MISSIONS_TAB: [
        "GUID", "No.", "Name", "Plate", "Start Date", "End Date",
        "Departure", "Arrival", "Staff Name",
        "Roundtrip", "Return Start", "Return End"
    ],

    MISSIONS_REPORT_TAB: [
        "GUID", "No.", "Name", "Plate", "Start Date", "End Date",
        "Departure", "Arrival", "Staff Name",
        "Roundtrip", "Return Start", "Return End"
    ],

    SUMMARY_TAB: ["Date", "PeriodType", "TotalsJSON", "HumanSummary"],

    DRIVERS_TAB: ["Username", "Plates"],

    LEAVE_TAB: ["Driver", "Start Date", "End Date", "Reason", "Notes"],

    MAINT_TAB: ["Plate", "Mileage", "Maintenance Item", "Cost", "Date", "Workshop", "Notes"],

    EXPENSE_TAB: ["Plate", "Driver", "DateTime", "Mileage", "Fuel Cost", "Parking Fee", "Other Fee"],
}

# ===== Minimal translations =====
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

        # missions
        "mission_start_prompt_plate": "Choose plate to start mission:",
        "mission_start_prompt_depart": "Select departure city:",
        "mission_start_prompt_staff": "Optional: enter staff name (or /skip).",
        "mission_start_ok": "✅ Mission start for {plate} at {start_date}, from {dep}.",

        "mission_end_prompt_plate": "Choose plate to end mission:",
        "mission_end_prompt_arrival": "Select arrival city:",
        "mission_end_ok": "✅ Mission ended for {plate} at {end_date}, arrived {arr}.",
        "mission_no_open": "No open mission found for {plate}.",

        # roundtrip
        "roundtrip_merged_notify": "✅ Roundtrip merged for {driver} on {plate}. {count_msg}",
        "roundtrip_monthly_count": "Driver {driver} completed {count} roundtrips this month.",

        # language
        "lang_set": "Language set to {lang}.",

        # weekly
        "weekly_report_generated": "Weekly report generated for {period}.",

        # unfinished
        "unfinished_warning": "⚠️ You have unfinished missions: {detail}",
    },

    "km": {
        "menu": "ម៉ឺនុយបូត — សូមជ្រើសប៊ូតុង:",
        "choose_start": "ជ្រើស plate ដើម្បីចាប់ផ្តើមដំណើរ:",
        "choose_end": "ជ្រើស plate ដើម្បីបញ្ចប់ដំណើរ:",
        "start_ok": "✅ ចាប់ផ្តើមដំណើរ {plate} ({driver}). {msg}",
        "end_ok": "✅ បញ្ចប់ដំណើរ {plate} ({driver}). {msg}",
        "not_allowed": "❌ មិនមានសិទ្ធិប្រើ plate: {plate}.",
        "invalid_sel": "ជម្រើសមិនត្រឹមត្រូវ។",
        "help": "ជំនួយ៖ ប្រើ /start_trip ឬ /end_trip ហើយជ្រើស plate.",
        "no_bot_token": "សូមកំណត់ BOT_TOKEN។",

        # missions
        "mission_start_prompt_plate": "ជ្រើស plate ដើម្បីចាប់ផ្តើម mission:",
        "mission_start_prompt_depart": "ជ្រើសទីក្រុងចេញ:",
        "mission_start_prompt_staff": "បញ្ចូលឈ្មោះបុគ្គលិក (ឬ /skip).",
        "mission_start_ok": "✅ ចាប់ផ្តើម mission {plate} នៅ {start_date} ចេញពី {dep}.",

        "mission_end_prompt_plate": "ជ្រើស plate ដើម្បីបញ្ចប់ mission:",
        "mission_end_prompt_arrival": "ជ្រើសទីក្រុងមកដល់:",
        "mission_end_ok": "✅ បញ្ចប់ mission {plate} នៅ {end_date} មកដល់ {arr}.",

        # roundtrip
        "roundtrip_merged_notify": "✅ រួមបញ្ចូល往返 {driver} លើ {plate}. {count_msg}",
        "roundtrip_monthly_count": "អ្នកបើក {driver} បាន往返 {count} ដងខែនេះ.",

        # language
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
        raise RuntimeError(
            "Google credentials not found. Set GOOGLE_CREDS_BASE64 or GOOGLE_CREDS_PATH or include credentials.json"
        )

    creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_json, SCOPES)
    client = gspread.authorize(creds)
    return client


def ensure_sheet_has_headers_conservative(ws, headers: List[str]):
    """
    Write headers ONLY if the sheet is empty.
    """
    try:
        values = ws.get_all_values()
        if not values:
            ws.insert_row(headers, index=1)
    except Exception:
        logger.exception("Failed to ensure headers on worksheet %s", ws.title)


_UUID_RE = re.compile(
    r'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$',
    re.I
)

def _missions_header_fix_if_needed(ws):
    """
    Detect header mismatch:
    If first data row contains GUID in column 1, but header[0] != 'GUID',
    rewrite ONLY the header row to correct format.
    """
    try:
        values = ws.get_all_values()
        if not values:
            return

        first_row = values[0]
        header_like_keywords = {
            "no", "no.", "name", "plate", "start",
            "end", "departure", "arrival", "staff", "roundtrip"
        }
        is_header_like = any(str(c).strip().lower() in header_like_keywords
                             for c in first_row if c)

        if not is_header_like:
            return

        if len(values) < 2:
            return

        second_row = values[1]
        first_cell = str(second_row[0]).strip() if second_row else ""

        # header repair condition
        if first_cell and _UUID_RE.match(first_cell):
            header_first = str(first_row[0]).strip().lower() if first_row else ""
            if header_first != "guid":
                headers = HEADERS_BY_TAB.get(MISSIONS_TAB)
                if not headers:
                    return

                # Ensure total columns = M_MANDATORY_COLS
                h = list(headers)
                while len(h) < M_MANDATORY_COLS:
                    h.append("")

                col_letter_end = chr(ord('A') + M_MANDATORY_COLS - 1)
                rng = f"A1:{col_letter_end}1"
                ws.update(rng, [h], value_input_option="USER_ENTERED")
                logger.info("[AUTO-FIX] Missions header corrected.")

    except Exception:
        logger.exception("Error fixing missions header.")


def open_worksheet(tab: str = ""):
    """
    Open (or create) worksheet tab.
    Apply conservative header ensure.
    Apply auto-repair for missions tab.
    """
    gc = get_gspread_client()
    sh = gc.open(GOOGLE_SHEET_NAME)

    def _create_tab(name: str, headers: Optional[List[str]] = None):
        try:
            cols = max(12, len(headers) if headers else 12)
            ws_new = sh.add_worksheet(name, rows="2000", cols=str(cols))
            if headers:
                ws_new.insert_row(headers, 1)
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
            return _create_tab(tab, headers)

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
# driver map loaders
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
# ===== Missions helpers =====

def _missions_get_values_and_data_rows(ws):
    values = ws.get_all_values()
    if not values:
        return [], 0

    first_row = values[0]
    header_like_keywords = {
        "guid", "no", "name", "plate", "start", "end",
        "departure", "arrival", "staff", "roundtrip"
    }
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


# ===== Mission START =====

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

        logger.info(
            "Mission start: GUID=%s no=%s driver=%s plate=%s dep=%s",
            guid, next_no, driver, plate, departure
        )

        return {"ok": True, "guid": guid, "no": next_no}

    except Exception as e:
        logger.exception("Failed to append mission start")
        return {"ok": False, "message": "Failed to write mission start: " + str(e)}


# ===== Mission END + Roundtrip Merge =====

def end_mission_record(driver: str, plate: str, arrival: str) -> dict:
    ws = open_worksheet(MISSIONS_TAB)
    try:
        vals, start_idx = _missions_get_values_and_data_rows(ws)

        # 1. Find OPEN mission (last one)
        for i in range(len(vals) - 1, start_idx - 1, -1):
            row = _ensure_row_length(vals[i], M_MANDATORY_COLS)

            rec_plate = str(row[M_IDX_PLATE]).strip()
            rec_name = str(row[M_IDX_NAME]).strip()
            rec_end = str(row[M_IDX_END]).strip()
            rec_start = str(row[M_IDX_START]).strip()

            if rec_plate == plate and rec_name == driver and not rec_end:
                row_number = i + 1
                end_ts = now_str()

                # update main end row
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

                logger.info("Mission end updated for row %d plate=%s driver=%s",
                            row_number, plate, driver)

                # 2. ROUNDTRIP LOGIC (multi-city supported)
                s_dt = parse_ts(rec_start) if rec_start else None
                if not s_dt:
                    return {"ok": True, "message": f"Mission ended at {end_ts}", "merged": False}

                window_start = s_dt - timedelta(hours=ROUNDTRIP_WINDOW_HOURS)
                window_end = s_dt + timedelta(hours=ROUNDTRIP_WINDOW_HOURS)

                # find all possible complementary legs
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

                    if rn != driver or rp != plate:
                        continue
                    if not rstart or not rend:
                        continue

                    r_s_dt = parse_ts(rstart)
                    if not r_s_dt:
                        continue

                    if not (window_start <= r_s_dt <= window_end):
                        continue

                    candidates.append({
                        "idx": j,
                        "start": r_s_dt,
                        "end": parse_ts(rend),
                        "dep": str(r2[M_IDX_DEPART]).strip(),
                        "arr": str(r2[M_IDX_ARRIVAL]).strip(),
                        "rstart": rstart,
                        "rend": rend
                    })

                # multi-city roundtrip support:
                # PP -> SHV -> KPS -> PP considered a roundtrip
                cur_dep = str(row[M_IDX_DEPART]).strip()
                cur_arr = arrival

                possible_pairs = []
                for comp in candidates:
                    # same driver and plate already checked
                    # any reversed route qualifies as return leg
                    if comp["dep"] != cur_dep and comp["arr"] != cur_arr:
                        possible_pairs.append(comp)

                if not possible_pairs:
                    return {"ok": True, "message": f"Mission ended at {end_ts}", "merged": False}

                # pick the closest return leg
                possible_pairs.sort(key=lambda x: abs((x["start"] - s_dt).total_seconds()))
                found_pair = possible_pairs[0]

                other_idx = found_pair["idx"]
                other_start = found_pair["start"]

                primary_idx = i if s_dt <= other_start else other_idx
                secondary_idx = other_idx if primary_idx == i else i

                primary_row_number = primary_idx + 1
                secondary_row_number = secondary_idx + 1

                if primary_idx == i:
                    return_start = found_pair["rstart"]
                    return_end = found_pair["rend"]
                else:
                    return_start = rec_start
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

                # delete secondary row
                try:
                    ws.delete_row(secondary_row_number)
                except Exception:
                    try:
                        ws.update_cell(secondary_row_number, M_IDX_ROUNDTRIP + 1, "Merged")
                    except Exception:
                        pass

                return {
                    "ok": True,
                    "message": f"Mission ended and merged at {end_ts}",
                    "merged": True,
                    "driver": driver,
                    "plate": plate
                }

        return {"ok": False, "message": "No open mission found"}

    except Exception as e:
        logger.exception("Failed to update mission end: %s", e)
        return {"ok": False, "message": "Failed to write mission end: " + str(e)}
# ===== Mission Report Helpers =====

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
                out.append([
                    r[M_IDX_GUID], r[M_IDX_NO], r[M_IDX_NAME], r[M_IDX_PLATE],
                    r[M_IDX_START], r[M_IDX_END], r[M_IDX_DEPART], r[M_IDX_ARRIVAL],
                    r[M_IDX_STAFF], r[M_IDX_ROUNDTRIP], r[M_IDX_RETURN_START], r[M_IDX_RETURN_END]
                ])
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

        # roundtrip summary for report
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
            ws.append_row(["No roundtrips found."], value_input_option="USER_ENTERED")

        return True

    except Exception:
        logger.exception("Failed to write mission report")
        return False


# ===== Roundtrip monthly & annual =====

def count_roundtrips_per_driver_month(start_date: datetime, end_date: datetime) -> Dict[str, int]:
    counts: Dict[str, int] = {}
    try:
        ws = open_worksheet(MISSIONS_TAB)
        vals, start_idx = _missions_get_values_and_data_rows(ws)
        for r in vals[start_idx:]:
            r = _ensure_row_length(r, M_MANDATORY_COLS)
            s = str(r[M_IDX_START]).strip()
            if not s:
                continue
            s_dt = parse_ts(s)
            if not s_dt or not (start_date <= s_dt < end_date):
                continue
            rt = str(r[M_IDX_ROUNDTRIP]).strip().lower()
            if rt != "yes":
                continue
            name = str(r[M_IDX_NAME]).strip() or "Unknown"
            counts[name] = counts.get(name, 0) + 1
    except Exception:
        logger.exception("Failed monthly roundtrip count")
    return counts


def count_roundtrips_per_driver_year(year: int) -> Dict[str, int]:
    """
    Annual statistics for all drivers.
    """
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
        ws.append_row(["Driver", "Roundtrip Count"], value_input_option="USER_ENTERED")

        for d, cnt in sorted(counts.items(), key=lambda x: (-x[1], x[0])):
            ws.append_row([d, cnt], value_input_option="USER_ENTERED")

        return tab_name
    except Exception:
        logger.exception("Failed to write summary tab")
        return None


def write_roundtrip_summary_csv(month_label: str, counts: Dict[str, int]) -> Optional[str]:
    fname = f"roundtrip_summary_{month_label}.csv"
    try:
        local_path = os.path.join(os.getcwd(), fname)
        with open(local_path, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(["Driver", "Roundtrip Count"])
            for d, cnt in sorted(counts.items(), key=lambda x: (-x[1], x[0])):
                w.writerow([d, cnt])
        return local_path
    except Exception:
        logger.exception("CSV write failed")
        return None
# ===== Driver Leave =====

def add_driver_leave(driver: str, start: str, end: str, reason: str, notes: str = "") -> dict:
    """
    Add a leave record to LEAVE_TAB.
    """
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

def add_vehicle_maintenance(plate: str, mileage: str, item: str, cost: str,
                            date: str, workshop: str, notes: str = "") -> dict:
    ws = open_worksheet(MAINT_TAB)
    try:
        ws.append_row(
            [plate, mileage, item, cost, date, workshop, notes],
            value_input_option="USER_ENTERED"
        )
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


# ===== Trip Expenses (Admin only) =====

ADMIN_USERS = set(os.getenv("BOT_ADMINS", "").split(",")) if os.getenv("BOT_ADMINS") else set()

def add_trip_expense(plate: str, driver: str, mileage: str, fuel: str, parking: str, other: str) -> dict:
    ws = open_worksheet(EXPENSE_TAB)
    try:
        ws.append_row(
            [plate, driver, now_str(), mileage, fuel, parking, other],
            value_input_option="USER_ENTERED"
        )
        return {"ok": True}
    except Exception as e:
        logger.exception("Failed to add trip expense")
        return {"ok": False, "message": str(e)}


# ===== Unfinished Missions =====

def detect_unfinished_missions() -> Dict[str, List[dict]]:
    """
    Return mapping driver -> list of unfinished missions.
    """
    ws = open_worksheet(MISSIONS_TAB)
    out: Dict[str, List[dict]] = {}
    try:
        vals, start_idx = _missions_get_values_and_data_rows(ws)
        for r in vals[start_idx:]:
            row = _ensure_row_length(r, M_MANDATORY_COLS)
            end = str(row[M_IDX_END]).strip()
            if not end:
                driver = str(row[M_IDX_NAME]).strip()
                entry = {
                    "plate": row[M_IDX_PLATE],
                    "start": row[M_IDX_START],
                    "depart": row[M_IDX_DEPART],
                }
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
            text = "Unfinished missions:\n" + "\n".join(
                [f"- Plate {i['plate']} from {i['depart']} at {i['start']}" for i in items]
            )
            try:
                await bot.send_message(chat_id=driver, text=text)
            except Exception:
                pass
    except Exception:
        logger.exception("Failed notify_unfinished_missions")


# ===== Sheet cleanup / auto-repair =====

def cleanup_old_rows(tab: str, max_rows: int = 2000):
    """
    Optional cleanup: trim sheet if too many rows.
    Keep header + latest N rows.
    """
    try:
        ws = open_worksheet(tab)
        rows = ws.get_all_values()
        if len(rows) > max_rows + 1:
            header = rows[0]
            keep = rows[-max_rows:]
            ws.clear()
            ws.append_row(header)
            for r in keep:
                ws.append_row(r)
            logger.info("Sheet %s cleaned to last %s rows", tab, max_rows)
    except Exception:
        logger.exception("Failed cleanup for %s", tab)


def auto_repair_all_sheets():
    """
    Try auto-repair headers for all known tabs.
    Recreate missing tabs.
    """
    for tab, headers in HEADERS_BY_TAB.items():
        try:
            ws = open_worksheet(tab)
            # ensure header exists
            ensure_sheet_has_headers_conservative(ws, headers)
            # missions special repair
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
        [InlineKeyboardButton("Start trip", callback_data="show_start"),
         InlineKeyboardButton("End trip", callback_data="show_end")],
        [InlineKeyboardButton("Mission start", callback_data="show_mission_start"),
         InlineKeyboardButton("Mission end", callback_data="show_mission_end")],
        [InlineKeyboardButton("Admin finance", callback_data="admin_finance"),
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
    if query:
        await query.answer()
    data = query.data if query else ""
    user = query.from_user if query else update.effective_user
    username = user.username or f"{user.first_name or ''} {user.last_name or ''}".strip()
    user_lang = context.user_data.get("lang", DEFAULT_LANG)

    # menu navigation
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
    if data == "admin_finance":
        # show admin buttons if authorized
        if username in ADMIN_USERS or username in ADMIN_USERS:
            kb = [
                [InlineKeyboardButton("Report Odometer", callback_data="fin|odo"),
                 InlineKeyboardButton("Report Fuel", callback_data="fin|fuel")],
                [InlineKeyboardButton("Report Parking", callback_data="fin|parking")],
            ]
            await query.edit_message_text("Admin finance:", reply_markup=InlineKeyboardMarkup(kb))
        else:
            await query.edit_message_text("Unauthorized")
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
                    logger.exception("Failed to build/send monthly summary.")
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


async def location_or_skip(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        if update.effective_message:
            await update.effective_message.delete()
    except Exception:
        pass

    user = update.effective_user
    user_lang = context.user_data.get("lang", DEFAULT_LANG)

    # delete last prompts if present
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
                    await update.effective_chat.send_message(f"Roundtrip CSV saved: {csv_path}")

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


# ===== Scheduling jobs: daily, weekly, detect unfinished =====

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

# weekly summary already implemented as send_weekly_summary_job above;
# reuse detect_unfinished_missions_job (notify_unfinished_missions) for scheduled checks.

def register_ui_handlers(application):
    application.add_handler(CommandHandler("menu", menu_command))
    application.add_handler(CommandHandler(["start_trip", "start"], start_trip_command))
    application.add_handler(CommandHandler(["end_trip", "end"], end_trip_command))
    application.add_handler(CommandHandler("lang", lang_command))
    application.add_handler(CommandHandler("mission_start", mission_start_command))
    application.add_handler(CommandHandler("mission_end", mission_end_command))
    application.add_handler(CommandHandler("mission_report", mission_report_command))
    application.add_handler(CallbackQueryHandler(plate_callback))
    application.add_handler(MessageHandler(filters.Regex(r'(?i)^/skip$') | (filters.TEXT & (~filters.COMMAND)), location_or_skip))
    application.add_handler(MessageHandler(filters.Regex(AUTO_KEYWORD_PATTERN) & filters.ChatType.GROUPS, auto_menu_listener))
    application.add_handler(CommandHandler("help", lambda u, c: u.message.reply_text(t(c.user_data.get("lang", DEFAULT_LANG), "help"))))

    # admin finance handlers (callback handled inside plate_callback for simplicity)
    application.add_handler(CommandHandler("report_odo", lambda u, c: u.message.reply_text("Use admin finance buttons or /report_odo PLATE ODOMETER")))
    application.add_handler(CommandHandler("report_fuel", lambda u, c: u.message.reply_text("Use admin finance buttons or /report_fuel PLATE AMOUNT")))
    application.add_handler(CommandHandler("report_parking", lambda u, c: u.message.reply_text("Use admin finance buttons or /report_parking PLATE AMOUNT")))

    try:
        async def _set_cmds():
            try:
                await application.bot.set_my_commands([
                    BotCommand("start_trip", "Start a trip (select plate)"),
                    BotCommand("end_trip", "End a trip (select plate)"),
                    BotCommand("menu", "Open trip menu"),
                    BotCommand("lang", "Set language /lang en|km"),
                    BotCommand("mission_start", "Start a driver mission"),
                    BotCommand("mission_end", "End a driver mission"),
                    BotCommand("mission_report", "Generate mission report: /mission_report month YYYY-MM"),
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
            # unfinished mission detector
            application.job_queue.run_repeating(detect_unfinished_missions_job, interval=6 * 3600, first=30, name="detect_unfinished")
            # notify drivers for unfinished missions (DM)
            application.job_queue.run_repeating(notify_unfinished_missions, interval=6 * 3600, first=60, name="notify_unfinished")
            # cleanup job
            if int(os.getenv("CLEAN_OLDER_THAN_DAYS", "0")) > 0:
                application.job_queue.run_daily(lambda ctx: clean_old_rows(), time=dtime(hour=3, minute=0), name="clean_old_rows")
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
            logger.info("LOCAL_TZ=%s but could not initialize ZoneInfo; using system time.", LOCAL_TZ)
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
