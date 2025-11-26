#!/usr/bin/env python3
"""
Driver Bot — GUID + conservative headers + monthly auto-report + correct roundtrip counting.

Features added/changed compared to prior version:
- MISSIONS table now has GUID as first column.
- ensure_sheet_has_headers is conservative: it only writes headers when sheet is completely empty.
- After merging a roundtrip, we use count_roundtrips_per_driver_month(...) to count driver's roundtrips.
- A daily job runs at SUMMARY_HOUR; if it's the 1st day of the month, it will auto-generate/send the previous month's mission report.
- All other functionality retained: start/end trips, mission start/end, 24h window merging, roundtrip summary tab + CSV, /lang, deletion of invoking messages, etc.
"""

import os
import json
import base64
import logging
import csv
import uuid
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

# Driver map env (username -> [plates])
DRIVER_PLATE_MAP_JSON = os.getenv("DRIVER_PLATE_MAP", "").strip() or None

# Scheduling / summary
SUMMARY_CHAT_ID = os.getenv("SUMMARY_CHAT_ID")  # where to send monthly report automatically on day 1
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

# Mission columns after adding GUID as first column:
# GUID(1), No.(2), Name(3), Plate(4), Start Date(5), End Date(6), Departure(7), Arrival(8),
# Staff Name(9), Roundtrip(10), Return Start(11), Return End(12)
M_COL_GUID = 1
M_COL_NO = 2
M_COL_NAME = 3
M_COL_PLATE = 4
M_COL_START = 5
M_COL_END = 6
M_COL_DEPART = 7
M_COL_ARRIVAL = 8
M_COL_STAFF = 9
M_COL_ROUNDTRIP = 10
M_COL_RETURN_START = 11
M_COL_RETURN_END = 12

# Column mapping for trip records (1-indexed)
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
ROUNDTRIP_WINDOW_HOURS = 24

# Google scopes
SCOPES = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]

# HEADERS template per tab — conservative: will only be written if sheet is empty
HEADERS_BY_TAB: Dict[str, List[str]] = {
    RECORDS_TAB: ["Date", "Driver", "Plate", "Start DateTime", "End DateTime", "Duration"],
    MISSIONS_TAB: ["GUID", "No.", "Name", "Plate", "Start Date", "End Date", "Departure", "Arrival", "Staff Name", "Roundtrip", "Return Start", "Return End"],
    MISSIONS_REPORT_TAB: ["GUID", "No.", "Name", "Plate", "Start Date", "End Date", "Departure", "Arrival", "Staff Name", "Roundtrip", "Return Start", "Return End"],
    SUMMARY_TAB: ["Date", "PeriodType", "TotalsJSON", "HumanSummary"],
    DRIVERS_TAB: ["Username", "Plates"],
    # Roundtrip_Summary_{YYYY-MM} will use ["Driver", "Roundtrip Count (month)"]
}

# Minimal translations (en + km)
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
        "setup_complete": "Setup complete — commands are set for this group.",
        "summary_subject": "Daily vehicle usage summary for {date}:",
        "summary_row": "{plate}: {hours}h{minutes}m total",
        "no_records": "No records found for {date}.",
        "lang_set": "Language set to {lang}.",
        "mission_start_prompt_plate": "Please choose the plate for the mission start:",
        "mission_start_prompt_depart": "Select departure city:",
        "mission_start_prompt_staff": "Optional: send staff name accompanying you (or /skip).",
        "mission_start_ok": "✅ Mission start recorded for {plate} at {start_date} departing {dep}.",
        "mission_end_prompt_plate": "Please choose the plate to end the mission (will match your last open mission):",
        "mission_end_prompt_arrival": "Select arrival city:",
        "mission_end_ok": "✅ Mission end recorded for {plate} at {end_date} arriving {arr}.",
        "mission_no_open": "No open mission found for {plate}.",
        "mission_report_month_ok": "Monthly mission report for {month} generated and written to sheet.",
        "mission_report_year_ok": "Yearly mission report for {year} generated and written to sheet.",
        "mission_invalid_cmd": "Usage: /mission_report month YYYY-MM  OR  /mission_report year YYYY",
        "roundtrip_merged_notify": "✅ Roundtrip merged for {driver} on plate {plate}. {count_msg}",
        "roundtrip_monthly_count": "Driver {driver} completed {count} roundtrips this month.",
        "roundtrip_summary_tab_ok": "Roundtrip summary tab created: {tab_name}",
        "roundtrip_csv_ok": "Roundtrip CSV written: {csv_path}",
    },
    "km": {
        "menu": "មិនីវរ​បូត — សូមចុចប៊ូតុងដើម្បីអនុវត្ត៖",
        "choose_start": "សូមជ្រើសលេខផ្ទះបណ្ដោះដើម្បីចាប់ផ្តើមដំណើរ:",
        "choose_end": "សូមជ្រើសលេខផ្ទះបណ្ដោះដើម្បីបញ្ចប់ដំណើរ:",
        "start_ok": "✅​ចាប់ផ្តើមដំណើរសម្រាប់ {plate} (អ្នកបើក: {driver}). {msg}",
        "end_ok": "✅ បញ្ចប់ដំណើរសម្រាប់ {plate} (អ្នកបើក: {driver}). {msg}",
        "not_allowed": "❌ អ្នកមិនមានសិទ្ធិប្រើលើរថយន្តนี้: {plate}.",
        "invalid_sel": "ជម្រើសមិនត្រឹមត្រូវ។",
        "help": "ជំនួយ៖ ចុច Start trip ឬ End trip ហើយជ្រើស plate.",
        "no_bot_token": "សូមកំណត់ BOT_TOKEN variable.",
        "setup_complete": "ចំណុចនេះបានរៀបចំរួច — បញ្ជាថានេះបានកំណត់សម្រាប់ក្រុម។",
        "summary_subject": "សេចក្តីសង្ខេបការប្រើរថយន្តរៀងរាល់ថ្ងៃ {date}:",
        "summary_row": "{plate}: {hours}ស {minutes}នាទីសរុប",
        "no_records": "មិនមានកំណត់ត្រាសម្រាប់ {date}.",
        "lang_set": "បានកំណត់ភាសា​ជា {lang}.",
        "mission_start_prompt_plate": "សូមជ្រើស plate សម្រាប់ចាប់ផ្តើមដើមកិច្ចធ្វើការទស្សនកិច្ច:",
        "mission_start_prompt_depart": "ជ្រើសទីក្រុងចេញដើម (Departure):",
        "mission_start_prompt_staff": "Optional: ផ្ញើឈ្មោះបុគ្គលិកដែលដើមជាមួយ (ឬ /skip).",
        "mission_start_ok": "✅ កត់ត្រាចាប់ផ្តើមដំណើរ សម្រាប់ {plate} នៅ {start_date} ចេញពី {dep}.",
        "mission_end_prompt_plate": "សូមជ្រើស plate ដើម្បីបញ្ចប់ដំណើរ (ចាំបាច់នឹងផ្គូតទៅកិច្ចការកំពុងបើក):",
        "mission_end_prompt_arrival": "ជ្រើសទីក្រុងមកដល់ (Arrival):",
        "mission_end_ok": "✅ កត់ត្រាបញ្ចប់ដំណើរសម្រាប់ {plate} នៅ {end_date} មកដល់ {arr}.",
        "mission_no_open": "មិនមានកិច្ចការបើកសម្រាប់ {plate}.",
        "mission_report_month_ok": "របាយការណ៍ខែ {month} បានបង្កើត និងបានសរសេរទៅ sheet។",
        "mission_report_year_ok": "របាយការណ៍ឆ្នាំ {year} បានបង្កើត និងបានសរសេរទៅ sheet។",
        "mission_invalid_cmd": "ប្រើ: /mission_report month YYYY-MM  ឬ  /mission_report year YYYY",
        "roundtrip_merged_notify": "✅ រួមបញ្ចូល往返សម្រាប់ {driver} លើ plate {plate}. {count_msg}",
        "roundtrip_monthly_count": "អ្នកបើក {driver} បានធ្វើ往返 {count} ដងក្នុងខែនេះ។",
        "roundtrip_summary_tab_ok": "បានបង្កើតតាប្រសិទ្ធិរួម往返: {tab_name}",
        "roundtrip_csv_ok": "បានសរសេរ CSV: {csv_path}",
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
    This avoids overwriting user data or manually created headers.
    """
    try:
        values = ws.get_all_values()
        if not values:
            ws.insert_row(headers, index=1)
    except Exception:
        logger.exception("Failed to ensure headers (conservative) on worksheet %s", ws.title)


def open_worksheet(tab: str = ""):
    """
    Open specific worksheet tab by name. If tab is provided but does not exist,
    it will be created with sensible headers depending on tab name.
    Conservative header policy: only write headers if sheet is empty.
    """
    gc = get_gspread_client()
    sh = gc.open(GOOGLE_SHEET_NAME)

    def _create_tab(name: str, headers: Optional[List[str]] = None):
        try:
            cols = max(10, len(headers) if headers else 10)
            ws_new = sh.add_worksheet(title=name, rows="2000", cols=str(cols))
            if headers:
                ws_new.insert_row(headers, index=1)
            return ws_new
        except Exception:
            # fallback to opening if add_worksheet fails
            try:
                return sh.worksheet(name)
            except Exception:
                raise

    if tab:
        try:
            ws = sh.worksheet(tab)
            # conservative header ensure
            template = None
            if tab in HEADERS_BY_TAB:
                template = HEADERS_BY_TAB[tab]
            elif tab.startswith("Roundtrip_Summary_"):
                template = ["Driver", "Roundtrip Count (month)"]
            if template:
                ensure_sheet_has_headers_conservative(ws, template)
            return ws
        except Exception:
            headers = None
            if tab in HEADERS_BY_TAB:
                headers = HEADERS_BY_TAB[tab]
            elif tab.startswith("Roundtrip_Summary_"):
                headers = ["Driver", "Roundtrip Count (month)"]
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


# ===== Driver map loading =====
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
        records = ws.get_all_records()
        for idx in range(len(records) - 1, -1, -1):
            rec = records[idx]
            rec_plate = str(rec.get("Plate No.", rec.get("Plate", rec.get("Plate No", "")))).strip()
            end_val = str(rec.get("End date&time", rec.get("End", ""))).strip()
            start_val = str(rec.get("Start date&time", rec.get("Start", ""))).strip()
            if rec_plate == plate and (end_val == "" or end_val is None):
                row_number = idx + 2  # header row + 1-indexing
                end_ts = now_str()
                duration_text = compute_duration(start_val, end_ts) if start_val else ""
                ws.update_cell(row_number, COL_END, end_ts)
                ws.update_cell(row_number, COL_DURATION, duration_text)
                logger.info("Recorded end trip for %s row %d", plate, row_number)
                return {"ok": True, "message": f"End time recorded for {plate} at {end_ts} (duration {duration_text})"}
        # no matching start found -> append end-only row
        end_ts = now_str()
        row = [today_date_str(), driver, plate, "", end_ts, ""]
        ws.append_row(row, value_input_option="USER_ENTERED")
        logger.info("No open start found; appended end-only row for %s", plate)
        return {"ok": True, "message": f"End time recorded (no matching start found) for {plate} at {end_ts}"}
    except Exception as e:
        logger.exception("Failed to update end trip")
        return {"ok": False, "message": "Failed to write end trip to sheet: " + str(e)}


# ===== Aggregation & summaries =====
def aggregate_for_period(start_date: datetime, end_date: datetime) -> Dict[str, int]:
    ws = open_worksheet(RECORDS_TAB)
    totals: Dict[str, int] = {}
    try:
        rows = ws.get_all_records()
        for r in rows:
            plate = str(r.get("Plate No.", r.get("Plate", r.get("Plate No", "")))).strip()
            start = str(r.get("Start date&time", r.get("Start", ""))).strip()
            end = str(r.get("End date&time", r.get("End", ""))).strip()
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
        return t(DEFAULT_LANG, "no_records", date=start.strftime(DATE_FMT))
    lines = []
    for plate, minutes in sorted(totals.items()):
        h, m = minutes_to_h_m(minutes)
        lines.append(t(DEFAULT_LANG, "summary_row", plate=plate, hours=h, minutes=m))
    try:
        ws = open_worksheet(SUMMARY_TAB)
        row = [start.strftime(DATE_FMT), "daily", json.dumps(totals), "\n".join(lines)]
        ws.append_row(row, value_input_option="USER_ENTERED")
    except Exception:
        logger.exception("Failed to write daily summary to sheet.")
    header = t(DEFAULT_LANG, "summary_subject", date=start.strftime(DATE_FMT))
    return header + "\n" + "\n".join(lines)


# ===== Missions: start / end / merge logic (with GUID) =====
def _missions_next_no(ws) -> int:
    try:
        all_vals = ws.get_all_values()
        if not all_vals:
            return 1
        # data rows = all rows minus header
        data_rows = len(all_vals) - 1
        return data_rows + 1
    except Exception:
        return 1


def start_mission_record(driver: str, plate: str, departure: str, staff_name: str = "") -> dict:
    ws = open_worksheet(MISSIONS_TAB)
    start_ts = now_str()
    try:
        next_no = _missions_next_no(ws)
        guid = str(uuid.uuid4())
        # Build row aligned to new column layout (GUID first)
        row = [guid, next_no, driver, plate, start_ts, "", departure, "", staff_name, "", "", ""]
        ws.append_row(row, value_input_option="USER_ENTERED")
        logger.info("Mission start recorded: GUID=%s, No=%s, driver=%s, plate=%s, dep=%s", guid, next_no, driver, plate, departure)
        return {"ok": True, "no": next_no, "guid": guid, "message": f"Mission start recorded for {plate} at {start_ts}"}
    except Exception as e:
        logger.exception("Failed to append mission start")
        return {"ok": False, "message": "Failed to write mission start to sheet: " + str(e)}


def end_mission_record(driver: str, plate: str, arrival: str) -> dict:
    """
    Find last open mission row (same driver & plate) without End Date -> update End Date & Arrival,
    then try to detect complementary leg within 24 hours window to form a full roundtrip.
    If found, merge the pair into primary row (earlier start) and delete the other row.
    Uses GUID-safe merging where possible.
    """
    ws = open_worksheet(MISSIONS_TAB)
    try:
        records = ws.get_all_records()
        # find last open mission row for this driver+plate (scan backward)
        for idx in range(len(records) - 1, -1, -1):
            rec = records[idx]
            rec_plate = str(rec.get("Plate", rec.get("plate", ""))).strip()
            rec_name = str(rec.get("Name", rec.get("Driver", ""))).strip()
            end_val = str(rec.get("End Date", rec.get("End", ""))).strip()
            start_val = str(rec.get("Start Date", rec.get("Start", ""))).strip()
            if rec_plate == plate and (end_val == "" or end_val is None) and (rec_name == driver):
                row_number = idx + 2  # header + 1
                end_ts = now_str()
                try:
                    ws.update_cell(row_number, M_COL_END, end_ts)  # End Date
                    ws.update_cell(row_number, M_COL_ARRIVAL, arrival)
                except Exception:
                    # fallback to fetching row values and re-writing
                    existing = ws.row_values(row_number)
                    while len(existing) < M_COL_RETURN_END:
                        existing.append("")
                    # column indexes are 1-based: update index M_COL_END-1
                    existing[M_COL_END - 1] = end_ts
                    existing[M_COL_ARRIVAL - 1] = arrival
                    ws.delete_row(row_number)
                    ws.insert_row(existing, row_number)
                logger.info("Mission end updated for %s row %d (arrival=%s)", plate, row_number, arrival)

                # Try to detect complementary leg within ROUNDTRIP_WINDOW_HOURS
                s_dt = parse_ts(start_val) if start_val else None
                if not s_dt:
                    return {"ok": True, "message": f"Mission end recorded for {plate} at {end_ts}", "merged": False}
                window_start = s_dt - timedelta(hours=ROUNDTRIP_WINDOW_HOURS)
                window_end = s_dt + timedelta(hours=ROUNDTRIP_WINDOW_HOURS)

                # Re-fetch records
                records2 = ws.get_all_records()
                completed = []
                for j, r2 in enumerate(records2):
                    if j == idx:
                        continue
                    rn = str(r2.get("Name", r2.get("Driver", ""))).strip()
                    rp = str(r2.get("Plate", r2.get("plate", ""))).strip()
                    rstart = str(r2.get("Start Date", r2.get("Start", ""))).strip()
                    rend = str(r2.get("End Date", r2.get("End Date", r2.get("End", "")))).strip() or str(r2.get("End", r2.get("End Date", ""))).strip()
                    dep = str(r2.get("Departure", r2.get("departure", ""))).strip()
                    arr = str(r2.get("Arrival", r2.get("arrival", ""))).strip()
                    if rn != driver or rp != plate:
                        continue
                    if not rstart or not rend:
                        continue
                    r_s_dt = parse_ts(rstart)
                    r_e_dt = parse_ts(rend)
                    if not r_s_dt or not r_e_dt:
                        continue
                    if not (window_start <= r_s_dt <= window_end):
                        continue
                    completed.append({"idx": j, "start": r_s_dt, "end": r_e_dt, "dep": dep, "arr": arr, "rstart": rstart, "rend": rend})

                # Look for complementary (dep/arr swapped)
                found_pair = None
                cur_dep = str(rec.get("Departure", rec.get("departure", ""))).strip()
                cur_arr = arrival
                for comp in completed:
                    if (cur_dep == "PP" and cur_arr == "SHV" and comp["dep"] == "SHV" and comp["arr"] == "PP") or \
                       (cur_dep == "SHV" and cur_arr == "PP" and comp["dep"] == "PP" and comp["arr"] == "SHV"):
                        found_pair = comp
                        break

                if not found_pair:
                    # no complementary leg found
                    return {"ok": True, "message": f"Mission end recorded for {plate} at {end_ts}", "merged": False}

                # decide primary/secondary based on earlier start
                other_idx = found_pair["idx"]
                other_start = found_pair["start"]
                primary_idx = idx if s_dt <= other_start else other_idx
                secondary_idx = other_idx if primary_idx == idx else idx

                primary_row_number = primary_idx + 2
                secondary_row_number = secondary_idx + 2

                # pick return start/end values
                if primary_idx == idx:
                    return_start = found_pair["start"].strftime(TS_FMT)
                    return_end = found_pair["end"].strftime(TS_FMT)
                else:
                    return_start = s_dt.strftime(TS_FMT)
                    return_end = end_ts

                # Update primary row's Roundtrip, Return Start, Return End
                try:
                    ws.update_cell(primary_row_number, M_COL_ROUNDTRIP, "Yes")
                    ws.update_cell(primary_row_number, M_COL_RETURN_START, return_start)
                    ws.update_cell(primary_row_number, M_COL_RETURN_END, return_end)
                except Exception:
                    try:
                        existing = ws.row_values(primary_row_number)
                        while len(existing) < M_COL_RETURN_END:
                            existing.append("")
                        existing[M_COL_ROUNDTRIP - 1] = "Yes"
                        existing[M_COL_RETURN_START - 1] = return_start
                        existing[M_COL_RETURN_END - 1] = return_end
                        ws.delete_row(primary_row_number)
                        ws.insert_row(existing, primary_row_number)
                    except Exception:
                        logger.exception("Failed to update primary merged row; aborting merge.")
                        return {"ok": True, "message": f"Mission end recorded for {plate} at {end_ts}", "merged": False}

                # Delete secondary row (best-effort). Prefer deleting by exact row number.
                try:
                    ws.delete_row(secondary_row_number)
                    logger.info("Deleted secondary mission row %d after merging into %d", secondary_row_number, primary_row_number)
                except Exception:
                    # if deletion fails, mark it as merged to avoid duplicate double counting
                    try:
                        ws.update_cell(secondary_row_number, M_COL_ROUNDTRIP, "Merged")
                    except Exception:
                        logger.exception("Failed to delete or mark secondary merged row.")

                return {"ok": True, "message": f"Mission end recorded for {plate} at {end_ts}", "merged": True, "driver": driver, "plate": plate}
        # no open mission
        return {"ok": False, "message": "No open mission found"}
    except Exception as e:
        logger.exception("Failed to update mission end")
        return {"ok": False, "message": "Failed to write mission end to sheet: " + str(e)}


def mission_rows_for_period(start_date: datetime, end_date: datetime) -> List[List[Any]]:
    ws = open_worksheet(MISSIONS_TAB)
    out = []
    try:
        rows = ws.get_all_records()
        for rec in rows:
            start = str(rec.get("Start Date", rec.get("Start", ""))).strip()
            if not start:
                continue
            s_dt = parse_ts(start)
            if not s_dt:
                continue
            if start_date <= s_dt < end_date:
                guid = rec.get("GUID", "")
                no = rec.get("No", rec.get("no", ""))
                name = rec.get("Name", rec.get("name", ""))
                plate = rec.get("Plate", rec.get("plate", ""))
                end = rec.get("End Date", rec.get("End", ""))
                dep = rec.get("Departure", rec.get("departure", ""))
                arr = rec.get("Arrival", rec.get("arrival", ""))
                staff = rec.get("Staff Name", rec.get("staff name", ""))
                roundtrip = rec.get("Roundtrip", rec.get("roundtrip", ""))
                return_start = rec.get("Return Start", "")
                return_end = rec.get("Return End", "")
                out.append([guid, no, name, plate, start, end, dep, arr, staff, roundtrip, return_start, return_end])
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
            while len(r) < M_COL_RETURN_END:
                r.append("")
            ws.append_row(r, value_input_option="USER_ENTERED")
        # Add roundtrip summary by driver (count Roundtrip == Yes)
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


# ===== New: monthly per-driver roundtrip summary & export (tab + CSV) =====
def count_roundtrips_per_driver_month(start_date: datetime, end_date: datetime) -> Dict[str, int]:
    """
    Count rows in MISSIONS_TAB where Roundtrip == "Yes" and Start Date in [start_date, end_date).
    Returns dict: {driver_name: count}
    """
    counts: Dict[str, int] = {}
    try:
        ws = open_worksheet(MISSIONS_TAB)
        rows = ws.get_all_records()
        for r in rows:
            start = str(r.get("Start Date", r.get("Start", ""))).strip()
            if not start:
                continue
            s_dt = parse_ts(start)
            if not s_dt:
                continue
            if not (start_date <= s_dt < end_date):
                continue
            rt = str(r.get("Roundtrip", r.get("roundtrip", ""))).strip().lower()
            if rt != "yes":
                continue
            name = str(r.get("Name", r.get("name", r.get("Driver", "")))).strip() or "Unknown"
            counts[name] = counts.get(name, 0) + 1
    except Exception:
        logger.exception("Failed to count roundtrips per driver")
    return counts


def write_roundtrip_summary_tab(month_label: str, counts: Dict[str, int]) -> Optional[str]:
    """
    Create (or clear if exists) a tab named Roundtrip_Summary_{month_label} and write header + rows.
    Return created tab name on success.
    """
    tab_name = f"Roundtrip_Summary_{month_label}"
    try:
        gc = get_gspread_client()
        sh = gc.open(GOOGLE_SHEET_NAME)
        # If exists, delete and recreate to ensure fresh content (best-effort)
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
        logger.info("Wrote roundtrip summary tab %s", tab_name)
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
        try:
            mnt_path = os.path.join("/mnt/data", fname)
            with open(mnt_path, "w", newline="", encoding="utf-8") as f2:
                writer2 = csv.writer(f2)
                writer2.writerow(["Driver", "Roundtrip Count (month)"])
                for driver, cnt in sorted(counts.items(), key=lambda x: (-x[1], x[0])):
                    writer2.writerow([driver, cnt])
            logger.info("Wrote CSV to %s and %s", local_path, mnt_path)
        except Exception:
            logger.info("Wrote CSV to %s (couldn't write to /mnt/data)", local_path)
        return local_path
    except Exception:
        logger.exception("Failed to write roundtrip summary CSV")
        return None


# ===== Telegram UI helpers & handlers (no GPS) =====
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


# All CommandHandlers attempt to delete the invoking message to avoid clutter in groups
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

    # show submenus
    if data == "show_start":
        await query.edit_message_text(t(user_lang, "choose_start"), reply_markup=build_plate_keyboard("start"))
        return
    if data == "show_end":
        await query.edit_message_text(t(user_lang, "choose_end"), reply_markup=build_plate_keyboard("end"))
        return
    if data == "menu_full":
        await query.edit_message_text(t(user_lang, "menu"), reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("Start trip (select plate)", callback_data="show_start"),
             InlineKeyboardButton("End trip (select plate)", callback_data="show_end")],
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
        await query.edit_message_text(t(user_lang, "mission_start_prompt_depart"), reply_markup=InlineKeyboardMarkup(kb))
        return

    # mission end selection
    if data.startswith("mission_end_plate|"):
        _, plate = data.split("|", 1)
        context.user_data["pending_mission"] = {"action": "end", "plate": plate}
        kb = [[InlineKeyboardButton("PP", callback_data="mission_arrival|PP"), InlineKeyboardButton("SHV", callback_data="mission_arrival|SHV")]]
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

        # Send a separate staff prompt message and remember its id so we can delete it when user replies.
        try:
            chat_id = update.effective_chat.id
            staff_prompt_msg = await context.bot.send_message(chat_id=chat_id, text=t(user_lang, "mission_start_prompt_staff"))
            context.user_data["last_bot_prompt"] = {"chat_id": staff_prompt_msg.chat_id, "message_id": staff_prompt_msg.message_id}
        except Exception:
            await query.edit_message_text(t(user_lang, "mission_start_prompt_staff"))
            context.user_data.pop("last_bot_prompt", None)
        return

    # after selecting arrival for a mission end
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

        # perform mission end record and possibly merge
        res = end_mission_record(username, plate, arrival)
        if res.get("ok"):
            # notify success
            await query.edit_message_text(t(user_lang, "mission_end_ok", plate=plate, end_date=now_str(), arr=arrival))
            # if merged, compute this driver's monthly roundtrip count using the robust function
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

    # start/end trip selection handlers from keyboard: now record immediately (no GPS)
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
# This deletes the user's reply message and the stored bot prompt message (the "Optional: send staff name..." prompt),
# then records the mission start (if pending_mission exists).
async def location_or_skip(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # delete the user's message (so staff /skip won't remain)
    try:
        if update.effective_message:
            await update.effective_message.delete()
    except Exception:
        pass

    user = update.effective_user
    user_lang = context.user_data.get("lang", DEFAULT_LANG)

    # If there is a stored bot prompt message from earlier, delete it now:
    last_prompt = context.user_data.get("last_bot_prompt")
    if last_prompt:
        try:
            chat_id = last_prompt.get("chat_id")
            msg_id = last_prompt.get("message_id")
            if chat_id and msg_id:
                await context.bot.delete_message(chat_id=chat_id, message_id=msg_id)
        except Exception:
            # ignore deletion failures
            pass
        context.user_data.pop("last_bot_prompt", None)

    # Handle staff-name reply for pending mission start
    pending_mission = context.user_data.get("pending_mission")
    if pending_mission and pending_mission.get("action") == "start":
        text = update.message.text.strip() if update.message.text else ""
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

    # Otherwise ignore
    return


# lang command: delete invoking message and save per-user preference
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


# mission_report: delete invoking message then run; extended to generate roundtrip summary tab + CSV for month
async def mission_report_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        if update.effective_message:
            await update.effective_message.delete()
    except Exception:
        pass

    args = context.args
    user_lang = context.user_data.get("lang", DEFAULT_LANG)
    if not args or len(args) < 2:
        await update.effective_chat.send_message(t(user_lang, "mission_invalid_cmd"))
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
            # After mission report, also produce per-driver roundtrip summary tab & CSV
            counts = count_roundtrips_per_driver_month(start, end)
            tab_name = write_roundtrip_summary_tab(start.strftime("%Y-%m"), counts)
            csv_path = write_roundtrip_summary_csv(start.strftime("%Y-%m"), counts)
            if ok:
                await update.effective_chat.send_message(t(user_lang, "mission_report_month_ok", month=start.strftime("%Y-%m")))
            else:
                await update.effective_chat.send_message("❌ Failed to write mission report.")
            # Notify about summary tab & CSV and attempt to send CSV file back
            if tab_name:
                try:
                    await update.effective_chat.send_message(t(user_lang, "roundtrip_summary_tab_ok", tab_name=tab_name))
                except Exception:
                    pass
            if csv_path:
                try:
                    # send CSV as document
                    with open(csv_path, "rb") as f:
                        await context.bot.send_document(chat_id=update.effective_chat.id, document=f, filename=os.path.basename(csv_path))
                    await update.effective_chat.send_message(t(user_lang, "roundtrip_csv_ok", csv_path=csv_path))
                except Exception:
                    try:
                        await update.effective_chat.send_message(t(user_lang, "roundtrip_csv_ok", csv_path=csv_path))
                    except Exception:
                        pass
        except Exception:
            await update.effective_chat.send_message(t(user_lang, "mission_invalid_cmd"))
    elif mode == "year":
        try:
            y = int(args[1])
            start = datetime(y, 1, 1)
            end = datetime(y + 1, 1, 1)
            rows = mission_rows_for_period(start, end)
            ok = write_mission_report_rows(rows, period_label=str(y))
            if ok:
                await update.effective_chat.send_message(t(user_lang, "mission_report_year_ok", year=str(y)))
            else:
                await update.effective_chat.send_message("❌ Failed to write mission report.")
        except Exception:
            await update.effective_chat.send_message(t(user_lang, "mission_invalid_cmd"))
    else:
        await update.effective_chat.send_message(t(user_lang, "mission_invalid_cmd"))


# Auto keyword listener (group)
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


# Scheduling: daily summary job; if day==1, also generate/send last month's mission report
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
    # send daily vehicle usage summary for yesterday
    yesterday = now.date() - timedelta(days=1)
    date_dt = datetime.combine(yesterday, dtime.min)
    text = write_daily_summary(date_dt)
    try:
        await context.bot.send_message(chat_id=chat_id, text=text)
    except Exception:
        logger.exception("Failed to send daily summary message.")

    # If it's the 1st day of month, also auto-generate last month's mission report
    if now.day == 1:
        try:
            # compute previous month range
            first_of_this_month = datetime(now.year, now.month, 1)
            prev_month_end = first_of_this_month
            prev_month_start = (first_of_this_month - timedelta(days=1)).replace(day=1)
            # generate rows & write report
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


# Register handlers
def register_ui_handlers(application):
    application.add_handler(CommandHandler("menu", menu_command))
    application.add_handler(CommandHandler(["start_trip", "start"], start_trip_command))
    application.add_handler(CommandHandler(["end_trip", "end"], end_trip_command))
    application.add_handler(CommandHandler("lang", lang_command))
    application.add_handler(CommandHandler("mission_start", mission_start_command))
    application.add_handler(CommandHandler("mission_end", mission_end_command))
    application.add_handler(CommandHandler("mission_report", mission_report_command))
    application.add_handler(CallbackQueryHandler(plate_callback))
    # location_or_skip now handles only staff-name replies (/skip and free text)
    application.add_handler(MessageHandler(filters.Regex(r'(?i)^/skip$') | (filters.TEXT & (~filters.COMMAND)), location_or_skip))
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
            logger.info("SUMMARY_CHAT_ID not configured; daily summary (and monthly auto-report) disabled.")
    except Exception:
        logger.exception("Failed to schedule daily summary.")


def main():
    ensure_env()
    if LOCAL_TZ:
        if ZoneInfo:
            try:
                ZoneInfo(LOCAL_TZ)
                logger.info("Using LOCAL_TZ=%s", LOCAL_TZ)
            except Exception:
                logger.info("LOCAL_TZ=%s but failed to initialize ZoneInfo; using system time.", LOCAL_TZ)
        else:
            logger.info("LOCAL_TZ=%s requested but ZoneInfo not available; using system time.", LOCAL_TZ)
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


# ===== One-time migration helper (run manually if needed) =====
def migrate_mixed_sheet(original_tab_name: str):
    """
    Migrate rows from original_tab_name into RECORDS_TAB and MISSIONS_TAB.
    Run once after backing up the sheet.
    """
    gc = get_gspread_client()
    sh = gc.open(GOOGLE_SHEET_NAME)
    try:
        src = sh.worksheet(original_tab_name)
    except Exception:
        logger.error("Original tab not found: %s", original_tab_name)
        return

    all_rows = src.get_all_records()
    records_ws = open_worksheet(RECORDS_TAB)
    missions_ws = open_worksheet(MISSIONS_TAB)

    existing = missions_ws.get_all_values()
    if not existing:
        next_no = 1
    else:
        header = existing[0]
        next_no = len(existing)  # conservative

    trip_count = 0
    mission_count = 0

    for r in all_rows:
        keys_lower = [k.lower() for k in r.keys()]
        is_mission = any(kw in keys_lower for kw in ("departure", "arrival", "staff", "roundtrip", "start date", "end date"))
        if is_mission:
            name = r.get("Name") or r.get("Driver") or ""
            plate = r.get("Plate") or ""
            start = r.get("Start Date") or r.get("Start") or ""
            end = r.get("End Date") or r.get("End") or ""
            dep = r.get("Departure") or ""
            arr = r.get("Arrival") or r.get("Arr") or ""
            staff = r.get("Staff Name") or r.get("Staff") or ""
            rt = r.get("Roundtrip") or ""
            return_start = r.get("Return Start") or ""
            return_end = r.get("Return End") or ""
            guid = str(uuid.uuid4())
            mission_row = [guid, next_no, name, plate, start, end, dep, arr, staff, rt, return_start, return_end]
            missions_ws.append_row(mission_row, value_input_option="USER_ENTERED")
            next_no += 1
            mission_count += 1
        else:
            date = r.get("Date") or r.get("date") or ""
            driver = r.get("Driver") or ""
            plate = r.get("Plate") or ""
            start = r.get("Start") or ""
            end = r.get("End") or ""
            duration = r.get("Duration") or ""
            trip_row = [date, driver, plate, start, end, duration]
            records_ws.append_row(trip_row, value_input_option="USER_ENTERED")
            trip_count += 1

    logger.info("Migration complete: %d trips -> %s, %d missions -> %s", trip_count, RECORDS_TAB, mission_count, MISSIONS_TAB)
