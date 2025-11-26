#!/usr/bin/env python3
"""
driver-bot enhanced version with missions and Roundtrip marking

Key additions:
- Missions sheet includes new column "Roundtrip" (9th column).
- When mission end is recorded, if Start Date and End Date are the same calendar day (same tz),
  the script writes "Yes" into Roundtrip, otherwise "No".
- Mission report (month/year) writes Roundtrip column and appends a summary block that counts
  Roundtrip occurrences per plate in that period.

Save as driver_bot.py and run (ensure env vars like BOT_TOKEN and Google creds are set).
"""

import os
import json
import base64
import logging
from datetime import datetime, timedelta, time as dtime
from typing import Optional, Dict, List, Any, Tuple

import gspread
from oauth2client.service_account import ServiceAccountCredentials

# zoneinfo support
try:
    from zoneinfo import ZoneInfo  # type: ignore
except Exception:
    try:
        from backports.zoneinfo import ZoneInfo  # type: ignore
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
    Location,
)
from telegram.constants import ChatAction
from telegram.ext import (
    ApplicationBuilder,
    CommandHandler,
    CallbackQueryHandler,
    MessageHandler,
    filters,
    ContextTypes,
    ConversationHandler,
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
GOOGLE_SHEET_TAB = os.getenv("GOOGLE_SHEET_TAB", "")  # optional
# Local TZ
_env_tz = os.getenv("LOCAL_TZ")
if _env_tz is None:
    LOCAL_TZ = "Asia/Phnom_Penh"
else:
    LOCAL_TZ = _env_tz.strip() or None

if LOCAL_TZ and ZoneInfo is None:
    logger.warning("LOCAL_TZ set but zoneinfo/backports.zoneinfo not available; falling back to system time.")

PLATES = [p.strip() for p in PLATE_LIST.split(",") if p.strip()]

# Driver map env or sheet
DRIVER_PLATE_MAP_JSON = os.getenv("DRIVER_PLATE_MAP", "").strip() or None

# Summary / scheduling
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

# Missions tabs
MISSIONS_TAB = os.getenv("MISSIONS_TAB", "Missions")  # where each mission row is stored
MISSIONS_REPORT_TAB = os.getenv("MISSIONS_REPORT_TAB", "Missions_Report")  # where monthly/yearly reports are appended

# Column mapping for records (1-indexed)
COL_DATE = 1
COL_DRIVER = 2
COL_PLATE = 3
COL_START = 4
COL_END = 5
COL_DURATION = 6
COL_START_LAT = 7
COL_START_LON = 8
COL_END_LAT = 9
COL_END_LON = 10

# Missions columns (1-indexed in sheet)
# We will use layout:
# 1: No., 2: Name, 3: Plate, 4: Start Date, 5: End Date, 6: Departure, 7: Arrival, 8: Staff Name, 9: Roundtrip

# Time formats
TS_FMT = "%Y-%m-%d %H:%M:%S"
DATE_FMT = "%Y-%m-%d"

# Google scopes
SCOPES = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]

# Conversation states
ASK_LOC_FOR = 1

# Translations (trimmed to essentials for brevity)
TR = {
    "en": {
        "menu": "Driver Bot Menu — tap a button to perform an action:",
        "choose_start": "Please choose the vehicle plate to START trip:",
        "choose_end": "Please choose the vehicle plate to END trip:",
        "prompt_location": "If you want to record GPS location, please send location now (or type /skip).",
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
        "skip_loc": "Location skipped.",
        "prompt_send_loc": "Please send your location now or /skip.",
        # missions
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
        "mission_report_roundtrip_summary": "Roundtrip counts (period):",
    },
    "km": {
        # Khmer strings shortened for example
        "menu": "មិនីវរ​បូត — សូមចុចប៊ូតុងដើម្បីអនុវត្ត៖",
        "choose_start": "សូមជ្រើសលេខផ្ទះបណ្ដោះដើម្បីចាប់ផ្តើមដំណើរ:",
        "choose_end": "សូមជ្រើសលេខផ្ទះបណ្ដោះដើម្បីបញ្ចប់ដំណើរ:",
        "prompt_location": "បើចង់កត់ត្រាទីតាំង GPS សូមផ្ញើទីតាំងឥឡូវ (ឬ /skip).",
        "start_ok": "✅ ចាប់ផ្តើមដំណើរសម្រាប់ {plate} (អ្នកបើក: {driver}). {msg}",
        "end_ok": "✅ បញ្ចប់ដំណើរសម្រាប់ {plate} (អ្នកបើក: {driver}). {msg}",
        "not_allowed": "❌ អ្នកមិនមានសិទ្ធិប្រើលើរថយន្តនេះ: {plate}.",
        "invalid_sel": "ជម្រើសមិនត្រឹមត្រូវ។",
        "help": "ជំនួយ៖ ចុច Start trip ឬ End trip ហើយជ្រើស plate.",
        "no_bot_token": "សូមកំណត់ BOT_TOKEN variable.",
        "setup_complete": "ចំណុចនេះបានរៀបចំរួច — បញ្ជាថានេះបានកំណត់សម្រាប់ក្រុម។",
        "summary_subject": "សេចក្តីសង្ខេបការប្រើរថយន្តរៀងរាល់ថ្ងៃ {date}:",
        "summary_row": "{plate}: {hours}ស {minutes}នាទីសរុប",
        "no_records": "មិនមានកំណត់ត្រាសម្រាប់ {date}.",
        "lang_set": "បានកំណត់ភាសា​ជា {lang}.",
        "skip_loc": "បានរំលងទីតាំង។",
        "prompt_send_loc": "សូមផ្ញើទីតាំងឥឡូវ ឬ /skip.",
        # missions
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
        "mission_report_roundtrip_summary": "ចំនួន往返 (រយៈពេល):",
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


def open_worksheet(tab: str = ""):
    gc = get_gspread_client()
    sh = gc.open(GOOGLE_SHEET_NAME)
    if tab:
        try:
            return sh.worksheet(tab)
        except Exception:
            return sh.sheet1
    else:
        if GOOGLE_SHEET_TAB:
            try:
                return sh.worksheet(GOOGLE_SHEET_TAB)
            except Exception:
                return sh.sheet1
        return sh.sheet1


# ===== Driver map =====
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


# ===== Record functions (with GPS) =====
def record_start_trip(driver: str, plate: str, lat: Optional[float] = None, lon: Optional[float] = None) -> dict:
    ws = open_worksheet(RECORDS_TAB)
    start_ts = now_str()
    row = [today_date_str(), driver, plate, start_ts, "", "", "", "", "", ""]
    if lat is not None and lon is not None:
        row[COL_START_LAT - 1] = str(lat)
        row[COL_START_LON - 1] = str(lon)
    try:
        ws.append_row(row, value_input_option="USER_ENTERED")
        logger.info("Recorded start trip: %s %s %s", driver, plate, start_ts)
        return {"ok": True, "message": f"Start time recorded for {plate} at {start_ts}"}
    except Exception as e:
        logger.exception("Failed to append start trip row")
        return {"ok": False, "message": "Failed to write start trip to sheet: " + str(e)}


def record_end_trip(driver: str, plate: str, lat: Optional[float] = None, lon: Optional[float] = None) -> dict:
    ws = open_worksheet(RECORDS_TAB)
    try:
        records = ws.get_all_records()
        for idx in range(len(records) - 1, -1, -1):
            rec = records[idx]
            rec_plate = str(rec.get("Plate No.", rec.get("Plate", rec.get("Plate No", "")))).strip()
            end_val = str(rec.get("End date&time", rec.get("End", ""))).strip()
            start_val = str(rec.get("Start date&time", rec.get("Start", ""))).strip()
            if rec_plate == plate and (end_val == "" or end_val is None):
                row_number = idx + 2
                end_ts = now_str()
                duration_text = compute_duration(start_val, end_ts) if start_val else ""
                ws.update_cell(row_number, COL_END, end_ts)
                ws.update_cell(row_number, COL_DURATION, duration_text)
                if lat is not None and lon is not None:
                    ws.update_cell(row_number, COL_END_LAT, str(lat))
                    ws.update_cell(row_number, COL_END_LON, str(lon))
                logger.info("Recorded end trip for %s row %d", plate, row_number)
                return {"ok": True, "message": f"End time recorded for {plate} at {end_ts} (duration {duration_text})"}
        # no matching start found -> append end-only row
        end_ts = now_str()
        row = [today_date_str(), driver, plate, "", end_ts, "", "", "", "", ""]
        if lat is not None and lon is not None:
            row[COL_END_LAT - 1] = str(lat)
            row[COL_END_LON - 1] = str(lon)
        ws.append_row(row, value_input_option="USER_ENTERED")
        logger.info("No open start found; appended end-only row for %s", plate)
        return {"ok": True, "message": f"End time recorded (no matching start found) for {plate} at {end_ts}"}
    except Exception as e:
        logger.exception("Failed to update end trip")
        return {"ok": False, "message": "Failed to write end trip to sheet: " + str(e)}


# ===== Aggregation & Summary writing =====
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


def write_weekly_summary(week_start_dt: datetime) -> str:
    start = datetime.combine(week_start_dt.date(), dtime.min)
    end = start + timedelta(days=7)
    totals = aggregate_for_period(start, end)
    if not totals:
        return t(DEFAULT_LANG, "no_records", date=start.strftime(DATE_FMT))
    lines = []
    for plate, minutes in sorted(totals.items()):
        h, m = minutes_to_h_m(minutes)
        lines.append(t(DEFAULT_LANG, "summary_row", plate=plate, hours=h, minutes=m))
    try:
        ws = open_worksheet(SUMMARY_TAB)
        row = [start.strftime(DATE_FMT), "weekly", json.dumps(totals), "\n".join(lines)]
        ws.append_row(row, value_input_option="USER_ENTERED")
    except Exception:
        logger.exception("Failed to write weekly summary to sheet.")
    header = t(DEFAULT_LANG, "summary_subject", date=start.strftime(DATE_FMT))
    return header + "\n" + "\n".join(lines)


# ===== Missions: helpers (with Roundtrip handling) =====
def _missions_next_no(ws) -> int:
    """Return next No. for missions sheet by counting existing rows (excluding header if present)."""
    try:
        all_vals = ws.get_all_values()
        if not all_vals:
            return 1
        header = all_vals[0]
        if any(h.lower().strip().startswith("no") or h.lower().strip().startswith("name") for h in header):
            # next no = number of data rows + 0 (header present)
            data_rows = len(all_vals) - 1
            return data_rows + 1
        else:
            # no header, treat all rows as data
            return len(all_vals) + 1
    except Exception:
        return 1


def start_mission_record(driver: str, plate: str, departure: str, staff_name: str = "") -> dict:
    """Append mission start row with Start Date and Departure. End Date/Arrival blank until end recorded."""
    ws = open_worksheet(MISSIONS_TAB)
    start_ts = now_str()
    try:
        next_no = _missions_next_no(ws)
        # include Roundtrip blank initially
        row = [next_no, driver, plate, start_ts, "", departure, "", staff_name, ""]
        ws.append_row(row, value_input_option="USER_ENTERED")
        logger.info("Mission start recorded: %s %s %s %s", next_no, driver, plate, departure)
        return {"ok": True, "no": next_no, "message": f"Mission start recorded for {plate} at {start_ts}"}
    except Exception as e:
        logger.exception("Failed to append mission start")
        return {"ok": False, "message": "Failed to write mission start to sheet: " + str(e)}


def end_mission_record(driver: str, plate: str, arrival: str) -> dict:
    """
    Find last mission row for driver+plate with empty End Date and fill End Date, Arrival and Roundtrip.
    If not found, return not found.
    """
    ws = open_worksheet(MISSIONS_TAB)
    try:
        records = ws.get_all_records()
        for idx in range(len(records) - 1, -1, -1):
            rec = records[idx]
            rec_plate = str(rec.get("Plate", rec.get("plate", rec.get("Plate No", "")))).strip()
            rec_name = str(rec.get("Name", rec.get("Driver", rec.get("name", "")))).strip()
            end_val = str(rec.get("End Date", rec.get("End", ""))).strip()
            start_val = str(rec.get("Start Date", rec.get("Start", ""))).strip()
            if rec_plate == plate and (end_val == "" or end_val is None) and (rec_name == driver):
                row_number = idx + 2  # account for header row
                end_ts = now_str()
                # write End Date (col 5), Arrival (col 7), Roundtrip (col 9)
                ws.update_cell(row_number, 5, end_ts)
                ws.update_cell(row_number, 7, arrival)
                # determine roundtrip: same calendar date?
                rt_value = "No"
                s_dt = parse_ts(start_val) if start_val else None
                e_dt = parse_ts(end_ts)
                if s_dt and e_dt:
                    # compare date in local tz (we used local wall-clock times)
                    if s_dt.date() == e_dt.date():
                        rt_value = "Yes"
                # Write Roundtrip into 9th column
                try:
                    ws.update_cell(row_number, 9, rt_value)
                except Exception:
                    # If sheet shorter, append blank cells to reach column; simpler: read row and rewrite full row:
                    try:
                        existing = ws.row_values(row_number)
                        # ensure length
                        while len(existing) < 9:
                            existing.append("")
                        existing[4] = end_ts  # index 4 => col 5
                        existing[6] = arrival  # index 6 => col 7
                        existing[8] = rt_value  # index 8 => col 9
                        ws.delete_row(row_number)
                        ws.insert_row(existing, row_number)
                    except Exception:
                        logger.exception("Fallback write for mission end failed.")
                logger.info("Mission end updated for %s row %d (Roundtrip=%s)", plate, row_number, rt_value)
                return {"ok": True, "message": f"Mission end recorded for {plate} at {end_ts}", "roundtrip": rt_value}
        return {"ok": False, "message": "No open mission found"}
    except Exception as e:
        logger.exception("Failed to update mission end")
        return {"ok": False, "message": "Failed to write mission end to sheet: " + str(e)}


def mission_rows_for_period(start_date: datetime, end_date: datetime) -> List[List[Any]]:
    """Return mission rows whose start date falls within [start_date, end_date). Each row as list of values including Roundtrip."""
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
                no = rec.get("No", rec.get("no", ""))
                name = rec.get("Name", rec.get("name", ""))
                plate = rec.get("Plate", rec.get("plate", ""))
                end = rec.get("End Date", rec.get("End", ""))
                dep = rec.get("Departure", rec.get("departure", ""))
                arr = rec.get("Arrival", rec.get("arrival", ""))
                staff = rec.get("Staff Name", rec.get("staff name", ""))
                roundtrip = rec.get("Roundtrip", rec.get("roundtrip", ""))
                out.append([no, name, plate, start, end, dep, arr, staff, roundtrip])
        return out
    except Exception:
        logger.exception("Failed to fetch mission rows")
        return []


def write_mission_report_rows(rows: List[List[Any]], period_label: str) -> bool:
    """Append mission rows into MISSIONS_REPORT_TAB with a header row indicating period_label.
       Also append a summary of Roundtrip counts per plate at the end of this report block.
    """
    try:
        ws = open_worksheet(MISSIONS_REPORT_TAB)
        # Append header for this report
        ws.append_row([f"Report: {period_label}"], value_input_option="USER_ENTERED")
        # Append column headers including Roundtrip
        ws.append_row(["No.", "Name", "Plate", "Start Date", "End Date", "Departure", "Arrival", "Staff Name", "Roundtrip"], value_input_option="USER_ENTERED")
        # Append data rows
        for r in rows:
            # ensure length 9
            while len(r) < 9:
                r.append("")
            ws.append_row(r, value_input_option="USER_ENTERED")
        # compute roundtrip summary counts per plate
        rt_counts: Dict[str, int] = {}
        for r in rows:
            plate = str(r[2]) if len(r) > 2 else ""
            roundtrip = str(r[8]).strip().lower() if len(r) > 8 else ""
            if plate:
                if roundtrip == "yes":
                    rt_counts[plate] = rt_counts.get(plate, 0) + 1
        # append a small summary block
        ws.append_row(["Roundtrip Summary:"], value_input_option="USER_ENTERED")
        if rt_counts:
            ws.append_row(["Plate", "Roundtrip Count"], value_input_option="USER_ENTERED")
            for plate, cnt in sorted(rt_counts.items()):
                ws.append_row([plate, cnt], value_input_option="USER_ENTERED")
        else:
            ws.append_row(["No roundtrips found in this period."], value_input_option="USER_ENTERED")
        return True
    except Exception:
        logger.exception("Failed to write mission report to sheet.")
        return False


# ===== Telegram UI helpers and command handlers (including mission commands) =====
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
        if update.effective_message:
            await update.effective_message.delete()
    except Exception:
        pass
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


# Mission start/end commands and callbacks (similar to previous script)
DEPARTURE_CHOICES = ["PP", "SHV"]
ARRIVAL_CHOICES = ["PP", "SHV"]


async def mission_start_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    user_lang = context.user_data.get("lang", DEFAULT_LANG)
    driver_map = get_driver_map()
    allowed = None
    if user and user.username and driver_map.get(user.username):
        allowed = driver_map.get(user.username)
    await update.effective_chat.send_message(t(user_lang, "mission_start_prompt_plate"), reply_markup=build_plate_keyboard("mission_start_plate", allowed_plates=allowed))


async def mission_end_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
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

    # existing actions for trips
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

    # mission flow callbacks
    if data.startswith("mission_start_plate|"):
        _, plate = data.split("|", 1)
        context.user_data["pending_mission"] = {"action": "start", "plate": plate}
        kb = [[InlineKeyboardButton("PP", callback_data="mission_depart|PP"), InlineKeyboardButton("SHV", callback_data="mission_depart|SHV")]]
        await query.edit_message_text(t(user_lang, "mission_start_prompt_depart"), reply_markup=InlineKeyboardMarkup(kb))
        return

    if data.startswith("mission_end_plate|"):
        _, plate = data.split("|", 1)
        context.user_data["pending_mission"] = {"action": "end", "plate": plate}
        kb = [[InlineKeyboardButton("PP", callback_data="mission_arrival|PP"), InlineKeyboardButton("SHV", callback_data="mission_arrival|SHV")]]
        await query.edit_message_text(t(user_lang, "mission_end_prompt_arrival"), reply_markup=InlineKeyboardMarkup(kb))
        return

    if data.startswith("mission_depart|"):
        _, dep = data.split("|", 1)
        pending = context.user_data.get("pending_mission")
        if not pending or pending.get("action") != "start":
            await query.edit_message_text(t(user_lang, "invalid_sel"))
            return
        pending["departure"] = dep
        context.user_data["pending_mission"] = pending
        await query.edit_message_text(t(user_lang, "mission_start_prompt_staff"))
        return

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
        else:
            await query.edit_message_text(t(user_lang, "mission_no_open", plate=plate))
        context.user_data.pop("pending_mission", None)
        return

    # start/end trip selection handlers
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
            context.user_data["pending_action"] = ("start", plate)
            await query.edit_message_text(t(user_lang, "prompt_location"))
            return
        elif action == "end":
            context.user_data["pending_action"] = ("end", plate)
            await query.edit_message_text(t(user_lang, "prompt_location"))
            return

    await query.edit_message_text(t(user_lang, "invalid_sel"))


async def location_or_skip(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    user_lang = context.user_data.get("lang", DEFAULT_LANG)
    pending = context.user_data.get("pending_action")
    if not pending:
        pending_mission = context.user_data.get("pending_mission")
        if pending_mission and pending_mission.get("action") == "start":
            text = update.message.text.strip() if update.message.text else ""
            if text and text.lower().strip() != "/skip":
                staff = text
            else:
                staff = ""
            plate = pending_mission.get("plate")
            departure = pending_mission.get("departure")
            username = user.username or user.full_name
            driver_map = get_driver_map()
            allowed = driver_map.get(user.username, []) if user and user.username else []
            if allowed and plate not in allowed:
                await update.message.reply_text(t(user_lang, "not_allowed", plate=plate))
                context.user_data.pop("pending_mission", None)
                return
            res = start_mission_record(username, plate, departure, staff_name=staff)
            if res.get("ok"):
                await update.message.reply_text(t(user_lang, "mission_start_ok", plate=plate, start_date=now_str(), dep=departure))
            else:
                await update.message.reply_text("❌ " + res.get("message", ""))
            context.user_data.pop("pending_mission", None)
            return
        return

    action, plate = pending
    lat = lon = None
    if update.message.location:
        loc: Location = update.message.location
        lat = loc.latitude
        lon = loc.longitude
        logger.info("Received location from %s: %s,%s", user.username, lat, lon)
    elif update.message.text and update.message.text.strip().lower() == "/skip":
        await update.message.reply_text(t(user_lang, "skip_loc"))
    else:
        await update.message.reply_text(t(user_lang, "prompt_send_loc"))
        return

    if action == "start":
        res = record_start_trip(user.username or user.full_name, plate, lat=lat, lon=lon)
        if res["ok"]:
            await update.effective_chat.send_message(t(user_lang, "start_ok", plate=plate, driver=user.username or user.full_name, msg=res["message"]))
        else:
            await update.effective_chat.send_message("❌ " + res["message"])
    elif action == "end":
        res = record_end_trip(user.username or user.full_name, plate, lat=lat, lon=lon)
        if res["ok"]:
            await update.effective_chat.send_message(t(user_lang, "end_ok", plate=plate, driver=user.username or user.full_name, msg=res["message"]))
        else:
            await update.effective_chat.send_message("❌ " + res["message"])
    context.user_data.pop("pending_action", None)


async def lang_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    args = context.args
    if not args:
        await update.message.reply_text("Usage: /lang en|km")
        return
    lang = args[0].lower()
    if lang not in SUPPORTED_LANGS:
        await update.message.reply_text("Supported langs: en, km")
        return
    context.user_data["lang"] = lang
    await update.message.reply_text(t(lang, "lang_set", lang=lang))


# Mission report command
async def mission_report_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Usage:
       /mission_report month YYYY-MM
       /mission_report year YYYY
    """
    args = context.args
    user_lang = context.user_data.get("lang", DEFAULT_LANG)
    if not args or len(args) < 2:
        await update.message.reply_text(t(user_lang, "mission_invalid_cmd"))
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
            if ok:
                await update.message.reply_text(t(user_lang, "mission_report_month_ok", month=start.strftime("%Y-%m")))
            else:
                await update.message.reply_text("❌ Failed to write mission report.")
        except Exception:
            await update.message.reply_text(t(user_lang, "mission_invalid_cmd"))
    elif mode == "year":
        try:
            y = int(args[1])
            start = datetime(y, 1, 1)
            end = datetime(y + 1, 1, 1)
            rows = mission_rows_for_period(start, end)
            ok = write_mission_report_rows(rows, period_label=str(y))
            if ok:
                await update.message.reply_text(t(user_lang, "mission_report_year_ok", year=str(y)))
            else:
                await update.message.reply_text("❌ Failed to write mission report.")
        except Exception:
            await update.message.reply_text(t(user_lang, "mission_invalid_cmd"))
    else:
        await update.message.reply_text(t(user_lang, "mission_invalid_cmd"))


# Auto keyword listener
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


# Scheduling jobs: daily summary
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


# Register handlers & startup
def register_ui_handlers(application):
    application.add_handler(CommandHandler("menu", menu_command))
    application.add_handler(CommandHandler(["start_trip", "start"], start_trip_command))
    application.add_handler(CommandHandler(["end_trip", "end"], end_trip_command))
    application.add_handler(CommandHandler("lang", lang_command))
    application.add_handler(CommandHandler("mission_start", mission_start_command))
    application.add_handler(CommandHandler("mission_end", mission_end_command))
    application.add_handler(CommandHandler("mission_report", mission_report_command))
    application.add_handler(CallbackQueryHandler(plate_callback))
    application.add_handler(MessageHandler(filters.LOCATION | filters.Regex(r'(?i)^/skip$') | filters.TEXT, location_or_skip))
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
            logger.info("SUMMARY_CHAT_ID not configured; daily summary disabled.")
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
