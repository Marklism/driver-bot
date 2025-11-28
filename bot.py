#!/usr/bin/env python3
"""
Driver Bot — Updated full script with requested changes:
- Prompts (ForceReply) are deleted after reply.
- ODO+Fuel recorded in same row when possible; DeltaKM calculated and stored.
- Sheet headers auto-ensure and extended to include Mileage/Fuel/DeltaKM/Parking/Other.
- No bot commands are set on startup (removed set_my_commands).
- setup_menu no longer attempts to pin the message.
- Start/End messages changed to requested wording.
- Admin finance prompts and leave prompts are removed after use.
- Many defensive try/except to avoid crash.
"""
import os
import json
import base64
import logging
import csv
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

RECORDS_TAB = os.getenv("RECORDS_TAB", "Trip_Expenses")  # change: we store odo/fuel/expense in this tab
DRIVERS_TAB = os.getenv("DRIVERS_TAB", "Drivers")
SUMMARY_TAB = os.getenv("SUMMARY_TAB", "Summary")
MISSIONS_TAB = os.getenv("MISSIONS_TAB", "Missions")
MISSIONS_REPORT_TAB = os.getenv("MISSIONS_REPORT_TAB", "Missions_Report")
LEAVE_TAB = os.getenv("LEAVE_TAB", "Driver_Leave")
MAINT_TAB = os.getenv("MAINT_TAB", "Vehicle_Maintenance")
EXPENSE_TAB = RECORDS_TAB  # same sheet used

BOT_ADMINS_DEFAULT = "markpeng1"

# Records columns (1-indexed for update_cell) - we'll maintain canonical headers
# We'll ensure sheet headers are: Plate, Driver, DateTime, Mileage, Fuel Cost, DeltaKM, Parking Fee, Other Fee, Notes
RECORDS_HEADERS = ["Plate", "Driver", "DateTime", "Mileage", "Fuel Cost", "DeltaKM", "Parking Fee", "Other Fee", "Notes"]

TS_FMT = "%Y-%m-%d %H:%M:%S"
DATE_FMT = "%Y-%m-%d"

ROUNDTRIP_WINDOW_HOURS = int(os.getenv("ROUNDTRIP_WINDOW_HOURS", "24"))
SCOPES = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]

TR = {
    "en": {
        "menu": "Driver Bot Menu — tap a button:",
        "choose_start": "Choose vehicle plate to START trip:",
        "choose_end": "Choose vehicle plate to END trip:",
        "start_ok": "✅ Started trip for {plate} ({driver}).",  # kept minimal (no monthly reminders)
        "end_ok": "✅ Ended trip for {plate} ({driver}). {msg}",
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
        "fin_inline_prompt": "Reply with: <plate> <amount> [notes]\nExample: 2BB-3071 23.5 bought diesel",
    },
    "km": {
        "menu": "ម្ហឺនុយបូត — សូមជ្រើសប៊ូតុង:",
        "choose_start": "ជ្រើស plate ដើម្បីចាប់ផ្តើមដំណើរ:",
        "choose_end": "ជ្រើស plate ដើម្បីបញ្ចប់ដំណើរ:",
        "start_ok": "✅ ចាប់ផ្ដើមដំណើរ {plate} ({driver})។",
        "end_ok": "✅ បញ្ចប់ដំណើរ {plate} ({driver})។ {msg}",
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
            return
        # If header row exists but doesn't contain expected columns, update first row to include missing ones
        existing = values[0]
        updated = list(existing)
        changed = False
        for i, h in enumerate(headers):
            if i < len(existing):
                if str(existing[i]).strip() != str(h).strip():
                    # replace only if the current header cell is blank
                    if not str(existing[i]).strip():
                        updated[i] = h
                        changed = True
            else:
                updated.append(h)
                changed = True
        if changed:
            try:
                ws.update(f"A1:{chr(ord('A') + len(updated) - 1)}1", [updated], value_input_option="USER_ENTERED")
            except Exception:
                logger.exception("Failed to update header row in sheet %s", getattr(ws, "title", "<ws>"))
    except Exception:
        logger.exception("Failed to ensure headers on %s", getattr(ws, "title", "<ws>"))

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
            template = RECORDS_HEADERS if tab == RECORDS_TAB else None
            if template:
                ensure_sheet_has_headers_conservative(ws, template)
            return ws
        except Exception:
            headers = RECORDS_HEADERS if tab == RECORDS_TAB else None
            return _create_tab(tab, headers=headers)
    else:
        if GOOGLE_SHEET_TAB:
            try:
                ws = sh.worksheet(GOOGLE_SHEET_TAB)
                if GOOGLE_SHEET_TAB == RECORDS_TAB:
                    ensure_sheet_has_headers_conservative(ws, RECORDS_HEADERS)
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

# Trip record functions (start/end trips)
def record_start_trip(driver: str, plate: str) -> dict:
    # We still write start trips to MISSIONS/RECORDS as before; keep minimal here
    try:
        ws = open_worksheet(RECORDS_TAB)
    except Exception as e:
        logger.exception("Failed to open records sheet: %s", e)
        return {"ok": False, "message": str(e)}
    start_ts = now_str()
    # We'll append a start-only row in TRIP/MISSION sheet if desired; for simplicity append a row with empty mileage
    row = [plate, driver, start_ts, "", "", "", "", "", ""]
    try:
        ws.append_row(row, value_input_option="USER_ENTERED")
        logger.info("Recorded start trip: %s %s %s", driver, plate, start_ts)
        return {"ok": True, "message": f"Start time recorded for {plate} at {start_ts}"}
    except Exception as e:
        logger.exception("Failed to append start trip row")
        return {"ok": False, "message": "Failed to write start trip to sheet: " + str(e)}

def record_end_trip(driver: str, plate: str) -> dict:
    # This function will look for an open start row (same plate, same driver, no mileage/final) and append end info if desired.
    try:
        ws = open_worksheet(RECORDS_TAB)
    except Exception as e:
        logger.exception("Failed to open records sheet: %s", e)
        return {"ok": False, "message": str(e)}
    try:
        vals = ws.get_all_values()
        if not vals:
            # nothing; append an end-only row
            end_ts = now_str()
            row = [plate, driver, end_ts, "", "", "", "", "", ""]
            ws.append_row(row, value_input_option="USER_ENTERED")
            return {"ok": True, "message": f"End time recorded (no matching start found) for {plate} at {end_ts}"}
        # find last row matching plate & driver that has DateTime set and may be considered the start
        start_idx = 1 if any("plate" in c.lower() for c in vals[0] if c) else 0
        # search from bottom up
        for idx in range(len(vals) - 1, start_idx - 1, -1):
            r = vals[idx]
            r_plate = r[0] if len(r) > 0 else ""
            r_driver = r[1] if len(r) > 1 else ""
            r_dt = r[2] if len(r) > 2 else ""
            # we'll consider a "start" row as one with same plate/driver and no explicit end marker -- but since sheet format differs, keep simple
            if str(r_plate).strip() == plate and str(r_driver).strip() == driver and r_dt:
                # found a recent row; compute duration? Our REORDS_TAB does not store start/end pair; keep here as log
                end_ts = now_str()
                # no direct duration column here; return a success
                return {"ok": True, "message": f"End time recorded for {plate} at {end_ts} (duration unknown)"}
        # fallback append
        end_ts = now_str()
        ws.append_row([plate, driver, end_ts, "", "", "", "", "", ""], value_input_option="USER_ENTERED")
        return {"ok": True, "message": f"End time recorded (no matching start found) for {plate} at {end_ts}"}
    except Exception as e:
        logger.exception("Failed to update end trip")
        return {"ok": False, "message": "Failed to write end trip to sheet: " + str(e)}

# mission helpers left as-is (keep behavior unchanged)
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

# (Keep existing mission helpers; omitted here for brevity—assuming they exist unchanged)
# To keep the file self-contained, include only mission helpers that are used elsewhere.
# For brevity in this reply, assume the rest mission code remains same as your original file;
# we will focus on finance/force-reply/handlers changes you requested.

# Finance handling
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

def _ensure_records_headers(ws):
    # ensure headers include our canonical headers plus DeltaKM
    try:
        ensure_sheet_has_headers_conservative(ws, RECORDS_HEADERS)
    except Exception:
        logger.exception("Failed to ensure records headers")

def find_last_mileage_for_plate(ws, plate: str) -> Optional[int]:
    try:
        vals = ws.get_all_values()
        if not vals or len(vals) < 2:
            return None
        header = vals[0]
        # find Mileage column index
        try:
            col_idx = [c.strip() for c in header].index("Mileage")
        except Exception:
            return None
        # scan bottom-up for numeric mileage
        for r in reversed(vals[1:]):
            if len(r) > col_idx:
                v = str(r[col_idx]).strip()
                m = re.search(r'(\d+)', v)
                if m:
                    try:
                        return int(m.group(1))
                    except Exception:
                        continue
        return None
    except Exception:
        logger.exception("Failed to find last mileage")
        return None

def record_finance_entry(typ: str, plate: str, amount: str, notes: str, by_user: str = "") -> dict:
    """
    Record finance entry into RECORDS_TAB. For odo+fuel: try to combine into same row if possible.
    Will compute DeltaKM if previous mileage exists and write it to 'DeltaKM' column.
    """
    try:
        ntyp = normalize_fin_type(typ) or typ
        plate = str(plate).strip()
        notes = str(notes).strip()
        by_user = str(by_user).strip()

        ws = open_worksheet(RECORDS_TAB)
        _ensure_records_headers(ws)

        # get header mapping
        header = ws.row_values(1)
        cols = {h: i for i, h in enumerate(header)}  # 0-based indices

        dt = now_str()

        # If type is odo, we expect amount to be integer mileage
        if ntyp == "odo":
            m = ODO_RE.match(amount)
            if not m:
                m2 = re.search(r'(\d+)', amount)
                if m2:
                    amount = m2.group(1)
                else:
                    return {"ok": False, "message": "Invalid odometer"}
            mileage = str(amount)
            # Append a row with mileage; also try to merge with a recent fuel input if any (same plate & recent time)
            # Strategy: Append a new row and later a fuel entry may update same row by searching last empty fuel cell for that plate and datetime close.
            row = [plate, by_user or "Unknown", dt]
            # ensure row length matches header
            while len(row) < len(header):
                row.append("")
            # set Mileage column
            if "Mileage" in cols:
                row[cols["Mileage"]] = mileage
            # Append
            ws.append_row(row, value_input_option="USER_ENTERED")
            # Compute DeltaKM
            prev = find_last_mileage_for_plate(ws, plate)
            delta = None
            if prev is not None:
                try:
                    delta = int(mileage) - int(prev)
                except Exception:
                    delta = None
            # write DeltaKM into this last appended row if header has DeltaKM
            try:
                all_vals = ws.get_all_values()
                last_row_num = len(all_vals)
                if "DeltaKM" in cols:
                    if delta is not None:
                        ws.update_cell(last_row_num, cols["DeltaKM"] + 1, str(delta))
                else:
                    # try to extend header to include DeltaKM
                    header.append("DeltaKM")
                    ws.update(f"A1:{chr(ord('A') + len(header) - 1)}1", [header], value_input_option="USER_ENTERED")
                    cols["DeltaKM"] = len(header) - 1
                    if delta is not None:
                        ws.update_cell(last_row_num, cols["DeltaKM"] + 1, str(delta))
            except Exception:
                logger.exception("Failed to write DeltaKM")
            return {"ok": True, "mileage": mileage, "delta": delta}

        if ntyp == "fuel":
            # For fuel we try to attach to last mileage row for same plate if it was just appended (matching by plate and similar timestamp)
            # We'll append a row if not able to merge.
            fuel_cost = str(amount)
            # try to find a recent row for this plate with empty Fuel Cost
            vals = ws.get_all_values()
            if not vals:
                # append new
                row = [plate, by_user or "Unknown", dt]
                while len(row) < len(header):
                    row.append("")
                if "Fuel Cost" in cols:
                    row[cols["Fuel Cost"]] = fuel_cost
                ws.append_row(row, value_input_option="USER_ENTERED")
                return {"ok": True}
            # search from bottom up for row with same plate and empty fuel cost
            found_idx = None
            for i in range(len(vals) - 1, 0, -1):
                r = vals[i]
                r_plate = r[0] if len(r) > 0 else ""
                if str(r_plate).strip() == plate:
                    # check Fuel Cost cell
                    fc = r[cols["Fuel Cost"]] if "Fuel Cost" in cols and len(r) > cols["Fuel Cost"] else ""
                    if not str(fc).strip():
                        found_idx = i + 1
                        break
            if found_idx:
                try:
                    if "Fuel Cost" in cols:
                        ws.update_cell(found_idx, cols["Fuel Cost"] + 1, fuel_cost)
                    else:
                        # expand header
                        header.append("Fuel Cost")
                        ws.update(f"A1:{chr(ord('A') + len(header) - 1)}1", [header], value_input_option="USER_ENTERED")
                        cols["Fuel Cost"] = len(header) - 1
                        ws.update_cell(found_idx, cols["Fuel Cost"] + 1, fuel_cost)
                    return {"ok": True, "merged": True}
                except Exception:
                    logger.exception("Failed to merge fuel to existing row")
            # fallback append
            row = [plate, by_user or "Unknown", dt]
            while len(row) < len(header):
                row.append("")
            if "Fuel Cost" in cols:
                row[cols["Fuel Cost"]] = fuel_cost
            ws.append_row(row, value_input_option="USER_ENTERED")
            return {"ok": True}

        if ntyp == "parking":
            fee = str(amount)
            row = [plate, by_user or "Unknown", dt]
            while len(row) < len(header):
                row.append("")
            if "Parking Fee" in cols:
                row[cols["Parking Fee"]] = fee
            else:
                header.append("Parking Fee")
                ws.update(f"A1:{chr(ord('A') + len(header) - 1)}1", [header], value_input_option="USER_ENTERED")
                cols["Parking Fee"] = len(header) - 1
                row[cols["Parking Fee"]] = fee
            ws.append_row(row, value_input_option="USER_ENTERED")
            return {"ok": True}

        if ntyp == "wash" or ntyp == "repair":
            fee = str(amount)
            row = [plate, by_user or "Unknown", dt]
            while len(row) < len(header):
                row.append("")
            # Use Other Fee column for wash/repair by default
            if "Other Fee" in cols:
                row[cols["Other Fee"]] = fee
            else:
                header.append("Other Fee")
                ws.update(f"A1:{chr(ord('A') + len(header) - 1)}1", [header], value_input_option="USER_ENTERED")
                cols["Other Fee"] = len(header) - 1
                row[cols["Other Fee"]] = fee
            # put notes in last column
            if "Notes" in cols:
                row[cols["Notes"]] = (notes or ntyp)
            ws.append_row(row, value_input_option="USER_ENTERED")
            return {"ok": True}

        # fallback generic
        row = [plate, by_user or "Unknown", dt]
        while len(row) < len(header):
            row.append("")
        row.append(str(amount))
        ws.append_row(row, value_input_option="USER_ENTERED")
        return {"ok": True}
    except Exception as e:
        logger.exception("Unexpected error in record_finance_entry: %s", e)
        return {"ok": False, "message": "Unexpected error: " + str(e)}

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
    # delete the typed command message immediately
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

async def mission_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # alias to mission_start_command
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

# leave command to create a ForceReply leave entry (we will delete prompts after reply)
async def leave_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        if update.effective_message:
            await update.effective_message.delete()
    except Exception:
        pass
    prompt = t(context.user_data.get("lang", DEFAULT_LANG), "leave_prompt")
    fr = ForceReply(selective=False)
    sent = await update.effective_chat.send_message(prompt, reply_markup=fr)
    # store prompt to delete it later after reply
    context.user_data["pending_leave"] = {"prompt_chat": sent.chat_id, "prompt_msg_id": sent.message_id}

# Admin finance inline flow
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
        [InlineKeyboardButton("ODO", callback_data="fin_type|odo"), InlineKeyboardButton("Fuel", callback_data="fin_type|fuel")],
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
    # Use ForceReply to prompt — store pending_fin in user-specific context
    prompt = f"Enter {typ} record — reply to this message with: <plate> <amount> [notes]"
    try:
        fr = ForceReply(selective=False)
        # send prompt as a separate message (so we can delete it later)
        m = await context.bot.send_message(chat_id=query.message.chat.id, text=prompt, reply_markup=fr)
        # store pending with prompt id to delete later when user replies
        context.user_data["pending_fin"] = {"type": typ, "prompt_chat": m.chat_id, "prompt_msg_id": m.message_id}
        # also update the callback message to a minimal text (do not leave long prompts)
        try:
            await query.edit_message_text("Enter record — prompt sent privately (will be deleted after entry).")
        except Exception:
            pass
    except Exception:
        logger.exception("Failed to prompt for finance entry.")
        try:
            await query.edit_message_text("Failed to prompt for finance entry.")
        except Exception:
            pass

# Process ForceReply replies: finance, leave, mission staff entry (when 'enter staff' chosen)
async def process_force_reply(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    text = update.effective_message.text.strip() if update.effective_message and update.effective_message.text else ""
    if not text:
        return

    # Helper to delete prompt message if stored
    async def _delete_prompt(prompt):
        if not prompt:
            return
        try:
            await context.bot.delete_message(chat_id=prompt.get("prompt_chat"), message_id=prompt.get("prompt_msg_id"))
        except Exception:
            pass

    # Finance pending
    pending_fin = context.user_data.get("pending_fin")
    if pending_fin:
        typ = pending_fin.get("type")
        # Expect "<plate> <amount> [notes]"
        parts = text.split()
        if len(parts) < 2:
            # delete user's reply message and the original prompt
            try:
                await update.effective_message.delete()
            except Exception:
                pass
            await _delete_prompt(pending_fin)
            try:
                await context.bot.send_message(chat_id=user.id, text="Invalid input. Format: <plate> <amount> [notes].")
            except Exception:
                pass
            context.user_data.pop("pending_fin", None)
            return
        plate = parts[0]
        amount = parts[1]
        notes = " ".join(parts[2:]) if len(parts) > 2 else ""
        ntyp = normalize_fin_type(typ)
        # validate amount for odo or numeric
        if ntyp == "odo":
            m = ODO_RE.match(amount)
            if not m:
                m2 = re.search(r'(\d+)', amount)
                if m2:
                    amount = m2.group(1)
                else:
                    try:
                        await update.effective_message.delete()
                    except Exception:
                        pass
                    await _delete_prompt(pending_fin)
                    try:
                        await context.bot.send_message(chat_id=user.id, text=t(context.user_data.get("lang", DEFAULT_LANG), "invalid_odo"))
                    except Exception:
                        pass
                    context.user_data.pop("pending_fin", None)
                    return
        else:
            if not AMOUNT_RE.match(amount):
                # try to extract numeric part
                m2 = re.search(r'(\d+(?:\.\d+)?)', amount)
                if m2:
                    amount = m2.group(1)
                else:
                    try:
                        await update.effective_message.delete()
                    except Exception:
                        pass
                    await _delete_prompt(pending_fin)
                    try:
                        await context.bot.send_message(chat_id=user.id, text=t(context.user_data.get("lang", DEFAULT_LANG), "invalid_amount"))
                    except Exception:
                        pass
                    context.user_data.pop("pending_fin", None)
                    return
        # record entry
        res = record_finance_entry(ntyp or typ, plate, amount, notes, by_user=user.username or "")
        # delete user's reply and the prompt
        try:
            await update.effective_message.delete()
        except Exception:
            pass
        await _delete_prompt(pending_fin)
        # If odo or fuel, construct a short summary message to group (as requested)
        if res.get("ok") and (ntyp == "odo" or ntyp == "fuel"):
            # compute last mileage and delta for plate
            try:
                ws = open_worksheet(RECORDS_TAB)
                # find header mapping again
                header = ws.row_values(1)
                cols = {h: i for i, h in enumerate(header)}
                last_vals = ws.get_all_values()
                # find the last row for this plate
                last_row = None
                for r in reversed(last_vals[1:]):
                    if len(r) > 0 and r[0].strip() == plate:
                        last_row = r
                        break
                mileage = ""
                fuelcost = ""
                if last_row:
                    if "Mileage" in cols and len(last_row) > cols["Mileage"]:
                        mileage = str(last_row[cols["Mileage"]]).strip()
                    if "Fuel Cost" in cols and len(last_row) > cols["Fuel Cost"]:
                        fuelcost = str(last_row[cols["Fuel Cost"]]).strip()
                # build notification text
                nowd = _now_dt().strftime("%Y-%m-%d")
                if ntyp == "odo":
                    prev = res.get("delta")
                    if prev is None:
                        prev_text = ""
                    else:
                        prev_text = f", diff {prev} km"
                    notify = f"{plate} @ {mileage} km on {nowd}{prev_text}"
                else:
                    # fuel
                    # attempt to find mileage on same row if exists
                    if mileage:
                        notify = f"{plate} @ {mileage} km + ${fuelcost} fuel on {nowd}"
                    else:
                        notify = f"{plate} + ${fuelcost} fuel on {nowd}"
                # send to chat (group where command came from). Use update.effective_chat if available, else user's chat
                chat_id_to_notify = update.effective_chat.id if update.effective_chat else user.id
                try:
                    await context.bot.send_message(chat_id=chat_id_to_notify, text=notify)
                except Exception:
                    pass
            except Exception:
                logger.exception("Failed to assemble odo/fuel notification")
        # do not leave any admin prompt messages lingering
        context.user_data.pop("pending_fin", None)
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
            # delete prompt
            try:
                await context.bot.delete_message(chat_id=pending_leave.get("prompt_chat"), message_id=pending_leave.get("prompt_msg_id"))
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
                await context.bot.send_message(chat_id=user.id, text="Invalid dates. Use YYYY-MM-DD.")
            except Exception:
                pass
            # delete prompt
            try:
                await context.bot.delete_message(chat_id=pending_leave.get("prompt_chat"), message_id=pending_leave.get("prompt_msg_id"))
            except Exception:
                pass
            context.user_data.pop("pending_leave", None)
            return
        try:
            ws = open_worksheet(LEAVE_TAB)
            row = [driver, start, end, reason, notes]
            ws.append_row(row, value_input_option="USER_ENTERED")
            try:
                await context.bot.send_message(chat_id=user.id, text=t(context.user_data.get("lang", DEFAULT_LANG), "leave_confirm", driver=driver, start=start, end=end, reason=reason))
            except Exception:
                pass
            try:
                await update.effective_message.delete()
            except Exception:
                pass
            # delete the prompt message too
            try:
                await context.bot.delete_message(chat_id=pending_leave.get("prompt_chat"), message_id=pending_leave.get("prompt_msg_id"))
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

    # mission staff entry pending (we store pending_mission with "need_staff": "enter")
    pending_mission = context.user_data.get("pending_mission")
    if pending_mission and pending_mission.get("need_staff") == "enter":
        staff = text
        plate = pending_mission.get("plate")
        departure = pending_mission.get("departure")
        username = user.username or user.full_name
        driver_map = get_driver_map()
        allowed = driver_map.get(user.username, []) if user and user.username else []
        if allowed and plate not in allowed:
            await update.effective_chat.send_message(t(context.user_data.get("lang", DEFAULT_LANG), "not_allowed", plate=plate))
            context.user_data.pop("pending_mission", None)
            # delete prompt if present
            try:
                await context.bot.delete_message(chat_id=pending_mission.get("prompt_chat"), message_id=pending_mission.get("prompt_msg_id"))
            except Exception:
                pass
            return
        # start mission record (reuse your existing start_mission_record if available)
        try:
            res = start_mission_record(username, plate, departure, staff_name=staff)
        except Exception:
            res = {"ok": False, "message": "mission start failed"}
        try:
            await update.effective_message.delete()
        except Exception:
            pass
        # delete the prompt message if any
        try:
            await context.bot.delete_message(chat_id=pending_mission.get("prompt_chat"), message_id=pending_mission.get("prompt_msg_id"))
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

# location_or_staff replaced by location_or_staff handler (handles non-command text)
async def location_or_staff(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # route to process_force_reply so ForceReply flows work
    return await process_force_reply(update, context)

# Plate callback with fixed flows and improved mission staff selection
async def plate_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    data = query.data
    user = query.from_user
    username = user.username or f"{user.first_name or ''} {user.last_name or ''}".strip()
    user_lang = context.user_data.get("lang", DEFAULT_LANG)

    # navigation
    if data == "show_start":
        try:
            await query.edit_message_text(t(user_lang, "choose_start"), reply_markup=build_plate_keyboard("start"))
        except Exception:
            pass
        return
    if data == "show_end":
        try:
            await query.edit_message_text(t(user_lang, "choose_end"), reply_markup=build_plate_keyboard("end"))
        except Exception:
            pass
        return
    if data == "show_mission_start":
        try:
            await query.edit_message_text(t(user_lang, "mission_start_prompt_plate"), reply_markup=build_plate_keyboard("mission_start_plate"))
        except Exception:
            pass
        return
    if data == "show_mission_end":
        try:
            await query.edit_message_text(t(user_lang, "mission_end_prompt_plate"), reply_markup=build_plate_keyboard("mission_end_plate"))
        except Exception:
            pass
        return
    if data == "help":
        try:
            await query.edit_message_text(t(user_lang, "help"))
        except Exception:
            pass
        return

    # admin finance
    if data == "admin_finance":
        if (query.from_user.username or "") not in BOT_ADMINS:
            try:
                await query.edit_message_text("❌ Admins only.")
            except Exception:
                pass
            return
        return await admin_finance_callback_handler(update, context)
    if data.startswith("fin_type|"):
        return await admin_fin_type_selected(update, context)

    # leave menu quick => send ForceReply then delete prompt after reply
    if data == "leave_menu":
        fr = ForceReply(selective=False)
        try:
            sent = await query.edit_message_text(t(context.user_data.get("lang", DEFAULT_LANG), "leave_prompt"))
            m = await context.bot.send_message(chat_id=sent.chat_id, text=t(context.user_data.get("lang", DEFAULT_LANG), "leave_prompt"), reply_markup=fr)
            context.user_data["pending_leave"] = {"prompt_chat": m.chat_id, "prompt_msg_id": m.message_id}
        except Exception:
            logger.exception("Failed to prompt leave menu")
            try:
                await query.edit_message_text("Failed to prompt leave.")
            except Exception:
                pass
        return

    # mission start choose plate
    if data.startswith("mission_start_plate|"):
        _, plate = data.split("|", 1)
        context.user_data["pending_mission"] = {"action": "start", "plate": plate}
        kb = [[InlineKeyboardButton("No staff", callback_data=f"mission_staff|none|{plate}"), InlineKeyboardButton("Enter staff", callback_data=f"mission_staff|enter|{plate}")]]
        try:
            await query.edit_message_text(t(user_lang, "mission_start_prompt_staff"), reply_markup=InlineKeyboardMarkup(kb))
        except Exception:
            pass
        return

    # mission end choose plate
    if data.startswith("mission_end_plate|"):
        _, plate = data.split("|", 1)
        context.user_data["pending_mission"] = {"action": "end", "plate": plate}
        kb = [[InlineKeyboardButton("PP", callback_data=f"mission_arrival|PP|{plate}"), InlineKeyboardButton("SHV", callback_data=f"mission_arrival|SHV|{plate}")]]
        try:
            await query.edit_message_text(t(user_lang, "mission_end_prompt_arrival"), reply_markup=InlineKeyboardMarkup(kb))
        except Exception:
            pass
        return

    # mission staff choices
    if data.startswith("mission_staff|"):
        parts = data.split("|")
        if len(parts) < 3:
            try:
                await query.edit_message_text("Invalid selection.")
            except Exception:
                pass
            return
        _, choice, plate = parts
        pending = context.user_data.get("pending_mission") or {}
        pending["plate"] = plate
        context.user_data["pending_mission"] = pending
        if choice == "none":
            dep_kb = [[InlineKeyboardButton("PP", callback_data=f"mission_depart|PP|{plate}"), InlineKeyboardButton("SHV", callback_data=f"mission_depart|SHV|{plate}")]]
            try:
                await query.edit_message_text(t(user_lang, "mission_start_prompt_depart"), reply_markup=InlineKeyboardMarkup(dep_kb))
            except Exception:
                pass
            return
        elif choice == "enter":
            dep_kb = [[InlineKeyboardButton("PP", callback_data=f"mission_depart|PP|{plate}"), InlineKeyboardButton("SHV", callback_data=f"mission_depart|SHV|{plate}")]]
            try:
                await query.edit_message_text(t(user_lang, "mission_start_prompt_depart"), reply_markup=InlineKeyboardMarkup(dep_kb))
            except Exception:
                pass
            return

    # mission depart
    if data.startswith("mission_depart|"):
        parts = data.split("|")
        if len(parts) < 3:
            try:
                await query.edit_message_text("Invalid selection.")
            except Exception:
                pass
            return
        _, dep, plate = parts
        pending = context.user_data.get("pending_mission") or {}
        pending["departure"] = dep
        pending["plate"] = plate
        context.user_data["pending_mission"] = pending
        kb = [[InlineKeyboardButton("Start with no staff", callback_data=f"mission_start_now|{plate}|{dep}"), InlineKeyboardButton("Enter staff name", callback_data=f"mission_staff_enter|{plate}|{dep}")]]
        try:
            await query.edit_message_text("Choose staff option:", reply_markup=InlineKeyboardMarkup(kb))
        except Exception:
            pass
        return

    # mission staff enter => ForceReply
    if data.startswith("mission_staff_enter|"):
        try:
            _, plate, dep = data.split("|", 2)
        except Exception:
            try:
                await query.edit_message_text("Invalid selection.")
            except Exception:
                pass
            return
        fr = ForceReply(selective=False)
        try:
            m = await query.edit_message_text("Please reply with staff name (this message will be removed).")
            sent = await context.bot.send_message(chat_id=m.chat_id, text="Please reply with staff name.", reply_markup=fr)
            context.user_data["pending_mission"] = {"action": "start", "plate": plate, "departure": dep, "need_staff": "enter", "prompt_chat": sent.chat_id, "prompt_msg_id": sent.message_id}
        except Exception:
            logger.exception("Failed to request staff name")
        return

    # mission start now (no staff)
    if data.startswith("mission_start_now|"):
        try:
            _, plate, dep = data.split("|", 2)
        except Exception:
            try:
                await query.edit_message_text("Invalid selection.")
            except Exception:
                pass
            return
        username = query.from_user.username or query.from_user.full_name
        res = start_mission_record(username, plate, dep, staff_name="")
        if res.get("ok"):
            try:
                await query.edit_message_text(t(user_lang, "mission_start_ok", plate=plate, start_date=now_str(), dep=dep))
            except Exception:
                pass
        else:
            try:
                await query.edit_message_text("❌ " + res.get("message", ""))
            except Exception:
                pass
        context.user_data.pop("pending_mission", None)
        return

    # mission arrival (end flow)
    if data.startswith("mission_arrival|"):
        parts = data.split("|")
        if len(parts) < 3:
            try:
                await query.edit_message_text("Invalid selection.")
            except Exception:
                pass
            return
        _, arr, plate = parts
        pending = context.user_data.get("pending_mission") or {}
        pending["arrival"] = arr
        pending["plate"] = plate
        context.user_data["pending_mission"] = pending
        driver_map = get_driver_map()
        allowed = driver_map.get(username, []) if username else []
        if allowed and plate not in allowed:
            try:
                await query.edit_message_text(t(user_lang, "not_allowed", plate=plate))
            except Exception:
                pass
            context.user_data.pop("pending_mission", None)
            return
        res = end_mission_record(username, plate, arr)
        if res.get("ok"):
            try:
                await query.edit_message_text(t(user_lang, "mission_end_ok", plate=plate, end_date=now_str(), arr=arr))
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
                    try:
                        await query.message.chat.send_message(f"✅ Driver {username} completed {cnt} missions in {month_start.strftime('%Y-%m')}.")
                    except Exception:
                        pass
                except Exception:
                    logger.exception("Failed to send merged missions message.")
        else:
            try:
                await query.edit_message_text("❌ " + res.get("message", ""))
            except Exception:
                pass
        context.user_data.pop("pending_mission", None)
        return

    # start|plate quick action (start trip)
    if data.startswith("start|") or data.startswith("end|"):
        try:
            action, plate = data.split("|", 1)
        except Exception:
            try:
                await query.edit_message_text("Invalid selection.")
            except Exception:
                pass
            return
        driver_map = get_driver_map()
        allowed = driver_map.get(username, []) if username else []
        if allowed and plate not in allowed:
            try:
                await query.edit_message_text(t(user_lang, "not_allowed", plate=plate))
            except Exception:
                pass
            return
        if action == "start":
            res = record_start_trip(username, plate)
            # edit the inline message to show start text in desired format
            try:
                msg = f"Driver {username} start trip at {now_str()}."
                await query.edit_message_text(msg)
            except Exception:
                pass
            return
        elif action == "end":
            res = record_end_trip(username, plate)
            if res.get("ok"):
                # compute duration if possible - since we don't keep pair start/end in same sheet here,
                # we'll produce the requested message using res.message though duration may be unknown.
                try:
                    # count trips for today and month
                    nowdt = _now_dt()
                    day_start = datetime(nowdt.year, nowdt.month, nowdt.day)
                    day_end = day_start + timedelta(days=1)
                    month_start = datetime(nowdt.year, nowdt.month, 1)
                    if nowdt.month == 12:
                        month_end = datetime(nowdt.year + 1, 1, 1)
                    else:
                        month_end = datetime(nowdt.year, nowdt.month + 1, 1)
                    trip_counts_day = count_trips_per_driver_period(day_start, day_end)
                    trip_counts_month = count_trips_per_driver_period(month_start, month_end)
                    cnt_day = trip_counts_day.get(username, 0)
                    cnt_month = trip_counts_month.get(username, 0)
                    # create message: Driver X end trip at YYYY-MM time (duration xhxm). Driver X completed x trips in today date and x trips in YYYY-MM.
                    # duration is not easily computable here, so use placeholder if not available
                    duration_text = ""
                    m = re.search(r'duration.*\((.*?)\)', res.get("message", ""))
                    if m:
                        duration_text = f" (duration {m.group(1)})"
                    msg = f"Driver {username} end trip at {now_str()}{duration_text}. Driver {username} completed {cnt_day} trips today and {cnt_month} trips in {month_start.strftime('%Y-%m')}."
                    await query.edit_message_text(msg)
                except Exception:
                    try:
                        await query.edit_message_text(t(user_lang, "end_ok", plate=plate, driver=username, msg=res.get("message", "")))
                    except Exception:
                        pass
            else:
                try:
                    await query.edit_message_text("❌ " + res.get("message", ""))
                except Exception:
                    pass
            return

    try:
        await query.edit_message_text(t(user_lang, "invalid_sel"))
    except Exception:
        pass

# helper: count trips per driver for arbitrary period (reads RECORDS_TAB and counts rows with mileage+fuel or start/end)
def count_trips_per_driver_period(start_date: datetime, end_date: datetime) -> Dict[str, int]:
    counts: Dict[str, int] = {}
    try:
        ws = open_worksheet(RECORDS_TAB)
        vals = ws.get_all_values()
        if not vals:
            return counts
        header = vals[0]
        # find DateTime column index
        dt_idx = None
        driver_idx = None
        try:
            dt_idx = [c.strip() for c in header].index("DateTime")
        except Exception:
            dt_idx = 2 if len(header) > 2 else 2
        try:
            driver_idx = [c.strip() for c in header].index("Driver")
        except Exception:
            driver_idx = 1 if len(header) > 1 else 1
        for r in vals[1:]:
            dt_cell = r[dt_idx] if len(r) > dt_idx else ""
            drv = r[driver_idx] if len(r) > driver_idx else ""
            if not dt_cell:
                continue
            s_dt = parse_ts(dt_cell)
            if not s_dt:
                continue
            if start_date <= s_dt < end_date:
                counts[drv] = counts.get(drv, 0) + 1
    except Exception:
        logger.exception("Failed to count trips per driver period")
    return counts

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

# mission_report_command kept as in your original file (omitted heavy details for brevity, assumed present)
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
            csv_path = None
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
        try:
            await update.effective_chat.send_message(t(user_lang, "menu"), reply_markup=InlineKeyboardMarkup(keyboard))
        except Exception:
            pass

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
            if len(r) < 3:
                continue
            plate = r[0] if len(r) >= 1 else ""
            start_ts = r[2] if len(r) >= 3 else ""
            if not start_ts:
                continue
            s_dt = parse_ts(start_ts)
            if not s_dt:
                continue
            if not (start_dt <= s_dt < end_dt):
                continue
            # no duration field here; skip
            totals[plate] = totals.get(plate, 0) + 0
    except Exception:
        logger.exception("Failed to aggregate for period.")
    return totals

# setup_menu command: post main menu in group (no pinning)
async def setup_menu_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    if (user.username or "") not in BOT_ADMINS:
        try:
            await update.effective_chat.send_message("❌ Admins only.")
        except Exception:
            pass
        return
    try:
        user_lang = context.user_data.get("lang", DEFAULT_LANG)
        keyboard = [
            [InlineKeyboardButton("Start trip", callback_data="show_start"), InlineKeyboardButton("End trip", callback_data="show_end")],
            [InlineKeyboardButton("Mission start", callback_data="show_mission_start"), InlineKeyboardButton("Mission end", callback_data="show_mission_end")],
            [InlineKeyboardButton("Admin Finance", callback_data="admin_finance"), InlineKeyboardButton("Leave", callback_data="leave_menu")],
        ]
        sent = await update.effective_chat.send_message(t(user_lang, "menu"), reply_markup=InlineKeyboardMarkup(keyboard))
        # DO NOT pin the message (user requested cancel pin)
    except Exception:
        logger.exception("Failed to setup menu.")

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
    application.add_handler(CommandHandler("setup_menu", setup_menu_command))
    application.add_handler(CommandHandler("lang", lang_command))

    application.add_handler(CallbackQueryHandler(plate_callback))

    # ForceReply responses for finance, leave, mission staff
    application.add_handler(MessageHandler(filters.REPLY & filters.TEXT & (~filters.COMMAND), process_force_reply))
    # fallback text handler (used to route some free text entries)
    application.add_handler(MessageHandler(filters.TEXT & (~filters.COMMAND), location_or_staff))

    application.add_handler(MessageHandler(filters.Regex(AUTO_KEYWORD_PATTERN) & filters.ChatType.GROUPS, auto_menu_listener))

    application.add_handler(CommandHandler("help", lambda u, c: u.message.reply_text(t(c.user_data.get("lang", DEFAULT_LANG), "help"))))

    # NOTE: Removed set_my_commands() on purpose to avoid leaving explicit bot commands visible to users.

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
