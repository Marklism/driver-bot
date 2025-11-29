#!/usr/bin/env python3
import os
import json
import base64
import logging
import uuid
import re
from datetime import datetime, timedelta, time as dtime
from typing import Optional, Dict, List, Any
import urllib.request

import gspread
from oauth2client.service_account import ServiceAccountCredentials

try:
    from zoneinfo import ZoneInfo
except Exception:
    ZoneInfo = None

from telegram import (
    InlineKeyboardButton, InlineKeyboardMarkup, BotCommand, Update,
    ReplyKeyboardMarkup, KeyboardButton, ForceReply
)
from telegram.constants import ChatAction
from telegram.ext import (
    ApplicationBuilder, CommandHandler, CallbackQueryHandler,
    MessageHandler, filters, ContextTypes, PicklePersistence
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("driver-bot")

BOT_TOKEN = os.getenv("BOT_TOKEN")
GOOGLE_CREDS_BASE64 = os.getenv("GOOGLE_CREDS_BASE64")
GOOGLE_CREDS_PATH = os.getenv("GOOGLE_CREDS_PATH")

PLATE_LIST = os.getenv("PLATE_LIST", "2BB-3071,2BB-0809,2CI-8066")
GOOGLE_SHEET_NAME = os.getenv("GOOGLE_SHEET_NAME", "Driver_Log")

_env_tz = os.getenv("LOCAL_TZ")
if _env_tz is None:
    LOCAL_TZ = "Asia/Phnom_Penh"
else:
    LOCAL_TZ = _env_tz.strip() or "Asia/Phnom_Penh"

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
FUEL_TAB = os.getenv("FUEL_TAB", "Fuel")
PARKING_TAB = os.getenv("PARKING_TAB", "Parking")
WASH_TAB = os.getenv("WASH_TAB", "Wash")
REPAIR_TAB = os.getenv("REPAIR_TAB", "Repair")

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
    EXPENSE_TAB: ["Plate", "Driver", "DateTime", "Mileage", "Delta KM", "Fuel Cost", "Parking Fee", "Other Fee", "Invoice", "DriverPaid"],
    FUEL_TAB: ["Plate", "Driver", "DateTime", "Mileage", "Delta KM", "Fuel Cost", "Invoice", "DriverPaid"],
    PARKING_TAB: ["Plate", "Driver", "DateTime", "Amount", "Notes"],
    WASH_TAB: ["Plate", "Driver", "DateTime", "Amount", "Notes"],
    REPAIR_TAB: ["Plate", "Driver", "DateTime", "Amount", "Notes"],
}

TR = {
    "en": {
        "menu": "Driver Bot Menu — tap a button:",
        "choose_start": "Choose vehicle plate to START trip:",
        "choose_end": "Choose vehicle plate to END trip:",
        "start_ok": "Driver {driver} (plate {plate}) starts trip at {ts}.",
        "end_ok": "Driver {driver} (plate {plate}) ends trip at {ts}.",
        "trip_summary": "Driver {driver} completed {n_today} trip(s) today and {n_month} trip(s) in {month} and {n_year} trip(s) in {year}. Plate {plate} completed {p_today} today, {p_month} in {month}, {p_year} in {year}.",
        "not_allowed": "❌ You are not allowed to operate plate: {plate}.",
        "invalid_sel": "Invalid selection.",
        "help": "Help: Use /start_trip or /end_trip and select a plate.",
        "no_bot_token": "Please set BOT_TOKEN environment variable.",
        "mission_start_prompt_plate": "Choose plate to start mission:",
        "mission_start_prompt_depart": "Select departure city:",
        "mission_start_ok": "Driver {driver} (plate {plate}) departures from {dep} at {ts}.",
        "mission_end_ok": "Driver {driver} (plate {plate}) arrives at {arr} at {ts}.",
        "roundtrip_merged_notify": "✅ Driver {driver} completed {count_month} mission(s) in {month} and {count_year} in {year}. Plate {plate} completed {p_month} in {month} and {p_year} in {year}. Mission days: {days}, Per-diem: ${perdiem:.1f}.",
        "invalid_amount": "Invalid amount — please send a numeric value like `23.5`.",
        "invalid_odo": "Invalid odometer — please send numeric KM like `12345` or `12345KM`.",
        "leave_prompt": "Reply to this message: <driver_username> <YYYY-MM-DD> <YYYY-MM-DD> <reason> [notes]\nExample: markpeng1 2025-12-01 2025-12-05 annual_leave",
        "leave_confirm": "Driver {driver} total leave days (to {end}): {total} day(s).",
        "enter_odo_km": "Enter odometer reading (KM) for {plate}:",
        "enter_fuel_cost": "Enter fuel cost in $ for {plate}: (optionally add `inv:INV123 paid:yes`)",
        "enter_amount_for": "Enter amount in $ for {typ} for {plate}:",
        "finance_short_parking": "{plate} parking fee ${amt} on {date} paid by {user}.",
        "finance_short_wash": "{plate} wash fee ${amt} on {date} paid by {user}.",
        "finance_short_repair": "{plate} repair fee ${amt} on {date} paid by {user}.",
        "finance_short_odo_fuel": "{plate} @ {odo} km + ${fuel} fuel on {date} paid by {user}, difference from previous odo is {delta} km.",
    },
}

def t(user_lang: Optional[str], key: str, **kwargs) -> str:
    lang = (user_lang or DEFAULT_LANG or "en").lower()
    if lang not in SUPPORTED_LANGS:
        lang = "en"
    return TR.get(lang, TR["en"]).get(key, TR["en"].get(key, "")).format(**kwargs)

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
    except Exception:
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
        logger.exception("Failed to ensure headers")

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
    except Exception:
        logger.exception("Failed to update headers")

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
                except Exception:
                    logger.exception("Failed to update mission headers")
    except Exception:
        logger.exception("Error checking missions header")

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
        return sh.sheet1

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
        logger.exception("Failed to parse driver map")
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
        logger.exception("Failed to load drivers tab")
        return {}

def get_driver_map() -> Dict[str, List[str]]:
    env_map = load_driver_map_from_env()
    if env_map:
        return env_map
    sheet_map = load_driver_map_from_sheet()
    return sheet_map

def _now_dt() -> datetime:
    if LOCAL_TZ and ZoneInfo:
        try:
            tz = ZoneInfo(LOCAL_TZ)
            return datetime.now(tz)
        except Exception:
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

def record_start_trip(driver: str, plate: str) -> dict:
    ws = open_worksheet(RECORDS_TAB)
    start_ts = now_str()
    row = [today_date_str(), driver, plate, start_ts, "", ""]
    try:
        ws.append_row(row, value_input_option="USER_ENTERED")
        return {"ok": True, "message": f"Start time recorded for {plate} at {start_ts}", "ts": start_ts}
    except Exception as e:
        logger.exception("Failed to append start trip")
        return {"ok": False, "message": str(e)}
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
                    try:
                        existing = ws.row_values(row_number)
                    except Exception:
                        existing = []
                    existing = list(existing)
                    while len(existing) < COL_DURATION:
                        existing.append("")
                    existing[COL_END - 1] = end_ts
                    existing[COL_DURATION - 1] = duration_text
                    try:
                        ws.delete_rows(row_number)
                    except Exception:
                        logger.exception("Failed fallback delete")
                    try:
                        ws.insert_row(existing, row_number)
                    except Exception:
                        logger.exception("Failed fallback insert")
                return {"ok": True, "message": f"End time recorded for {plate} at {end_ts} (duration {duration_text})", "ts": end_ts, "duration": duration_text}
        end_ts = now_str()
        row = [today_date_str(), driver, plate, "", end_ts, ""]
        ws.append_row(row, value_input_option="USER_ENTERED")
        return {"ok": True, "message": f"End time recorded (no matching start) for {plate} at {end_ts}", "ts": end_ts, "duration": ""}
    except Exception as e:
        logger.exception("Failed to update end trip")
        return {"ok": False, "message": str(e)}

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

def start_mission_record(driver: str, plate: str, departure: str) -> dict:
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
        row[M_IDX_STAFF] = ""
        row[M_IDX_ROUNDTRIP] = ""
        row[M_IDX_RETURN_START] = ""
        row[M_IDX_RETURN_END] = ""
        ws.append_row(row, value_input_option="USER_ENTERED")
        return {"ok": True, "guid": guid, "no": next_no}
    except Exception as e:
        logger.exception("Failed to append mission start")
        return {"ok": False, "message": str(e)}

def end_mission_record(driver: str, plate: str, arrival: str) -> dict:
    try:
        ws = open_worksheet(MISSIONS_TAB)
    except Exception as e:
        logger.exception("Failed to open missions sheet")
        return {"ok": False, "message": str(e)}
    try:
        vals, start_idx = _missions_get_values_and_data_rows(ws)
        for i in range(len(vals) - 1, start_idx - 1, -1):
            row = _ensure_row_length(vals[i], M_MANDATORY_COLS)
            rec_plate = str(row[M_IDX_PLATE]).strip()
            rec_name = str(row[M_IDX_NAME]).strip()
            rec_end = str(row[M_IDX_END]).strip()
            rec_start = str(row[M_IDX_START]).strip()
            rec_dep = str(row[M_IDX_DEPART]).strip()
            if rec_plate == plate and rec_name == driver and not rec_end:
                row_number = i + 1
                end_ts = now_str()
                try:
                    ws.update_cell(row_number, M_IDX_END + 1, end_ts)
                    ws.update_cell(row_number, M_IDX_ARRIVAL + 1, arrival)
                except Exception:
                    try:
                        existing = ws.row_values(row_number)
                    except Exception:
                        existing = []
                    existing = _ensure_row_length(existing, M_MANDATORY_COLS)
                    existing[M_IDX_END] = end_ts
                    existing[M_IDX_ARRIVAL] = arrival
                    try:
                        ws.delete_rows(row_number)
                    except Exception:
                        logger.exception("Failed fallback delete mission row")
                    try:
                        ws.insert_row(existing, row_number)
                    except Exception:
                        logger.exception("Failed fallback insert mission row")
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
                cur_dep = rec_dep
                cur_arr = arrival
                for comp in candidates:
                    if (cur_dep == "PP" and cur_arr == "SHV" and comp["dep"] == "SHV" and comp["arr"] == "PP") or \
                       (cur_dep == "SHV" and cur_arr == "PP" and comp["dep"] == "PP" and comp["arr"] == "SHV"):
                        found_pair = comp
                        break
                if not found_pair:
                    for comp in candidates:
                        if comp["dep"] == cur_arr and comp["arr"] == cur_dep:
                            found_pair = comp
                            break
                if not found_pair and candidates:
                    candidates.sort(key=lambda x: abs((x["start"] - s_dt).total_seconds()))
                    found_pair = candidates[0]
                if not found_pair:
                    return {"ok": True, "message": f"Mission end recorded for {plate} at {end_ts}", "merged": False}
                other_idx = found_pair["idx"]
                other_start = found_pair["start"]
                primary_idx = i if s_dt <= other_start else other_idx
                secondary_idx = other_idx if primary_idx == i else i
                primary_row_number = primary_idx + 1
                secondary_row_number = secondary_idx + 1
                if primary_idx == i:
                    return_start = found_pair["rstart"]
                    return_end = found_pair["rend"] if found_pair["rend"] else (found_pair["end"].strftime(TS_FMT) if found_pair["end"] else "")
                else:
                    return_start = rec_start
                    return_end = end_ts
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
                        logger.exception("Failed fallback delete primary")
                    try:
                        ws.insert_row(existing, primary_row_number)
                    except Exception:
                        logger.exception("Failed fallback insert primary")
                try:
                    sec_vals = _ensure_row_length(vals2[secondary_idx], M_MANDATORY_COLS) if secondary_idx < len(vals2) else None
                    sec_guid = sec_vals[M_IDX_GUID] if sec_vals else None
                    if sec_guid:
                        all_vals_post, start_idx_post = _missions_get_values_and_data_rows(ws)
                        for k in range(start_idx_post, len(all_vals_post)):
                            r_k = _ensure_row_length(all_vals_post[k], M_MANDATORY_COLS)
                            if str(r_k[M_IDX_GUID]).strip() == str(sec_guid).strip():
                                try:
                                    ws.delete_rows(k + 1)
                                    break
                                except Exception:
                                    try:
                                        ws.update_cell(k + 1, M_IDX_ROUNDTRIP + 1, "Merged")
                                    except Exception:
                                        logger.exception("Failed to mark merged secondary")
                                    break
                    else:
                        try:
                            ws.delete_rows(secondary_row_number)
                        except Exception:
                            try:
                                ws.update_cell(secondary_row_number, M_IDX_ROUNDTRIP + 1, "Merged")
                            except Exception:
                                logger.exception("Failed to mark merged secondary2")
                except Exception:
                    logger.exception("Failed cleaning up secondary mission row after merge")
                merged_flag = (secondary_idx == i)
                return {"ok": True, "message": f"Mission end recorded and merged for {plate} at {end_ts}", "merged": merged_flag, "driver": driver, "plate": plate}
        return {"ok": False, "message": "No open mission found"}
    except Exception as e:
        logger.exception("Failed to update mission end")
        return {"ok": False, "message": str(e)}

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
        logger.exception("Failed to write mission report")
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
        logger.exception("Failed to count roundtrips")
    return counts

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

AMOUNT_RE = re.compile(r'^\s*(\d+(?:\.\d+)?)\s*$', re.I)
ODO_RE = re.compile(r'^\s*(\d+)(?:\s*km)?\s*$', re.I)
FIN_TYPES = {"odo", "fuel", "parking", "wash", "repair"}
FIN_TYPE_ALIASES = {"odo": "odo", "km": "odo", "fuel": "fuel", "parking": "parking", "wash": "wash", "repair": "repair"}

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
        ws = open_worksheet(FUEL_TAB)
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
        logger.exception("Failed to find last mileage")
        return None

def record_finance_combined_odo_fuel(plate: str, mileage: str, fuel_cost: str, by_user: str = "", invoice: str = "", driver_paid: str = "") -> dict:
    try:
        ws = open_worksheet(FUEL_TAB)
        prev_m = _find_last_mileage_for_plate(plate)
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
        row = [plate, by_user or "Unknown", dt, str(m_int) if m_int is not None else str(mileage), delta, str(fuel_cost), invoice or "", driver_paid or ""]
        ws.append_row(row, value_input_option="USER_ENTERED")
        return {"ok": True, "delta": delta, "mileage": m_int, "fuel": fuel_cost}
    except Exception as e:
        logger.exception("Failed to append odo+fuel")
        return {"ok": False, "message": str(e)}

def record_finance_entry_single_row(typ: str, plate: str, amount: str, notes: str, by_user: str = "") -> dict:
    try:
        ntyp = normalize_fin_type(typ) or typ
        plate = str(plate).strip()
        notes = str(notes).strip()
        by_user = str(by_user).strip()
        if ntyp == "parking":
            ws = open_worksheet(PARKING_TAB)
            dt = now_str()
            row = [plate, by_user or "Unknown", dt, str(amount), notes]
            ws.append_row(row, value_input_option="USER_ENTERED")
            return {"ok": True}
        if ntyp == "wash":
            ws = open_worksheet(WASH_TAB)
            dt = now_str()
            row = [plate, by_user or "Unknown", dt, str(amount), notes]
            ws.append_row(row, value_input_option="USER_ENTERED")
            return {"ok": True}
        if ntyp == "repair":
            ws = open_worksheet(REPAIR_TAB)
            dt = now_str()
            row = [plate, by_user or "Unknown", dt, str(amount), notes]
            ws.append_row(row, value_input_option="USER_ENTERED")
            return {"ok": True}
        if ntyp == "odo":
            ws = open_worksheet(EXPENSE_TAB)
            dt = now_str()
            row = [plate, by_user or "Unknown", dt, str(amount), "", "", "", "", "", ""]
            ws.append_row(row, value_input_option="USER_ENTERED")
            return {"ok": True}
        ws = open_worksheet(EXPENSE_TAB)
        dt = now_str()
        row = [plate, by_user or "Unknown", dt, "", "", "", "", "", str(amount), ""]
        ws.append_row(row, value_input_option="USER_ENTERED")
        return {"ok": True}
    except Exception as e:
        logger.exception("Failed to record finance entry")
        return {"ok": False, "message": str(e)}

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

async def safe_delete_message(bot, chat_id, message_id):
    try:
        await bot.delete_message(chat_id=chat_id, message_id=message_id)
    except Exception:
        pass

async def safe_edit_or_send(query, text: str, reply_markup=None):
    if not text:
        return None
    try:
        if query and getattr(query, "message", None):
            try:
                return await query.edit_message_text(text, reply_markup=reply_markup)
            except Exception:
                return await query.message.chat.send_message(text=text, reply_markup=reply_markup)
        else:
            if getattr(query, "message", None):
                return await query.message.chat.send_message(text=text, reply_markup=reply_markup)
            else:
                return None
    except Exception:
        try:
            chat_id = query.message.chat.id if getattr(query, "message", None) else None
            if chat_id:
                return await query.message.chat.send_message(text=text, reply_markup=reply_markup)
        except Exception:
            logger.exception("safe_edit_or_send failed")
        return None

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

async def leave_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        if update.effective_message:
            await update.effective_message.delete()
    except Exception:
        pass
    prompt = t(context.user_data.get("lang", DEFAULT_LANG), "leave_prompt")
    fr = ForceReply(selective=False)
    sent = await update.effective_chat.send_message(prompt, reply_markup=fr)
    context.user_data["pending_leave"] = {"prompt_chat": sent.chat_id, "prompt_msg_id": sent.message_id}

async def admin_finance_callback_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    user = query.from_user
    username = user.username or (user.first_name or "")
    if username not in BOT_ADMINS:
        try:
            await safe_edit_or_send(query, "❌ You are not an admin.")
        except Exception:
            pass
        return
    kb = [
        [InlineKeyboardButton("ODO+Fuel", callback_data="fin_type|odo_fuel"), InlineKeyboardButton("Fuel (solo)", callback_data="fin_type|fuel")],
        [InlineKeyboardButton("Parking", callback_data="fin_type|parking"), InlineKeyboardButton("Wash", callback_data="fin_type|wash")],
        [InlineKeyboardButton("Repair", callback_data="fin_type|repair")],
    ]
    try:
        await safe_edit_or_send(query, "Select finance type:", reply_markup=InlineKeyboardMarkup(kb))
    except Exception:
        logger.exception("Failed to prompt finance options")

async def admin_fin_type_selected(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    data = query.data
    parts = data.split("|", 1)
    if len(parts) != 2:
        await safe_edit_or_send(query, "Invalid selection.")
        return
    _, typ = parts
    user = query.from_user
    username = user.username or (user.first_name or "")
    if username not in BOT_ADMINS:
        await safe_edit_or_send(query, "❌ Not admin.")
        return
    try:
        await safe_edit_or_send(query, "Choose plate:", reply_markup=build_plate_keyboard(f"fin_plate|{typ}"))
    except Exception:
        logger.exception("Failed plate selection for finance")

async def process_force_reply(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    text = update.effective_message.text.strip() if update.effective_message and update.effective_message.text else ""
    if not text:
        return

    pending_multi = context.user_data.get("pending_fin_multi")
    if pending_multi:
        ptype = pending_multi.get("type")
        plate = pending_multi.get("plate")
        step = pending_multi.get("step")
        origin = pending_multi.get("origin")
        if ptype == "odo_fuel":
            if step == "km":
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
                try:
                    await update.effective_message.delete()
                except Exception:
                    pass
                fr = ForceReply(selective=False)
                try:
                    mmsg = await context.bot.send_message(chat_id=update.effective_chat.id, text=t(context.user_data.get("lang", DEFAULT_LANG), "enter_fuel_cost", plate=plate), reply_markup=fr)
                    pending_multi["prompt_chat"] = mmsg.chat_id
                    pending_multi["prompt_msg_id"] = mmsg.message_id
                    context.user_data["pending_fin_multi"] = pending_multi
                except Exception:
                    logger.exception("Failed to prompt for fuel cost")
                    try:
                        if origin:
                            await safe_delete_message(context.bot, origin.get("chat"), origin.get("msg_id"))
                    except Exception:
                        pass
                    context.user_data.pop("pending_fin_multi", None)
                return
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
                        context.user_data.pop("pending_fin_multi", None)
                        return
                else:
                    fuel_amt = am.group(1)
                km = pending_multi.get("km", "")
                try:
                    res = record_finance_combined_odo_fuel(plate, km, fuel_amt, by_user=user.username or "", invoice=invoice, driver_paid=driver_paid)
                except Exception:
                    res = {"ok": False}
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
                try:
                    if origin:
                        await safe_delete_message(context.bot, origin.get("chat"), origin.get("msg_id"))
                except Exception:
                    pass
                try:
                    delta_txt = res.get("delta", "")
                    m_val = res.get("mileage", km)
                    fuel_val = res.get("fuel", fuel_amt)
                    nowd = _now_dt().strftime(DATE_FMT)
                    msg = t(context.user_data.get("lang", DEFAULT_LANG), "finance_short_odo_fuel", plate=plate, odo=m_val, fuel=fuel_val, date=nowd, user=user.username or "")
                    await update.effective_chat.send_message(msg)
                except Exception:
                    logger.exception("Failed to send group notification for odo+fuel")
                try:
                    await context.bot.send_message(chat_id=user.id, text=f"Recorded {plate}: {km}KM and ${fuel_amt} fuel. Delta {delta_txt} km. Invoice={invoice} Paid={driver_paid}")
                except Exception:
                    pass
                context.user_data.pop("pending_fin_multi", None)
                return

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
            nowd = _now_dt().strftime(DATE_FMT)
            if typ == "parking":
                msg_pub = t(context.user_data.get("lang", DEFAULT_LANG), "finance_short_parking", plate=plate, amt=amt, date=nowd, user=user.username or "")
            elif typ == "wash":
                msg_pub = t(context.user_data.get("lang", DEFAULT_LANG), "finance_short_wash", plate=plate, amt=amt, date=nowd, user=user.username or "")
            elif typ == "repair":
                msg_pub = t(context.user_data.get("lang", DEFAULT_LANG), "finance_short_repair", plate=plate, amt=amt, date=nowd, user=user.username or "")
            else:
                msg_pub = f"{plate} {typ} ${amt} on {nowd} paid by {user.username or ''}."
            try:
                await update.effective_chat.send_message(msg_pub)
            except Exception:
                logger.exception("Failed to publish finance short message")
            try:
                await context.bot.send_message(chat_id=user.id, text=f"Recorded {typ} ${amt} for {plate}. Invoice={invoice} Paid={driver_paid}")
            except Exception:
                pass
            context.user_data.pop("pending_fin_simple", None)
            return

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
        day_count = (ed - sd).days + 1
        try:
            ws = open_worksheet(LEAVE_TAB)
            row = [driver, start, end, reason, notes]
            ws.append_row(row, value_input_option="USER_ENTERED")
        except Exception:
            logger.exception("Failed to append leave")
        try:
            await update.effective_message.delete()
        except Exception:
            pass
        try:
            await safe_delete_message(context.bot, pending_leave.get("prompt_chat"), pending_leave.get("prompt_msg_id"))
        except Exception:
            pass
        total_days = 0
        try:
            vals = open_worksheet(LEAVE_TAB).get_all_records()
            for r in vals:
                dr = str(r.get("Driver", r.get("driver", ""))).strip()
                if dr != driver:
                    continue
                s = r.get("Start Date", r.get("start date", r.get("Start", "")))
                e = r.get("End Date", r.get("end date", r.get("End", "")))
                try:
                    sd2 = datetime.strptime(str(s), "%Y-%m-%d")
                    ed2 = datetime.strptime(str(e), "%Y-%m-%d")
                    total_days += (ed2 - sd2).days + 1
                except Exception:
                    continue
        except Exception:
            logger.exception("Failed compute cumulative leave")
        try:
            await context.bot.send_message(chat_id=update.effective_chat.id, text=t(context.user_data.get("lang", DEFAULT_LANG), "leave_confirm", driver=driver, end=end, total=total_days))
        except Exception:
            pass
        context.user_data.pop("pending_leave", None)
        return

async def location_or_staff(update: Update, context: ContextTypes.DEFAULT_TYPE):
    return await process_force_reply(update, context)

async def plate_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    data = q.data
    user = q.from_user
    username = user.username or f"{user.first_name or ''} {user.last_name or ''}".strip()
    user_lang = context.user_data.get("lang", DEFAULT_LANG)

    if data == "show_start":
        await safe_edit_or_send(q, t(user_lang, "choose_start"), reply_markup=build_plate_keyboard("start"))
        return
    if data == "show_end":
        await safe_edit_or_send(q, t(user_lang, "choose_end"), reply_markup=build_plate_keyboard("end"))
        return
    if data == "show_mission_start":
        await safe_edit_or_send(q, t(user_lang, "mission_start_prompt_plate"), reply_markup=build_plate_keyboard("mission_start_plate"))
        return
    if data == "show_mission_end":
        await safe_edit_or_send(q, t(user_lang, "mission_end_prompt_plate"), reply_markup=build_plate_keyboard("mission_end_plate"))
        return
    if data == "help":
        await safe_edit_or_send(q, t(user_lang, "help"))
        return

    if data == "admin_finance":
        if (q.from_user.username or "") not in BOT_ADMINS:
            await safe_edit_or_send(q, "❌ Admins only.")
            return
        return await admin_finance_callback_handler(update, context)
    if data.startswith("fin_type|"):
        return await admin_fin_type_selected(update, context)

    if data.startswith("fin_plate|"):
        parts = data.split("|", 2)
        if len(parts) < 3:
            await safe_edit_or_send(q, "Invalid selection.")
            return
        _, typ, plate = parts
        if (q.from_user.username or "") not in BOT_ADMINS:
            await safe_edit_or_send(q, "❌ Admins only.")
            return
        origin_info = {"chat": q.message.chat.id, "msg_id": q.message.message_id, "typ": typ}
        if typ == "odo_fuel":
            context.user_data["pending_fin_multi"] = {"type": "odo_fuel", "plate": plate, "step": "km", "origin": origin_info}
            fr = ForceReply(selective=False)
            try:
                await safe_edit_or_send(q, t(context.user_data.get("lang", DEFAULT_LANG), "enter_odo_km", plate=plate))
                mmsg = await context.bot.send_message(chat_id=q.message.chat.id, text=t(context.user_data.get("lang", DEFAULT_LANG), "enter_odo_km", plate=plate), reply_markup=fr)
                context.user_data["pending_fin_multi"]["prompt_chat"] = mmsg.chat_id
                context.user_data["pending_fin_multi"]["prompt_msg_id"] = mmsg.message_id
            except Exception:
                logger.exception("Failed prompt odo km")
                context.user_data.pop("pending_fin_multi", None)
            return
        if typ in ("parking", "wash", "repair", "fuel"):
            context.user_data["pending_fin_simple"] = {"type": typ, "plate": plate, "origin": origin_info}
            fr = ForceReply(selective=False)
            try:
                await safe_edit_or_send(q, t(context.user_data.get("lang", DEFAULT_LANG), "enter_amount_for", typ=typ, plate=plate))
                mmsg = await context.bot.send_message(chat_id=q.message.chat.id, text=t(context.user_data.get("lang", DEFAULT_LANG), "enter_amount_for", typ=typ, plate=plate), reply_markup=fr)
                context.user_data["pending_fin_simple"]["prompt_chat"] = mmsg.chat_id
                context.user_data["pending_fin_simple"]["prompt_msg_id"] = mmsg.message_id
            except Exception:
                logger.exception("Failed to prompt amount")
                context.user_data.pop("pending_fin_simple", None)
            return

    if data == "leave_menu":
        fr = ForceReply(selective=False)
        try:
            await safe_edit_or_send(q, t(context.user_data.get("lang", DEFAULT_LANG), "leave_prompt"))
            m = await context.bot.send_message(chat_id=q.message.chat.id, text=t(context.user_data.get("lang", DEFAULT_LANG), "leave_prompt"), reply_markup=fr)
            context.user_data["pending_leave"] = {"prompt_chat": m.chat_id, "prompt_msg_id": m.message_id}
        except Exception:
            logger.exception("Failed to prompt leave")
        return

    if data.startswith("mission_start_plate|"):
        _, plate = data.split("|", 1)
        context.user_data["pending_mission"] = {"action": "start", "plate": plate}
        kb = [[InlineKeyboardButton("PP", callback_data=f"mission_depart|PP|{plate}"), InlineKeyboardButton("SHV", callback_data=f"mission_depart|SHV|{plate}")]]
        await safe_edit_or_send(q, t(user_lang, "mission_start_prompt_depart"), reply_markup=InlineKeyboardMarkup(kb))
        return

    if data.startswith("mission_end_plate|"):
        _, plate = data.split("|", 1)
        context.user_data["pending_mission"] = {"action": "end", "plate": plate}
        kb = [[InlineKeyboardButton("Auto detect arrival", callback_data=f"mission_arrival_auto|{plate}")]]
        await safe_edit_or_send(q, t(user_lang, "mission_end_prompt_plate"), reply_markup=InlineKeyboardMarkup(kb))
        return

    if data.startswith("mission_depart|"):
        _, dep, plate = data.split("|", 2)
        pending = context.user_data.get("pending_mission") or {}
        pending["departure"] = dep
        pending["plate"] = plate
        context.user_data["pending_mission"] = pending
        await safe_edit_or_send(q, t(user_lang, "mission_start_ok", driver=q.from_user.username or q.from_user.full_name, plate=plate, dep=dep, ts=now_str()))
        return

    if data.startswith("mission_arrival_auto|"):
        _, plate = data.split("|", 1)
        pending = context.user_data.get("pending_mission") or {}
        # find last open start for this plate/driver
        username = q.from_user.username or q.from_user.full_name
        try:
            ws = open_worksheet(MISSIONS_TAB)
            vals, start_idx = _missions_get_values_and_data_rows(ws)
            found = None
            for i in range(len(vals) - 1, start_idx - 1, -1):
                r = _ensure_row_length(vals[i], M_MANDATORY_COLS)
                if str(r[M_IDX_PLATE]).strip() == plate and str(r[M_IDX_NAME]).strip() == username and not str(r[M_IDX_END]).strip():
                    found = (i, r)
                    break
            if not found:
                await safe_edit_or_send(q, f"No open mission start found for {plate}.")
                context.user_data.pop("pending_mission", None)
                return
            idx, r = found
            dep = str(r[M_IDX_DEPART]).strip()
            arr = "SHV" if dep == "PP" else "PP"
            res = end_mission_record(username, plate, arr)
            if res.get("ok"):
                # send separate departure/arrival messages (single-line each)
                try:
                    await q.message.chat.send_message(t(user_lang, "mission_start_ok", driver=username, plate=plate, dep=dep, ts=r[M_IDX_START]))
                except Exception:
                    pass
                try:
                    await q.message.chat.send_message(t(user_lang, "mission_end_ok", driver=username, plate=plate, arr=arr, ts=now_str()))
                except Exception:
                    pass
                if res.get("merged"):
                    nowdt = _now_dt()
                    month_start = datetime(nowdt.year, nowdt.month, 1)
                    if nowdt.month == 12:
                        month_end = datetime(nowdt.year + 1, 1, 1)
                    else:
                        month_end = datetime(nowdt.year, nowdt.month + 1, 1)
                    counts = count_roundtrips_per_driver_month(month_start, month_end)
                    cnt = counts.get(username, 0)
                    # plate counts
                    p_counts = {}
                    for k, v in counts.items():
                        pass
                    # compute plate counts
                    try:
                        vals2, sidx = _missions_get_values_and_data_rows(open_worksheet(MISSIONS_TAB))
                        p_month = 0
                        p_year = 0
                        for row in vals2[sidx:]:
                            row = _ensure_row_length(row, M_MANDATORY_COLS)
                            if str(row[M_IDX_PLATE]).strip() == plate and str(row[M_IDX_ROUNDTRIP]).strip().lower() == "yes":
                                s = parse_ts(str(row[M_IDX_START]).strip())
                                if s:
                                    if month_start <= s < month_end:
                                        p_month += 1
                                    if datetime(nowdt.year,1,1) <= s < datetime(nowdt.year+1,1,1):
                                        p_year += 1
                    except Exception:
                        logger.exception("Failed plate counts")
                    # mission days + per diem (simple: each merged mission counts 1 day)
                    mission_days = cnt
                    per_diem = mission_days * 15.0
                    try:
                        await q.message.chat.send_message(t(user_lang, "roundtrip_merged_notify", driver=username, count_month=cnt, month=month_start.strftime("%Y-%m"), count_year=0, year=nowdt.year, plate=plate, p_month=p_month, p_year=p_year, days=mission_days, perdiem=per_diem))
                    except Exception:
                        pass
            else:
                await safe_edit_or_send(q, "❌ " + res.get("message", ""))
        except Exception:
            logger.exception("Failed mission arrival auto")
        context.user_data.pop("pending_mission", None)
        return

    if data.startswith("start|") or data.startswith("end|"):
        try:
            action, plate = data.split("|", 1)
        except Exception:
            await safe_edit_or_send(q, "Invalid selection.")
            return
        driver_map = get_driver_map()
        allowed = driver_map.get(username, []) if username else []
        if allowed and plate not in allowed:
            await safe_edit_or_send(q, t(user_lang, "not_allowed", plate=plate))
            return
        if action == "start":
            res = record_start_trip(username, plate)
            if res.get("ok"):
                try:
                    await safe_edit_or_send(q, t(user_lang, "start_ok", driver=username, plate=plate, ts=res.get("ts")))
                except Exception:
                    try:
                        await q.message.chat.send_message(t(user_lang, "start_ok", driver=username, plate=plate, ts=res.get("ts")))
                        await safe_delete_message(context.bot, q.message.chat.id, q.message.message_id)
                    except Exception:
                        pass
            else:
                await safe_edit_or_send(q, "❌ " + res.get("message", ""))
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
                # plate counts
                p_today = 0
                p_month = 0
                p_year = 0
                try:
                    ws = open_worksheet(RECORDS_TAB)
                    vals = ws.get_all_values()
                    if vals:
                        start_idx = 1 if any("date" in c.lower() for c in vals[0] if c) else 0
                        for r in vals[start_idx:]:
                            if len(r) < COL_START:
                                continue
                            dr = r[1] if len(r) > 1 else ""
                            pl = r[2] if len(r) > 2 else ""
                            s_ts = r[3] if len(r) > 3 else ""
                            e_ts = r[4] if len(r) > 4 else ""
                            if pl != plate:
                                continue
                            if not s_ts or not e_ts:
                                continue
                            sdt = parse_ts(s_ts)
                            if not sdt:
                                continue
                            if sdt.date() == nowdt.date():
                                p_today += 1
                            if month_start <= sdt < month_end:
                                p_month += 1
                            if datetime(nowdt.year,1,1) <= sdt < datetime(nowdt.year+1,1,1):
                                p_year += 1
                except Exception:
                    logger.exception("Failed compute plate trip counts")
                try:
                    month_label = month_start.strftime("%Y-%m")
                    year_label = str(nowdt.year)
                    await safe_edit_or_send(q, t(user_lang, "end_ok", driver=username, plate=plate, ts=ts))
                    await q.message.chat.send_message(t(user_lang, "trip_summary", driver=username, n_today=n_today, n_month=n_month, month=month_label, year=year_label, plate=plate, p_today=p_today, p_month=p_month, p_year=p_year))
                except Exception:
                    logger.exception("Failed post-trip messages")
            else:
                await safe_edit_or_send(q, "❌ " + res.get("message", ""))
            return

    await safe_edit_or_send(q, t(user_lang, "invalid_sel"))

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

async def send_daily_summary_job(context: ContextTypes.DEFAULT_TYPE):
    job_data = context.job.data if hasattr(context.job, "data") else {}
    chat_id = job_data.get("chat_id") or SUMMARY_CHAT_ID
    if not chat_id:
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
        logger.exception("Failed to send daily summary")
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
        logger.exception("Failed to aggregate for period")
    return totals

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
        except Exception:
            pass
    except Exception:
        logger.exception("Failed to setup menu")

async def delete_command_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        if update.effective_message:
            await update.effective_message.delete()
    except Exception:
        pass

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
    application.add_handler(CommandHandler("summarize_expenses", summarize_expenses_command) if 'summarize_expenses_command' in globals() else CommandHandler("summarize_expenses", lambda u, c: u.message.reply_text("Use: /summarize_expenses month YYYY-MM or /summarize_expenses year YYYY")))

    application.add_handler(CallbackQueryHandler(plate_callback))

    application.add_handler(MessageHandler(filters.REPLY & filters.TEXT & (~filters.COMMAND), process_force_reply))
    application.add_handler(MessageHandler(filters.TEXT & (~filters.COMMAND), location_or_staff))

    application.add_handler(MessageHandler(filters.Regex(AUTO_KEYWORD_PATTERN) & filters.ChatType.GROUPS, auto_menu_listener))

    application.add_handler(MessageHandler(filters.COMMAND, delete_command_message), group=1)

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
                    BotCommand("summarize_expenses", "Summarize expenses: /summarize_expenses month YYYY-MM or /summarize_expenses year YYYY"),
                ])
            except Exception:
                logger.exception("Failed to set bot commands")
        if hasattr(application, "create_task"):
            application.create_task(_set_cmds())
    except Exception:
        logger.debug("Could not schedule set_my_commands")

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
    except Exception:
        logger.exception("Failed to schedule daily summary")

def _delete_telegram_webhook(token: str) -> bool:
    try:
        url = f"https://api.telegram.org/bot{token}/deleteWebhook"
        req = urllib.request.Request(url, method="POST")
        with urllib.request.urlopen(req, timeout=10) as resp:
            data = resp.read().decode("utf-8", errors="ignore")
            if '"ok":true' in data or '"ok": true' in data:
                return True
            return True
    except Exception:
        return False

def summarize_expenses(period_type: str, period_value: str) -> Dict[str, float]:
    """
    Return dict mapping plate -> total expense for the given period.
    period_type: "month" (YYYY-MM) or "year" (YYYY)
    """
    totals: Dict[str, float] = {}
    try:
        # collect from FUEL, PARKING, WASH, REPAIR and EXPENSE_TAB
        def _collect_from_tab(tab, amount_col_index):
            try:
                ws = open_worksheet(tab)
                vals = ws.get_all_records()
                for r in vals:
                    plate = str(r.get("Plate", r.get("plate", "") )).strip()
                    date_str = str(r.get("DateTime", r.get("Date", r.get("date", "")))).strip()
                    amount = r.get("Fuel Cost", r.get("Amount", r.get("Cost", r.get("Parking Fee", r.get("Other Fee", "")))))
                    try:
                        amt = float(amount) if amount not in (None, "") else 0.0
                    except Exception:
                        # try regex
                        m = re.search(r'(\d+(?:\.\d+)?)', str(amount))
                        amt = float(m.group(1)) if m else 0.0
                    if not plate:
                        continue
                    # filter by period
                    if period_type == "month":
                        try:
                            dt = datetime.strptime(date_str.split()[0], "%Y-%m-%d")
                            if not date_str.startswith(period_value):
                                continue
                        except Exception:
                            continue
                    elif period_type == "year":
                        try:
                            dt = datetime.strptime(date_str.split()[0], "%Y-%m-%d")
                            if dt.year != int(period_value):
                                continue
                        except Exception:
                            continue
                    totals[plate] = totals.get(plate, 0.0) + amt
            except Exception:
                logger.exception("Failed collect from tab %s", tab)

        # tabs to scan
        tabs = [FUEL_TAB, PARKING_TAB, WASH_TAB, REPAIR_TAB, EXPENSE_TAB]
        for tab in tabs:
            _collect_from_tab(tab, None)
    except Exception:
        logger.exception("Failed summarize_expenses")
    return totals

async def summarize_expenses_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        if update.effective_message:
            await update.effective_message.delete()
    except Exception:
        pass
    args = context.args
    if not args or len(args) < 2:
        await update.effective_chat.send_message("Usage: /summarize_expenses month YYYY-MM  OR /summarize_expenses year YYYY")
        return
    mode = args[0].lower()
    value = args[1]
    if mode not in ("month", "year"):
        await update.effective_chat.send_message("Invalid mode. Use month or year.")
        return
    try:
        totals = summarize_expenses(mode, value)
        if not totals:
            await update.effective_chat.send_message("No expenses found for period.")
            return
        lines = [f"Expense summary for {mode} {value}:"]
        for plate, amt in sorted(totals.items(), key=lambda x: -x[1]):
            lines.append(f"{plate}: ${amt:.2f}")
        await update.effective_chat.send_message("\n".join(lines))
    except Exception:
        logger.exception("Failed summarize_expenses_command")
        await update.effective_chat.send_message("Failed to summarize expenses.")

def main():
    ensure_env()
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
        try:
            application.run_webhook(listen="0.0.0.0", port=PORT, webhook_url=WEBHOOK_URL)
        except Exception:
            logger.exception("Failed to start webhook mode")
    else:
        try:
            ok = _delete_telegram_webhook(BOT_TOKEN)
            if not ok:
                logger.warning("deleteWebhook returned non-ok; continuing to polling")
        except Exception:
            logger.exception("deleteWebhook error; continuing to polling")
        try:
            application.run_polling()
        except Exception:
            logger.exception("Polling exited with exception")

if __name__ == "__main__":
    main()
