#!/usr/bin/env python3
"""
driver-bot enhanced version with:
- daily/weekly aggregation written to Google Sheet Summary tab
- driver->plates restriction (env DRIVER_PLATE_MAP JSON or Drivers tab)
- GPS location capture (optional prompt) for start/end
- daily summary message to group
- multi-language support (en, km)
- configurable LOCAL_TZ (IANA)

Save as driver_bot.py and run. Requires:
pip install python-telegram-bot==20.* gspread oauth2client backports.zoneinfo (optional)
"""

import os
import json
import base64
import logging
from datetime import datetime, timedelta, time as dtime
from typing import Optional, Dict, List, Any, Tuple

import gspread
from oauth2client.service_account import ServiceAccountCredentials

# zoneinfo: try stdlib then backports
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
# Local TZ: default to Asia/Phnom_Penh if not set (to match your environment)
_env_tz = os.getenv("LOCAL_TZ")
if _env_tz is None:
    LOCAL_TZ = "Asia/Phnom_Penh"
else:
    LOCAL_TZ = _env_tz.strip() or None

if LOCAL_TZ and ZoneInfo is None:
    logger.warning("LOCAL_TZ set but zoneinfo/backports.zoneinfo not available; falling back to system time.")

PLATES = [p.strip() for p in PLATE_LIST.split(",") if p.strip()]

# Driver map: try env var; if not, will load from Google sheet "Drivers"
DRIVER_PLATE_MAP_JSON = os.getenv("DRIVER_PLATE_MAP", "").strip() or None

# Summary / scheduling
SUMMARY_CHAT_ID = os.getenv("SUMMARY_CHAT_ID")  # chat to send daily summary; optional
SUMMARY_HOUR = int(os.getenv("SUMMARY_HOUR", "20"))  # hour of day in SUMMARY_TZ
SUMMARY_TZ = os.getenv("SUMMARY_TZ", LOCAL_TZ or "Asia/Phnom_Penh")

# Language: 'en' or 'km' (default en)
DEFAULT_LANG = os.getenv("LANG", "en").lower()
SUPPORTED_LANGS = ("en", "km")

# Sheet tabs
RECORDS_TAB = os.getenv("RECORDS_TAB", "Driver_Log")  # raw records
DRIVERS_TAB = os.getenv("DRIVERS_TAB", "Drivers")
SUMMARY_TAB = os.getenv("SUMMARY_TAB", "Summary")

# Column mapping for records (1-indexed)
# We extend columns to store GPS coords
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
# make sure Google sheet has headers reflecting these or script will still append rows (no header enforcement)

# Time formats
TS_FMT = "%Y-%m-%d %H:%M:%S"
DATE_FMT = "%Y-%m-%d"

# Google scopes
SCOPES = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]

# Conversation states for location collection
ASK_LOC_FOR = 1  # state: waiting for location after asking start/end

# Persistence keys in context.user_data: "pending_action" -> ("start" or "end", plate)
#                    "lang" -> per-user language preference

# ===== Translations =====
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
    },
    "km": {
        # Khmer translations (short/simple). You can refine these strings as needed.
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
    },
}


# ===== Helper: i18n =====
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


# ===== Driver->plates management =====
def load_driver_map_from_env() -> Dict[str, List[str]]:
    if not DRIVER_PLATE_MAP_JSON:
        return {}
    try:
        obj = json.loads(DRIVER_PLATE_MAP_JSON)
        # normalize to username -> [plates]
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
    # set start gps if provided (cols 7/8)
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
    """
    Compute total minutes per plate between start_date (inclusive) and end_date (exclusive).
    Returns dict plate->minutes.
    """
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
            # if both exist, consider portion overlapping period
            if s_dt and e_dt:
                # treat as naive local datetimes matching sheet times
                # convert to date for range check
                # Overlap calculation:
                actual_start = max(s_dt, start_date)
                actual_end = min(e_dt, end_date)
                if actual_end > actual_start:
                    minutes = int((actual_end - actual_start).total_seconds() // 60)
                    totals[plate] = totals.get(plate, 0) + minutes
            else:
                # skip incomplete record for aggregation
                continue
        return totals
    except Exception:
        logger.exception("Failed to aggregate records")
        return {}


def minutes_to_h_m(total_minutes: int) -> Tuple[int, int]:
    h = total_minutes // 60
    m = total_minutes % 60
    return h, m


def write_daily_summary(date_dt: datetime) -> str:
    """
    Aggregate for the date (00:00 to 00:00 next day) and write a row to SUMMARY_TAB.
    Returns text summary to send in chat.
    """
    start = datetime.combine(date_dt.date(), dtime.min)
    end = start + timedelta(days=1)
    totals = aggregate_for_period(start, end)
    if not totals:
        return t(DEFAULT_LANG, "no_records", date=start.strftime(DATE_FMT))
    # Prepare summary lines
    lines = []
    for plate, minutes in sorted(totals.items()):
        h, m = minutes_to_h_m(minutes)
        lines.append(t(DEFAULT_LANG, "summary_row", plate=plate, hours=h, minutes=m))
    # write to summary sheet: create if missing
    try:
        ws = open_worksheet(SUMMARY_TAB)
        # append a row: Date, PeriodType (daily), TotalMinutesJSON, HumanText
        row = [start.strftime(DATE_FMT), "daily", json.dumps(totals), "\n".join(lines)]
        ws.append_row(row, value_input_option="USER_ENTERED")
    except Exception:
        logger.exception("Failed to write daily summary to sheet.")
    header = t(DEFAULT_LANG, "summary_subject", date=start.strftime(DATE_FMT))
    return header + "\n" + "\n".join(lines)


def write_weekly_summary(week_start_dt: datetime) -> str:
    """
    Aggregate for week (week_start_dt inclusive, +7 days).
    week_start_dt should be the monday of the week or any chosen boundary.
    """
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


# ===== Telegram UI helpers =====
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


# ===== Conversation & callbacks =====
async def menu_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_lang = context.user_data.get("lang", DEFAULT_LANG)
    text = t(user_lang, "menu")
    keyboard = [
        [InlineKeyboardButton("Start trip (select plate)", callback_data="show_start"),
         InlineKeyboardButton("End trip (select plate)", callback_data="show_end")],
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


async def plate_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    data = query.data
    user = query.from_user
    username = user.username or f"{user.first_name or ''} {user.last_name or ''}".strip()
    user_lang = context.user_data.get("lang", DEFAULT_LANG)

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

    try:
        action, plate = data.split("|", 1)
    except Exception:
        await query.edit_message_text(t(user_lang, "invalid_sel"))
        return

    # Authorization check
    driver_map = get_driver_map()
    if username and driver_map:
        allowed = driver_map.get(username, [])
        if allowed and plate not in allowed:
            await query.edit_message_text(t(user_lang, "not_allowed", plate=plate))
            return

    # If authorized, ask for optional location: prompt and set pending_action in user_data
    context.user_data["pending_action"] = (action, plate)
    context.user_data["pending_from_query_id"] = query.message.message_id
    # ask for location
    await query.edit_message_text(t(user_lang, "prompt_location"))
    # ask user to send location or /skip
    return  # next message handler will catch location and /skip


# Handler for location messages or /skip after a pending_action exists
async def location_or_skip(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    user_lang = context.user_data.get("lang", DEFAULT_LANG)
    pending = context.user_data.get("pending_action")
    if not pending:
        # ignore stray locations
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
        # user sent other text — ignore (but could prompt)
        await update.message.reply_text(t(user_lang, "prompt_send_loc"))
        return

    # perform action now
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
    # clear pending
    context.user_data.pop("pending_action", None)
    context.user_data.pop("pending_from_query_id", None)


# /lang command
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


# Auto keyword listener (group) - optional quick menu
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
    # job run: compute yesterday's summary (or today's depending on desired)
    # We'll aggregate for previous day (yesterday)
    job_data = context.job.data if hasattr(context.job, "data") else {}
    chat_id = job_data.get("chat_id") or SUMMARY_CHAT_ID
    if not chat_id:
        logger.info("SUMMARY_CHAT_ID not set; skipping daily summary.")
        return
    # decide date to summarize: yesterday in SUMMARY_TZ
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
    application.add_handler(CallbackQueryHandler(plate_callback))
    # location/skip handler: catch Location messages or /skip text
    application.add_handler(MessageHandler(filters.LOCATION | filters.Regex(r'(?i)^/skip$'), location_or_skip))
    application.add_handler(MessageHandler(filters.Regex(AUTO_KEYWORD_PATTERN) & filters.ChatType.GROUPS, auto_menu_listener))
    # help fallback
    application.add_handler(CommandHandler("help", lambda u, c: u.message.reply_text(t(c.user_data.get("lang", DEFAULT_LANG), "help"))))

    # set bot commands
    try:
        async def _set_cmds():
            try:
                await application.bot.set_my_commands([
                    BotCommand("start_trip", "Start a trip (select plate)"),
                    BotCommand("end_trip", "End a trip (select plate)"),
                    BotCommand("menu", "Open trip menu"),
                    BotCommand("lang", "Set language /lang en|km"),
                ])
            except Exception:
                logger.exception("Failed to set bot commands.")
        if hasattr(application, "create_task"):
            application.create_task(_set_cmds())
    except Exception:
        logger.debug("Could not schedule set_my_commands.")


# Main
def ensure_env():
    if not BOT_TOKEN:
        raise RuntimeError(t(DEFAULT_LANG, "no_bot_token"))


def schedule_daily_summary(application):
    # schedule job at SUMMARY_HOUR in SUMMARY_TZ
    try:
        if SUMMARY_CHAT_ID:
            if ZoneInfo and SUMMARY_TZ:
                tz = ZoneInfo(SUMMARY_TZ)
            else:
                tz = None
            # schedule using job_queue
            job_kwargs = {"days": 1}
            # determine time object
            job_time = dtime(hour=SUMMARY_HOUR, minute=0, second=0)
            application.job_queue.run_daily(send_daily_summary_job, time=job_time, context={"chat_id": SUMMARY_CHAT_ID}, name="daily_summary", tz=tz)
            logger.info("Scheduled daily summary at %02d:00 (%s) to %s", SUMMARY_HOUR, SUMMARY_TZ, SUMMARY_CHAT_ID)
        else:
            logger.info("SUMMARY_CHAT_ID not configured; daily summary disabled.")
    except Exception:
        logger.exception("Failed to schedule daily summary.")


def main():
    ensure_env()
    # log timezone
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

    # Build application with persistence (optional)
    persistence = None
    try:
        # create a small file-based persistence to remember per-user lang across restarts
        persistence = PicklePersistence(filepath="driver_bot_persistence.pkl")
    except Exception:
        persistence = None

    application = ApplicationBuilder().token(BOT_TOKEN).persistence(persistence).build()

    register_ui_handlers(application)

    # schedule daily summary job if configured
    schedule_daily_summary(application)

    # start
    logger.info("Starting driver-bot polling...")
    application.run_polling()


if __name__ == "__main__":
    main()
