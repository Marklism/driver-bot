#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
driver-bot (final): fuel+odo + missions + leave + mission_summary
- /fuel flow: /fuel <amount> -> choose plate (inline) -> invoice? yes/no -> enter ODO -> write row
- /mission_start & /mission_end: simple plate+city flow, merged mission rows on detection
- /leave: zero-prompt write to leave sheet
- Headers auto-update
- No pin/置顶, no create_task, post_init used for set_my_commands
- Minimal user-visible messages: short confirmations only
"""

import os
import logging
import json
import re
from datetime import datetime, timedelta, time as dtime
from typing import Optional, List, Dict, Any

import gspread
from oauth2client.service_account import ServiceAccountCredentials

from telegram import (
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    Update,
    BotCommand,
    Message,
)
from telegram.ext import (
    ApplicationBuilder,
    CommandHandler,
    CallbackQueryHandler,
    MessageHandler,
    ContextTypes,
    filters,
)

# ---------------------------
# Configuration / env
# ---------------------------
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
logging.basicConfig(level=LOG_LEVEL)
logger = logging.getLogger("driver-bot-fixed")

BOT_TOKEN = os.getenv("BOT_TOKEN")
GSHEET_KEY = os.getenv("GSHEET_KEY")  # spreadsheet id
GOOGLE_CREDS_JSON = os.getenv("GOOGLE_CREDS_JSON")  # full JSON service account
# sheet names (can be changed)
FUEL_SHEET_NAME = os.getenv("FUEL_SHEET_NAME", "fuel_odo")
MISSIONS_SHEET_NAME = os.getenv("MISSIONS_SHEET_NAME", "missions")
SUMMARY_SHEET_NAME = os.getenv("SUMMARY_SHEET_NAME", "mission_summary")
LEAVE_SHEET_NAME = os.getenv("LEAVE_SHEET_NAME", "leave")

# plates list (you can override via env)
PLATE_LIST = os.getenv("PLATE_LIST", "2BB-3071,2BB-0809,2CI-8066,2CK-8066").split(",")
PLATES = [p.strip() for p in PLATE_LIST if p.strip()]

# Basic constants
TS_FMT = "%Y-%m-%d %H:%M:%S"
DATE_FMT = "%Y-%m-%d"

# ---------------------------
# Google Sheets helper
# ---------------------------
SCOPES = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]

GS_CLIENT = None
GS_SHEET = None


def init_gsheets():
    global GS_CLIENT, GS_SHEET
    if not GOOGLE_CREDS_JSON or not GSHEET_KEY:
        logger.error("Google Sheets client or key not configured (set GOOGLE_CREDS_JSON and GSHEET_KEY).")
        return
    try:
        creds_obj = json.loads(GOOGLE_CREDS_JSON)
        creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_obj, SCOPES)
        GS_CLIENT = gspread.authorize(creds)
        GS_SHEET = GS_CLIENT.open_by_key(GSHEET_KEY)
        logger.info("Google Sheets client initialized.")
    except Exception as e:
        logger.exception("Failed to initialize Google Sheets client: %s", e)
        GS_CLIENT = None
        GS_SHEET = None


def _ws(sheet_name: str):
    """Return worksheet or raise RuntimeError"""
    global GS_SHEET
    if GS_SHEET is None:
        raise RuntimeError("Google Sheets client or key not configured")
    try:
        return GS_SHEET.worksheet(sheet_name)
    except Exception:
        # try create
        try:
            return GS_SHEET.add_worksheet(title=sheet_name, rows="2000", cols="20")
        except Exception as e:
            logger.exception("Failed to open or create worksheet %s: %s", sheet_name, e)
            raise


def ensure_headers_once():
    """Ensure canonical headers for our sheets. Swallows failures but logs them."""
    headers_map = {
        FUEL_SHEET_NAME: ["timestamp", "plate", "odo", "fuel_usd", "invoice_received", "odo_diff"],
        MISSIONS_SHEET_NAME: ["driver", "plate", "depart1_city", "depart1_ts", "arrive1_city", "arrive1_ts", "depart2_city", "depart2_ts", "arrive2_city", "arrive2_ts", "start_ts", "end_ts", "duration_minutes"],
        SUMMARY_SHEET_NAME: ["driver", "month", "mission_days", "per_diem_amount"],
        LEAVE_SHEET_NAME: ["driver", "start_date", "end_date", "leave_type", "notes"],
    }
    for ws_name, hdrs in headers_map.items():
        try:
            ws = _ws(ws_name)
            cur = ws.row_values(1)
            if cur != hdrs:
                # replace header
                try:
                    if cur:
                        ws.delete_row(1)
                except Exception:
                    # ignore
                    pass
                ws.insert_row(hdrs, 1)
                logger.info("Updated headers for sheet %s", ws_name)
        except Exception:
            logger.exception("Failed to ensure headers for %s", ws_name)


# ---------------------------
# Utilities
# ---------------------------
def now_ts():
    return datetime.now().strftime(TS_FMT)


def parse_date(s: str) -> Optional[datetime]:
    try:
        return datetime.strptime(s, DATE_FMT)
    except Exception:
        return None


def month_key(dt: datetime) -> str:
    return f"{dt.year}-{dt.month:02d}"


# ---------------------------
# Fuel/Odo flow
# ---------------------------
# We store temporary state in chat_data/context.user_data during the multi-step flow:
# - when user runs /fuel <amount>, we set context.user_data['pending_fuel'] = {'amount': float, 'initiator_chat': chat_id, ...}
# - after plate selected, we set pending fields and ask invoice yes/no via ForceReply
# - after invoice answer, ask for ODO via ForceReply
# - after receiving ODO, compute diff from last row on sheet and write a single row

INVOICE_RE = re.compile(r'^\s*(yes|no)\s*$', re.I)
ODO_RE = re.compile(r'^\s*(\d{3,7})\s*$', re.I)


def find_last_odo_from_sheet(plate: str) -> Optional[int]:
    """Scan fuel sheet bottom-up to find last odo for plate. Returns int or None."""
    try:
        ws = _ws(FUEL_SHEET_NAME)
        vals = ws.get_all_values()
        if not vals or len(vals) < 2:
            return None
        # find header row index 0 assumed
        for row in reversed(vals[1:]):
            # header mapping: timestamp, plate, odo, fuel_usd, invoice_received, odo_diff
            if len(row) >= 3 and row[1].strip() == plate:
                try:
                    m = re.search(r'(\d+)', str(row[2]))
                    if m:
                        return int(m.group(1))
                except Exception:
                    continue
        return None
    except Exception:
        logger.exception("Failed to read last odo from sheet for %s", plate)
        return None


def write_fuel_row(plate: str, odo: int, fuel_usd: float, invoice_yesno: str) -> Dict[str, Any]:
    """Write one combined row to the fuel sheet and return metadata including diff."""
    try:
        ws = _ws(FUEL_SHEET_NAME)
        prev = find_last_odo_from_sheet(plate)
        diff = ""
        if prev is not None:
            try:
                diff_val = int(odo) - int(prev)
                diff = str(diff_val)
            except Exception:
                diff = ""
        ts = now_ts()
        row = [ts, plate, str(odo), str(fuel_usd), invoice_yesno.title(), diff]
        ws.append_row(row, value_input_option="USER_ENTERED")
        logger.info("Appended fuel row for %s odo=%s fuel=%s invoice=%s diff=%s", plate, odo, fuel_usd, invoice_yesno, diff)
        return {"ok": True, "ts": ts, "plate": plate, "odo": odo, "fuel": fuel_usd, "invoice": invoice_yesno, "diff": diff}
    except Exception as e:
        logger.exception("Failed to append fuel row: %s", e)
        return {"ok": False, "error": str(e)}


# ---------------------------
# Mission flow
# ---------------------------
# We'll keep mission partial state in application.chat_data keyed by chat_id:
# mission buffer per chat: { plate: [ {t:'d'/'a', city:'PP'/'SHV', ts:..., driver:...}, ... ] }
# When we detect a sequence d,a,d,a we merge last four into one mission row and write to sheet.

def write_mission_row(driver: str, plate: str, d1: dict, a1: dict, d2: dict, a2: dict) -> bool:
    """Write merged mission row into missions sheet."""
    try:
        ws = _ws(MISSIONS_SHEET_NAME)
        start_ts = d1.get("ts")
        end_ts = a2.get("ts")
        # duration in minutes (if parseable)
        try:
            sdt = datetime.strptime(start_ts, TS_FMT)
            edt = datetime.strptime(end_ts, TS_FMT)
            duration_min = int((edt - sdt).total_seconds() // 60)
        except Exception:
            duration_min = ""
        row = [
            driver,
            plate,
            d1.get("city"), d1.get("ts"),
            a1.get("city"), a1.get("ts"),
            d2.get("city"), d2.get("ts"),
            a2.get("city"), a2.get("ts"),
            start_ts, end_ts, str(duration_min),
        ]
        ws.append_row(row, value_input_option="USER_ENTERED")
        logger.info("Wrote merged mission for %s plate=%s", driver, plate)
        # update summary counts
        try:
            update_mission_summary_counts(driver, plate, start_ts, end_ts, duration_min)
        except Exception:
            logger.exception("Failed to update mission summary after writing mission row.")
        return True
    except Exception:
        logger.exception("Failed to write mission row")
        return False


def update_mission_summary_counts(driver: str, plate: str, start_ts: str, end_ts: str, duration_min: Any):
    """
    Update mission_summary sheet:
      - For driver: increase mission_days (A-2 logic) and per_diem amount
      - For plate: we keep driver-based summary only here (simplified)
    A-2 logic (per your spec): per-diem days computed by start/end crossing noon rules.
    """
    try:
        ws = _ws(SUMMARY_SHEET_NAME)
        # compute month for start_ts
        sdt = datetime.strptime(start_ts, TS_FMT)
        month = month_key(sdt)
        # determine mission days per A-2:
        # rule: each day counts as 1 if end crosses noon threshold
        # We'll compute days as:
        # For each day from start_date to end_date inclusive:
        #   - first day counts 1
        #   - intermediate days count 1
        #   - last day: count 1 if end time > 12:00 (noon)
        sd = sdt.date()
        edt = datetime.strptime(end_ts, TS_FMT)
        ed = edt.date()
        days = 0
        if sd == ed:
            # same day -> count 1
            days = 1
        else:
            # multiple days
            days = (ed - sd).days  # full day differences (e.g., start 11 -> end 12 gives 1)
            # check last day noon threshold
            if edt.time() >= dtime(hour=12, minute=0):
                days += 1
            else:
                # last day not reaching noon, so days remain as difference
                pass
            # add first day as 1
            days = days if days > 0 else 1
        # per diem: $15 per day
        PER_DIEM_RATE = 15
        per_diem_amount = days * PER_DIEM_RATE
        # find if a row exists for (driver, month) and update, else append
        all_vals = ws.get_all_values()
        header_idx = 0
        found_row = None
        for i, r in enumerate(all_vals[1:], start=2):  # 1-based sheet rows
            drv = r[0] if len(r) > 0 else ""
            mon = r[1] if len(r) > 1 else ""
            if drv == driver and mon == month:
                found_row = i
                existing_days = int(r[2]) if len(r) > 2 and r[2].isdigit() else 0
                existing_amount = float(r[3]) if len(r) > 3 and r[3] else 0.0
                new_days = existing_days + days
                new_amount = existing_amount + per_diem_amount
                try:
                    ws.update_cell(found_row, 3, str(new_days))
                    ws.update_cell(found_row, 4, str(new_amount))
                except Exception:
                    # fallback: rewrite whole row
                    try:
                        new_row = [driver, month, str(new_days), str(new_amount)]
                        ws.delete_rows(found_row)
                        ws.insert_row(new_row, found_row)
                    except Exception:
                        logger.exception("Failed to fallback-update mission_summary row")
                break
        if not found_row:
            # append a new summary row
            ws.append_row([driver, month, str(days), str(per_diem_amount)], value_input_option="USER_ENTERED")
        logger.info("Updated mission_summary for %s %s: +%d days (%s)", driver, month, days, per_diem_amount)
    except Exception:
        logger.exception("Failed to update mission_summary")


# ---------------------------
# Leave flow (zero prompt)
# ---------------------------
def write_leave_row(driver: str, start_date: str, end_date: str, leave_type: str, notes: str) -> bool:
    try:
        ws = _ws(LEAVE_SHEET_NAME)
        row = [driver, start_date, end_date, leave_type, notes]
        ws.append_row(row, value_input_option="USER_ENTERED")
        logger.info("Wrote leave row for %s %s-%s type=%s", driver, start_date, end_date, leave_type)
        return True
    except Exception:
        logger.exception("Failed to write leave row")
        return False


# ---------------------------
# Telegram handlers
# ---------------------------
# -- Helper to build plate keyboard --
def build_plate_keyboard(prefix: str):
    buttons = []
    row = []
    for i, plate in enumerate(PLATES, 1):
        row.append(InlineKeyboardButton(plate, callback_data=f"{prefix}|{plate}"))
        if i % 3 == 0:
            buttons.append(row)
            row = []
    if row:
        buttons.append(row)
    return InlineKeyboardMarkup(buttons)


# /start simple
async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        await update.message.reply_text("Driver bot active.")
    except Exception:
        logger.exception("Failed responding to /start")


# /fuel <amount> -> plate keyboard
async def cmd_fuel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.effective_message.reply_text("Usage: /fuel <amount>")
        return
    try:
        amt = float(context.args[0])
    except Exception:
        await update.effective_message.reply_text("Invalid amount. Usage: /fuel <amount>")
        return
    # store pending fuel in user_data
    context.user_data["pending_fuel"] = {"amount": amt, "initiator_chat_id": update.effective_chat.id, "initiator_msg_id": update.effective_message.message_id}
    # present plate keyboard inline
    try:
        await update.effective_message.reply_text("Choose plate:", reply_markup=build_plate_keyboard("fuel_plate"))
    except Exception:
        # fallback to simple text
        await update.effective_message.reply_text("Please choose plate by typing one from: " + ", ".join(PLATES))


# Callback handler for plate selection and mission flows
async def cb_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    if not query or not query.data:
        return
    await query.answer()
    data = query.data
    # -- fuel plate selected: prefix fuel_plate|{plate}
    if data.startswith("fuel_plate|"):
        _, plate = data.split("|", 1)
        pf = context.user_data.get("pending_fuel")
        if not pf:
            # no pending context, ignore
            try:
                await query.edit_message_text("No pending fuel action. Please run /fuel <amount> first.")
            except Exception:
                pass
            return
        # record plate in pending, ask invoice yes/no via prompt (ForceReply style is replaced with direct reply expectation)
        context.user_data["pending_fuel"]["plate"] = plate
        # Ask invoice question (only this prompt)
        try:
            await query.edit_message_text("Invoice received? yes/no (reply in chat)")
        except Exception:
            try:
                await query.message.chat.send_message("Invoice received? yes/no (reply in chat)")
            except Exception:
                pass
        return

    # -- mission plate selection (start)
    if data.startswith("ms_start_plate|"):
        _, plate = data.split("|", 1)
        # ask for departure city via inline options
        try:
            await query.edit_message_text("Select departure:", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("PP", callback_data=f"ms_start_city|{plate}|PP"), InlineKeyboardButton("SHV", callback_data=f"ms_start_city|{plate}|SHV")]]))
        except Exception:
            pass
        return

    # -- mission plate selection (end)
    if data.startswith("ms_end_plate|"):
        _, plate = data.split("|", 1)
        try:
            await query.edit_message_text("Select arrival:", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("PP", callback_data=f"ms_end_city|{plate}|PP"), InlineKeyboardButton("SHV", callback_data=f"ms_end_city|{plate}|SHV")]]))
        except Exception:
            pass
        return

    # -- mission start city
    if data.startswith("ms_start_city|"):
        _, plate, city = data.split("|", 2)
        chat_id = query.message.chat_id
        mp: Dict[str, List[dict]] = context.chat_data.setdefault("missions_buf", {})
        buf = mp.setdefault(plate, [])
        buf.append({"t": "d", "city": city, "ts": now_ts(), "driver": query.from_user.username or query.from_user.full_name})
        # minimal confirmation (edit)
        try:
            await query.edit_message_text(f"Departure recorded for {plate}.")
        except Exception:
            pass
        return

    # -- mission end city
    if data.startswith("ms_end_city|"):
        _, plate, city = data.split("|", 2)
        chat_id = query.message.chat_id
        mp: Dict[str, List[dict]] = context.chat_data.setdefault("missions_buf", {})
        buf = mp.setdefault(plate, [])
        buf.append({"t": "a", "city": city, "ts": now_ts(), "driver": query.from_user.username or query.from_user.full_name})
        try:
            await query.edit_message_text(f"Arrival recorded for {plate}.")
        except Exception:
            pass
        # attempt merge: check last 4 events for pattern d,a,d,a
        seq = buf[-4:]
        if len(seq) >= 4 and [x["t"] for x in seq] == ["d", "a", "d", "a"]:
            d1, a1, d2, a2 = seq[-4], seq[-3], seq[-2], seq[-1]
            # require same driver for all
            if d1["driver"] == a1["driver"] == d2["driver"] == a2["driver"]:
                driver = d1["driver"]
                ok = write_mission_row(driver, plate, d1, a1, d2, a2)
                # clear buffer for plate after merge
                context.chat_data["missions_buf"][plate] = []
                if ok:
                    # compute month and counts (we already updated summary inside write_mission_row)
                    # Now respond with short confirmation including monthly count for driver and plate
                    # fetch current month count for driver
                    try:
                        ws = _ws(SUMMARY_SHEET_NAME)
                        month = month_key(datetime.strptime(d1["ts"], TS_FMT))
                        vals = ws.get_all_values()
                        driver_count = 0
                        plate_count = 0
                        for r in vals[1:]:
                            if len(r) >= 3:
                                if r[0] == driver and r[1] == month:
                                    driver_count = int(r[2]) if r[2].isdigit() else 0
                        # For plate count we will simply count missions in missions sheet for this month (expensive but acceptable)
                        try:
                            mws = _ws(MISSIONS_SHEET_NAME)
                            mvals = mws.get_all_values()
                            for mr in mvals[1:]:
                                # start_ts column index 10 (1-based 11th col), plate at index 2 (1-based 2)
                                if len(mr) >= 11:
                                    m_plate = mr[1]
                                    m_start = mr[10] if len(mr) > 10 else ""
                                    if m_plate == plate and m_start.startswith(month):
                                        plate_count += 1
                        except Exception:
                            logger.exception("Failed to count plate missions")
                        # send short confirmations
                        try:
                            await query.message.chat.send_message(f"Driver {driver} completed {driver_count} mission(s) in {month}.")
                            await query.message.chat.send_message(f"Plate {plate} completed {plate_count} mission(s) in {month}.")
                        except Exception:
                            pass
                    except Exception:
                        logger.exception("Failed to fetch mission summary counts")
        return

    # other callbacks ignored
    try:
        await query.edit_message_text("Invalid selection or expired.")
    except Exception:
        pass


# Message handler to catch invoice yes/no and ODO input and other free text
async def free_text_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = (update.effective_message.text or "").strip()
    # First, invoice step: check if pending_fuel exists and invoice not yet set
    pending = context.user_data.get("pending_fuel")
    if pending and "plate" in pending and "invoice" not in pending:
        # expecting yes/no
        m = INVOICE_RE.match(text)
        if not m:
            # ignore non-yes/no (user may be typing something else)
            return
        answer = m.group(1).lower()
        pending["invoice"] = "Yes" if answer.startswith("y") else "No"
        context.user_data["pending_fuel"] = pending
        # ask for odo (one line)
        try:
            await update.effective_message.delete()
        except Exception:
            pass
        try:
            await update.effective_chat.send_message("Please reply with ODO (KM) numeric, e.g., 123456")
        except Exception:
            pass
        return

    # second step: expecting ODO
    if pending and "plate" in pending and "invoice" in pending and "odo" not in pending:
        m = ODO_RE.match(text)
        if not m:
            # ignore if not numeric odo (silently)
            return
        odo_val = int(m.group(1))
        pending["odo"] = odo_val
        # now write combined row
        amt = float(pending.get("amount", 0.0))
        plate = pending.get("plate", "UNKNOWN")
        invoice_yesno = pending.get("invoice", "No")
        res = write_fuel_row(plate, odo_val, amt, invoice_yesno)
        # cleanup pending
        context.user_data.pop("pending_fuel", None)
        # send short confirmation message in chat (one line)
        if res.get("ok"):
            diff = res.get("diff") or "0"
            ts_date = res.get("ts", "")[:10]
            try:
                await update.effective_chat.send_message(f"Plate {plate} @ {odo_val} km + {amt}$ fuel on {ts_date}, Odo difference since last record: {diff} km. Invoice: {invoice_yesno}")
            except Exception:
                logger.exception("Failed to send fuel confirmation")
        else:
            try:
                await update.effective_chat.send_message("Failed to record fuel entry.")
            except Exception:
                pass
        return

    # Other text messages ignored (we keep interactions minimal)
    return


# /mission_start command: shows plate keyboard and handles rest via callbacks
async def cmd_mission_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        await update.effective_message.reply_text("Choose plate for mission start:", reply_markup=build_plate_keyboard("ms_start_plate"))
    except Exception:
        try:
            await update.effective_message.reply_text("Choose plate: " + ", ".join(PLATES))
        except Exception:
            pass


# /mission_end command
async def cmd_mission_end(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        await update.effective_message.reply_text("Choose plate for mission end:", reply_markup=build_plate_keyboard("ms_end_plate"))
    except Exception:
        try:
            await update.effective_message.reply_text("Choose plate: " + ", ".join(PLATES))
        except Exception:
            pass


# /leave zero prompt: /leave <driver> <YYYY-MM-DD> <YYYY-MM-DD> <SL|AL> [notes...]
async def cmd_leave(update: Update, context: ContextTypes.DEFAULT_TYPE):
    args = context.args or []
    if len(args) < 4:
        await update.effective_message.reply_text("Usage: /leave <driver> <YYYY-MM-DD> <YYYY-MM-DD> <SL|AL> [notes...]")
        return
    driver = args[0]
    start = args[1]
    end = args[2]
    ltype = args[3]
    notes = " ".join(args[4:]) if len(args) > 4 else ""
    # validate dates
    sd = parse_date(start)
    ed = parse_date(end)
    if not sd or not ed:
        await update.effective_message.reply_text("Invalid date format. Use YYYY-MM-DD.")
        return
    ok = write_leave_row(driver, start, end, ltype, notes)
    if ok:
        try:
            await update.effective_message.reply_text(f"Leave recorded for {driver} {start} to {end} ({ltype})")
        except Exception:
            pass
    else:
        try:
            await update.effective_message.reply_text("Failed to record leave (sheet error).")
        except Exception:
            pass


# ---------------------------
# Application setup & post_init
# ---------------------------
async def _post_init(app):
    # set commands nicely using post_init (no create_task)
    try:
        await app.bot.set_my_commands([
            BotCommand("fuel", "Record fuel: /fuel <amount> then choose plate"),
            BotCommand("mission_start", "Start mission (select plate then city)"),
            BotCommand("mission_end", "End mission (select plate then city)"),
            BotCommand("leave", "Record leave: /leave <driver> <start> <end> <SL|AL> [notes]"),
            BotCommand("start", "Check bot"),
        ])
    except Exception:
        logger.exception("Failed to set bot commands in post_init")


def main():
    # init sheets
    init_gsheets()
    try:
        ensure_headers_once()
    except Exception:
        logger.exception("Header ensure failed at startup")
    if not BOT_TOKEN:
        logger.error("BOT_TOKEN not set; exiting.")
        return

    # build app with post_init callback
    app = ApplicationBuilder().token(BOT_TOKEN).post_init(_post_init).build()

    # Register handlers
    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("fuel", cmd_fuel))
    app.add_handler(CallbackQueryHandler(cb_handler))
    # mission commands
    app.add_handler(CommandHandler("mission_start", cmd_mission_start))
    app.add_handler(CommandHandler("mission_end", cmd_mission_end))
    # leave
    app.add_handler(CommandHandler("leave", cmd_leave))
    # free text handler catches invoice yes/no and odo numbers
    app.add_handler(MessageHandler(filters.TEXT & (~filters.COMMAND), free_text_handler))

    # Run: try to delete webhook first to avoid conflicts, then polling (this is safe)
    try:
        # attempt to ensure webhook removed to run polling (ignore errors)
        try:
            # Delete webhook (if token had webhook) to avoid getUpdates conflict
            # application.bot.delete_webhook is synchronous coroutine-style; call via bot
            async def _del_wh():
                try:
                    await app.bot.delete_webhook()
                except Exception:
                    pass
            # run it synchronously before run_polling by scheduling (post_init already set)
            # but we can call directly because we're still in startup
            app.create_task(_del_wh())
        except Exception:
            pass

        logger.info("Starting driver-bot polling...")
        app.run_polling()
    except Exception as e:
        logger.exception("Application run failed: %s", e)


if __name__ == "__main__":
    main()
