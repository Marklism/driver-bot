
# ================= A5 FIXED: OT REPORT (Drivers-based buttons) =================
# Buttons source: Drivers!Username
# Data source: OT Record
# Period: 16th 04:00 -> next 16th 04:00

from datetime import datetime, timedelta, time as dtime
from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.ext import ContextTypes, CommandHandler, CallbackQueryHandler
import csv, io, os

LOCAL_TZ = os.getenv("LOCAL_TZ", "Asia/Phnom_Penh")

def _parse_period(yyyymm: str):
    base = datetime.strptime(yyyymm, "%Y-%m")
    start = base.replace(day=16, hour=4, minute=0, second=0)
    if base.month == 12:
        end = start.replace(year=base.year + 1, month=1)
    else:
        end = start.replace(month=base.month + 1)
    return start, end

async def ot_report_entry(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.message.reply_text("Usage: /ot_report YYYY-MM")
        return

    yyyymm = context.args[0]
    try:
        start, end = _parse_period(yyyymm)
    except Exception:
        await update.message.reply_text("Invalid format. Use YYYY-MM")
        return

    # --- LOAD DRIVERS ---
    drivers_rows = read_sheet("Drivers")
    usernames = [r.get("Username") for r in drivers_rows if r.get("Username")]

    if not usernames:
        await update.message.reply_text("No drivers found in Drivers sheet.")
        return

    keyboard = [
        [InlineKeyboardButton(u, callback_data=f"otrep|{yyyymm}|{u}")]
        for u in usernames
    ]

    text = (
        "OT Report period:\n"
        f"{start:%Y-%m-%d %H:%M:%S} → {end:%Y-%m-%d %H:%M:%S}\n\n"
        "Select driver:"
    )

    await update.message.reply_text(
        text,
        reply_markup=InlineKeyboardMarkup(keyboard)
    )

async def ot_report_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()

    _, yyyymm, username = q.data.split("|")
    start, end = _parse_period(yyyymm)

    rows = read_sheet("OT Record")

    out = io.StringIO()
    writer = csv.writer(out)
    writer.writerow(["Username", "Date", "OT_Hours"])

    total = 0.0
    for r in rows:
        if r.get("Username") != username:
            continue
        try:
            ts = datetime.fromisoformat(r.get("DateTime"))
        except Exception:
            continue
        if start <= ts < end:
            h = float(r.get("OT_Hours") or 0)
            total += h
            writer.writerow([username, ts.date(), f"{h:.2f}"])

    out.seek(0)
    filename = f"ot_{username}_{yyyymm}.csv"
    await q.message.reply_document(
        document=io.BytesIO(out.getvalue().encode()),
        filename=filename,
        caption=f"{username} total OT: {total:.2f} hour(s)"
    )

def register_ot_report(application):
    application.add_handler(CommandHandler("ot_report", ot_report_entry))
    application.add_handler(CallbackQueryHandler(ot_report_callback, pattern=r"^otrep\|"))

# ================= END A5 FIX =================
