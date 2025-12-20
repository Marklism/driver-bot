#!/usr/bin/env python3
import os
import logging
import csv
import io
from datetime import datetime, timedelta

import gspread
from google.oauth2.service_account import Credentials

from telegram import (
    Update,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    BotCommand,
)
from telegram.ext import (
    ApplicationBuilder,
    CommandHandler,
    CallbackQueryHandler,
    ContextTypes,
)

# -------------------- BASIC SETUP --------------------
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("driver-bot")

BOT_TOKEN = os.getenv("BOT_TOKEN")
SHEET_ID = os.getenv("SHEET_ID")
CREDS_B64 = os.getenv("GOOGLE_CREDS_B64")

# -------------------- GOOGLE SHEET --------------------
def get_gs_client():
    import base64, json
    info = json.loads(base64.b64decode(CREDS_B64))
    scopes = ["https://www.googleapis.com/auth/spreadsheets.readonly"]
    creds = Credentials.from_service_account_info(info, scopes=scopes)
    return gspread.authorize(creds)

def load_ot_records():
    client = get_gs_client()
    sh = client.open_by_key(SHEET_ID)
    ws = sh.worksheet("OT Record")
    return ws.get_all_records()

# -------------------- PERIOD LOGIC --------------------
def parse_period(anchor_ym: str):
    """
    anchor_ym: YYYY-MM
    Period = 16th 04:00 of anchor month
          -> 16th 04:00 of next month
    """
    start = datetime.strptime(anchor_ym + "-16 04:00", "%Y-%m-%d %H:%M")
    # move to next month
    if start.month == 12:
        end = start.replace(year=start.year + 1, month=1)
    else:
        end = start.replace(month=start.month + 1)
    return start, end

# -------------------- COMMANDS --------------------
async def ot_report(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.message.reply_text("Usage: /ot_report YYYY-MM")
        return

    anchor = context.args[0]
    try:
        start, end = parse_period(anchor)
    except Exception:
        await update.message.reply_text("Invalid format. Use YYYY-MM")
        return

    rows = load_ot_records()
    drivers = sorted({r.get("driver") for r in rows if r.get("driver")})

    context.user_data["ot_anchor"] = anchor

    keyboard = [
        [InlineKeyboardButton(d, callback_data=f"otdrv|{d}")]
        for d in drivers
    ]

    await update.message.reply_text(
        f"OT Report period:\n{start} → {end}\nSelect driver:",
        reply_markup=InlineKeyboardMarkup(keyboard),
    )

async def ot_driver_selected(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    driver = query.data.split("|", 1)[1]
    anchor = context.user_data.get("ot_anchor")
    start, end = parse_period(anchor)

    rows = load_ot_records()
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(["driver", "date", "ot_hours"])

    for r in rows:
        if r.get("driver") != driver:
            continue
        try:
            ts = datetime.fromisoformat(str(r.get("date")))
        except Exception:
            continue
        if start <= ts < end:
            writer.writerow([
                driver,
                r.get("date"),
                r.get("ot_hours", r.get("ot", ""))
            ])

    output.seek(0)
    await query.message.reply_document(
        document=io.BytesIO(output.getvalue().encode("utf-8")),
        filename=f"ot_{driver}_{anchor}.csv",
        caption=f"OT Report for {driver}\n{start} → {end}",
    )

# -------------------- MAIN --------------------
def main():
    if not BOT_TOKEN:
        raise RuntimeError("BOT_TOKEN missing")

    app = ApplicationBuilder().token(BOT_TOKEN).build()

    app.add_handler(CommandHandler("ot_report", ot_report))
    app.add_handler(CallbackQueryHandler(ot_driver_selected, pattern=r"^otdrv\|"))

    app.bot.delete_webhook(drop_pending_updates=True)
    app.bot.set_my_commands([
        BotCommand("ot_report", "OT report (16th 04:00 → next 16th 04:00)"),
    ])

    logger.info("A5 OT REPORT (button + custom period) LOADED")
    app.run_polling()

if __name__ == "__main__":
    main()
