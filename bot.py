#!/usr/bin/env python3
import os
import logging
import csv
import io
from datetime import datetime

import gspread
from google.oauth2.service_account import Credentials

from telegram import Update, BotCommand
from telegram.ext import (
    ApplicationBuilder,
    CommandHandler,
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

def load_ot_records(year_month: str):
    client = get_gs_client()
    sh = client.open_by_key(SHEET_ID)
    ws = sh.worksheet("OT Record")
    rows = ws.get_all_records()

    results = []
    for r in rows:
        d = str(r.get("date", "")).strip()
        if d.startswith(year_month):
            results.append({
                "driver": r.get("driver", ""),
                "date": d,
                "ot_hours": r.get("ot_hours", r.get("ot", "")),
            })
    return results

# -------------------- COMMANDS --------------------
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("Driver Bot is running.")

async def ot_csv(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.message.reply_text("Usage: /ot_csv YYYY-MM")
        return

    ym = context.args[0]
    try:
        datetime.strptime(ym, "%Y-%m")
    except ValueError:
        await update.message.reply_text("Invalid format. Use YYYY-MM")
        return

    try:
        records = load_ot_records(ym)
    except Exception as e:
        await update.message.reply_text(f"Failed to load OT Record: {e}")
        return

    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(["driver", "date", "ot_hours"])

    for r in records:
        writer.writerow([r["driver"], r["date"], r["ot_hours"]])

    output.seek(0)
    await update.message.reply_document(
        document=io.BytesIO(output.getvalue().encode("utf-8")),
        filename=f"ot_report_{ym}.csv",
        caption=f"OT Report {ym}"
    )

# -------------------- MAIN --------------------
def main():
    if not BOT_TOKEN:
        raise RuntimeError("BOT_TOKEN missing")

    application = ApplicationBuilder().token(BOT_TOKEN).build()

    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("ot_csv", ot_csv))

    # Proper async-safe setup handled internally by run_polling()
    application.run_polling(
        allowed_updates=Update.ALL_TYPES,
        drop_pending_updates=True
    )

    logger.info("OT CSV BOT RUNNING (stable, no event-loop issues)")

if __name__ == "__main__":
    main()
