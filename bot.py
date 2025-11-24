import os
import logging
import datetime
import base64
from typing import Optional, Dict

import gspread
from oauth2client.service_account import ServiceAccountCredentials
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ApplicationBuilder, CommandHandler, CallbackQueryHandler, ContextTypes

# ================== 配置 ==================
BOT_TOKEN = os.getenv("BOT_TOKEN")
GOOGLE_SHEET_NAME = os.getenv("GOOGLE_SHEET_NAME", "Driver_Log")
PLATE_LIST_ENV = os.getenv("PLATE_LIST")

PLATE_NUMBERS = [p.strip() for p in PLATE_LIST_ENV.split(",")] if PLATE_LIST_ENV else [
    "2BB-3071","2BB-0809","2CI-8066","2CK-8066","2CJ-8066",
    "3H-8066","2AV-6527","2AZ-6828","2AX-4635","2BV-8320"
]

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# ============ Google Sheet 连接 ===============
def ensure_credentials_file():
    """
    读取 GOOGLE_CREDS_BASE64，如果存在则解码生成 credentials.json
    """
    b64 = os.getenv("GOOGLE_CREDS_BASE64")
    if b64:
        try:
            data = base64.b64decode(b64)
            with open("credentials.json", "wb") as f:
                f.write(data)
            logger.info("通过 GOOGLE_CREDS_BASE64 生成 credentials.json")
            return "credentials.json"
        except Exception as e:
            logger.error("base64 解码失败: %s", e)
            return None

    if os.path.exists("credentials.json"):
        return "credentials.json"

    logger.error("缺少 credentials.json")
    return None


def connect_google_sheet():
    cred_path = ensure_credentials_file()
    if not cred_path:
        raise RuntimeError("无法加载 Google 凭证")

    scope = ["https://spreadsheets.google.com/feeds",
             "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name(cred_path, scope)
    client = gspread.authorize(creds)
    return client.open(GOOGLE_SHEET_NAME).sheet1


# ============ BOT 主逻辑 ============
active_trips: Dict[int, Dict] = {}


def build_plate_keyboard(prefix):
    keyboard = []
    for plate in PLATE_NUMBERS:
        keyboard.append([InlineKeyboardButton(plate, callback_data=f"{prefix}:{plate}")])
    return InlineKeyboardMarkup(keyboard)


async def start_trip(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("请选择出车车牌号：", reply_markup=build_plate_keyboard("start"))


async def end_trip(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("请选择返回车牌号：", reply_markup=build_plate_keyboard("end"))


async def button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    action, plate = query.data.split(":")
    user = query.from_user
    user_id = user.id
    user_name = user.full_name or user.username or str(user_id)

    now = datetime.datetime.now()
    date_str = now.date().isoformat()
    time_str = now.strftime("%H:%M:%S")

    sheet = connect_google_sheet()

    if action == "start":
        active_trips[user_id] = {"plate": plate, "start": now}
        sheet.append_row([date_str, user_name, plate, time_str, "", ""])

        await query.edit_message_text(f"📤 {user_name} 出车\n🚗 {plate}\n🕒 {date_str} {time_str}")

    elif action == "end":
        if user_id not in active_trips:
            await query.edit_message_text("⚠️ 未找到出车记录，请先使用 /start_trip")
            return

        start_dt = active_trips[user_id]["start"]
        duration = now - start_dt

        hours = duration.seconds // 3600
        minutes = (duration.seconds % 3600) // 60
        dur_str = f"{hours}h{minutes}m"

        sheet.append_row([date_str, user_name, plate, "", time_str, dur_str])

        del active_trips[user_id]

        await query.edit_message_text(f"📥 {user_name} 返回\n🚗 {plate}\n🕒 {date_str} {time_str}\n⏳ 时长：{dur_str}")


# ============ 程序入口 ============
def main():
    if not BOT_TOKEN:
        raise RuntimeError("请先设置 BOT_TOKEN 环境变量（Telegram Bot Token）")

    app = ApplicationBuilder().token(8215522246:AAHZMW4_laHjbJ57NUaoN-NBWK7AyGYIJIk).build()

    app.add_handler(CommandHandler("start_trip", start_trip))
    app.add_handler(CommandHandler("end_trip", end_trip))
    app.add_handler(CallbackQueryHandler(button_handler))

    logger.info("🚀 Bot 已启动，等待指令中...")
    app.run_polling()


if __name__ == "__main__":
    main()
