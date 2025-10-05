import os
import asyncio
import logging
from dotenv import load_dotenv

from commands import handle_message, handle_photo_command
from db import init_db

load_dotenv()
TOKEN = os.getenv("BOT_TOKEN")
if not TOKEN:
    raise ValueError("Не найден токен бота. Проверь .env и переменную BOT_TOKEN")

logging.basicConfig(level=logging.INFO)

import aiogram
AIOMAJOR = int(aiogram.__version__.split(".")[0])

if AIOMAJOR >= 3:
    # -------- aiogram v3 (разные раскладки импорта) --------
    try:
        from aiogram import Bot, Dispatcher, F, Router
    except Exception:
        from aiogram import Bot, Dispatcher, F
        from aiogram.router import Router
    from aiogram.types import Message

    bot = Bot(token=TOKEN)
    dp = Dispatcher()
    router = Router()

    @router.message(F.photo & F.caption)
    async def on_photo(message: Message):
        await handle_photo_command(message)

    @router.message()  # ← ловим все апдейты и фильтруем уже внутри
    async def on_text(message: Message):
        if not getattr(message, "text", None):
            return
        if getattr(message.from_user, "is_bot", False):
            return
        await handle_message(message)

    async def main():
        await init_db()
        dp.include_router(router)
        await dp.start_polling(bot)

    if __name__ == "__main__":
        try:
            asyncio.run(main())
        except RuntimeError:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            loop.run_until_complete(main())


else:
    # -------- aiogram v2 --------
    from aiogram import Bot, Dispatcher, types
    from aiogram.utils import executor

    bot = Bot(token=TOKEN)
    dp = Dispatcher(bot)

    @dp.message_handler(content_types=types.ContentTypes.ANY)
    async def fallback_handler(message: types.Message):
        if message.photo and message.caption:
            await handle_photo_command(message)
        elif message.text:
            await handle_message(message)

    async def on_startup(_):
        await init_db()

    if __name__ == "__main__":
        executor.start_polling(dp, skip_updates=True, on_startup=on_startup)
