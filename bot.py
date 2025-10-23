import os
import asyncio
import logging
import signal
from dotenv import load_dotenv
from aiohttp import web
from db import init_db
from contextlib import suppress
from commands import router 


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
        from aiogram import Bot, Dispatcher
    except Exception:
        from aiogram import Bot, Dispatcher
        from aiogram.router import Router

    from aiogram.client.session.aiohttp import AiohttpSession

    session = AiohttpSession(timeout=70)
    bot = Bot(token=TOKEN, session=session)

    async def _health(_):
        return web.Response(text="ok")

    async def run_health():
        app = web.Application()
        app.router.add_get("/healthz", _health)
        port = int(os.getenv("PORT", "8080"))
        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, "0.0.0.0", port)
        await site.start()

    stop_event = asyncio.Event()
    def _stop(*_):
        stop_event.set()

    async def main():
        # 1) токен и сессия
        token = os.getenv("BOT_TOKEN")
        if not token:
            raise ValueError("BOT_TOKEN отсутствует")
        session = AiohttpSession(timeout=70)
        bot = Bot(token=token, session=session)
        dp = Dispatcher()

        # 2) критично: БД + роутер
        await init_db()
        dp.include_router(router)

        # 3) сигналы и параллельный запуск
        loop = asyncio.get_running_loop()
        for s in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(s, _stop)

        try:
            await asyncio.gather(
                run_health(),
                dp.start_polling(bot, stop_event=stop_event),
            )
        except Exception:
            logging.exception("BOT CRASH")
            raise


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
