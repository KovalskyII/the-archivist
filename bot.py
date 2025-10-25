import os
import asyncio
import logging
import signal
from dotenv import load_dotenv
from aiohttp import web
from db import init_db
import aiogram
from aiogram import Bot, Dispatcher
import socket
import aiohttp
from aiogram.client.session.aiohttp import AiohttpSession


load_dotenv()

logging.basicConfig(level=logging.INFO)

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
    import aiogram, logging
    logging.info(f"aiogram version: {aiogram.__version__}")
    # 1) токен и сессия
    token = os.getenv("BOT_TOKEN")
    if not token:
        raise ValueError("BOT_TOKEN отсутствует")

    session = AiohttpSession(timeout=90)
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
            dp.start_polling(
                bot,
                stop_event=stop_event,
                polling_timeout=40,                              # уже стоит — ок
                allowed_updates=dp.resolve_used_update_types(),  # не тянем лишнее
                drop_pending_updates=True,                      # не разгребаем «хвост» после рестартов
            ),
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

