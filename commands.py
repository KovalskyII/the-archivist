import re
import os
import sys
import asyncio
import random
from typing import List, Tuple

from aiogram import types
from aiogram.types import FSInputFile

from db import (
    # базовые
    get_balance, change_balance, set_role, get_role,
    grant_key, revoke_key, has_key, get_last_history,
    get_top_users, get_all_roles, reset_user_balance,
    reset_all_balances, set_role_image, get_role_with_image,
    get_key_holders, get_known_users,

    # анти-дубль
    is_msg_processed, mark_msg_processed,

    # перки
    grant_perk, revoke_perk, get_perks, get_perk_holders, get_perks_summary,

    # ЗП/кража
    get_seconds_since_last_salary_claim, record_salary_claim,
    get_seconds_since_last_theft, record_theft,

    # экономика/сейф
    vault_init, get_economy_stats, get_last_vault_cap,
    get_burn_bps, set_burn_bps, get_income, set_income,

    # конфиги игр/лимитов/цен
    get_multipliers, set_multiplier, get_casino_on, set_casino_on,
    get_limit_bet, set_limit_bet, get_limit_rain, set_limit_rain,
    get_price_emerald, set_price_emerald, get_price_perk, set_price_perk,

    # рынок
    create_offer, cancel_offer, list_active_offers, record_burn,
)

KURATOR_ID = 164059195
DB_PATH = "/data/bot_data.sqlite"

# Код перка -> (эмоджи, человекочитаемое название)
PERK_REGISTRY = {
    "иммунитет": ("🛡️", "Иммунитет к бану"),
    "зп": ("💵", "Зарплата (ежедневно)"),
    "вор": ("🗡️", "Своровать нуары (раз в сутки)"),
}

def mention_html(user_id: int, fallback: str = "Участник") -> str:
    safe = html.escape(fallback, quote=False)
    return f"<a href='tg://user?id={user_id}'>{safe}</a>"

def render_perks(perk_codes: set[str]) -> str:
    if not perk_codes:
        return "У Вас пока нет перков."
    lines = ["Ваши перки:"]
    items = []
    for code in perk_codes:
        meta = PERK_REGISTRY.get(code)
        if meta:
            emoji, title = meta
            items.append((title.lower(), f"{emoji} {title}"))
        else:
            items.append((code, f"• {code}"))
    for _, line in sorted(items):
        lines.append(line)
    return "\n".join(lines)

# -------- вспомогательные форматтеры --------

def fmt_money(n: int) -> str:
    return f"🪙{n} нуаров"

def fmt_percent_bps(bps: int) -> str:
    # 100 bps = 1%
    whole = bps // 100
    frac = bps % 100
    return f"{whole}.{frac:02d}%"

# --------- основной обработчик ---------

async def handle_message(message: types.Message):
    if not message.text:
        return

    # анти-дубль на «команды» (idempotency по конкретному message_id)
    if await is_msg_processed(message.chat.id, message.message_id):
        return
    await mark_msg_processed(message.chat.id, message.message_id)

    text = message.text.strip()
    text_l = text.lower()
    author_id = message.from_user.id

    if message.from_user.is_bot:
        return

    # ======= Команды для всех =======

    if text_l == "мой карман":
        bal = await get_balance(author_id)
        await message.reply(f"У Вас в кармане {fmt_money(bal)}.")
        return

    if text_l == "моя роль":
        await handle_my_role(message)
        return

    if text_l == "роль" and message.reply_to_message:
        await handle_who_role(message)
        return

    if text_l in ("список команд", "команды"):
        await handle_commands_catalog(message)
        return

    if text_l == "клуб":
        await message.answer(
            "🎩 <b>Клуб Le Cadeau Noir</b>\n"
            "<i>В переводе с французского — «Чёрный подарок»</i>\n\n"
            "🌑 <b>Концепция:</b>\n"
            "Закрытый элегантный Telegram-клуб для ценителей стиля, таинственности и криптоподарков.\n"
            "Участники клуба обмениваются виртуальными (и иногда реальными) подарками.\n"
            "Каждый подарок — это не просто жест, а символ уважения, флирта или признательности.\n\n"
            "🎓 <b>Этикет:</b>\n"
            "Всё происходит в атмосфере вежливости, загадочности и утончённого шика.\n"
            "Прямые предложения не приветствуются — всё через намёки, ролевую игру и символы.",
            parse_mode="HTML"
        )
        return

    if text_l == "рейтинг клуба":
        await handle_rating(message)
        return

    if text_l == "члены клуба":
        await handle_club_members(message)
        return

    if text_l == "хранители ключа":
        await handle_key_holders_cmd(message)
        return

    if text_l.startswith("передать "):
        await handle_peredat(message)
        return

    if text_l.startswith("ставлю"):
        await handle_kubik(message)
        return

    if text_l == "мои перки":
        await handle_my_perks(message)
        return

    if text_l == "перки" and message.reply_to_message:
        await handle_perks_of(message)
        return

    if text_l == "получить зп":
        await handle_salary_claim(message)
        return

    if text_l.startswith("дождь "):
        await handle_dozhd(message)
        return

    # рынок
    if text_l == "рынок":
        await handle_market_show(message)
        return

    if text_l == "купить эмеральд":
        await handle_buy_emerald(message)
        return

    m = re.match(r"^купить\s+перк\s+(.+)$", text_l)
    if m:
        code = m.group(1).strip()
        await handle_buy_perk(message, code)
        return

    m = re.match(r"^выставить\s+(\S+)\s+(\d+)$", text_l)
    if m:
        await handle_offer_create(message, m.group(1), int(m.group(2)))
        return

    m = re.match(r"^купить\s+(\d+)$", text_l)
    if m:
        await handle_offer_buy(message, int(m.group(1)))
        return

    m = re.match(r"^снять\s+лот\s+(\d+)$", text_l)
    if m:
        await handle_offer_cancel(message, int(m.group(1)))
        return

    # кража
    if text_l in ("украсть", "своровать") and message.reply_to_message:
        await handle_theft(message)
        return

    # экономика/сейф
    if text_l.startswith("включить сейф"):
        await handle_vault_enable(message)
        return

    if text_l.startswith("перезапустить сейф"):
        await handle_vault_reset(message)
        return

    if text_l == "сейф":
        await handle_vault_stats(message)
        return

    # конфиги
    m = re.match(r"^сжигание\s+(\d+)$", text_l)
    if m and author_id == KURATOR_ID:
        await handle_burn_bps_set(message, int(m.group(1)))
        return

    m = re.match(r"^цена\s+эмеральд\s+(\d+)$", text_l)
    if m and author_id == KURATOR_ID:
        await handle_price_emerald_set(message, int(m.group(1)))
        return

    m = re.match(r"^цена\s+перк\s+(\S+)\s+(\d+)$", text_l)
    if m and author_id == KURATOR_ID:
        await handle_price_perk_set(message, m.group(1), int(m.group(2)))
        return

    m = re.match(r"^множитель\s+(кубик|дартс|боулинг|автоматы)\s+(\d+)$", text_l)
    if m and author_id == KURATOR_ID:
        await handle_multiplier_set(message, m.group(1), int(m.group(2)))
        return

    if text_l in ("казино открыть", "казино закрыть") and author_id == KURATOR_ID:
        await handle_casino_toggle(message)
        return

    m = re.match(r"^доходы\s+(\d+)$", text_l)
    if m and author_id == KURATOR_ID:
        await handle_income_set(message, int(m.group(1)))
        return

    m = re.match(r"^лимит\s+ставка\s+(\d+)$", text_l)
    if m and author_id == KURATOR_ID:
        await handle_limit_bet_set(message, int(m.group(1)))
        return

    m = re.match(r"^лимит\s+дождь\s+(\d+)$", text_l)
    if m and author_id == KURATOR_ID:
        await handle_limit_rain_set(message, int(m.group(1)))
        return

    # держатели перка / реестр
    m = re.match(r"^(?:у кого перк|держатели перка)\s+(\S+)$", text_l)
    if m and author_id == KURATOR_ID:
        await handle_perk_holders_list(message, m.group(1))
        return

    if text_l == "перки реестр" and author_id == KURATOR_ID:
        await handle_perk_registry(message)
        return

    # ======= Команды с ключом =======
    user_has_key = (author_id == KURATOR_ID) or await has_key(author_id)

    if user_has_key:
        if re.match(r"^(вручить|выдать)\s+(-?\d+)$", text_l):
            await handle_vruchit(message)
            return
        if re.match(r"^(взыскать|отнять)\s+(-?\d+)$", text_l):
            await handle_otnyat(message, text_l, author_id)
            return
        if text_l == "карман":
            await handle_kurator_karman(message)
            return

    # ======= Команды только Куратора =======
    if author_id == KURATOR_ID:
        if text_l.startswith("назначить ") and message.reply_to_message:
            await handle_naznachit(message)
            return
        if text_l == "снять роль" and message.reply_to_message:
            await handle_snyat_rol(message)
            return
        if text_l == "ключ от сейфа" and message.reply_to_message:
            await handle_kluch(message)
            return
        if text_l == "снять ключ" and message.reply_to_message:
            await handle_snyat_kluch(message)
            return
        if text_l == "обнулить клуб":
            await asyncio.sleep(1)
            await handle_clear_db(message)
            return
        if text_l.startswith("обнулить балансы"):
            await handle_obnulit_balansy(message)
            return
        if text_l.startswith("обнулить баланс"):
            await handle_obnulit_balans(message)
            return
        if text_l.startswith("даровать ") and message.reply_to_message:
            code = text_l.split(" ", 1)[1].strip()
            if code in PERK_REGISTRY:
                await handle_grant_perk_universal(message, code)
                return
        if text_l.startswith("уничтожить ") and message.reply_to_message:
            code = text_l.split(" ", 1)[1].strip()
            if code in PERK_REGISTRY:
                await handle_revoke_perk_universal(message, code)
                return

# ---------- базовые куски (ролы, фото, рейтинги и т.п.) ----------

async def handle_photo_command(message: types.Message):
    if message.from_user.id != KURATOR_ID:
        return
    if not (message.caption and message.photo):
        return
    text = message.caption.lower().strip()
    if text.startswith("фото роли") and message.reply_to_message:
        target_user_id = message.reply_to_message.from_user.id
        photo_id = message.photo[-1].file_id
        await set_role_image(target_user_id, photo_id)
        await message.reply("Фото роли обновлено.")

async def handle_my_role(message: types.Message):
    author_id = message.from_user.id
    try:
        role_row = await get_role_with_image(author_id)
    except Exception:
        role_info = await get_role(author_id)
        role_row = (role_info.get("role"), role_info.get("description"), None) if role_info else None
    if role_row:
        role_name, role_desc, image_file_id = role_row
        text_resp = f"🎭 *{role_name}*\n\n_{role_desc}_"
        if image_file_id:
            await message.reply_photo(photo=image_file_id, caption=text_resp, parse_mode="Markdown")
        else:
            if author_id == KURATOR_ID and os.path.exists("images/kurator.jpg"):
                try:
                    await message.reply_photo(photo=FSInputFile("images/kurator.jpg"), caption=text_resp, parse_mode="Markdown")
                except Exception:
                    await message.reply(text_resp, parse_mode="Markdown")
            else:
                await message.reply(text_resp, parse_mode="Markdown")
    else:
        await message.reply("Я вас не узнаю.")

async def handle_who_role(message: types.Message):
    target_id = message.reply_to_message.from_user.id
    try:
        role_row = await get_role_with_image(target_id)
    except Exception:
        role_info = await get_role(target_id)
        role_row = (role_info.get("role"), role_info.get("description"), None) if role_info else None
    if role_row:
        role_name, role_desc, image_file_id = role_row
        text_resp = f"🎭 *{role_name}*\n\n_{role_desc}_"
        if image_file_id:
            await message.reply_photo(photo=image_file_id, caption=text_resp, parse_mode="Markdown")
        else:
            await message.reply(text_resp, parse_mode="Markdown")
    else:
        await message.reply("Я не знаю кто это.")

async def handle_rating(message: types.Message):
    rows = await get_top_users(limit=10)
    if not rows:
        await message.reply("Ни у кого в клубе нет нуаров.")
        return
    lines = ["💰 Богатейшие члены клуба Le Cadeau Noir:\n"]
    for i, (user_id, balance) in enumerate(rows, start=1):
        name = "Участник"
        try:
            member = await message.bot.get_chat_member(message.chat.id, user_id)
            name = member.user.full_name or name
        except Exception:
            pass
        lines.append(f"{i}. {mention_html(user_id, name)} — {balance} нуаров")
    await message.reply("\n".join(lines), parse_mode="HTML")

async def handle_club_members(message: types.Message):
    rows = await get_all_roles()
    if not rows:
        await message.reply("Пока что в клубе пусто.")
        return
    lines = ["🎭 <b>Члены клуба:</b>\n"]
    for user_id, role in rows:
        name = "Участник"
        try:
            member = await message.bot.get_chat_member(message.chat.id, user_id)
            name = member.user.full_name or name
        except Exception:
            pass
        mention = mention_html(user_id, name)
        lines.append(f"{mention} — <b>{role}</b>")
    await message.reply("\n".join(lines), parse_mode="HTML")

async def handle_key_holders_cmd(message: types.Message):
    user_ids = await get_key_holders()
    if not user_ids:
        await message.reply("Пока ни у кого нет ключа.")
        return
    lines = ["🗝️ <b>Хранители ключа:</b>\n"]
    for user_id in user_ids:
        name = "Участник"
        try:
            member = await message.bot.get_chat_member(message.chat.id, user_id)
            name = member.user.full_name or name
        except Exception:
            pass
        lines.append(f"{mention_html(user_id, name)}")
    await message.reply("\n".join(lines), parse_mode="HTML")

async def handle_clear_db(message: types.Message):
    if message.from_user.id != KURATOR_ID:
        await message.reply("Только куратор может обнулить клуб.")
        return
    try:
        await message.reply("🗑Клуб обнуляется...")
        if os.path.exists(DB_PATH):
            os.remove(DB_PATH)
        await message.answer("💢Код Армагедон. Клуб обнулен. Теперь только я и вы, Куратор.")
        os.execv(sys.executable, [sys.executable] + sys.argv)
    except Exception as e:
        await message.reply(f"Ошибка при обнулении: {e}")

async def handle_obnulit_balans(message: types.Message):
    if not message.reply_to_message:
        await message.reply("Чтобы обнулить баланс, ответь на сообщение участника.")
        return
    user_id = message.reply_to_message.from_user.id
    await reset_user_balance(user_id)
    await message.reply("✅Баланс участника обнулён.")

async def handle_obnulit_balansy(message: types.Message):
    await reset_all_balances()
    await message.reply("✅Все балансы обнулены.")

# ----------- деньги: вручить / взыскать / передать / дождь -----------

async def _get_vault_room() -> int:
    stats = await get_economy_stats()
    if not stats:
        return -1  # сейф не включён
    return stats["vault"]

async def handle_vruchit(message: types.Message):
    if not message.reply_to_message:
        await message.reply("Обращение не по этикету Клуба. Пример: 'вручить 5' (ответом на участника)")
        return
    m = re.match(r"(?:вручить|выдать)\s+(-?\d+)", message.text.strip(), re.IGNORECASE)
    if not m:
        await message.reply("Обращение не по этикету Клуба. Пример: 'вручить|выдать 5'")
        return
    amount = int(m.group(1))
    if amount <= 0:
        await message.reply("Я не могу выдать минус.")
        return

    # проверка сейфа
    room = await _get_vault_room()
    if room == -1:
        await message.reply("Сейф ещё не включён. Команда: 'включить сейф <CAP>' (для куратора).")
        return
    if amount > room:
        await message.reply(f"В сейфе недостаточно нуаров. Доступно: {room}")
        return

    recipient = message.reply_to_message.from_user
    await change_balance(recipient.id, amount, "выдача из сейфа", message.from_user.id)
    await message.reply(f"🧮Я выдал {amount} нуаров {mention_html(recipient.id, recipient.full_name)}", parse_mode="HTML")

async def handle_otnyat(message: types.Message, text: str, author_id: int):
    if not message.reply_to_message:
        await message.reply("Обращение не по этикету Клуба. Пример: 'взыскать 3' (ответом на участника)")
        return
    m = re.match(r"(?:взыскать|отнять)\s+(-?\d+)", text, re.IGNORECASE)
    if not m:
        await message.reply("Обращение не по этикету Клуба. Пример: 'взыскать|отнять 3'")
        return
    amount = int(m.group(1))
    if amount <= 0:
        await message.reply("Я не могу отнять минус.")
        return
    recipient = message.reply_to_message.from_user
    current_balance = await get_balance(recipient.id)
    if amount > current_balance:
        await message.reply(f"У {recipient.full_name} нет такого количества нуаров. Баланс: {current_balance}")
        return
    await change_balance(recipient.id, -amount, "взыскание в сейф", author_id)
    await message.reply(f"🧮Я взыскал {amount} нуаров у {mention_html(recipient.id, recipient.full_name)}", parse_mode="HTML")

async def handle_peredat(message: types.Message):
    if not message.reply_to_message:
        await message.reply("Чтобы передать нуары, ответьте на сообщение получателя. Пример: 'передать 10'")
        return
    m = re.match(r"передать\s+(\d+)", message.text.strip(), re.IGNORECASE)
    if not m:
        await message.reply("Обращение не по этикету Клуба. Пример: 'передать 10'")
        return
    amount = int(m.group(1))
    if amount <= 0:
        await message.reply("Я не могу передать минус.")
        return
    giver_id = message.from_user.id
    recipient = message.reply_to_message.from_user
    recipient_id = recipient.id
    if giver_id == recipient_id:
        await message.reply("Нельзя передать нуары самому себе.")
        return
    balance = await get_balance(giver_id)
    if amount > balance:
        await message.reply(f"У Вас недостаточно нуаров. Баланс: {balance}")
        return
    await change_balance(giver_id, -amount, "передача", giver_id)
    await change_balance(recipient_id, amount, "передача", giver_id)
    await message.reply(
        f"💸Я передал {amount} нуаров от {mention_html(giver_id, message.from_user.full_name)} к {mention_html(recipient_id, recipient.full_name)}",
        parse_mode="HTML"
    )

async def handle_dozhd(message: types.Message):
    m = re.match(r"^дождь\s+(\d+)$", message.text.strip(), re.IGNORECASE)
    if not m:
        await message.reply("Обращение не по этикету Клуба. Пример: 'дождь 10'")
        return
    total = int(m.group(1))
    if total < 5:
        await message.reply("Минимальный дождь — 5 нуаров.")
        return

    # лимит дождя
    max_rain = await get_limit_rain()
    if max_rain and total > max_rain:
        await message.reply(f"Лимит дождя: не более {max_rain} за одну команду.")
        return

    giver_id = message.from_user.id
    bal = await get_balance(giver_id)
    if total > bal:
        await message.reply(f"У Вас недостаточно нуаров. Баланс: {bal}")
        return
    candidate_ids = [uid for uid in await get_known_users() if uid != giver_id]
    eligible = []
    for uid in candidate_ids:
        try:
            member = await message.bot.get_chat_member(message.chat.id, uid)
            if member.status in ("left", "kicked"):
                continue
            if getattr(member.user, "is_bot", False):
                continue
            name = member.user.full_name or "Участник"
            eligible.append((uid, name))
        except Exception:
            continue
    if not eligible:
        await message.reply("Некого намочить — я не вижу участников в этом чате.")
        return
    random.shuffle(eligible)
    recipients = eligible[:5]
    n = len(recipients)
    base = total // n
    rest = total % n
    per_user = [base + (1 if i < rest else 0) for i in range(n)]
    await change_balance(giver_id, -total, "дождь", giver_id)
    for (uid, _name), amt in zip(recipients, per_user):
        if amt > 0:
            await change_balance(uid, amt, "дождь", giver_id)
    breakdown = [
        f"{mention_html(uid, name)} — намок на {amt} нуаров"
        for (uid, name), amt in zip(recipients, per_user) if amt > 0
    ]
    await message.reply("🌧 Прошёл дождь. Намокли: " + ", ".join(breakdown), parse_mode="HTML")

# ------------- игры (пока только кубик, остальные готовы к добавлению) -------------

async def handle_kubik(message: types.Message):
    m = re.match(r"^\s*ставлю\s+(\d+)\s+на\s+(?:🎲|кубик)\s*$", message.text.strip(), re.IGNORECASE)
    if not m:
        await message.reply("Обращение не по этикету Клуба. Пример: 'Ставлю 10 на 🎲|кубик'")
        return
    # казино доступность
    if not await get_casino_on():
        await message.reply("🎰 Казино закрыто.")
        return
    amount = int(m.group(1))
    if amount <= 0:
        await message.reply("Я не могу принять отрицательную ставку.")
        return
    # лимит ставки
    max_bet = await get_limit_bet()
    if max_bet and amount > max_bet:
        await message.reply(f"Лимит ставки: не более {max_bet}.")
        return

    gambler_id = message.from_user.id
    balance = await get_balance(gambler_id)
    if amount > balance:
        await message.reply(f"🔍У Вас недостаточно нуаров. Баланс: {balance}")
        return

    mults = await get_multipliers()
    win_mult = mults["dice"]
    # проверка сейфа на потенциальную выплату
    room = await _get_vault_room()
    if room == -1:
        await message.reply("Сейф ещё не включён.")
        return
    potential = amount * win_mult
    if potential > room:
        await message.reply("Казино закрыто на переучёт — в сейфе недостаточно средств для такой выплаты.")
        return

    sent: types.Message = await message.answer_dice(emoji="🎲")
    roll_value = sent.dice.value
    await asyncio.sleep(3.5)
    if roll_value == 6:
        await change_balance(gambler_id, amount * win_mult, "ставка выигрыш (кубик)", gambler_id)
        await message.reply(
            f"🎉Фортуна на вашей стороне, {mention_html(gambler_id, message.from_user.full_name)}. "
            f"Вы получаете {fmt_money(amount * win_mult)}",
            parse_mode="HTML"
        )
    else:
        await change_balance(gambler_id, -amount, "ставка проигрыш (кубик)", gambler_id)
        await message.reply(
            f"🪦Ставки погубят вас, {mention_html(gambler_id, message.from_user.full_name)}. "
            f"Вы потеряли {fmt_money(amount)}.",
            parse_mode="HTML"
        )

# ------------- перки: мои/чужие, даровать/уничтожить, ЗП, вор -------------

async def handle_my_perks(message: types.Message):
    perk_codes = await get_perks(message.from_user.id)
    await message.reply(render_perks(perk_codes))

async def handle_perks_of(message: types.Message):
    target = message.reply_to_message.from_user
    perk_codes = await get_perks(target.id)
    if not perk_codes:
        await message.reply(f"У {target.full_name} пока нет перков.")
        return
    lines = [f"Перки {mention_html(target.id, target.full_name)}:"]
    items = []
    for code in perk_codes:
        meta = PERK_REGISTRY.get(code)
        if meta:
            emoji, title = meta
            items.append((title.lower(), f"{emoji} {title}"))
        else:
            items.append((code, f"• {code}"))
    for _, line in sorted(items):
        lines.append(line)
    await message.reply("\n".join(lines), parse_mode="HTML")

async def handle_grant_perk_universal(message: types.Message, code: str):
    if not message.reply_to_message:
        await message.reply("Даровать перк можно только ответом на сообщение участника.")
        return
    target = message.reply_to_message.from_user
    perks = await get_perks(target.id)
    emoji, title = PERK_REGISTRY.get(code, ("", code))
    if code in perks:
        await message.reply(f"У {mention_html(target.id, target.full_name)} уже есть «{title}».", parse_mode="HTML")
        return
    await grant_perk(target.id, code)
    await message.reply(f"Перк «{title}» дарован {mention_html(target.id, target.full_name)}.", parse_mode="HTML")

async def handle_revoke_perk_universal(message: types.Message, code: str):
    if not message.reply_to_message:
        await message.reply("Уничтожить перк можно только ответом на сообщение участника.")
        return
    target = message.reply_to_message.from_user
    perks = await get_perks(target.id)
    emoji, title = PERK_REGISTRY.get(code, ("", code))
    if code not in perks:
        await message.reply(f"У {mention_html(target.id, target.full_name)} нет перка «{title}».", parse_mode="HTML")
        return
    await revoke_perk(target.id, code)
    await message.reply(f"Перк «{title}» уничтожен у {mention_html(target.id, target.full_name)}.", parse_mode="HTML")

async def handle_salary_claim(message: types.Message):
    user_id = message.from_user.id
    perks = await get_perks(user_id)
    if "зп" not in perks:
        await message.reply("У Вас нет такой привилегии.")
        return
    seconds = await get_seconds_since_last_salary_claim(user_id, "зп")
    COOLDOWN = 24 * 60 * 60
    if seconds is not None and seconds < COOLDOWN:
        remain = COOLDOWN - seconds
        hours = remain // 3600
        minutes = (remain % 3600) // 60
        await message.reply(f"Зарплата уже получена. Повторно — через {hours}ч {minutes}м.")
        return
    income = await get_income()
    # проверка сейфа
    room = await _get_vault_room()
    if room == -1:
        await message.reply("Сейф ещё не включён.")
        return
    if income > room:
        await message.reply("В сейфе недостаточно нуаров для выплаты «зп».")
        return
    await record_salary_claim(user_id, income, "зп")
    await change_balance(user_id, income, "зп", user_id)
    await message.reply(f"💵 Начислено {income} нуаров по перку «Зарплата».")    

async def handle_theft(message: types.Message):
    thief_id = message.from_user.id
    perks = await get_perks(thief_id)
    if "вор" not in perks:
        await message.reply("У Вас нет такой привилегии.")
        return
    if not message.reply_to_message:
        await message.reply("Кража работает только ответом на сообщение жертвы.")
        return
    victim = message.reply_to_message.from_user
    if victim.is_bot:
        await message.reply("Красть у бота бессмысленно.")
        return
    seconds = await get_seconds_since_last_theft(thief_id)
    COOLDOWN = 24 * 60 * 60
    if seconds is not None and seconds < COOLDOWN:
        remain = COOLDOWN - seconds
        hours = remain // 3600
        minutes = (remain % 3600) // 60
        await message.reply(f"Слишком часто. Повторно — через {hours}ч {minutes}м.")
        return
    income = await get_income()
    victim_balance = await get_balance(victim.id)
    if victim_balance < income or income <= 0:
        # неудача, кулдаун фиксируем
        await record_theft(thief_id, 0, victim.id, success=False)
        await message.reply("🐕 Сторожевые собаки подняли лай — вор ретировался. Попробуйте через 24 часа.")
        return
    # успех: перевод victim -> thief
    await change_balance(victim.id, -income, "кража", thief_id)
    await change_balance(thief_id, income, "кража", thief_id)
    await record_theft(thief_id, income, victim.id, success=True)
    await message.reply(
        f"🗡️ {mention_html(thief_id, message.from_user.full_name)} украл {fmt_money(income)} у "
        f"{mention_html(victim.id, victim.full_name)}.",
        parse_mode="HTML"
    )

# ------------- рынок -------------

async def handle_market_show(message: types.Message):
    # Эмеральд
    price_emerald = await get_price_emerald()
    # Перки
    perk_lines = []
    for code, (emoji, title) in PERK_REGISTRY.items():
        price = await get_price_perk(code)
        price_str = f"{price} нуаров" if price else "не продаётся"
        usage = ""
        if code == "зп":
            usage = " — команда использования: «получить зп»"
        elif code == "вор":
            usage = " — команда использования: «украсть/своровать» (reply)"
        elif code == "иммунитет":
            usage = " — одноразовая защита (амулет)"
        perk_lines.append(f"{emoji} <b>{title}</b> — {price_str}{usage}\nКоманда: купить перк {code}")
    # Офферы
    offers = await list_active_offers()
    offer_lines = []
    for o in offers:
        seller = o["seller_id"]
        price = o["price"]
        link = o["link"] or "(ссылка не указана)"
        try:
            member = await message.bot.get_chat_member(message.chat.id, seller)
            seller_name = member.user.username and f"@{member.user.username}" or member.user.full_name
        except Exception:
            seller_name = "Участник"
        offer_lines.append(f"#{o['offer_id']} — {link} — {price} нуаров — продавец: {seller_name} — Команда: купить {o['offer_id']}")

    burn_bps = await get_burn_bps()
    txt = (
        "🛒 <b>Рынок</b>\n\n"
        f"💎 Эмеральд — {price_emerald} нуаров — Команда: купить эмеральд\n\n"
        "🎖 <b>Перки</b>:\n" + ("\n".join(perk_lines) if perk_lines else "Пусто") + "\n\n"
        "📦 <b>Лоты участников</b>:\n" + ("\n".join(offer_lines) if offer_lines else "Пока нет активных лотов.") + "\n\n"
        f"🔥 Сжигание на рынке: {fmt_percent_bps(burn_bps)} (округление вниз)"
    )
    await message.reply(txt, parse_mode="HTML")

async def handle_offer_create(message: types.Message, link: str, price: int):
    if price <= 0:
        await message.reply("Цена должна быть положительной.")
        return
    offer_id = await create_offer(message.from_user.id, link, price)
    await message.reply(f"Лот выставлен. ID: {offer_id}. Снять: «снять лот {offer_id}». Команда покупки появится на рынке.", parse_mode="HTML")

async def handle_offer_cancel(message: types.Message, offer_id: int):
    # снять может владелец или куратор
    offers = await list_active_offers()
    owner_id = None
    for o in offers:
        if o["offer_id"] == offer_id:
            owner_id = o["seller_id"]
            break
    if owner_id is None:
        await message.reply("Такого активного лота нет.")
        return
    if message.from_user.id != owner_id and message.from_user.id != KURATOR_ID:
        await message.reply("Снять лот может только продавец или куратор.")
        return
    await cancel_offer(offer_id, message.from_user.id)
    await message.reply("Лот снят.")

async def _apply_burn_and_return(price: int) -> int:
    """Возвращает величину burn по текущему bps (округление вниз)."""
    bps = await get_burn_bps()
    return (price * bps) // 10000

async def handle_offer_buy(message: types.Message, offer_id: int):
    # найти лот
    offers = await list_active_offers()
    offer = None
    for o in offers:
        if o["offer_id"] == offer_id:
            offer = o
            break
    if not offer:
        await message.reply("Такого активного лота нет.")
        return

    buyer_id = message.from_user.id
    price = offer["price"]
    bal = await get_balance(buyer_id)
    if price > bal:
        await message.reply(f"Недостаточно нуаров. Требуется {price}, на руках {bal}.")
        return

    burn = await _apply_burn_and_return(price)
    to_seller = price - burn

    # списываем у покупателя
    await change_balance(buyer_id, -price, f"покупка лота #{offer_id}", buyer_id)
    # начисляем продавцу
    if to_seller > 0:
        await change_balance(offer["seller_id"], to_seller, f"продажа лота #{offer_id}", buyer_id)
    # сжигаем
    if burn > 0:
        await record_burn(burn, f"offer_id={offer_id}")

    # записать продажу
    from db import insert_history  # локальный импорт чтобы не зацикливаться вверху
    sale_id = await insert_history(buyer_id, "offer_sold", price, f"offer_id={offer_id};seller={offer['seller_id']}")

    # контракт
    from datetime import datetime
    today = datetime.utcnow().strftime("%Y%m%d")
    contract_id = f"C-{today}-{sale_id}"
    seller_mention = mention_html(offer["seller_id"], "Продавец")
    await message.reply(
        "🧾 Контракт {cid}\n"
        f"Покупатель: {mention_html(buyer_id, message.from_user.full_name)}\n"
        f"Товар: «лот #{offer_id}» ({offer['link'] or 'ссылка не указана'})\n"
        f"Цена: {price}\n"
        f"Комиссия (сжигание): {burn}\n"
        f"Перевод продавцу: {to_seller}\n"
        f"Гарант: @kovalskyii\n"
        f"Продавец: {seller_mention}".format(cid=contract_id),
        parse_mode="HTML"
    )

async def handle_buy_emerald(message: types.Message):
    price = await get_price_emerald()
    buyer_id = message.from_user.id
    bal = await get_balance(buyer_id)
    if price > bal:
        await message.reply(f"Недостаточно нуаров. Требуется {price}, на руках {bal}.")
        return
    burn = await _apply_burn_and_return(price)
    # списываем у покупателя (остаток как бы уходит в сейф, т.к. никому не начисляем)
    await change_balance(buyer_id, -price, "покупка эмеральда", buyer_id)
    if burn > 0:
        await record_burn(burn, "emerald")
    # контракт/чек
    from db import insert_history
    sale_id = await insert_history(buyer_id, "emerald_buy", price, None)
    from datetime import datetime
    today = datetime.utcnow().strftime("%Y%m%d")
    contract_id = f"C-{today}-{sale_id}"
    await message.reply(
        "🧾 Контракт {cid}\n"
        f"Покупатель: {mention_html(buyer_id, message.from_user.full_name)}\n"
        f"Товар: «Эмеральд»\n"
        f"Цена: {price}\n"
        f"Комиссия (сжигание): {burn}\n"
        f"Перевод в сейф: {price - burn}\n"
        f"Гарант: @kovalskyii".format(cid=contract_id),
        parse_mode="HTML"
    )

async def handle_buy_perk(message: types.Message, code: str):
    code = code.strip().lower()
    if code not in PERK_REGISTRY:
        await message.reply("Такого перка нет.")
        return
    price = await get_price_perk(code)
    if price is None:
        await message.reply("Этот перк сейчас не продаётся.")
        return
    buyer_id = message.from_user.id
    bal = await get_balance(buyer_id)
    if price > bal:
        await message.reply(f"Недостаточно нуаров. Требуется {price}, на руках {bal}.")
        return
    burn = await _apply_burn_and_return(price)
    # списываем у покупателя
    await change_balance(buyer_id, -price, f"покупка перка {code}", buyer_id)
    if burn > 0:
        await record_burn(burn, f"perk={code}")
    # выдаём перк
    await grant_perk(buyer_id, code)

    # чек
    from db import insert_history
    sale_id = await insert_history(buyer_id, "perk_buy", price, code)
    from datetime import datetime
    today = datetime.utcnow().strftime("%Y%m%d")
    contract_id = f"C-{today}-{sale_id}"
    emoji, title = PERK_REGISTRY[code]
    await message.reply(
        f"🧾 Контракт {contract_id}\n"
        f"Покупатель: {mention_html(buyer_id, message.from_user.full_name)}\n"
        f"Товар: «{title}»\n"
        f"Цена: {price}\n"
        f"Комиссия (сжигание): {burn}\n"
        f"Перевод в сейф: {price - burn}\n"
        f"Гарант: @kovalskyii",
        parse_mode="HTML"
    )

# --------- Витрина конфигов / сейф ---------

async def handle_vault_enable(message: types.Message):
    if message.from_user.id != KURATOR_ID:
        return
    m = re.match(r"^включить\s+сейф\s+(\d+)$", message.text.strip().lower())
    if not m:
        await message.reply("Пример: «включить сейф 1000000»")
        return
    cap = int(m.group(1))
    circulating = await get_circulating_safe()
    rid = await vault_init(cap, circulating)
    if rid is None:
        await message.reply("Кап меньше текущего оборота — увеличьте кап.")
        return
    await message.reply(f"Сейф включён. Кап: {cap}. В обороте: {circulating}. Остальное заложено в сейф.")

async def get_circulating_safe() -> int:
    # обёртка на случай изоляции
    from db import get_circulating
    return await get_circulating()

async def handle_vault_reset(message: types.Message):
    if message.from_user.id != KURATOR_ID:
        return
    m = re.match(r"^перезапустить\s+сейф\s+(\d+)\s+подтверждаю$", message.text.strip().lower())
    if not m:
        await message.reply('Пример: «перезапустить сейф 1000000 подтверждаю»')
        return
    cap = int(m.group(1))
    circulating = await get_circulating_safe()
    rid = await vault_init(cap, circulating)
    if rid is None:
        await message.reply("Кап меньше текущего оборота — увеличьте кап.")
        return
    await message.reply(f"Сейф перезапущен. Новый кап: {cap}. В обороте: {circulating}.")

async def handle_vault_stats(message: types.Message):
    stats = await get_economy_stats()
    if not stats:
        await message.reply("Сейф ещё не включён.")
        return
    bps = stats["burn_bps"]
    pct = fmt_percent_bps(bps)
    # процент сожжённого от капа
    burned_pct = 0.0
    if stats["cap"] > 0:
        burned_pct = (stats["burned"] / stats["cap"]) * 100
    income = stats["income"]
    txt = (
        "🏦 <b>Экономика Клуба</b>\n\n"
        f"Кап: {stats['cap']}\n"
        f"В обороте: {stats['circulating']}\n"
        f"Сожжено: {stats['burned']} ({burned_pct:.2f}%)\n"
        f"В сейфе: {stats['vault']}\n"
        f"Сжигание (рынок): {pct}\n"
        f"Доходы (зп/кража): {income}"
    )
    await message.reply(txt, parse_mode="HTML")

# --------- конфиги сеттеры ---------

async def handle_burn_bps_set(message: types.Message, v: int):
    await set_burn_bps(v)
    await message.reply(f"Сжигание рынка установлено: {fmt_percent_bps(await get_burn_bps())}")

async def handle_price_emerald_set(message: types.Message, v: int):
    await set_price_emerald(v)
    await message.reply(f"Цена эмеральда: {v} нуаров.")

async def handle_price_perk_set(message: types.Message, code: str, v: int):
    code = code.strip().lower()
    if code not in PERK_REGISTRY:
        await message.reply("Такого перка нет.")
        return
    await set_price_perk(code, v)
    await message.reply(f"Цена перка «{PERK_REGISTRY[code][1]}»: {v} нуаров.")

async def handle_multiplier_set(message: types.Message, game: str, x: int):
    await set_multiplier(game, x)
    await message.reply(f"Множитель для «{game}» установлен: ×{x}")

async def handle_casino_toggle(message: types.Message):
    turn_on = message.text.strip().endswith("открыть")
    await set_casino_on(turn_on)
    await message.reply("🎰 Казино открыто." if turn_on else "🎰 Казино закрыто.")

async def handle_income_set(message: types.Message, v: int):
    await set_income(v)
    await message.reply(f"Доходы (зп/кража) установлены: {v}.")

async def handle_limit_bet_set(message: types.Message, v: int):
    await set_limit_bet(v)
    await message.reply("Лимит ставки отключён." if v == 0 else f"Лимит ставки: {v}.")

async def handle_limit_rain_set(message: types.Message, v: int):
    await set_limit_rain(v)
    await message.reply("Лимит дождя отключён." if v == 0 else f"Лимит дождя: {v}.")

# --------- «карман» куратора ---------

async def handle_kurator_karman(message: types.Message):
    if not message.reply_to_message:
        await message.reply("Этикет Клуба требует ответа на сообщение участника.")
        return
    target = message.reply_to_message.from_user
    balance = await get_balance(target.id)
    await message.reply(
        f"💼 {mention_html(target.id, target.full_name)} хранит в своём кармане {balance} нуаров.",
        parse_mode="HTML"
    )

# --------- динамический «список команд» ---------

async def handle_commands_catalog(message: types.Message):
    # Куратор
    curator = [
        "включить сейф <CAP> — включить экономику и рассчитать сейф",
        "перезапустить сейф <CAP> подтверждаю — аварийная переинициализация экономики",
        "сжигание <bps> — коэффициент сжигания рынка (100 = 1%)",
        "цена эмеральд <N> — установить цену эмеральда",
        "цена перк <код> <N> — установить цену перка",
        "множитель кубик|дартс|боулинг|автоматы <X> — множитель выигрыша",
        "казино открыть|закрыть — включить/выключить игры",
        "доходы <N> — размер ежедневной «зп» и кражи",
        "лимит ставка <N> — максимальная ставка за игру (0 — без лимита)",
        "лимит дождь <N> — максимальная сумма за одну команду «дождь» (0 — без лимита)",
        "у кого перк <код> — список обладателей перка",
        "перки реестр — сводка по всем перкам",
        "назначить \"роль\" <описание> (reply) — выдать роль",
        "снять роль (reply) — лишить роли",
        "ключ от сейфа (reply) / снять ключ (reply)",
        "обнулить баланс (reply) / обнулить балансы / обнулить клуб",
    ]
    # Владельцы ключа
    keyholders = [
        "вручить <N> (reply) — выдать из сейфа",
        "взыскать <N> (reply) — забрать в сейф",
        "карман (reply) — посмотреть баланс участника",
    ]
    # Члены клуба
    members = [
        "мой карман / моя роль / роль (reply)",
        "рейтинг клуба / члены клуба / хранители ключа",
        "передать <N> (reply) — перевод участнику",
        "дождь <N> — раздать до 5 случайным",
        "ставлю <N> на 🎲 — ставка в кубик",
        "рынок — витрина товаров и лотов",
        "купить эмеральд / купить перк <код> / купить <offer_id>",
        "выставить <ссылка> <цена> / снять лот <offer_id>",
        "мои перки / перки (reply)",
        "получить зп — ежедневная выплата по перку",
        "украсть / своровать (reply) — кража по перку «вор»",
        "сейф — сводка экономики клуба",
    ]

def bullets(items: list[str]) -> str:
    def fmt(s: str) -> str:
        # Любое <что-то> превратим в <code>что-то</code>
        return re.sub(r"<([^<>]+)>", r"<code>\1</code>", s)
    return "\n".join(f"• {fmt(s)}" for s in items)

    txt = (
        "📜 <b>Список команд</b>\n\n"
        "👑 <b>Куратор</b>\n" + bullets(curator) + "\n\n"
        "🗝 <b>Владельцы ключа</b>\n" + bullets(keyholders) + "\n\n"
        "🎭 <b>Члены клуба</b>\n" + bullets(members)
    )
    await message.reply(txt, parse_mode="HTML")
