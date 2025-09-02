import re
import os
import sys
import asyncio
from aiogram import types
from aiogram.types import FSInputFile

from db import (
    get_balance, change_balance, set_role, get_role,
    grant_key, revoke_key, has_key, get_last_history,
    get_top_users, get_all_roles, reset_user_balance,
    reset_all_balances, set_role_image, get_role_with_image,
    get_key_holders, grant_perk, revoke_perk, get_perks
)

KURATOR_ID = 164059195
DB_PATH = "/data/bot_data.sqlite"

# Код перка -> (эмоджи, человекочитаемое название)
PERK_REGISTRY = {
    "иммунитет": ("🛡️", "Иммунитет к бану"),
    # дальше можно добавлять новые:
    # "розыск": ("🚨", "Неприкосновенность при розыске"),
    # "флирт": ("💋", "Привилегия флирта"),
}


def mention_html(user_id: int, fallback: str = "Участник") -> str:
    return f"<a href='tg://user?id={user_id}'>{fallback}</a>"

def render_perks(perk_codes: set[str]) -> str:
    if not perk_codes:
        return "У Вас пока нет перков."
    lines = ["Ваши перки:"]
    # детерминированный порядок: по названию
    items = []
    for code in perk_codes:
        meta = PERK_REGISTRY.get(code)
        if meta:
            emoji, title = meta
            items.append((title.lower(), f"{emoji} {title}"))
        else:
            # неизвестные коды тоже покажем технически
            items.append((code, f"• {code}"))
    for _, line in sorted(items):
        lines.append(line)
    return "\n".join(lines)


async def handle_message(message: types.Message):
    if not message.text:
        return

    text = message.text.lower().strip()
    author_id = message.from_user.id

    # Игнорируем сообщения от ботов
    if message.from_user.is_bot:
        return

    # --- Команды для всех ---
    if text == "мой карман":
        bal = await get_balance(author_id)
        await message.reply(f"У Вас в кармане 🪙{bal} нуаров.")
        return

    if text == "моя роль":
        try:
            role_row = await get_role_with_image(author_id)
        except Exception:
            role_info = await get_role(author_id)
            role_row = (role_info.get("role"), role_info.get("description"), None) if role_info else None

        if role_row:
            role_name, role_desc, image_file_id = role_row
            text_response = f"🎭 *{role_name}*\n\n_{role_desc}_"
            if image_file_id:
                await message.reply_photo(photo=image_file_id, caption=text_response, parse_mode="Markdown")
            else:
                if author_id == KURATOR_ID and os.path.exists("images/kurator.jpg"):
                    try:
                        await message.reply_photo(photo=FSInputFile("images/kurator.jpg"), caption=text_response, parse_mode="Markdown")
                    except Exception:
                        await message.reply(text_response, parse_mode="Markdown")
                else:
                    await message.reply(text_response, parse_mode="Markdown")
        else:
            await message.reply("Я вас не узнаю.")
        return

    if text == "роль" and message.reply_to_message:
        target_id = message.reply_to_message.from_user.id
        try:
            role_row = await get_role_with_image(target_id)
        except Exception:
            role_info = await get_role(target_id)
            role_row = (role_info.get("role"), role_info.get("description"), None) if role_info else None

        if role_row:
            role_name, role_desc, image_file_id = role_row
            text_response = f"🎭 *{role_name}*\n\n_{role_desc}_"
            if image_file_id:
                await message.reply_photo(photo=image_file_id, caption=text_response, parse_mode="Markdown")
            else:
                await message.reply(text_response, parse_mode="Markdown")
        else:
            await message.reply("Я не знаю кто это.")
        return

    if text == "список команд":
        await handle_list(message)
        return

    if text == "клуб":
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

    if text == "рейтинг клуба":
        await handle_rating(message)
        return

    if text == "члены клуба":
        await handle_club_members(message)
        return

    if text == "хранители ключа":
        await handle_key_holders(message)
        return

    if text.startswith("передать "):
        await handle_peredat(message)
        return

    if text.startswith("ставлю"):
        await handle_kubik(message)
        return

    if text == "мои перки":
        await handle_my_perks(message)
        return

    # (опционально) посмотреть перки другого участника по reply
    if text == "перки" and message.reply_to_message:
        await handle_perks_of(message)
        return


    # --- Проверка ключа ---
    user_has_key = (author_id == KURATOR_ID) or await has_key(author_id)

    # --- Команды с ключом ---
    if user_has_key:
        if text.startswith(("вручить ", "выдать ")):
            await handle_vruchit(message)
            return
        if text.startswith(("взыскать ", "отнять ")):
            await handle_otnyat(message, text, author_id)
            return
        if text =="карман":
            await handle_kurator_karman(message)
            return

    # --- Команды только Куратора ---
    if author_id == KURATOR_ID:
        if text.startswith("назначить ") and message.reply_to_message:
            await handle_naznachit(message)
            return
        if text == "снять роль" and message.reply_to_message:
            await handle_snyat_rol(message)
            return
        if text == "ключ от сейфа" and message.reply_to_message:
            await handle_kluch(message)
            return
        if text == "снять ключ" and message.reply_to_message:
            await handle_snyat_kluch(message)
            return
        if text == "обнулить клуб":
            await asyncio.sleep(1)
            await handle_clear_db(message)
            return
        if text.startswith("обнулить балансы"):
            await handle_obnulit_balansy(message)
            return
        if text.startswith("обнулить баланс"):
            await handle_obnulit_balans(message)
            return
        # выдача/снятие конкретного перка (пример — "иммунитет") в ответ на сообщение участника
        if text == "даровать иммунитет" and message.reply_to_message:
            await handle_grant_immunity(message)
            return
        if text == "снять иммунитет" and message.reply_to_message:
            await handle_revoke_immunity(message)
            return



async def handle_photo_command(message: types.Message):
    # Только куратор устанавливает фото роли
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

# --- Ключевые обработчики ---

async def handle_vruchit(message: types.Message):
    if message.reply_to_message:
        m = re.match(r"(?:вручить|выдать)\s+(-?\d+)", message.text.strip(), re.IGNORECASE)
        if not m:
            await message.reply("Обращение не по этикету Клуба. Пример: 'вручить|выдать 5'")
            return
        amount = int(m.group(1))
        if amount <= 0:
            await message.reply("Я не могу выдать минус.")
            return
        recipient = message.reply_to_message.from_user
        await change_balance(recipient.id, amount, "без причины", message.from_user.id)
        await message.reply(
            f"🧮Я выдал {amount} нуаров {mention_html(recipient.id, recipient.full_name)}",
            parse_mode="HTML"
        )

async def handle_otnyat(message: types.Message, text: str, author_id: int):
    if message.reply_to_message:
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

        await change_balance(recipient.id, -amount, "без причины", author_id)
        await message.reply(
            f"🧮Я взыскал {amount} нуаров у {mention_html(recipient.id, recipient.full_name)}",
            parse_mode="HTML"
        )

async def handle_naznachit(message: types.Message):
    # Формат: назначить "название роли" описание роли
    m = re.match(r'назначить\s+"([^"]+)"\s+(.+)', message.text.strip(), re.IGNORECASE)
    if not m:
        await message.reply('Я не совсем понял')
        return
    role_name, role_desc = m.groups()

    if not message.reply_to_message:
        await message.reply("Кому мне выдать роль, Куратор?")
        return

    user_id = message.reply_to_message.from_user.id
    await set_role(user_id, role_name, role_desc)
    uname = message.reply_to_message.from_user.username
    fname = message.reply_to_message.from_user.full_name
    mention = f"@{uname}" if uname else mention_html(user_id, fname)
    await message.reply(f"Назначена роль '{role_name}' пользователю {mention}", parse_mode="HTML")

async def handle_snyat_rol(message: types.Message):
    if not message.reply_to_message:
        await message.reply("Но кого мне лишить роли, Куратор?")
        return
    user_id = message.reply_to_message.from_user.id
    await set_role(user_id, None, None)
    uname = message.reply_to_message.from_user.username
    fname = message.reply_to_message.from_user.full_name
    mention = f"@{uname}" if uname else mention_html(user_id, fname)
    await message.reply(f"Роль снята у {mention}", parse_mode="HTML")

async def handle_kluch(message: types.Message):
    if not message.reply_to_message:
        await message.reply("Кому мне выдать ключ, Куратор?")
        return
    user_id = message.reply_to_message.from_user.id
    await grant_key(user_id)
    uname = message.reply_to_message.from_user.username
    fname = message.reply_to_message.from_user.full_name
    mention = f"@{uname}" if uname else mention_html(user_id, fname)
    await message.reply(f"🗝Ключ от сейфа выдан {mention}", parse_mode="HTML")

async def handle_snyat_kluch(message: types.Message):
    if not message.reply_to_message:
        await message.reply("У кого мне отобрать ключ, Куратор?")
        return
    user_id = message.reply_to_message.from_user.id
    await revoke_key(user_id)
    uname = message.reply_to_message.from_user.username
    fname = message.reply_to_message.from_user.full_name
    mention = f"@{uname}" if uname else mention_html(user_id, fname)
    await message.reply(f"🗝Ключ от сейфа отнят у {mention}", parse_mode="HTML")

async def handle_list(message: types.Message):
    try:
        with open("Список команд.txt", "r", encoding="utf-8") as f:
            help_text = f.read()
        await message.reply(help_text)
    except Exception as e:
        print(f"Ошибка при чтении списка команд: {e}")
        await message.reply("Не удалось загрузить список команд.")

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
        # как в рейтинге: пытаемся взять полное имя из текущего чата
        name = "Участник"
        try:
            member = await message.bot.get_chat_member(message.chat.id, user_id)
            name = member.user.full_name or name
        except Exception:
            pass

        mention = mention_html(user_id, name)  # кликабельное имя, не @username
        lines.append(f"{mention} — <b>{role}</b>")

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

async def handle_key_holders(message: types.Message):
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

async def handle_peredat(message: types.Message):
    # Команда работает ТОЛЬКО в ответ на сообщение получателя
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

    # Нельзя переводить себе
    if giver_id == recipient_id:
        await message.reply("Нельзя передать нуары самому себе.")
        return

    # Проверяем баланс дарителя
    balance = await get_balance(giver_id)
    if amount > balance:
        await message.reply(f"У Вас недостаточно нуаров. Баланс: {balance}")
        return

    # Списываем у дарителя и зачисляем получателю
    await change_balance(giver_id, -amount, "передача", giver_id)
    await change_balance(recipient_id, amount, "передача", giver_id)

    giver_name = message.from_user.full_name
    recipient_name = recipient.full_name

    await message.reply(
        f"💸Я передал {amount} нуаров от {mention_html(giver_id, giver_name)} к {mention_html(recipient_id, recipient_name)}",
        parse_mode="HTML"
    )

async def handle_kurator_karman(message: types.Message):
    # Работает только в ответ на сообщение
    if not message.reply_to_message:
        await message.reply("Этикет Клуба требует ответа на сообщение участника.")
        return

    target = message.reply_to_message.from_user
    balance = await get_balance(target.id)

    await message.reply(
        f"💼 {mention_html(target.id, target.full_name)} хранит в своём кармане {balance} нуаров.",
        parse_mode="HTML"
    )

async def handle_kubik(message: types.Message):

    m = re.match(r"^\s*ставлю\s+(\d+)\s+на\s+(?:🎲|кубик)\s*$", message.text.strip(), re.IGNORECASE)
    if not m:
        await message.reply("Обращение не по этикету Клуба. Пример: 'Ставлю 10 на 🎲|кубик'")
        return

    amount = int(m.group(1))
    if amount <= 0:
        await message.reply("Я не могу принять отрицательную ставку.")
        return

    gambler_id = message.from_user.id
    gambler_name = message.from_user.full_name

    # Проверяем баланс лудика
    balance = await get_balance(gambler_id)
    if amount > balance:
        await message.reply(f"🔍У Вас недостаточно нуаров. Баланс: {balance}")
        return

    # Бросаем кубик сервером Телеграма
    sent: types.Message = await message.answer_dice(emoji="🎲")
    roll_value = sent.dice.value  # 1..6

    await asyncio.sleep(3.5)
    if roll_value == 6:
        await change_balance(gambler_id, amount*3, "ставка", gambler_id)
        await message.reply(
            f"🎉Фортуна на вашей стороне,{mention_html(gambler_id, gambler_name)}. Вы получаете 🪙{amount*3} нуаров",
            parse_mode="HTML"
        )
    else:
        await change_balance(gambler_id, -amount, "ставка", gambler_id)
        await message.reply(
            f"🪦Ставки погубят вас, {mention_html(gambler_id, gambler_name)}. Вы потеряли 🪙{amount} нуаров.",
            parse_mode="HTML"
        )




async def handle_my_perks(message: types.Message):
    perk_codes = await get_perks(message.from_user.id)
    await message.reply(render_perks(perk_codes))

async def handle_perks_of(message: types.Message):
    # по reply — перки другого участника
    target = message.reply_to_message.from_user
    perk_codes = await get_perks(target.id)
    if not perk_codes:
        await message.reply(f"У {target.full_name} пока нет перков.")
        return
    # покажем с именем
    lines = [f"Перки {mention_html(target.id, target.full_name)}:"]
    # используем тот же сортировщик
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

# --- Кураторские: иммунитет ---
async def handle_grant_immunity(message: types.Message):
    target = message.reply_to_message.from_user
    await grant_perk(target.id, "иммунитет")
    await message.reply(f"Иммунитет дарован @{target.username or target.full_name}")

async def handle_revoke_immunity(message: types.Message):
    target = message.reply_to_message.from_user
    await revoke_perk(target.id, "иммунитет")
    await message.reply(f"Иммунитет снят у @{target.username or target.full_name}")
