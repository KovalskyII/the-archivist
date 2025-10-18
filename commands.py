import re
import os
import sys
import asyncio
import random
import html
from typing import List, Tuple
from datetime import datetime, timezone
import aiosqlite

from aiogram import types
from aiogram.types import FSInputFile

from db import (
    # базовые
    get_balance, change_balance, set_role, get_role,
    grant_key, revoke_key, has_key, get_last_history,
    get_top_users, get_all_roles, reset_user_balance,
    reset_all_balances, set_role_image, get_role_with_image,
    get_key_holders, get_known_users, hero_get_current, hero_set_for_today,
    hero_has_claimed_today, hero_record_claim,
    get_stipend_base, get_stipend_bonus, set_stipend_base, set_stipend_bonus,
    get_generosity_mult_pct, add_generosity_points, generosity_try_payout,
    get_market_turnover_days, codeword_get_active, codeword_mark_win,
    codeword_set, codeword_cancel_active, set_generosity_mult_pct,
    set_generosity_threshold, set_price_pin, set_price_pin_loud,
    insert_history, get_circulating, get_price_pin, get_price_pin_loud,
    get_generosity_points, get_generosity_threshold, hero_get_current_with_until,
    get_perk_shield_chance, set_perk_shield_chance,
    get_perk_croupier_chance, set_perk_croupier_chance,
    get_perk_philanthrope_chance, set_perk_philanthrope_chance,
    get_perk_lucky_chance, set_perk_lucky_chance,
    cell_get_balance, cell_deposit, cell_withdraw,
    bank_touch_all_and_total, bank_zero_all_and_sum,
    get_cell_stor_fee_pct,
    get_seconds_since_last_bank_rob, record_bank_rob,
    get_bank_rob_cooldown_days, set_bank_rob_cooldown_days,
    get_cell_dep_fee_pct, set_cell_dep_fee_pct,
    get_cell_stor_fee_pct, set_cell_stor_fee_pct,
    get_perk_credits, perk_credit_add, perk_credit_use,
    create_perk_offer, get_perk_escrow_owner, perk_escrow_open, perk_escrow_close,
    get_pin_q_mult,




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
CLUB_CHAT_ID = -1002431055065
ALLOWED_CONCERT_CHATS = {CLUB_CHAT_ID}

DB_PATH = "/data/bot_data.sqlite"

# --- Герой дня (концерт) ---
HERO_CONCERT_MIN = 10
HERO_CONCERT_MAX = 50
HERO_TITLE = "Певец дня"  # название титула

BET_LOCKS: dict[int, asyncio.Lock] = {}
def get_bet_lock(uid: int) -> asyncio.Lock:
    lock = BET_LOCKS.get(uid)
    if lock is None:
        lock = BET_LOCKS[uid] = asyncio.Lock()
    return lock

# Код перка -> (эмоджи, человекочитаемое название)
PERK_REGISTRY = {
    "надбавка": ("💼", "Надбавка к жалованию"),
    "кража": ("🗡️", "Своровать нуары"),
    # NEW:
    "щит": ("🛡️", "Щит от кражи"),              # 50% сорвать кражу
    "крупье": ("🎩", "Крупье"),                 # 15% рефанд 50% ставки при проигрыше
    "филантроп": ("🎁", "Филантроп"),           # 15% шестой получатель дождя за счёт сейфа
    "везунчик": ("🍀", "Везунчик"),             # 33% стать шестым в чужом дожде
    "премия": ("🏅", "Премия"),
    "грабитель": ("🧨", "Грабитель банка"),
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

def chance(pct: float) -> bool:
    try:
        return random.random() < (float(pct) / 100.0)
    except:
        return False


# -------- вспомогательные форматтеры --------

def fmt_int(n: int) -> str:
    return f"{n:,}"

def fmt_money(n: int) -> str:
    return f"{fmt_int(n)} 🪙"

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

    from db import touch_user
    await touch_user(author_id, message.from_user.username)

    if message.from_user.is_bot:
        return

    # --- ловушка для код-слова (только в целевом чате) ---
    if message.text and message.chat.id == CLUB_CHAT_ID:
        cw = await codeword_get_active(CLUB_CHAT_ID)
        if cw:
            def norm(s: str) -> str:
                return re.sub(r"[^a-zA-Zа-яА-ЯёЁ0-9]+", "", s).lower().strip()
            guess  = norm(message.text)
            target = norm(cw["word"] or "")
            if target and guess == target:
                prize = int(cw["prize"])
                await change_balance(message.from_user.id, prize, "codeword_prize", message.from_user.id)
                await codeword_mark_win(CLUB_CHAT_ID, message.from_user.id, prize, cw["word"])
                await message.reply(
                    f"🎉 Слово угадано! Конечно же это — <b>{html.escape(cw['word'])}</b>.\n"
                    f"Ты получаешь: {fmt_money(prize)}.",
                    parse_mode="HTML"
                )
                return

    # ======= Команды для всех =======
    t = message.text.lower().strip().split("@", 1)[0]
    if t in ("список команд", "команды", "/команды", "/help"):
        try:
            await handle_commands_catalog(message)
        except Exception as e:
            # подстраховка: покажем понятную ошибку, чтобы не падать
            await message.reply(f"Не удалось сформировать список команд: {e}")
        return

        
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

    if text_l in ("хранители ключа", "владельцы ключа"):
        await handle_key_holders_cmd(message)
        return

    if text_l.startswith("передать "):
        await handle_peredat(message)
        return

    if text_l.startswith("ставлю"):
        tl = text_l
        if ("🎲" in tl) or ("кубик" in tl):
            await handle_kubik(message); return
        if ("🎯" in tl) or ("дартс" in tl):
            await handle_darts(message); return
        if ("🎳" in tl) or ("боулинг" in tl):
            await handle_bowling(message); return
        if ("🎰" in tl) or ("автоматы" in tl) or ("слоты" in tl):
            await handle_slots(message); return
        # если не распознали игру — подскажем формат
        await message.reply("Уточните игру: «ставлю N на 🎲/кубик | 🎯/дартс | 🎳/боулинг | 🎰/автоматы».")
        return

    if text_l == "мои перки":
        await handle_my_perks(message)
        return

    if text_l == "перки" and message.reply_to_message:
        await handle_perks_of(message)
        return

    if text_l in ("получить жалование", "я сру"):
        await handle_stipend_claim(message)
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

    # разместить лот: поддержка пробелов в ссылке, цена — последнее число
    raw = message.text.strip()
    m = re.match(r'^выставить\s+(.+?)\s+(\d+)\s*$', raw, flags=re.IGNORECASE | re.DOTALL)
    if m:
        link = m.group(1).strip()
        price = int(m.group(2))
        await handle_offer_create(message, link, price)
        return

    m = re.match(r"^купить\s+(\d+)$", text_l)
    if m:
        await handle_offer_buy(message, int(m.group(1)))
        return

    m = re.match(r"^купить\s+лот\s+(\d+)$", text_l)
    if m:
        await handle_offer_buy(message, int(m.group(1)))
        return

    m = re.match(r"^снять\s+лот\s+(\d+)$", text_l)
    if m:
        await handle_offer_cancel(message, int(m.group(1)))
        return

    m = re.match(r"^продать\s+перк\s+(\S+)\s+(\d+)$", text_l)
    if m:
        code = m.group(1).strip().lower()
        price = int(m.group(2))
        await handle_perk_sell(message, code, price)
        return


    # кража
    if text_l in ("украсть", "своровать") and message.reply_to_message:
        await handle_theft(message)
        return

    # экономика/сейф
    if text_l == "сейф":
        await handle_vault_stats(message)
        return

    # держатели перка / реестр
    m = re.match(r"^(?:у кого перк|держатели перка)\s+(\S+)$", text_l)
    if m:
        await handle_perk_holders_list(message, m.group(1))
        return

    if text_l == "перки реестр":
        await handle_perk_registry(message)
        return

    if text_l == "концерт":
        # только в разрешённых группах/супергруппах
        if message.chat.type not in ("group", "supergroup") or message.chat.id not in ALLOWED_CONCERT_CHATS:
            await message.reply("Команда «концерт» доступна только в клубном чате.")
            return
        await handle_hero_of_day(message)
        return

    if text_l == "выступить":
        # только в разрешённых группах/супергруппах
        if message.chat.type not in ("group", "supergroup") or message.chat.id not in ALLOWED_CONCERT_CHATS:
            await message.reply("Команда «выступить» доступна только в клубном чате.")
            return
        await handle_hero_concert(message)
        return

    if text_l == "закрепить пост":
        await _pin_paid(message, loud=False); return
    if text_l == "закрепить пост громко":
        await _pin_paid(message, loud=True); return

        # ===== ЯЧЕЙКИ / БАНК =====
    m = re.match(r"^депозит\s+(\d+)$", text_l)
    if m:
        await handle_cell_deposit_cmd(message, int(m.group(1)))
        return

    m = re.match(r"^(?:вывод|вывести)\s+(\d+)$", text_l)
    if m:
        await handle_cell_withdraw_cmd(message, int(m.group(1)))
        return

    if text_l in ("ячейка", "моя ячейка"):
        await handle_cell_balance_cmd(message)
        return

    if text_l == "банк":
        await handle_bank_summary_cmd(message)
        return

    if text_l == "ограбить банк":
        await handle_bank_rob_cmd(message)
        return

    if text_l in ("вывод все", "вывести все","вывод всё", "вывести всё"):
        await handle_cell_withdraw_all_cmd(message)
        return

    m = re.match(r"^сжечь\s+(\d+)$", text_l)
    if m:
        await handle_burn_cmd(message, int(m.group(1)))
        return


    # ======= ЩЕДРОСТЬ (только Куратор, но работает и в ЛС, и в чате) =======
    if text_l.startswith("щедрость"):
        # только Куратор
        if author_id != KURATOR_ID:
            await message.reply("Эта команда доступна только Куратору.")
            return

        # статус
        if text_l.strip() == "щедрость статус":
            try:
                pts  = await get_generosity_points(message.from_user.id)
                mult = await get_generosity_mult_pct()
                thr  = await get_generosity_threshold()
                await message.reply(f"Щедрость: множитель {mult}%, порог {thr}, у вас очков: {pts}.")
            except Exception as e:
                await message.reply(f"Ошибка статуса щедрости: {e}")
            return

        # очки (reply = чьи-то, иначе — свои)
        if text_l.strip() == "щедрость очки":
            try:
                uid = message.reply_to_message.from_user.id if message.reply_to_message else message.from_user.id
                name = (message.reply_to_message.from_user.full_name
                        if message.reply_to_message else message.from_user.full_name)
                pts = await get_generosity_points(uid)
                await message.reply(f"Очки щедрости у {html.escape(name)}: {pts}.")
            except Exception as e:
                await message.reply(f"Ошибка чтения очков: {e}")
            return

        # щедрость множитель <p>
        m = re.match(r"^щедрость\s+множитель\s+(\d+)\s*$", text_l)
        if m:
            v = int(m.group(1))
            await set_generosity_mult_pct(v)
            cur = await get_generosity_mult_pct()
            await message.reply(f"🛠️ Готово. Множитель щедрости: {cur}%.")
            return

        # щедрость награда <N>
        m = re.match(r"^щедрость\s+награда\s+(\d+)\s*$", text_l)
        if m:
            v = int(m.group(1))
            await set_generosity_threshold(v)
            cur = await get_generosity_threshold()
            await message.reply(f"🛠️ Готово. Порог награды щедрости: {fmt_money(cur)}.")
            return

        # обнуление очков конкретному участнику (reply)
        if text_l.strip() == "щедрость обнулить" and message.reply_to_message:
            try:
                uid = message.reply_to_message.from_user.id
                pts = await _generosity_reset_points_for(uid)
                await message.reply(f"Очки щедрости обнулены. Списано: {pts}.")
            except Exception as e:
                await message.reply(f"Ошибка обнуления: {e}")
            return

        # массовое обнуление
        if text_l.strip() == "щедрость обнулить все подтверждаю":
            try:
                total_users = 0
                total_pts = 0
                for uid in await get_known_users():
                    pts = await get_generosity_points(uid)
                    if pts > 0:
                        await insert_history(uid, "generosity_pay_points", pts, "reset_all")
                        total_pts += pts
                        total_users += 1
                await message.reply(f"Обнуление завершено. Пользователей: {total_users}, списано очков: {total_pts}.")
            except Exception as e:
                await message.reply(f"Ошибка массового обнуления: {e}")
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

        if text_l.startswith("включить сейф"):
            await handle_vault_enable(message)
            return

        if text_l.startswith("перезапустить сейф"):
            await handle_vault_reset(message)
            return

        m = re.match(r"^установить\s+код\s+(\S+)\s+(\d+)\s*(.*)$", text_l)
        if m and author_id == KURATOR_ID:
            if message.chat.type != "private":
                await message.reply("Загадывать код можно только в ЛС. Напишите мне в личку.")
                return

            word = m.group(1)
            prize = int(m.group(2))
            hint  = (m.group(3) or "").strip()
            target_chat_id = CLUB_CHAT_ID

            cur = await codeword_get_active(target_chat_id)
            if cur:
                await message.reply("Уже запущена игра КОД-СЛОВО. Сначала отмените текущую.")
                return

            await codeword_set(target_chat_id, word.lower(), prize, KURATOR_ID)

            try:
                extra_hint = f"\n<b>Подсказка:</b> {html.escape(hint)}" if hint else ""
                await message.bot.send_message(
                    target_chat_id,
                    "🧩 <b>Викторина «КОД-СЛОВО»</b>\n\n"
                    f"Угадайте слово, загаданное Куратором и получите {fmt_money(prize)}."
                    + extra_hint,
                    parse_mode="HTML"
                )
                await message.reply("Код установлен. Я объявил игру в Клубе — ждём угадывания там.")
            except Exception as e:
                await message.reply(
                    f"Код установлен, но объявить в Клубе не удалось ({e}). "
                    f"Проверь права бота и CLUB_CHAT_ID."
                )
            return



        if text_l == "отменить код" and author_id == KURATOR_ID:
            target_chat_id = CLUB_CHAT_ID
            ok = await codeword_cancel_active(target_chat_id, KURATOR_ID)
            if ok:
                await message.reply("Игра отменена.")
                try:
                    await message.bot.send_message(target_chat_id, "🛑 Викторина КОД-СЛОВО остановлена.")
                except Exception:
                    pass
            else:
                await message.reply("Активной игры в Клубе нет.")
            return

        # сжигание <bps>
        m = re.match(r"^сжигание\s+(\d+)$", text_l)
        if m:
            await set_burn_bps(int(m.group(1)))
            cur = await get_burn_bps()
            await message.reply(f"🛠️ Готово. Сжигание установлено на {fmt_percent_bps(cur)}.")
            return

        # цена эмеральд <N>
        m = re.match(r"^цена\s+эмеральд\s+(\d+)$", text_l)
        if m:
            v = int(m.group(1))
            await set_price_emerald(v)
            cur = await get_price_emerald()
            await message.reply(f"🛠️ Готово. Цена Эмеральда: {fmt_money(cur)}.")
            return

        # цена перк <код> <N>
        m = re.match(r"^цена\s+перк\s+(\S+)\s+(\d+)$", text_l)
        if m:
            code = m.group(1).strip().lower()
            v = int(m.group(2))
            if code not in PERK_REGISTRY:
                await message.reply("Такого перка нет.")
                return
            await set_price_perk(code, v)
            cur = await get_price_perk(code)
            await message.reply(f"🛠️ Готово. Цена перка «{PERK_REGISTRY[code][1]}»: {fmt_money(cur)}.")
            return

        # множитель <игра> <X>
        m = re.match(r"^множитель\s+(кубик|дартс|боулинг|автоматы)\s+(\d+)$", text_l)
        if m:
            game = m.group(1)
            x = int(m.group(2))
            await set_multiplier(game, x)
            await message.reply(f"🛠️ Готово. Множитель для «{game}»: ×{x}.")
            return

        # казино открыть|закрыть
        if text_l in ("казино открыть", "казино закрыть"):
            turn_on = text_l.endswith("открыть")
            await set_casino_on(turn_on)
            await message.reply("🎰 Казино открыто." if turn_on else "🎰 Казино закрыто.")
            return

        # лимит ставка <N>
        m = re.match(r"^лимит\s+ставка\s+(\d+)$", text_l)
        if m:
            v = int(m.group(1))
            await set_limit_bet(v)
            await message.reply("🛠️ Лимит ставки отключён." if v == 0 else f"🛠️ Лимит ставки: {fmt_int(v)}.")
            return


        if t in ("команды куратора", "мои команды", "/команды_куратора"):
            await handle_commands_curator(message)
            return

        m = re.match(r"^щит\s+шанс\s+(\d+)\s*$", text_l)
        if m:
            p = int(m.group(1))
            await set_perk_shield_chance(p)
            cur = await get_perk_shield_chance()
            await message.reply(f"🛡️ Шанс перка «Щит» обновлён: {cur}%")
            return

        m = re.match(r"^крупье\s+шанс\s+(\d+)\s*$", text_l)
        if m:
            p = int(m.group(1))
            await set_perk_croupier_chance(p)
            cur = await get_perk_croupier_chance()
            await message.reply(f"🎲 Шанс перка «Крупье» обновлён: {cur}%")
            return

        m = re.match(r"^филантроп\s+шанс\s+(\d+)\s*$", text_l)
        if m:
            p = int(m.group(1))
            await set_perk_philanthrope_chance(p)
            cur = await get_perk_philanthrope_chance()
            await message.reply(f"🎁 Шанс перка «Филантроп» обновлён: {cur}%")
            return

        m = re.match(r"^везунчик\s+шанс\s+(\d+)\s*$", text_l)
        if m:
            p = int(m.group(1))
            await set_perk_lucky_chance(p)
            cur = await get_perk_lucky_chance()
            await message.reply(f"🍀 Шанс перка «Везунчик» обновлён: {cur}%")
            return

        # банк комиссия депозит <P>
        m = re.match(r"^банк\s+комиссия\s+депозит\s+(\d+)\s*$", text_l)
        if m:
            p = int(m.group(1))
            await set_cell_dep_fee_pct(p)
            cur = await get_cell_dep_fee_pct()
            await message.reply(f"🛠️ Комиссия депозита установлена: {cur}%")
            return

        # банк комиссия хранение <P> (за 4 часа)
        m = re.match(r"^банк\s+комиссия\s+хранение\s+(\d+)\s*$", text_l)
        if m:
            p = int(m.group(1))
            await set_cell_stor_fee_pct(p)
            cur = await get_cell_stor_fee_pct()
            await message.reply(f"🛠️ Комиссия хранения установлена: {cur}% / 6ч")
            return

        # банк кд <дней>
        m = re.match(r"^грабитель\s+кд\s+(\d+)\s*$", text_l)
        if m:
            d = int(m.group(1))
            await set_bank_rob_cooldown_days(d)
            cur = await get_bank_rob_cooldown_days()
            await message.reply(f"🛠️ КД перка «Грабитель» установлен: {cur} дн.")
            return

        # новый хендлер базового индекса
        m = re.match(r"^индекс\s+(\d+)$", text_l)
        if m and author_id == KURATOR_ID:
            base = int(m.group(1))
            await set_stipend_base(base)
            bonus_mult = 4  # можно вынести в конфиг при желании
            bonus = base * bonus_mult
            await set_stipend_bonus(bonus)
            await set_income(bonus)
            pin_q = await get_pin_q_mult()
            await set_price_pin(bonus)
            await set_price_pin_loud(bonus * 2)
            cur_b = await get_stipend_base()
            cur_bonus = await get_stipend_bonus()
            cur_income = await get_income()
            await message.reply(
                "🛠️ Индекс обновлён.\n"
                f"• База жалования: {fmt_money(cur_b)}\n"
                f"• Надбавка: {fmt_money(cur_bonus)}\n"
                f"• Кража: {fmt_money(cur_income)}\n"
                f"• Цена тихого пина: {fmt_money(await get_price_pin())}\n"
                f"• Цена громкого пина: {fmt_money(await get_price_pin_loud())}"
            )
            return




# ---------- базовые куски (ролы, фото, рейтинги и т.п.) ----------
# === Кураторские хендлеры ролей и ключа (точно под старую логику) ===

async def handle_naznachit(message: types.Message):
    """
    Формат:  назначить "Роль" Описание
    ВАЖНО: название роли строго в двойных кавычках, эмодзи в команду не передаём.
    Работает ТОЛЬКО reply (на того, кому назначаем).
    """
    if message.from_user.id != KURATOR_ID:
        return
    if not message.reply_to_message:
        await message.reply('Нужно ответить на сообщение участника. Формат: назначить "Роль" Описание')
        return

    # Парсим ИЗ ОРИГИНАЛЬНОГО ТЕКСТА, без lower(), чтобы не сломать регистр/символы роли и описания
    raw = (message.text or "").strip()
    m = re.match(r'^\s*назначить\s+"([^"]+)"\s+(.+)\s*$', raw, flags=re.DOTALL)
    if not m:
        await message.reply('Формат: назначить "Роль" Описание\nПример: назначить "Аристократ" Любит тонкий юмор')
        return

    role_name = m.group(1).strip()
    role_desc = m.group(2).strip()

    target = message.reply_to_message.from_user
    # set_role ожидает (user_id, role, description)
    await set_role(target.id, role_name, role_desc)

    # Превью в том же стиле, как «моя роль»/«роль»
    preview = f"🎭 *{role_name}*\n\n_{role_desc}_"
    await message.reply_to_message.reply(preview, parse_mode="Markdown")


async def handle_snyat_rol(message: types.Message):
    """
    Снимает роль у адресата (reply). Фото роли не трогаем.
    """
    if message.from_user.id != KURATOR_ID:
        return
    if not message.reply_to_message:
        await message.reply("Нужно ответить на сообщение участника.")
        return

    target = message.reply_to_message.from_user
    # Сброс роли: кладём None/None — чтение «моя роль» корректно покажет «не знаю»
    await set_role(target.id, None, None)
    await message.reply_to_message.reply("Роль снята.")


async def handle_kluch(message: types.Message):
    """
    Выдать ключ от сейфа (reply).
    Владельцы ключа могут: «вручить», «взыскать/отнять», «карман».
    """
    if message.from_user.id != KURATOR_ID:
        return
    if not message.reply_to_message:
        await message.reply("Нужно ответить на сообщение участника.")
        return

    target = message.reply_to_message.from_user
    await grant_key(target.id)
    await message.reply_to_message.reply(f"🗝️ Ключ от сейфа выдан {mention_html(target.id, target.full_name)}.", parse_mode="HTML")


async def handle_snyat_kluch(message: types.Message):
    """
    Снять ключ от сейфа (reply).
    """
    if message.from_user.id != KURATOR_ID:
        return
    if not message.reply_to_message:
        await message.reply("Нужно ответить на сообщение участника.")
        return

    target = message.reply_to_message.from_user
    await revoke_key(target.id)
    await message.reply_to_message.reply(f"🗝️ Ключ от сейфа снят с {mention_html(target.id, target.full_name)}.", parse_mode="HTML")



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
    lines = ["💰 <b>Богатейшие члены Клуба Le Cadeau Noir:</b>\n"]
    for i, (user_id, balance) in enumerate(rows, start=1):
        name = "Участник"
        try:
            member = await message.bot.get_chat_member(message.chat.id, user_id)
            name = member.user.full_name or name
        except Exception:
            pass
        lines.append(f"{i}. {mention_html(user_id, name)} — {fmt_money(balance)}")
    await message.reply("\n".join(lines), parse_mode="HTML")

async def handle_club_members(message: types.Message):
    rows = await get_all_roles()
    if not rows:
        await message.reply("Пока что в клубе пусто.")
        return
    lines = ["🎭 <b>Члены Клуба Le Cadeau Noir:</b>\n"]
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
        await message.reply(f"В сейфе недостаточно нуаров. Доступно: {fmt_money(room)}")
        return

    recipient = message.reply_to_message.from_user
    await change_balance(recipient.id, amount, "выдача из сейфа", message.from_user.id)
    await message.reply(f"🧮Я выдал {fmt_money(amount)} {mention_html(recipient.id, recipient.full_name)}", parse_mode="HTML")

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
        await message.reply(f"У {html.escape(recipient.full_name)} нет такого количества нуаров. Баланс: {fmt_money(current_balance)}")
        return
    await change_balance(recipient.id, -amount, "взыскание в сейф", author_id)
    await message.reply(f"🧮Я взыскал {fmt_money(amount)} у {mention_html(recipient.id, recipient.full_name)}", parse_mode="HTML")

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
        await message.reply(f"У Вас недостаточно нуаров. Баланс: {fmt_money(balance)}")
        return
    await change_balance(giver_id, -amount, "передача", giver_id)
    await change_balance(recipient_id, amount, "передача", giver_id)
    pct = await get_generosity_mult_pct()
    pts = (amount * pct) // 100
    await add_generosity_points(giver_id, pts, "transfer")
    payout = await generosity_try_payout(giver_id)
    if payout > 0:
        await message.reply(f"🎁 Бонус щедрости: +{fmt_money(payout)}")
    await message.reply(
        f"💸Я передал {fmt_money(amount)} от {mention_html(giver_id, message.from_user.full_name)} к {mention_html(recipient_id, recipient.full_name)}",
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
        await message.reply(f"Лимит дождя: не более {fmt_money(max_rain)} за одну команду.")
        return

    giver_id = message.from_user.id
    bal = await get_balance(giver_id)
    if total > bal:
        await message.reply(f"У Вас недостаточно нуаров. Баланс: {fmt_money(bal)}")
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
    # --- новая логика выбора получателей ---
    # веса: базовый 100; для "везунчиков" 100 + p_lucky
    p_lucky = await get_perk_lucky_chance()
    weights = []
    lucky_ids = set()  # соберём всех, у кого есть перк «везунчик»
    for uid, name in eligible:
        perks_u = await get_perks(uid)
        is_lucky = ("везунчик" in perks_u)
        if is_lucky:
            lucky_ids.add(uid)
        w = 100 + (p_lucky if is_lucky else 0)
        weights.append(w)

    # взвешенная выборка без замены на 5 человек
    def weighted_sample_without_replacement(items, weights, k):
        items = list(items)
        weights = list(weights)
        chosen = []
        for _ in range(min(k, len(items))):
            total_w = sum(weights)
            r = random.uniform(0, total_w)
            acc = 0.0
            pick_idx = 0
            for i, w in enumerate(weights):
                acc += w
                if r <= acc:
                    pick_idx = i
                    break
            chosen.append(items[pick_idx])
            items.pop(pick_idx)
            weights.pop(pick_idx)
        return chosen, items  # (выбранные, оставшиеся)

    recipients, rest_pool = weighted_sample_without_replacement(eligible, weights, 5)

    n = len(recipients)
    base = total // n
    rest = total % n
    per_user = [base + (1 if i < rest else 0) for i in range(n)]

    # списываем у дарителя и начисляем пятёрке
    await change_balance(giver_id, -total, "дождь", giver_id)
    for (uid, _name), amt in zip(recipients, per_user):
        if amt > 0:
            await change_balance(uid, amt, "дождь", giver_id)

    # Отметим «везунчиков» среди получателей и добавим резюме
    lucky_among_recipients = sum(1 for uid, _name in recipients if uid in lucky_ids)

    def name_with_tags(uid, name):
        tag = " 🍀" if uid in lucky_ids else ""
        return f"{mention_html(uid, name)}{tag}"

    # --- «Филантроп»: шестой равновероятно из оставшихся ---
    giver_perks = await get_perks(giver_id)
    extra_lines = []
    base_share = per_user[0] if per_user else 0

    if "филантроп" in giver_perks and base_share > 0 and chance(await get_perk_philanthrope_chance()):
        if rest_pool:
            sixth_uid, sixth_name = random.choice(rest_pool)  # равномерно среди оставшихся
            await change_balance(sixth_uid, base_share, "дождь_филантроп", giver_id)
            extra_lines.append(f"{mention_html(sixth_uid, sixth_name)} — получил дополнительно {fmt_money(base_share)} от филантропа")


    breakdown = [
        f"{name_with_tags(uid, name)} — намок на {fmt_money(amt)}"
        for (uid, name), amt in zip(recipients, per_user) if amt > 0
    ]

    if extra_lines:
        breakdown.extend(extra_lines)
    
    pct = await get_generosity_mult_pct()
    pts = (total * pct) // 100
    await add_generosity_points(giver_id, pts, "rain")
    payout = await generosity_try_payout(giver_id)
    if payout > 0:
        await message.reply(f"🎁 Бонус щедрости: +{fmt_money(payout)}")
    await message.reply("🌧 Прошёл дождь. Намокли: " + ", ".join(breakdown), parse_mode="HTML")

# ------------- игры (пока только кубик, остальные готовы к добавлению) -------------

async def _precheck_and_reserve_bet(message: types.Message, amount: int, game_tag: str, win_mult: int) -> bool:
    """Проверки + моментальное списание ставки. Возвращает True, если всё ок и ставка зарезервирована."""
    # казино доступность
    if not await get_casino_on():
        await message.reply("🎰 Казино закрыто.")
        return False

    if amount <= 0:
        await message.reply("Я не могу принять отрицательную ставку.")
        return False

    # лимит ставки
    max_bet = await get_limit_bet()
    if max_bet and amount > max_bet:
        await message.reply(f"Лимит ставки: не более {fmt_money(max_bet)}.")
        return False

    gambler_id = message.from_user.id
    balance = await get_balance(gambler_id)
    if amount > balance:
        await message.reply(f"🔍У Вас недостаточно нуаров. Баланс: {fmt_money(balance)}")
        return False

    # проверка сейфа на потенциальную выплату
    room = await _get_vault_room()
    if room == -1:
        await message.reply("Сейф ещё не включён.")
        return False

    potential = amount * win_mult
    if potential > room:
        await message.reply("Казино закрыто на переучёт — в сейфе недостаточно средств для такой выплаты.")
        return False

    # МОМЕНТАЛЬНО списываем ставку (резерв)
    await change_balance(gambler_id, -amount, f"ставка (резерв) {game_tag}", gambler_id)
    return True


async def handle_kubik(message: types.Message):
    m = re.match(r"^\s*ставлю\s+(\d+)\s+на\s+(?:🎲|кубик)\s*$", message.text.strip(), re.IGNORECASE)
    if not m:
        await message.reply("Пример: «ставлю 10 на 🎲|кубик»")
        return
    amount = int(m.group(1))
    user_id = message.from_user.id
    lock = get_bet_lock(user_id)

    if lock.locked():
        await message.reply("Подождите окончания предыдущей ставки.")
        return

    async with lock:
        mults = await get_multipliers()
        win_mult = mults["dice"]

        # проверки + моментальное списание
        ok = await _precheck_and_reserve_bet(message, amount, "(кубик)", win_mult)
        if not ok:
            return

        # отправляем анимацию — если упадёт, вернём ставку
        try:
            sent: types.Message = await message.answer_dice(emoji="🎲")
        except Exception:
            await change_balance(user_id, amount, "рефанд ставки (ошибка анимации кубик)", user_id)
            await message.reply("Не удалось бросить кубик. Ставка возвращена.")
            return

        roll_value = sent.dice.value  # 1..6
        await asyncio.sleep(3.5)

        if roll_value == 6:
            await change_balance(user_id, amount * win_mult, "ставка выигрыш (кубик)", user_id)
            await message.reply(
                f"🎉Фортуна на вашей стороне, {mention_html(user_id, message.from_user.full_name)}. "
                f"Вы получаете {fmt_money(amount * win_mult)}",
                parse_mode="HTML"
            )
        else:
            # Проигрыш: ставка уже списана ранее, ничего дополнительно не списываем
            await message.reply(
                f"🪦Ставки погубят вас, {mention_html(user_id, message.from_user.full_name)}. "
                f"Вы потеряли {fmt_money(amount)}.",
                parse_mode="HTML"
            )
            # NEW: «Крупье» — 15% шанс вернуть 50% ставки при проигрыше
            user_perks = await get_perks(user_id)
            p = await get_perk_croupier_chance()
            if "крупье" in user_perks and chance(p):
                refund = amount // 2
                if refund > 0:
                    await change_balance(user_id, refund, "крупье_рефанд(кубик)", user_id)
                    await message.reply(f"🎩 Крупье пожалел вас и вернул {fmt_money(refund)}.")


async def handle_darts(message: types.Message):
    m = re.match(r"^\s*ставлю\s+(\d+)\s+на\s+(?:🎯|дартс)\s*$", message.text.strip(), re.IGNORECASE)
    if not m:
        await message.reply("Пример: «ставлю 10 на 🎯|дартс»")
        return
    amount = int(m.group(1))
    user_id = message.from_user.id
    lock = get_bet_lock(user_id)

    if lock.locked():
        await message.reply("Подождите окончания предыдущей ставки.")
        return

    async with lock:
        mults = await get_multipliers()
        win_mult = mults["darts"]

        ok = await _precheck_and_reserve_bet(message, amount, "(дартс)", win_mult)
        if not ok:
            return

        try:
            sent: types.Message = await message.answer_dice(emoji="🎯")
        except Exception:
            await change_balance(user_id, amount, "рефанд ставки (ошибка анимации дартс)", user_id)
            await message.reply("Не удалось бросить дротик. Ставка возвращена.")
            return

        roll_value = sent.dice.value  # 1..6
        await asyncio.sleep(3.0)

        if roll_value == 6:  # буллсай
            await change_balance(user_id, amount * win_mult, "ставка выигрыш (дартс)", user_id)
            await message.reply(
                f"🎯 Метко! {mention_html(user_id, message.from_user.full_name)} получает {fmt_money(amount * win_mult)}",
                parse_mode="HTML"
            )
        else:
            await message.reply(
                f"🙈 Не попал. {mention_html(user_id, message.from_user.full_name)} теряет {fmt_money(amount)}.",
                parse_mode="HTML"
            )
            # NEW: «Крупье» — 15% шанс вернуть 50% ставки при проигрыше
            user_perks = await get_perks(user_id)
            p_croup = await get_perk_croupier_chance()
            if "крупье" in user_perks and chance(p_croup):
                refund = amount // 2
                if refund > 0:
                    await change_balance(user_id, refund, "крупье_рефанд(дартс)", user_id)
                    await message.reply(f"🎩 Крупье пожалел вас и вернул {fmt_money(refund)}.")


async def handle_bowling(message: types.Message):
    m = re.match(r"^\s*ставлю\s+(\d+)\s+на\s+(?:🎳|боулинг)\s*$", message.text.strip(), re.IGNORECASE)
    if not m:
        await message.reply("Пример: «ставлю 10 на 🎳|боулинг»")
        return
    amount = int(m.group(1))
    user_id = message.from_user.id
    lock = get_bet_lock(user_id)

    if lock.locked():
        await message.reply("Подождите окончания предыдущей ставки.")
        return

    async with lock:
        mults = await get_multipliers()
        win_mult = mults["bowling"]

        ok = await _precheck_and_reserve_bet(message, amount, "(боулинг)", win_mult)
        if not ok:
            return

        try:
            sent: types.Message = await message.answer_dice(emoji="🎳")
        except Exception:
            await change_balance(user_id, amount, "рефанд ставки (ошибка анимации боулинг)", user_id)
            await message.reply("Не удалось запустить боулинг. Ставка возвращена.")
            return

        roll_value = sent.dice.value  # 1..6
        await asyncio.sleep(3.0)

        if roll_value == 6:  # страйк
            await change_balance(user_id, amount * win_mult, "ставка выигрыш (боулинг)", user_id)
            await message.reply(
                f"🎳 Страйк! {mention_html(user_id, message.from_user.full_name)} получает {fmt_money(amount * win_mult)}",
                parse_mode="HTML"
            )
        else:
            await message.reply(
                f"💨 Мимо кеглей. {mention_html(user_id, message.from_user.full_name)} теряет {fmt_money(amount)}.",
                parse_mode="HTML"
            )
            # NEW: «Крупье» — 15% шанс вернуть 50% ставки при проигрыше
            user_perks = await get_perks(user_id)
            p_croup = await get_perk_croupier_chance()
            if "крупье" in user_perks and chance(p_croup):
                refund = amount // 2
                if refund > 0:
                    await change_balance(user_id, refund, "крупье_рефанд(боулинг)", user_id)
                    await message.reply(f"🎩 Крупье пожалел вас и вернул {fmt_money(refund)}.")



async def handle_slots(message: types.Message):
    m = re.match(r"^\s*ставлю\s+(\d+)\s+на\s+(?:🎰|автоматы|слоты)\s*$", message.text.strip(), re.IGNORECASE)
    if not m:
        await message.reply("Пример: «ставлю 10 на 🎰|автоматы»")
        return
    amount = int(m.group(1))
    user_id = message.from_user.id
    lock = get_bet_lock(user_id)

    if lock.locked():
        await message.reply("Подождите окончания предыдущей ставки.")
        return

    async with lock:
        mults = await get_multipliers()
        win_mult = mults["slots"]

        ok = await _precheck_and_reserve_bet(message, amount, "(автоматы)", win_mult)
        if not ok:
            return

        try:
            sent: types.Message = await message.answer_dice(emoji="🎰")
        except Exception:
            await change_balance(user_id, amount, "рефанд ставки (ошибка анимации автоматы)", user_id)
            await message.reply("Не удалось запустить слот-машину. Ставка возвращена.")
            return

        roll_value = sent.dice.value  # у Telegram 1..64
        await asyncio.sleep(3.2)

        if roll_value == 64:  # джекпот (три семёрки)
            await change_balance(user_id, amount * win_mult, "ставка выигрыш (автоматы)", user_id)
            await message.reply(
                f"🎰 Джекпот! {mention_html(user_id, message.from_user.full_name)} получает {fmt_money(amount * win_mult)}",
                parse_mode="HTML"
            )
        else:
            await message.reply(
                f"🍒 Не повезло. {mention_html(user_id, message.from_user.full_name)} теряет {fmt_money(amount)}.",
                parse_mode="HTML"
            )
            # NEW: «Крупье» — 15% шанс вернуть 50% ставки при проигрыше
            user_perks = await get_perks(user_id)
            p_croup = await get_perk_croupier_chance()
            if "крупье" in user_perks and chance(p_croup):
                refund = amount // 2
                if refund > 0:
                    await change_balance(user_id, refund, "крупье_рефанд(автоматы)", user_id)
                    await message.reply(f"🎩 Крупье пожалел вас и вернул {fmt_money(refund)}.")



# ------------- перки: мои/чужие, даровать/уничтожить, ЗП, вор -------------

async def handle_my_perks(message: types.Message):
    user_id = message.from_user.id
    perk_codes = await get_perks(user_id)

    # соберём ваучеры по всем известным кодам
    vouchers_active_lines = []
    vouchers_inactive_lines = []

    # формируем аккуратный вывод
    if not perk_codes:
        base_lines = ["У Вас пока нет перков."]
    else:
        base_lines = ["Ваши перки:"]
        items = []
        for code in perk_codes:
            emoji, title = PERK_REGISTRY.get(code, ("", code))
            # кредиты
            creds = await get_perk_credits(user_id, code)
            suffix = f" (ваучеры: {creds})" if creds > 0 else ""
            items.append((title.lower(), f"{emoji} {title}{suffix}"))
        for _, line in sorted(items):
            base_lines.append(line)

    # ваучеры, для которых перка нет
    for code in PERK_REGISTRY.keys():
        if code in perk_codes:
            continue
        creds = await get_perk_credits(user_id, code)
        if creds > 0:
            emoji, title = PERK_REGISTRY.get(code, ("", code))
            vouchers_inactive_lines.append(f"{emoji} {title} — {creds}")

    if vouchers_inactive_lines:
        base_lines.append("\nВаучеры (неактивные):")
        base_lines.extend(vouchers_inactive_lines)

    await message.reply("\n".join(base_lines))


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
    # снимаем активный перк
    await revoke_perk(target.id, code)

    # если есть ваучер — сразу активируем один (автоподмена)
    credits = await get_perk_credits(target.id, code)
    if credits > 0 and await perk_credit_use(target.id, code):
        await grant_perk(target.id, code)
        await message.reply(
            f"Перк «{title}» уничтожен у {mention_html(target.id, target.full_name)}, "
            f"но ваучер автоматически активирован (осталось ваучеров: {credits - 1}).",
            parse_mode="HTML"
        )
        return

    # если ваучеров нет — просто подтвердим уничтожение
    await message.reply(
        f"Перк «{title}» уничтожен у {mention_html(target.id, target.full_name)}.",
        parse_mode="HTML"
    )

async def handle_perk_holders_list(message: types.Message, code_raw: str):
    code = code_raw.strip().lower()

    # 1) проверяем, что такой перк вообще существует
    if code not in PERK_REGISTRY:
        available = ", ".join(sorted(PERK_REGISTRY.keys()))
        await message.reply(f"Такого перка нет. Доступные коды: {available}")
        return

    emoji, title = PERK_REGISTRY[code]

    # 2) собираем держателей
    holders = await get_perk_holders(code)
    if not holders:
        await message.reply(f"{emoji} Никто пока не обладает перком «{title}».")
        return

    # 3) красиво выводим список с кликабельными именами
    lines = [f"{emoji} Обладатели перка «{title}»:"]

    for uid in holders:
        name = "Участник"
        try:
            member = await message.bot.get_chat_member(message.chat.id, uid)
            name = member.user.full_name or name
        except Exception:
            pass
        lines.append(f"• {mention_html(uid, name)}")

    await message.reply("\n".join(lines), parse_mode="HTML")



async def handle_perk_registry(message: types.Message):
    summary = await get_perks_summary()  # список [(code, count)]
    if not summary:
        await message.reply("Пока никто не получал перков.")
        return

    lines = ["Сводка по перкам:"]
    for code, cnt in summary:
        emoji, title = PERK_REGISTRY.get(code, ("", code))
        nice = f"{emoji} {title}".strip()
        lines.append(f"• {nice} — {cnt}")

    await message.reply("\n".join(lines))


async def handle_stipend_claim(message: types.Message):
    user_id = message.from_user.id

    # кулдаун на жалование — используем те же функции, но с иным reason
    seconds = await get_seconds_since_last_salary_claim(user_id, "жалование")
    COOLDOWN = 12 * 60 * 60
    if seconds is not None and seconds < COOLDOWN:
        remain = COOLDOWN - seconds
        hours = remain // 3600
        minutes = (remain % 3600) // 60
        await message.reply(f"Жалование уже получено. Повторно — через {hours}ч {minutes}м.")
        return

    perks = await get_perks(user_id)

    # база и надбавка (как было)
    base = await get_stipend_base()
    bonus = await get_stipend_bonus() if "надбавка" in perks else 0

    # ПРЕМИЯ: 20% ×2; 50% ×1; 10% ×0.5; 20% ×0 — всегда от ТЕКУЩЕЙ «надбавки»
    premium_bonus = 0
    premium_note = None
    if "премия" in perks:
        roll = random.randint(1, 100)  # 1..100
        sb = await get_stipend_bonus()  # именно от надбавки
        if roll <= 20:
            premium_bonus = int(sb * 2.0)
            premium_note = "🏅 Премия ×2"
        elif roll <= 70:
            premium_bonus = int(sb * 1.0)
            premium_note = "🏅 Премия ×1"
        elif roll <= 80:
            premium_bonus = int(sb * 0.5)
            premium_note = "🏅 Премия ×0.5"
        else:
            premium_bonus = 0
            premium_note = "🏅 Премия ×0"

    total = base + bonus + premium_bonus

    # проверка сейфа до начисления
    room = await _get_vault_room()
    if room == -1:
        await message.reply("Сейф ещё не включён.")
        return
    if total > room:
        await message.reply("В сейфе недостаточно нуаров для жалования.")
        return

    # запись КД (reason='жалование') и начисления
    await record_salary_claim(user_id, total, "жалование")
    await change_balance(user_id, total, "жалование", user_id)

    # ответ
    lines = [f"💼 Выплачено жалование: {fmt_money(total)}."]
    lines.append(f"база: {fmt_money(base)}")
    if bonus > 0:
        lines.append(f"надбавка: {fmt_money(bonus)}")
    if "премия" in perks:
        lines.append(f"{premium_note}: {fmt_money(premium_bonus)}")
    await message.reply("\n".join(lines))


async def handle_theft(message: types.Message):
    thief_id = message.from_user.id
    perks = await get_perks(thief_id)
    p = await get_perk_shield_chance()
    if "кража" not in perks:
        await message.reply("У Вас нет такой привилегии.")
        return
    if not message.reply_to_message:
        await message.reply("Кража работает только ответом на сообщение жертвы.")
        return
    victim = message.reply_to_message.from_user
    if victim.is_bot:
        await message.reply("Красть у бота бессмысленно.")
        return

    # NEW: «Щит» у жертвы — 50% срыв кражи
    victim_perks = await get_perks(victim.id)
    if "щит" in victim_perks and chance(p):
        await record_theft(thief_id, 0, victim.id, success=False)
        await message.reply("🛡️ Щит жертвы вспыхнул — вы охуели от таких спецэфектов. Отсидитесь 12 часов.")
        return

    seconds = await get_seconds_since_last_theft(thief_id)
    COOLDOWN = 12 * 60 * 60
    if seconds is not None and seconds < COOLDOWN:
        remain = COOLDOWN - seconds
        hours = remain // 3600
        minutes = (remain % 3600) // 60
        await message.reply(f"Нужно схорониться. Повторная ходка через {hours}ч {minutes}м.")
        return
    income = await get_income()
    victim_balance = await get_balance(victim.id)
    if victim_balance < income or income <= 0:
        await record_theft(thief_id, 0, victim.id, success=False)
        await message.reply(f"🐕 Сторожевые собаки подняли лай — пришлось бежать. Схоронитесь на 12 часов.")
        return

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
    try:
        price_emerald = await get_price_emerald()
        burn_bps = await get_burn_bps()

        t24  = await get_market_turnover_days(1)
        t7   = await get_market_turnover_days(7)
        t30  = await get_market_turnover_days(30)

        # Индексы/шансы перков и связанные величины
        shield = await get_perk_shield_chance()
        croup  = await get_perk_croupier_chance()
        phil   = await get_perk_philanthrope_chance()
        lucky  = await get_perk_lucky_chance()
        bonus  = await get_stipend_bonus()   # надбавка к жалованию (сумма)
        theft  = await get_income()          # размер удачной кражи (сумма)

        def perk_display_name(code: str, mode: str = "cap") -> str:
            if mode == "caps":
                return code.upper()
            return code.capitalize()

        perks_header = "Команда покупки: купить перк (имя перка)"

        # ===== Перки =====
        perk_blocks = []
        for code, (emoji, title) in PERK_REGISTRY.items():
            price = await get_price_perk(code)
            price_str = f"{fmt_int(price)} 🪙" if price is not None else "не продаётся"
            name = perk_display_name(code, mode="caps")  # "CAPS" или "cap"

            if code == "надбавка":
                usage = f"автоматический бонус при использовании «получить жалование». текущая надбавка: +{fmt_money(bonus)}"
            elif code == "кража":
                usage = f"возможность украсть по команде «украсть» / «своровать» (reply). сумма удачной кражи: {fmt_money(theft)}"
            elif code == "щит":
                usage = f"шанс уклониться от кражи: {shield}%"
            elif code == "крупье":
                usage = f"шанс 50% рефанда при проигрыше в играх: {croup}%"
            elif code == "филантроп":
                usage = f"шанс что ваш дождь окатит еще одного: {phil}%"
            elif code == "везунчик":
                usage = f"шанс попасть под чужой дождь: {lucky}%"
            elif code == "премия":
                usage = "модель премии: 20%×2 | 50%×1 | 10%×0.5 | 20%×0"
            elif code == "грабитель":
                usage = "КАВАБАНГА!!!"
            else:
                usage = "—"

            perk_blocks.append(
                f"{emoji} <b>{name}</b>\n"
                f"<b>Цена:</b> {price_str}\n"
                f"<b>Описание:</b> {usage}"
            )


        # ===== Лоты участников =====
        offers = await list_active_offers()
        offer_blocks = []
        for o in offers:
            seller_id = o["seller_id"]
            price = o["price"]
            link = html.escape(o["link"] or "(ссылка не указана)")
            offer_id = o["offer_id"]

            # юзерка продавца (если нет username — выводим кликабельное имя)

            try:
                member = await message.bot.get_chat_member(message.chat.id, seller_id)
                seller_repr = mention_html(seller_id, member.user.full_name or "Участник")
            except Exception:
                seller_repr = mention_html(seller_id, "Участник")

            if o.get("type") == "perk":
                code = (o.get("perk_code") or "").strip().lower()
                emoji, title = PERK_REGISTRY.get(code, ("", code))
                offer_blocks.append(
                    f"<b>Товар:</b> Перк «{title}» {emoji}\n"
                    f"<b>Номер лота:</b> {offer_id}\n"
                    f"<b>Цена:</b> {fmt_money(price)}\n"
                    f"<b>Продавец:</b> {seller_repr}\n"
                    f"<b>Команда покупки:</b> купить лот {offer_id}"
                )
            else:
                link = html.escape(o.get("link") or "(ссылка не указана)")
                offer_blocks.append(
                    f"<b>Товар:</b> {link}\n"
                    f"<b>Номер лота:</b> {offer_id}\n"
                    f"<b>Цена:</b> {fmt_money(price)}\n"
                    f"<b>Продавец:</b> {seller_repr}\n"
                    f"<b>Команда покупки:</b> купить лот {offer_id}"
                )



        turnover_line = (
            f"📈 <b>Оборот</b>: 24ч — {fmt_money(t24)} • 7д — {fmt_money(t7)} • 30д — {fmt_money(t30)}"
        )
        burn_line = f"🔥 <b>Сжигание на рынке</b>: {fmt_percent_bps(burn_bps)}"

        parts = []
        parts.append("🛒 <b>РЫНОК</b>\n\n")
        parts.append(f"💎 <b>Эмеральд:</b> {fmt_money(price_emerald)}\n<b>Команда покупки:</b> купить эмеральд\n\n")
        parts.append("🎖 <b>ПЕРКИ</b>\n")
        parts.append(perks_header + "\n\n")
        parts.append("\n\n".join(perk_blocks) if perk_blocks else "Пока ничего нет.")
        parts.append("\n\n📦 <b>ЛОТЫ УЧАСТНИКОВ</b>\n")
        parts.append("\n\n".join(offer_blocks) if offer_blocks else "Пока нет активных лотов.")
        parts.append("\n\n" + turnover_line + "\n" + burn_line)

        txt = "".join(parts)

        try:
            # aiogram v3
            await message.reply(
                txt,
                parse_mode="HTML",
                link_preview_options=types.LinkPreviewOptions(is_disabled=True)
            )
        except TypeError:
            # aiogram v2
            await message.reply(
                txt,
                parse_mode="HTML",
                disable_web_page_preview=True
            )
    except Exception as e:
        await message.reply(f"⚠️ Ошибка рынка: {e!r}")   

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

    # если это перковый лот — нужно закрыть эскроу и вернуть право владельцу
    offers_all = await list_active_offers()
    offer = next((o for o in offers_all if o["offer_id"] == offer_id), None)
    if offer and offer.get("type") == "perk":
        code = (offer.get("perk_code") or "").strip().lower()
        # сперва закрываем эскроу
        await perk_escrow_close(offer["seller_id"], code, offer_id, "cancel")

        # если у продавца ТЕПЕРЬ уже есть этот перк (мог купить заново) — вернём ему ваучером,
        # иначе — вернём активным перком
        seller_perks = await get_perks(offer["seller_id"])
        if code in seller_perks:
            await perk_credit_add(offer["seller_id"], code)
        else:
            await grant_perk(offer["seller_id"], code)


    await cancel_offer(offer_id, message.from_user.id)
    await message.reply("Лот снят.")

async def handle_perk_sell(message: types.Message, code: str, price: int):
    if code not in PERK_REGISTRY:
        await message.reply("Такого перка нет.")
        return
    if price <= 0:
        await message.reply("Цена должна быть положительной.")
        return

    user_id = message.from_user.id
    code = code.strip().lower()

    # текущее состояние пользователя
    perks = await get_perks(user_id)
    has_perk = (code in perks)
    credits = await get_perk_credits(user_id, code)

    # --- Приоритет ИСТОЧНИКА: сначала ваучер, потом актив ---
    # 1) Если есть хоть один ваучер, продаём ваучер (активный перк не трогаем)
    if credits > 0:
        ok = await perk_credit_use(user_id, code)  # минус 1 кредит
        if not ok:
            # теоретически не должно случиться (мы только что считали), но подстрахуемся
            await message.reply("Нет доступного ваучера этого перка.")
            return
        offer_id = await create_perk_offer(user_id, code, price)
        await perk_escrow_open(user_id, code, offer_id)
        await message.reply(f"Лот (перк «{PERK_REGISTRY[code][1]}») выставлен. ID: {offer_id}.")
        return

    # 2) Ваучеров нет, но есть актив — отправляем АКТИВ в эскроу
    if has_perk:
        # снимаем актив и отдаём его в эскроу
        await revoke_perk(user_id, code)

        # ВАЖНО: не делаем авто-подмену актив↔ваучер.
        # Если ваучеров нет, актив реально уходит в эскроу.

        # страховка: убедимся, что перк снят
        after = await get_perks(user_id)
        if code in after:
            await message.reply("Не удалось передать перк в эскроу. Попробуйте ещё раз или сообщите куратору.")
            return

        offer_id = await create_perk_offer(user_id, code, price)
        await perk_escrow_open(user_id, code, offer_id)
        await message.reply(f"Лот (перк «{PERK_REGISTRY[code][1]}») выставлен. ID: {offer_id}.")
        return

    # сюда попадём, только если нет ни активного перка, ни ваучеров
    await message.reply("У вас нет этого перка и ваучеров тоже нет.")



async def _apply_burn_and_return(price: int) -> int:
    """Возвращает величину burn по текущему bps (округление вниз)."""
    bps = await get_burn_bps()
    return (price * bps) // 10000

async def handle_offer_buy(message: types.Message, offer_id: int):
    # найти лот
    perk_note = ""
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
        await message.reply(f"Недостаточно нуаров. Требуется {fmt_money(price)}, на руках {fmt_money(bal)}.")
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
    sale_id = await insert_history(buyer_id, "offer_sold", price, f"offer_id={offer_id};seller={offer['seller_id']}")

    # если перковый лот — перевыдать перк/кредит
    if offer.get("type") == "perk":
        code = (offer.get("perk_code") or "").strip().lower()
        if code in PERK_REGISTRY:
            buyer_perks = await get_perks(buyer_id)
            granted = False
            if code in buyer_perks:
                await perk_credit_add(buyer_id, code)
                perk_note = "Выдан ваучер (у вас уже есть этот перк)."
            else:
                await grant_perk(buyer_id, code)
                granted = True
                perk_note = "Выдан активный перк."
            # закрываем эскроу
            seller_id = offer["seller_id"]
            await perk_escrow_close(seller_id, code, offer_id, "sold")


    # контракт
    today = datetime.utcnow().strftime("%Y%m%d")
    contract_id = f"C-{today}-{sale_id}"

    # Сформируем строку «Товар» и примечание по перку (если перковый лот)
    # перед чеком
    product_line = f"Товар: «лот #{offer_id}» ({offer['link'] or 'ссылка не указана'})\n"
    if offer.get("type") == "perk":
        code = (offer.get("perk_code") or "").strip().lower()
        emoji, title = PERK_REGISTRY.get(code, ("", code))
        product_line = f"Товар: Перк «{title}» {emoji}\n"

    seller_mention = mention_html(offer["seller_id"], "Продавец")

    await message.reply(
        f"🧾 Контракт {contract_id}\n"
        f"Покупатель: {mention_html(buyer_id, message.from_user.full_name)}\n"
        f"{product_line}"
        f"Цена: {fmt_money(price)}\n"
        f"Комиссия (сжигание/налог): {fmt_money(burn)}\n"
        f"Перевод продавцу: {fmt_money(to_seller)}\n"
        f"{(perk_note + chr(10)) if perk_note else ''}"
        f"Гарант: @kovalskyii\n"
        f"Продавец: {seller_mention}",
        parse_mode="HTML"
    )


async def handle_buy_emerald(message: types.Message):
    price = await get_price_emerald()
    buyer_id = message.from_user.id
    bal = await get_balance(buyer_id)
    if price > bal:
        await message.reply(f"Недостаточно нуаров. Требуется {fmt_money(price)}, на руках {fmt_money(bal)}.")
        return
    burn = await _apply_burn_and_return(price)
    # списываем у покупателя (остаток как бы уходит в сейф, т.к. никому не начисляем)
    await change_balance(buyer_id, -price, "покупка эмеральда", buyer_id)
    if burn > 0:
        await record_burn(burn, "emerald")
    # контракт/чек
    sale_id = await insert_history(buyer_id, "emerald_buy", price, None)
    today = datetime.utcnow().strftime("%Y%m%d")
    contract_id = f"C-{today}-{sale_id}"
    await message.reply(
        f"🧾 Контракт {contract_id}\n"
        f"Покупатель: {mention_html(buyer_id, message.from_user.full_name)}\n"
        f"Товар: «Эмеральд»\n"
        f"Цена: {fmt_money(price)}\n"
        f"Комиссия (сжигание/налог): {fmt_money(burn)}\n"
        f"Перевод в сейф: {fmt_money(price - burn)}\n"
        f"Гарант: @kovalskyii",
        parse_mode="HTML"
    )

async def handle_buy_perk(message: types.Message, code: str):
    code = code.strip().lower()
    if code not in PERK_REGISTRY:
        await message.reply("Такого перка нет.")
        return
    buyer_id = message.from_user.id
    perks = await get_perks(buyer_id)
    if code in perks:
        await message.reply("У вас уже есть этот перк. Повторно купить нельзя.")
        return

    price = await get_price_perk(code)
    if price is None:
        await message.reply("Этот перк сейчас не продаётся.")
        return

    
    bal = await get_balance(buyer_id)
    if price > bal:
        await message.reply(f"Недостаточно нуаров. Требуется {fmt_money(price)}, на руках {fmt_money(bal)}.")
        return

    burn = await _apply_burn_and_return(price)
    # списываем у покупателя
    await change_balance(buyer_id, -price, f"покупка перка {code}", buyer_id)
    if burn > 0:
        await record_burn(burn, f"perk={code}")
    # выдаём перк
    await grant_perk(buyer_id, code)

    # чек
    sale_id = await insert_history(buyer_id, "perk_buy", price, code)
    today = datetime.utcnow().strftime("%Y%m%d")
    contract_id = f"C-{today}-{sale_id}"
    emoji, title = PERK_REGISTRY[code]
    await message.reply(
        f"🧾 Контракт {contract_id}\n"
        f"Покупатель: {mention_html(buyer_id, message.from_user.full_name)}\n"
        f"Товар: «{title}»\n"
        f"Цена: {fmt_money(price)}\n"
        f"Комиссия (сжигание/налог): {fmt_money(burn)}\n"
        f"Перевод в сейф: {fmt_money(price - burn)}\n"
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
    await message.reply(f"Сейф включён. Кап: {fmt_int(cap)}. В обороте: {fmt_int(circulating)}. Остальное заложено в сейф.")

async def get_circulating_safe() -> int:
    # обёртка на случай изоляции
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
    await message.reply(f"Сейф перезапущен. Кап: {fmt_int(cap)}. В обороте: {fmt_int(circulating)}. Остальное заложено в сейф.")


async def handle_vault_stats(message: types.Message):
    stats = await get_economy_stats()
    if not stats:
        await message.reply("Сейф ещё не включён.")
        return

    cap_s          = fmt_int(stats["cap"])
    circulating_s  = fmt_int(stats["circulating"])
    burned_s       = fmt_int(stats["burned"])
    vault_s        = fmt_int(stats["vault"])
    supply_s       = fmt_int(stats.get("supply", stats["cap"] - stats["burned"]))
    bps_pct        = fmt_percent_bps(stats["burn_bps"])
    burned_pct     = (stats["burned"] / stats["cap"] * 100) if stats["cap"] > 0 else 0.0
    base  = await get_stipend_base()
    bonus = await get_stipend_bonus()
    theft  = await get_income()


    txt = (
        "🏦 <b>ЭКОНОМИКА КЛУБА</b>\n\n"
        f"🧱 <b>КАП:</b> {cap_s}\n"
        f"🪙 <b>Текущий саплай:</b> {supply_s}\n" 
        f"🔐 <b>В сейфе:</b> {vault_s}\n"
        f"🔄 <b>На руках:</b> {circulating_s}\n"
        f"🔥 <b>Сожжено:</b> {burned_s} ({burned_pct:.2f}%)\n"
        
        f"· · · · · · · · · · · · · · · · · · · · · · · · · · · · · · · ·\n"
        f"<b>ИНДЕКСЫ и КОЭФФИЦИЕНТЫ</b>\n\n"
        f"🧯 <b>Сжигание (налоги):</b> {bps_pct}\n"
        f"💼 <b>Жалование:</b> {fmt_money(base)}\n"

    )
    await message.reply(txt, parse_mode="HTML")



# --------- конфиги сеттеры ---------

async def handle_burn_bps_set(message: types.Message, v: int):
    await set_burn_bps(v)
    cur = await get_burn_bps()
    await message.reply(f"🛠️ Готово. Сжигание установлено на {fmt_percent_bps(cur)}.")

async def handle_price_emerald_set(message: types.Message, v: int):
    await set_price_emerald(v)
    await message.reply(f"🛠️ Готово. Цена Эмеральда: {fmt_money(v)}.")

async def handle_price_perk_set(message: types.Message, code: str, v: int):
    code = code.strip().lower()
    if code not in PERK_REGISTRY:
        await message.reply("Такого перка нет.")
        return
    await set_price_perk(code, v)
    await message.reply(f"🛠️ Готово. Цена перка «{PERK_REGISTRY[code][1]}»: {fmt_money(v)}.")

async def handle_multiplier_set(message: types.Message, game: str, x: int):
    await set_multiplier(game, x)
    await message.reply(f"🛠️ Готово. Множитель для «{game}»: ×{x}.")

async def handle_casino_toggle(message: types.Message):
    turn_on = message.text.strip().endswith("открыть")
    await set_casino_on(turn_on)
    await message.reply("🎰 Казино открыто." if turn_on else "🎰 Казино закрыто.")

async def handle_income_set(message: types.Message, v: int):
    await set_income(v)
    await message.reply(f"🛠️ Готово. Сумма удачной кражи: {fmt_money(v)}.")

async def handle_limit_bet_set(message: types.Message, v: int):
    await set_limit_bet(v)
    await message.reply("🛠️ Лимит ставки отключён." if v == 0 else f"🛠️ Лимит ставки: {fmt_int(v)}.")

async def handle_limit_rain_set(message: types.Message, v: int):
    await set_limit_rain(v)
    await message.reply("🛠️ Лимит дождя отключён." if v == 0 else f"🛠️ Лимит дождя: {fmt_money(v)}.")


# --------- «карман» куратора ---------

async def handle_kurator_karman(message: types.Message):
    if not message.reply_to_message:
        await message.reply("Этикет Клуба требует ответа на сообщение участника.")
        return
    target = message.reply_to_message.from_user
    balance = await get_balance(target.id)
    await message.reply(
        f"💼 {mention_html(target.id, target.full_name)} хранит в своём кармане {fmt_money(balance)}.",
        parse_mode="HTML"
    )

# --------- динамический «список команд» ---------

# commands.py
def bullets(items: list[str]) -> str:
    # Превратим <...> в «...» и добавим маркер
    safe_lines = []
    for s in items:
        s = s.replace("<", "«").replace(">", "»")
        safe_lines.append(f"• {s}")
    return "\n".join(safe_lines)

async def handle_commands_catalog(message: types.Message):

    price_pin = await get_price_pin()
    price_pin_loud = await get_price_pin_loud()

    keyholders = [
        "вручить <N> (reply) — выдать из сейфа",
        "взыскать <N> (reply) — забрать в сейф",
        "карман (reply) — посмотреть баланс участника",
    ]
    members = [
        "мой карман - просмотр своего баланса",
        "моя роль - просмотр своей роли",
        "роль(reply) - просмотр роли другого участника Клуба",
        "рейтинг клуба - список богатейших членов Клуба",
        "члены клуба",
        "хранители ключа / владельцы ключа",
        "передать <N>(reply) — перевод участнику",
        "дождь <N> — раздать до 5 случайным",
        "ставлю <N> на 🎲/кубик | 🎯/дартс | 🎳/боулинг | 🎰/автоматы — ставка в игру",
        "рынок — витрина товаров и лотов",
        "купить эмеральд / купить перк <код> / купить лот <offer_id>",
        "выставить <ссылка> <цена> / снять лот <offer_id>",
        "мои перки - просмотр своих перков",
        "перки(reply) - просмотр перков другого участника Клуба",
        "продать перк <код> <цена> - выставление перка на продажу",
        "получить жалование — базовая выплата раз в 12 часов",
        "украсть/своровать(reply) — кража по перку «кража», раз в 12 часов",
        "сейф — сводка экономики клуба",
        "концерт - раз в 12 часов выбирает Героя Дня",
        "выступить - команда Героя Дня, разовый гонорар",
        "браво(reply) - похвалить выступление",
        "депозит <N> — пополнить свою ячейку",
        "вывод <N> — вывести из ячейки в карман",
        "вывод все/всё / вывести все/всё - вывести весь баланс",
        "моя ячейка / ячейка — показать баланс своей ячейки",
        "банк — сводка по сумме всех ячеек и ставкам комиссий",
        "сжечь <N> — уничтожить нуары из своего кармана",

    ]
    paid = [
    f"закрепить пост (reply) — закрепить выбранное сообщение: {fmt_money(price_pin)}",
    f"закрепить пост громко (reply) — закрепить с уведомлением для всех: {fmt_money(price_pin_loud)}",
    ]

    txt = (
        "📜 <b>СПИСОК КОМАНД</b>\n\n"
        "🗝 <b>Владельцы ключа</b>\n" + bullets(keyholders) + "\n\n"
        "🎭 <b>Члены клуба</b>\n" + bullets(members) + "\n\n"
        "💳 <b>Платные команды</b>\n" + bullets(paid)
    )
    await message.reply(txt, parse_mode="HTML")

async def handle_commands_curator(message: types.Message):
    if message.from_user.id != KURATOR_ID:
        await message.reply("Эта команда доступна только Куратору.")
        return

    blocks = [
        ("🏦 Сейф/Банк/экономика", [
            "включить сейф <CAP> / перезапустить сейф <CAP> подтверждаю",
            "сейф — сводка экономики",
            "сжигание <bps> — 100 bps = 1%",
            "банк комиссия депозит <P> - процент комиссии за вклад в ячейку",
            "банк комиссия хранение <P> - процент комиссии за хранение в ячейке",
            "индекс <N> - установить базовый индекс экономики",

        ]),
        ("🎰 Казино", [
            "казино открыть|закрыть",
            "множитель кубик|дартс|боулинг|автоматы <X>",
            "лимит ставка <N>",
        ]),
        ("💎 Рынок и цены", [
            "цена эмеральд <N>",
            "цена перк <код> <N>",
        ]),
        ("🎖 Перки", [
            "у кого перк <код>|держатели перка / перки реестр",
            "даровать <код> (reply) / уничтожить <код> (reply)",
            "щит шанс <P> — установить шанс увернуться от кражи",
            "крупье шанс <P> — шанс частичного возврата ставки при проигрыше»",
            "филантроп шанс <P> — шанс подарка шестому при дожде",
            "везунчик шанс <P> — шанс автопопадания в дождь",
            "грабитель кд <дней> - кд перка грабитель",
        ]),
        ("🎭 Роли и ключи", [
            "назначить \"Роль\" описание (reply) / снять роль (reply)",
            "ключ от сейфа (reply) / снять ключ (reply)",
        ]),
        ("🧹 Сбросы/служебные", [
            "обнулить баланс (reply) / обнулить балансы / обнулить клуб",
        ]),
        ("🎁 Щедрость", [
            "щедрость множитель <p>% / щедрость награда <N>",
            "щедрость статус / щедрость очки / щедрость обнулить (reply)",
            "щедрость обнулить все подтверждаю",
        ]),
        ("🧩 Код-слово", [
            "установить код <слово> <сумма> <подсказка>",
            "отменить код",
        ]),
    ]

    parts = ["📜 <b>КОМАНДЫ КУРАТОРА</b>"]
    for title, items in blocks:
        parts.append(f"\n{title}\n" + bullets(items))

    await message.reply("\n".join(parts), parse_mode="HTML", disable_web_page_preview=True)


# --------- ГЕРОЙ ДНЯ ---------

async def handle_hero_of_day(message: types.Message):
    chat_id = message.chat.id

    current, until = await hero_get_current_with_until(chat_id)
    if current is not None:
        try:
            member = await message.bot.get_chat_member(chat_id, current)
            name = member.user.full_name or "Участник"
        except Exception:
            name = "Участник"

        # красивое КД
        from datetime import timezone
        now = datetime.now(timezone.utc)
        remain = until - now if until else None
        cd_line = ""
        if remain and remain.total_seconds() > 0:
            total = int(remain.total_seconds())
            h = total // 3600
            m = (total % 3600) // 60
            cd_line = f"\nОставшееся время: <b>{h}ч {m}м</b>."

        await message.reply(
            f"🎤 Сегодня выступает — {mention_html(current, name)}.\n"
            f"Команда для {HERO_TITLE.lower()}: «выступить».{cd_line}",
            parse_mode="HTML"
        )
        return


    # выбираем случайного участника (не бота, в чате, из известных)
    candidates = []
    for uid in await get_known_users():
        try:
            member = await message.bot.get_chat_member(chat_id, uid)
            if getattr(member.user, "is_bot", False):
                continue
            if member.status in ("left", "kicked"):
                continue
            candidates.append(uid)
        except Exception:
            continue

    if not candidates:
        await message.reply("Пока не вижу участников на роль исполнителя.")
        return

    hero_id = random.choice(candidates)
    await hero_set_for_today(chat_id, hero_id, hours=12)

    # тексты анонса (без пингов)
    try:
        member = await message.bot.get_chat_member(chat_id, hero_id)
        hero_name = member.user.full_name or "Участник"
    except Exception:
        hero_name = "Участник"

    await message.reply(
        "🎪 Мы готовим большой концерт. Но нам нужен исполнитель.\n"
        "Прошлый улетел в Дубай на скачки блох на кузнечиках…\n"
        f"Кажется, {mention_html(hero_id, hero_name)} нам подойдёт!\n\n"
        f"🏷 Титул на сегодня: <b>{HERO_TITLE}</b>\n"
        "Команда для выступления: «выступить».",
        parse_mode="HTML"
    )

async def handle_hero_concert(message: types.Message):
    chat_id = message.chat.id
    user_id = message.from_user.id

    current = await hero_get_current(chat_id)
    if current is None:
        await message.reply("Сегодня исполнитель ещё не выбран. Команда: «концерт».")
        return
    if current != user_id:
        await message.reply("Вы не являетесь сегодняшним исполнителем.")
        return

    if await hero_has_claimed_today(chat_id, user_id, hours=12):
        await message.reply("Ваш гонорар уже получен. На следующем концерте выступит кто-то еще.")
        return

    reward = random.randint(HERO_CONCERT_MIN, HERO_CONCERT_MAX)
    await hero_record_claim(chat_id, user_id, reward)
    await change_balance(user_id, reward, "выступить", user_id)

    sent = await message.reply(
        "🎤 Это было грандиозно! Концерт почти затмил Битлз.\n"
        f"Зрители в переходе ликовали и накидали вам {fmt_money(reward)} в шапку.",
    )
    ts_unix = int(datetime.now(timezone.utc).timestamp())
    await hero_save_claim_msg(message.chat.id, user_id, sent.message_id, ts_unix)

async def handle_bravo(message: types.Message):
    chat_id = message.chat.id
    hero_msg = await hero_get_last_claim_msg(chat_id)
    if not hero_msg:
        await message.reply("Сегодня никто не выступал.")
        return

    msg_id = hero_msg["msg_id"]
    ts    = hero_msg["ts"]
    from time import time
    window = await get_bravo_window_sec()
    if int(time()) - int(ts) > window:
        await message.reply("Уже всё разошлись, кому вы кричите, ненормальный?")
        return

    # только реплаем на пост выступления
    if not message.reply_to_message or message.reply_to_message.message_id != msg_id:
        await message.reply("Нужно ответить на сообщение о выступлении.")
        return

    # самопохвала
    if message.reply_to_message.from_user and message.reply_to_message.from_user.id == message.from_user.id:
        await message.reply("Сам себя не похвалишь — никто не похвалит.")
        return

    # лимит мест
    claimed = await bravo_count_for_msg(chat_id, msg_id)
    max_v = await get_bravo_max_viewers()
    if claimed >= max_v:
        # после 10-го: рубим остальным
        await message.reply("Ну всё-всё, иди работай.")
        return

    # один раз на человека
    if await bravo_already_claimed(message.from_user.id, chat_id, msg_id):
        await message.reply("Не сотрите ладони в кровь, милейший.")
        return

    # награда = жалованию (база)
    reward = await get_stipend_base()
    await record_bravo(message.from_user.id, chat_id, msg_id, reward)
    await change_balance(message.from_user.id, reward, "браво", message.from_user.id)
    await message.reply(f"Вам тоже понравилось? Было великолепно! Держите {fmt_money(reward)} за поддержку юного таланта")


async def _pin_paid(message: types.Message, loud: bool):
    if not message.reply_to_message:
        await message.reply("Нужно ответить на сообщение, которое хотите закрепить.")
        return
    price = await get_price_pin_loud() if loud else await get_price_pin()

    user_id = message.from_user.id
    bal = await get_balance(user_id)
    if price > bal:
        await message.reply(f"Не хватает нуаров. Цена: {fmt_money(price)}. На руках: {fmt_money(bal)}.")
        return

    # списываем (идёт в сейф; никому не начисляем)
    await change_balance(user_id, -price, "util_pin" + ("_loud" if loud else ""), user_id)

    # пин
    try:
        await message.bot.pin_chat_message(
            chat_id=message.chat.id,
            message_id=message.reply_to_message.message_id,
            disable_notification=not loud  # тихий = True, громкий = False
        )
        await message.reply(f"Сообщение закреплено. С вас снято: {fmt_money(price)}")
    except Exception as e:
        await message.reply(f"Не удалось закрепить: {e}")

async def _generosity_reset_points_for(user_id: int) -> int:
    pts = await get_generosity_points(user_id)
    if pts > 0:
        # спишем очки «в ноль» единым движением
        await insert_history(user_id, "generosity_pay_points", pts, "reset")
    return pts

async def handle_cell_deposit_cmd(message: types.Message, amount: int):
    if amount <= 0:
        await message.reply("Сумма должна быть положительной.")
        return
    bal = await get_balance(message.from_user.id)
    if amount > bal:
        await message.reply(f"Недостаточно нуаров. На руках: {fmt_money(bal)}.")
        return
    # списываем с кармана
    await change_balance(message.from_user.id, -amount, "cell_deposit", message.from_user.id)
    gross, fee, new_cell = await cell_deposit(message.from_user.id, amount)
    await message.reply(
        "✅ Депозит выполнен\n"
        f"Внесено: {fmt_money(gross)}\n"
        f"Комиссия: {fmt_money(fee)}\n"
        f"Зачислено: {fmt_money(gross - fee)}\n"
        f"Баланс ячейки: {fmt_money(new_cell)}"
    )

async def handle_cell_withdraw_cmd(message: types.Message, amount: int):
    if amount <= 0:
        await message.reply("Сумма должна быть положительной.")
        return
    taken, new_cell = await cell_withdraw(message.from_user.id, amount)
    if taken <= 0:
        await message.reply("В ячейке недостаточно средств.")
        return
    # возвращаем на карман
    await change_balance(message.from_user.id, taken, "cell_withdraw_payout", message.from_user.id)
    await message.reply(
        "✅ Вывод выполнен\n"
        f"Выведено: {fmt_money(taken)}\n"
        f"Баланс ячейки: {fmt_money(new_cell)}"
    )

async def handle_cell_withdraw_all_cmd(message: types.Message):
    # узнаём текущий баланс ячейки и выводим всё
    bal = await cell_get_balance(message.from_user.id)
    if bal <= 0:
        await message.reply("В ячейке пусто.")
        return
    taken, new_cell = await cell_withdraw(message.from_user.id, bal)
    if taken > 0:
        await change_balance(message.from_user.id, taken, "cell_withdraw_all_payout", message.from_user.id)
    await message.reply(
        "✅ Вывод всего баланса\n"
        f"Выведено: {fmt_money(taken)}\n"
        f"Баланс ячейки: {fmt_money(new_cell)}"
    )


async def handle_cell_balance_cmd(message: types.Message):
    bal = await cell_get_balance(message.from_user.id)
    await message.reply("🔒 Ячейка\n" f"Баланс: {fmt_money(bal)}")

async def handle_bank_summary_cmd(message: types.Message):
    total = await bank_touch_all_and_total()
    dep = await get_cell_dep_fee_pct()
    stor = await get_cell_stor_fee_pct()
    await message.reply(
        "🏛 Банк\n"
        f"Общий баланс ячеек: {fmt_money(total)}\n"
        f"Комиссия пополнения: {dep}%\n"
        f"Комиссия хранения: {stor}% / 6ч"
    )

async def handle_bank_rob_cmd(message: types.Message):
    user_id = message.from_user.id
    perks = await get_perks(user_id)
    if "грабитель" not in perks:
        await message.reply("У Вас нет такой привилегии.")
        return

    # КД из конфига (в днях)
    seconds = await get_seconds_since_last_bank_rob(user_id)
    cd_days = await get_bank_rob_cooldown_days()
    COOLDOWN = cd_days * 24 * 60 * 60
    if seconds is not None and seconds < COOLDOWN:
        remain = COOLDOWN - seconds
        days  = remain // (24*3600)
        hours = (remain % (24*3600)) // 3600
        minutes = (remain % 3600) // 60
        await message.reply(f"Подготовка нового налёта возьмет еще {days}д {hours}ч {minutes}м.")
        return

    roll = random.randint(1, 100)
    if roll <= 50:
        # успех
        _ = await bank_touch_all_and_total()
        loot = await bank_zero_all_and_sum()
        await record_bank_rob(user_id, "success", loot)
        if loot > 0:
            await change_balance(user_id, loot, "bank_rob_success", user_id)
        await message.reply(
            f"🎭 В твоей команде явно был сам Джокер! Вы вынесли всё подчистую. "
            f"Я насчитал {fmt_money(loot)} нуаров!"
        )
        try:
            await message.bot.send_message(
                message.chat.id,
                f"🚨 Банк был ограблен. Ячейки пусты. Персонал напуган. Ущерб оценивается в {fmt_money(loot)}."
            )
        except Exception:
            pass
        return

    if roll <= 95:
        # промах
        await record_bank_rob(user_id, "fail", 0)
        await message.reply("🚓 Кажется они вызвали копов! Валим!")
        try:
            await message.bot.send_message(message.chat.id, "🛡️ Охрана банка отбила нападение грабителей.")
        except Exception:
            pass
        return

    # провал с потерей перка
    await record_bank_rob(user_id, "busted", 0)
    await revoke_perk(user_id, "грабитель")
    await message.reply("🧿 Полиция уже была на месте. Вас ждали. Вы арестованы. Оружие изъято.")
    try:
        await message.bot.send_message(
            message.chat.id,
            "🕵️ Засада ФБР была удачной. Перк «Грабитель банка» изъят."
        )
    except Exception:
        pass

async def handle_burn_cmd(message: types.Message, amount: int):
    if amount <= 0:
        await message.reply("Сумма должна быть положительной.")
        return
    user_id = message.from_user.id
    bal = await get_balance(user_id)
    if amount > bal:
        await message.reply(f"Недостаточно нуаров для сжигания. На руках: {fmt_money(bal)}.")
        return

    # списываем с кармана
    await change_balance(user_id, -amount, "burn_self", user_id)
    # фиксируем сжигание (учтётся в экономике)
    await record_burn(amount, f"user_burn:{user_id}")

    await message.reply(f"🔥 Ты сжег {fmt_money(amount)}. Было тепло, но теперь они утеряны навсегда.")
