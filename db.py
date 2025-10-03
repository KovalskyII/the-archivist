import aiosqlite
from typing import Optional, Dict, Any, List, Tuple

DB_PATH = "/data/bot_data.sqlite"
SCHEMA_VERSION = 1  # схему не меняем

CREATE_USERS = """
CREATE TABLE IF NOT EXISTS users (
    user_id   INTEGER PRIMARY KEY,
    username  TEXT,
    balance   INTEGER NOT NULL DEFAULT 0,
    key       INTEGER NOT NULL DEFAULT 0
);
"""

CREATE_ROLES = """
CREATE TABLE IF NOT EXISTS roles (
    user_id    INTEGER PRIMARY KEY,
    role_name  TEXT,
    role_desc  TEXT,
    role_image TEXT
);
"""

CREATE_HISTORY = """
CREATE TABLE IF NOT EXISTS history (
    id      INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER,
    action  TEXT,
    amount  INTEGER,
    reason  TEXT,
    date    TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
"""

EXPECTED_USERS_COLS  = ["user_id", "username", "balance", "key"]
EXPECTED_ROLES_COLS  = ["user_id", "role_name", "role_desc", "role_image"]
EXPECTED_HIST_COLS   = ["id", "user_id", "action", "amount", "reason", "date"]

# ------- базовая инициализация/проверка -------

async def _table_columns(db, table: str):
    async with db.execute(f"PRAGMA table_info({table})") as cur:
        rows = await cur.fetchall()
    return [r[1] for r in rows]

async def _schema_ok(db) -> bool:
    for table, expected in (
        ("users", EXPECTED_USERS_COLS),
        ("roles", EXPECTED_ROLES_COLS),
        ("history", EXPECTED_HIST_COLS),
    ):
        async with db.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (table,)) as cur:
            row = await cur.fetchone()
        if not row:
            return False
        cols = await _table_columns(db, table)
        if cols != expected:
            return False
    return True

async def _recreate_all(db):
    await db.execute("DROP TABLE IF EXISTS history")
    await db.execute("DROP TABLE IF EXISTS roles")
    await db.execute("DROP TABLE IF EXISTS users")
    await db.execute(CREATE_USERS)
    await db.execute(CREATE_ROLES)
    await db.execute(CREATE_HISTORY)
    await db.execute(f"PRAGMA user_version = {SCHEMA_VERSION}")
    await db.commit()

async def init_db():
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("PRAGMA foreign_keys = ON")
        async with db.execute("PRAGMA user_version") as cur:
            row = await cur.fetchone()
        current_ver = row[0] if row else 0

        await db.execute(CREATE_USERS)
        await db.execute(CREATE_ROLES)
        await db.execute(CREATE_HISTORY)
        await db.commit()

        if current_ver != SCHEMA_VERSION or not await _schema_ok(db):
            await _recreate_all(db)

# ------- утилиты -------

async def insert_history(user_id: Optional[int], action: str, amount: Optional[int], reason: Optional[str]) -> int:
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "INSERT INTO history (user_id, action, amount, reason) VALUES (?, ?, ?, ?)",
            (user_id, action, amount, reason),
        )
        await db.commit()
        async with db.execute("SELECT last_insert_rowid()") as cur:
            rid = await cur.fetchone()
            return int(rid[0])

# ------- баланс -------

async def get_balance(user_id: int) -> int:
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT balance FROM users WHERE user_id = ?", (user_id,)) as cur:
            row = await cur.fetchone()
            return row[0] if row else 0

async def ensure_user(db, user_id: int):
    async with db.execute("SELECT 1 FROM users WHERE user_id=?", (user_id,)) as cur:
        if await cur.fetchone() is None:
            await db.execute("INSERT INTO users (user_id, username, balance, key) VALUES (?, NULL, 0, 0)", (user_id,))

async def change_balance(user_id: int, amount: int, reason: str, author_id: int):
    async with aiosqlite.connect(DB_PATH) as db:
        await ensure_user(db, user_id)
        async with db.execute("SELECT balance FROM users WHERE user_id = ?", (user_id,)) as cur:
            row = await cur.fetchone()
            current_balance = row[0]
        new_balance = current_balance + amount
        if new_balance < 0:
            new_balance = 0
        await db.execute("UPDATE users SET balance = ? WHERE user_id = ?", (new_balance, user_id))
        await db.execute(
            "INSERT INTO history (user_id, action, amount, reason) VALUES (?, 'change_balance', ?, ?)",
            (user_id, amount, reason),
        )
        await db.commit()

async def reset_user_balance(user_id: int):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("UPDATE users SET balance = 0 WHERE user_id = ?", (user_id,))
        await db.execute("INSERT INTO history (user_id, action, amount, reason) VALUES (?, 'reset_balance', 0, NULL)", (user_id,))
        await db.commit()

async def reset_all_balances():
    # Сбрасываем всем, пишем сводную запись в history (user_id=NULL)
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("UPDATE users SET balance = 0")
        await db.execute("INSERT INTO history (user_id, action, amount, reason) VALUES (NULL, 'reset_all_balances', NULL, NULL)")
        await db.commit()

# ------- роли -------

async def set_role(user_id: int, role_name: str | None, role_desc: str | None):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
            INSERT INTO roles (user_id, role_name, role_desc, role_image)
            VALUES (?, ?, ?, COALESCE((SELECT role_image FROM roles WHERE user_id=?), NULL))
            ON CONFLICT(user_id) DO UPDATE SET role_name=excluded.role_name, role_desc=excluded.role_desc
        """, (user_id, role_name, role_desc, user_id))
        await db.commit()

async def get_role(user_id: int):
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT role_name, role_desc FROM roles WHERE user_id = ?", (user_id,)) as cur:
            row = await cur.fetchone()
            if row:
                return {"role": row[0], "description": row[1]}
            return None

async def set_role_image(user_id: int, image_file_id: str):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
            INSERT INTO roles (user_id, role_name, role_desc, role_image)
            VALUES (?, NULL, NULL, ?)
            ON CONFLICT(user_id) DO UPDATE SET role_image = excluded.role_image
        """, (user_id, image_file_id))
        await db.commit()

async def get_role_with_image(user_id: int):
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT role_name, role_desc, role_image FROM roles WHERE user_id = ?", (user_id,)) as cur:
            row = await cur.fetchone()
            return row

# ------- ключи -------

async def grant_key(user_id: int):
    async with aiosqlite.connect(DB_PATH) as db:
        await ensure_user(db, user_id)
        await db.execute("UPDATE users SET key = 1 WHERE user_id = ?", (user_id,))
        await db.execute("INSERT INTO history (user_id, action, amount, reason) VALUES (?, 'grant_key', NULL, NULL)", (user_id,))
        await db.commit()

async def revoke_key(user_id: int):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("UPDATE users SET key = 0 WHERE user_id = ?", (user_id,))
        await db.execute("INSERT INTO history (user_id, action, amount, reason) VALUES (?, 'revoke_key', NULL, NULL)", (user_id,))
        await db.commit()

async def has_key(user_id: int) -> bool:
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT key FROM users WHERE user_id = ?", (user_id,)) as cur:
            row = await cur.fetchone()
            return bool(row and row[0] == 1)

# ------- реестры/списки -------

async def get_last_history(limit: int = 5):
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("""
            SELECT user_id, action, amount, reason, date
            FROM history ORDER BY id DESC LIMIT ?
        """, (limit,)) as cur:
            return await cur.fetchall()

async def get_top_users(limit: int = 10):
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("""
            SELECT user_id, balance FROM users
            WHERE balance > 0
            ORDER BY balance DESC
            LIMIT ?
        """, (limit,)) as cur:
            return await cur.fetchall()

async def get_all_roles():
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("""
            SELECT user_id, role_name FROM roles
            WHERE role_name IS NOT NULL AND TRIM(role_name) != ''
        """) as cur:
            return await cur.fetchall()

async def get_key_holders():
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("""
            SELECT user_id FROM users
            WHERE key = 1 ORDER BY user_id ASC
        """) as cur:
            rows = await cur.fetchall()
            return [r[0] for r in rows]

async def get_known_users() -> list[int]:
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT user_id FROM users") as cur:
            rows = await cur.fetchall()
            return [r[0] for r in rows]

# ------- перки через history -------

async def grant_perk(user_id: int, perk_code: str):
    return await insert_history(user_id, "perk_grant", None, perk_code)

async def revoke_perk(user_id: int, perk_code: str):
    return await insert_history(user_id, "perk_revoke", None, perk_code)

async def get_perks(user_id: int) -> set[str]:
    perks = set()
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("""
            SELECT action, reason FROM history
            WHERE user_id = ? AND action IN ('perk_grant','perk_revoke')
            ORDER BY id ASC
        """, (user_id,)) as cur:
            rows = await cur.fetchall()
    for action, code in rows:
        if not code:
            continue
        code = code.strip().lower()
        if action == "perk_grant":
            perks.add(code)
        else:
            perks.discard(code)
    return perks

async def get_perk_holders(perk_code: str) -> List[int]:
    # восстановим по истории актуальный набор
    state = {}
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("""
            SELECT user_id, action FROM history
            WHERE action IN ('perk_grant','perk_revoke') AND reason = ?
            ORDER BY id ASC
        """, (perk_code,)) as cur:
            rows = await cur.fetchall()
    for uid, action in rows:
        if uid is None:
            continue
        state[uid] = (action == "perk_grant")
    return [uid for uid, has in state.items() if has]

async def get_perks_summary() -> List[Tuple[str, int]]:
    # по всем кодам, которые встречались
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("""
            SELECT DISTINCT reason FROM history
            WHERE action IN ('perk_grant','perk_revoke') AND reason IS NOT NULL
        """) as cur:
            codes = [r[0] for r in await cur.fetchall()]
    out = []
    for code in sorted(set([c.strip().lower() for c in codes if c])):
        holders = await get_perk_holders(code)
        out.append((code, len(holders)))
    return out

# ------- ЗП/кража кулдауны -------

async def get_seconds_since_last_salary_claim(user_id: int, perk_code: str = "зп") -> int | None:
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            """
            SELECT CAST(strftime('%s','now') AS INTEGER) - CAST(strftime('%s', date) AS INTEGER)
            FROM history
            WHERE user_id = ? AND action = 'salary_claim' AND reason = ?
            ORDER BY id DESC LIMIT 1
            """,
            (user_id, perk_code),
        ) as cur:
            row = await cur.fetchone()
            if row is None:
                return None
            return row[0]

async def record_salary_claim(user_id: int, amount: int, perk_code: str = "зп"):
    await insert_history(user_id, "salary_claim", amount, perk_code)

async def get_seconds_since_last_theft(user_id: int) -> int | None:
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            """
            SELECT CAST(strftime('%s','now') AS INTEGER) - CAST(strftime('%s', date) AS INTEGER)
            FROM history
            WHERE user_id = ? AND action = 'theft'
            ORDER BY id DESC LIMIT 1
            """,
            (user_id,),
        ) as cur:
            row = await cur.fetchone()
            if row is None:
                return None
            return row[0]

async def record_theft(user_id: int, amount: int, victim_id: int, success: bool):
    reason = f"victim={victim_id};success={'1' if success else '0'}"
    await insert_history(user_id, "theft", amount if success else 0, reason)

# ------- анти-дубль -------

async def is_msg_processed(chat_id: int, message_id: int) -> bool:
    key = f"{chat_id}:{message_id}"
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            "SELECT 1 FROM history WHERE action='msg_processed' AND reason=? LIMIT 1",
            (key,),
        ) as cur:
            return await cur.fetchone() is not None

async def mark_msg_processed(chat_id: int, message_id: int):
    key = f"{chat_id}:{message_id}"
    await insert_history(None, "msg_processed", None, key)

# ------- конфиги (через history: action='config', reason=key, amount=int_value) -------

async def set_config_int(key: str, value: int):
    await insert_history(None, "config", value, key)

async def get_config_int(key: str, default: int) -> int:
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            "SELECT amount FROM history WHERE action='config' AND reason=? ORDER BY id DESC LIMIT 1",
            (key,),
        ) as cur:
            row = await cur.fetchone()
            return row[0] if row and row[0] is not None else default

# воспринимаемые ключи конфигов
CFG_BURN_BPS      = "burn_bps"        # 100 = 1%
CFG_INCOME        = "income"          # размер зп/кражи
CFG_MULT_DICE     = "mult_dice"
CFG_MULT_DARTS    = "mult_darts"
CFG_MULT_BOWLING  = "mult_bowling"
CFG_MULT_SLOTS    = "mult_slots"
CFG_CASINO_ON     = "casino_on"       # 0/1
CFG_LIMIT_BET     = "limit_bet"       # 0=нет
CFG_LIMIT_RAIN    = "limit_rain"      # 0=нет
CFG_PRICE_EMERALD = "price_emerald"   # цена эмеральда
# цены перков: key = price_perk:<code>

async def get_burn_bps() -> int:
    return await get_config_int(CFG_BURN_BPS, 100)  # 1% по умолчанию

async def set_burn_bps(v: int):
    if v < 0: v = 0
    if v > 500: v = 500
    await set_config_int(CFG_BURN_BPS, v)

async def get_income() -> int:
    return await get_config_int(CFG_INCOME, 5)

async def set_income(v: int):
    await set_config_int(CFG_INCOME, max(0, v))

async def get_multipliers() -> Dict[str, int]:
    return {
        "dice": await get_config_int(CFG_MULT_DICE, 3),
        "darts": await get_config_int(CFG_MULT_DARTS, 4),
        "bowling": await get_config_int(CFG_MULT_BOWLING, 5),
        "slots": await get_config_int(CFG_MULT_SLOTS, 6),
    }

async def set_multiplier(game: str, x: int):
    key_map = {
        "кубик": CFG_MULT_DICE, "dice": CFG_MULT_DICE,
        "дартс": CFG_MULT_DARTS, "darts": CFG_MULT_DARTS,
        "боулинг": CFG_MULT_BOWLING, "bowling": CFG_MULT_BOWLING,
        "автоматы": CFG_MULT_SLOTS, "slots": CFG_MULT_SLOTS,
    }
    k = key_map.get(game.lower())
    if k:
        await set_config_int(k, max(1, x))

async def get_casino_on() -> bool:
    return bool(await get_config_int(CFG_CASINO_ON, 1))

async def set_casino_on(on: bool):
    await set_config_int(CFG_CASINO_ON, 1 if on else 0)

async def get_limit_bet() -> int:
    return await get_config_int(CFG_LIMIT_BET, 0)

async def set_limit_bet(v: int):
    await set_config_int(CFG_LIMIT_BET, max(0, v))

async def get_limit_rain() -> int:
    return await get_config_int(CFG_LIMIT_RAIN, 0)

async def set_limit_rain(v: int):
    await set_config_int(CFG_LIMIT_RAIN, max(0, v))

async def get_price_emerald() -> int:
    return await get_config_int(CFG_PRICE_EMERALD, 1000)

async def set_price_emerald(v: int):
    await set_config_int(CFG_PRICE_EMERALD, max(1, v))

async def get_price_perk(code: str) -> Optional[int]:
    key = f"price_perk:{code}"
    val = await get_config_int(key, -1)
    return None if val < 0 else val

async def set_price_perk(code: str, v: int):
    key = f"price_perk:{code}"
    await set_config_int(key, max(1, v))

# ------- сейф/экономика -------

async def vault_init(cap: int, circulating_now: int):
    init_vault = cap - circulating_now
    if init_vault < 0:
        return None  # сигнализируем вызывающему — кап меньше оборота
    return await insert_history(None, "vault_init", init_vault, f"cap={cap}")

async def get_last_vault_cap() -> Optional[int]:
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("""
            SELECT reason FROM history
            WHERE action='vault_init'
            ORDER BY id DESC LIMIT 1
        """) as cur:
            row = await cur.fetchone()
    if not row or not row[0]:
        return None
    # reason: "cap=<N>"
    try:
        if "cap=" in row[0]:
            return int(row[0].split("cap=")[1])
    except:
        return None
    return None

async def get_epoch_start_id() -> Optional[int]:
    # id последнего vault_init
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("""
            SELECT id FROM history
            WHERE action='vault_init'
            ORDER BY id DESC LIMIT 1
        """) as cur:
            row = await cur.fetchone()
            return int(row[0]) if row else None

async def get_burned_since_epoch() -> int:
    start_id = await get_epoch_start_id()
    if start_id is None:
        return 0
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("""
            SELECT COALESCE(SUM(amount),0) FROM history
            WHERE id > ? AND action='burn'
        """, (start_id,)) as cur:
            row = await cur.fetchone()
            return int(row[0] or 0)

async def get_circulating() -> int:
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT COALESCE(SUM(balance),0) FROM users") as cur:
            row = await cur.fetchone()
            return int(row[0] or 0)

async def get_economy_stats() -> Optional[Dict[str, Any]]:
    cap = await get_last_vault_cap()
    if cap is None:
        return None
    burned = await get_burned_since_epoch()
    circulating = await get_circulating()
    vault = cap - burned - circulating
    if vault < 0:
        vault = 0

    supply = cap - burned
    if supply < 0:
        supply = 0

    bps = await get_burn_bps()
    income = await get_income()
    return {
        "cap": cap,
        "burned": burned,
        "circulating": circulating,
        "vault": vault,
        "supply": supply,
        "burn_bps": bps,
        "income": income,
    }

# операции "сжигания" и записи «входа/выхода» для аудита (расчёт vault делаем по формуле выше)
async def record_burn(amount: int, reason: str):
    await insert_history(None, "burn", amount, reason)

# ------- рынок (офферы через history) -------

# offer_create: user_id=seller, amount=price, reason=f"link=<url>"
# offer_cancel: user_id=seller or NULL (если куратор), amount=offer_id, reason="cancel"
# offer_sold:   user_id=buyer, amount=price, reason=f"offer_id=<id>;seller=<seller_id>"

async def create_offer(seller_id: int, link: str, price: int) -> int:
    return await insert_history(seller_id, "offer_create", price, f"link={link}")

async def cancel_offer(offer_id: int, by_user: Optional[int]):
    await insert_history(by_user, "offer_cancel", offer_id, "cancel")

async def list_active_offers() -> List[Dict[str, Any]]:
    # восстанавливаем активные: offer_create без cancel/sold
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("""
            SELECT id, user_id, amount, reason, date FROM history
            WHERE action='offer_create'
            ORDER BY id DESC
        """) as cur:
            creates = await cur.fetchall()

        # составим множества отмен/продаж
        async with db.execute("SELECT amount FROM history WHERE action='offer_cancel'") as cur:
            cancels = {r[0] for r in await cur.fetchall()}
        async with db.execute("""
            SELECT reason FROM history WHERE action='offer_sold'
        """) as cur:
            sold_rows = await cur.fetchall()
        sold_ids = set()
        for (reason,) in sold_rows:
            if not reason:
                continue
            for part in reason.split(";"):
                part = part.strip()
                if part.startswith("offer_id="):
                    try:
                        sold_ids.add(int(part.split("=")[1]))
                    except:
                        pass

    out = []
    for cid, seller, price, reason, date in creates:
        if cid in cancels or cid in sold_ids:
            continue
        link = ""
        if reason and "link=" in reason:
            link = reason.split("link=", 1)[1]
        out.append({"offer_id": cid, "seller_id": seller, "price": int(price or 0), "link": link, "date": date})
    return out
