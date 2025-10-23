import aiosqlite
import re
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

CFG_BRAVO_WINDOW_SEC   = "bravo_window_sec"   # дефолт 600
CFG_BRAVO_MAX_VIEWERS  = "bravo_max_viewers"  # дефолт 10
CFG_PIN_Q_MULT         = "pin_q_mult"         # дефолт 9 (тихий = bonus * 9)


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
        await db.execute("PRAGMA journal_mode=WAL;")
        await db.execute("PRAGMA synchronous=NORMAL;")
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

# ==== LEGACY ALIASES FOR PERK CODES ====
PERK_ALIASES = {
    "вор": "кража",
    # сюда можно добавлять и другие переименования в будущем
}

def _normalize_perk_code(code: str) -> str:
    c = (code or "").strip().lower()
    return PERK_ALIASES.get(c, c)


async def grant_perk(user_id: int, perk_code: str):
    perk_code = _normalize_perk_code(perk_code)
    return await insert_history(user_id, "perk_grant", None, perk_code)

async def revoke_perk(user_id: int, perk_code: str):
    perk_code = _normalize_perk_code(perk_code)
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
        code = _normalize_perk_code(code)
        if action == "perk_grant":
            perks.add(code)
        else:
            perks.discard(code)
    return perks

async def get_perk_holders(perk_code: str) -> List[int]:
    target = _normalize_perk_code(perk_code)
    state = {}
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("""
            SELECT user_id, action, reason FROM history
            WHERE action IN ('perk_grant','perk_revoke')
            ORDER BY id ASC
        """) as cur:
            rows = await cur.fetchall()
    for uid, action, reason in rows:
        if uid is None:
            continue
        code = _normalize_perk_code(reason)
        if code != target:
            continue
        state[uid] = (action == "perk_grant")
    return [uid for uid, has in state.items() if has]


async def get_perks_summary() -> List[Tuple[str, int]]:
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("""
            SELECT user_id, action, reason FROM history
            WHERE action IN ('perk_grant','perk_revoke') AND reason IS NOT NULL
            ORDER BY id ASC
        """) as cur:
            rows = await cur.fetchall()

    state: dict[tuple[str,int], bool] = {}
    for uid, action, reason in rows:
        code = _normalize_perk_code(reason)
        if not code:
            continue
        state[(code, uid)] = (action == "perk_grant")

    counts: dict[str, int] = {}
    for (code, uid), has in state.items():
        if has:
            counts[code] = counts.get(code, 0) + 1

    out = []
    for code in sorted(counts.keys()):
        out.append((code, counts[code]))
    return out

def _reason_get(reason: str | None, key: str) -> str | None:
    if not reason:
        return None
    for part in reason.split(";"):
        part = part.strip()
        if part.startswith(key + "="):
            return part.split("=", 1)[1]
    return None

async def perk_credit_add(user_id: int, code: str):
    code = _normalize_perk_code(code)
    await insert_history(user_id, "perk_credit_add", 1, f"code={code}")

async def perk_credit_use(user_id: int, code: str) -> bool:
    code = _normalize_perk_code(code)
    # проверим, что кредит есть
    if (await get_perk_credits(user_id, code)) <= 0:
        return False
    await insert_history(user_id, "perk_credit_use", 1, f"code={code}")
    return True

async def get_perk_credits(user_id: int, code: str) -> int:
    code = _normalize_perk_code(code)
    add = use = 0
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("""
            SELECT action, COALESCE(amount,0), reason
            FROM history
            WHERE user_id=? AND action IN ('perk_credit_add','perk_credit_use')
        """, (user_id,)) as cur:
            rows = await cur.fetchall()
    for action, amt, reason in rows:
        if _reason_get(reason, "code") != code:
            continue
        if action == "perk_credit_add": add += int(amt or 0)
        else:                            use += int(amt or 0)
    return max(0, add - use)

async def perk_escrow_open(user_id: int, code: str, offer_id: int):
    code = _normalize_perk_code(code)
    await insert_history(user_id, "perk_escrow_open", None, f"code={code};offer_id={offer_id}")

async def perk_escrow_close(user_id: int, code: str, offer_id: int, typ: str):
    # typ: 'sold' | 'cancel'
    code = _normalize_perk_code(code)
    await insert_history(user_id, "perk_escrow_close", None, f"code={code};offer_id={offer_id};type={typ}")

async def get_perk_escrow_owner(offer_id: int) -> tuple[int | None, str | None]:
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("""
            SELECT user_id, reason FROM history
            WHERE action='perk_escrow_open' AND reason LIKE ?
            ORDER BY id DESC LIMIT 1
        """, (f"%offer_id={offer_id}%",)) as cur:
            row = await cur.fetchone()
    if not row: 
        return (None, None)
    uid, reason = row
    return (int(uid), _reason_get(reason, "code"))


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
# ==== BANK (ячейки): комиссии ====
CFG_CELL_DEP_FEE_PCT   = "cell_dep_fee_pct"    # комиссия за депозит, % от внесённой суммы
CFG_CELL_STOR_FEE_PCT  = "cell_stor_fee_pct"   # комиссия хранения, % за каждые 6 часа

async def get_cell_dep_fee_pct() -> int:
    return await get_config_int(CFG_CELL_DEP_FEE_PCT, 3)  # дефолт 3%

async def set_cell_dep_fee_pct(v: int):
    await set_config_int(CFG_CELL_DEP_FEE_PCT, max(0, v))

async def get_cell_stor_fee_pct() -> int:
    return await get_config_int(CFG_CELL_STOR_FEE_PCT, 1)  # дефолт 1% / 6ч

async def set_cell_stor_fee_pct(v: int):
    await set_config_int(CFG_CELL_STOR_FEE_PCT, max(0, v))

# ===== ЯЧЕЙКИ (БАНК) НА HISTORY =====
import math
FOUR_HOURS = 6 * 60 * 60  # в секундах

async def _now_ts(db=None) -> int:
    if db is not None:
        async with db.execute("SELECT CAST(strftime('%s','now') AS INTEGER)") as cur:
            row = await cur.fetchone()
        return int(row[0])
    async with aiosqlite.connect(DB_PATH) as xdb:
        async with xdb.execute("SELECT CAST(strftime('%s','now') AS INTEGER)") as cur:
            row = await cur.fetchone()
        return int(row[0])

async def _cell_get_last_ts(user_id: int) -> int | None:
    # последняя метка времени начисления хранения
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("""
            SELECT amount FROM history
            WHERE user_id=? AND action='cell_ts'
            ORDER BY id DESC LIMIT 1
        """, (user_id,)) as cur:
            row = await cur.fetchone()
    return None if row is None else int(row[0])

async def _cell_set_last_ts(user_id: int, ts: int):
    await insert_history(user_id, "cell_ts", ts, None)

async def _cell_calc_balance(user_id: int) -> int:
    """
    Баланс ячейки = сумма депо - сумма выводов - сумма комиссий хранения.
    Депозит пишем NET (после входной комиссии).
    """
    dep = wd = fee = 0
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("""
            SELECT action, COALESCE(amount,0) FROM history
            WHERE user_id=? AND action IN ('cell_dep','cell_wd','cell_fee')
        """, (user_id,)) as cur:
            rows = await cur.fetchall()
    for action, amt in rows:
        a = int(amt or 0)
        if action == "cell_dep":
            dep += a
        elif action == "cell_wd":
            wd += a
        elif action == "cell_fee":
            fee += a
    bal = dep - wd - fee
    return max(0, bal)

async def cell_touch(user_id: int) -> tuple[int,int]:
    """
    Применяем накопленные 4-часовые комиссии хранения «лениво».
    Возвращает (списано_сейчас, новый_баланс).
    """
    total_fee = 0
    async with aiosqlite.connect(DB_PATH) as db:
        now = await _now_ts(db)
    last = await _cell_get_last_ts(user_id)
    if last is None:
        # первая инициализация метки без списаний
        await _cell_set_last_ts(user_id, now)
        return 0, await _cell_calc_balance(user_id)

    intervals = max(0, (now - last) // FOUR_HOURS)
    if intervals == 0:
        return 0, await _cell_calc_balance(user_id)

    bal = await _cell_calc_balance(user_id)
    if bal <= 0:
        # просто двигаем метку времени
        await _cell_set_last_ts(user_id, last + intervals*FOUR_HOURS)
        return 0, 0

    pct = await get_cell_stor_fee_pct()
    for _ in range(intervals):
        fee = (bal * pct + 99) // 100
        if fee <= 0:
            break
        # записываем списание и уменьшаем локальный баланс
        await insert_history(user_id, "cell_fee", fee, None)
        total_fee += fee
        bal -= fee
        if bal <= 0:
            bal = 0
            break

    await _cell_set_last_ts(user_id, last + intervals*FOUR_HOURS)
    return total_fee, bal

# ==== BANK: КД грабителя (в днях) ====
CFG_BANK_ROB_CD_DAYS = "bank_rob_cd_days"

async def get_bank_rob_cooldown_days() -> int:
    return await get_config_int(CFG_BANK_ROB_CD_DAYS, 7)  # дефолт 7 дней

async def set_bank_rob_cooldown_days(v: int):
    await set_config_int(CFG_BANK_ROB_CD_DAYS, max(1, v))


async def cell_get_balance(user_id: int) -> int:
    await cell_touch(user_id)
    return await _cell_calc_balance(user_id)

async def cell_deposit(user_id: int, gross_amount: int) -> tuple[int,int,int]:
    """
    Депозит в ячейку. Возврат: (внесено_брутто, комиссия_входа, новый_баланс_ячейки).
    Комиссия входа уходит «в сейф» логически (мы логируем её как отдельное событие).
    """
    await cell_touch(user_id)
    dep_pct = await get_cell_dep_fee_pct()
    fee = (gross_amount * dep_pct + 99) // 100
    net = max(0, gross_amount - fee)
    # логируем net как приход в ячейку
    await insert_history(user_id, "cell_dep", net, f"gross={gross_amount};fee={fee}")
    if fee > 0:
        await insert_history(None, "cell_deposit_fee", fee, f"user_id={user_id}")
    new_bal = await _cell_calc_balance(user_id)
    return gross_amount, fee, new_bal

async def cell_withdraw(user_id: int, amount: int) -> tuple[int,int]:
    """
    Вывод из ячейки. Возврат: (выведено, новый_баланс_ячейки).
    Деньги на карман зачисляешь в командах через change_balance.
    """
    await cell_touch(user_id)
    bal = await _cell_calc_balance(user_id)
    take = min(max(0, amount), bal)
    if take > 0:
        await insert_history(user_id, "cell_wd", take, None)
    new_bal = await _cell_calc_balance(user_id)
    return take, new_bal

async def _cell_users() -> list[int]:
    # все, кто когда-либо взаимодействовал с ячейками
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("""
            SELECT DISTINCT user_id FROM history
            WHERE action IN ('cell_dep','cell_wd','cell_fee','cell_ts')
              AND user_id IS NOT NULL
        """) as cur:
            rows = await cur.fetchall()
    return [int(r[0]) for r in rows]

async def bank_touch_all_and_total() -> int:
    total = 0
    for uid in await _cell_users():
        await cell_touch(uid)
        total += await _cell_calc_balance(uid)
    return total

async def bank_zero_all_and_sum() -> int:
    total = 0
    for uid in await _cell_users():
        await cell_touch(uid)
        bal = await _cell_calc_balance(uid)
        if bal > 0:
            total += bal
            await insert_history(uid, "cell_wd", bal, "bank_rob")
    return total


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
        "darts": await get_config_int(CFG_MULT_DARTS, 3),
        "bowling": await get_config_int(CFG_MULT_BOWLING, 3),
        "slots": await get_config_int(CFG_MULT_SLOTS, 20),
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
    code = _normalize_perk_code(code)
    key = f"price_perk:{code}"
    val = await get_config_int(key, -1)
    if val >= 0:
        return val
    # если цены для нового кода нет — попробуем «устаревшие» ключи, которые ведут к этому коду
    for legacy, new in PERK_ALIASES.items():
        if new == code:
            legacy_val = await get_config_int(f"price_perk:{legacy}", -1)
            if legacy_val >= 0:
                return legacy_val
    return None


async def set_price_perk(code: str, v: int):
    code = _normalize_perk_code(code)
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
        link = _reason_get(reason, "link") or ""
        perk_code = _reason_get(reason, "perk_code")
        offer_type = "perk" if perk_code else "regular"

        out.append({
            "offer_id": cid,
            "seller_id": seller,
            "price": int(price or 0),
            "link": link,
            "perk_code": perk_code,
            "type": offer_type,
            "date": date
        })
    return out

async def create_perk_offer(seller_id: int, code: str, price: int) -> int:
    code = _normalize_perk_code(code)
    return await insert_history(seller_id, "offer_create", price, f"perk_code={code}")


# ------- герой дня (через history) -------

# ------- герой дня (покомнатно) -------
from datetime import datetime, timezone, timedelta

def _utc_now():
    return datetime.now(timezone.utc)

def _iso_utc(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).isoformat()

def _same_utc_day(a: datetime, b: datetime) -> bool:
    return a.astimezone(timezone.utc).date() == b.astimezone(timezone.utc).date()

async def hero_set_for_today(chat_id: int, user_id: int, hours: int = 24) -> int:
    """
    Назначить героя на чат chat_id на ближайшие 'hours' (обычно 24ч).
    reason: "chat_id=<id>;until=<ISO>"
    """
    until = _utc_now() + timedelta(hours=hours)
    reason = f"chat_id={chat_id};until={_iso_utc(until)}"
    return await insert_history(user_id, "hero_set", None, reason)

async def hero_get_current(chat_id: int) -> int | None:
    """
    Вернёт user_id героя, если ещё актуален в данном чате, иначе None.
    Берём последний hero_set для chat_id и проверяем until > now.
    """
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("""
            SELECT user_id, reason FROM history
            WHERE action='hero_set' AND reason LIKE ?
            ORDER BY id DESC LIMIT 20
        """, (f"%chat_id={chat_id}%",)) as cur:
            rows = await cur.fetchall()

    now = _utc_now()
    for uid, reason in rows:
        until = None
        for part in (reason or "").split(";"):
            p = part.strip()
            if p.startswith("until="):
                try:
                    until = datetime.fromisoformat(p.split("=",1)[1])
                except Exception:
                    until = None
        if until and now < until:
            return int(uid)
        # если встретили протухшую запись — продолжаем искать выше по истории
    return None

async def hero_has_claimed_today(chat_id: int, user_id: int, hours: int = 0) -> bool:
    """
    True, если с последнего hero_claim в этом чате прошло меньше `hours` часов.
    По умолчанию — 12 часов.
    """
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("""
            SELECT date FROM history
            WHERE user_id=? AND action='hero_claim' AND reason LIKE ?
            ORDER BY id DESC LIMIT 1
        """, (user_id, f"%chat_id={chat_id}%")) as cur:
            row = await cur.fetchone()

    if not row:
        return False

    # SQLite кладёт 'YYYY-MM-DD HH:MM:SS' (без таймзоны) — считаем это UTC.
    ts = row[0]
    try:
        last = datetime.fromisoformat(ts + ("+00:00" if "Z" not in ts and "+" not in ts else ""))
    except Exception:
        return False

    now = datetime.now(timezone.utc)
    return (now - last) < timedelta(hours=hours)

async def hero_record_claim(chat_id: int, user_id: int, amount: int):
    """Фиксируем разовый гонорар героя дня в конкретном чате."""
    await insert_history(user_id, "hero_claim", amount, f"chat_id={chat_id}")

async def hero_get_current_with_until(chat_id: int) -> tuple[int | None, datetime | None]:
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("""
            SELECT user_id, reason FROM history
            WHERE action='hero_set' AND reason LIKE ?
            ORDER BY id DESC LIMIT 20
        """, (f"%chat_id={chat_id}%",)) as cur:
            rows = await cur.fetchall()

    now = _utc_now()
    for uid, reason in rows:
        until = None
        for part in (reason or "").split(";"):
            p = part.strip()
            if p.startswith("until="):
                try:
                    until = datetime.fromisoformat(p.split("=",1)[1])
                except Exception:
                    until = None
        if until and now < until:
            return int(uid), until
        if until:
            break
    return None, None



# ==== NEW: жалование/надбавка ====
CFG_STIPEND_BASE   = "stipend_base"    # базовое жалование всем
CFG_STIPEND_BONUS  = "stipend_bonus"   # надбавка по одноимённому перку

async def get_stipend_base() -> int:
    return await get_config_int(CFG_STIPEND_BASE, 5)

async def set_stipend_base(v: int):
    await set_config_int(CFG_STIPEND_BASE, max(0, v))

async def get_stipend_bonus() -> int:
    return await get_config_int(CFG_STIPEND_BONUS, 45)  # пример: база 5 + бонус 45 = 50

async def set_stipend_bonus(v: int):
    await set_config_int(CFG_STIPEND_BONUS, max(0, v))


# ==== NEW: щедрость ====
CFG_GEN_MULT_PCT = "generosity_mult_pct"   # проценты, 5 = 5%
CFG_GEN_THRESHOLD = "generosity_threshold" # порог очков для выплаты
# ==== NEW: шансы перков (0..100 %) ====
CFG_PERK_SHIELD_CHANCE      = "perk_shield_chance"       # "Щит": шанс увернуться от кражи
CFG_PERK_CROUPIER_CHANCE    = "perk_croupier_chance"     # "Крупье": шанс частичного рефанда в играх
CFG_PERK_PHILANTHROPE_CHANCE= "perk_philanthrope_chance" # "Филантроп": шанс подарка шестому в дожде
CFG_PERK_LUCKY_CHANCE       = "perk_lucky_chance"        # "Везунчик": шанс автопопадания в дождь

async def get_perk_shield_chance() -> int:
    return max(0, min(100, await get_config_int(CFG_PERK_SHIELD_CHANCE, 50)))  # дефолт 50%

async def set_perk_shield_chance(p: int):
    await set_config_int(CFG_PERK_SHIELD_CHANCE, max(0, min(100, p)))

async def get_perk_croupier_chance() -> int:
    return max(0, min(100, await get_config_int(CFG_PERK_CROUPIER_CHANCE, 15)))  # дефолт 15%

async def set_perk_croupier_chance(p: int):
    await set_config_int(CFG_PERK_CROUPIER_CHANCE, max(0, min(100, p)))

async def get_perk_philanthrope_chance() -> int:
    return max(0, min(100, await get_config_int(CFG_PERK_PHILANTHROPE_CHANCE, 15)))  # дефолт 15%

async def set_perk_philanthrope_chance(p: int):
    await set_config_int(CFG_PERK_PHILANTHROPE_CHANCE, max(0, min(100, p)))

async def get_perk_lucky_chance() -> int:
    return max(0, min(100, await get_config_int(CFG_PERK_LUCKY_CHANCE, 33)))  # дефолт 33%

async def set_perk_lucky_chance(p: int):
    await set_config_int(CFG_PERK_LUCKY_CHANCE, max(0, min(100, p)))


async def get_generosity_mult_pct() -> int:
    return await get_config_int(CFG_GEN_MULT_PCT, 5)

async def set_generosity_mult_pct(v: int):
    await set_config_int(CFG_GEN_MULT_PCT, max(0, v))

async def get_generosity_threshold() -> int:
    return await get_config_int(CFG_GEN_THRESHOLD, 50)

async def set_generosity_threshold(v: int):
    await set_config_int(CFG_GEN_THRESHOLD, max(1, v))

async def add_generosity_points(user_id: int, pts: int, source: str):
    # pts можно 0—тогда ничего страшного
    if pts <= 0:
        return
    await insert_history(user_id, "generosity_add", pts, f"src={source}")

async def get_generosity_points(user_id: int) -> int:
    # сумма add - сумма списаний (выплат) в очках
    total = 0
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("""
            SELECT COALESCE(SUM(amount),0) FROM history
            WHERE user_id=? AND action='generosity_add'
        """,(user_id,)) as cur:
            a = await cur.fetchone()
            total += int(a[0] or 0)
        async with db.execute("""
            SELECT COALESCE(SUM(amount),0) FROM history
            WHERE user_id=? AND action='generosity_pay_points'
        """,(user_id,)) as cur:
            b = await cur.fetchone()
            total -= int(b[0] or 0)
    return max(0, total)

async def generosity_try_payout(user_id: int) -> int:
    """
    Если очки >= порога — списываем порог очков и выдаём столько же нуаров.
    Возвращает размер выплаты (0 если не было).
    """
    points = await get_generosity_points(user_id)
    threshold = await get_generosity_threshold()
    if points < threshold:
        return 0
    # списываем порог очков
    await insert_history(user_id, "generosity_pay_points", threshold, None)
    # выдаём столько же нуаров из сейфа
    await insert_history(user_id, "generosity_payout", threshold, None)
    await change_balance(user_id, threshold, "щедрость", user_id)
    return threshold


# ==== NEW: платные утилиты-пины ====
CFG_PRICE_PIN        = "price_util_pin"        # тихий пин
CFG_PRICE_PIN_LOUD   = "price_util_pin_loud"   # громкий пин (с уведомлением)

async def get_price_pin() -> int:
    return await get_config_int(CFG_PRICE_PIN, 100)

async def set_price_pin(v: int):
    await set_config_int(CFG_PRICE_PIN, max(1, v))

async def get_price_pin_loud() -> int:
    return await get_config_int(CFG_PRICE_PIN_LOUD, 500)

async def set_price_pin_loud(v: int):
    await set_config_int(CFG_PRICE_PIN_LOUD, max(1, v))


# ==== NEW: код-слово ====
# Сохраняем "активную" игру код-слово через history
# codeword_set: user_id=куратор, amount=приз, reason="chat_id=<id>;word=<w>;active=1"
# codeword_win: user_id=победитель, amount=приз, reason="chat_id=<id>;word=<w>"
# codeword_cancel: user_id=куратор, amount=NULL, reason="chat_id=<id>;word=<w>"

async def codeword_set(chat_id: int, word: str, prize: int, curator_id: int):
    return await insert_history(curator_id, "codeword_set", prize, f"chat_id={chat_id};word={word};active=1")

async def codeword_cancel_active(chat_id: int, curator_id: int):
    cw = await codeword_get_active(chat_id)
    if not cw:
        return False
    word = cw["word"]
    await insert_history(curator_id, "codeword_cancel", None, f"chat_id={chat_id};word={word}")
    # снимаем флаг активной (записываем «деактивацию» отдельной записью)
    await insert_history(curator_id, "codeword_set", cw["prize"], f"chat_id={chat_id};word={word};active=0")
    return True

async def codeword_get_active(chat_id: int):
    # ищем последнюю запись set для заданного чата и проверяем её активность
    last = None
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("""
            SELECT id, user_id, amount, reason, date
            FROM history WHERE action='codeword_set' AND reason LIKE ?
            ORDER BY id DESC LIMIT 20
        """, (f"%chat_id={chat_id}%",)) as cur:
            rows = await cur.fetchall()
    for rid, uid, amount, reason, date in rows:
        # читаем word=...;active=x
        word = None
        active = None
        for part in (reason or "").split(";"):
            part = part.strip()
            if part.startswith("word="):
                word = part.split("=",1)[1]
            elif part.startswith("active="):
                try: active = int(part.split("=",1)[1])
                except: active = None
        if active == 1 and word:
            last = {"id": rid, "curator_id": uid, "prize": int(amount or 0), "word": word, "date": date}
            break
        if active == 0:
            break
    return last

async def codeword_mark_win(chat_id: int, winner_id: int, prize: int, word: str):
    await insert_history(winner_id, "codeword_win", prize, f"chat_id={chat_id};word={word}")
    # деактивируем
    await insert_history(winner_id, "codeword_set", prize, f"chat_id={chat_id};word={word};active=0")


# ==== NEW: обороты рынка ====
# Суммируем суммы по событиям (perk_buy / emerald_buy / offer_sold) за окно в днях
async def get_market_turnover_days(days: int) -> int:
    total = 0
    async with aiosqlite.connect(DB_PATH) as db:
        for action in ("perk_buy", "emerald_buy", "offer_sold"):
            async with db.execute(f"""
                SELECT COALESCE(SUM(amount),0) FROM history
                WHERE action='{action}' AND date >= datetime('now', ?)
            """, (f'-{days} days',)) as cur:
                row = await cur.fetchone()
                total += int(row[0] or 0)
    return total

# ==== Ограбление банка (КД и лог) ====
async def get_seconds_since_last_bank_rob(user_id: int) -> int | None:
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("""
            SELECT CAST(strftime('%s','now') AS INTEGER) - CAST(strftime('%s', date) AS INTEGER)
            FROM history
            WHERE user_id=? AND action='bank_rob'
            ORDER BY id DESC LIMIT 1
        """, (user_id,)) as cur:
            row = await cur.fetchone()
            return None if row is None else int(row[0])

async def record_bank_rob(user_id: int, outcome: str, amount: int):
    # outcome: success | fail | busted
    await insert_history(user_id, "bank_rob", amount, outcome)

async def touch_user(user_id: int, username: str | None = None):
    async with aiosqlite.connect(DB_PATH) as db:
        await ensure_user(db, user_id)
        if username is not None:
            await db.execute("UPDATE users SET username=? WHERE user_id=?", (username, user_id))
            await db.commit()

async def get_bravo_window_sec() -> int:
    return await get_config_int(CFG_BRAVO_WINDOW_SEC, 600)

async def get_bravo_max_viewers() -> int:
    return await get_config_int(CFG_BRAVO_MAX_VIEWERS, 5)

async def get_pin_q_mult() -> int:
    return await get_config_int(CFG_PIN_Q_MULT, 9)

async def set_pin_q_mult(v: int):
    await set_config_int(CFG_PIN_Q_MULT, max(1, v))

async def hero_save_claim_msg(chat_id: int, hero_id: int, msg_id: int, ts_unix: int):
    await insert_history(hero_id, "hero_claim_msg", None, f"chat_id={chat_id};msg_id={msg_id};ts={ts_unix}")

async def hero_get_last_claim_msg(chat_id: int) -> dict|None:
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("""
            SELECT user_id, reason, date FROM history
            WHERE action='hero_claim_msg' AND reason LIKE ?
            ORDER BY id DESC LIMIT 1
        """,(f"%chat_id={chat_id}%",)) as cur:
            row = await cur.fetchone()
    if not row: return None
    uid, reason, date = row
    return {
        "hero_id": int(uid),
        "msg_id": int(_reason_get(reason, "msg_id") or 0),
        "ts": int(_reason_get(reason, "ts") or 0)
    }

async def bravo_count_for_msg(chat_id: int, msg_id: int) -> int:
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("""
            SELECT COUNT(1) FROM history
            WHERE action='bravo_claim' AND reason=?
        """,(f"chat_id={chat_id};msg_id={msg_id}",)) as cur:
            row = await cur.fetchone()
            return int(row[0] or 0)

async def bravo_already_claimed(user_id: int, chat_id: int, msg_id: int) -> bool:
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("""
            SELECT 1 FROM history
            WHERE user_id=? AND action='bravo_claim' AND reason=?
            LIMIT 1
        """,(user_id, f"chat_id={chat_id};msg_id={msg_id}")) as cur:
            return (await cur.fetchone()) is not None

async def record_bravo(user_id: int, chat_id: int, msg_id: int, reward: int):
    await insert_history(user_id, "bravo_claim", reward, f"chat_id={chat_id};msg_id={msg_id}")

# --- SAFE FREE = vault - bank(total) ---
async def get_vault_free_amount() -> int:
    """
    Сколько можно реально выдать из сейфа:
    свободный сейф = сейф - сумма всех ячеек (банк).
    Возвращает 0, если сейф выключен или ушли в минус.
    """
    stats = await get_economy_stats()  # уже собирает текущий сейф
    if not stats:
        return 0
    # важно: сперва «дотронуться» до банка, чтобы применились комиссии хранения
    total_bank = await bank_touch_all_and_total()
    return max(0, int(stats["vault"]) - int(total_bank))

# --- BANK/VАULT helpers ---

async def get_bank_total() -> int:
    """
    Сумма всех ячеек (банк). Вызывает lazy-touch хранения.
    """
    total = await bank_touch_all_and_total()  # функция у тебя уже есть
    return int(total)

async def get_vault_net() -> int:
    """
    Сейф «свободно» = сырое значение сейфа - банк (ячейки), не ниже 0.
    """
    stats = await get_economy_stats()  # как и раньше получаем сырое значение сейфа
    bank = await get_bank_total()
    return max(0, int(stats.get("vault", 0)) - bank)
