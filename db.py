import aiosqlite

DB_PATH = "/data/bot_data.sqlite"
SCHEMA_VERSION = 1  # при изменении схемы увеличивай это число

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

async def _table_columns(db, table: str):
    async with db.execute(f"PRAGMA table_info({table})") as cur:
        rows = await cur.fetchall()
    return [r[1] for r in rows]  # name is at index 1

async def _schema_ok(db) -> bool:
    # Проверяем наличие таблиц и состав столбцов
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
        if cols != expected:  # важен и порядок, и наличие столбцов
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
        # Включаем foreign keys на всякий
        await db.execute("PRAGMA foreign_keys = ON")
        # Узнаём текущую версию
        async with db.execute("PRAGMA user_version") as cur:
            row = await cur.fetchone()
        current_ver = row[0] if row else 0

        # Создаём таблицы (если их нет)
        await db.execute(CREATE_USERS)
        await db.execute(CREATE_ROLES)
        await db.execute(CREATE_HISTORY)
        await db.commit()

        # Если версия не совпадает или таблицы «битые» — пересобираем
        if current_ver != SCHEMA_VERSION or not await _schema_ok(db):
            await _recreate_all(db)

# --- Баланс ---
async def get_balance(user_id: int) -> int:
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT balance FROM users WHERE user_id = ?", (user_id,)) as cur:
            row = await cur.fetchone()
            return row[0] if row else 0

async def change_balance(user_id: int, amount: int, reason: str, author_id: int):
    async with aiosqlite.connect(DB_PATH) as db:
        # гарантируем наличие пользователя
        async with db.execute("SELECT balance FROM users WHERE user_id = ?", (user_id,)) as cur:
            row = await cur.fetchone()
        if row is None:
            await db.execute("INSERT INTO users (user_id, username, balance, key) VALUES (?, ?, ?, ?)",
                             (user_id, None, 0, 0))
            current_balance = 0
        else:
            current_balance = row[0]

        new_balance = current_balance + amount
        if new_balance < 0:
            new_balance = 0

        await db.execute("UPDATE users SET balance = ? WHERE user_id = ?", (new_balance, user_id))
        await db.execute(
            "INSERT INTO history (user_id, action, amount, reason) VALUES (?, ?, ?, ?)",
            (user_id, 'change_balance', amount, reason)
        )
        await db.commit()

async def reset_user_balance(user_id: int):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("UPDATE users SET balance = 0 WHERE user_id = ?", (user_id,))
        await db.commit()

async def reset_all_balances():
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("UPDATE users SET balance = 0")
        await db.commit()

# --- Роли ---
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
            return row  # (role_name, role_desc, role_image) или None

# --- Ключи ---
async def grant_key(user_id: int):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
            INSERT INTO users (user_id, username, balance, key)
            VALUES (?, NULL, 0, 1)
            ON CONFLICT(user_id) DO UPDATE SET key = 1
        """, (user_id,))
        await db.commit()

async def revoke_key(user_id: int):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("UPDATE users SET key = 0 WHERE user_id = ?", (user_id,))
        await db.commit()

async def has_key(user_id: int) -> bool:
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT key FROM users WHERE user_id = ?", (user_id,)) as cur:
            row = await cur.fetchone()
            return bool(row and row[0] == 1)

# --- История/Топ/Роли списка ---
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

# --- Держатели ключа ---
async def get_key_holders():
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("""
            SELECT user_id FROM users
            WHERE key = 1
            ORDER BY user_id ASC
        """) as cur:
            rows = await cur.fetchall()
            return [r[0] for r in rows]