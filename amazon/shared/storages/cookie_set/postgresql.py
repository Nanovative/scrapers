import json
import uuid
import asyncio
import asyncpg
import logging

from datetime import datetime, timedelta
from typing import Optional, List
from shared.models.cookie import Cookie, AmazonCookieSet
from shared.models.enums import BrowserType
from shared.storages.cookie_set.base import CookieSetStorage


class PostgreSQLCookieSetStorage(CookieSetStorage):
    sql_queries = {
        "check_schema": """
            SELECT nspname
            FROM "pg_catalog"."pg_namespace"
            WHERE nspname = 'scraping';
        """,
        "init_schema": """
            CREATE SCHEMA IF NOT EXISTS scraping;
        """,
        "init_table": """
            CREATE TABLE IF NOT EXISTS "scraping"."amazon_cookie_sets" (
                id UUID DEFAULT gen_random_uuid(),
                postcode INT,
                browser_type VARCHAR(15),
                cookies JSONB,
                location VARCHAR(255),
                expires TIMESTAMP DEFAULT NOW() + INTERVAL '3 days',
                usable_times INT DEFAULT 5,
                created_at TIMESTAMP DEFAULT NOW(),
                last_used TIMESTAMP,
                CONSTRAINT not_negative_usable_times CHECK (usable_times >= 0)
            );
        """,
        "get_count": """
            SELECT COUNT(*)
            FROM "scraping"."amazon_cookie_sets"
            WHERE browser_type = $1;
        """,
        "insert": """
            INSERT INTO "scraping"."amazon_cookie_sets"(
                postcode, 
                location, 
                cookies, 
                expires, 
                browser_type
            )
            VALUES($1, $2, $3, $4, $5);
        """,
        "cleanup": """
            DELETE FROM "scraping"."amazon_cookie_sets" 
            WHERE browser_type = $1 AND (expires < $2 OR usable_times <= 0);
        """,
        "get_usable_cookie_set": """
            SELECT id, postcode, location, cookies, expires
            FROM "scraping"."amazon_cookie_sets"
            WHERE browser_type = $1 AND expires > $2 AND usable_times > 0
            ORDER BY expires ASC, last_used DESC
            LIMIT 1
        """,
        "update_cookie_set": """
            UPDATE "scraping"."amazon_cookie_sets"
            SET usable_times = usable_times - 1, last_used = $1
            WHERE id = $2
        """,
    }

    def __init__(
        self,
        /,
        conn_str: str = None,
        max_conn: int = 2,
        max_cookie_set: int = 100,
        browser_type: BrowserType = BrowserType.firefox,
        **kwargs,
    ) -> None:
        self.conn_str = conn_str
        self.max_conn = max_conn
        self.min_conn = min(1, self.max_conn)
        self.max_cookie_set = max_cookie_set
        self.pool: asyncpg.Pool = None
        self.browser_type = browser_type

    async def initialize(self):
        pool = await asyncpg.create_pool(
            dsn=self.conn_str,
            min_size=self.min_conn,
            max_size=self.max_conn,
        )
        try:
            schema_check = await pool.fetchrow(self.sql_queries["check_schema"])
            if schema_check is None:
                logging.info("Schema is not present, creating one ...")
                await pool.execute(self.sql_queries["init_schema"])
        except Exception as e:
            pass

        conn: asyncpg.connection.Connection = await pool.acquire()
        is_success = True
        try:
            await conn.execute(self.sql_queries["init_table"])
        except Exception as e:
            logging.info(f"Got problem when initializing cookie set storage: {e}")
            is_success = False
        finally:
            await pool.release(conn)
            if not is_success:
                await pool.close()
                raise Exception("Could not initialize PostgreSQL cookie set storage")
            self.pool = pool

    async def close(self):
        await self.pool.close()

    async def current_size(self) -> int:
        record: asyncpg.Record = await self.pool.fetchrow(
            self.sql_queries["get_count"], self.browser_type.value
        )
        return record[0]

    def max_size(self):
        return self.max_cookie_set

    async def _add(
        self,
        postcode: int,
        location: str,
        cookies: List[Cookie],
        coroutine_id: uuid.UUID = None,
    ) -> bool:
        item = AmazonCookieSet(
            postcode=postcode,
            cookies=cookies,
            location=location,
            expires=datetime.now() + timedelta(days=3),
        )

        is_success = True

        # Store in database
        conn: asyncpg.connection.Connection = await self.pool.acquire()
        try:
            async with conn.transaction() as tx:
                await conn.execute(
                    self.sql_queries["insert"],
                    postcode,
                    location,
                    json.dumps(cookies),
                    item.expires,
                    self.browser_type.value,
                )
        except Exception as e:
            # Transaction error comes here, automatically rollback
            logging.info(
                f"[coroutine_id={coroutine_id}]: Can't add cookie set to pool: {e}"
            )
            is_success = False
        finally:
            await self.pool.release(conn)

        return is_success

    async def _clean(self, coroutine_id: uuid.UUID = None) -> None:
        conn: asyncpg.connection.Connection = await self.pool.acquire()
        try:
            async with conn.transaction() as tx:
                await conn.execute(
                    self.sql_queries["cleanup"],
                    self.browser_type.value,
                    datetime.now(),
                )
        except Exception as e:
            # Transaction error comes here, automatically rollback
            logging.info(f"[coroutine_id={coroutine_id}]: Can't clean cookie sets: {e}")

        finally:
            await self.pool.release(conn)

    async def _get(self, coroutine_id: uuid.UUID = None) -> Optional[AmazonCookieSet]:
        conn: asyncpg.connection.Connection = await self.pool.acquire()
        cookie_set = None
        current_time = datetime.now()
        try:
            async with conn.transaction():
                row: asyncpg.Record = await conn.fetchrow(
                    self.sql_queries["get_usable_cookie_set"],
                    self.browser_type.value,
                    current_time,
                )
                if not row:
                    raise Exception("No cookie set found")

                cookie_set_id = row["id"]
                cookie_set = AmazonCookieSet(
                    id=cookie_set_id,
                    postcode=row["postcode"],
                    location=row["location"],
                    cookies=json.loads(row["cookies"]),
                    expires=row["expires"],
                )
                cookie_set_id = row["id"]
                await conn.execute(
                    self.sql_queries["update_cookie_set"],
                    current_time,
                    cookie_set_id,
                )

        except Exception as e:
            # Transaction error comes here, automatically rollback
            logging.info(f"[coroutine_id={coroutine_id}]: Can't fetch cookie set: {e}")
        finally:
            await self.pool.release(conn)

        return cookie_set

    async def add(
        self,
        postcode: int,
        location: str,
        cookies: List[Cookie],
        coroutine_id: uuid.UUID = None,
        lock: asyncio.Lock = None,
    ) -> bool:
        if lock is None:
            return await self._add(postcode, location, cookies, coroutine_id)
        async with lock:
            return await self._add(postcode, location, cookies, coroutine_id)

    async def clean(
        self, coroutine_id: uuid.UUID = None, lock: asyncio.Lock = None
    ) -> None:
        if lock is None:
            return await self._clean(coroutine_id)
        async with lock:
            return await self._clean(coroutine_id)

    async def get(
        self, coroutine_id: uuid.UUID = None, lock: asyncio.Lock = None
    ) -> Optional[AmazonCookieSet]:
        if lock is None:
            return await self._get(coroutine_id)
        async with lock:
            return await self._get(coroutine_id)

    async def is_full(self) -> bool:
        return (await self.current_size()) >= self.max_size()

    async def is_empty(self) -> bool:
        return (await self.current_size()) == 0
