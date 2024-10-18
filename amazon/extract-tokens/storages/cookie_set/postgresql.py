import json
import asyncio
import asyncpg

from datetime import datetime
from typing import TypeVar, Optional, List
from models.cookie import Cookie, AmazonCookieSet
from storages.cookie_set.base import CookieSetStorage


class PostgreSQLCookieSetStorage(CookieSetStorage):
    __sql_queries = {
        "init_schema": """
            CREATE SCHEMA IF NOT EXISTS scraping;
        """,
        "init_table": """
            CREATE TABLE IF NOT EXISTS "scraping"."amazon_cookie_sets" (
                id UUID DEFAULT gen_random_uuid(),
                postcode INT,
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
            FROM "scraping"."amazon_cookie_sets";
        """,
        "insert": """
            INSERT INTO "scraping"."amazon_cookie_sets"(postcode, location, cookies, expires)
            VALUES($1, $2, $3, $4);
        """,
        "cleanup": """
            DELETE FROM "scraping"."amazon_cookie_sets" 
            WHERE expires < $1 OR usable_times <= 0;
        """,
        "get_usable_cookie_set": """
            SELECT id, postcode, location, cookies, expires
            FROM "scraping"."amazon_cookie_sets"
            WHERE expires > $1 AND usable_times > 0
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
        self, /, conn_str: str = None, max_conn: int = 4, max_cookie_set: int = 100
    ) -> None:
        self.conn_str = conn_str
        self.max_conn = max_conn
        self.max_cookie_set = max_cookie_set
        self.pool: asyncpg.Pool = None

    async def initialize(self):
        pool = await asyncpg.create_pool(
            self.conn_str, min_size=max(2, self.max_conn), max_size=self.max_conn
        )
        conn: asyncpg.connection.Connection = await pool.acquire()
        is_success = True
        try:
            await conn.execute(self.__sql_queries["init_schema"])
            await conn.execute(self.__sql_queries["init_table"])
        except Exception as e:
            print("Got an exception:", e)
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
        data: asyncpg.Record = await self.pool.fetchrow(self.__sql_queries["get_count"])
        return data[0]

    def max_size(self):
        return self.max_cookie_set

    async def _add(self, postcode: int, location: str, cookies: List[Cookie]) -> bool:
        item = AmazonCookieSet(
            postcode=postcode,
            cookies=cookies,
            location=location,
        )

        is_success = True

        # Store in database
        conn: asyncpg.connection.Connection = await self.pool.acquire()
        try:
            async with conn.transaction() as tx:
                await conn.execute(
                    self.__sql_queries["insert"],
                    postcode,
                    location,
                    json.dumps(cookies),
                    item.expires,
                )
        except Exception as e:
            # Transaction error comes here, automatically rollback
            print("Can't add cookie set to pool:", e)
            is_success = False
        finally:
            await self.pool.release(conn)

        return is_success

    async def _clean(self) -> None:
        conn: asyncpg.connection.Connection = await self.pool.acquire()
        try:
            async with conn.transaction() as tx:
                await conn.execute(
                    self.__sql_queries["cleanup"],
                    datetime.now(),
                )
        except Exception as e:
            # Transaction error comes here, automatically rollback
            print("Can't clean cookie sets:", e)

        finally:
            await self.pool.release(conn)

    async def _get(self) -> Optional[AmazonCookieSet]:
        conn: asyncpg.connection.Connection = await self.pool.acquire()
        cookie_set = None
        current_time = datetime.now()
        try:
            async with conn.transaction():
                row: asyncpg.Record = await conn.fetchrow(
                    self.__sql_queries["get_usable_cookie_set"],
                    current_time,
                )
                if not row:
                    raise Exception("No cookie set found")

                cookie_set = AmazonCookieSet(
                    postcode=row["postcode"],
                    location=row["location"],
                    cookies=json.loads(row["cookies"]),
                    expires=row["expires"],
                )
                cookie_set_id = row["id"]
                await conn.execute(
                    self.__sql_queries["update_cookie_set"],
                    current_time,
                    cookie_set_id,
                )

        except Exception as e:
            # Transaction error comes here, automatically rollback
            print("Can't fetch cookie set:", e)
        finally:
            await self.pool.release(conn)

        return cookie_set

    async def add(
        self,
        postcode: int,
        location: str,
        cookies: List[Cookie],
        lock: asyncio.Lock = None,
    ) -> bool:
        if lock is None:
            return await self._add(postcode, location, cookies)
        async with lock:
            return await self._add(postcode, location, cookies)

    async def clean(self, lock: asyncio.Lock = None) -> None:
        if lock is None:
            return await self._clean()
        async with lock:
            return await self._clean()

    async def get(self, lock: asyncio.Lock = None) -> Optional[AmazonCookieSet]:
        if lock is None:
            return await self._get()
        async with lock:
            return await self._get()

    async def is_full(self) -> bool:
        return (await self.current_size()) >= self.max_size()

    async def is_empty(self) -> bool:
        return (await self.current_size()) == 0
