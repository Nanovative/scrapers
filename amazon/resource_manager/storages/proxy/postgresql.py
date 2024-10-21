from asyncio import Lock
import uuid
import asyncpg

from datetime import datetime
from typing import Optional
from utils import coroutine_print
from models.proxy import Proxy
from storages.proxy.base import ProxyStorage


class PostgreSQLProxyStorage(ProxyStorage):
    __sql_queries = {
        "check_schema": """
            SELECT nspname
            FROM "pg_catalog"."pg_namespace"
            WHERE nspname = 'scraping';
        """,
        "init_schema": """
            CREATE SCHEMA IF NOT EXISTS scraping;
        """,
        "init_table": """
            CREATE TABLE IF NOT EXISTS "scraping"."proxies" (
                id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                tag VARCHAR(20) NOT NULL DEFAULT 'general',
                content VARCHAR(255),
                provider VARCHAR(50),
                created_at TIMESTAMP DEFAULT NOW(),
                last_used TIMESTAMP
            );
        """,
        "get_unique_tags": """
            SELECT DISTINCT tag
            FROM "scraping"."proxies";
        """,
        "insert": """
            INSERT INTO "scraping"."proxies" (
                tag,
                provider,
                created_at,
                content
            ) VALUES ($1, $2, $3, $4);
        """,
        "delete": """
            DELETE FROM "scraping"."proxies"
            WHERE tag = $1 AND provider = $2;
        """,
        "update_proxy": """
            UPDATE "scraping"."proxies" 
            SET last_used = $1 
            WHERE id = $2;
        """,
        "get_LRU_proxy": """
            SELECT id, content 
            FROM "scraping"."proxies"
            WHERE tag = $1 AND provider = $2
            ORDER BY COALESCE(last_used, '1900-01-01') ASC
            LIMIT 1;
        """,
        "get_count": """
            SELECT COUNT(*)
            FROM "scraping"."proxies"
            WHERE tag = $1 AND provider = $2
        """,
    }

    def __init__(
        self,
        /,
        conn_str: str = None,
        max_conn: int = 2,
        max_cookie_set: int = 100,
        **kwargs,
    ) -> None:
        self.conn_str = conn_str
        self.max_conn = max_conn
        self.min_conn = min(1, self.max_conn)
        self.max_cookie_set = max_cookie_set
        self.pool: asyncpg.Pool = None
        self.default_tag = "general"

    async def initialize(self):
        pool = await asyncpg.create_pool(
            dsn=self.conn_str,
            min_size=self.min_conn,
            max_size=self.max_conn,
        )
        try:
            schema_check = await pool.fetchrow(self.__sql_queries["check_schema"])
            if schema_check is None:
                print("Schema is not present, creating one ...")
                await pool.execute(self.__sql_queries["init_schema"])
        except Exception as e:
            pass

        conn: asyncpg.connection.Connection = await pool.acquire()
        is_success = True
        try:
            await conn.execute(self.__sql_queries["init_table"])
        except Exception as e:
            print("Got problem when initializing proxy storage:", e)
            is_success = False
        finally:
            await pool.release(conn)
            if not is_success:
                await pool.close()
                raise Exception("Could not initialize PostgreSQL proxy storage")
            self.pool = pool

    async def close(self):
        await self.pool.close()

    async def current_size(self, tag: str = None, provider: str = "iproyal") -> int:
        tag = self.default_tag if not tag else tag
        record: asyncpg.Record = await self.pool.fetchrow(
            self.__sql_queries["get_count"],
            tag,
            provider,
        )
        return record[0]

    async def _rotate(
        self,
        tag: str = None,
        provider: str = "iproyal",
        coroutine_id: uuid.UUID = None,
    ) -> Optional[Proxy]:
        conn: asyncpg.connection.Connection = await self.pool.acquire()
        proxy = None
        current_time = datetime.now()
        tag = self.default_tag if not tag else tag
        try:
            async with conn.transaction():
                row: asyncpg.Record = await conn.fetchrow(
                    self.__sql_queries["get_LRU_proxy"],
                    tag,
                    provider,
                )
                if not row:
                    raise Exception("No proxy found")

                proxy_id, proxy_content = row["id"], row["content"]
                proxy = Proxy(
                    provider=provider, proxies=[proxy_content], last_used=current_time
                )
                await conn.execute(
                    self.__sql_queries["update_proxy"],
                    current_time,
                    proxy_id,
                )

        except Exception as e:
            # Transaction error comes here, automatically rollback
            coroutine_print(coroutine_id, "Can't rotate proxy:", e)
        finally:
            await self.pool.release(conn)

        return proxy

    async def _replace(
        self,
        proxies: list[str],
        tag: str = None,
        provider: str = "iproyal",
        coroutine_id: uuid.UUID = None,
    ) -> bool:
        tag = self.default_tag if not tag else tag
        conn: asyncpg.connection.Connection = await self.pool.acquire()
        current_time = datetime.now()
        is_success = True
        tag = self.default_tag if not tag else tag
        try:
            async with conn.transaction():
                await conn.execute(
                    self.__sql_queries["delete"],
                    tag,
                    provider,
                )

                await conn.executemany(
                    self.__sql_queries["insert"],
                    [(tag, provider, current_time, proxy) for proxy in proxies],
                )

        except Exception as e:
            # Transaction error comes here, automatically rollback
            coroutine_print(coroutine_id, "Can't replace proxies:", e)
            is_success = False
        finally:
            await self.pool.release(conn)

        return is_success

    async def _get_tags(self) -> list[str]:
        records: list[asyncpg.Record] = await self.pool.fetchmany(
            self.__sql_queries["get_unique_tags"],
        )
        return [record["tag"] for record in records]

    async def get_tags(
        self,
        lock: Lock = None,
    ) -> list[str]:
        if lock is None:
            return await self._get_tags()
        async with lock:
            return await self._get_tags()

    async def rotate(
        self,
        tag: str = None,
        provider: str = "iproyal",
        coroutine_id: uuid.UUID = None,
        lock: Lock = None,
    ) -> Optional[Proxy]:
        if lock is None:
            return await self._rotate(tag, provider, coroutine_id)
        async with lock:
            return await self._rotate(tag, provider, coroutine_id)

    async def replace(
        self,
        proxies: list[str],
        tag: str = None,
        provider: str = "iproyal",
        coroutine_id: uuid.UUID = None,
        lock: Lock = None,
    ) -> bool:
        if lock is None:
            return await self._replace(proxies, tag, provider, coroutine_id)
        async with lock:
            return await self._replace(proxies, tag, provider, coroutine_id)

    async def is_empty(self, tag: str = None, provider: str = "iproyal") -> bool:
        return (await self.current_size(tag, provider)) == 0
