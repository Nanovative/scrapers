import logging
import uuid
import asyncpg
from asyncio import Lock
from typing import List, Optional
from shared.models.category import Category
from shared.storages.category.base import CategoryStorage


class PostgreSQLCategoryStorage(CategoryStorage):
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
            CREATE TABLE IF NOT EXISTS "scraping"."amazon_categories" (
                id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
                name VARCHAR(255) NOT NULL,
                depth INT NOT NULL CHECK (depth >= -1),
                ancestor VARCHAR(100),
                parent VARCHAR(100),
                path TEXT,
                url TEXT,
                is_leaf BOOLEAN,
                CONSTRAINT unique_name_per_level UNIQUE (name, depth)
            );
        """,
        "get_by_name": """
            SELECT * FROM "scraping"."amazon_categories"
            WHERE name = $1;
        """,
        "get_by_depth": """
            SELECT * FROM "scraping"."amazon_categories"
            WHERE depth <= $1;
        """,
        "get_by_ancestor": """
            SELECT * FROM "scraping"."amazon_categories"
            WHERE ancestor = $1;
        """,
        "get_by_parent": """
            SELECT * FROM "scraping"."amazon_categories"
            WHERE parent = $1;
        """,
        "get_by_leaf": """
            SELECT * FROM "scraping"."amazon_categories"
            WHERE is_leaf = $1;
        """,
        "get_by_ancestors_and_depth": """
            SELECT * FROM "scraping"."amazon_categories"
            WHERE ancestor = ANY($1) AND depth <= $2;
        """,
        "insert": """
            INSERT INTO "scraping"."amazon_categories" (
                name, depth, ancestor, parent, path, url, is_leaf
            ) VALUES ($1, $2, $3, $4, $5, $6, $7);
        """,
        "delete": """
            DELETE FROM "scraping"."amazon_categories";
        """,
    }

    def __init__(
        self,
        /,
        conn_str: str = None,
        max_conn: int = 2,
        **kwargs,
    ) -> None:
        self.conn_str = conn_str
        self.max_conn = max_conn
        self.min_conn = min(1, self.max_conn)
        self.pool: asyncpg.Pool = None

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
            logging.error(f"Error checking schema: {e}")

        conn: asyncpg.Connection = await pool.acquire()
        is_success = True
        try:
            await conn.execute(self.sql_queries["init_table"])
        except Exception as e:
            logging.error(f"Error initializing category storage: {e}")
            is_success = False
        finally:
            await pool.release(conn)
            if not is_success:
                await pool.close()
                raise Exception("Could not initialize PostgreSQL category storage")
            self.pool = pool

    async def close(self):
        await self.pool.close()

    async def replace(
        self,
        categories: List[Category],
        coroutine_id: uuid.UUID = None,
        lock: Lock = None,
    ) -> bool:
        if lock is None:
            return await self._replace(categories, coroutine_id)
        async with lock:
            return await self._replace(categories, coroutine_id)

    async def _replace(
        self,
        categories: List[Category],
        coroutine_id: uuid.UUID = None,
    ) -> bool:
        conn: asyncpg.Connection = await self.pool.acquire()
        is_success = True
        try:
            async with conn.transaction():
                await conn.execute(self.sql_queries["delete"])
                lst_categories = [
                    (
                        category.name,
                        category.depth,
                        category.ancestor,
                        category.parent,
                        category.path,
                        category.url,
                        category.is_leaf,
                    )
                    for category in categories
                ]
                await conn.executemany(
                    self.sql_queries["insert"],
                    lst_categories,
                )

        except Exception as e:
            logging.error(
                f"[coroutine_id={coroutine_id}]: Can't replace categories: {e}"
            )
            is_success = False
        finally:
            await self.pool.release(conn)

        return is_success

    async def get_by_name(
        self,
        name: str,
        coroutine_id: uuid.UUID = None,
        lock: Lock = None,
    ) -> Optional[Category]:
        if lock is None:
            return await self._get_by_name(name, coroutine_id)
        async with lock:
            return await self._get_by_name(name, coroutine_id)

    async def _get_by_name(
        self,
        name: str,
        coroutine_id: uuid.UUID = None,
    ) -> Optional[Category]:
        conn: asyncpg.Connection = await self.pool.acquire()
        category = None
        try:
            row = await conn.fetchrow(self.sql_queries["get_by_name"], name)
            if row:
                category = Category(
                    id=row["id"],
                    name=row["name"],
                    depth=row["depth"],
                    ancestor=row["ancestor"],
                    parent=row["parent"],
                    path=row["path"],
                    url=row["url"],
                    is_leaf=row["is_leaf"],
                )
        except Exception as e:
            logging.error(
                f"[coroutine_id={coroutine_id}]: Error getting category by name: {e}"
            )
        finally:
            await self.pool.release(conn)

        return category

    async def get_by_depth(
        self,
        depth: int,
        coroutine_id: uuid.UUID = None,
        lock: Lock = None,
    ) -> List[Category]:
        if lock is None:
            return await self._get_by_depth(depth, coroutine_id)
        async with lock:
            return await self._get_by_depth(depth, coroutine_id)

    async def _get_by_depth(
        self,
        depth: int,
        coroutine_id: uuid.UUID = None,
    ) -> List[Category]:
        conn: asyncpg.Connection = await self.pool.acquire()
        categories = []
        try:
            rows = await conn.fetch(self.sql_queries["get_by_depth"], depth)
            categories = [
                Category(
                    id=row["id"],
                    name=row["name"],
                    depth=row["depth"],
                    ancestor=row["ancestor"],
                    parent=row["parent"],
                    path=row["path"],
                    url=row["url"],
                    is_leaf=row["is_leaf"],
                )
                for row in rows
            ]
        except Exception as e:
            logging.error(
                f"[coroutine_id={coroutine_id}]: Error getting categories by depth: {e}"
            )
        finally:
            await self.pool.release(conn)

        return categories

    async def get_by_ancestor(
        self,
        ancestor: str,
        coroutine_id: uuid.UUID = None,
        lock: Lock = None,
    ) -> List[Category]:
        if lock is None:
            return await self._get_by_ancestor(ancestor, coroutine_id)
        async with lock:
            return await self._get_by_ancestor(ancestor, coroutine_id)

    async def _get_by_ancestor(
        self,
        ancestor: str,
        coroutine_id: uuid.UUID = None,
    ) -> List[Category]:
        conn: asyncpg.Connection = await self.pool.acquire()
        categories = []
        try:
            rows = await conn.fetch(self.sql_queries["get_by_ancestor"], ancestor)
            categories = [
                Category(
                    id=row["id"],
                    name=row["name"],
                    depth=row["depth"],
                    ancestor=row["ancestor"],
                    parent=row["parent"],
                    path=row["path"],
                    url=row["url"],
                    is_leaf=row["is_leaf"],
                )
                for row in rows
            ]
        except Exception as e:
            logging.error(
                f"[coroutine_id={coroutine_id}]: Error getting categories by ancestor: {e}"
            )
        finally:
            await self.pool.release(conn)

        return categories

    async def get_by_parent(
        self,
        parent: str,
        coroutine_id: uuid.UUID = None,
        lock: Lock = None,
    ) -> List[Category]:
        if lock is None:
            return await self._get_by_parent(parent, coroutine_id)
        async with lock:
            return await self._get_by_parent(parent, coroutine_id)

    async def _get_by_parent(
        self,
        parent: str,
        coroutine_id: uuid.UUID = None,
    ) -> List[Category]:
        conn: asyncpg.Connection = await self.pool.acquire()
        categories = []
        try:
            rows = await conn.fetch(self.sql_queries["get_by_parent"], parent)
            categories = [
                Category(
                    id=row["id"],
                    name=row["name"],
                    depth=row["depth"],
                    ancestor=row["ancestor"],
                    parent=row["parent"],
                    path=row["path"],
                    url=row["url"],
                    is_leaf=row["is_leaf"],
                )
                for row in rows
            ]
        except Exception as e:
            logging.error(
                f"[coroutine_id={coroutine_id}]: Error getting categories by parent: {e}"
            )
        finally:
            await self.pool.release(conn)

        return categories

    async def get_by_leaf(
        self,
        is_leaf: bool,
        coroutine_id: uuid.UUID = None,
        lock: Lock = None,
    ) -> List[Category]:
        if lock is None:
            return await self._get_by_leaf(is_leaf, coroutine_id)
        async with lock:
            return await self._get_by_leaf(is_leaf, coroutine_id)

    async def _get_by_leaf(
        self,
        is_leaf: bool,
        coroutine_id: uuid.UUID = None,
    ) -> List[Category]:
        conn: asyncpg.Connection = await self.pool.acquire()
        categories = []
        try:
            rows = await conn.fetch(self.sql_queries["get_by_leaf"], is_leaf)
            categories = [
                Category(
                    id=row["id"],
                    name=row["name"],
                    depth=row["depth"],
                    ancestor=row["ancestor"],
                    parent=row["parent"],
                    path=row["path"],
                    url=row["url"],
                    is_leaf=row["is_leaf"],
                )
                for row in rows
            ]
        except Exception as e:
            logging.error(
                f"[coroutine_id={coroutine_id}]: Error getting categories by leaf status: {e}"
            )
        finally:
            await self.pool.release(conn)

        return categories

    async def get_by_ancestors_and_depth(
        self,
        ancestors: List[str],
        depth: int,
        coroutine_id: uuid.UUID = None,
        lock: Lock = None,
    ) -> List[Category]:
        if lock is None:
            return await self._get_by_ancestors_and_depth(
                ancestors, depth, coroutine_id
            )
        async with lock:
            return await self._get_by_ancestors_and_depth(
                ancestors, depth, coroutine_id
            )

    async def _get_by_ancestors_and_depth(
        self,
        ancestors: List[str],
        depth: int,
        coroutine_id: uuid.UUID = None,
    ) -> List[Category]:
        conn: asyncpg.Connection = await self.pool.acquire()
        categories = []
        try:
            rows = await conn.fetch(
                self.sql_queries["get_by_ancestors_and_depth"], ancestors, depth
            )
            categories = [
                Category(
                    id=row["id"],
                    name=row["name"],
                    depth=row["depth"],
                    ancestor=row["ancestor"],
                    parent=row["parent"],
                    path=row["path"],
                    url=row["url"],
                    is_leaf=row["is_leaf"],
                )
                for row in rows
            ]
        except Exception as e:
            logging.error(
                f"[coroutine_id={coroutine_id}]: Error getting categories by ancestors and depth: {e}"
            )
        finally:
            await self.pool.release(conn)

        return categories
