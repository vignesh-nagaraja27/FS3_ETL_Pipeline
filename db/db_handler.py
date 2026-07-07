# -*- coding: utf-8 -*-
"""
Database Handler Module
----------------------
Manages safe, thread-safe database connections using a Singleton connection pool
(ThreadedConnectionPool) to prevent database connection exhaustion and server slowdowns.

Utilizes Python's context manager pattern to ensure that every connection borrowed from
the pool is strictly returned, even in the event of unexpected exceptions.
"""

import time
import threading

from psycopg2 import OperationalError, InterfaceError
from psycopg2.pool import ThreadedConnectionPool, PoolError
from utils.logger import LOGGER


class DatabaseHandler:
    """
    Singleton class managing the PostgreSQL ThreadedConnectionPool.
    Ensures only a single connection pool is instantiated throughout the system lifetime.
    """

    _instance = None
    _pool = None
    _initialized = False
    _pool_lock: threading.RLock
    _counter_lock: threading.Lock
    _in_use: int
    _max_conn: int
    _debug_pool_stats: bool
    _singleton_lock = threading.Lock()  # Guards ONLY singleton construction

    def __new__(cls, *args, **kwargs):
        with cls._singleton_lock:
            if not cls._instance:
                cls._instance = super(DatabaseHandler, cls).__new__(cls)
                cls._instance._pool = None
                cls._instance._initialized = False
                # Per-instance reentrant lock for pool operations (init / close).
                cls._instance._pool_lock = threading.RLock()
                # Own atomic counter to safely observe in-use connections.
                cls._instance._in_use = 0
                cls._instance._counter_lock = threading.Lock()
                cls._instance._max_conn = 0
                cls._instance._debug_pool_stats = False
        return cls._instance

    def initialize(
        self,
        db_config: dict,
        min_conn: int = 2,
        max_conn: int = 15,
        debug_pool_stats: bool = False,
    ):
        """
        Initializes the connection pool. Must be called once during startup.

        Args:
            db_config (dict): DB settings containing host, port, dbname, user, password.
            min_conn (int): Minimum number of connections in the pool.
            max_conn (int): Maximum number of connections in the pool.
            debug_pool_stats (bool): If True, log pool usage stats on each borrow/return.
        """
        with self._pool_lock:
            if self._initialized:
                return

            try:
                self._pool = ThreadedConnectionPool(
                    minconn=min_conn,
                    maxconn=max_conn,
                    host=db_config.get("host"),
                    port=db_config.get("port", 5432),
                    dbname=db_config.get("dbname"),
                    user=db_config.get("user"),
                    password=db_config.get("password"),
                )
                self._max_conn = max_conn
                self._debug_pool_stats = debug_pool_stats
                self._initialized = True
                LOGGER.info(
                    "Database Connection Pool successfully initialized "
                    f"(min={min_conn}, max={max_conn})."
                )
            except Exception as e:
                LOGGER.error(f"Failed to initialize ThreadedConnectionPool: {e}")
                raise

    def get_connection(self, max_retries: int = 5, base_delay: float = 0.1):
        """
        Retrieves a database connection from the pool.
        Returns a context manager to guarantee connection release.

        Usage:
            db_handler = DatabaseHandler()
            with db_handler.get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT ...")
        """
        if not self._initialized:
            raise RuntimeError(
                "DatabaseHandler connection pool is not initialized. "
                "Call initialize() first."
            )
        if self._pool is None:
            raise RuntimeError("Connection pool is not initialized.")

        return ConnectionContext(
            handler=self,
            pool=self._pool,
            max_retries=max_retries,
            base_delay=base_delay,
        )

    # ---- Internal helpers for safe usage counting -------------------------

    def _incr_in_use(self):
        with self._counter_lock:
            self._in_use += 1
            return self._in_use

    def _decr_in_use(self):
        with self._counter_lock:
            self._in_use = max(0, self._in_use - 1)
            return self._in_use

    def _log_pool_stats(self, label: str):
        """Safe, self-tracked pool stats (does NOT touch psycopg2 internals)."""
        if not self._debug_pool_stats:
            return
        with self._counter_lock:
            in_use = self._in_use
        available = max(0, self._max_conn - in_use)
        LOGGER.info(
            f"[DB POOL] {label} | thread={threading.current_thread().name} "
            f"in_use={in_use} available~={available} max={self._max_conn}"
        )

    def close_all(self):
        """Closes all connections in the pool. Call during system shutdown."""
        with self._pool_lock:
            if self._pool:
                try:
                    self._pool.closeall()
                    LOGGER.info("All connection pool sessions closed successfully.")
                except Exception as e:
                    LOGGER.error(f"Error closing the connection pool: {e}")
                finally:
                    self._pool = None
                    self._initialized = False
                    with self._counter_lock:
                        self._in_use = 0


class ConnectionContext:
    """
    Context manager to handle safe connection borrowing, transactions, and pool returns.

    - Retries getconn() with exponential backoff when the pool is momentarily exhausted.
    - Resets transaction state on borrow to avoid inheriting a poisoned session.
    - Commits on clean exit, rolls back on error.
    - ALWAYS returns the connection; discards (close=True) connections that look broken.
    """

    def __init__(
        self,
        handler: "DatabaseHandler",
        pool: ThreadedConnectionPool,
        max_retries: int = 5,
        base_delay: float = 0.1,
    ):
        self.handler = handler
        self.pool = pool
        self.conn = None
        self.max_retries = max_retries
        self.base_delay = base_delay

    def __enter__(self):
        self.handler._log_pool_stats("BEFORE GET")
        LOGGER.info(f"DB GETCONN REQUEST THREAD={threading.current_thread().name}")

        last_error = None
        for attempt in range(self.max_retries):
            try:
                start = time.time()
                self.conn = self.pool.getconn()
                elapsed = time.time() - start
                LOGGER.info(
                    f"DB GETCONN ACQUIRED "
                    f"THREAD={threading.current_thread().name} "
                    f"WAIT={elapsed:.3f}s"
                )
                break
            except PoolError as e:
                # Pool exhausted: wait with capped exponential backoff, then retry.
                last_error = e
                delay = min(self.base_delay * (2**attempt), 2.0)
                LOGGER.warning(
                    f"DB pool exhausted (attempt {attempt + 1}/{self.max_retries}), "
                    f"retrying in {delay:.2f}s | thread="
                    f"{threading.current_thread().name}"
                )
                time.sleep(delay)
        else:
            LOGGER.error(
                f"DB pool exhausted after {self.max_retries} retries | "
                f"thread={threading.current_thread().name}"
            )
            raise PoolError(
                f"Connection pool exhausted after {self.max_retries} retries"
            ) from last_error

        # Track usage & ensure a clean transaction state before handing over.
        self.handler._incr_in_use()
        try:
            self.conn.rollback()
        except Exception as e:
            LOGGER.warning(f"Could not reset connection state on borrow: {e}")
        self.handler._log_pool_stats("AFTER GET")
        return self.conn

    def __exit__(self, exc_type, exc_val, exc_tb):
        if not self.conn:
            return False  # nothing to release; do not suppress exceptions

        broken = False

        # 1) Commit or rollback based on how the 'with' block exited.
        try:
            if exc_type is not None:
                self.conn.rollback()
                LOGGER.warning("Database transaction rolled back due to error.")
            else:
                self.conn.commit()
        except (OperationalError, InterfaceError) as e:
            # Connection itself is likely dead -> mark for discard.
            broken = True
            LOGGER.error(f"Connection error during commit/rollback: {e}")
        except Exception as e:
            LOGGER.error(f"Error handling transaction cleanup: {e}")

        # 2) ALWAYS return the connection to the pool (discard if broken).
        #    Wrapped so a putconn failure never masks the original exception.
        try:
            self.handler._log_pool_stats("BEFORE RETURN")
            self.pool.putconn(self.conn, close=broken)
            if broken:
                LOGGER.warning("Broken connection discarded and replaced in the pool.")
        except Exception as e:
            LOGGER.error(f"Error returning connection to pool: {e}")
        finally:
            self.conn = None
            self.handler._decr_in_use()
            self.handler._log_pool_stats("AFTER RETURN")

        # Return False -> never suppress exceptions from inside the 'with' block.
        return False
