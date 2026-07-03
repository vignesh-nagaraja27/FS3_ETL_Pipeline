# -*- coding: utf-8 -*-
"""
Database Handler Module
----------------------
Manages safe, thread-safe database connections using a Singleton connection pool
(ThreadedConnectionPool) to prevent database connection exhaustion and server slowdowns.

Utilizes python's context manager pattern to ensure that every connection borrowed from
the pool is strictly returned, even in the event of unexpected exceptions.
"""

import threading

# import psycopg2
from psycopg2.pool import ThreadedConnectionPool
from utils.logger import LOGGER


class DatabaseHandler:
    """
    Singleton class managing the PostgreSQL ThreadedConnectionPool.
    Ensures only a single connection pool is instantiated throughout the system lifetime.
    """

    _instance = None
    _lock = threading.Lock()

    def __new__(cls, *args, **kwargs):
        with cls._lock:
            if not cls._instance:
                cls._instance = super(DatabaseHandler, cls).__new__(cls)
                cls._instance._pool = None
                cls._instance._initialized = False
        return cls._instance

    def initialize(self, db_config: dict, min_conn: int = 2, max_conn: int = 15):
        """
        Initializes the connection pool. Must be called once during startup.

        Args:
            db_config (dict): DB settings containing host, port, dbname, user, password.
            min_conn (int): Minimum number of connections in the pool.
            max_conn (int): Maximum number of connections in the pool.
        """
        with self._lock:
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
                self._initialized = True
                LOGGER.info("Database Connection Pool successfully initialized.")
            except Exception as e:
                LOGGER.error(f"Failed to initialize ThreadedConnectionPool: {e}")
                raise

    def get_connection(self):
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
                "DatabaseHandler connection pool is not initialized. Call initialize() first."
            )

        return ConnectionContext(self._pool)

    def close_all(self):
        """Closes all connections in the pool. Call during system shutdown."""
        with self._lock:
            if self._pool:
                try:
                    self._pool.closeall()
                    self._initialized = False
                    LOGGER.info("All connection pool sessions closed successfully.")
                except Exception as e:
                    LOGGER.error(f"Error closing the connection pool: {e}")


class ConnectionContext:
    """
    Context manager to handle safe connection borrowing, transactions, and pool returns.
    """

    def __init__(self, pool: ThreadedConnectionPool):
        self.pool = pool
        self.conn = None

    def __enter__(self):
        self.conn = self.pool.getconn()
        return self.conn

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.conn:
            try:
                if exc_type is not None:
                    # Rollback transaction on unhandled exception inside the 'with' block
                    self.conn.rollback()
                    LOGGER.warning("Database transaction rolled back due to error.")
                else:
                    # Commit transaction if no error occurred
                    self.conn.commit()
            except Exception as e:
                LOGGER.error(f"Error handling transaction cleanup: {e}")
            finally:
                # Always return connection back to the pool
                self.pool.putconn(self.conn)
