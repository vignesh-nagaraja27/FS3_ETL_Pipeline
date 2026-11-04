import threading
import time
from db.db_handler import DatabaseHandler


class FilePatternRepository:
    # Refresh active file patterns from DB every 30 seconds so newly added rules
    # in fs3.pre_m_file_pattern take effect without restarting the ETL service.
    CACHE_TTL_SEC = 30.0

    def __init__(self):
        self._db = DatabaseHandler()
        self._patterns = None
        self._last_loaded_at = 0.0
        self._lock = threading.Lock()

    def get_all_active_patterns(self, force_refresh: bool = False):
        now = time.monotonic()
        if (
            not force_refresh
            and self._patterns is not None
            and (now - self._last_loaded_at) < self.CACHE_TTL_SEC
        ):
            return self._patterns

        sql = """
            SELECT
                machine_type,
                file_pattern,
                vehicle_model,
                parts_code,
                side,
                op_no,
                vpo_code
            FROM fs3.pre_m_file_pattern
            WHERE active_flag = 'Y'
            ORDER BY LENGTH(file_pattern) DESC
        """

        with self._lock:
            now = time.monotonic()
            if (
                not force_refresh
                and self._patterns is not None
                and (now - self._last_loaded_at) < self.CACHE_TTL_SEC
            ):
                return self._patterns

            with self._db.get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(sql)
                    self._patterns = cur.fetchall()
            self._last_loaded_at = time.monotonic()

        return self._patterns

    def find_match(self, filename):

        for row in self.get_all_active_patterns():
            machine_type = row[0]
            file_pattern = row[1]

            if filename.startswith(file_pattern):
                return {
                    "machine_type": machine_type,
                    "file_pattern": file_pattern,
                    "vehicle_model": row[2],
                    "parts_code": row[3],
                    "side": row[4],
                    "op_no": row[5],
                    "vpo_code": row[6],
                }

        parts = filename.rsplit(".", 1)[0].split("_")

        if len(parts) >= 3:
            vehicle_model = parts[0].strip()
            parts_code = parts[1].strip()
            op_no = parts[2].strip()

            if (
                vehicle_model
                and parts_code
                and op_no
                and op_no.upper().startswith("OP")
            ):
                return {
                    "machine_type": "3D_HD",
                    "file_pattern": None,
                    "vehicle_model": vehicle_model,
                    "parts_code": parts_code,
                    "side": None,
                    "op_no": op_no,
                    "vpo_code": f"{vehicle_model}_{parts_code}_{op_no}",
                }

        return None
