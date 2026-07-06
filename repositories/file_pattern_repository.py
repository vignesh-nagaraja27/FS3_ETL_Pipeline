from db.db_handler import DatabaseHandler


class FilePatternRepository:
    def __init__(self):
        self._db = DatabaseHandler()
        self._patterns = None

    def get_all_active_patterns(self):

        if self._patterns is not None:
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

        with self._db.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql)
                self._patterns = cur.fetchall()

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

        return None
