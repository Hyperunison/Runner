import logging
import math
import shlex
import subprocess
from typing import List, Dict, Optional
import xport
import xport.v56

from src.Service.Workflows.OMOPification.BaseDatabaseExporter import BaseDatabaseExporter


class XPTExporter(BaseDatabaseExporter):
    dir: str = "var/"

    def __init__(self):
        super().__init__()

    def create_all_tables(self, tables: List[Dict[str, any]]):
        logging.info("Removing all XPT files from previous exports")
        cmd = "rm -rf " + self.dir + "*.xpt"
        logging.info(cmd)
        args = shlex.split(cmd)
        p = subprocess.run(args, capture_output=True)
        logs = str(p.stdout.decode('utf-8'))
        logging.info(logs)

    NUMERIC_TYPES = {'integer', 'bigint', 'int', 'float', 'double', 'numeric', 'decimal', 'real', 'smallint'}

    def _cast_to_numeric_types(
        self,
        rows: List[Dict[str, str]],
        columns: List[Dict[str, str]]
    ) -> List[Dict[str, any]]:
        col_types = {col['name']: col['type'].lower() for col in columns} if columns else {}
        result = []
        for row in rows:
            typed_row = {}
            for col_name, value in row.items():
                col_type = col_types.get(col_name, '')
                if col_type in self.NUMERIC_TYPES:
                    if value is None or value == '':
                        typed_row[col_name] = math.nan
                    else:
                        try:
                            if col_type in ('float', 'double', 'numeric', 'decimal', 'real'):
                                typed_row[col_name] = float(value)
                            else:
                                typed_row[col_name] = int(value)
                        except (ValueError, TypeError):
                            typed_row[col_name] = value
                else:
                    typed_row[col_name] = value
            result.append(typed_row)
        return result

    def insert_rows(
        self,
        table_name: str,
        rows: List[Dict[str, str]],
        fields_map: Dict[str, Dict[str, str]],
        columns: List[Dict[str, str]]
    ):
        if not rows:
            raise ValueError("No rows provided for export.")

        typed_rows = self._cast_to_numeric_types(rows, columns)
        safe_rows = self._shorten_columns(typed_rows)
        dataset = xport.Dataset(safe_rows, name=table_name[:8])

        xpt_path = f"{self.dir}/{table_name}.xpt"
        with open(xpt_path, "wb") as f:
            xport.v56.dump(dataset, f)

    def write_single_table_dump(self, table_name: str) -> Optional[str]:
        return self.get_export_table_filename(table_name)

    def get_export_table_filename(self, table_name: str) -> Optional[str]:
        return self.dir + "{}.xpt".format(table_name)

    def _safe_shorten(self, name: str, existing: set) -> str:
        short = name[:8]
        i = 1
        while short in existing:
            short = (name[:7] + str(i))[:8]
            i += 1
        existing.add(short)
        return short

    def _shorten_columns(self, rows: List[Dict[str, str]]) -> List[Dict[str, str]]:
        """
        XPT supports only 8-chars long column names and table names
        """
        existing = set()
        name_map = {k: self._safe_shorten(k, existing) for k in rows[0].keys()}
        return [{name_map[k]: v for k, v in row.items()} for row in rows]