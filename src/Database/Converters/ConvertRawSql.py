import sqlglot

class ConvertRawSql:

    def convert_raw_sql(self, raw_sql: str, target_type: str) -> str:
        if target_type == "postgresql":
            return raw_sql
        if target_type == "mysql":
            return sqlglot.transpile(raw_sql, read="postgres", write="mysql")[0]
        if target_type == "mssql":
            return sqlglot.transpile(raw_sql, read="postgres", write="tsql")[0]
        raise ValueError(f"Target type {target_type} not supported")