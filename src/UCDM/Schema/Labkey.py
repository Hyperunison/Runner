import logging
import os
from typing import Dict, List, Tuple
from sqlalchemy.exc import ProgrammingError
from .BaseSchema import BaseSchema
from labkey.api_wrapper import APIWrapper
from ..TableStat import TableStat
from sqlalchemy import text
from urllib.parse import urlparse

class Labkey (BaseSchema):
    type = 'labkey'
    schema = 'lists'
    manager_schema = 'ListManager'
    credentials_path = '/root/.netrc'

    known_functions = ['timestampadd']

    def __init__(self, dsn: str, min_count: int):
        parsed_dsn = urlparse(dsn)
        labkey_server = parsed_dsn.hostname
        project_name = parsed_dsn.path.split('/')[1]
        user = parsed_dsn.username
        password = parsed_dsn.password
        with open(self.credentials_path, "w+") as f:
            f.write("machine {}\nlogin {}\npassword {}\n".format(labkey_server, user, password))
        os.chmod(self.credentials_path, 0o400)
        self.engine = APIWrapper(labkey_server, project_name, "labkey", use_ssl=True)
        super().__init__(dsn, min_count)

    def get_tables_list(self) -> List[str]:
        rows = self.engine.query.select_rows(self.manager_schema, self.manager_schema, columns=['*'], max_rows=100000, include_total_count=True)
        result: List[str] = []
        for i in rows['rows']:
            result.append(i['Name'])

        return result

    def get_table_columns(self, table_name: str) -> Tuple[int, List[Dict[str, str]]]:
        rows = self.engine.query.select_rows(self.schema, table_name, columns=['*'], max_rows=1, include_total_count=True)
        fields = rows['metaData']['fields']
        count = rows['rowCount']
        columns: List[Dict[str, str]] = []
        for i in fields:
            item: Dict[str, str] = {}
            item['column'] = i['name']
            item['type'] = i['type']
            item['nullable'] = i['isNullable'] == 'YES'
            columns.append(item)
        return count, columns

    def get_cte_columns(self, table_name: str, cte: str) -> Tuple[int, List[Dict[str, str]]]:
        sql_columns = "WITH {} AS ({}) SELECT * from {} LIMIT 1".format(table_name, cte, table_name)
        rows = self.engine.query.execute_sql(self.schema, sql=sql_columns)
        fields = rows['metaData']['fields']
        count = rows['rowCount']
        columns: List[Dict[str, str]] = []
        for i in fields:
            item: Dict[str, str] = {}
            item['column'] = i['name']
            item['type'] = i['type']
            item['nullable'] = i['isNullable'] == 'YES'
            columns.append(item)
        return count, columns

    def get_table_column_stats(self, table_name: str, column_name: str, cte: str) -> TableStat:
        with_cte_label = 'with CTE' if cte else ''
        try:
            sql = "SELECT count(distinct \"{v}\") as unique_count from {table}".format(v=column_name, table=table_name)
            sql = self.wrap_sql_by_cte(sql, table_name, cte)
            result = self.engine.query.execute_sql(self.schema, sql=sql)
            unique_count = result['rows'][0]['unique_count']
        except ProgrammingError as e:
            stat = TableStat()
            stat.table_name = table_name
            stat.column_name = column_name
            stat.abandoned = True
            return stat

        try:
            sql = "SELECT min(\"{v}\") as min_value, max(\"{v}\") as max_value, avg(\"{v}\") as avg_value from {table}".format(v=column_name, table=table_name)
            sql = self.wrap_sql_by_cte(sql, table_name, cte)
            logging.info(sql)
            result = self.engine.query.execute_sql(self.schema, sql=sql)

            min_value = result['rows'][0]['min_value']
            max_value = result['rows'][0]['max_value']
            avg_value = result['rows'][0]['avg_value']
            median50_value = self.get_median(table_name, column_name, min_value, max_value, cte)
            median25_value = self.get_median(table_name, column_name, min_value, median50_value, cte)
            median12_value = self.get_median(table_name, column_name, min_value, median25_value, cte)
            median37_value = self.get_median(table_name, column_name, median25_value, median50_value, cte)
            median75_value = self.get_median(table_name, column_name, median50_value, max_value, cte)
            median63_value = self.get_median(table_name, column_name, median50_value, median75_value, cte)
            median88_value = self.get_median(table_name, column_name, median75_value, max_value, cte)
            values_counts = []
        except Exception as e:
            logging.debug("Can't get min/max values for {}.{} {}".format(table_name, column_name, with_cte_label))
            min_value=None
            max_value=None
            avg_value=None
            median50_value=None
            median25_value=None
            median12_value=None
            median37_value=None
            median75_value=None
            median63_value=None
            median88_value=None
            pass

        sql = "SELECT \"{column}\" as value, count(*) as cnt from {table} WHERE NOT \"{column}\" IS NULL GROUP BY \"{column}\" HAVING COUNT(*) >= {min} ORDER BY 1 DESC LIMIT 100".format(
            column=column_name, table=table_name, min=self.min_count)
        sql = self.wrap_sql_by_cte(sql, table_name, cte)
        logging.debug(sql)
        values_counts = self.fetch_all(sql)
        logging.info("Frequent values counts for {}.{} {}: {}".format(
            table_name,
            column_name,
            with_cte_label,
            (values_counts)
        ))

        nulls_count_sql = "SELECT COUNT(*) as cnt FROM {} WHERE \"{}\" is null".format(table_name, column_name)
        nulls_count_sql = self.wrap_sql_by_cte(nulls_count_sql, table_name, cte)
        nulls_count = self.fetch_row(nulls_count_sql)['cnt']

        stat = TableStat()
        stat.table_name = table_name
        stat.column_name = column_name
        stat.abandoned = False
        stat.unique_count = unique_count
        stat.nulls_count = nulls_count
        stat.min_value = min_value
        stat.max_value = max_value
        stat.avg_value = avg_value
        stat.median12_value = median12_value
        stat.median25_value = median25_value
        stat.median37_value = median37_value
        stat.median50_value = median50_value
        stat.median63_value = median63_value
        stat.median75_value = median75_value
        stat.median88_value = median88_value
        stat.values = values_counts
        return stat

    def get_median(self, table: str, column: str, min, max, cte: str):
        if min is None or max is None:
            return None
        sql = "SELECT median({column}) as med  FROM {table} WHERE {column} > {min} and {column} < {max}".format(
            column=column,
            table=table,
            min=min,
            max=max
        )
        sql = self.wrap_sql_by_cte(sql, table, cte)
        return self.fetch_row(sql)['med']

    def fetch_row(self, sql: str) -> Dict:
        result = self.engine.query.execute_sql(self.schema, text(sql))
        result = [dict(row) for row in result['rows']]
        return result[0]

    def fetch_all(self, sql: str):
        result = self.engine.query.execute_sql(self.schema, text(sql))
        result = [dict(row) for row in result['rows']]
        return result

    def rollback(self):
        return

    def reconnect(self):
        return

    def sql_expression_interval(self, count: str, unit: str) -> str:
        raise NotImplementedError("Interval type is not exists in LabKey")

    def sql_expression_cast_data_type(self, expression: str, data_type: str) -> str:
        return "CAST({} AS {})".format(expression, data_type)

    def statement_callback(self, statement) -> Dict:
        date_functions = ['hours', 'days', 'weeks', 'months', 'years']
        if statement['type'] == 'binary' and statement['operator'] in ["+", "-"]:
            # Catching adding interval to date/datetime
            if (statement['right']['type'] == 'function' and statement['right']['name'] in date_functions) or (statement['left']['type'] == 'function' and statement['left']['name'] in date_functions):
                interval_map = dict()
                interval_map['hours'] = 'SQL_TSI_HOUR'
                interval_map['days'] = 'SQL_TSI_DAY'
                interval_map['weeks'] = 'SQL_TSI_WEEK'
                interval_map['months'] = 'SQL_TSI_MONTH'
                interval_map['years'] = 'SQL_TSI_YEAR'
                if statement['right']['type'] == 'function' and statement['right']['name'] in date_functions:
                    interval = statement['right']
                    date = statement['left']
                else:
                    interval = statement['left']
                    date = statement['right']

                result = dict()
                result['type'] = 'function'
                result['name'] = 'timestampadd'

                node1 = dict()
                node1['type'] = 'constant'
                node1['json'] = '"'+interval_map[interval['name']]+'"'

                node2 = dict()
                node2['type'] = 'constant'
                if statement['operator'] == '+':
                    node2['json'] = interval['nodes'][0]['json']
                else:
                    node2['json'] = "-" + interval['nodes'][0]['json']

                result['nodes'] = [node1, node2, date]

            return result
        else:
            return statement