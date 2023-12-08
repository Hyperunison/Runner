import json
import logging
import datetime
from typing import List, Dict, Tuple
from sqlalchemy.exc import ProgrammingError
from sqlalchemy import create_engine, text
from sqlalchemy.ext.declarative import declarative_base
from src.Api import Api
from src.Message.CohortAPIRequest import CohortAPIRequest
from src.Message.UpdateTableColumnStats import UpdateTableColumnStats
from src.Message.UpdateTableColumnsList import UpdateTableColumnsList
from psycopg2.errors import UndefinedFunction, UndefinedTable

Base = declarative_base()

class SQLJoin:
    table: str
    alias: str
    condition: str

    def __init__(self, _table: str, _alias: str, _condition: str):
        self.table = _table
        self.alias = _alias
        self.condition = _condition


class SQLQuery:
    joins: List[SQLJoin]
    conditions: List[str]
    select: Dict[str, str]

    def __init__(self):
        self.conditions = []
        self.joins = []
        self.select = {}


def escape_string(s: str) -> str:
    return s.replace('\\', '\\\\').replace("'", "\\'")

class VariableMapper:
    map: Dict[str, str] = {
    }

    def convert_var_name(self, var: str) -> str:
        if not self.map.__contains__(var):
            raise Exception("Unknown object var {}".format(var))

        return self.map[var]

    def declare_var(self, ucdm: str, local: str):
        self.map[ucdm] = local


class DataSchema:
    min_count: int = 0
    dst: str = ""

    def __init__(self, dsn: str, min_count: int):
        self.engine = create_engine(dsn).connect()
        self.min_count = min_count
        super().__init__()

    def build_cohort_definition_sql_query(
            self,
            mapper,
            participantTable,
            participantIdField,
            joins,
            where,
            export,
            distribution: bool
    ) -> str:
        logging.info("Cohort request got: {}".format(json.dumps(where)))
        query = SQLQuery()

        for exp in where:
            query.conditions.append(self.build_sql_expression(exp, query, mapper))
        if len(query.conditions) > 0:
            sql_where = "    (" + (")\nAND\n    (".join(query.conditions)) + ")"
        else:
            sql_where = "true"

        select_array: list[str] = []
        for exp in export:
            alias = exp['as'] if 'as' in exp else  query.select[exp['name']]
            query.select[alias] = self.build_sql_expression(exp, query, mapper)
            select_array.append('{} as "{}"'.format(query.select[alias], alias))


        select_string = ", ".join(select_array)

        sql = "SELECT\n    {},\n".format(select_string)

        if distribution:
            sql += "    count(distinct {}.{}) as count\n".format(participantTable, participantIdField)
        else:
            sql += mapper.convert_var_name("{}.{}".format(participantTable, participantIdField))+"\n"

        sql +="FROM {}\n".format(participantTable)

        for j in joins:
            sql += "JOIN {} as {} ON {} \n".format(j['table'], j['alias'], j['on'])

        sql += "WHERE\n{}\n".format(sql_where)
        if distribution:
            sql += "GROUP BY {} \n".format(", ".join(map(str, range(1, len(select_array) + 1)))) + \
                   "HAVING COUNT(distinct {}.{}) >= {}\n".format(participantTable, participantIdField, self.min_count) + \
                   "ORDER BY {}".format(", ".join(map(str, range(1, len(select_array) + 1))))

        logging.info("Generated SQL query: \n{}".format(sql))

        return sql

    def fetch_all(self, sql: str):
        result = self.engine.execute(text(sql)).mappings().all()
        result = [dict(row) for row in result]

        for item in result:
            for key, value in item.items():
                if isinstance(value, datetime.date):
                    item[key] = value.strftime('%Y-%m-%d')

        return result

    def fetch_row(self, sql: str) -> Dict:
        result = self.engine.execute(text(sql)).mappings().all()
        result = [dict(row) for row in result]

        return result[0]

    def execute_cohort_definition(self, cohort_definition: CohortAPIRequest, api: Api):
        key = cohort_definition.cohort_definition['key']
        fields = cohort_definition.cohort_definition['fields']
        participantTable = cohort_definition.cohort_definition['participantTableName']
        participantIdField = cohort_definition.cohort_definition['participantIdField']

        mapper = VariableMapper()
        for ucdm in fields.keys():
            mapper.declare_var(ucdm, fields[ucdm])

        sql = self.build_cohort_definition_sql_query(
            mapper,
            participantTable,
            participantIdField,
            cohort_definition.cohort_definition['join'],
            cohort_definition.cohort_definition['where'],
            cohort_definition.cohort_definition['export'],
            True
        )
        try:
            result = self.fetch_all(sql)
            logging.info("Cohort definition result: {}".format(str(result)))
            api.set_cohort_definition_aggregation(result, sql, cohort_definition.reply_channel, key, cohort_definition.raw_only)
        except ProgrammingError as e:
            logging.error("SQL query error: {}".format(e.orig))
            # rollback transaction to avoid error state in transaction
            self.engine.rollback()
            api.set_cohort_definition_aggregation(
                {},
                "/*\n{}*/\n\n{}\n".format(e.orig, sql),
                cohort_definition.reply_channel,
                key,
                cohort_definition.raw_only
            )

    def get_tables_list(self) -> List[str]:
        sql = "SELECT table_schema || '.' || table_name as tbl FROM information_schema.tables WHERE table_type = 'BASE TABLE'" + \
              "AND table_schema NOT IN ('pg_catalog', 'information_schema');"
        lst = self.fetch_all(sql)
        result: List[str] = []
        for i in lst:
            result.append(i['tbl'])

        return result

    def get_table_columns(self, table_name: str) -> Tuple[int, List[Dict[str, str]]]:
        sql = "select count(*) as cnt from {}".format(table_name)
        count = self.fetch_row(sql)['cnt']

        if '.' in table_name:
            schema, table = table_name.split('.')
            sql = "SELECT column_name, data_type, is_nullable FROM information_schema.columns WHERE table_name = '{}' and table_schema='{}'".format(table, schema)
        else:
            sql = "SELECT column_name, data_type, is_nullable FROM information_schema.columns WHERE table_name = '{}'".format(table_name)

        lst = self.fetch_all(sql)
        columns: List[Dict[str, str]] = []
        for i in lst:
            item: Dict[str, str] = {}
            item['column'] = i['column_name']
            item['type'] = i['data_type']
            item['nullable'] = i['is_nullable'] == 'YES'
            columns.append(item)

        return count, columns

    def update_tables_list(self, api: Api, protected_schemas: List[str], protected_tables: List[str]):
        logging.info("Update tables list packet got")
        tables = self.get_tables_list()
        result: List[str] = []
        for table in tables:
            if table in protected_tables or ('.' in table and table.split('.')[1] in protected_tables):
                logging.debug("Skip table {}, as it's listed in protected_tables".format(table))
                continue
            if '.' in table and table.split('.')[0] in protected_schemas:
                logging.debug("Skip table {}, as it's listed in protected_schemas".format(table))
                continue
            result.append(table)
        api.set_tables_list(result)

    def update_table_columns_list(self, api: Api, message: UpdateTableColumnsList, protected_columns: List[str]):
        table_name = message.table_name
        logging.info("Update tables columns list packet got for table {}".format(table_name))
        rows_count, columns = self.get_table_columns(table_name)
        result: List[str] = []
        types_result: List[str] = []
        nullable_result: List[str] = []
        for column in columns:
            if column in protected_columns:
                logging.debug("Skip column {}.{}, as it's listed in protected_columns".format(table_name, column))
                continue
            result.append(column['column'])
            types_result.append(column['type'])
            nullable_result.append(column['nullable'])
        api.set_table_stats(table_name, rows_count, result, types_result, nullable_result)

    def get_median(self, table: str, column: str, min, max):
        if min is None or max is None:
            return None
        sql = "SELECT PERCENTILE_CONT(0.5) WITHIN GROUP(ORDER BY {column}) as median FROM {table} WHERE {column} > {min} and {column} < {max}".format(column=column, table=table, min=min, max=max)
        return self.fetch_row(sql)['median']
    def update_table_column_stats(self, api: Api, message: UpdateTableColumnStats, min_count, protected_tables: List[str], protected_columns: List[str]):
        table_name = message.table_name
        column_name = message.column_name
        logging.info("Update table column stats packet got for column {}.{}".format(table_name, column_name))

        if table_name in protected_tables or ('.' in table_name and table_name.split('.')[1] in protected_tables):
            logging.error("Skip column {}.{}, as it's listed in protected_tables".format(table_name, column_name))
            return

        if table_name+"."+column_name in protected_columns:
            logging.error("Skip column {}.{}, as it's listed in protected_columns".format(table_name, column_name))
            return

        sql = "SELECT count(distinct \"{v}\") as unique_count from {table}".format(v=column_name, table=table_name)
        try:
            row = self.fetch_row(sql)
        except ProgrammingError as e:
            self.engine.rollback()
            if isinstance(e.orig, UndefinedTable):
                logging.error("Table {} not exists, mark is as abandoned".format(table_name))
                api.set_table_info(table_name, abandoned=True)
                api.set_table_column_stats(table_name, column_name, '', '',
                                           '', '', '', '', '',
                                           '', '', '', '', '')
                return
            else:
                raise e


        unique_count = row['unique_count']

        try:
            sql = "SELECT min(\"{v}\") as min_value, max(\"{v}\") as max_value, avg(\"{v}\") as avg_value from {table}".format(v=column_name, table=table_name)
            row = self.fetch_row(sql)

            min_value = row['min_value']
            max_value = row['max_value']
            avg_value = row['avg_value']
            median50_value = self.get_median(table_name, column_name, min_value, max_value)
            median25_value = self.get_median(table_name, column_name, min_value, median50_value)
            median12_value = self.get_median(table_name, column_name, min_value, median25_value)
            median37_value = self.get_median(table_name, column_name, median25_value, median50_value)
            median75_value = self.get_median(table_name, column_name, median50_value, max_value)
            median63_value = self.get_median(table_name, column_name, median50_value, median75_value)
            median88_value = self.get_median(table_name, column_name, median75_value, max_value)
        except ProgrammingError as e:
            self.engine.rollback()
            if not isinstance(e.orig, UndefinedFunction):
                raise e
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

            sql = "SELECT \"{}\" as value, count(*) as cnt from {} WHERE NOT \"{}\" IS NULL GROUP BY 1 HAVING COUNT(*) > {} ORDER BY 1 DESC LIMIT 10".format(column_name, table_name, column_name, min_count)
            rows = self.fetch_all(sql)
            values = [d['value'] for d in rows]
            counts = [d['cnt'] for d in rows]
            api.set_table_column_values(table_name, column_name, values, counts)


        nulls_count = self.fetch_row("SELECT COUNT(*) as cnt FROM {} WHERE \"{}\" is null".format(table_name, column_name))['cnt']

        api.set_table_column_stats(table_name, column_name, unique_count, nulls_count,
            min_value, max_value, avg_value, median12_value, median25_value, median37_value,
            median50_value, median63_value, median75_value, median88_value)


    def build_sql_expression(self, statement: list, query: SQLQuery, mapper: VariableMapper) -> str:
        logging.debug("Statement got {}".format(json.dumps(statement)))

        if statement['type'] == 'variable':
            logging.debug("VARIABLE {} got".format(statement['name']))
            return mapper.convert_var_name(statement['name'])
        if statement['type'] == 'constant':
            value = json.loads(statement['json'])
            if isinstance(value, int):
                return "{}".format(value)
            if isinstance(value, float):
                return "{}".format(value)
            return "'{}'".format(escape_string(str(value)))

        if statement['type'] == 'binary':
            operator = statement['operator']
            if operator == 'in' or operator == 'not in':
                constants = []
                for const in statement['right']['nodes']:
                    constants.append(self.build_sql_expression(const, query, mapper))
                return "{} {} ({})".format(
                    self.add_staples_around_statement(statement['left'], query, mapper),
                    operator.upper(),
                    ','.join(constants)
                )

            if operator == "==":
                operator = "="

            return '{} {} {}'.format(
                self.add_staples_around_statement(statement['left'], query, mapper),
                operator.upper(),
                self.add_staples_around_statement(statement['right'], query, mapper)
            )

        if statement['type'] == 'unary':
            operator = statement['operator']
            node = statement['node']

            return '{} ({})'.format(
                operator.upper(),
                self.add_staples_around_statement(node, query, mapper),
            )

        if statement['type'] == 'function':
            if statement['name'] == 'ifelse':
                condition = statement['nodes'][0]
                result1 = statement['nodes'][1]
                result2 = statement['nodes'][2]
                return  "\n    CASE \n"+ \
                    "        WHEN " + self.build_sql_expression(condition, query, mapper) +" "+ \
                    "THEN " + self.build_sql_expression(result1, query, mapper) +" \n"+ \
                    "        ELSE " + self.build_sql_expression(result2, query, mapper) +" \n"+ \
                    "    END"
            if statement['name'] == 'hours':
                var = json.loads(statement['nodes'][0]['json'])
                return var * 60 * 60
            if statement['name'] == 'days':
                var = json.loads(statement['nodes'][0]['json'])
                return var * 24 * 60 * 60
            if statement['name'] == 'weeks':
                var = json.loads(statement['nodes'][0]['json'])
                return var * 7 * 24 * 60 * 60
            if statement['name'] == 'months':
                var = json.loads(statement['nodes'][0]['json'])
                return var * 30 * 24 * 60 * 60
            if statement['name'] == 'years':
                var = json.loads(statement['nodes'][0]['json'])
                return var * 365 * 24 * 60 * 60

    def add_staples_around_statement(self, statement, query, mapper: VariableMapper) -> str:
        s = self.build_sql_expression(statement, query, mapper)
        if statement['type'] == 'variable':
            return s
        if statement['type'] == 'constant':
            return s

        return "({})".format(s)


