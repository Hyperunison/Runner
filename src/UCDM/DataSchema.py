import hashlib
import json
import logging
import os
import re
import sys
import time
import signal
import traceback
from typing import List, Dict, Tuple, Optional
from src.Api import Api
from src.Database.Converters.ConvertRawSql import ConvertRawSql
from src.Database.Utils.DsnParser import DsnParser
from src.Message.CohortAPIRequest import CohortAPIRequest
from src.Message.KillCohortAPIRequest import KillCohortAPIRequest
from src.Message.UpdateTableColumnStats import UpdateTableColumnStats
from src.Message.UpdateTableColumnsList import UpdateTableColumnsList
from src.Service.ApiLogger import ApiLogger
from src.Message.partial import CohortDefinition
from src.UCDM.Schema.Labkey import Labkey
from src.UCDM.Schema.Postgres import Postgres
from src.UCDM.Schema.BaseSchema import BaseSchema
from src.UCDM.Schema.SchemaFactory import SchemaFactory


ALLOWED_JOIN_TYPES = {'LEFT', 'INNER'}


def validate_join_type(value: str) -> str:
    jt = value.upper() if value else 'LEFT'
    if jt not in ALLOWED_JOIN_TYPES:
        raise ValueError(f"Invalid joinType: '{value}'. Allowed: {ALLOWED_JOIN_TYPES}")
    return jt


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
    return s.replace('\\', '\\\\').replace("'", "''")


class VariableMapper:
    map: Dict[str, str] = {
    }

    def __init__(self, fields):
        if type(fields).__name__ == "dict":
            for ucdm in fields.keys():
                self.declare_var(ucdm, fields[ucdm])

    def convert_var_name(self, var: str) -> str:
        if not self.map.__contains__(var):
            raise Exception("Unknown object var {}".format(var))

        return self.map[var]

    def declare_var(self, ucdm: str, local: str):
        logging.info("Dealaring var {}: {}".format(ucdm, local))
        self.map[ucdm] = local


class DataProtection:
    """Checks schemas, tables and columns against regex-based protection patterns
    loaded from config['data_protected'].
    """

    def __init__(self, config: Dict):
        self.schemas: List[str] = config.get('schemas', [])
        self.tables: List[str] = config.get('tables', [])
        self.columns: List[str] = config.get('columns', [])

    @staticmethod
    def _matches(name: str, patterns: List[str]) -> bool:
        for pattern in patterns:
            try:
                if re.fullmatch(pattern, name):
                    return True
            except re.error:
                logging.warning("Invalid regex pattern in data_protected: '{}'".format(pattern))
                if pattern == name:
                    return True
        return False

    def is_protected_schema(self, table_name: str) -> bool:
        """Check if the schema part of 'schema.table' is protected."""
        if '.' in table_name:
            return self._matches(table_name.split('.')[0], self.schemas)
        return False

    def is_protected_table(self, table_name: str) -> bool:
        """Check if a table is protected (by full name or bare table name)."""
        if self._matches(table_name, self.tables):
            return True
        if '.' in table_name and self._matches(table_name.split('.')[1], self.tables):
            return True
        return False

    def is_protected_column(self, table_name: str, column_name: str) -> bool:
        """Check if a column is protected (by 'table.column' or bare column name)."""
        full_name = table_name + "." + column_name
        return self._matches(full_name, self.columns) or self._matches(column_name, self.columns)


class DataSchema:
    min_count: int = 0
    dst: str = ""
    schema: BaseSchema
    schema_factory: SchemaFactory

    def __init__(self, dsn: str, min_count: int, materialization_view_prefix: str):
        self.min_count = min_count
        self.materialization_view_prefix = materialization_view_prefix
        self.schema_factory = SchemaFactory()
        self.schema = self.create_schema(dsn, min_count)
        super().__init__()

    def create_schema(self, dsn: str, min_count: int) -> BaseSchema:
        return self.schema_factory.create(
            dsn=dsn,
            min_count=min_count,
        )

    def build_with_cte_list(self, with_tables: Dict) -> Dict[str, str]:
        if len(with_tables) == 0:
            return {}
        result: Dict[str, str] = {}
        for table_name in with_tables:
            sql = ''
            first: bool = True
            for cohort_definition in with_tables[table_name]:
                mapper = VariableMapper(cohort_definition.fields)
                cohort_definition.limit = None
                parts = self.build_cohort_definition_sql_query_internal(
                    mapper,
                    cohort_definition,
                    False,
                    True,
                )
                sql_part = parts[0]
                result = dict(list(result.items()) + list(parts[1].items()))
                if not first:
                    sql += "\nUNION ALL\n"
                first = False
                sql += sql_part
            result[table_name] = sql
        return result

    def build_cohort_definition_sql_query(
            self,
            mapper,
            cohort_definition: CohortDefinition,
            distribution: bool,
            add_participant_id: bool = False,
    ) -> str:
        parts = self.build_cohort_definition_sql_query_internal(mapper, cohort_definition, distribution, add_participant_id)

        cte_part = self.get_cte_sql(parts[1])
        sql = '{}\n{}'.format(cte_part, parts[0]) if cte_part else parts[0]
        sql = self.transform_sql_to_specific_database(sql)

        logging.info("Generated SQL query: \n{}".format(sql))

        return sql

    def build_cohort_definition_sql_query_internal(
            self,
            mapper,
            cohort_definition: CohortDefinition,
            distribution: bool,
            add_participant_id: bool = False,
    ) -> Tuple[str, Dict[str, str]]:
        logging.info("Cohort request got: {}".format(json.dumps(cohort_definition.where)))
        query = SQLQuery()

        cte_list = {}
        materialized_table_map = {}
        for cte in cohort_definition.cte:
            if cte.get('materialized'):
                physical_name = self.materialization_view_prefix + cte['tableName']
                materialized_table_map[cte['tableName']] = physical_name
                # Keep a thin CTE alias so field SQL expressions can still reference the CTE by name
                cte_list[cte['tableName']] = 'SELECT * FROM {}'.format(physical_name)
            else:
                cte_list[cte['tableName']] = cte['cte']

        if len(cohort_definition.with_tables) > 0:
            with_cte_list = self.build_with_cte_list(cohort_definition.with_tables)
        else:
            with_cte_list: Dict[str, str] = {}
        cte_list = dict(list(cte_list.items()) + list(with_cte_list.items()))

        # Ranked CTEs must come after withTables CTEs (they may reference them)
        for ranked_cte in cohort_definition.ranked_cte:
            cte_list[ranked_cte['tableName']] = ranked_cte['cte']

        for exp in cohort_definition.where:
            query.conditions.append(self.build_sql_expression(exp, query, mapper))
        if len(query.conditions) > 0:
            sql_where = "    (" + (")\nAND\n    (".join(query.conditions)) + ")"
        else:
            sql_where = "true"

        select_array: list[str] = []

        if add_participant_id:
            select_array.append("{}.{} as participant_id".format(
                cohort_definition.participant_table,
                cohort_definition.participant_id_field
            ))

        group_array: list[str] = []
        for exp in cohort_definition.export:
            alias = exp['as'] if 'as' in exp else query.select[exp['name']]
            query.select[alias] = self.build_sql_expression(exp, query, mapper)
            select_array.append('{} as "{}"'.format(query.select[alias], alias))
            # Labkey does not support GROUP BY <constant>
            if exp['type'] != 'constant':
                group_array.append('"{}"'.format(alias))
                # group_array.append(self.build_sql_expression(exp, query, mapper))

        select_string = ", ".join(select_array)

        sql = ''

        if distribution:
            sql += "SELECT\n    {},\n".format(select_string)
            sql += "    count(distinct {}.\"{}\") as count_uniq_participants\n".format(
                cohort_definition.participant_table,
                cohort_definition.participant_id_field
            )
        else:
            # distinct is useful, as without participant_id may be a lot of duplicates
            sql += "SELECT\n    DISTINCT {}\n".format(select_string)
            # sql += "{}.{} as participant_id\n".format(participantTable, participantIdField)

        sql += "FROM {}\n".format(cohort_definition.participant_table)

        for j in cohort_definition.joins:
            table = materialized_table_map.get(j['table'], j['table'])
            join_type = validate_join_type(j.get('joinType', 'LEFT'))
            sql += "{} JOIN {} as {} ON {} \n".format(join_type, table, j['alias'], j['on'])

        sql += "WHERE\n{}\n".format(sql_where)
        if distribution and len(group_array) > 0:
            sql += 'GROUP BY {} \n'.format(', '.join(map(str, group_array))) + \
                   "HAVING COUNT(distinct {}.\"{}\") >= {}\n".format(
                       cohort_definition.participant_table,
                       cohort_definition.participant_id_field,
                       self.min_count
                   )

        if not cohort_definition.limit is None:
            sql += "\nLIMIT {}".format(cohort_definition.limit)

        return (sql, cte_list)

    def get_cte_sql(self, cte_list: Dict[str, str]) -> str:
        if len(cte_list) == 0:
            return ''

        sql = 'WITH '

        first: bool = True
        for table_name, cte in cte_list.items():
            if not first:
                sql += ',\n'
            sql += '{} AS ({})'.format(table_name, cte)
            first = False

        return sql

    def transform_sql_to_specific_database(self, sql: str) -> str:
        parser = DsnParser()
        engine_type = parser.get_engine_type(self.schema.dsn)
        converter = ConvertRawSql()
        return converter.convert_raw_sql(sql, engine_type)

    def execute_cohort_definition(self, cohort_api_request: CohortAPIRequest, api: Api):
        key = cohort_api_request.cohort_definition.key
        mapper = VariableMapper(cohort_api_request.cohort_definition.fields)

        sql = self.build_cohort_definition_sql_query(
            mapper,
            cohort_api_request.cohort_definition,
            True,
            False,
        )
        api.set_cohort_sql_query(
            cohort_api_request.cohort_api_request_id,
            sql
        )
        # pid = self.fork(api)
        child_pid = os.getpid()
        api.set_car_status(cohort_api_request.cohort_api_request_id, "process", child_pid)
        logging.info("Processing request in child process: {}".format(child_pid))
        try:
            result = self.schema.fetch_all(sql)
            logging.info("Cohort definition result: {}".format(str(result)))
        except Exception as e:
            logging.error("SQL query error: {}".format(e))
            # rollback transaction to avoid error state in transaction
            self.schema.rollback()
            api.set_cohort_definition_aggregation(
                {},
                "/*\n{}*/\n\n{}\n".format(e, sql),
                cohort_api_request.reply_channel,
                key,
                cohort_api_request.raw_only
            )
            api.set_car_status(cohort_api_request.cohort_api_request_id, "error", child_pid)
            api.set_cohort_error(
                cohort_api_request.cohort_api_request_id,
                "SQL query error: {}".format(e)
            )
            return

        try:
            api.set_cohort_definition_aggregation(
                result,
                sql,
                cohort_api_request.reply_channel,
                key,
                cohort_api_request.raw_only
            )
            api.set_car_status(cohort_api_request.cohort_api_request_id, "success", child_pid)
        except Exception as e:
            logging.error("API error while sending cohort result: {}".format(e))
            api.set_car_status(cohort_api_request.cohort_api_request_id, "error", child_pid)
            api.set_cohort_error(
                cohort_api_request.cohort_api_request_id,
                "API error: {}".format(e)
            )

    def kill_cohort_definition(self, kill_message: KillCohortAPIRequest, api: Api):
        try:
            os.kill(kill_message.pid, signal.SIGKILL)
            done = False
            while not done:
                try:
                    wpid, status = os.waitpid(kill_message.pid, os.WNOHANG)
                    if wpid == 0:
                        time.sleep(1)
                    else:
                        done = True
                except ChildProcessError:
                    done = True
            if done:
                api.set_car_status(kill_message.cohort_api_request_id, "killed")
                logging.info(f"Process with PID {kill_message.pid} killed successfully.")
        except OSError as e:
            logging.error(f"Error while killing PID {kill_message.pid} : {e}")
            api.set_car_status(kill_message.cohort_api_request_id, "error")

    def build_sql_expression(self, statement: list, query: SQLQuery, mapper: VariableMapper) -> str:
        logging.debug("Statement got {}".format(json.dumps(statement)))

        statement = self.schema.statement_callback(statement)

        if statement['type'] == 'variable':
            logging.debug("VARIABLE {} got".format(statement['name']))
            return mapper.convert_var_name(statement['name'])
        if statement['type'] == 'constant':

            value = json.loads(statement['json'])
            if isinstance(value, int):
                return "{}".format(value)
            if isinstance(value, float):
                return "{}".format(value)
            if value is None:
                return "null"
            return "'{}'".format(escape_string(str(value)))

        if statement['type'] == 'array':
            expressions = []
            for const in statement['nodes']:
                expressions.append(self.build_sql_expression(const, query, mapper))
            return '({})'.format(', '.join(expressions))
        if statement['type'] == 'binary':
            operator = statement['operator']
            if operator == 'in' or operator == 'not in':
                right = self.build_sql_expression(statement['right'], query, mapper)
                if right == '()':
                    right = '(null)'
                return "{} {} {}".format(
                    self.add_staples_around_statement(statement['left'], query, mapper),
                    operator.upper(),
                    right,
                )

            if operator == "==":
                operator = "="

            return '{} {} {}'.format(
                self.add_staples_around_statement(statement['left'], query, mapper),
                operator.upper(),
                self.add_staples_around_statement(statement['right'], query, mapper)
            )

        if statement['type'] in ('not_exists', 'exists'):
            joins = statement.get('join', [])
            sub_wheres = statement.get('where', [])
            if not joins:
                logging.warning("{} block has no join info, skipping".format(statement['type']))
                return "true"
            prefix = "NOT EXISTS" if statement['type'] == 'not_exists' else "EXISTS"
            primary = joins[0]
            from_clause = "{} as {}".format(primary['table'], primary['alias'])
            extra_joins = ""
            for j in joins[1:]:
                sub_join_type = validate_join_type(j.get('joinType', 'LEFT'))
                extra_joins += " {} JOIN {} as {} ON {} ".format(sub_join_type, j['table'], j['alias'], j['on'])
            sub_conditions = [primary['on']]
            for w in sub_wheres:
                sub_conditions.append(self.build_sql_expression(w, query, mapper))
            sub_where = " AND ".join(sub_conditions)
            return "{} (SELECT 1 FROM {}{} WHERE {})".format(
                prefix, from_clause, extra_joins, sub_where
            )

        if statement['type'] == 'unary':
            operator = statement['operator']
            node = statement['node']

            return '{} ({})'.format(
                operator.upper(),
                self.add_staples_around_statement(node, query, mapper),
            )

        if statement['type'] == 'function':
            logging.info('Function call got: {}'.format(statement['name']))
            if statement['name'] == 'ifelse':
                condition = statement['nodes'][0]
                result1 = statement['nodes'][1]
                result2 = statement['nodes'][2]
                return "\n    CASE \n" + \
                    "        WHEN " + self.build_sql_expression(condition, query, mapper) + " " + \
                    "THEN " + self.build_sql_expression(result1, query, mapper) + " \n" + \
                    "        ELSE " + self.build_sql_expression(result2, query, mapper) + " \n" + \
                    "    END"
            if statement['name'] in ['hours', 'days', 'weeks', 'months', 'years']:
                sql_exp = self.build_sql_expression(statement['nodes'][0], query, mapper)
                return self.schema.sql_expression_interval(
                    sql_exp,
                    statement['name']
                )

            if statement['name'] in ['date', 'datetime', 'real', 'bigint', 'varchar', 'timestamp']:
                return self.schema.sql_expression_cast_data_type(
                    self.build_sql_expression(statement['nodes'][0], query, mapper),
                    statement['name']
                )

            # known functions
            if statement['name'] in self.schema.known_functions:
                sql = statement['name'] + "(" + ", ".join(
                    str(self.build_sql_expression(node, query, mapper)) for node in statement['nodes']) + ")"
                return sql

            raise NotImplementedError("Unknown function {}".format(statement['name']))

    def add_staples_around_statement(self, statement, query, mapper: VariableMapper) -> str:
        s = self.build_sql_expression(statement, query, mapper)
        if statement['type'] == 'variable':
            return s
        if statement['type'] == 'constant':
            return s

        return "({})".format(s)

    def fetch_all(self, sql: str):
        return self.schema.fetch_all(sql)

    def fetch_chunks(self, sql: str, chunk_size: int):
        return self.schema.fetch_chunks(sql, chunk_size)

    def update_tables_list(self, api: Api, protection: DataProtection):
        logging.info("Update tables list packet got")
        tables = self.schema.get_tables_list()
        result: List[str] = []
        for table in tables:
            if protection.is_protected_table(table):
                logging.debug("Skip table {}, as it matches protected_tables".format(table))
                continue
            if protection.is_protected_schema(table):
                logging.debug("Skip table {}, as it matches protected_schemas".format(table))
                continue
            result.append(table)
        api.set_tables_list(result)

    def update_table_columns_list(self, api: Api, message: UpdateTableColumnsList, protection: DataProtection):
        table_name = message.table_name
        cte = message.cte
        materialized_as = getattr(message, 'materialized_as', None)
        with_cte_label = 'with CTE' if cte else ''

        if protection.is_protected_table(table_name):
            logging.error("Skip table {} {}, as it matches protected_tables".format(table_name, with_cte_label))
            return

        if protection.is_protected_schema(table_name):
            logging.error("Skip table {} {}, as it matches protected_schemas".format(table_name, with_cte_label))
            return

        if materialized_as:
            logging.info("Update table columns list for materialized table {} -> {}".format(table_name, materialized_as))
            rows_count, columns = self.schema.get_table_columns(materialized_as)
        elif cte:
            logging.info("Update tables columns list packet got for complex expression alias = {}, with CTE".format(table_name))
            rows_count, columns = self.schema.get_cte_columns(table_name, cte)
        else:
            logging.info("Update tables columns list packet got for table {}".format(table_name))
            rows_count, columns = self.schema.get_table_columns(table_name)
        result: List[str] = []
        types_result: List[str] = []
        nullable_result: List[str] = []
        for column in columns:
            if protection.is_protected_column(table_name, column['column']):
                logging.debug("Skip column {}.{}, as it matches protected_columns".format(table_name, column['column']))
                continue
            result.append(column['column'])
            types_result.append(column['type'])
            nullable_result.append(column['nullable'])
        api.set_table_stats(table_name, rows_count, result, types_result, nullable_result)

    def materialize_cte(self, api: Api, message):
        table_name = message.table_name
        materialized_as = self.materialization_view_prefix + table_name
        cte = message.cte
        indexes = message.indexes
        job_id = message.job_id
        biobank_data_table_id = message.biobank_data_table_id

        logging.info("Materializing CTE {} as {}".format(table_name, materialized_as))
        try:
            stored_checksum = self.schema.get_materialized_view_definition(materialized_as)
            new_checksum = hashlib.md5(cte.strip().encode()).hexdigest()
            if stored_checksum is not None:
                if stored_checksum == new_checksum:
                    sql = "REFRESH MATERIALIZED VIEW {}".format(materialized_as)
                    logging.info("SQL: {}".format(sql))
                    self.schema.refresh_materialized_view(materialized_as)
                    logging.info("Materialized view {} refreshed (SQL unchanged)".format(materialized_as))
                else:
                    logging.info("Materialized view {} SQL changed, recreating".format(materialized_as))
                    drop_sql = "DROP MATERIALIZED VIEW {}".format(materialized_as)
                    logging.info("SQL: {}".format(drop_sql))
                    self.schema.drop_materialized_view(materialized_as)
                    create_sql = "CREATE MATERIALIZED VIEW {} AS ({})".format(materialized_as, cte)
                    logging.info("SQL: {}".format(create_sql))
                    self.schema.create_materialized_view(materialized_as, cte)
                    logging.info("Materialized view {} recreated successfully".format(materialized_as))
            else:
                create_sql = "CREATE MATERIALIZED VIEW {} AS ({})".format(materialized_as, cte)
                logging.info("SQL: {}".format(create_sql))
                self.schema.create_materialized_view(materialized_as, cte)
                logging.info("Materialized view {} created successfully".format(materialized_as))

            for index_def in indexes:
                index_name = index_def.get('name')
                column = index_def.get('column')
                index_sql = "CREATE INDEX IF NOT EXISTS {} ON {} (\"{}\")".format(index_name, materialized_as, column)
                logging.info("SQL: {}".format(index_sql))
                try:
                    self.schema.execute_sql(index_sql)
                    logging.info("Index {} on {}.{} created".format(index_name, materialized_as, column))
                except Exception as e:
                    logging.warning("Index {} failed, skipping: {}".format(index_name, e))

            api.set_cte_materialization_status(biobank_data_table_id, job_id, 'success')
        except Exception as e:
            logging.error("Materialization of {} failed: {}".format(materialized_as, e))
            api.add_job_logs(message.runner_message_id, traceback.format_exc())
            api.set_cte_materialization_status(biobank_data_table_id, job_id, 'error')
            raise e

    def update_table_column_stats(self, api: Api, message: UpdateTableColumnStats, min_count,
                                  protection: DataProtection):
        table_name = message.table_name
        column_name = message.column_name
        with_cte_label = 'with CTE' if message.cte else ''
        logging.info("Update table column stats packet got for column {}.{} {}".format(
            table_name,
            column_name,
            with_cte_label
        ))

        if protection.is_protected_schema(table_name):
            logging.error("Skip column {}.{} {}, as it matches protected_schemas".format(
                table_name, column_name, with_cte_label))
            return

        if protection.is_protected_table(table_name):
            logging.error("Skip column {}.{} {}, as it matches protected_tables".format(
                table_name, column_name, with_cte_label))
            return

        if protection.is_protected_column(table_name, column_name):
            logging.error("Skip column {}.{} {}, as it matches protected_columns".format(
                table_name, column_name, with_cte_label))
            return
        stat = self.schema.get_table_column_stats(table_name, column_name, message.cte)

        api.set_table_info(stat.table_name, stat.abandoned)
        if stat.abandoned:
            logging.error("Table {} {} not exists, mark is as abandoned".format(table_name, with_cte_label))
            return
        if stat.values:
            values = [d['value'] for d in stat.values]
            counts = [d['cnt'] for d in stat.values]
            logging.info("Sending frequent values for {}.{} {}: {}".format(
                table_name,
                column_name,
                with_cte_label,
                ','.join([str(val) for val in values])
            ))
            api.set_table_column_values(stat.table_name, stat.column_name, values, counts)
        else:
            logging.info("No frequent values found for {}.{} {}".format(table_name, column_name, with_cte_label))
        api.set_table_column_stats(stat.table_name, stat.column_name,
                                   stat.unique_count, stat.nulls_count,
                                   stat.min_value, stat.max_value, stat.avg_value,
                                   stat.median12_value, stat.median25_value, stat.median37_value, stat.median50_value,
                                   stat.median63_value, stat.median75_value, stat.median88_value)

    def reconnect(self):
        self.schema.reconnect()

    def execute_sql(self, sql: str):
        self.schema.execute_sql(sql)

    def execute_sql_params(self, sql: str, params) -> None:
        self.schema.execute_sql_params(sql, params)
