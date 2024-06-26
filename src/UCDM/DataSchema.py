import json
import logging
import os
import sys
import time
import signal
from typing import List, Dict, Tuple, Optional
from src.Api import Api
from src.Message.CohortAPIRequest import CohortAPIRequest
from src.Message.KillCohortAPIRequest import KillCohortAPIRequest
from src.Message.UpdateTableColumnStats import UpdateTableColumnStats
from src.Message.UpdateTableColumnsList import UpdateTableColumnsList
from src.UCDM.Schema.Labkey import Labkey
from src.UCDM.Schema.Postgres import Postgres
from src.UCDM.Schema.BaseSchema import BaseSchema


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


class DataSchema:
    min_count: int = 0
    dst: str = ""
    schema: BaseSchema

    def __init__(self, dsn: str, schema: str, min_count: int):
        self.min_count = min_count
        self.schema = self.create_schema(dsn, schema, min_count)
        super().__init__()

    def create_schema(self, dsn: str, schema: str, min_count: int) -> BaseSchema:
        if schema == 'labkey':
            return Labkey(dsn, min_count)
        if schema == 'postgres':
            return Postgres(dsn, min_count)
        raise Exception("Unknown schema {}".format(schema))

    def build_with_sql(self, with_tables: Dict) -> str:
        if len(with_tables) == 0:
            return ''
        first_table: bool = True
        sql: str = "WITH "
        for table_name in with_tables:
            if not first_table:
                sql += ", "
            first_table = False
            sql += "{} AS (\n".format(table_name)
            first: bool = True
            for cohort_definition in with_tables[table_name]:
                mapper = VariableMapper(cohort_definition['fields'])
                sql_part = self.build_cohort_definition_sql_query(
                    mapper,
                    cohort_definition['participantTableName'],
                    cohort_definition['participantIdField'],
                    cohort_definition['join'],
                    cohort_definition['where'],
                    cohort_definition['export'],
                    None,
                    cohort_definition['withTables'],
                    False,
                    True,
                )
                if not first:
                    sql += "\nUNION ALL\n"
                first = False
                sql += sql_part
            sql += ")\n"
        return sql

    def build_cohort_definition_sql_query(
            self,
            mapper,
            participantTable,
            participantIdField,
            joins,
            where,
            export,
            limit: Optional[int],
            with_tables: any,
            distribution: bool,
            add_participant_id: bool = False,
    ) -> str:
        logging.info("Cohort request got: {}".format(json.dumps(where)))
        query = SQLQuery()

        if isinstance(with_tables, dict):
            with_sql = self.build_with_sql(with_tables)
        else:
            with_sql = ''

        for exp in where:
            query.conditions.append(self.build_sql_expression(exp, query, mapper))
        if len(query.conditions) > 0:
            sql_where = "    (" + (")\nAND\n    (".join(query.conditions)) + ")"
        else:
            sql_where = "true"

        select_array: list[str] = []
        group_array: list[str] = []
        for exp in export:
            alias = exp['as'] if 'as' in exp else query.select[exp['name']]
            query.select[alias] = self.build_sql_expression(exp, query, mapper)
            select_array.append('{} as "{}"'.format(query.select[alias], alias))
            if exp['type'] != 'constant':
                # Labkey does not support GROUP BY <constant>
                group_array.append(alias)

        select_string = ", ".join(select_array)

        sql = with_sql + "\n\n"

        if add_participant_id:
            select_string += " , {}.{} as participant_id".format(participantTable, participantIdField)

        if distribution:
            sql += "SELECT\n    {},\n".format(select_string)
            sql += "    count(distinct {}.\"{}\") as count_uniq_participants\n".format(participantTable, participantIdField)
        else:
            # distinct is useful, as without participant_id may be a lot of duplicates
            sql += "SELECT\n    DISTINCT {}\n".format(select_string)
            # sql += "{}.{} as participant_id\n".format(participantTable, participantIdField)

        sql += "FROM {}\n".format(participantTable)

        for j in joins:
            sql += "JOIN {} as {} ON {} \n".format(j['table'], j['alias'], j['on'])

        sql += "WHERE\n{}\n".format(sql_where)
        if distribution and len(group_array) > 0:
            sql += 'GROUP BY "{}" \n'.format('", "'.join(map(str, group_array))) + \
                   "HAVING COUNT(distinct {}.\"{}\") >= {}\n".format(participantTable, participantIdField,
                                                                     self.min_count)

        if not limit is None:
            sql += "\nLIMIT {}".format(limit)
        logging.info("Generated SQL query: \n{}".format(sql))

        return sql

    def fork(self, api: Api) -> int:
        pid = os.fork()
        logging.info("Forked, pid={}".format(pid))

        # if pid == 0:
        #     # child process
        #     try:
        #         logging.info("Tty start debugging")
        #         import pydevd_pycharm
        #         pydevd_pycharm.settrace('host.docker.internal', port=55147, stdoutToServer=True, stderrToServer=True)
        #         logging.info("Debug server connection established for pid {}".format(pid))
        #     except Exception as e:
        #         logging.info("Debug server connection was not established for pid {}, error {}".format(pid, e))
        #         pass

        api.api_instance.api_client.close()
        api.api_instance.api_client.rest_client.pool_manager.clear()
        self.schema.reconnect()

        logging.info("Returning pid {}".format(pid))

        return pid

    def execute_cohort_definition(self, cohort_definition: CohortAPIRequest, api: Api):
        key = cohort_definition.cohort_definition['key']
        participantTable = cohort_definition.cohort_definition['participantTableName']
        participantIdField = cohort_definition.cohort_definition['participantIdField']

        mapper = VariableMapper(cohort_definition.cohort_definition['fields'])

        sql = self.build_cohort_definition_sql_query(
            mapper,
            participantTable,
            participantIdField,
            cohort_definition.cohort_definition['join'],
            cohort_definition.cohort_definition['where'],
            cohort_definition.cohort_definition['export'],
            cohort_definition.cohort_definition['limit'],
            cohort_definition.cohort_definition['withTables'],
            True,
            False,
        )
        api.set_cohort_sql_query(
            cohort_definition.cohort_api_request_id,
            sql
        )
        pid = self.fork(api)
        if pid != 0:
            # Master process, continue working
            return
        child_pid = os.getpid()
        logging.info("Processing request in child process: {}".format(child_pid))
        try:
            api.set_car_status(cohort_definition.cohort_api_request_id, "process", child_pid)
            result = self.schema.fetch_all(sql)
            logging.info("Cohort definition result: {}".format(str(result)))
            api.set_cohort_definition_aggregation(
                result,
                sql,
                cohort_definition.reply_channel,
                key,
                cohort_definition.raw_only
            )
            api.set_car_status(cohort_definition.cohort_api_request_id, "success", child_pid)
        except Exception as e:
            logging.error("SQL query error: {}".format(e))
            # rollback transaction to avoid error state in transaction
            self.schema.rollback()
            api.set_cohort_definition_aggregation(
                {},
                "/*\n{}*/\n\n{}\n".format(e, sql),
                cohort_definition.reply_channel,
                key,
                cohort_definition.raw_only
            )
            api.set_car_status(cohort_definition.cohort_api_request_id, "error", child_pid)
            api.set_cohort_error(
                cohort_definition.cohort_api_request_id,
                "SQL query error: {}".format(e)
            )
        finally:
            logging.debug("Exiting child process {}".format(child_pid))
            sys.exit(0)

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
                print(f"Process with PID {kill_message.pid} killed successfully.")
        except OSError as e:
            print(f"Error while killing PID {kill_message.pid} : {e}")
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
                count = json.loads(statement['nodes'][0]['json'])
                return self.schema.sql_expression_interval(count, statement['name'])

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

    def update_tables_list(self, api: Api, protected_schemas: List[str], protected_tables: List[str]):
        logging.info("Update tables list packet got")
        tables = self.schema.get_tables_list()
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
        cte = message.cte
        if cte:
            logging.info("Update tables columns list packet got for complex expression alias = {}, with CTE".format(table_name))
            rows_count, columns = self.schema.get_cte_columns(table_name, cte)
        else:
            logging.info("Update tables columns list packet got for table {}".format(table_name))
            rows_count, columns = self.schema.get_table_columns(table_name)
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

    def update_table_column_stats(self, api: Api, message: UpdateTableColumnStats, min_count,
                                  protected_tables: List[str], protected_columns: List[str]):
        table_name = message.table_name
        column_name = message.column_name
        logging.info("Update table column stats packet got for column {}.{}".format(table_name, column_name))

        if table_name in protected_tables or ('.' in table_name and table_name.split('.')[1] in protected_tables):
            logging.error("Skip column {}.{}, as it's listed in protected_tables".format(table_name, column_name))
            return

        if table_name + "." + column_name in protected_columns:
            logging.error("Skip column {}.{}, as it's listed in protected_columns".format(table_name, column_name))
            return
        stat = self.schema.get_table_column_stats(table_name, column_name)

        api.set_table_info(stat.table_name, stat.abandoned)
        if stat.abandoned:
            logging.error("Table {} not exists, mark is as abandoned".format(table_name))
            return
        if stat.values:
            values = [d['value'] for d in stat.values]
            counts = [d['cnt'] for d in stat.values]
            logging.info("Sending frequent values for {}.{}: {}".format(table_name, column_name,
                                                                        ','.join([str(val) for val in values])))
            api.set_table_column_values(stat.table_name, stat.column_name, values, counts)
        else:
            logging.info("No frequent values found for {}.{}".format(table_name, column_name))
        api.set_table_column_stats(stat.table_name, stat.column_name,
                                   stat.unique_count, stat.nulls_count,
                                   stat.min_value, stat.max_value, stat.avg_value,
                                   stat.median12_value, stat.median25_value, stat.median37_value, stat.median50_value,
                                   stat.median63_value, stat.median75_value, stat.median88_value)

    def reconnect(self):
        self.schema.reconnect()
