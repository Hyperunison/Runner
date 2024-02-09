import json
import logging
import os
import sys
import time
import signal
from typing import List, Dict, Tuple
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
        if fields is Dict:
            for ucdm in fields.keys():
                self.declare_var(ucdm, fields[ucdm])

    def convert_var_name(self, var: str) -> str:
        if not self.map.__contains__(var):
            raise Exception("Unknown object var {}".format(var))

        return self.map[var]

    def declare_var(self, ucdm: str, local: str):
        self.map[ucdm] = local


class DataSchema:
    min_count: int = 0
    dst: str = ""
    schema: BaseSchema

    def __init__(self, dsn: str, schema:str, min_count: int):
        self.min_count = min_count
        self.schema = self.create_schema(dsn, schema, min_count)
        super().__init__()

    def create_schema(self, dsn: str, schema: str, min_count: int) -> BaseSchema:
        if schema == 'labkey':
            return Labkey(dsn, min_count)
        if schema == 'postgres':
            return Postgres(dsn,    min_count)
        raise Exception("Unknown schema {}".format(schema))

    def build_cohort_definition_sql_query(
            self,
            mapper,
            participantTable,
            participantIdField,
            joins,
            where,
            export,
            limit: int,
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
        group_array: list[str] = []
        for exp in export:
            alias = exp['as'] if 'as' in exp else  query.select[exp['name']]
            query.select[alias] = self.build_sql_expression(exp, query, mapper)
            select_array.append('{} as "{}"'.format(query.select[alias], alias))
            if exp['type'] != 'constant':
                # Labkey does not support GROUP BY <constant>
                group_array.append(query.select[alias])

        select_string = ", ".join(select_array)

        sql = "SELECT\n    {},\n".format(select_string)

        if distribution:
            sql += "    count(distinct {}.\"{}\") as count_uniq_participants\n".format(participantTable, participantIdField)
        else:
            sql += "{}.{}\n".format(participantTable, participantIdField)

        sql +="FROM {}\n".format(participantTable)

        for j in joins:
            sql += "JOIN {} as {} ON {} \n".format(j['table'], j['alias'], j['on'])

        sql += "WHERE\n{}\n".format(sql_where)
        if distribution:
            sql += "GROUP BY {} \n".format(", ".join(map(str, group_array))) + \
                   "HAVING COUNT(distinct {}.\"{}\") >= {}\n".format(participantTable, participantIdField, self.min_count) + \
                   "ORDER BY {}".format(", ".join(map(str, group_array)))
        sql += "\nLIMIT {}".format(limit)
        logging.info("Generated SQL query: \n{}".format(sql))

        return sql


    def execute_cohort_definition(self, cohort_definition: CohortAPIRequest, api: Api):
        pid = os.fork()
        if pid != 0:
            logging.info("Forked, run in fork, pid={}".format(pid))
            api.set_car_status(cohort_definition.cohort_api_request_id, "process", pid)
            return
        api.api_instance.api_client.close()
        api.api_instance.api_client.rest_client.pool_manager.clear()
        self.schema.reconnect()
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
            True
        )
        try:
            result = self.schema.fetch_all(sql)
            logging.info("Cohort definition result: {}".format(str(result)))
            api.set_cohort_definition_aggregation(result, sql, cohort_definition.reply_channel, key, cohort_definition.raw_only)
            api.set_car_status(cohort_definition.cohort_api_request_id, "success", pid)
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
            api.set_car_status(cohort_definition.cohort_api_request_id, "error", pid)
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
        stat = self.schema.get_table_column_stats(table_name, column_name)

        api.set_table_info(stat.table_name, stat.abandoned)
        if stat.abandoned:
            logging.error("Table {} not exists, mark is as abandoned".format(table_name))
            return
        if stat.values:
            values = [d['value'] for d in stat.values]
            counts = [d['cnt'] for d in stat.values]
            api.set_table_column_values(stat.table_name,  stat.column_name,  values, counts)
        api.set_table_column_stats(stat.table_name, stat.column_name,
                                   stat.unique_count, stat.nulls_count,
                                   stat.min_value, stat.max_value, stat.avg_value,
                                   stat.median12_value,stat.median25_value, stat.median37_value, stat.median50_value,
                                   stat.median63_value, stat.median75_value, stat.median88_value)

    def reconnect(self):
        self.schema.reconnect()