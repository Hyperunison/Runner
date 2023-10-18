import json
import logging
import random
from typing import List, Dict

from sqlalchemy.exc import ProgrammingError
from sqlalchemy import create_engine, text
from sqlalchemy.ext.declarative import declarative_base

from src.Api import Api
from src.Message.CohortAPIRequest import CohortAPIRequest
from src.UCDM.Schema.BaseSchema import BaseSchema

Base = declarative_base()


class SQLQuery:
    conditions: List[str]
    select: Dict[str, str]

    def __init__(self):
        self.conditions = []
        self.select = {}


def escape_string(s: str) -> str:
    return s.replace('\\', '\\\\').replace("'", "\\'")


def rand_alias_name(prefix: str) -> str:
    return "{}{}".format(prefix, str(random.random()).replace("0.", ""))


class VariableMapper:
    map: Dict[str, str] = {
        "patient_id": '"patient"."patient_id" as participant_id',
        "race": "patient.race",
        "ethnicity": "patient.ethnicity",
        "gender": "patient.sex",
        "age": "age",
        "year_of_birth": "date_part('year', form_completion_date)-patient.age",
        "date_of_birth": "form_completion_date - (patient.age||' years')::interval",
    }

    def convert_var_name(self, var: str) -> str:
        if not self.map.__contains__(var):
            raise Exception("Unknown object var {}".format(var))

        return self.map[var]

    def declare_var(self, ucdm: str, local: str):
        self.map[ucdm] = local


class CBioPortal(BaseSchema):
    min_count: int = 0
    dst: str = ""
    table: str = ""

    def __init__(self, dsn: str, min_count: int, table: str):
        self.engine = create_engine(dsn).connect()
        self.min_count = min_count
        self.table = table
        super().__init__()

    def build_cohort_definition_sql_query(self, where, export, distribution: bool) -> str:
        logging.info("Where definition got: {}".format(json.dumps(where)))
        query = SQLQuery()
        mapper = VariableMapper()

        for exp in where:
            query.conditions.append(self.build_sql_expression(exp, query, mapper))

        if len(query.conditions) > 0:
            sql_where = "(" + (")\nAND (".join(query.conditions)) + ")"
        else:
            sql_where = "true"

        select_array: list[str] = []
        for exp in export:
            alias = exp['as'] if 'as' in exp else  query.select[exp['name']]
            query.select[alias] = self.build_sql_expression(exp, query, mapper)
            select_array.append('{} as "{}"'.format(query.select[alias], alias))

        select_string = ", ".join(select_array)

        if distribution:
            additional_field = "count(distinct patient.patient_id) as count"
        else:
            additional_field = mapper.convert_var_name("patient_id")

        sql = "SELECT {}, {} FROM {} as patient\n".format(select_string, additional_field, self.table)

        sql += "WHERE {}\n".format(sql_where)

        if distribution:
            sql += "GROUP BY {} \n".format(", ".join(map(str, range(1, len(select_array) + 1)))) + \
                   "HAVING COUNT(*) >= {}\n".format(self.min_count) + \
                   "ORDER BY {}\n".format(", ".join(map(str, range(1, len(select_array) + 1))))

        return sql
    def execute_cohort_definition(self, cohort_definition: CohortAPIRequest, api: Api):
        key = cohort_definition.cohort_definition['key']
        sql = self.build_cohort_definition_sql_query(
            cohort_definition.cohort_definition['where'],
            cohort_definition.cohort_definition['export']
        )

        try:
            result = self.engine.execute(text(sql)).mappings().all()
            result = [dict(row) for row in result]
            logging.info("Cohort definition result: {}".format(str(result)))
            api.set_cohort_definition_aggregation(result, sql, cohort_definition.reply_channel, key,
                                                  cohort_definition.raw_only)
            logging.info("Generated SQL query: \n{}".format(sql))
        except ProgrammingError as e:
            logging.error("SQL query error: {}".format(e.orig))
            api.set_cohort_definition_aggregation(
                {},
                "/*\n{}*/\n\n{}\n".format(e.orig, sql),
                cohort_definition.reply_channel,
                key,
                cohort_definition.raw_only
            )

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
                operator,
                self.add_staples_around_statement(statement['right'], query, mapper)
            )

        if statement['type'] == 'exists':
            if statement['event'] == 'condition':
                alias = statement['alias']
                mapper.declare_var(alias + '.icd10', 'patient.icd_10')
                mapper.declare_var(alias + '.start_date', 'form_completion_date')
                mapper.declare_var(alias + '.stop_reason', 'null')
                arr = list[str]()

                if len(statement['where']) == 0:
                    return 'true'
                for stmt in statement['where']:
                    arr.append(self.build_sql_expression(stmt, query, mapper))

                return "(" + ") AND (".join(arr) + ")"
            if statement['event'] == 'measurement':
                alias = statement['alias']
                mapper.declare_var(alias + '.name', 'null')
                mapper.declare_var(alias + '.date', 'null')
                mapper.declare_var(alias + '.value', 'null')
                arr = list[str]()

                if len(statement['where']) == 0:
                    return 'true'
                for stmt in statement['where']:
                    arr.append(self.build_sql_expression(stmt, query, mapper))

                return "(" + ") AND (".join(arr) + ")"

            if statement['event'] == 'procedure':
                alias = statement['alias']
                mapper.declare_var(alias + '.name', 'null')
                mapper.declare_var(alias + '.date', 'null')
                mapper.declare_var(alias + '.value', 'null')
                arr = list[str]()

                if len(statement['where']) == 0:
                    return 'true'
                for stmt in statement['where']:
                    arr.append(self.build_sql_expression(stmt, query, mapper))

                return "(" + ") AND (".join(arr) + ")"

            if statement['event'] == 'drug':
                alias = statement['alias']
                mapper.declare_var(alias + '.name', 'null')
                mapper.declare_var(alias + '.start_date', 'null')
                mapper.declare_var(alias + '.end_date', 'null')
                arr = list[str]()

                if len(statement['where']) == 0:
                    return 'true'
                for stmt in statement['where']:
                    arr.append(self.build_sql_expression(stmt, query, mapper))

                return "(" + ") AND (".join(arr) + ")"
            raise ValueError("Unknown event type got: {}".format(statement['event']))

    def add_staples_around_statement(self, statement, query, mapper: VariableMapper) -> str:
        s = self.build_sql_expression(statement, query, mapper)
        if statement['type'] == 'variable':
            return s
        if statement['type'] == 'constant':
            return s

        return "({})".format(s)
