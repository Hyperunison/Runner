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


def rand_alias_name(prefix: str) -> str:
    rnd = str(1+random.random()).replace(".", "")
    return "{}{}".format(prefix, rnd)


class VariableMapper:
    map: Dict[str, str] = {
        "patient_id": '"patient"."person_id" as participant_id',
        "race": '"patient"."race"',
        "ethnicity": '"patient"."ethnicity"',
        "gender": '"patient"."gender"',
        "age": 'year(NOW())-"patient"."year_of_birth"',
        "year_of_birth": '"patient"."year_of_birth"',
        "date_of_birth": '"patient"."birth_datetime"',
    }

    def convert_var_name(self, var: str) -> str:
        if not self.map.__contains__(var):
            raise Exception("Unknown object var {}".format(var))

        return self.map[var]

    def declare_var(self, ucdm: str, local: str):
        self.map[ucdm] = local


class Omop(BaseSchema):
    min_count: int = 0
    dst: str = ""

    def __init__(self, dsn: str, min_count: int):
        self.engine = create_engine(dsn).connect()
        self.min_count = min_count
        super().__init__()

    def build_cohort_definition_sql_query(self, where, export, distribution: bool) -> str:
        logging.info("Cohort request got: {}".format(json.dumps(where)))
        query = SQLQuery()
        mapper = VariableMapper()

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
            sql += "    count(distinct patient.person_id) as count\n"
        else:
            sql += mapper.convert_var_name("patient_id")+"\n"

        sql +="FROM(\n" + \
              "    SELECT person_id, year_of_birth, birth_datetime, \n" + \
              "    (\n" + \
              "        SELECT concept.concept_code\n" + \
              "        FROM concept\n" + \
              "        WHERE concept_id = gender_concept_id\n" + \
              "    ) AS gender,\n" + \
              "    (\n" + \
              "        SELECT concept.concept_code\n" + \
              "        FROM concept\n" + \
              "        WHERE concept_id = ethnicity_concept_id\n" + \
              "    ) AS ethnicity,\n" + \
              "    (\n" + \
              "        SELECT concept.concept_code\n" + \
              "        FROM concept\n" + \
              "        WHERE concept_id = race_concept_id\n" + \
              "    ) AS race\n" + \
              "FROM person) as patient\n"

        for j in query.joins:
            sql += "JOIN {} as {} ON {} \n".format(j.table, j.alias, j.condition)

        sql += "WHERE\n{}\n".format(sql_where)
        if distribution:
            sql += "GROUP BY {} \n".format(", ".join(map(str, range(1, len(select_array) + 1)))) + \
                   "HAVING COUNT(distinct patient.person_id) >= {}\n".format(self.min_count) + \
                   "ORDER BY {}".format(", ".join(map(str, range(1, len(select_array) + 1))))

        logging.info("Generated SQL query: \n{}".format(sql))

        return sql

    def resolve_cohort_definition(self, sql: str):
        result = self.engine.execute(text(sql)).mappings().all()
        result = [dict(row) for row in result]

        return result
    def execute_cohort_definition(self, cohort_definition: CohortAPIRequest, api: Api):
        key = cohort_definition.cohort_definition['key']
        sql = self.build_cohort_definition_sql_query(
            cohort_definition.cohort_definition['where'],
            cohort_definition.cohort_definition['export'],
            True
        )
        try:
            result = self.resolve_cohort_definition(sql)
            logging.info("Cohort definition result: {}".format(str(result)))
            api.set_cohort_definition_aggregation(result, sql, cohort_definition.reply_channel, key, cohort_definition.raw_only)
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

        if statement['type'] == 'exists':
            if statement['event'] == 'condition':
                alias = statement['alias']
                tmp_alias = rand_alias_name("diagnoses_")
                query.joins.append(SQLJoin(
                    'condition_occurrence',
                    tmp_alias,
                    '{}.person_id=patient.person_id'.format(tmp_alias),
                ))
                query.joins.append(SQLJoin(
                    'concept',
                    alias,
                    '{}.concept_id={}.condition_concept_id'.format(alias, tmp_alias)
                ))
                mapper.declare_var(alias+'.icd10', alias+'.concept_name')
                mapper.declare_var(alias+'.start_date', tmp_alias+'.condition_start_date')
                mapper.declare_var(alias+'.end_date', tmp_alias+'.condition_end_date')
                mapper.declare_var(alias+'.stop_reason', tmp_alias+'.stop_reason')
                arr = list[str]()

                if len(statement['where']) == 0:
                    return 'true'
                for stmt in statement['where']:
                    arr.append(self.build_sql_expression(stmt, query, mapper))

                return "(" + ") AND (".join(arr) + ")"
            if statement['event'] == 'measurement':
                alias = statement['alias']
                tmp_alias = rand_alias_name("measurement_")
                query.joins.append(SQLJoin(
                    'measurement',
                    tmp_alias,
                    '{}.person_id=patient.person_id'.format(tmp_alias),
                ))
                query.joins.append(SQLJoin(
                    'concept',
                    alias,
                    "{}.concept_id={}.measurement_concept_id".format(alias, tmp_alias, alias)
                ))
                mapper.declare_var(alias+'.name', alias+'.concept_name')
                mapper.declare_var(alias+'.date', tmp_alias+'.measurement_date')
                mapper.declare_var(alias+'.value', tmp_alias+'.measurement_source_value')
                arr = list[str]()

                if len(statement['where']) == 0:
                    return 'true'
                for stmt in statement['where']:
                    arr.append(self.build_sql_expression(stmt, query, mapper))

                return "(" + ") AND (".join(arr) + ")"

            if statement['event'] == 'procedure':
                alias = statement['alias']
                tmp_alias = rand_alias_name("procedure_")
                query.joins.append(SQLJoin(
                    'procedure_occurrence',
                    tmp_alias,
                    '{}.person_id=patient.person_id'.format(tmp_alias),
                ))
                query.joins.append(SQLJoin(
                    'concept',
                    alias,
                    "{}.concept_id={}.procedure_concept_id".format(alias, tmp_alias, alias)
                ))
                mapper.declare_var(alias+'.name', alias+'.concept_name')
                mapper.declare_var(alias+'.date', tmp_alias+'.procedure_date')
                mapper.declare_var(alias+'.value', tmp_alias+'.procedure_source_value')
                arr = list[str]()

                if len(statement['where']) == 0:
                    return 'true'
                for stmt in statement['where']:
                    arr.append(self.build_sql_expression(stmt, query, mapper))

                return "(" + ") AND (".join(arr) + ")"

            if statement['event'] == 'drug':
                alias = statement['alias']
                tmp_alias = rand_alias_name("drug_era_")
                query.joins.append(SQLJoin(
                    'drug_era',
                    tmp_alias,
                    '{}.person_id=patient.person_id'.format(tmp_alias),
                ))
                query.joins.append(SQLJoin(
                    'concept',
                    alias,
                    "{}.concept_id={}.drug_concept_id".format(alias, tmp_alias, alias)
                ))
                mapper.declare_var(alias+'.name', alias+'.concept_name')
                mapper.declare_var(alias+'.start_date', tmp_alias+'.drug_era_start_date')
                mapper.declare_var(alias+'.end_date', tmp_alias+'.drug_era_end_date')
                arr = list[str]()

                if len(statement['where']) == 0:
                    return 'true'
                for stmt in statement['where']:
                    arr.append(self.build_sql_expression(stmt, query, mapper))

                return "(" + ") AND (".join(arr) + ")"

            raise ValueError("Unknown event type got: {}".format(statement['event']))

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


