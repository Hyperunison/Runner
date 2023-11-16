import json
import logging
from typing import List, Dict
from sqlalchemy.exc import ProgrammingError
from sqlalchemy import create_engine, text
from sqlalchemy.ext.declarative import declarative_base
from src.Api import Api
from src.Message.CohortAPIRequest import CohortAPIRequest

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

    def resolve_cohort_definition(self, sql: str):
        result = self.engine.execute(text(sql)).mappings().all()
        result = [dict(row) for row in result]

        return result
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
            result = self.resolve_cohort_definition(sql)
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


