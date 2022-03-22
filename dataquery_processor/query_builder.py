import logging

from pypika import Table, Criterion, MSSQLQuery as Query, functions as fn
from dataquery_processor import _config


def map_table(table):
    return _config.get_table_mapping(table)


def map_measure(measure):
    return _config.get_measure_mapping(measure)


class QueryBuilder(object):

    def __init__(self, manifest):
        self.years = manifest['years']
        self.fieldnames = []
        self.constraints = []
        for field in manifest['items']:
            if 'allowedValues' in field.keys() and len(field['allowedValues']) > 0:
                self.constraints.append(field)
            else:
                self.fieldnames.append(field['fieldName'])
        self.measure = map_measure(manifest['measure'])
        self.table = Table(map_table(manifest['datasource']), schema='dbo')
        logging.debug("Query builder using table " + str(self.table))

    def create_query(self):
        c = self.create_constraints()
        select_fields = self.fieldnames.copy()
        select_fields.append(fn.Sum(self.table[self.measure]))
        q = Query().\
            from_(self.table).\
            select(*select_fields).\
            groupby(*self.fieldnames).\
            where(Criterion.all(c))
        return q.get_sql()

    def create_constraints(self):
        clauses = []
        for constraint in self.constraints:
            column = constraint['fieldName']
            for value in constraint['allowedValues']:
                clauses.append(self.table[column] == value)
        clauses.append(self.table['Academic year'].isin(self.years))
        return clauses

    def map_measure(self):
        pass



