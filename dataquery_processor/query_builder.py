from pypika import Table, Query, Criterion
from dataquery_processor import _config


def map_table(table):
    return _config.get_table_mapping(table)


class QueryBuilder(object):

    def __init__(self, manifest):
        self.fieldnames = []
        self.constraints = []
        for field in manifest['items']:
            if 'allowedValues' in field.keys() and len(field['allowedValues']) > 0:
                self.constraints.append(field)
            else:
                self.fieldnames.append(field['fieldName'])
        self.table = Table(map_table(manifest['datasource']))

    def create_query(self):
        c = self.create_constraints()
        if len(c) > 0:
            q = Query().\
                from_(self.table).\
                select(self.fieldnames).\
                groupby(*self.fieldnames).\
                where(Criterion.all(c))
        else:
            q = Query(). \
                from_(self.table). \
                select(self.fieldnames)\
                .groupby(*self.fieldnames)

        return q.get_sql()

    def create_constraints(self):
        clauses = []
        for constraint in self.constraints:
            column = constraint['fieldName']
            for value in constraint['allowedValues']:
                clauses.append(self.table[column] == value)
        return clauses

    def map_measure(self):
        pass


