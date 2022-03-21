from pypika import Table, Query, Criterion
from dataquery_processor import _config


def map_table(table):
    return _config.get_table_mapping(table)


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
        self.table = Table(map_table(manifest['datasource']), schema='dbo')
        print(self.table)
        print(map_table(manifest['datasource']))

    def create_query(self):
        c = self.create_constraints()
        q = Query().\
            from_(self.table).\
            select(*self.fieldnames).\
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



