
#
# Using command line BCP.exe
#


#
# Using ODBC and stream to CSV
#
import csv
import pyodbc
from dataquery_processor import _config


class OdbcQueryRunner(object):
    def __init__(self):
        print(_config.conn)
        conn = pyodbc.connect(_config.conn)
        self.cursor = conn.cursor()

    def run_query_and_save_results(self, query, file):
        rows = self.cursor.execute(query)

        with open(file, 'w', newline='') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow([x[0] for x in self.cursor.description])  # column headers
            for row in rows:
                writer.writerow(row)