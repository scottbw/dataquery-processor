import json
import os
from datetime import datetime
import csv
import logging
from dataquery_processor import QueryBuilder

logger = logging.getLogger(__name__)


class DateTimeEncoder(json.JSONEncoder):
    def default(self, z):
        if isinstance(z, datetime):
            return (str(z))
        else:
            return super().default(z)


class OrderProcessor(object):

    def __init__(self, order):
        self.order = order
        self.in_progress = True
        self.completed = False

    def process(self):
        logger.info('processing order ' + self.order['orderRef'])
        self.__generate_query__()
        self.__execute_query__()
        self.__write_receipt_manifest__()
        self.completed = True
        self.in_progress = False
        return True

    def __generate_query__(self):
        logger.info('generating query for order ' + self.order['orderRef'])
        self.query = QueryBuilder(self.order).create_query()
        filename = self.__create_folder__() + "query.sql"
        with open(filename, "w", encoding='utf-8') as file:
            file.write(self.query)

    def __execute_query__(self):
        logger.info('executing query for order ' + self.order['orderRef'])
        logging.debug("SQL = " + self.query)
        filename = self.__create_folder__() + "data.csv"

        fieldnames = []
        for field in self.order['items']:
            fieldnames.append(field['fieldName'])

        rows = []
        for row_number in range(1, 100):
            row = {}
            for fieldname in fieldnames:
                row[fieldname] = "BANANA"
            rows.append(row)

        with open(filename, "w", encoding='utf-8') as file:
            writer = csv.DictWriter(file, fieldnames)
            writer.writeheader()
            writer.writerows(rows)
        self.output_filename = filename
        self.job_completed = datetime.now()

    def __write_receipt_manifest__(self):
        logger.info('writing manifest for order ' + self.order['orderRef'])

        manifest = self.order
        manifest['outputFile'] = self.output_filename
        manifest['jobCompleted'] = self.job_completed

        path = self.__create_folder__()
        filename = path + "manifest.json"
        with open(filename, "w", encoding='utf-8') as file:
            output = json.dumps(self.order, cls=DateTimeEncoder, indent=4, separators=(", ", ": "), sort_keys=True)
            file.write(output)

    def __create_folder__(self):
        path = "output" + os.sep + self.__get_output_path__() + os.sep
        if not os.path.exists(path):
            os.mkdir(path)
        return path

    def __get_output_path__(self):
        if self.order['orderRef']:
            return self.order['orderRef']
        else:
            "none"

