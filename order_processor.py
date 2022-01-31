import json
import os


class OrderProcessor(object):

    def __init__(self, order):
        self.order = order
        self.in_progress = True
        self.completed = False

    def process(self):
        print('processing order ' + self.order['orderRef'])
        self.__generate_query__()
        self.__execute_query__()
        self.__write_receipt_manifest__()
        return True

    def __generate_query__(self):
        print('generating query for order ' + self.order['orderRef'])
        pass

    def __execute_query__(self):
        print('executing query for order ' + self.order['orderRef'])
        filename = self.__create_folder__() + "data.csv"
        with open(filename, "w", encoding='utf-8') as file:
            output = "placeholder"
            file.write(output)

    def __write_receipt_manifest__(self):
        print('writing manifest for order ' + self.order['orderRef'])

        path = self.__create_folder__()
        filename = path + "manifest.json"
        with open(filename, "w", encoding='utf-8') as file:
            output = json.dumps(self.order)
            file.write(output)

    def __create_folder__(self):
        path = "output" + os.sep + self.order['orderRef'] + os.sep
        if not os.path.exists(path):
            os.mkdir(path)
        return path

