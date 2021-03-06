import configparser
from dataquery_processor.pathutils import get_config_path


class Config:

    def __init__(self):
        path = get_config_path("config.ini")
        self.config = configparser.ConfigParser()
        self.config.read(path, encoding='utf-8')
        self.conn = self.config.get("ODBC", "conn")

    def get_table_mapping(self, mapping):
        return self.config.get('table_mappings', mapping)

    def get_measure_mapping(self, mapping):
        return self.config.get('measure_mappings', mapping)

    def get_column_mapping(self, column):
        if self.config.has_option('column_mappings', column):
            return self.config.get('column_mappings', column)
        return column


_config = Config()



