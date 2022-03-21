import configparser
from dataquery_processor.pathutils import get_config_path


class Config:

    def __init__(self):
        path = get_config_path("config.ini")
        self.config = configparser.ConfigParser()
        self.config.read(path, encoding='utf-8')

    def get_table_mapping(self, mapping):
        return self.config.get('table_mappings', mapping)


_config = Config()


