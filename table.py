from typing import Dict

from shared import registered_tables


class DynamoTable:

    def __init__(self, name: str, row_mapper: Dict[str, str] = None):
        self.name = name
        self.row_mapper = row_mapper

        registered_tables[self.name] = self
