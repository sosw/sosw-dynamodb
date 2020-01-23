# Registered tables. Key is table name, value is DynamoTable instance
from exceptions import TableNotRegisteredException


registered_tables = {}


def get_row_mapper(table_name: str):
    if table_name not in registered_tables:
        raise Exception(TableNotRegisteredException(f"Table '{table_name}' is not registered"))

    return registered_tables[table_name]
