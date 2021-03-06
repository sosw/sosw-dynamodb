import json
import logging
from typing import Dict, Optional, List
from boto3.dynamodb.types import TypeSerializer, TypeDeserializer

from helpers import to_bool


logger = logging.getLogger('sosw-dynamodb')
logger.setLevel(logging.INFO)


type_serializer = TypeSerializer()
type_deserializer = TypeDeserializer()


def dynamo_to_dict(dynamo_row: Dict, row_mapper: Dict[str, str], only_fields: Optional[List[str]] = None,
                   dont_json_loads_results: bool = None) -> Dict:
    """
    Convert the ugly DynamoDB syntax of the row, to regular dictionary.
    We currently support only String or Numeric values. Latest ones are converted to int or float.
    Takes settings from row_mapper.

    e.g.:               {'key1': {'N': '3'}, 'key2': {'S': 'value2'}}
    will convert to:    {'key1': 3, 'key2': 'value2'}

    :param dict dynamo_row:       DynamoDB row item
    :param dict row_mapper:       Attributes and their types. E.g. {'key1': 'N', 'key2': 'S'}
    :param list only_fields:      If provided, will only return the given attributes.
    :param dont_json_loads_results: Set it to False if you don't want to json.loads string-jsons.
    :return: The row in a key-value format
    :rtype: dict
    """

    result = {}

    for key, val_dict in dynamo_row.items():

        if not only_fields or key in only_fields:

            for val_type, val in val_dict.items():

                key_type = row_mapper.get(key) or val_type

                # type_deserializer.deserialize() parses 'N' to `Decimal` type but it cant be parsed to a datetime
                # so we cast it to either an integer or a float.
                if key_type == 'N':
                    result[key] = float(val) if '.' in val else int(val)
                elif key_type == 'M':
                    result[key] = dynamo_to_dict(val, row_mapper=row_mapper)
                elif key_type == 'S':
                    # Try to load to a dictionary if looks like JSON.
                    if val.startswith('{') and val.endswith('}') and not dont_json_loads_results:
                        try:
                            result[key] = json.loads(val)
                        except ValueError:
                            logger.warning(f"A JSON-looking string failed to parse: {val}")
                            result[key] = val
                    else:
                        result[key] = val
                else:
                    result[key] = type_deserializer.deserialize(val_dict)

    return result


def dict_to_dynamo(row_dict, row_mapper: Dict[str, str], add_prefix=None):
    """
    Convert the row from regular dictionary to the ugly DynamoDB syntax. Takes settings from row_mapper.

    e.g.                {'key1': 'value1', 'key2': 'value2'}
    will convert to:    {'key1': {'Type1': 'value1'}, 'key2': {'Type2': 'value2'}}

    :param dict row_dict:   A row we want to convert to dynamo syntax.
    :param dict row_mapper: Attributes and their types. E.g. {'key1': 'N', 'key2': 'S'}
    :param str add_prefix:  A string prefix to add to the key in the result dict. Useful for queries like update.

    :return:                DynamoDB Task item
    :rtype:                 dict
    """

    if add_prefix is None:
        add_prefix = ''

    result = {}

    # Keys from row mapper
    for key, key_type in row_mapper.items():
        val = row_dict.get(key)
        if val is not None:
            key_with_prefix = f"{add_prefix}{key}"
            if key_type == 'BOOL':
                result[key_with_prefix] = {'BOOL': to_bool(val)}
            elif key_type == 'N':
                result[key_with_prefix] = {'N': str(val)}
            elif key_type == 'S':
                result[key_with_prefix] = {'S': str(val)}
            elif key_type == 'M':
                result[key_with_prefix] = {'M': dict_to_dynamo(val, row_mapper=row_mapper)}
            else:
                result[key_with_prefix] = type_serializer.serialize(val)

    result_keys = result.keys()
    if add_prefix:
        result_keys = [x[len(add_prefix):] for x in result.keys()]

    # Keys which are not in row mapper
    for key in list(set(row_dict.keys()) - set(result_keys)):
        val = row_dict.get(key)
        key_with_prefix = f"{add_prefix}{key}"
        if isinstance(val, bool):
            result[key_with_prefix] = {'BOOL': to_bool(val)}
        elif isinstance(val, (int, float)) or (isinstance(val, str) and val.isnumeric()):
            result[key_with_prefix] = {'N': str(val)}
        elif isinstance(val, str):
            result[key_with_prefix] = {'S': str(val)}
        elif isinstance(val, dict):
            result[key_with_prefix] = {'M': dict_to_dynamo(val, row_mapper=row_mapper)}
        else:
            result[key_with_prefix] = type_serializer.serialize(val)

    logger.debug(f"dict_to_dynamo result: {result}")
    return result
