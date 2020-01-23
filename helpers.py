def chunks(l, n):
    """Yield successive n-sized chunks from l."""

    for i in range(0, len(l), n):
        yield l[i:i + n]


def to_bool(val):
    if isinstance(val, (bool, int, float)):
        return bool(val)
    elif isinstance(val, str):
        if val.lower() in ['true', '1']:
            return True
        elif val.lower() in ['false', '0']:
            return False
    raise Exception(f"Can't convert unexpected value to bool: {val}, type: {type(val)}")
