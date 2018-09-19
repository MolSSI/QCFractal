"""
Utilities for dictionary and JSON handeling.
"""


def replace_dict_keys(data, replacement):
    """
    Recursively replaces the keys in data from a dictionary `replacement`.
    """

    if isinstance(data, dict):
        ret = {}
        for k, v in data.items():

            # Replace key
            if k in replacement:
                k = replacement[k]

            # Recurse value if needed.
            new_v = v
            if isinstance(v, dict):
                new_v = replace_dict_keys(v, replacement)
            elif isinstance(v, (list, tuple)):
                new_v = [replace_dict_keys(x, replacement) for x in v]
                if isinstance(v, tuple):
                    new_v = tuple(new_v)

            ret[k] = new_v
        return ret

    elif isinstance(data, (tuple, list)):
        new_data = [replace_dict_keys(x, replacement) for x in data]
        if isinstance(data, tuple):
            new_data = tuple(new_data)

        return new_data

    else:
        return data
