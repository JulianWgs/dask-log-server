import ast


def _strip_instances(iterable, excluded_instances=None):
    """

    Parameters
    ----------
    iterable: list, dict, tuple
        Iterable (in most cases a dask task graph).
    excluded_instances:
        Names of excluded types, which will not be stripped. The default is None.

    Returns
    -------
    list, dict, tuple
        Iterable only with built-in types.

    """
    if excluded_instances is None:
        excluded_instances = list()

    if isinstance(iterable, list):
        stripped_iterable = list()
        for item in iterable:
            stripped_iterable.append(_strip_instances(item, excluded_instances))
        return stripped_iterable

    elif isinstance(iterable, tuple):
        stripped_iterable = list()
        for item in iterable:
            stripped_iterable.append(_strip_instances(item, excluded_instances))
        return tuple(stripped_iterable)

    elif isinstance(iterable, dict):
        stripped_iterable = dict()
        for key, value in iterable.items():
            stripped_iterable[key] = _strip_instances(value, excluded_instances)
        return stripped_iterable
    elif isinstance(iterable, (int, bool, float, str)) or iterable is None:
        return iterable
    else:
        try:
            full_name = iterable.__module__ + "." + iterable.__name__
        except:
            full_name = (
                    iterable.__class__.__module__ + "." + iterable.__class__.__name__
            )
        if full_name in excluded_instances:
            return iterable
        else:
            return callable


def _flatten_tuple(nested_tuple, tuple_index, single_indices):
    entries = list(nested_tuple[tuple_index])
    single_data = [nested_tuple[single_index] for single_index in single_indices]
    return [[entry] + single_data for entry in entries]


def _to_tuple(string):
    try:
        return ast.literal_eval(string)
    except:
        return string


def _flatten_dict(nested_dict, list_key, single_keys):
    list_of_dict = nested_dict[list_key]
    column_data = {single_key: nested_dict[single_key] for single_key in single_keys}
    [dict_.update(column_data) for dict_ in list_of_dict]
    return list_of_dict


def _func_name(label):
    return label.split("-#")[0].replace("(", "").replace("'", "")


def _get_nested(dict_, keys):
    """
    Nested get method for dictionaries (and lists, tuples).
    """
    try:
        for key in keys:
            dict_ = dict_[key]
    except (KeyError, IndexError, TypeError):
        return None
    return dict_
