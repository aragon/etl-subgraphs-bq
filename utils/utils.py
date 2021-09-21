import collections
import os
from pathlib import Path

def correct_env_var(env_var_name):
    env_var = os.getenv(env_var_name)
    if '~' in env_var:
        # As it's relative path to home, reset env var
        env_var_new = Path(env_var).expanduser().as_posix()
        os.environ[env_var_name] = env_var_new

def flatten(dictionary, parent_key=False, sep='_', flatten_lists=False):
    """
    Turn a nested dictionary into a flattened dictionary
    :param dictionary: The dictionary to flatten
    :param parent_key: The string to prepend to dictionary's keys
    :param separator: The string used to separate flattened keys
    :return: A flattened dictionary

    Also flattens lists.
    """

    items = []
    for key, value in dictionary.items():
        new_key = str(parent_key) + sep + key if parent_key else key
        if isinstance(value, collections.MutableMapping):
            items.extend(flatten(value, new_key, sep, flatten_lists).items())
        elif flatten_lists and isinstance(value, list):
            for k, v in enumerate(value):
                items.extend(flatten({str(k): v}, new_key, sep, flatten_lists).items())
        else:
            items.append((new_key, value))
    return dict(items)


def flatten_1(d, parent_key='', sep='_'):
    '''
    Takes nested dict and returns one level dict 
    with keys appended by `sep`
    '''
    # From https://stackoverflow.com/questions/6027558/flatten-nested-dictionaries-compressing-keys/6027615#6027615
    items = []
    for k, v in d.items():
        new_key = parent_key + sep + k if parent_key else k
        if isinstance(v, collections.MutableMapping):
            items.extend(flatten(v, new_key, sep=sep).items())
        else:
            items.append((new_key, v))
    return dict(items)

def flatten_2(dictionary, parent_key=False, separator='_'):
    """
    Turn a nested dictionary into a flattened dictionary
    :param dictionary: The dictionary to flatten
    :param parent_key: The string to prepend to dictionary's keys
    :param separator: The string used to separate flattened keys
    :return: A flattened dictionary

    Also flattens lists.
    """

    items = []
    for key, value in dictionary.items():
        new_key = str(parent_key) + separator + key if parent_key else key
        if isinstance(value, collections.MutableMapping):
            items.extend(flatten_2(value, new_key, separator).items())
        elif isinstance(value, list):
            for k, v in enumerate(value):
                items.extend(flatten_2({str(k): v}, new_key).items())
        else:
            items.append((new_key, value))
    return dict(items)