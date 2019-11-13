# -*- coding: utf-8 -*-
"""Read and initialize configuration object as global variable."""

import toml
import os

from common.logger import log


# class Config(object):
#     """Create Config object from dictionary (created by toml file).
#
#     Fields are dynamically created based on the input dictonary.
#     Source: https://stackoverflow.com/questions/31643861/converting-a-nested-dict-to-python-object
#     """
#
#     def __init__(self, entries):
#         for k,v in entries.items():
#             if isinstance(v,dict):
#                 self.__dict__[k] = Config(v)
#             else:
#                 self.__dict__[k] = v

class Config(object):
    """Create Config object from dictionary (created by toml file).

    Fields are dynamically created based on the input dictonary.
    """

    def __init__(self, entries):
        # Dynamically create config fields, if no instance was created yet
        self.__dict__.update(entries)


def read_toml(path, file):
    """Read .toml and return dict."""

    toml_file = os.path.join(os.getcwd(), path, file)

    with open(toml_file, "r") as f:
        toml_dict = toml.load(f, _dict=dict)

    return toml_dict


def create_config(config_path, config_file):
    """Create and initialize Config object.

    Creates config from single file or all files in given config folder."""

    config_dict = read_toml(config_path, config_file)

    # create config object
    config = Config(config_dict)

    return config


config = create_config('modules/crawler/common', 'config.toml')
log.info("Configuration successfully created.")
