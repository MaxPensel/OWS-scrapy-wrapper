"""
Created on 11.11.2019

@author: Maximilian Pensel

Copyright 2019 Maximilian Pensel <maximilian.pensel@gmx.de>

This file is part of OWS-scrapy-wrapper.

OWS-scrapy-wrapper is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

OWS-scrapy-wrapper is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with OWS-scrapy-wrapper.  If not, see <https://www.gnu.org/licenses/>.
"""
import importlib
import json
import os
import sys
from logging import INFO, Logger, Formatter, StreamHandler, FileHandler
from urllib.parse import urlparse


class CrawlSpecification:

    def __init__(self,
                 name: str = None,
                 output: str = None,
                 logs: str = None,
                 urls: [str] = None,
                 blacklist: [str] = None,
                 whitelist: [str] = None,
                 parser: str = None,
                 parser_data: {} = None,
                 pipelines: {} = None,
                 finalizers: {} = None):

        self.name = name
        self.output = output
        self.logs = logs

        if urls is None:
            urls = list()
        self.urls = urls

        if blacklist is None:
            blacklist = list()
        self.blacklist = blacklist

        if whitelist is None:
            whitelist = list()
        self.whitelist = whitelist

        self.parser = parser

        if parser_data is None:
            parser_data = dict()
        self.parser_data = parser_data

        if pipelines is None:
            pipelines = dict()
        self.pipelines = pipelines

        if finalizers is None:
            finalizers = dict()
        self.finalizers = finalizers

    def update(self,
               name: str = None,
               output: str = None,
               logs: str = None,
               urls: [str] = None,
               blacklist: [str] = None,
               whitelist: [str] = None,
               parser: str = None,
               parser_data: {} = None,
               pipelines: {} = None,
               finalizers: {} = None):
        if name:
            self.name = name
        if output:
            self.output = output
        if logs:
            self.logs = logs
        if urls:
            self.urls = urls
        if blacklist:
            self.blacklist = blacklist
        if whitelist:
            self.whitelist = whitelist
        if parser:
            self.parser = parser
        if parser_data:
            self.parser_data = parser_data
        if pipelines:
            self.pipelines = pipelines
        if finalizers:
            self.finalizers = finalizers

    def serialize(self, pretty=True):
        if pretty:
            return json.dumps(self.__dict__, sort_keys=True, indent=4, separators=(',', ': '))
        else:
            return json.dumps(self.__dict__, sort_keys=True, separators=(',', ':'))

    def deserialize(self, json_str):
        data = json.loads(json_str)
        for key in data:
            setattr(self, key, data[key])


def url2filename(url):
    return (urlparse(url).netloc + urlparse(url).path).replace("/", "_")


def simple_logger(loger_name="core", file_path=None, console_level=INFO, file_level=INFO) -> Logger:
    """
    Create a logging.Logger instance and configure it with console stream handler and optionally file handler.
    Format is kept simple and universal with '[modname] time - level - message'
    If no file_path is given (default: None), then log messages are only provided in console.
    If file_path is provided, it will also create the required directory structure if necessary.

    :param modname: name of the module producing log messages (default: core)
    :param file_path: path to log file, give relative path, starting from %workspace%/logs/  (default: None)
    :param console_level: level of log messages for the console stream handler (default: INFO)
    :param file_level: level of log messages for the file stream handler (default: INFO)
    :return: configured instance of logging.Logger
    """
    # setup logger format
    log_fmt = Formatter(fmt="[{0}] %(asctime)s - %(levelname)s - %(message)s".format(loger_name))

    logger = Logger(loger_name)

    log_ch = StreamHandler(sys.stdout)
    log_ch.setLevel(console_level)
    log_ch.setFormatter(log_fmt)

    logger.addHandler(log_ch)

    if file_path:
        parent_dir = os.path.abspath(os.path.join(file_path, os.pardir))
        if not file_path.endswith(".log"):
            logger.warning("Log files are recommended to end in '.log'.")
        if not os.path.exists(parent_dir):
            logger.info("Directory {0} is being created for logging.".format(parent_dir))
            os.makedirs(parent_dir, exist_ok=True)

        log_fh = FileHandler(file_path, mode="a")
        log_fh.setFormatter(log_fmt)
        log_fh.setLevel(file_level)

        logger.addHandler(log_fh)

    return logger


def get_class(class_path):
    """
    Return the class object of the specified class-path, e.g. core.QtExtensions.SimpleMessageBox.
    :param class_path:
    :return: The class, if class_path references an existing, instantiatable class, None otherwise.
    """
    pieces = class_path.split(".")
    package_path = ".".join(pieces[:-1])
    class_name = pieces[-1:][0]

    package = importlib.import_module(package_path)
    if not hasattr(package, class_name):
        raise AttributeError("The module {0} has no attribute '{1}'".format(package_path, class_name))

    clazz = getattr(package, class_name)
    if not isinstance(clazz, type):
        raise TypeError("{1} is not an instantiatable class in {0}".format(package_path, class_name))

    return clazz