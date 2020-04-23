#! python3
"""
The main intention and idea of this wrapper is based on a scrapy wrapper script by Philipp Poschmann.
The only code remaining from their original script (aside from a few function/class names) is the list of
denied_extensions in the GenericCrawlSpider class.

Created on 15.06.2019

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
import json
import logging
import sys
import os

from scrapy import Request

import shared
from parsers import ParagraphParser
from shared import CrawlSpecification

import pandas
from scrapy.spiders import CrawlSpider, Rule
from scrapy.linkextractors.lxmlhtml import LxmlLinkExtractor
from scrapy.crawler import CrawlerProcess
from urllib.parse import urlparse
from scrapy.settings import Settings
from scrapy.utils.url import url_is_from_any_domain, url_has_any_extension
from langdetect import DetectorFactory


ACCEPTED_LANG = ["de", "en"]
LANGSTATS_ID = "allowed_languages"
LANGSTATS = pandas.DataFrame(columns=ACCEPTED_LANG)
LANGSTATS.index.name = "url"


DEBUG = False
if len(sys.argv) >= 3 and sys.argv[2] == "DEBUG":
    DEBUG = True

if DEBUG:
    log_level = logging.DEBUG
else:
    log_level = logging.INFO

VERSION = "0.4.1"

# Prepare logging, before reading specification only log on console
MLOG = shared.simple_logger(loger_name="scrapy_wrapper")
MLOG.info("Running scrapy_wrapper on version {}".format(VERSION))


def load_settings(settings_path) -> CrawlSpecification:
    """
    Loads the json string in settings_path and deserializes to CrawlSpecification object.
    Determines required behaviour with respect to CrawlSpecification.mode.
    :param settings_path: file path to the json crawl specification file
    :return: parsed and semantically updated CrawlSpecification object
    """
    try:
        settings_file = open(settings_path, "r")
        settings = CrawlSpecification()
        json_str = settings_file.read()
        settings.deserialize(json_str)

        MLOG.info("Starting crawl with the following settings:\n{0}".format(settings.serialize()))
    except Exception as exc:
        MLOG.exception("{0}: {1}".format(type(exc).__name__, exc))
        return None

    return settings


class VerboseLxmlLinkExtractor(LxmlLinkExtractor):

    def __init__(self, logname="scrapy_wrapper", spec=None, **kwargs):
        super().__init__(**kwargs)
        if spec.logs:
            self.logger = shared.simple_logger(loger_name="linkextractor",
                                               file_path=os.path.join(spec.logs, logname + ".log")
                                              )

    def _link_allowed(self, link):
        _matches = lambda url, regexs: any(r.search(url) for r in regexs)
        _is_valid_url = lambda url: url.split('://', 1)[0] in {'http', 'https', 'file', 'ftp'}

        if not _is_valid_url(link.url):
            self.logger.warning(f"Not allowed: {link.url} // no valid url")
            return False
        if self.allow_res and not _matches(link.url, self.allow_res):
            self.logger.warning(f"Not allowed: {link.url} // does not match whitelist")
            return False
        if self.deny_res and _matches(link.url, self.deny_res):
            self.logger.warning(f"Not allowed: {link.url} // matches blacklist")
            return False
        parsed_url = urlparse(link.url)
        if self.allow_domains and not url_is_from_any_domain(parsed_url, self.allow_domains):
            self.logger.warning(f"Not allowed: {link.url} // domain not listed as allowed")
            return False
        if self.deny_domains and url_is_from_any_domain(parsed_url, self.deny_domains):
            self.logger.warning(f"Not allowed: {link.url} // domain is listed as denied")
            return False
        if self.deny_extensions and url_has_any_extension(parsed_url, self.deny_extensions):
            self.logger.warning(f"Not allowed: {link.url} // extension is denied")
            return False
        if self.restrict_text and not _matches(link.text, self.restrict_text):
            return False
        return True

def create_spider(settings, start_url, crawler_name):
    class GenericCrawlSpider(CrawlSpider):

        crawl_specification = settings

        # load parser from specification
        try:
            parser_class = shared.get_class(crawl_specification.parser)
            parser = parser_class(data=crawl_specification.parser_data)
        except AttributeError or TypeError as exc:
            MLOG.exception(exc)

        domain = urlparse(start_url).netloc

        name = crawler_name

        allowed_domains = [domain]

        start_urls = [start_url]

        denied_extensions = ['mng', 'pct', 'bmp', 'gif', 'jpg', 'jpeg', 'png', 'pst', 'psp', 'tif', 'tiff', 'ai', 'drw',
                             'dxf', 'eps', 'ps', 'svg', 'mp3', 'wma', 'ogg', 'wav', 'ra', 'aac', 'mid', 'au', 'aiff',
                             '3gp', 'asf', 'asx', 'avi', 'mov', 'mp4', 'mpg', 'qt', 'rm', 'swf', 'wmv',
                             'm4a', 'm4v', 'flv', 'xls', 'xlsx', 'ppt', 'pptx', 'pps', 'doc', 'docx', 'odt', 'ods',
                             'odg', 'odp', 'css', 'exe', 'bin', 'rss', 'zip', 'rar', 'gz', 'tar'
                             ]
        if isinstance(crawl_specification.parser, ParagraphParser):
            denied_extensions.append("pdf")

        rules = [
            Rule(VerboseLxmlLinkExtractor(logname=crawler_name,
                                          spec=crawl_specification,
                                          deny=crawl_specification.blacklist,
                                          allow=crawl_specification.whitelist,
                                          deny_extensions=denied_extensions),
                 callback=parser.parse,
                 follow=True,
                 errback=parser.errback)
        ]

        # ensure that start_urls are also parsed
        parse_start_url = parser.parse

        def __init__(self):
            super().__init__()
            # setup individual logger for every spider
            if self.crawl_specification.logs:
                self.s_log = shared.simple_logger(loger_name="crawlspider",
                                                  file_path=os.path.join(self.crawl_specification.logs,
                                                                         self.name + ".log")
                                                  )
            else:
                self.s_log = shared.simple_logger(loger_name="crawlspider")

            # enter spider to parser
            self.parser.spider = self

            for hand in self.s_log.handlers:
                self.logger.logger.addHandler(hand)
            self.s_log.info("[__init__] - Crawlspider logger setup finished.")


        def start_requests(self):
            for url in self.start_urls:
                yield Request(url)

    return GenericCrawlSpider


class GenericScrapySettings(Settings):

    def __init__(self):
        super().__init__(values={
            "DEPTH_LIMIT": 5,
            "FEED_EXPORT_ENCODING": "utf-8",
            "LOG_LEVEL": "WARNING",
            "DEPTH_PRIORITY": 1,
            "SCHEDULER_DISK_QUEUE": 'scrapy.squeues.PickleFifoDiskQueue',
            "SCHEDULER_MEMORY_QUEUE": 'scrapy.squeues.FifoMemoryQueue',
            "ROBOTSTXT_OBEY": True
            })


def run_crawl(call_parameter, worker_flag=False):
    """Run crawl with given parameter."""
    global MLOG
    # setup consistent language detection
    DetectorFactory.seed = 0

    if os.path.exists(call_parameter):
        crawl_specification = load_settings(call_parameter)
    else:
        # assume the first parameter to be the json string
        crawl_specification = CrawlSpecification()
        crawl_specification.deserialize(call_parameter)

    if not crawl_specification:
        MLOG.error("Crawl settings could not be loaded. Exiting scrapy_wrapper.")
        sys.exit(1)

    scrapy_settings = GenericScrapySettings()
    if crawl_specification.logs:
        if not os.path.exists(crawl_specification.logs):
            os.makedirs(crawl_specification.logs, exist_ok=True)
        # reset the master log for the wrapper to include file logging
        MLOG = shared.simple_logger(loger_name="scrapy_wrapper",
                                    file_path=os.path.join(crawl_specification.logs, "scrapy_wrapper.log"),
                                    file_level=log_level)
        # specifically assign a log file for scrapy
        scrapy_settings.set("LOG_FILE", os.path.join(crawl_specification.logs, "scrapy.log"))

    scrapy_settings.set("ITEM_PIPELINES", crawl_specification.pipelines)

    MLOG.info("Initiating scrapy crawler process")
    process = CrawlerProcess(settings=scrapy_settings)
    start_urls = list(set(crawl_specification.urls))
    allowed_domains = list(map(lambda x: urlparse(x).netloc, start_urls))
    for url in start_urls:
        name = shared.url2filename(url)
        MLOG.info("Creating spider {0}".format(name))
        process.crawl(create_spider(crawl_specification, url, name))
    try:
        process.start()
    except Exception as exc:
        MLOG.exception("{0}: {1}".format(type(exc).__name__, exc))

    # every spider finished, finalize crawl
    for finalizer_path in crawl_specification.finalizers:
        finalizer = shared.get_class(finalizer_path)
        if finalizer:
            # somehow pass the collected language statistics from parser
            finalizer(crawl_specification, crawl_specification.finalizers[finalizer_path]).finalize_crawl()

    if worker_flag == True:
        return True


def get_info():
    """ Compile Info for scrapy_wrapper and its included pipeline options """
    info = dict()
    info["version"] = VERSION

    import pipelines  # only locally import when requested
    info["finalizers"] = [".".join((cls.__module__, cls.__name__))
                          for cls in pipelines.CrawlFinalizer.__subclasses__()]
    info["content_pipelines"] = [".".join((cls.__module__, cls.__name__))
                                 for cls in pipelines.ContentPipeline.__subclasses__()]

    import parsers
    info["parsers"] = {".".join((cls.__module__, cls.__name__)):
                           [".".join((ppl.__module__, ppl.__name__)) for ppl in cls.ACCEPTED_PIPELINES]
                       for cls in parsers.ResponseParser.__subclasses__()}

    return info


def generate_template_specification(parser_class=None):
    p_class = "<Parser Class>"
    p_data = "<Parser Data Dictionary>"
    if parser_class:
        p_class = str(parser_class.__module__ + "." + parser_class.__qualname__)
        if hasattr(parser_class, "generate_example_data"):
            p_data = parser_class.generate_example_data()

    spec = CrawlSpecification(name="<Crawl Name>",
                              output="<Output Directory>",
                              logs="<Log Directory>",
                              urls=["<start_url1>", "<start_url2>"],
                              blacklist=["<blacklist regex 1>", "<blacklist regex 2>"],
                              whitelist=["<whitelist regex 1>", "<whitelist regex 2>"],
                              parser=p_class,
                              parser_data=p_data,
                              pipelines={"<Pipeline Class>": 300},
                              finalizers={"<Finalizer Class>": "<Finalizer Data Dictionary>"})
    return spec


if __name__ == '__main__':

    # get call parameter
    if len(sys.argv) >= 2:
        call_parameter = sys.argv[1]
    else:
        print("Neither crawl specification file nor json string given. Call scrapy_wrapper.py as follows:\n"
              "python scrapy_wrapper.py (<spec_file>|<spec json string>|INFO) [DEBUG]")
        sys.exit(1)

    if call_parameter == "INFO":
        print(json.dumps(get_info()))
        sys.exit(0)

    # start crawling
    run_crawl(call_parameter)
