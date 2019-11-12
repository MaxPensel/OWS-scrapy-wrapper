"""
Created on 24.09.2019

@author: Maximilian Pensel

Copyright 2019 Maximilian Pensel <maximilian.pensel@gmx.de>

This file is part of OpenWebScraper.

OpenWebScraper is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

OpenWebScraper is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with OpenWebScraper.  If not, see <https://www.gnu.org/licenses/>.
"""

import os
import shutil
from urllib.parse import urlparse

import pandas
import common
from common import CrawlSpecification

#import core
#from core.Workspace import WorkspaceManager
#from modules.crawler import filemanager
#from modules.crawler.model import CrawlSpecification

###
# Crawl Finalizers
###


class CrawlFinalizer:

    def __init__(self, spec: CrawlSpecification):
        self.crawl_specification = spec

    def finalize_crawl(self, data: {} = None):
        pass


class LocalCrawlFinalizer(CrawlFinalizer):
    LANGSTATS_ID = "allowed_languages"

    def __init__(self, spec: CrawlSpecification, settings: {}):
        super().__init__(spec)
        self.log = common.simple_logger("LocalCrawlFinalizer", file_path=os.path.join(WorkspaceManager().get_log_path(),
                                                                                    self.crawl_specification.name,
                                                                                    "scrapy.log"))

    def finalize_crawl(self, data: {} = None):
        self.log.info("Finalizing crawl ...")
        if data is None:
            data = dict()

        if LocalCrawlFinalizer.LANGSTATS_ID in data:
            # save LANGSTATS
            self.log.info("Saving language statistics to ")
            filemanager.save_dataframe(self.crawl_specification.name,
                                       LocalCrawlFinalizer.LANGSTATS_ID,
                                       data[LocalCrawlFinalizer.LANGSTATS_ID])

        stats_df = pandas.DataFrame(columns=["total paragraphs", "unique paragraphs", "unique urls"])
        stats_df.index.name = "url"
        one_incomplete = False
        for csv in filemanager.get_datafiles(self.crawl_specification.name):
            # As soon as there still is an incomplete file set one_incomplete = True
            one_incomplete = one_incomplete or filemanager.incomplete_flag in csv
            try:
                df = filemanager.load_crawl_data(self.crawl_specification.name, csv, convert=False)
                stats_df.at[csv, "total paragraphs"] = df.count()["url"]
                if df.empty:
                    stats_df.at[csv, "unique paragraphs"] = 0
                    stats_df.at[csv, "unique urls"] = 0
                else:
                    unique = df.nunique()
                    stats_df.at[csv, "unique paragraphs"] = unique["content"]
                    stats_df.at[csv, "unique urls"] = unique["url"]
            except Exception as exc:
                stats_df.at[csv.replace(".csv", ""), "total paragraphs"] = "Could not process"
                self.log.exception("Error while analyzing results. {0}: {1}".format(type(exc).__name__, exc))

        filemanager.save_dataframe(self.crawl_specification.name, "stats", stats_df)

        if not one_incomplete:  # and not DEBUG:
            filemanager.move_crawl_specification(self.crawl_specification.name)

        self.log.info("Done finalizing crawl.")


class RemoteCrawlFinalizer(CrawlFinalizer):

    def __init__(self, spec: CrawlSpecification, settings: {}):
        super().__init__(spec)
        self.log = core.simple_logger("RemoteCrawlFinalizer", file_path=os.path.join(WorkspaceManager().get_log_path(),
                                                                                     self.crawl_specification.name,
                                                                                     "scrapy.log"))

    def finalize_crawl(self, data: {} = None):
        self.log.info("Finalizing crawl ...")
        if data is None:
            data = dict()

        # TODO: this method is automatically called after the entire crawl has finished, gather the crawl results from
        #       the workspace (using filemanager) and compose an http request for further processing

        # fetching crawl results
        for csv_filepath in filemanager.get_datafiles(self.crawl_specification.name, abspath=True):
            with open(csv_filepath, mode="r", encoding="utf-8") as csv_file:
                csv_content = csv_file.read()
                # TODO: add this content to a dict in order to compose http request

        # fetching log contents
        for log_filename in os.listdir(os.path.join(WorkspaceManager().get_log_path(), self.crawl_specification.name)):
            log_filepath = os.path.abspath(log_filename)
            with open(log_filepath, mode="r", encoding="utf-8") as log_filename:
                log_content = log_filename.read()
                # TODO: add this content to a dict in order to compose http request

        # TODO: send away the http request

        self.log.info("Done finalizing crawl.")


###
# Pipelines
###

class Paragraph2CsvPipeline(object):

    INCOMPLETE_FLAG = "-INCOMPLETE"

    def process_item(self, item, spider):

        df_item = dict()
        for key in item:
            # df_item = {"url": [url], "content": [content]}
            df_item[key] = [item[key]]

        url = item['url']
        domain = urlparse(url).netloc
        if domain in spider.allowed_domains:
            spider.s_log.info("[process_item] - Adding content for {0} to {1}".format(str(url), str(spider.name)))

            fullpath = os.path.join(spider.crawl_specification.output, spider.name + self.INCOMPLETE_FLAG + ".csv")

            # careful, csv file may not exist for some reason (moved, deleted, ..)
            if os.path.exists(fullpath):
                df = pandas.DataFrame.from_dict(df_item)
                df.to_csv(fullpath, mode="a", sep=";", index=False, encoding="utf-8", header=False, line_terminator="")

        return item

    def open_spider(self, spider):
        spider.s_log.info(" vvvvvvvvvvvvvvvvvvvvvvvvvvvv OPENING SPIDER {0} vvvvvvvvvvvvvvvvvvvvvvvvvvvv"
                          .format(spider.name))
        # make sure output directory exists
        if not os.path.exists(spider.crawl_specification.output):
            os.makedirs(spider.crawl_specification.output, exist_ok=True)

        # initialize necessary csv data file
        fullpath = os.path.join(spider.crawl_specification.output, spider.name + self.INCOMPLETE_FLAG + ".csv")

        df = pandas.DataFrame(columns=["url", "content", "depth"])
        df.to_csv(fullpath, sep=";", index=False, encoding="utf-8")

    def close_spider(self, spider):
        spider.s_log.info(" ^^^^^^^^^^^^^^^^^^^^^^^^^^^^ CLOSING SPIDER {0} ^^^^^^^^^^^^^^^^^^^^^^^^^^^^"
                          .format(spider.name))

        fullpath_inc = os.path.join(spider.crawl_specification.output, spider.name + self.INCOMPLETE_FLAG + ".csv")
        fullpath_com = os.path.join(spider.crawl_specification.output, spider.name + ".csv")

        shutil.move(fullpath_inc, fullpath_com)
