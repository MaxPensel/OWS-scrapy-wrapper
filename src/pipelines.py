"""
Created on 24.09.2019

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

import os
import shutil
import sys
from urllib.parse import urlparse

import math
import numpy as np
import pandas

import shared
from shared import CrawlSpecification

from remote.finalizers import finalize_paragraphs, finalize_raw

###
# Crawl Finalizers
###

class CrawlFinalizer:

    def __init__(self, spec: CrawlSpecification):
        self.crawl_specification = spec

    def finalize_crawl(self, data: {} = None):
        pass


class RemoteCrawlFinalizer(CrawlFinalizer):

    def __init__(self, spec: CrawlSpecification, settings: {}):
        super().__init__(spec)
        self.log = shared.simple_logger("RemoteCrawlFinalizer", file_path=os.path.join(self.crawl_specification.logs,
                                                                                       self.crawl_specification.name,
                                                                                       "scrapy.log"))

    def finalize_crawl(self, data: {} = None):
        """This method is automatically called after the entire crawl has finished, gather the crawl results from
        the workspace (using filemanager) and compose an http request for further processing"""
        self.log.info(self.crawl_specification)

        if self.crawl_specification.parser == "parsers.ParagraphParser":
            finalized_flag = finalize_paragraphs(self.crawl_specification.name, self.crawl_specification.output, self.crawl_specification.logs, self.log)

        elif self.crawl_specification.parser == "parsers.RawParser":
            finalized_flag = finalize_raw(self.crawl_specification.name, self.crawl_specification.output, self.crawl_specification.logs, self.log)

        return finalized_flag

###
# Pipelines
###

class ContentPipeline:

    def open_spider(self, spider):
        spider.s_log.info(f" vvvvvvvvvvvvvvvvvvvvvvvvvvvv OPENING SPIDER {spider.name} vvvvvvvvvvvvvvvvvvvvvvvvvvvv")

    def close_spider(self, spider):
        spider.s_log.info(f" ^^^^^^^^^^^^^^^^^^^^^^^^^^^^ CLOSING SPIDER {spider.name} ^^^^^^^^^^^^^^^^^^^^^^^^^^^^")


class Paragraph2CsvPipeline(ContentPipeline):

    INCOMPLETE_FLAG = "-INCOMPLETE"

    def process_item(self, item, spider):

        df_item = dict()
        for key in item:
            # df_item = {"url": [url], "content": [content], "origin": [origin], "depth": [depth]}
            df_item[key] = [item[key]]

        url = item['url']
        domain = urlparse(url).netloc
        if domain in spider.allowed_domains:
            spider.s_log.debug("[process_item] - Adding content for {0} to {1}".format(str(url), str(spider.name)))

            fullpath = os.path.join(spider.crawl_specification.output, spider.name + self.INCOMPLETE_FLAG + ".csv")

            # careful, csv file may not exist for some reason (moved, deleted, ..)
            if os.path.exists(fullpath):
                df = pandas.DataFrame.from_dict(df_item)
                df.to_csv(fullpath, mode="a", sep=";", index=False, encoding="utf-8", header=False, line_terminator="")

        return item

    def open_spider(self, spider):
        super().open_spider(spider)
        # make sure output directory exists
        if not os.path.exists(spider.crawl_specification.output):
            os.makedirs(spider.crawl_specification.output, exist_ok=True)

        # initialize necessary csv data file
        fullpath = os.path.join(spider.crawl_specification.output, spider.name + self.INCOMPLETE_FLAG + ".csv")

        df = pandas.DataFrame(columns=["url", "content", "par_language", "page_language", "origin", "depth"])
        df.to_csv(fullpath, sep=";", index=False, encoding="utf-8")

    def close_spider(self, spider):
        super().close_spider(spider)

        fullpath_inc = os.path.join(spider.crawl_specification.output, spider.name + self.INCOMPLETE_FLAG + ".csv")
        fullpath_com = os.path.join(spider.crawl_specification.output, spider.name + ".csv")

        shutil.move(fullpath_inc, fullpath_com)


class Raw2FilePipeline(ContentPipeline):

    def process_item(self, item, spider):
        url = item["url"]
        content = item["content"]

        p_url = urlparse(url)
        domain = p_url.netloc
        if domain in spider.allowed_domains:
            domain_data_dir = os.path.join(spider.crawl_specification.output, spider.name)

            # careful, csv file may not exist for some reason (moved, deleted, ..)
            if not os.path.exists(domain_data_dir):
                os.makedirs(domain_data_dir, exist_ok=True)

            if "." in p_url.path.split("/")[-1]:
                filename = shared.url2filename(p_url.path)
            else:
                sep = ""
                if not p_url.path.endswith("/"):
                    sep = "/"

                filename = shared.url2filename(p_url.path + sep + "index.html")

            # abbreviate and uniquify long filenames
            cutoff = 100
            insert = "(...)"
            front = math.floor((cutoff - len(insert)) / 2)
            back = math.ceil((cutoff - len(insert)) / 2)
            if len(filename) > cutoff:
                spider.s_log.warning(f"Abbreviating very long filename ({len(filename)} characters): {filename}")
                filename = filename[:front] + insert + filename[-back:]
                fn_unique = filename
                unique = 1
                while os.path.exists(os.path.join(domain_data_dir, fn_unique)):
                    fn_unique = ".".join(filename.split(".")[:-1]) + f" ({unique})." + filename.split(".")[-1]
                    unique += 1
                filename = fn_unique


            with open(os.path.join(domain_data_dir, filename), "wb") as file:
                file.write(content)
                spider.s_log.debug(f"[process_item] - Added content for {url} to {spider.name}")

        return item
