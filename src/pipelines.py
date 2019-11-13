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
from urllib.parse import urlparse

import pandas
import shared
from shared import CrawlSpecification

from remote.result_producer import send_result
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

        self.log.info("Finalizing crawl ...")

        # maximum size for message chunk (100 MB)
        max_message_size = 104857600

        if data is None:
            data = dict()

        # fetching crawl results
        for csv_filename in os.listdir(self.crawl_specification.output):
            # create filename without extension
            filename = ""
            if csv_filename.endswith('.csv'):
                filename = csv_filename[:-4]
            # create csv file path
            csv_filepath = os.path.join(self.crawl_specification.output, csv_filename)
            # initialize data
            data['crawl'] = self.crawl_specification.name
            data['filename'] = filename
            self.log.info("data: {}".format(data))

            # read df
            df = pandas.read_csv(csv_filepath, sep=';', quotechar='"', encoding="utf-8")
            # self.log.info(df)
            # get filesize to estimate number of required messages
            result_size = os.path.getsize(csv_filepath)
            self.log.info("Result size: {}".format(result_size))

            # split df into chunks if size is larger than max_message_size
            if result_size > max_message_size:
                self.log.info("Multiple messages required!")
                # number of required chunks
                chunk_count =  math.ceil(result_size / max_message_size)
                chunks = np.array_split(df, chunk_count)
                #self.log.info(chunks)
                # send all chunks to queue
                for index, chunk in enumerate(chunks):
                    #self.log.info(chunk)
                    filename_part = "{}_part{}".format(filename, index)
                    data['filename'] = filename_part
                    data['data'] = chunk.to_csv(index=False, sep=';')
                    #self.log.info(data['data'])
                    send_flag = send_result(data)
            # send complete dictionary
            else:
                self.log.info("One message required!")
                data['data'] = df.to_csv(index=False, sep=';')
                #self.log.info(data['data'])
                send_flag = send_result(data)


        # # fetching log contents
        # for log_filename in os.listdir(os.path.join(self.crawl_specification.logs, self.crawl_specification.name)):
        #     log_filepath = os.path.abspath(log_filename)
        #     with open(log_filepath, mode="r", encoding="utf-8") as log_filename:
        #         log_content = log_filename.read()
        #         # TODO: add this content to a dict in order to compose http request
        #
        # # fetching log contents
        # log_path = os.path.join(WorkspaceManager().get_log_path(), self.crawl_specification.name)
        # self.log.info(log_path)
        # for log_filename in os.listdir(log_path):
        #     log_filepath = os.path.join(log_path, log_filename)
        #     self.log.info(log_filepath)
        #     with open(log_filepath, mode="r", encoding="utf-8") as logfile:
        #         log_content = logfile.read()
        #         self.log.info(log_content)

        # Clear directories
        self.log.info("Clearing data directory.")
        data_path = self.crawl_specification.output
        delted = shutil.rmtree(data_path, ignore_errors=True)

        self.log.info("Clearing log directory.")
        log_path = self.crawl_specification.logs
        # shutil.rmtree(log_path, ignore_errors=True)
        for log_filename in os.listdir(log_path):
            log_file_path = os.path.join(log_path, log_filename)
            # try to delete log files
            try:
                os.remove(log_file_path)
            # otherwise set empty text file
            except:
                self.log.info("{} is still busy. Try to delete at next crawl.".format(log_filename))
                if log_filename.endswith('.log'):
                    with open(log_file_path , "w") as text_file:
                        text_file.write("")
                # if "log_file" is identified as folder, remove crawl specific log
                else:
                    with open(os.path.join(log_file_path, "scrapy.log") , "w") as text_file:
                        text_file.write("")

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
