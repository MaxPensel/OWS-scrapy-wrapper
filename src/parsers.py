"""
Created on 19.09.2019

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

import logging
import os
import tempfile

import textract_pdf
from langdetect import detect
from langdetect.lang_detect_exception import LangDetectException
from scrapy import Item, Field
from textract_pdf.exceptions import CommandLineError


class ResponseParser:

    def __init__(self, callbacks=None, data: {} = None, spider=None):
        if callbacks is None:
            callbacks = dict()
        self.callbacks = callbacks
        if data is None:
            data = dict()
        self.data = data
        self.spider = spider

    def parse(self, response):
        content_type = str(response.headers.get(b"Content-Type", "").lower())
        for ctype in self.callbacks:
            if ctype in content_type:
                return self.callbacks[ctype](response)

        self.log(logging.WARN, "No callback found to parse content type '{0}'".format(content_type))

    def log(self, level, message):
        if self.spider:
            self.spider.s_log.log(level, message)
        else:
            print("[{0}] {1}".format(level, message))


class ParagraphParser(ResponseParser):

    KEY_KEEP_LANGDETECT_ERRORS = "keep_langdetect_errors"
    KEY_LANGUAGES = "allowed_languages"
    KEY_XPATHS = "xpaths"

    DEFAULT_ALLOWED_LANGUAGES = ["de", "en"]
    DEFAULT_XPATHS = ["//p", "//td"]

    def __init__(self, data: {} = None, spider=None):
        super().__init__(data=data, spider=spider)

        # set defaults
        if ParagraphParser.KEY_XPATHS not in self.data:
            self.data[ParagraphParser.KEY_XPATHS] = ParagraphParser.DEFAULT_XPATHS
        if ParagraphParser.KEY_LANGUAGES not in self.data:
            self.data[ParagraphParser.KEY_LANGUAGES] = ParagraphParser.DEFAULT_ALLOWED_LANGUAGES
        if ParagraphParser.KEY_KEEP_LANGDETECT_ERRORS not in self.data:
            self.data[ParagraphParser.KEY_KEEP_LANGDETECT_ERRORS] = True

        self.callbacks["text/html"] = self.parse_html
        self.callbacks["application/pdf"] = self.parse_pdf

        self.detected_languages = dict()

    def parse_html(self, response):
        items = []

        for xp in self.data[ParagraphParser.KEY_XPATHS]:
            paragraphs = response.xpath(xp)
            for par in paragraphs:
                par_content = "".join(par.xpath(".//text()").extract())
                items.extend(self.process_paragraph(response, par_content))

        return items

    def parse_pdf(self, response):
        tmp_file = tempfile.TemporaryFile(suffix=".pdf", prefix="scrapy_", delete=False)
        tmp_file.write(response.body)
        tmp_file.close()
        try:
            content = textract_pdf.process(tmp_file.name)
        except CommandLineError as exc:  # Catching either ExtensionNotSupported or MissingFileError
            self.log(logging.ERROR, "[parse_pdf] - {0}: {1}".format(type(exc).__name__, exc))
            return []  # In any case, text extraction failed so no items were parsed

        content = content.decode("utf-8")  # convert byte string to utf-8 string
        items = []
        for par_content in content.splitlines():
            items.extend(self.process_paragraph(response, par_content))

        # Cleanup temporary pdf file
        os.unlink(tmp_file.name)

        return items

    def process_paragraph(self, response, par_content):
        items = []

        if par_content.strip():  # immediately ignore empty or only whitespace paragraphs
            try:
                lang = detect(par_content)
                if lang in self.data[ParagraphParser.KEY_LANGUAGES]:
                    items.append(ParagraphItem(url=response.url, content=par_content, depth=response.meta["depth"]))
                self.register_paragraph_language(lang)
            except LangDetectException as exc:
                if self.data[ParagraphParser.KEY_KEEP_LANGDETECT_ERRORS]:
                    self.log(logging.WARN, "[process_paragraph] - "
                                           "{0} on langdetect input '{1}'. You chose to store the content anyway!"
                                           .format(exc, par_content))
                    items.append(ParagraphItem(url=response.url, content=par_content, depth=response.meta["depth"]))
                    self.register_paragraph_language(str(exc))

        return items

    def register_paragraph_language(self, lang):
        if lang not in self.detected_languages:
            self.detected_languages[lang] = 0
        self.detected_languages[lang] += 1


###
# Scrapy item definitions
###

class ParagraphItem(Item):
    url = Field()
    content = Field()
    depth = Field()


class RawContentItem(Item):
    url = Field()
    content = Field()
    depth = Field()
