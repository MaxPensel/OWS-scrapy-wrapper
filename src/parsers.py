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
import pipelines
from langdetect import detect, detect_langs
from langdetect.lang_detect_exception import LangDetectException
from scrapy import Item, Field
from textract_pdf.exceptions import CommandLineError


class ResponseParser:

    ACCEPTED_PIPELINES = []

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

    def errback(self, failure):
        self.log(logging.WARN, f"Rule failure on {failure.request.url}: {failure.value}")

    @staticmethod
    def generate_example_data():
        return {"<Data Key>": "<Data Value>"}


class ParagraphParser(ResponseParser):

    ACCEPTED_PIPELINES = [pipelines.Paragraph2CsvPipeline]

    KEY_KEEP_LANGDETECT_ERRORS = "keep_langdetect_errors"
    KEY_LANGUAGES = "allowed_languages"
    KEY_XPATHS = "xpaths"

    DEFAULT_ALLOWED_LANGUAGES = ["de", "en"]
    DEFAULT_XPATHS = ["//p", "//td"]

    V_DISABLED = "disabled"
    V_ANY = "any"

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
                items.extend(self.process_paragraph(response, par_content, origin=xp))

        self.log(logging.INFO, "[parse_html] - Matched {0} paragraphs in {1}".format(len(items), response.url))

        items = self.detect_language(items)

        return items

    def parse_pdf(self, response):
        tmp_file = tempfile.NamedTemporaryFile(suffix=".pdf", prefix="scrapy_", delete=False)
        tmp_file.write(response.body)
        tmp_file.close()

        try:
            content = textract_pdf.process(tmp_file.name)
        except CommandLineError as exc:  # Catching either ExtensionNotSupported or MissingFileError
            self.log(logging.ERROR, "[parse_pdf] - {0}: {1}".format(type(exc).__name__, exc))
            # Cleanup temporary pdf file
            os.unlink(tmp_file.name)
            return []  # In any case, text extraction failed so no items were parsed

        content = content.decode("utf-8")  # convert byte string to utf-8 string
        items = []
        for par_content in content.splitlines():
            items.extend(self.process_paragraph(response, par_content, origin="pdf"))

        # Cleanup temporary pdf file
        os.unlink(tmp_file.name)

        self.log(logging.INFO, "[parse_pdf] - Matched {0} paragraphs in {1}".format(len(items), response.url))

        items = self.detect_language(items)

        return items

    def process_paragraph(self, response, par_content, origin):
        """ Supplement paragraph data with detected language, supplement with 'None' if disabled """
        items = []

        if par_content.strip():  # immediately ignore empty or only whitespace paragraphs
            try:
                if ParagraphParser.V_DISABLED in self.data[ParagraphParser.KEY_LANGUAGES]:
                    lang = None
                else:
                    lang = detect(par_content)
                # if "any" or lang in self.data[ParagraphParser.KEY_LANGUAGES]:
                items.append(ParagraphItem(url=response.url,
                                           content=par_content,
                                           par_lang=lang,
                                           page_lang=None,
                                           origin=origin,
                                           depth=response.meta["depth"]))
                self.register_paragraph_language(lang)
            except LangDetectException as exc:
                self.log(logging.WARN, "[process_paragraph] - "
                                       "{0} on langdetect input '{1}'."
                                       .format(exc, par_content))
                items.append(ParagraphItem(url=response.url,
                                           content=par_content,
                                           par_lang=exc,
                                           page_lang=None,
                                           origin=origin,
                                           depth=response.meta["depth"]))
                self.register_paragraph_language(str(exc))

        return items

    def detect_language(self, items):
        """ Filter out all items of a response depending on their detected language, don't filter if disabled """
        if ParagraphParser.V_DISABLED in self.data[ParagraphParser.KEY_LANGUAGES]:
            return items

        all_content = " ".join([item["content"] for item in items])
        try:
            languages = detect_langs(all_content)
            self.log(logging.INFO,
                     "[detect_language] - Language distribution on {0} paragraphs: {1}".format(len(items), languages))

            if ParagraphParser.V_ANY in self.data[ParagraphParser.KEY_LANGUAGES]:
                # if "any" language is accepted, store page language probabilities
                for item in items:
                    item["page_lang"] = str(languages)
                return items
            else:
                # accept all paragraphs if the chance that their combination matches one of the accepted languages
                # is greater than 0.5
                for lang in languages:
                    if lang.lang in self.data[ParagraphParser.KEY_LANGUAGES] and lang.prob > 0.5:
                        # add page_lang info to each item
                        for item in items:
                            item["page_lang"] = lang.lang
                        return items
        except LangDetectException as exc:
            if self.data[ParagraphParser.KEY_KEEP_LANGDETECT_ERRORS]:
                self.log(logging.WARN, "[process_paragraph] - {0} on langdetect input '{1}'.")
                return items


        # none of the accepted languages was even remotely present
        return []

    def register_paragraph_language(self, lang):
        """ Keep track of discovered languages (deprecated) """
        if lang not in self.detected_languages:
            self.detected_languages[lang] = 0
        self.detected_languages[lang] += 1

    @staticmethod
    def generate_example_data():
        return {ParagraphParser.KEY_LANGUAGES: ["de", "en", "any", "disabled"],
                ParagraphParser.KEY_KEEP_LANGDETECT_ERRORS: False,
                ParagraphParser.KEY_XPATHS: ["//p", "//h1", "//h2"]}


class RawParser(ResponseParser):

    ACCEPTED_PIPELINES = [pipelines.Raw2FilePipeline]

    KEY_ALLOWED_CONTENT_TYPES = "allowed_content_type"

    DEFAULT_ALLOWED_CONTENT_TYPES = ["text/html", "application/pdf"]

    def __init__(self, data: {} = None, spider=None):
        super().__init__(data=data, spider=spider)

        # set defaults
        if RawParser.KEY_ALLOWED_CONTENT_TYPES not in self.data:
            self.data[RawParser.KEY_ALLOWED_CONTENT_TYPES] = RawParser.DEFAULT_ALLOWED_CONTENT_TYPES

        for ct in self.data[RawParser.KEY_ALLOWED_CONTENT_TYPES]:
            self.callbacks[ct] = self.parse_response

    def parse_response(self, response):
        if hasattr(response, 'text'):
            cont = response.text
            log_note = "as text"
        else:
            cont = response.body
            log_note = "as bytes"

        self.log(logging.INFO, f"Storing response {response} {log_note}")

        return [RawContentItem(url=response.url, content=cont, depth=response.meta["depth"])]

    @staticmethod
    def generate_example_data():
        return {RawParser.KEY_ALLOWED_CONTENT_TYPES: RawParser.DEFAULT_ALLOWED_CONTENT_TYPES}


###
# Scrapy item definitions
###

class ParagraphItem(Item):
    url = Field()
    content = Field()
    par_lang = Field()
    page_lang = Field()
    origin = Field()
    depth = Field()


class RawContentItem(Item):
    url = Field()
    content = Field()
    depth = Field()
