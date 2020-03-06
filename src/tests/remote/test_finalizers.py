import pytest
import os

import shared
from remote.finalizers import finalize_paragraphs, finalize_raw


def test_finalize_paragraphs():
    """Correct finalization for crawl with csv data."""

    logger = shared.simple_logger("RemoteCrawlFinalizer", file_path=os.path.join('result_logs',
                                                                               'my_crawl',
                                                                               "scrapy.log"))

    finalize_paragraphs('my_crawl', 'result_data', 'result_logs', logger)


    assert True == False



def test_finalize_raw():
    """Correct finalization for crawl with raw data."""

    logger = shared.simple_logger("RemoteCrawlFinalizer", file_path=os.path.join('result_logs',
                                                                               'my_crawl',
                                                                               "scrapy.log"))

    finalize_raw('my_crawl', 'result_data', 'result_logs', logger)

    assert True == False
