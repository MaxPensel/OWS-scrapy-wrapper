"""Main module for scrapy worker."""

# add root to python path
import sys
sys.path.insert(1, '/app/OpenWebScraper')

# Local modules
from common.config import config
from common.logger import log

from remote.task_consumer import task_consumer

# import time
# while True:
#     time.sleep(50)

# start consuming task (blocking connection)
log.info("Starting task consumer")
task_consumer.consume(auto_ack=False)
