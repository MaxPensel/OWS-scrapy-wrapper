"""Main module for scrapy worker."""

# add root to python path
import sys
sys.path.insert(1, '/app/OpenWebScraper')

# Local modules
from common.config import config
from common.logger import log

from remote.task_consumer import TaskConsumer

# Initialize task consumer
task_consumer = TaskConsumer(config.rmq['host'], config.rmq['port'], config.rmq['heartbeat'],
                             config.rmq['exchange_name'], config.rmq['exchange_type'],
                             config.rmq['task_queue'], config.rmq['task_routing_key'],
                             durable_queue=True)

# start consuming task (blocking connection)
log.info("Starting task consumer")
task_consumer.consume(auto_ack=False)
