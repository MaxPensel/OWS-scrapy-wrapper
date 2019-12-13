import pika

from common.config import config
from common.logger import log

from common.messaging.consumer import Consumer
from scrapy_wrapper import run_crawl

class TaskConsumer(Consumer):
    """Consumes crawl tasks."""

    def __init__(self, mq_host, mq_port, heartbeat, exchange_name, exchange_type,
                 task_queue_name, task_routing_key, durable_queue=True):
        Consumer.__init__(self, mq_host, mq_port, heartbeat, exchange_name, exchange_type, task_queue_name, task_routing_key, durable_queue=durable_queue)

    def callback(self, ch, method, properties, body):
        """Action performed when recreiving messages."""

        log.info("Received task with body {}".format(body))

        try:
            # Ignore messages that are redelivered
            if method.redelivered == True:
                log.info("Task discarded as copy")
                ch.basic_ack(delivery_tag=method.delivery_tag)
            else:
                log.info("Start processing task")
                # Execute crawling task
                log.info("Execute crawl with spec: {}".format(body))
                finished = run_crawl(body, worker_flag=True)
                log.info("Scrapy worker finished: {}".format(finished))
                # Unsubscribe before sending ack to avoid accepting a task on closing
                log.info("Unsubscribing task queue.")
                self.channel.basic_cancel(self.consumer_tag)
                # send acc before closing
                ch.basic_ack(delivery_tag=method.delivery_tag)
                log.info("Finished processing task")
                exit()

        except Exception as e:
            log.exception(e)
            # ch.basic_ack(delivery_tag=method.delivery_tag)
            # log.info("Failed processing task")
            raise


# Initialize task consumer
task_consumer = TaskConsumer(config.rmq['host'], config.rmq['port'], config.rmq['heartbeat'],
                             config.rmq['exchange_name'], config.rmq['exchange_type'],
                             config.rmq['task_queue'], config.rmq['task_routing_key'],
                             durable_queue=True)
