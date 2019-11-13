"""Consumer object."""

import pika
from datetime import datetime as dt

from modules.crawler.common.logger import log
from .establish_rmq_connection import establish_rmq_connection


class Consumer(object):
    """Consumer for receiving messages from queues.

    Inactive consumer does not send heartbeats. Therefore, connection has to be
    restablished after certain time of inactivity.
    """

    def __init__(self, mq_host, mq_port, heartbeat, exchange_name, exchange_type, queue_name, routing_key, durable_queue=True):
        self.mq_host = mq_host
        self.mq_port = mq_port
        self.heartbeat = heartbeat
        self.exchange_name = exchange_name
        self.exchange_type = exchange_type
        self.queue_name = queue_name
        self.routing_key = routing_key
        self.durable_queue = durable_queue


    def establish_connection(self):
        """Establish connection to rmq server."""

        self.connection = establish_rmq_connection(self.mq_host, self.mq_port, self.heartbeat)
        self.channel = self.connection.channel()
        # initialize last hearbeat time stamp
        self.last_heartbeat = dt.now()


    def consume(self, auto_ack=False):
        """Start consuming messages on given queue"""
        # establish connection
        self.establish_connection()

        # create exchange
        self.channel.exchange_declare(exchange=self.exchange_name, exchange_type=self.exchange_type)

        # if durable queue is required, create queue with selected name, otherwise create exlusive queue
        if self.durable_queue == True:
            # create route
            self.channel.queue_declare(queue=self.queue_name, durable=self.durable_queue)
        else:
            result = self.channel.queue_declare('', exclusive=True)
            self.queue_name = result.method.queue

        # bind queue to exchange and routing key
        self.channel.queue_bind(exchange=self.exchange_name, queue=self.queue_name, routing_key=self.routing_key)

        # accept only one message at once
        self.channel.basic_qos(prefetch_count=1)

        # set queue to be consumed
        # enable auto ack if no durable queue is required
        self.consumer_tag = self.channel.basic_consume(queue=self.queue_name, on_message_callback=self.callback, auto_ack=auto_ack)

        log.info("Start consuming messages on queue {} with routing key {}".format(self.queue_name, self.routing_key))
        log.info("...") # Due to not showing message above

        self.channel.start_consuming()


    def callback(self, ch, method, properties, body):
        """Action performed when recreiving messages.

        WARNING: Function has to be rewritten when implementing actual consumer.
        """

        log.info("Message received with body {}".format(body))
        # Send acknowledgment that message has been processed
        ch.basic_ack(delivery_tag=method.delivery_tag)
        log.info("Message processed")


    def close(self):
        """Close consumer connection to rmq server."""

        log.info("Stop consuming messages on queue {} with routing key {}.".format(self.queue_name, self.routing_key))

        # For unknown reasons sometimes StreamLostError: ("Stream connection lost: IndexError('pop from an empty deque') occures while closing
        try:
            self.channel.basic_cancel(self.consumer_tag)
            self.connection.close()
        except Exception as ex:
            log.exception("Error while closing consumer: {}".format(ex))
            pass
