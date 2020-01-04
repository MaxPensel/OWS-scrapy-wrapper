"""Producer object."""

import pika
from datetime import datetime as dt

from common.logger import log
from .establish_rmq_connection import establish_rmq_connection


class Producer(object):
    """Producer for sending messages into queues.

    Inactive producer does not send heartbeats. Therefore, connection has to be
    restablished after certain time of inactivity.
    """

    def __init__(self, mq_host, mq_port, heartbeat):
        self.mq_host = mq_host
        self.mq_port = mq_port
        self.heartbeat = heartbeat
        # establish connection
        self.establish_connection()


    def establish_connection(self):
        """Establish connection to rmq server."""

        self.connection = establish_rmq_connection(self.mq_host, self.mq_port, self.heartbeat)
        self.channel = self.connection.channel()
        # initialize last hearbeat time stamp
        self.last_heartbeat = dt.now()


    def publish(self, exchange_name, exchange_type, queue_name, routing_key, message, durable_queue=True):
        """Publish message to queue.

        Reestablishes connection if expired."""

        # check if connection is expired and reestablish connection
        if (dt.now() - self.last_heartbeat).total_seconds() >= self.heartbeat:
            self.establish_connection()

        # create exchange
        self.channel.exchange_declare(exchange=exchange_name, exchange_type=exchange_type)

        # initialize durable queue and mark messages as persistent if required
        if durable_queue == True:
            # create route
            self.channel.queue_declare(queue=queue_name, durable=durable_queue)
            # create queue
            self.channel.queue_bind(exchange=exchange_name, queue=queue_name, routing_key=routing_key)
            # set delivery mode
            delivery_mode = 2
        else:
            delivery_mode = 1


        self.channel.basic_publish(
            exchange=exchange_name,
            routing_key=routing_key,
            body=message,
            properties=pika.BasicProperties(
                delivery_mode=delivery_mode,  # make message persistent (delivery_mode=2)
            ))

        log.info("Message sent to queue {} with routing key {} and body {}.".format(queue_name, routing_key, message))


    def close(self):
        """Close producer connection to rmq server."""

        log.info("Close producer connection")
        self.connection.close()
