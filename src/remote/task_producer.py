"""Task messaging"""

import json

from common.config import config

from common.messaging.producer import Producer

task_producer = Producer(config.rmq['host'], config.rmq['port'], config.rmq['heartbeat'])

def send_task(task_data):
    """Send task (dict) to queue."""

    # send task
    task_producer.publish(config.rmq['exchange_name'],
                          config.rmq['exchange_type'],
                          config.rmq['task_queue'],
                          config.rmq['task_routing_key'],
                          json.dumps(task_data),
                          durable_queue=True)
