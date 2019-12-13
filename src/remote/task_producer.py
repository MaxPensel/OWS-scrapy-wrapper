"""Task messaging"""

import json

from common.config import config

from common.messaging.producer import Producer

task_producer = None

def send_task(task_data):
    """Send task (dict) to queue."""
    global task_producer

    if not task_producer:
        # Does the task_producer need to be global? It Establishes a connection on importing this file
        task_producer = Producer(config.rmq['host'], config.rmq['port'], config.rmq['heartbeat'])

    # send task
    task_producer.publish(config.rmq['exchange_name'],
                          config.rmq['exchange_type'],
                          config.rmq['task_queue'],
                          config.rmq['task_routing_key'],
                          json.dumps(task_data),
                          durable_queue=True)
