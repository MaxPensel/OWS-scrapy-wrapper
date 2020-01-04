import json

from common.config import config

from common.messaging.producer import Producer


result_producer = None


def send_result(result_data):
    """Send task (dict) to queue."""
    global result_producer

    if not result_producer:
        # Does the result_producer need to be global? It Establishes a connection on importing this file
        result_producer = Producer(config.rmq['host'], config.rmq['port'], config.rmq['heartbeat'])

    # send task
    result_producer.publish(config.rmq['exchange_name'],
                          config.rmq['exchange_type'],
                          config.rmq['result_queue'],
                          config.rmq['result_routing_key'],
                          json.dumps(result_data),
                          durable_queue=True)

    return True
