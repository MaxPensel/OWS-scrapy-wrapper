import json

from modules.crawler.common.config import config

from modules.crawler.common.messaging.producer import Producer


result_producer = Producer(config.rmq['host'], config.rmq['port'], config.rmq['heartbeat'])

def send_result(result_data):
    """Send task (dict) to queue."""

    # send task
    result_producer.publish(config.rmq['exchange_name'],
                          config.rmq['exchange_type'],
                          config.rmq['result_queue'],
                          config.rmq['result_routing_key'],
                          json.dumps(result_data),
                          durable_queue=True)

    return True
