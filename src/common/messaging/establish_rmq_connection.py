"""Establish connection to rabbitmq server."""

import pika
import socket
import time

from common.logger import log


def isOpen(ip, port, timeout):
    """Check if port on host is open."""

    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.settimeout(timeout)
    try:
            # log.debug("Checking if {} is UP...".format(ip))
            s.connect((ip, int(port)))
            s.shutdown(socket.SHUT_RDWR)
            return True
    except:
            return False
    finally:
            s.close()


def checkHost(ip, port, retry, delay, timeout):
    """Check if ip is reachable on port."""

    ipup = False
    for i in range(retry):
        if isOpen(ip, port, timeout):
            ipup = True
            break
        else:
            time.sleep(delay)
    return ipup


def establish_rmq_connection(ip, port, heartbeat):
    """Establish a connection to rabbit mq server.

    Retries until rabbit mq server is available."""

    # initialize rmq server state
    rmq_up = False
    # try to connect to rmq server
    while rmq_up == False:
        log.info("Trying to establish connection to rabbitmq at {}...".format(ip))
        rmq_up = checkHost(ip, port, 10, 3, 3)

    # establish rmq connection
    params = pika.ConnectionParameters(host=ip, heartbeat=heartbeat)
    connection = pika.BlockingConnection(params)
    log.info("Connection to rabbitmq established at {} with hearbeart interval of {}.".format(ip, heartbeat))

    return connection
