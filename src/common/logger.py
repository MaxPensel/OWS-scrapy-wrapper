# https://inventwithpython.com/blog/2012/04/06/stop-using-print-for-debugging-a-5-minute-quickstart-guide-to-pythons-logging-module/
# https://www.loggly.com/blog/4-reasons-a-python-logging-library-is-much-better-than-putting-print-statements-everywhere/
import logging

log = logging.getLogger()
log.setLevel(logging.INFO)

# formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
# truncate message to 300 charachters
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message).300s - %(processName)s %(filename)s:%(lineno)s')

# # log to file
# fh = logging.FileHandler('log.txt')
# fh.setLevel(logging.DEBUG)
# fh.setFormatter(formatter)
# log.addHandler(fh)

# log to stream
ch = logging.StreamHandler()
ch.setLevel(logging.INFO)
ch.setFormatter(formatter)
log.addHandler(ch)

# log.debug('This is a test log message.')
