"""Finalize remote crawls.

Either paragraphs or raw html.

"""

import os
import pandas
import shutil
import time
from remote.result_producer import send_result


def finalize_paragraphs(crawl_name, data_path, log_path, logger):
    """Finalize paragraph crawl."""

    logger.info("Finalizing paragraph crawl ...")

    # maximum size
    max_message_size = 104857600 #  for message chunk (100 MB)
    max_message_size = 10485760 #  for message chunk (10 MB)

    data = dict()

    data['raw'] = False
    data['crawl'] = crawl_name

    # fetching crawl results
    for csv_filename in os.listdir(data_path):
        # create filename without extension
        filename = ""
        if csv_filename.endswith('.csv'):
            filename = csv_filename[:-4]
        # create csv file path
        csv_filepath = os.path.join(data_path, csv_filename)
        # initialize data
        data['url'] = filename
        data['filename'] = filename
        logger.info("data: {}".format(data))

        # read df
        df = pandas.read_csv(csv_filepath, sep=';',
                             quotechar='"', encoding="utf-8")
        # logger.info(df)
        # get filesize to estimate number of required messages
        result_size = os.path.getsize(csv_filepath)
        logger.info("Result size: {}".format(result_size))

        # split df into chunks if size is larger than max_message_size
        if result_size > max_message_size:
            logger.info("Multiple messages required!")
            # number of required chunks
            chunk_count = math.ceil(result_size / max_message_size)
            chunks = np.array_split(df, chunk_count)
            # logger.info(chunks)
            # send all chunks to queue
            for index, chunk in enumerate(chunks):
                # logger.info(chunk)
                filename_part = "{}_part{}_{}".format(
                  filename, index + 1, chunk_count)
                data['filename'] = filename_part
                data['data'] = chunk.to_csv(index=False, sep=';')
                # logger.info(data['data'])
                time.sleep(0.1)
                send_flag = send_result(data)
        # send complete dictionary
        else:
            logger.info("One message required!")
            data['data'] = df.to_csv(index=False, sep=';')
            # logger.info(data['data'])
            time.sleep(0.1)
            send_flag = send_result(data)

    # # fetching log contents
    # for log_filename in os.listdir(os.path.join(self.crawl_specification.logs, self.crawl_specification.name)):
    #     log_filepath = os.path.abspath(log_filename)
    #     with open(log_filepath, mode="r", encoding="utf-8") as log_filename:
    #         log_content = log_filename.read()
    #         # TODO: add this content to a dict in order to compose http request
    #
    # # fetching log contents
    # log_path = os.path.join(WorkspaceManager().get_log_path(), self.crawl_specification.name)
    # logger.info(log_path)
    # for log_filename in os.listdir(log_path):
    #     log_filepath = os.path.join(log_path, log_filename)
    #     logger.info(log_filepath)
    #     with open(log_filepath, mode="r", encoding="utf-8") as logfile:
    #         log_content = logfile.read()
    #         logger.info(log_content)

    # Clear directories
    cleared_flag = clear_directories(data_path, log_path, logger)

    logger.info("Done finalizing paragraph crawl.")

    return True




def finalize_raw(crawl_name, data_path, log_path, logger):
    """Finalize paragraph crawl."""

    logger.info("Finalizing raw crawl ...")

    data = dict()
    data['crawl'] = crawl_name
    data['raw'] = True

    # get top level folders
    root, dirs, files = next(os.walk(data_path))

    # read data of all dirs (only 1 if 1 url per task)
    for dir in dirs:
        data['url'] = dir
        # read all files in url folder
        for filename in os.listdir(os.path.join(root, dir)):
            # set filename in rmq data
            data['filename'] = filename
            # create path to read data
            filepath = os.path.join(root, dir, filename)
            # process html files
            if filepath.endswith('.html'):
                with open(filepath, "r", encoding='utf-8') as f:
                    text = f.read()
                    data['data'] = text
                    time.sleep(0.1)
                    send_flag = send_result(data)

    # Clear directories
    cleared_flag = clear_directories(data_path, log_path, logger)

    logger.info("Done finalizing raw crawl.")

    return True


def clear_directories(data_path, log_path, logger):
    """Clear result and log data."""

    # Clear directories
    logger.info("Clearing data directory {}.".format(data_path))
    delted = shutil.rmtree(data_path, ignore_errors=True)

    logger.info("Clearing log directory {}.".format(log_path))
    # shutil.rmtree(log_path, ignore_errors=True)
    for log_filename in os.listdir(log_path):
        log_file_path = os.path.join(log_path, log_filename)
        # try to delete log files
        try:
            os.remove(log_file_path)
        # otherwise set empty text file
        except:
            logger.info(
                "{} is still busy. Try to delete at next crawl.".format(log_filename))
            if log_filename.endswith('.log'):
                with open(log_file_path, "w") as text_file:
                    text_file.write("")
            # if "log_file" is identified as folder, remove crawl specific log
            else:
                with open(os.path.join(log_file_path, "scrapy.log"), "w") as text_file:
                    text_file.write("")

    return True
