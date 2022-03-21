from dataquery_processor import QueueController
from dataquery_processor import OrderProcessor
from dataquery_processor import get_config_path
import logging
import os
from logging.config import fileConfig


__package__ = 'dataquery_processor'

if os.path.exists(get_config_path('logging_config.ini')):
    fileConfig(get_config_path('logging_config.ini'))
logger = logging.getLogger(__name__)


def main():
    q = QueueController()
    message = q.read_message()
    if message:
        proc = OrderProcessor(message.order())
        if proc.process():
            logger.info("Job completed. Deleting message from queue")
            q.delete_message(message)
    else:
        logger.info("No new orders to process")


if __name__ == "__main__":
    main()
