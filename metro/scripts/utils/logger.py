import logging

logger = logging.getLogger(__name__)


class Logger:
    def __init__(self):
        logging.basicConfig(level=logging.INFO)

    def info(self, message):
        logger.info(message)

    def error(self, message):
        logger.error(message)

    def debug(self, message):
        logger.debug(message)
