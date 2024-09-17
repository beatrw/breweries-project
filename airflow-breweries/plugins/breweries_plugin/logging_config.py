import logging


def configure_logging():
    log_format = "%(asctime)s | %(levelname)-8s | %(message)s"
    logging.basicConfig(
        level=logging.INFO,
        format=log_format,
        handlers=[
            logging.StreamHandler()
        ]
    )

    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    return logger
