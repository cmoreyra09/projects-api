import logging
from logging.handlers import RotatingFileHandler
import os

def setup_logger(log_file, logger_name):

    # If the file dont existe , create file
    log_dir = os.path.dirname(log_file)
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)


    # prevent duplicate logs
    logger = logging.getLogger(logger_name)
    if not logger.handlers:
        logger.setLevel(logging.DEBUG)
        
        # Handler for the log file
        file_handler = RotatingFileHandler(log_file, maxBytes=10**6, backupCount=3)
        file_formatter = logging.Formatter('%(asctime)s  |  %(levelname)s  |  %(message)s')
        file_handler.setFormatter(file_formatter)
        
        logger.addHandler(file_handler)

    return logger

