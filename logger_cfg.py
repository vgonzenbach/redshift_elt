"""Helps set up logger for all files"""
import logging
import os

def setup_logger(filepath):
    """Set ups logger specific to each file"""
    logger = logging.getLogger(os.path.basename(filepath))
    logger.setLevel(logging.DEBUG)

    formatter = logging.Formatter('[%(asctime)s][%(name)s][%(levelname)s] %(message)s')

    stdout_handler = logging.StreamHandler()
    stdout_handler.setFormatter(formatter)

    logger.addHandler(stdout_handler)
    
    return logger

