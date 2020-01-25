import logging
from typing import Optional

from libreary.version import VERSION
from libreary.libreary import Libreary
from libreary.exceptions import *
from libreary.adapter_manager import AdapterManager
from libreary.ingester import Ingester
from libreary.scheduler import Scheduler

__author__ = 'Ben Glick'
__version__ = VERSION


AUTO_LOGNAME = -1

"""
Logging
-------
Following the general logging philosophy of python libraries, by default
LIBREary doesn't log anything.

However the following helper functions are provided for logging:
1. set_stream_logger
    This sets the logger to the StreamHandler. This is quite useful when working from
    a Jupyter notebook.
2. set_file_logger
    This sets the logging to a file. This is ideal for reporting issues to the dev team.
Constants
---------
AUTO_LOGNAME
    Special value that indicates libreary should construct a filename for logging.
"""


def set_stream_logger(name: str = 'libreary', level: int = logging.DEBUG,
                      format_string: Optional[str] = None):
    """Add a stream log handler.
    Args:
         - name (string) : Set the logger name.
         - level (logging.LEVEL) : Set to logging.DEBUG by default.
         - format_string (string) : Set to None by default.
    Returns:
         - None
    """
    if format_string is None:
        # format_string = "%(asctime)s %(name)s [%(levelname)s] Thread:%(thread)d %(message)s"
        format_string = "%(asctime)s %(name)s:%(lineno)d [%(levelname)s]  %(message)s"

    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)
    handler = logging.StreamHandler()
    handler.setLevel(level)
    formatter = logging.Formatter(format_string, datefmt='%Y-%m-%d %H:%M:%S')
    handler.setFormatter(formatter)
    logger.addHandler(handler)

    # Concurrent.futures errors are also of interest, as exceptions
    # which propagate out of the top of a callback are logged this way
    # and then discarded. (see #240)
    futures_logger = logging.getLogger("concurrent.futures")
    futures_logger.addHandler(handler)


def set_file_logger(filename: str, name: str = 'libreary',
                    level: int = logging.DEBUG, format_string: Optional[str] = None):
    """Add a stream log handler.
    Args:
        - filename (string): Name of the file to write logs to
        - name (string): Logger name
        - level (logging.LEVEL): Set the logging level.
        - format_string (string): Set the format string
    Returns:
       -  None
    """
    if format_string is None:
        format_string = "%(asctime)s.%(msecs)03d %(name)s:%(lineno)d [%(levelname)s]  %(message)s"

    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)
    handler = logging.FileHandler(filename)
    handler.setLevel(level)
    formatter = logging.Formatter(format_string, datefmt='%Y-%m-%d %H:%M:%S')
    handler.setFormatter(formatter)
    logger.addHandler(handler)

    # see note in set_stream_logger for notes about logging
    # concurrent.futures
    futures_logger = logging.getLogger("concurrent.futures")
    futures_logger.addHandler(handler)


class NullHandler(logging.Handler):
    """Setup default logging to /dev/null since this is library."""

    def emit(self, record):
        pass


__all__ = ["Libreary", "AdapterManager", "Ingester", "Scheduler", "set_stream_logger",
           "set_file_logger", "AUTO_LOGNAME"]


logging.getLogger('libreary').addHandler(NullHandler())
