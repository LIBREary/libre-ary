import sqlite3
import os
from shutil import copyfile
import hashlib
import uuid
import json
from typing import List
import logging

from libreary.exceptions import ChecksumMismatchException

logger = logging.getLogger(__name__)

class SQLite3MetadataManager(object):
    """docstring for SQLite3MetadataManager

    SQLite3 Metadata Manager is the most basic. 

    It expects a SQLite3 file, formatted as described in
    the LIBREary documentation.

    This object contains the following methods:

    - verify_db_structure

    """
    def __init__(self, config):
        """
        Constructor for the MedadataManager object. This object can be created manually, but
        in most cases, it will be constructed by the LIBRE-ary main object. It expects a python dict
        :param config, which should be structured as follows:
        ```{json}
        {
        "db_file": "path to SQLite3 DB file for metadata"
        }
        ```
        """
        try:
            self.metadata_db = os.path.realpath(
                config.get("db_file"))
            self.conn = sqlite3.connect(self.metadata_db)
            self.cursor = self.conn.cursor()
            logger.debug("Metadata Manager Configuration Valid. Creating Metadata Manager")
        except KeyError:
            logger.error("Ingester Configuration Invalid")
            raise KeyError

    def verify_db_structure(self) -> bool:
        pass