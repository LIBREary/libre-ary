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

    def add_level(self, name: str, frequency: int,
                  adapters: List[dict], copies=1) -> None:
    """
        Add a level to the metadata database.

        :param name - name for the level
        :param frequency - check frequency for level. Currently unimplemented
        :param adapters - dict object specifying adapters the level uses. Example:
            ```{json}
            [
                {
                "id": "local1",
                "type":"LocalAdapter"
                },
                {
                "id": "local2",
                "type":"LocalAdapter"
                }
            ]

            ```
        :param copies - copies to store for each adapter. Currently, only 1 is supported
        """
        str_adapters = json.dumps(adapters)
        self.cursor.execute(
            "insert into levels values (?, ?, ?, ?)",
            (name,
             frequency,
             str_adapters,
             copies))
        self.conn.commit()