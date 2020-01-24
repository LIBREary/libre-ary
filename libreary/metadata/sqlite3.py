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
            logger.debug(
                "Metadata Manager Configuration Valid. Creating Metadata Manager")
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
        logger.debug(f"Adding level {name}")
        str_adapters = json.dumps(adapters)
        self.cursor.execute(
            "insert into levels values (?, ?, ?, ?)",
            (name,
             frequency,
             str_adapters,
             copies))
        self.conn.commit()


    def ingest_to_db(self, canonical_adapter_locator: str, levels: List[str], filename: str, checksum: str, obj_uuid: str, description: str) -> None:
        """
        Ingest an object's metadata to the metadata database.

        :param canonical_adapter_locator - locator from the canonical adapter. 
               usually something like a file path or object ID
        :param levels - list of strings, each the name of a level the object should be stored at
        :param filename - the filename of the object to be ingested
        :param checksum - the checksum of object to be ingested
        :param UUID - the UUID to be tagged with the object
        :param description - a friendly, searchable description of the object
        """
        logger.debug(f"Ingesting object {obj_uuid} with name {filename}")
        self.cursor.execute("insert into resources values (?, ?, ?, ?, ?, ?, ?)",
                            (None, canonical_adapter_locator, levels, filename, checksum, obj_uuid, description))

        self.conn.commit()

    def list_resources(self) -> List[List[str]]:
        """
        Return a list of summaries of each resource. This summary includes:

        `id`, `path`, `levels`, `file name`, `checksum`, `object uuid`, `description`

        This method trusts the metadata database. There should be a separate method to
        verify the metadata db so that we know we can trust this info
        """
        return self.cursor.execute("select * from resources").fetchall()

    def get_resource_info(self, r_id: str) -> List[str]:
        """
        Get all of the resource metadata for a resource

        This returns metadata that's kept in the `resources` table, not the `copies` table
        """
        return self.cursor.execute(
            "select * from resources where id=?", (r_id,))

    def delete_resource(self, r_id: str) -> None:
        """
        Delete a resource's metadata from the `resources` table

        :param r_id - the resource's uuid
        """
        self.cursor.execute("delete from resources where id=?", (r_id,))
        self.conn.commit()
