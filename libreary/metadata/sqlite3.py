import sqlite3
import os
import json
from typing import List
import logging

logger = logging.getLogger(__name__)


class SQLite3MetadataManager(object):
    """docstring for SQLite3MetadataManager

    SQLite3 Metadata Manager is the most basic.

    It expects a SQLite3 file, formatted as described in
    the LIBREary documentation.
    """

    def __init__(self, config: dict):
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
            self.type = config.get("manager_type")
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
            "insert into levels values (?, ?, ?, ?, ?)",
            (None,
             name,
             frequency,
             str_adapters,
             copies))
        self.conn.commit()

    def ingest_to_db(self, canonical_adapter_locator: str,
                     levels: List[str], filename: str, checksum: str, obj_uuid: str, description: str) -> None:
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
        Get all of the resource metadata for a resource That summary includes:

        `id`, `path`, `levels`, `file name`, `checksum`, `object uuid`, `description`

        This method trusts the metadata database. There should be a separate method to
        verify the metadata db so that we know we can trust this info

        This returns metadata that's kept in the `resources` table, not the `copies` table
        """
        return self.cursor.execute(
            "select * from resources where uuid=?", (r_id,)).fetchall()

    def delete_resource(self, r_id: str) -> None:
        """
        Delete a resource's metadata from the `resources` table

        :param r_id - the resource's uuid
        """
        self.cursor.execute("delete from resources where uuid=?", (r_id,))
        self.conn.commit()

    def minimal_test_ingest(self, locator: str, real_checksum: str, r_id: str):
        """
        Minimally ingest a resource for adapter testing

        Adapters depend on certain information in the `resources` table.

        In order for adapter manager to test adapters, it needs to have minimally ingested them.
        This testing has predefined levels, filenames, and descriptions.

        In general, I don't advise running this method yourself. Allow the adapter manager to do it.

        :param locator - locator of the copy you're testing
        :param real_checksum - object checksum
        :param r_id - r_id of test resource
        """
        self.cursor.execute("insert into resources values (?, ?, ?, ?, ?, ?, ?)",
                            (None, locator, "low,", "libreary_test_file.txt", real_checksum, r_id, "A resource for testing LIBREary adapters with"))
        self.conn.commit()

    def get_levels(self):
        """
        Return all configured levels
        """
        return self.cursor.execute("select * from levels").fetchall()

    def update_resource_levels(self, r_id: str, new_levels: List[str]):
        """
        Change a resource's levels

        :param r_id - resource id of resource to update
        :param new_levels - list of names of new levels
        """
        sql = "update resources set levels = ? where uuid=?"
        self.cursor.execute(sql, (r_id, ",".join([l for l in new_levels])))
        self.conn.commit()

    def summarize_copies(self, r_id: str) -> List[List[str]]:
        """
        Get a summary of all copies of a single resource. That summary includes:

        `copy_id`, `resource_id`, `adapter_identifier`, `locator`, `checksum`, `adapter type`, `canonical (bool)`
        for each copy

        This method trusts the metadata database. There should be a separate method to
        verify the metadata db so that we know we can trust this info

        :param r_id - UUID of resource you'd like to learn about
        """
        sql = "select * from copies where resource_id = ?"
        return self.cursor.execute(sql, (r_id,)).fetchall()

    def get_canonical_copy_metadata(self, r_id: str) -> List[List[str]]:
        """
        Get a summary of the canonical copy of an object's medatada. That summary includes:
        `copy_id`, `resource_id`, `adapter_identifier`, `locator`, `checksum`, `adapter type`, `canonical (bool)`

        :param r_id - UUID of resource you'd like to learn about
        """
        sql = "select * from copies where resource_id = ? and canonical=1"
        return self.cursor.execute(sql, (
            r_id,)).fetchall()

    def get_copy_info(self, r_id: str, adapter_id: str):
        """
        Get a summary of a copy of an object. Can be canonical or not.

        :param r_id - object you want to learn about
        :param adapter_id - adapter storing the copy
        """
        return self.cursor.execute(
            "select * from copies where resource_id=? and adapter_identifier=?", [
                r_id, adapter_id]).fetchall()

    def delete_copy_metadata(self, copy_id: int):
        """
        Delete object metadata for a single copy

        :param copy_id -  The copy id (not resource uuid) to delete
        """
        print(copy_id)
        self.cursor.execute("delete from copies where copy_id=?",
                            (copy_id,))
        self.conn.commit()

    def add_copy(self, r_id: str, adapter_id: str, new_location: str,
                 sha1Hashed: str, adapter_type: str, canonical: bool = False):
        """
        Add a copy of an object to the metadata database


        """
        self.cursor.execute(
            "insert into copies values ( ?, ?, ?, ?, ?, ?, ?)",
            [None, r_id, adapter_id, new_location, sha1Hashed, adapter_type, canonical])
        self.conn.commit()

    def search(self, search_term: str):
        """
        Search the metadata db for information about resources.

        :param search_term - a string with which to search against the metadata db.
            Can match UUID, filename, original path, or description.
        """
        search_term = "%" + search_term + "%"
        return self.cursor.execute(f"select * from resources where name like ? or path like ? or uuid like ? or description like ?",
                                   (search_term, search_term, search_term, search_term)).fetchall()
