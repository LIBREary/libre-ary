import sqlite3
import os
from shutil import copyfile
import hashlib
import uuid
import json
from typing import List

from libreary.adapter_manager import AdapterManager


class Ingester:

    def __init__(self, config: dict):
        """
        Constructor for the Ingester object. This object can be created manually, but
        in most cases, it will be constructed by the LIBRE-ary main object. It expects a python dict
        :param config, which should be structured as follows:
        ```{json}
        {
                "metadata": {
                    "db_file": "path to SQLite3 DB file for metadata"
                },
                "adapters": # List of adapters - each entry should look like:
                [{
                    "type":"AdapterType (name of class)",
                    "id": "Adapter Identifier"
                }],
                "options": {
                    "dropbox_dir": "Path to dropbox directory, where files you want to ingest should be placed",
                    "output_dir": "Path to directory you want files to be retrieved to",
                    "config_dir": "Path to config directory"
                },
                "canonical_adapter":"Adapter Identifier for Canonical Adapter"
            }
        ```
        """
        self.metadata_db = os.path.realpath(config['metadata'].get("db_file"))
        self.conn = sqlite3.connect(self.metadata_db)
        self.cursor = self.conn.cursor()
        self.dropbox_dir = config["options"]["dropbox_dir"]
        self.canonical_adapter_id = config["canonical_adapter"]
        self.canonical_adapter_type = config["canonical_adapter"]
        self.config_dir = config["options"]["config_dir"]

    def ingest(self, current_file_path: str, levels: List[str],
               description: str, delete_after_store: bool = False) -> str:
        """
        Ingest an object to LIBREary. This method:
        - Creates the canonical copy of the object
        - Creates the entry in the `resources` table describing the resource
        - Optionally, delete the file out of the dropbox dir.

        :param current_file_path -
        """
        filename = current_file_path.split("/")[-1]
        sha1Hash = hashlib.sha1(open(current_file_path, "rb").read())
        checksum = sha1Hash.hexdigest()

        canonical_adapter = AdapterManager.create_adapter(
            self.canonical_adapter_type, self.canonical_adapter_id, self.config_dir)

        obj_uuid = str(uuid.uuid4())

        canonical_adapter_locator = canonical_adapter._store_canonical(
            current_file_path, obj_uuid, checksum, filename)

        levels = ",".join([str(l) for l in levels])

        # Ingest to db

        self.cursor.execute("insert into resources values (?, ?, ?, ?, ?, ?, ?)",
                            (None, canonical_adapter_locator, levels, filename, checksum, obj_uuid, description))

        self.conn.commit()

        # If file is not in dropbox, copy it there

        if delete_after_store:
            os.remove(current_file_path)

        return obj_uuid

    def verify_ingestion(self, r_id: str) -> bool:
        """
        Make sure an object has been properly ingested.

        :param r_id - the UUID of the resource you are verifying
        """
        pass

    def list_resources(self) -> List[List[str]]:
        """
        Return a list of summaries of each resource. This summary includes:

        `id`, `path`, `levels`, `file name`, `checksum`, `object uuid`, `description`

        This method trusts the metadata database. There should be a separate method to
        verify the metadata db so that we know we can trust this info
        """
        return self.cursor.execute("select * from resources").fetchall()

    def delete_resource(self, r_id: str) -> None:
        """
        Delete a resource from the LIBREary.

        This method deletes the canonical copy and removes the corresponding entry in the `resources`
            table.

        :param r_id - the UUID of the resouce you're deleting
        """
        resource_info = self.cursor.execute(
            "select * from resources where id=?", (r_id,))
        canonical_checksum = resource_info[4]

        canonical_adapter = AdapterManager.create_adapter(
            self.canonical_adapter_type, self.canonical_adapter_id, self.config_dir)
        checksum = canonical_adapter.get_actual_checksum(r_id)

        if checksum == canonical_checksum:
            canonical_adapter._delete_canonical(r_id)
        else:
            print("Checksum Mismatch")

        self.cursor.execute("delete from resources where id=?", (r_id,))
        self.conn.commit()
