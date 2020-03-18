import os
import hashlib
import uuid
from typing import List
import logging

from libreary.adapter_manager import AdapterManager
from libreary.exceptions import ChecksumMismatchException
from libreary.adapters import LocalAdapter
from libreary.metadata import SQLite3MetadataManager
from libreary.exceptions import NoCopyExistsException

logger = logging.getLogger(__name__)


class Ingester:

    def __init__(self, config: dict, metadata_man: object = None):
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
        try:
            self.config = config
            self.dropbox_dir = config["options"]["dropbox_dir"]
            self.canonical_adapter_id = config["canonical_adapter"]
            self.canonical_adapter_type = config["canonical_adapter_type"]
            self.config_dir = config["options"]["config_dir"]

            self.metadata_man = metadata_man
            if self.metadata_man is None:
                raise KeyError

            logger.debug("Ingester configuration valid, creating Ingester.")
        except KeyError as e:
            logger.error("Ingester Configuration Invalid")
            raise e

    def ingest(self, current_file_path: str, levels: List[str],
               description: str, delete_after_store: bool = False, metadata_schema: List = [], metadata: List = []) -> str:
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
            self.canonical_adapter_type, self.canonical_adapter_id, self.config_dir, self.config["metadata"])

        obj_uuid = str(uuid.uuid4())

        logger.debug(f"Ingesting resource {obj_uuid} with filename {filename}")

        canonical_adapter_locator = canonical_adapter._store_canonical(
            current_file_path, obj_uuid, checksum, filename)

        levels = ",".join([str(l) for l in levels])

        # Ingest to db
        self.metadata_man.ingest_to_db(
            canonical_adapter_locator,
            levels,
            filename,
            checksum,
            obj_uuid,
            description)

        # Ingest file metadata:
        if len(metadata_schema) == len(metadata) and len(metadata_schema) != 0:
            self.metadata_man.set_object_metadata_schema(
                obj_uuid, metadata_schema)
            self.metadata_man.set_all_object_metadata(obj_uuid, metadata)

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
        return self.metadata_man.list_resources()

    def delete_resource(self, r_id: str) -> None:
        """
        Delete a resource from the LIBREary.

        This method deletes the canonical copy and removes the corresponding entry in the `resources`
            table.

        :param r_id - the UUID of the resouce you're deleting
        """

        try:
            resource_info = self.metadata_man.get_resource_info(r_id)[0]
            canonical_checksum = resource_info[4]
        except IndexError:
            logger.debug(f"Already deleted {r_id}")

        canonical_adapter = AdapterManager.create_adapter(
            self.canonical_adapter_type, self.canonical_adapter_id, self.config_dir, self.config["metadata"])

        try:
            checksum = canonical_adapter.get_actual_checksum(r_id)
        except NoCopyExistsException:
            self.metadata_man.delete_resource(r_id)
            return

        if checksum == canonical_checksum:
            logger.debug(f"Deleting canonical copy of object {r_id}")
            canonical_adapter._delete_canonical(r_id)
        else:
            raise ChecksumMismatchException

        logger.debug(f"Deleting object {r_id} from resources database")
        self.metadata_man.delete_resource(r_id)
