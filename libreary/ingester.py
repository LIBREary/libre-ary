import os
import hashlib
import uuid
from typing import List
import logging

from libreary.adapter_manager import AdapterManager
from libreary.exceptions import ChecksumMismatchException
from libreary.adapters import LocalAdapter
from libreary.metadata import SQLite3MetadataManager

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

        canonical_adapter = self.create_adapter(
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

        # If file is not in dropbox, copy it there

        if delete_after_store:
            os.remove(current_file_path)

        return obj_uuid

    def create_adapter(self, adapter_type: str, adapter_id: str,
                       config_dir: str, metadata_man_config: dict) -> object:
        """
        Static method for creating and returning an adapter object.
        This is essentially an Adapter factory.

        :param adapter_type - must be the name of a valid adapter class.
        :param adapter_id - the identifier you want to label this adapter with
        :param config_dir - configuration directory. Must contain a file called
            `{adapter_id}_config.json`
        """
        cfg = AdapterManager.create_config_for_adapter(
            adapter_id, adapter_type, config_dir)
        adapter = eval("{}({}, SQLite3MetadataManager({}))".format(adapter_type, cfg, metadata_man_config))
        return adapter

    @staticmethod
    def create_config_for_adapter(
            adapter_id: str, adapter_type: str, config_dir: str) -> dict:
        """
        Static method for creating an adapter configuration. This is necessary for
        the adapter factory.

        :param adapter_type - must be the name of a valid adapter class.
        :param adapter_id - the identifier you want to label this adapter with
        :param config_dir - configuration directory. Must contain a file called
            `{adapter_id}_config.json`
        """
        base_config = json.load(
            open("{}/{}_config.json".format(config_dir, adapter_id)))
        general_config = json.load(
            open("{}/config.json".format(config_dir)))

        full_adapter_conf = {}
        full_adapter_conf["adapter"] = base_config["adapter"]
        full_adapter_conf["adapter"]["adapter_type"] = adapter_type
        full_adapter_conf["metadata"] = general_config["metadata"]
        full_adapter_conf["options"] = general_config["options"]

        return full_adapter_conf

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

        resource_info = self.metadata_man.get_resource_info(r_id)
        canonical_checksum = resource_info[4]

        canonical_adapter = AdapterManager.create_adapter(
            self.canonical_adapter_type, self.canonical_adapter_id, self.config_dir, self.metadata_man)
        checksum = canonical_adapter.get_actual_checksum(r_id)

        if checksum == canonical_checksum:
            logger.debug(f"Deleting canonical copy of object {r_id}")
            canonical_adapter._delete_canonical(r_id)
        else:
            logger.error("Checksum Mismatch")
            raise ChecksumMismatchException

        logger.debug(f"Deleting object {r_id} from resources database")
        self.metadata_man.delete_resource(r_id)
