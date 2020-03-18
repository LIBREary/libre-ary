import json
from typing import List
import logging

from libreary.adapter_manager import AdapterManager
from libreary.ingester import Ingester
from libreary.metadata.sqlite3 import SQLite3MetadataManager

logger = logging.getLogger(__name__)

metadata_manager_translate_table = {
    "SQLite3MetadataManager": SQLite3MetadataManager
}


class Libreary:
    """
    This is the user-facing class for LIBRE-ary. Users of LIBRE-ary should only interact
    with this class directly. LIBRE-ary objects are able to handle all of the
    functionality of this module. Developers should feel free to extend the
    functionality of this class and are encouraged to submit pull requests
    to the main repository.

    This class currently contains the following methods:

    - ingest (load a resource into LIBRE-ary)
    - retrieve (retrieve a copy of an object)
    - delete (delete an object)
    - update (update an object)
    - search (search for information about objects)
    - run_full_check (check all resources to verify integrity)
    - check_single_resource (check only a single resource)
    """

    def __init__(self, config_dir: str):
        """
        Constructor for Libreary object.
            :param config_dir - a string pointing to a directory
            containing Libreary configuration. In the config directory
            must be a file called `config.json` which contains main
            configuration, and a separate config file for each adapter
            you plan to use. More detail on adapter configs can be found
            in the adapters constructor documentation.


            The structure of `config_dir/config.json` should be as follows:
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

            The canonical adapter is the adapter which will store the "canonical" copy
            of each resource, which will then be used as the "real" version of that digital object.

            This object creeates an adapter manager and an ingester. For more information,
            see those classes.

            The output and dropbox directories are volatile and should not be used for object storage.
        """
        # Config stuff
        self.config_dir = config_dir
        self.config_path = "{}/config.json".format(self.config_dir)
        self.config = json.load(open(self.config_path))

        try:
            # Directories we care about
            self.dropbox_dir = self.config["options"]["dropbox_dir"]
            self.ret_dir = self.config["options"]["output_dir"]

            # Objects we need
            self.metadata_man = metadata_manager_translate_table[self.config["metadata"]["manager_type"]](
                self.config["metadata"])
            self.adapter_man = AdapterManager(
                self.config, metadata_man=self.metadata_man)
            self.ingester = Ingester(
                self.config, metadata_man=self.metadata_man)
            logger.debug("LIBREary configuration valid. Proceeding.")
        except KeyError:
            logger.error("Invalid LIBREary config. Exiting.")
            raise KeyError

    def run_check(deep: bool = False) -> List[str]:
        """
        Check all of the objects in the LIBRE-ary. This follows the following process:

        For each object:
            Get canonical copy and actual checksum. Make sure canonical copy matches expected checksum
            If it doesn't:
                Attempt to recover canonical copy
            Get a list of all levels that the object has been labelled as:
            For each level:
                Get a list of adapters is is stored in:
                For each adapter:
                    Check to make sure that copy's checksum matches what it should:
                    If it doesn't:
                        Attempt to recover it.


        :param deep speficies whether to use a deep search. A deep search will calculate actual checksums
        of each copy of each object, while a shallow one will trust that the checksum in the metadata
        database matches that of the actual object.
        """
        logger.debug(f"Running check of all objects in LIBREary. Deep: {deep}")

    def ingest(self, current_file_path: str, levels: List[str],
               description: str, delete_after_store: bool = False, metadata_schema: List[str] = [], metadata: List[dict] = []) -> str:
        """
        Ingest a new object to the LIBRE-ary. This:
            1. Creates an entry in the `resources` table in the metadata db
            2. Creates an object UUID
            3. Ingests the canonical copy
            4. Sends copies to adapters which match specified levels
            5. Returns object ID

        :param current_file_path - the current path to the file you wish to ingest
        :param levels - a list of names of levels. These levels must exist in the
            `levels` table in the metadata db
        :param description - a description of this object. This is useful when you
            want to search for objects later
        :param delete_after_store - Boolean. If True, the Ingester will delete the object after it's stored.
        :param metadata_schema - list of names of metadata fields to be associated with object
        :param metadata - values of the fields in the metadata schema. Structured like this:
            [{"field": "collection", "value": "cats"},
             {"field": "owner", "value": "ben glick"}]
        """

        # Don't want ingester to delete it, because then AM will need to
        # retrieve.
        obj_id = self.ingester.ingest(
            current_file_path,
            levels,
            description,
            delete_after_store=False,
            metadata_schema=metadata_schema,
            metadata=metadata)
        self.adapter_man.send_resource_to_adapters(
            obj_id, delete_after_send=delete_after_store)
        logger.debug(
            f"Ingesting object {obj_id} to LIBREary. Description: {description}")
        return obj_id

    def retrieve(self, r_id: str) -> str:
        """
        Retrieve an object. This will save a copy of the object
            as `<self.output_dir>/<object_filename>`

        The output and dropbox directories are volatile and should not be used for object storage.

        Adapters and other objects frequently may delete or write files in these directories.

        :param r_id - The resource UUID that corresponds to the object you'd like to retrieve.

        Returns a path to the retireved object.
        """
        logger.debug(f"Retrieving object {r_id}")
        new_location = self.adapter_man.retrieve_by_preference(r_id)
        return new_location

    def delete(self, r_id: str) -> None:
        """
        Delete an object from LIBRE-ary. This:
            1. Deletes the resource from all of the adapters it was stored in
            2. Deletes the resource from the metadata db entirely
            3. Removes the canonical copy

        Be careful with this function, as there is no undo option.
        """
        logger.debug(f"Deleting object {r_id}")

        # Delete metadata only if object has metadata
        if len(self.metadata_man.list_object_metadata_schema(r_id)) > 0:
            self.metadata_man.delete_object_metadata_entirely(r_id)

        self.adapter_man.delete_resource_from_adapters(r_id)
        self.ingester.delete_resource((r_id))

    def update(self, r_id: str, updated_path: str) -> None:
        """
        Update a resource with a new object. Preserves UUID and all other metadata (levels, etc.)

        :param r_id - the UUID of the object you'd like to update
        :param updated_path - path to the contents of the updated object.

        """
        logger.debug(f"Updating object {r_id}")

    def search(self, search_term: str) -> List[str]:
        """
        Search the metadata db for information about resources.

        :param search_term - a string with which to search against the metadata db.
            Can match UUID, filename, original path, or description.
        """
        return self.metadata_man.search(search_term)

    def check_single_resource(self, r_id: str, deep: bool = False) -> bool:
        """
        Check a single object in the LIBRE-ary. This follows the following process:


        Get canonical copy and actual checksum. Make sure canonical copy matches expected checksum
        If it doesn't:
            Attempt to recover canonical copy
        Get a list of all levels that the object has been labelled as:
        For each level:
            Get a list of adapters is is stored in:
            For each adapter:
                Check to make sure that copy's checksum matches what it should:
                If it doesn't:
                    Attempt to recover it.


        :param deep speficies whether to use a deep search. A deep search will calculate actual checksums
        of each copy of each object, while a shallow one will trust that the checksum in the metadata
        database matches that of the actual object.

        :param r_id - the resource ID of the object you'd like to check
        """
        logger.debug(f"Checking object {r_id}")

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
        logger.debug(f"Adding new level: {name}")
        self.metadata_man.add_level(name, frequency, adapters, copies=1)
        self.adapter_man.reload_levels_adapters()

    def delete_level(self, level_name: str):
        """
        Delete a level from the metadata database.

        :param level_name - name of the level to delete
        """
        logger.debug(f"Deleting Level {level_name}")
        self.metadata_man.remove_level(level_name)
        self.adapter_man.reload_levels_adapters()
