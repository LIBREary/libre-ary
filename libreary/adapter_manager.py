import json
import os
import string
import random
import hashlib
import shutil
from typing import List
import logging
import ast

from libreary.adapters.BaseAdapter import BaseAdapter
from libreary.adapters.local import LocalAdapter
from libreary.adapters.s3 import S3Adapter
from libreary.adapters.drive import GoogleDriveAdapter
from libreary.exceptions import ResourceNotIngestedException, ChecksumMismatchException, NoCopyExistsException
from libreary.exceptions import RestorationFailedException, AdapterCreationFailedException, AdapterRestored
from libreary.metadata import SQLite3MetadataManager

logger = logging.getLogger(__name__)

adapters_translate_table = {
    "LocalAdapter": LocalAdapter,
    "S3Adapter": S3Adapter,
    "GoogleDriveAdapter": GoogleDriveAdapter,
}
metadata_man_translate_table = {
    "SQLite3MetadataManager": SQLite3MetadataManager,
}


class AdapterManager:
    """
    The AdapterManager is responsible for all interaction with adapters, except for intial ingestion.

    It is able to keep track of all of the adapters we have, do integrity checks on them,
    perform initial distribution, compare versions from different adapters, and make insert and delete calls.

    The Adapter Manager is responsible for most of the operation of ingestion, deletion, and management of
    digital objects within LIBREary. Most customization will occur by subclassing the AdapterManager.

    This class currently contains the following methods:

    - reload_levels_adapters (create adapter objects and set levels based on configuration)
    - get_all_levels (get levels based on what exists in metadata db)
    - get_all_adapters (create adapter objects based on levels)
    - set_additional_adapter (manually create an adapter object and add it to the AdapterManager's list of adapters)
    - verify_adapter (make sure an adapter is working properly)
    - create_adapter [static method] (factory function for adapter objects)
    - send_resource_to_adapters (send copies a resource to all the places they need to be)
    - get_adapters_by_level (get all adapters from a level)
    - delete_resource_from_adapters (delete non-canonical copies of an object)
    - change_resource_level (change the level of an object)
    - get_canonical_copy_metadata
    - summarize_copies
    - retrieve_by_preference (retrieve an object, prefering the canonical adapter)
    - check_single_resource_single_adapter (make sure that a copy matches the canonical copy)
    - verify_adapter_metadata (verify that a file can be retrieved)
    - get_resource_metadata
    - restore_canonical_copy (restore a faulty canonical copy)
    - compare_copies (check if two copies of a resource are the same)
    - verify_copy (check if a copy matches the canonicl copy)
    """

    def __init__(self, config: dict, metadata_man: object = None):
        """
        Constructor for the AdapterManager object. This object can be created manually, but
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
            self.all_adapters = self.config["adapters"]
            self.dropbox_dir = config["options"]["dropbox_dir"]
            self.ret_dir = config["options"]["output_dir"]
            self.config_dir = config["options"]["config_dir"]
            self.levels = {}
            self.adapters = {}
            self.canonical_adapter = self.config["canonical_adapter"]
            self.metadata_man = metadata_man
            if self.metadata_man is None:
                raise KeyError
            logger.debug(
                "Adapter Manager Configuration Valid, creating Adapter Manager")
        except KeyError:
            logger.error("Adapter Manager Configuration Invalid")
            raise KeyError
        # Run this any time you expect levels and adapters to change
        # For most use cases, this will only be on construction
        # This method should be run externally, any time a new level is added
        self.reload_levels_adapters()

    def reload_levels_adapters(self) -> None:
        """
        Set the `self.adapters` and `self.levels` instance variables.

        This object needs to be stateful in this way because each adapter might
        either require time-sensitive authentication information (tokens, etc), or
        may be computationally expensive to create. For this reason, we want the
        `AdapterManager` to have instance variables with adapter objects.
        """
        self._set_levels()
        self._set_adapters()

    def get_all_levels(self) -> List[dict]:
        """
        Returns all levels in the metadata database.

        Returns a list of dictionaries each with the following format:
        ```
        {"id": (int) level ID,
         "name": (str) level name,
         "frequency": (int) scheduled check frequency,
         "adapters": (dict) dictionary of adapters associated with this level}
        ```
        """
        level_data = self.metadata_man.get_levels()
        levels = {}
        for level in level_data:
            levels[level[1]] = {"id": level[0],
                                "name": level[1],
                                "frequency": level[2],
                                "adapters": json.loads(level[3])}
            logger.debug(
                f"Found level {level[1]} with adapters {json.loads(level[3])}")
        return levels

    def _set_levels(self) -> None:
        """
        Convenience method to reset levels as an instance variable
        """
        self.levels = self.get_all_levels()

    def get_all_adapters(self) -> List[dict]:
        """
        Set up all of the adapters we will need, based on all levels
        from the metadata database.

        Parses `self.levels` to create adapter objects.

        The structure of the return value is is a dictionary structured as follows:

        ```
        {
            "adapter_id1": <AdapterObject>,
            "adapter_id2": <AdapterObject>,...
        }
        ```

        Ensure that `self.levels` is set properly before running this, by
        calling `self._set_levels()`
        """
        adapters = {}
        for level in self.levels.values():
            # Each level may need several adapters
            for adapter in level["adapters"]:
                adapters[adapter["id"]] = AdapterManager.create_adapter(
                    adapter["type"], adapter["id"], self.config_dir, self.config["metadata"])
                logger.debug(
                    f"Created adapter {adapter['id']} of type {adapter['type']}")
        logger.debug(f"Summary of all adapters: {adapters}")
        return adapters

    def _set_adapters(self) -> None:
        """
        Convenience method to set `self.adapters` to the output of `get_all_adapters

        The structure of `self.adapters` is is a dictionary structured as follows:

        ```
        {
            "adapter_id1": <AdapterObject>,
            "adapter_id2": <AdapterObject>,...
        }
        ```

        Ensure that `self.levels` is set properly before running this, by
        calling `self._set_levels()`
        """
        self.adapters = self.get_all_adapters()

    def set_additional_adapter(self, adapter_id: str,
                               adapter_type: str) -> BaseAdapter:
        """
        Manually add an adapter to the pool of adapters.

        :param adapter_id - the adapter ID of the adapter you're creating
            There should be a matching config file for this adapter ID

        :param adapter_type - the type of the adapter you wish to create.
            Must be the actual class name, i.e. "LocalAdapter".
        """
        adapter = AdapterManager.create_adapter(
            adapter_type, adapter_id, self.config_dir, self.config["metadata"])
        self.adapters[adapter_id] = adapter
        logger.debug(
            f"Manually added adapter {adapter_id} of type {adapter_type}")
        return adapter

    def verify_adapter(self, adapter_id: str) -> bool:
        """
        Make sure an adapter is working. To do this, we store, retrieve,
        and delete a file that we know the contents of,
        and make sure the checksums are as they should be.

        :param adapter_id - The adapter ID you'd like to verify.
        """
        dropbox_path = "{}/libreary_test_file.txt".format(
            self.config["options"]["dropbox_dir"])
        adapter = self.adapters[adapter_id]
        data_to_store = ''.join(random.choice(string.ascii_letters)
                                for i in range(500))
        with open(dropbox_path, "w") as fh:
            fh.write(data_to_store)
        real_checksum = hashlib.sha1(
            open(dropbox_path, "rb").read()).hexdigest()
        r_id = "LIBREARY_TEST_RESORURCE"

        # To circumvent full ingestion process, we manually use _ingest_canonical
        # Not recommended for end users to do this.
        locator = adapter._store_canonical(
            dropbox_path,
            r_id,
            real_checksum,
            "libreary_test_resource.txt")

        self.metadata_man.minimal_test_ingest(locator, real_checksum, r_id)

        new_path = adapter.retrieve(r_id)
        new_checksum = hashlib.sha1(open(new_path, "rb").read()).hexdigest()

        r_val = False
        if new_checksum == real_checksum:
            logger.debug(f"Verified Adapter {adapter_id}")
            r_val = True

        adapter._delete_canonical(r_id)

        try:
            os.remove(new_path)
        except Exception:
            return

        os.remove(dropbox_path)

        # Delete from res table
        self.metadata_man.delete_resource(r_id)

        return r_val

    @staticmethod
    def create_adapter(adapter_type: str, adapter_id: str,
                       config_dir: str, metadata_man_config: dict, metadata_man_type="SQLite3MetadataManager") -> BaseAdapter:
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
        adapter = adapters_translate_table[adapter_type](
            ast.literal_eval(f"{cfg}"),
            metadata_man_translate_table[metadata_man_type](
                ast.literal_eval(f"{metadata_man_config}")))

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

    def send_resource_to_adapters(
            self, r_id: str, delete_after_send: bool = False) -> None:
        """
        Sends a resource to all the places it should go. The resource must
        have already been ingested through the Ingester. This method:
            1. Figures out what levels a resource has been assigned
            2. Figures out what adapters are associated with that level
            3. Figured out any overlap, to avoid storing things twice in one adapter
            4. Stores copies to each adapter
            5. optionally, deletes any remaining files in the dropbox directory

            :param r_id - resource UUID you wish to distribute
            :param delete_after_send - boolean indicating whether to delete
                files after storage
        """
        try:
            resource_metadata = self.get_resource_metadata(r_id)[0]
        except IndexError:
            raise ResourceNotIngestedException

        # Make sure that resource is in dropbox:
        filename = resource_metadata[3]
        expected_location = "{}/{}".format(self.dropbox_dir, filename)

        file_there = False
        if os.path.isfile(expected_location):
            file_hash = hashlib.sha1(
                open(
                    expected_location,
                    "rb").read()).hexdigest()
            expected_hash = resource_metadata[4]
            if file_hash == expected_hash:
                # there's a file in that location, and its checksum matches
                file_there = True

        # If the file isn't where we want it, put it there
        if not file_there:
            logger.debug(
                f"Could not find object {r_id} in Dropbox Directory. Moving it")
            # retrieve moves it to the retrieval dir
            current_path = self.adapters[self.canonical_adapter].retrieve(r_id)
            # so we move it to the dropbox_dir
            shutil.move(current_path, expected_location)

        levels = resource_metadata[2].split(",")
        for level in levels:
            adapters = self.get_adapters_by_level(level)
            for adapter in adapters:
                logger.debug(f"Storing object {r_id} to adapter {adapter}")
                adapter.store(r_id)

        if delete_after_send:
            logger.debug(f"Deleting object {r_id} after send")
            os.remove(expected_location)

    def get_adapters_by_level(self, level: str) -> List[BaseAdapter]:
        """
        Get a list of adapter objects based on a level.
        Returns a list of callable adapter objects.

        :param level - the name of the level you want the adapters for
        """
        adapter_names = self.levels[level]["adapters"]
        adapters = []
        for adapter in adapter_names:
            adapters.append(self.adapters[adapter["id"]])
        return adapters

    def delete_resource_from_adapters(self, r_id: str) -> None:
        """Deletes a resource from all adapters it's stored in.
           Does not delete canonical copy

           :param r_id - UUID of resource to delete copies of
        """
        try:
            resource_metadata = self.get_resource_metadata(r_id)[0]
        except IndexError:
            raise ResourceNotIngestedException

        levels = resource_metadata[2].split(",")
        for level in levels:
            adapters = self.get_adapters_by_level(level)
            for adapter in adapters:
                logger.debug(f"Deleting object {r_id} from {adapter}")
                adapter.delete(r_id)

    def change_resource_level(self, r_id: str, new_levels: List[str]) -> None:
        """
        Assign a new set of levels to a resource.
        Removes all levels from a resource, replaces them with :param new_levels

        :param r_id - UUID of resource you'd like to change the levels of
        :param new_levels: list of names of levels to assign to the resource
        """

        logger.debug(f"Changing object {r_id} to new levels: {new_levels}")
        # Because d_r_f_a doesn't delete canonical copy, we can simply use
        # it to reset
        self.delete_resource_from_adapters(r_id)
        self.metadata_man.update_resource_levels(r_id, new_levels)
        # now that we've updated the levels, we refresh levels
        self.reload_levels_adapters()
        # now, we can just act as if it has never been sent off:
        self.send_resource_to_adapters(r_id)

    def summarize_copies(self, r_id: str) -> List[List[str]]:
        """
        Get a summary of all copies of a single resource. That summary includes:

        `copy_id`, `resource_id`, `adapter_identifier`, `locator`, `checksum`, `adapter type`, `canonical (bool)`
        for each copy

        This method trusts the metadata database. There should be a separate method to
        verify the metadata db so that we know we can trust this info

        :param r_id - UUID of resource you'd like to learn about
        """
        return self.metadata_man.summarize_copies(r_id)

    def get_canonical_copy_metadata(self, r_id: str) -> List[List[str]]:
        """
        Get a summary of the canonical copy of an object's medatada. That summary includes:
        `copy_id`, `resource_id`, `adapter_identifier`, `locator`, `checksum`, `adapter type`, `canonical (bool)`

        :param r_id - UUID of resource you'd like to learn about
        """
        return self.metadata_man.get_canonical_copy_metadata(self, r_id)

    def retrieve_by_preference(self, r_id: str) -> str:
        """
        Retrieve a resource.

        Get a copy of a file, preferring canonical adapter, then enforcing some preference hierarchy
        This will be called when Libreary is asked to retrieve.

        This places a file in the configured `output_dir` and returns a path to the retrieved file.

        Keep in mind that the output directory may be volatile and should not be used for storage.

        :param r_id - UUID of resource you'd like to retrieve
        """
        # First, try the canonical copy:
        try:
            new_loc = self.adapters[self.canonical_adapter].retrieve(r_id)
            return new_loc
        except ChecksumMismatchException:
            logger.error(
                "Canonical Recovery Failed. Attempting to Restore Canonical Copy")
            self.restore_canonical_copy(r_id)

        for adapter in self.adapters.values():
            try:
                new_loc = adapter.retrieve(r_id)
                return new_loc
            except ChecksumMismatchException:
                logger.error(
                    "Canonical Recovery Failed. Attempting to Restore Canonical Copy")
                self.restore_from_canonical_copy(adapter.adapter_id, r_id)

    def check_single_resource_single_adapter(
            self, r_id: str, adapter_type: str, adapter_id: str) -> bool:
        """
        Ensure that a copy of an object matches its canonical checksum.
        This method trusts that the metadata db has the proper canonical checksum.

        If a copy is found to be faulty, a restore is attempted.
        If a copy is fount to not exist, it is created.

        :param r_id - UUID of resource you'd like to check
        :param r_id - adapter_id for copy of resource you are checking
        """
        resource_info = self.get_resource_metadata(r_id)
        canonical_checksum = resource_info[4]

        copies = self.get_all_copies_metadata(r_id)

        for copy in copies:
            if adapter_id == copy[2]:
                found = True
                if copy[2] != canonical_checksum:
                    try:
                        logger.debug(
                            f"Trying to restore resource {r_id} from canonical copy")
                        self.restore_from_canonical_copy(
                            adapter_id, r_id)
                    except RestorationFailedException:
                        logger.error(
                            "Restoration of {} in {} failed".format(
                                r_id, adapter_id))
                        found = False
            # didn't find the copy from this adapter
        if not found:
            logger.debug(
                f"Could not find resource {r_id} in adapter {adapter_id}")
            try:
                a = AdapterManager.create_adapter(
                    adapter_type, adapter_id, self.config_dir, self.config["metadata"])
                a.store(r_id)
                found = True
            except AdapterCreationFailedException:
                logger.error("Could not create adapter {adapter_id}")
                found = False
        return found

    def verify_adapter_metadata(
            self, adapter_id: str, r_id: str, delete_after_check: bool = True) -> bool:
        """
        Ensure that a copy of an object matches its canonical checksum.
        This method does not trust that the metadata db has the proper
        canonical checksum.

        Verifies that the file is actually retirevable via
        adapter id, not just there according to the metadata.

        Note, this retrieves the file, so it's relatively expensive.

        If a copy is found to be faulty, a restore is attempted.
        If a copy is fount to not exist, it is created.

        :param r_id - UUID of resource you'd like to check
        :param r_id - adapter_id for copy of resource you are checking
        """
        current_resource_info = self.get_resource_metadata(r_id)
        recorded_checksum = current_resource_info[4]
        current_path = self.adapters[adapter_id].retrieve(r_id)
        sha1Hash = hashlib.sha1(open(current_path, "rb").read())
        new_checksum = sha1Hash.hexdigest()

        r_val = True

        if new_checksum != recorded_checksum:
            try:
                logger.debug(f"Restoring copy of {r_id} in {adapter_id}")
                self.restore_from_canonical_copy(adapter_id, r_id)
            except RestorationFailedException:
                logger.error(
                    "Restoration of {} in {} failed".format(
                        r_id, adapter_id))
                r_val = False

        if delete_after_check:
            os.remove(current_path)

        return r_val

    def get_resource_metadata(self, r_id: str) -> List[List[str]]:
        """
        Get a summary of information about a resource. That summary includes:

        `id`, `path`, `levels`, `file name`, `checksum`, `object uuid`, `description`

        This method trusts the metadata database. There should be a separate method to
        verify the metadata db so that we know we can trust this info

        :param r_id - UUID of resource you'd like to learn about
        """
        return self.metadata_man.get_resource_info(r_id)

    def restore_canonical_copy(self, r_id: str) -> None:
        """
        Attempt to Restore a detected fault in the canonical copy of an object.

        Delete the canonical copy of an object, but keep non-canonical copies.
        After that, create a new canonical copy, preserving resource UUID,
            but with the correct object contents.

        :param r_id - UUID of resource you'd like to restore
        """
        try:
            resource_info = self.get_resource_metadata(r_id)
            real_checksum = resource_info[4]
            levels = resource_info[2].split(",")
            filename = resource_info[3]
        except IndexError:
            raise ResourceNotIngestedException

        self.adapters[self.canonical_adapter]._delete_canonical(r_id)

        current_location = 0

        try:
            for level in levels:
                adapters = self.get_adapters_by_level(level)
                for adapter in adapters:
                    try:
                        logger.debug(
                            f"Trying to restore copy of {r_id} from adapter {adapter}")
                        current_location = adapter.retrieve(r_id)
                        raise AdapterRestored
                    except ResourceNotIngestedException:
                        continue
                    except ChecksumMismatchException:
                        continue
                    except NoCopyExistsException:
                        continue
                logger.error(f"Failed to restore copy of {r_id}")
                return
        except AdapterRestored:
            self.adapters[self.canonical_adapter].store_canonical(
                current_location, r_id, real_checksum, filename)

    def restore_from_canonical_copy(self, adapter_id: str, r_id: str) -> None:
        """
        Restore a copy of an object from its canonical copy.
        To restore from the canonical copy, we can simply delete and
            re-ingest the fraudulent copy.

        :param adapter_id - the ID of the adapter with the broken copy
        :param r_id - The resource UUID of the resource we've detected an issue with
        """
        logger.debug(
            f"Restoring object {r_id} in adapter {adapter_id} from canonical copy.")
        self.adapters[adapter_id].delete(r_id)
        self.adapters[adapter_id].store(r_id)

    def compare_copies(self, r_id: str, adapter_id_1: str,
                       adapter_id_2: str, deep: bool = False) -> bool:
        """
        Compare copies of a resource in two adapters. Returns True iff
            the checksums of each copy match.

        A deep compare will actually compute the current checksum of the
            file stored in the adapter specified. Some adapters can do this
            with no file I/O, while others will have to actually retrieve the file
            to perform this operation

        :param r_id - the UUID of the resource to compare
        :param adapter_id_1 - Adapter ID of the first adapter
        :param adapter_id_2 - Adapter ID of the second adapter
        :param deep - specify whether to run a deep or shallow check
        """
        try:
            copy_info_1 = self.metadata_man.get_copy_info(r_id, adapter_id_1)
            copy_info_2 = self.metadata_man.get_copy_info(r_id, adapter_id_2)
        except IndexError:
            logger.error(f"No copy of object {r_id} exists.")
            raise NoCopyExistsException

        if not deep:
            return copy_info_1[4] == copy_info_2[4]

        return self.adapters[adapter_id_1].get_actual_checksum(
            r_id) == self.adapters[adapter_id_2].get_actual_checksum(r_id)

    def verify_copy(self, r_id: str, adapter_id: str,
                    deep: bool = False) -> bool:
        """
        Compare copies of a resource in two adapters, one being canonical.
        Returns True iff the checksums of each copy match.

        A deep compare will actually compute the current checksum of the
            file stored in the adapter specified. Some adapters can do this
            with no file I/O, while others will have to actually retrieve the file
            to perform this operation

        :param r_id - the UUID of the resource to compare
        :param adapter_id - Adapter ID of the adapter to check against
        :param deep - specify whether to run a deep or shallow check
        """
        return self.compare_copies(
            r_id, adapter_id, self.canonical_adapter, deep=deep)
