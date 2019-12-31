import json
import os
import string
import random
import hashlib
import shutil

import sqlite3

from libreary.config_parser import ConfigParser
from libreary.adapters.local import LocalAdapter
from libreary.adapters.s3 import S3Adapter

class AdapterManager:
    """
    The AdapterManager is responsible for all interaction with adapters, except for intial ingestion.

    It is able to keep track of all of the adapters we have, do integrity checks on them,
    perform initial distribution, compare versions from different adapters, and make insert and delete calls.

    This is useful to keep the base Libreary class as simple as possible.
    """

    def __init__(self, config):
        self.config = config
        self.all_adapters = self.config["adapters"]
        self.metadata_db = os.path.realpath(config['metadata'].get("db_file"))
        self.conn = sqlite3.connect(self.metadata_db)
        self.cursor = self.conn.cursor()
        self.dropbox_dir = config["options"]["dropbox_dir"]
        self.ret_dir = config["options"]["output_dir"]
        self.config_dir = config["options"]["config_dir"]
        self.levels = {}
        self.adapters = {}
        self.canonical_adapter = self.config["canonical_adapter"]
        # Run this any time you expect levels and adapters to change
        # For most use cases, this will only be on construction
        # This method should be run externally, any time a new level is added
        self.reload_levels_adapters()

    def reload_levels_adapters(self):
        self._set_levels()
        self._set_adapters()

    def get_all_levels(self):
        level_data = self.cursor.execute("select * from levels").fetchall()
        levels = {}
        for level in level_data:
            levels[level[1]] = {"id": level[0],
                           "name": level[1],
                           "frequency": level[2],
                           "adapters": json.loads(level[3])}
        return levels

    def _set_levels(self):
        self.levels = self.get_all_levels()

    def get_all_adapters(self):
        """
        Set up all of the adapters we will need, based on all levels that exist

        Ensure that self.levels is set properly before running this
        """
        adapters = {}
        for level in self.levels.values():
            # Each level may need several adapters
            for adapter in level["adapters"]:
                adapters[adapter["id"]] = self.create_adapter(
                    adapter["type"], adapter["id"], self.config_dir)
        return adapters

    def _set_adapters(self):
        self.adapters = self.get_all_adapters()

    def set_additional_adapter(self, adapter_id, adapter_type):
        """
        Manually add an adapter to the pool of adapters
        """
        adapter = self.create_adapter(adapter_type, adapter_id)
        self.adapters["adapter_id"] = adapter

    def verify_adapter(self, adapter_id):
        """
        Make sure an adapter is working. We store, retrieve, and delete a file
        that we know the contents of, and make sure the checksums all add up.
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
        r_id = "LIBREARY_TEST_RESOURCE"
        # To circumvent full ingestion process, we manually use _ingest_canonical
        # Not recommended for end users to do this.
        adapter._store_canonical(
            dropbox_path,
            r_id,
            real_checksum,
            "libreary_test_resource.txt")
        new_path = adapter.retrieve(r_id)
        new_checksum = hashlib.sha1(open(new_path, "rb").read()).hexdigest()

        r_val = False
        if new_checksum == real_checksum:
            r_val = True

        adapter._delete_canonical(r_id)
        os.remove(new_path)
        os.remove(dropbox_path)

        return r_val

    @staticmethod
    def create_adapter(adapter_type, adapter_id, config_dir):
        """
        Adapter factory type function.
        We want to be able to use adapter configs to create adapters.
        This will be useful for the ingester as well.

        :param adapter_type must be the name of a valid adapter class.
        """
        parser = ConfigParser(config_dir)
        cfg = parser.create_config_for_adapter(adapter_id, adapter_type)
        adapter = eval("{}({})".format(adapter_type, cfg))
        return adapter

    def send_resource_to_adapters(self, r_id):
        """
        Sends a resource to all the places it should go.
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
            file_hash = hashlib.sha1(open(expected_location, "rb").read()).hexdigest()
            expected_hash = resource_metadata[4]
            if file_hash == expected_hash:
                # there's a file in that location, and its checksum matches
                file_there = True
        
        # If the file isn't where we want it, put it there
        if not file_there:
            # retrieve moves it to the retrieval dir
            current_path = self.adapters[self.canonical_adapter].retrieve(r_id)
            # so we move it to the dropbox_dir
            shutil.move(current_path, expected_location)

        levels = resource_metadata[2].split(",")
        for level in levels:
            adapters = self.get_adapters_by_level(level)
            for adapter in adapters:
                adapter.store(r_id)

    def get_adapters_by_level(self, level):
        """
        Get a list of adapter objects based on a level
        """
        adapter_names = self.levels[level]["adapters"]
        adapters = []
        for adapter in adapter_names:
            adapters.append(self.adapters[adapter["id"]])
        return adapters

    def delete_resource_from_adapters(self, r_id):
        """Deletes a resource from all adapters it's stored in. 
           Does not delete canonical copy
        """
        try:
            resource_metadata = self.get_resource_metadata(r_id)[0]
        except IndexError:
            raise ResourceNotIngestedException

        levels = resource_metadata[2].split(",")
        for level in levels:
            adapters = self.get_adapters_by_level(level)
            for adapter in adapters:
                adapter.delete(r_id)


    def change_resource_level(self, r_id, new_levels):
        """
        Removes all levels from a resource, replaces them with :param new_levels
        """
        
        # Because d_r_f_a doesn't delete canonical copy, we can simply use
        # it to reset
        self.delete_resource_from_adapters(r_id)
        sql = "update resources set levels = '?' where uuid=?"
        self.cursor.execute(sql, (r_id, ",".join([l for l in new_levels])))
        self.conn.commit()
        # now that we've updated the levels, we refresh levels
        self.reload_levels_adapters()
        # now, we can just act as if it has never been sent off:
        self.send_resource_to_adapters(r_id)



    def summarize_copies(self, r_id):
        """
        This method trusts the metadata database. There should be a separate method to
        verify the metadata db so that we know we can trust this info
        """
        sql = "select * from copies where resource_id = '{}'".format(r_id)
        return self.cursor.execute(sql).fetchall()

    def get_canonical_copy_metadata(self, r_id):
        sql = "select * from copies where resource_id = '{}' and canonical=1".format(
            r_id)
        return self.cursor.execute(sql).fetchall()

    def retrieve_by_preference(self, r_id):
        """
        get a copy of a file, preferring canonical adapter, perhaps then enforcing some preference hierarchy
        This will be called when Libreary is asked to retrieve.
        """
        # First, try the canonical copy:
        try:
            new_loc = self.adapters[self.canonical_adapter].retrieve(r_id)
            return new_loc
        except ChecksumMismatchException:
            print("Canonical Recovery Failed. Attempting to Restore Canonical Copy")
            self.restore_canonical_copy(r_id)

        for adapter in self.adapters.values():
            try:
                new_loc = adapter.retrieve(r_id)
                return new_loc
            except ChecksumMismatchException:
                print("Canonical Recovery Failed. Attempting to Restore Canonical Copy")
                self.restore_from_canonical_copy(adapter.adapter_id, r_id)

    def check_single_resource_single_adapter(self, r_id, adapter_id):
        resource_info = self.get_resource_metadata(r_id)
        canonical_checksum = resource_info[4]
        level = resource_info[3]

        copies = self.get_all_copies_metadata(r_id)

        for copy in copies:
            if adapter_id == copy[2]:
                found = True
                if copy[2] != canonical_checksum:
                    a = AdapterManager.create_adapter(self.adapter_type, adapter_id)
                    a.delete(r_id)
                    a.store(r_id)
            # didn't find the copy from this adapter
        if not found:
            try:
                a = AdapterManager.create_adapter(self.adapter_type, adapter_id)
                a.store(r_id)
                found = True
            except AdapterCreationFailedException:
                found = False
        return found

    def verify_adapter_metadata(
            self, adapter_id, r_id, delete_after_check=True):
        """
        A different kind of check. Verify that the file is actually retirevable via
        adapter id, not just there according to the metadata.

        Note, this retrieves the file, so it's relatively expensive.
        """
        current_resource_info = self.get_resource_metadata(r_id)
        recorded_checksum = current_resource_info[4]
        current_path = self.adapters[adapter_id].retrieve(r_id)
        sha1Hash = hashlib.sha1(open(current_path, "rb").read())
        new_checksum = sha1Hash.hexdigest()

        r_val = True

        if new_checksum != recorded_checksum:
            try:
                self.restore_from_canonical_copy(self, adapter_id, r_id)
            except RestorationFailedException:
                print(
                    "Restoration of {} in {} failed".format(
                        r_id, adapter_id))
                r_val = False

        if delete_after_check:
            os.remove(current_path)

        return r_val

    def get_resource_metadata(self, r_id):
        return self.cursor.execute(
            "select * from resources where uuid='{}'".format(r_id)).fetchall()

    def restore_canonical_copy(self, r_id):
        """
        Restore a detected fault when the canonical adapter has a resource that doesn't match
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
                        current_location = adapter.retrieve(r_id)
                        raise AdapterRestored
                    except ResourceNotIngestedException:
                        continue
                    except ChecksumMismatchException:
                        continue
                    except NoCopyExistsException:
                        continue
        except AdapterRestored:
            self.adapters[self.canonical_adapter].store_canonical(current_location, r_id, real_checksum, filename)

    def restore_from_canonical_copy(self, adapter_id, r_id):
        """
        To restore from the canonical copy, we can simply delete and re-ingest
        """
        self.adapters[adapter_id].delete(r_id)
        self.adapters[adapter_id].store(r_id)

    def compare_copies(self, r_id, adapter_id_1, adapter_id_2, deep=False):
        try:
            copy_info_1 = copy_info = self.cursor.execute(
                "select * from copies where resource_id=? and adapter_identifier=? limit 1",
                (r_id, adapter_id_1)).fetchall()[0]
            copy_info_1 = copy_info = self.cursor.execute(
                "select * from copies where resource_id=? and adapter_identifier=? limit 1",
                (r_id, adapter_id_2)).fetchall()[0]
        except IndexError:
            raise NoCopyExistsException

        if not deep:
            return copy_info_1[4] == copy_info_2[4]

        return self.adapters[adapter_id_1].get_actual_checksum(r_id) == self.adapters[adapter_id_2].get_actual_checksum(r_id)

    def verify_copy(self, r_id, adapter_id, deep=False):
        """
        Determine whether a copy matches the canonical copy
        """
        return self.compare_copies(r_id, adapter_id, self.canonical_adapter, deep=deep)
