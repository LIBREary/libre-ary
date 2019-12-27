import json
import os

import sqlite3

from config_parser import ConfigParser
from adapters.LocalAdapter import LocalAdapter
from exceptions import ChecksumMismatchException

CONFIG_DIR = "config"

class AdapterManager:
    """
    The AdapterManager is responsible for all interaction with adapters, except for intial ingestion.

    It will be able to keep track of all of the adapters we have, do integrity checks on them,
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
        self.levels = []
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
        levels = []
        for level in level_data:
            levels.append({"id": level[0], "name": level[1], "frequency": level[2], "adapters": json.loads(level[3])})
        return levels

    def _set_levels(self):
        self.levels = self.get_all_levels()

    def get_all_adapters(self):
        """
        Set up all of the adapters we will need.

        Ensure that self.levels is set properly before running this
        """
        adapters = {}
        for level in self.levels:
            # Each level may need several adapters
            for adapter in level["adapters"]:
                adapters[adapter["id"]] = AdapterManager.create_adapter(adapter["type"], adapter["id"])
        return adapters

    def _set_adapters(self):
        self.adapters = self.get_all_adapters()

    def verify_adapter(adapter_id):
        """
        Make sure an adapter is working. We store, retrieve, and delete a file
        that we know the contents of, and make sure the checksums all add up.
        """
        pass

    @staticmethod
    def create_adapter(adapter_type, adapter_id):
        """
        Adapter factory type function. 
        We want to be able to use adapter configs to create adapters. 
        This will be useful for the ingester as well.
        We eventually want this to be a static method.
        """
        parser = ConfigParser()
        cfg = parser.create_config_for_adapter(adapter_id, adapter_type)
        adapter = eval("{}({})".format(adapter_type, cfg))
        return adapter

    def send_resource_to_adapters():
        pass

    def delete_resource_from_adapters():
        pass

    def change_resource_level():
        pass

    def verify_ingestion():
        """
        Make sure an object has been properly ingested.
        """
        pass

    def summarize_copies(self, r_id):
        """
        This method trusts the metadata database. There should be a separate method to 
        verify the metadata db so that we know we can trust this info
        """
        sql = "select * from copies where resource_id = '{}'".format(r_id)
        return self.cursor.execute(sql).fetchall()

    def get_canonical_copy_metadata(self, r_id):
        sql = "select * from copies where resource_id = '{}' and canonical=1".format(r_id)
        return self.cursor.execute(sql).fetchall()

    def compare_copies(self, r_id, adapter_id_1, adapter_id_2):
        pass

    def retrieve_by_preference(r_id):
        """
        get a copy of a file, preferring canonical adapter, perhaps then enforcing some preference hierarchy
        This will be called when Libreary is asked to retrieve.
        """
        # First, try the canonical copy:
        try:
            self.adapters[self.canonical_adapter].retrieve(r_id)
        except ChecksumMismatchException:
            print("Canonical Recovery Failed. Attempting to Restore Canonical Copy")
            self.restore_canonical_copy(r_id)

        for adapter in self.adapters.values():
            try:
                adapter.retrieve(r_id)
                return True
            except ChecksumMismatchException:
                print("Canonical Recovery Failed. Attempting to Restore Canonical Copy")
                self.restore_from_canonical_copy(adapter.adapter_id, r_id)     


    def update_checksum(resource_id, adapter_id):
        pass

    def check_single_resource_single_adapter(self, r_id, adapter_id):
        resource_info = load_metadata(r_id)
        canonical_checksum = resource_info[4]
        level = resource_info[3]

        copies = get_all_copies_metadata(r_id)

        for copy in copies:
            if adapter == copy[2]:
                found = True
                if copy[2] != canonical_checksum:
                    a = create_adapter(self.adapter_type, adapter)
                    a.delete(r_id)
                    a.store(r_id)
            # didn't find the copy from this adapter
        if not found:
            try:
                a = create_adapter(self.adapter_type, adapter)
                a.store(r_id)
                found = True
            except Exception as e:
                found = False
        return found
        
    def verify_adapter_metadata(self, adapter_id, r_id):
        """
        A different kind of check. Verify that the file is actually retirevable via
        adapter id, not just there according to the metadata. This will be a function of 
        levels
        """
        pass

    def get_resource_metadata(self, r_id):
        return self.cursor.execute(
            "select * from resources where id={}".format(r_id)).fetchall()

    def get_level_info(self, l_id):
        return self.cursor.execute("select * from levels where id=?", (l_id)).fetchone()

if __name__ == '__main__':
    config = json.load(open("{}/{}".format(CONFIG_DIR, "adapter_manager_config.json")))
    am = AdapterManager(config)
    print(am.adapters)


