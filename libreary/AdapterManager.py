import json
import os

import sqlite3

from config_parser import ConfigParser
from adapters.LocalAdapter import LocalAdapter

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

    def get_all_levels():
        pass

    def get_adapters_by_level():
        pass

    def verify_adapter():
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

    def summarize_copies():
        pass

    def compare_copies():
        pass

    def retrieve_by_preference():
        pass

    def check_single_resource_single_adapter(self, r_id, adapter_id):
        resource_info = load_metadata(r_id)
        canonical_checksum = resource_info[4]
        level = resource_info[3]

        level_info = get_level_info(level)
        adapters = level_info[3].split(",")

        copies = get_all_copies(r_id)

        found = False

        for adapter in adapters:
            for copy in copies:
                if adapter == copy[2]:
                    found = True
                    if copy[2] != canonical_checksum:
                        a = create_adapter(self.adapter_type, adapter)
                        a.delete(r_id)
                        a.store(r_id)
            # didn't find the copy from this adapter
            if not found:
                a = create_adapter(self.adapter_type, adapter)
                a.store(r_id)
            else:
                found = False

    def get_resource_metadata(self, r_id):
        return self.cursor.execute(
            "select * from resources where id={}".format(r_id)).fetchall()

    def get_level_info(self, l_id):
        return self.cursor.execute("select * from levels where id=?", (l_id)).fetchone()

    def get_all_copies_metadata(self, r_id):
        copies = self.cursor.execute("select * from copies where resource_id=?", (r_id))
        return copies.fetchall()

if __name__ == '__main__':
    config = json.load(open("{}/{}".format(CONFIG_DIR, "adapter_manager_config.json")))
    #am = AdapterManager(config)
    local1 = AdapterManager.create_adapter("LocalAdapter", "local1")
    print(local1)


