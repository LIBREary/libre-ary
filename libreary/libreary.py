import json
import os
import sqlite3

from libreary.adapter_manager import AdapterManager
from libreary.ingester import Ingester


class Libreary:
    """
    Main class which will abstract interaction. Instantiating this class should set up all of the stuff
    """

    def __init__(self, config_dir):
        """
        Set up all of the necessary tooling - We need to get an:
        - metadata manager
        - set of adapters (adapterman)
        - ingester
        """
        # Config stuff
        self.config_dir = config_dir
        self.config_path = "{}/config.json".format(self.config_dir)
        self.config = json.load(open(self.config_path))

        # Metadata stuff
        self.metadata_db = os.path.realpath(self.config['metadata'].get("db_file"))
        self.conn = sqlite3.connect(self.metadata_db)
        self.cursor = self.conn.cursor()

        # Directories we care about
        self.dropbox_dir = self.config["options"]["dropbox_dir"]
        self.ret_dir = self.config["options"]["output_dir"]
        
        # Objects we need
        self.adapter_man = AdapterManager(self.config)
        self.ingester = Ingester(self.config)

    def run_check(deep=False):
        """
        A deep check uses actual checksums, while a shallow check trusts the metadata database
        """
        pass

    def ingest(self,):
        obj_id = self.ingester.ingest(current_file_path, levels, description, delete_after_store=False)
        return obj_id

    def retrieve(self, r_id):
        """
        strategy: tell adapter manager 'i want this object'. 
        THe adapter manager will sort out which copy to retrieve
        """
        new_location = self.adapter_man.retrieve_by_preference(r_id)
        return new_location

    def delete(self, r_id):
        """
        Deletes from adapters
        Deletes from canonical
        verifies deletion
        optionally, returns a copy to the retrieval directory
        """
        self.adapter_man.delete_resource_from_adapters(r_id)
        self.ingester.delete_resource((r_id))

    def update(self, r_id, updated_path):
        pass

    def search(self, search_term):
        pass

    def check_single_resource(self, r_id):
        pass