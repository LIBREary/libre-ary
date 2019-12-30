import sqlite3
import os
from shutil import copyfile
import hashlib
import uuid
import json

from libreary.adapter_manager import AdapterManager
from libreary.config_parser import ConfigParser

class Ingester:

    def __init__(self, config):
        self.metadata_db = os.path.realpath(config['metadata'].get("db_file"))
        self.conn = sqlite3.connect(self.metadata_db)
        self.cursor = self.conn.cursor()
        self.dropbox_dir = config["options"]["dropbox_dir"]
        self.canonical_adapter_id = config["canonical_adapter"]
        self.canonical_adapter_type = config["canonical_adapter_id"]
        self.config_dir = config["options"]["config_dir"]

    def ingest(self, current_file_path, levels, description, delete_after_store=False):
        """returns resource uuid"""
        filename = current_file_path.split("/")[-1]
        sha1Hash = hashlib.sha1(open(current_file_path,"rb").read())
        checksum = sha1Hash.hexdigest()

        parser = ConfigParser(self.config_dir)
        canonical_adapter_config = parser.create_config_for_adapter(self.canonical_adapter_id, self.canonical_adapter_type)


        canonical_adapter = AdapterManager.create_adapter(self.canonical_adapter_type, self.canonical_adapter_id, self.config_dir)

        obj_uuid = str(uuid.uuid4())

        canonical_adapter_locator = canonical_adapter._store_canonical(current_file_path, obj_uuid, checksum, filename)

        levels = ",".join([str(l) for l in levels])

        # Ingest to db
        
        self.cursor.execute("insert into resources values (?, ?, ?, ?, ?, ?, ?)", 
            (None, canonical_adapter_locator, levels, filename, checksum, obj_uuid, description))

        self.conn.commit()

        # If file is not in dropbox, copy it there

        if delete_after_store:
            os.remove(current_file_path)

        return obj_uuid


    def verify_ingestion(self, r_id):
        """
        Make sure an object has been properly ingested.
        """
        pass


    def list_resources(self):
        return self.cursor.execute("select * from resources").fetchall()

    def delete_resource(self, r_id):
        resource_info = self.cursor.execute("select * from resources where id=?", (r_id,))
        canonical_path = "{}/{}".format(self.dropbox_dir, resource_info[2])
        canonical_checksum =  resource_info[4]

        sha1Hash = hashlib.sha1(open(canonical_path,"rb").read())
        checksum = sha1Hash.hexdigest()

        if checksum == canonical_checksum:
            os.remove(canonical_path)
        else:
            print("Checksum Mismatch")

        self.cursor.execute("delete from resources where id=?", (r_id,))

        self.conn.commit()