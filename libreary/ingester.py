import sqlite3
import os
from shutil import copyfile
import hashlib
import uuid

from adapters.LocalAdapter import LocalAdapter
from config_parser import ConfigParser

CONFIG_DIR = "config"

class Ingester:

    def __init__(self, config):
        self.metadata_db = os.path.realpath(config['metadata'].get("db_file"))
        self.conn = sqlite3.connect(self.metadata_db)
        self.cursor = self.conn.cursor()
        self.dropbox_dir = config["options"]["dropbox_dir"]
        self.canonical_adapter_config = config["canonical_adapter"]

    def ingest(self, current_file_path, levels, delete_after_store=False):
        """returns resource uuid"""
        filename = file.split("/")[-1]
        sha1Hash = hashlib.sha1(open(file,"rb").read())
        checksum = sha1Hash.hexdigest()

        parser = ConfigParser()
        canonical_adapter_config = parser.create_config_for_adapter("local1", "local")

        # later, we will have this line. For now, we only support LocalAdapter
        # canonical_adapter = AdapterFactory.adapter(canonical_adapter_config)

        uuid = str(uuid.uuid4())
        canoncial_adapter = LocalAdapter(canonical_adapter_config)
        canonical_adapter._store_canonical(current_file_path, uuid, checksum, filename, delete_after_store=False)

        levels = ",".join([str(l) for l in levels])

        # Ingest to db
        
        self.cursor.execute("insert into resources values (?, ?, ?, ?,?, ?)", 
            (None, filename, dropbox_path, levels, checksum, uuid))

        self.conn.commit()

        return uuid


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


if __name__ == '__main__':
    config = json.load(open("{}/{}".format(CONFIG_DIR, "ingester_config.json")))
    i = Ingester(config)
    i.ingest("/Users/glick/Desktop/librelocal/helppls.txt", [1,])