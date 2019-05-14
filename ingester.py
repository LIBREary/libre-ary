import sqlite3
import os
from shutil import copyfile
import hashlib

class Ingester:

    def __init__(self, config):
        self.metadata_db = os.path.realpath(config['metadata'].get("db_file"))
        self.conn = sqlite3.connect(self.metadata_db)
        self.cursor = self.conn.cursor()
        self.dropbox_dir = config["options"]["dropbox_dir"]

    def ingest(self, file, levels):
        """returns resource id"""
        filename = file.split("/")[-1]
        sha1Hash = hashlib.sha1(open(file,"rb").read())
        checksum = sha1Hash.hexdigest()

        # Create Canonical Copy
        new_location =  "{}/{}".format(self.dropbox_dir, filename)

        i = 1
        while True:
            if os.path.isfile(new_location):
                nloc = new_location.split("_")
                print(nloc)
                new_location = "{}_{}".format(nloc[0], i)
                i += 1
            else:
                copyfile(file, new_location)
                break

        levels = ",".join([str(l) for l in levels])

        # Ingest to db

        self.cursor.execute("insert into resources values (?, ?, ?, ?,?)", 
            (None, filename, filename, levels, checksum))

        self.conn.commit()

        return self.cursor.execute("select * from resources where name=? limit 1", (filename,)).fetchone()[0]
        


    def list_resources(self):
        return self.cursor.execute("select * from resources").fetchall()

    def delete_resource(self, r_id):
        pass


if __name__ == '__main__':
    config = { "metadata": {
            "db_file": "metadata/md_index.db",
        },
        "adapter": {
            "storage_dir": "~/Desktop/librelocal",
            "adapter_identifier": "local1"
        },
        "options": {
            "dropbox_dir": "dropbox"
        } }
    i = Ingester(config)
    i.ingest("/Users/glick/Desktop/librelocal/helppls.txt", [1,])