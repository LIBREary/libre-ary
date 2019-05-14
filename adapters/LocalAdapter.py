import sqlite3
import os
from shutil import copyfile
import hashlib

from BaseAdapter import BaseAdapter


class LocalAdapter(BaseAdapter):
    """docstring for LocalAdapter
		
		LocalAdapter is a basic adapter which saves files 
		to a local directory specified in the adapter's config

		Later in this project's plan, the LocalAdapter will be used
		for ingesting the master copies as well as as a (probably)
		commonly used adapter.

		It's also very nice to use for testing, as saving files is easy (ish)
		to debug and doesn't cost any money (unlike a cloud service)
	"""

    adapter_type = "local"

    def __init__(self, config):
        self.metadata_db = config['metadata'].get("db_file")
        self.adapter_id = config["adapter"]["adapter_id"]
        self.db_conn = sqlite3.connect("metadata_db")
        self.cursor = conn.cursor()
        self.storage_dir = config["adapters"][adapter_type][storage_dir]
        self.dropbox_dir = config["options"]["dropbox_dir"]

    def store(r_id):
        file_metadata = load_metadata(r_id)[0]
        dropbox_path = file_metadata[2]
        checksum = file_metadata[4]
        name = file_metadata[1]
        current_location = "{}/{}".format(self.dropbox_dir, dropbox_path)
        new_location = "{}/{}".format(self.storage_dir, dropbox_path)
        new_dir = "/".join(new_location.split("/")[:-1])

        sha1Hash = hashlib.sha1(open(current_location).read())
        sha1Hashed = sha1Hash.hexdigest()

        if not os.path.isdir(new_dir):
            os.path.makedirs(new_dir)

        if os.path.isfile(new_location):
            new_location = "{}_{}".format(new_location, r_id)

        other_copies = self.cursor.execute(
            "select * from copies where resource_id=? and adapter_id=? limit 1",
            r_id, self.adapter_id)
        if len(other_copies) != 0:
            print("Other copies from this adapter exist")
            exit(0)

        if sha1Hashed == checksum:
            copyfile(current_location, new_location)
        else:
            print("Checksum Mismatch")
            raise Exception
            exit()

        self.cursor.execute(
            "insert into copies values ( ?, ?, ?, ?, ?)",
            [r_id, adapter_type, new_location, sha1Hashed, self.adapter_id])

    def retrieve(r_id):
        copy_info = self.cursor.execute(
            "select * from copies where resource_id=? and adapter_id=? limit 1",
            (r_id, self.adapter_id))[0]
        expected_hash = load_metadata(r_id)[0][4]
        copy_path = copy_info[3]
        real_hash = copy_info[4]

        new_location = "{}/{}".format(self.dropbox_dir, r_id)

        if real_hash == expected_hash:
            copyfile(copy_path, new_location)
        else:
            print("Checksum Mismatch")

    def update(r_id, updated):
        pass

    def delete(r_id):
        copy_info = self.cursor.execute(
            "select * from copies where resource_id=? and adapter_id=? limit 1",
            (r_id, self.adapter_id))[0]
        expected_hash = load_metadata(r_id)[0][4]
        copy_path = copy_info[3]

        os.remove(copy_path)

        self.cursor.execute("delete from copies where copy_id=?",
                            [copy_info[0]])

    def load_metadata(r_id):
        return self.cursor.execute(
            "select * from resources where id={}".format(r_id))


if __name__ == '__main__':
    config = {
        "metadata": {
            "db_file": "../metadata/md_index.db",
        },
        "adapters": {
            "storage_dir": "~/Desktop/librelocal",
            "adapter_identifier": "local1"
        },
        "options": {
            "dropbox_dir": "../dropbox"
        }
    }
    la = LocalAdapter(config)
