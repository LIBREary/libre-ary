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
        metadata_db = config['metadata'].get("db_file")
        db_conn = sqlite3.connect("metadata_db")
        cursor = conn.cursor()

    def store(r_id):
        file_metadata = load_metadata(r_id)[0]
        dropbox_path = file_metadata[2]
        checksum = file_metadata[4]
        name = file_metadata[1]
        current_location = "{}/{}".format(config["options"][dropbox_dir],
                                          dropbox_path)
        new_location = "{}/{}".format(
            config["adapters"][adapter_type][storage_dir], dropbox_path)
        new_dir = "/".join(new_location.split("/")[:-1])

        sha1Hash = hashlib.sha1(open(current_location).read())
        sha1Hashed = sha1Hash.hexdigest()

        if not os.path.isdir(new_dir):
            os.path.makedirs(new_dir)
        if os.path.isfile(new_location):
            new_location = "{}_{}".format(new_location, r_id)

        copyfile(current_location, new_location)

        cursor.execute("insert into copies values ( ?, ?, ?, ?)",
                       [r_id, adapter_type, new_location, sha1Hashed])

    def retrieve(r_id):
        pass

    def update(r_id, updated):
        pass

    def load_metadata(r_id):
        return cursor.execute("select * from resources where id=r_id")


if __name__ == '__main__':
    config = {
        "metadata": {
            "db_file": "../metadata/md_index.db",
        },
        "adapters": {
            "local": {
                "storage_dir": "~/Desktop/librelocal",
            }
        },
        "options": {
            "dropbox_dir": "../dropbox"
        }
    }
    la = LocalAdapter(config)
