import sqlite3
import os
from shutil import copyfile
import hashlib
import json

CONFIG_DIR = "../config"

class LocalAdapter():
    """docstring for LocalAdapter
        
        LocalAdapter is a basic adapter which saves files 
        to a local directory specified in the adapter's config

        Later in this project's plan, the LocalAdapter will be used
        for ingesting the master copies as well as as a (probably)
        commonly used adapter.

        It's also very nice to use for testing, as saving files is easy (ish)
        to debug and doesn't cost any money (unlike a cloud service)
    """

    

    def __init__(self, config):
        self.metadata_db = os.path.realpath(config['metadata'].get("db_file"))
        self.adapter_id = config["adapter"]["adapter_identifier"]
        self.conn = sqlite3.connect(self.metadata_db)
        self.cursor = self.conn.cursor()
        self.storage_dir = config["adapter"]["storage_dir"]
        self.dropbox_dir = config["options"]["dropbox_dir"]
        self.adapter_type = "local"
        self.ret_dir = config["options"]["output_dir"]

    def store(self, r_id):
        """
        Store assumes that the file is in the dropbox_dir
        Is this ok? if so, do we just have AdapterManager take care of this?

        """
        file_metadata = self.load_metadata(r_id)[0]
        dropbox_path = file_metadata[1]
        checksum = file_metadata[4]
        name = file_metadata[3]
        current_location = "{}/{}".format(self.dropbox_dir, name)
        new_location = os.path.expanduser("{}/{}".format(self.storage_dir, name))
        new_dir = os.path.expanduser("/".join(new_location.split("/")[:-1]))

        sha1Hash = hashlib.sha1(open(current_location,"rb").read())
        sha1Hashed = sha1Hash.hexdigest()

        if not os.path.isdir(new_dir):
            os.makedirs(new_dir)

        if os.path.isfile(new_location):
            new_location = "{}_{}".format(new_location, r_id)

        other_copies = self.cursor.execute(
            "select * from copies where resource_id='{}' and adapter_identifier='{}' and not canonical = 1 limit 1".format(
            r_id, self.adapter_id)).fetchall()
        if len(other_copies) != 0:
            print("Other copies from this adapter exist")
            raise StorageFailedException

        if sha1Hashed == checksum:
            copyfile(current_location, new_location)
        else:
            print("Checksum Mismatch")
            raise Exception

        self.cursor.execute(
            "insert into copies values ( ?,?, ?, ?, ?, ?, ?)",
            [None, r_id, self.adapter_type, new_location, sha1Hashed, self.adapter_id, False])
        self.conn.commit()

    def retrieve(self, r_id):
        copy_info = self.cursor.execute(
            "select * from copies where resource_id=? and adapter_identifier=? limit 1",
            (r_id, self.adapter_id)).fetchall()[0]
        expected_hash = copy_info[4]
        copy_path = copy_info[3]
        real_hash = copy_info[4]

        new_location = "{}/{}".format(self.ret_dir, r_id)

        if real_hash == expected_hash:
            copyfile(copy_path, new_location)
        else:
            print("Checksum Mismatch")
            
        return new_location

    def update(self, r_id, updated):
        pass

    def _store_canonical(self, current_path, r_id, checksum, filename):
        """
            If we're using the LocalAdapter as a canonical adapter, we need
            to be able to store from a current path, taking in a generated UUID,
            rather than looking info up from the database.
        """
        current_location = current_path
        name = filename
        new_location = os.path.expanduser("{}/{}".format(self.storage_dir, filename))
        new_dir = os.path.expanduser("/".join(new_location.split("/")[:-1]))

        sha1Hash = hashlib.sha1(open(current_location,"rb").read())
        sha1Hashed = sha1Hash.hexdigest()

        if not os.path.isdir(new_dir):
            os.makedirs(new_dir)

        if os.path.isfile(new_location):
            new_location = "{}_{}".format(new_location, r_id)

        sql = "select * from copies where resource_id='{}' and adapter_identifier='{}' and canonical = 1 limit 1".format( str(r_id), self.adapter_id)
        other_copies = self.cursor.execute(sql).fetchall()
        if len(other_copies) != 0:
            print("Other copies from this adapter exist")
            raise StorageFailedException

        if sha1Hashed == checksum:
            copyfile(current_location, new_location)
        else:
            print("Checksum Mismatch")
            raise Exception
            exit()

        self.cursor.execute(
            "insert into copies values ( ?,?, ?, ?, ?, ?, ?)",
            [None, r_id, self.adapter_id,  new_location, sha1Hashed, self.adapter_type, True])
        self.conn.commit()

        return new_location

    def delete(self, r_id):
        copy_info = self.cursor.execute(
            "select * from copies where resource_id=? and adapter_identifier=? and not canonical = 1 limit 1",
            (r_id, self.adapter_id)).fetchall()[0]
        expected_hash = copy_info[4]
        copy_path = copy_info[3]

        os.remove(copy_path)

        self.cursor.execute("delete from copies where copy_id=?",
                            [copy_info[0]])
        self.conn.commit()

    def delete_canonical(self, r_id):
        copy_info = self.cursor.execute(
            "select * from copies where resource_id=? and adapter_identifier=? and canonical = 1 limit 1",
            (r_id, self.adapter_id)).fetchall()[0]
        expected_hash = copy_info[4]
        copy_path = copy_info[3]

        os.remove(copy_path)

        self.cursor.execute("delete from copies where copy_id=?",
                            [copy_info[0]])
        self.conn.commit()

    def load_metadata(self, r_id):
        return self.cursor.execute(
            "select * from resources where uuid='{}'".format(r_id)).fetchall()

    def get_actual_checksum(self, r_id):
        """
        Return an exact checksum of a resource, not relying on the metadata db
        """
        copy_info = self.cursor.execute(
            "select * from copies where resource_id=? and adapter_identifier=? limit 1",
            (r_id, self.adapter_id)).fetchall()[0]
        path = copy_info[3]
        hash_obj = hashlib.sha1(open(path,"rb").read())
        checksum = hash_obj.hexdigest()
        return checksum