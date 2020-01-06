import sqlite3
import os
from shutil import copyfile
import hashlib
import json
from typing import List

from libreary.exceptions import ResourceNotIngestedException, ChecksumMismatchException, NoCopyExistsException
from libreary.exceptions import RestorationFailedException, AdapterCreationFailedException, AdapterRestored, StorageFailedException


class LocalAdapter():
    """
        An Adapter allows LIBREary to save copies of digital objects
            to different places across cyberspace. Working with many
            adapters in concert, one should be able do save sufficient
            copies to places they want them.

        LocalAdapter is a basic adapter which saves files
        to a local directory specified in the adapter's config

        Later in this project's plan, the LocalAdapter will be used
        for ingesting the master copies as well as as a (probably)
        commonly used adapter.

        It's also very nice to use for testing, as saving files is easy (ish)
        to debug and doesn't cost any money (unlike a cloud service) or have
        any configuration difficulty.
    """

    def __init__(self, config: dict):
        """
        Constructor for LocalAdapter. Expects a python dict :param `config`
            in the following format:

        ```{json}
        {
        "metadata": {
            "db_file": "path to metadata db"
        },
        "adapter": {
            "storage_dir": "Directory to store objects in",
            "adapter_identifier": "Friendly identifier",
            "adapter_type": "LocalAdapter"
        },
        "options": {
            "dropbox_dir": "path to dropbox directory",
            "output_dir": "path to output directory"
        },
        "canonical":"(boolean) true if this is the canonical adapter"
        }
        ```

        """
        self.metadata_db = os.path.realpath(config['metadata'].get("db_file"))
        self.adapter_id = config["adapter"]["adapter_identifier"]
        self.conn = sqlite3.connect(self.metadata_db)
        self.cursor = self.conn.cursor()
        self.storage_dir = config["adapter"]["storage_dir"]
        self.dropbox_dir = config["options"]["dropbox_dir"]
        self.adapter_type = "LocalAdapter"
        self.ret_dir = config["options"]["output_dir"]

    def store(self, r_id: str) -> str:
        """
        Store a copy of a resource in this adapter.

        Store assumes that the file is in the `dropbox_dir`.
        AdapterManager will always verify that this is the case.

        :param r_id - the resource to store's UUID
        """
        file_metadata = self.load_metadata(r_id)[0]
        checksum = file_metadata[4]
        name = file_metadata[3]
        current_location = "{}/{}".format(self.dropbox_dir, name)
        new_location = os.path.expanduser(
            "{}/{}".format(self.storage_dir, name))
        new_dir = os.path.expanduser("/".join(new_location.split("/")[:-1]))

        sha1Hash = hashlib.sha1(open(current_location, "rb").read())
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
            return

        if sha1Hashed == checksum:
            copyfile(current_location, new_location)
        else:
            print("Checksum Mismatch")
            raise Exception

        self.cursor.execute(
            "insert into copies values ( ?,?, ?, ?, ?, ?, ?)",
            [None, r_id, self.adapter_id, new_location, sha1Hashed, self.adapter_type, False])
        self.conn.commit()

    def retrieve(self, r_id: str) -> str:
        """
        Retrieve a copy of a resource from this adapter.

        Retrieve assumes that the file can be stored to the `output_dir`.
        AdapterManager will always verify that this is the case.

        Returns the path to the resource.

        May overwrite files in the `output_dir`

        :param r_id - the resource to retrieve's UUID
        """
        try:
            filename = self.load_metadata(r_id)[0][3]
        except IndexError:
            raise ResourceNotIngestedException
        try:
            copy_info = self.cursor.execute(
                "select * from copies where resource_id=? and adapter_identifier=? limit 1",
                (r_id, self.adapter_id)).fetchall()[0]
        except IndexError:
            raise NoCopyExistsException
        expected_hash = copy_info[4]
        copy_path = copy_info[3]
        real_hash = copy_info[4]

        new_location = "{}/{}".format(self.ret_dir, filename)

        if real_hash == expected_hash:
            copyfile(copy_path, new_location)
        else:
            print("Checksum Mismatch")

        return new_location

    def update(self, r_id: str, updated_path: str) -> None:
        """
        Update a resource with a new object. Preserves UUID and all other metadata (levels, etc.)

        :param r_id - the UUID of the object you'd like to update
        :param updated_path - path to the contents of the updated object.

        """
        pass

    def _store_canonical(self, current_path: str, r_id: str,
                         checksum: str, filename: str) -> str:
        """
            Store a canonical copy of a resource in this adapter.

            If we're using the LocalAdapter as a canonical adapter, we need
            to be able to store from a current path, taking in a generated UUID,
            rather than looking info up from the database.

            :param current_path - current path to object
            :param r_id - UUID of resource you're storing
            :param checksum - checksum of resource
            :param filename - filename of resource you're storing

        """
        current_location = current_path
        new_location = os.path.expanduser(
            "{}/{}_canonical".format(self.storage_dir, filename))
        new_dir = os.path.expanduser("/".join(new_location.split("/")[:-1]))

        sha1Hash = hashlib.sha1(open(current_location, "rb").read())
        sha1Hashed = sha1Hash.hexdigest()

        if not os.path.isdir(new_dir):
            os.makedirs(new_dir)

        if os.path.isfile(new_location):
            new_location = "{}_{}".format(new_location, r_id)

        sql = "select * from copies where resource_id='{}' and adapter_identifier='{}' and canonical = 1 limit 1".format(
            str(r_id), self.adapter_id)
        other_copies = self.cursor.execute(sql).fetchall()
        if len(other_copies) != 0:
            print("Other canonical copies from this adapter exist")
            raise StorageFailedException

        if sha1Hashed == checksum:
            copyfile(current_location, new_location)
        else:
            print("Checksum Mismatch")
            raise Exception
            exit()

        self.cursor.execute(
            "insert into copies values ( ?,?, ?, ?, ?, ?, ?)",
            [None, r_id, self.adapter_id, new_location, sha1Hashed, self.adapter_type, True])
        self.conn.commit()

        return new_location

    def delete(self, r_id: str) -> None:
        """
        Delete a copy of a resource from this adapter.
        Delete the corresponding entry in the `copies` table.

        :param r_id - the resource to retrieve's UUID
        """
        copy_info = self.cursor.execute(
            "select * from copies where resource_id=? and adapter_identifier=? and not canonical = 1 limit 1",
            (r_id, self.adapter_id)).fetchall()

        if len(copy_info) == 0:
            # We've already deleted, probably as part of another level
            return

        copy_info = copy_info[0]

        copy_path = copy_info[3]

        os.remove(copy_path)

        self.cursor.execute("delete from copies where copy_id=?",
                            [copy_info[0]])
        self.conn.commit()

    def _delete_canonical(self, r_id: str) -> None:
        """
        Delete a canonical copy of a resource from this adapter.
        Delete the corresponding entry in the `copies` table.

        :param r_id - the resource to retrieve's UUID
        """
        copy_info = self.cursor.execute(
            "select * from copies where resource_id=? and adapter_identifier=? and canonical = 1 limit 1",
            (r_id, self.adapter_id)).fetchall()[0]
        copy_path = copy_info[3]

        os.remove(copy_path)

        self.cursor.execute("delete from copies where copy_id=?",
                            [copy_info[0]])
        self.conn.commit()

    def load_metadata(self, r_id: str) -> List[List[str]]:
        """
        Get a summary of information about a resource. That summary includes:

        `id`, `path`, `levels`, `file name`, `checksum`, `object uuid`, `description`

        This method trusts the metadata database. There should be a separate method to
        verify the metadata db so that we know we can trust this info

        :param r_id - UUID of resource you'd like to learn about
        """
        return self.cursor.execute(
            "select * from resources where uuid='{}'".format(r_id)).fetchall()

    def get_actual_checksum(self, r_id: str) -> str:
        """
        Returns an exact checksum of a resource, not relying on the metadata db.

        If possible, implementations of get_actual_checksum should do no file I/O.
            In the case of LocalAdapter, we're able to do this without copying files
            around.

        :param r_id - resource we want the checksum of
        """
        copy_info = self.cursor.execute(
            "select * from copies where resource_id=? and adapter_identifier=? limit 1",
            (r_id, self.adapter_id)).fetchall()[0]
        path = copy_info[3]
        hash_obj = hashlib.sha1(open(path, "rb").read())
        checksum = hash_obj.hexdigest()
        return checksum
