import os
from shutil import copyfile
import hashlib
import logging

from libreary.exceptions import ResourceNotIngestedException, ChecksumMismatchException
from libreary.exceptions import StorageFailedException, NoCopyExistsException

logger = logging.getLogger(__name__)


class LocalAdapter:
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

    def __init__(self, config: dict, metadata_man: object = None):
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
        try:
            self.adapter_id = config["adapter"]["adapter_identifier"]
            self.storage_dir = config["adapter"]["storage_dir"]
            self.dropbox_dir = config["options"]["dropbox_dir"]
            self.adapter_type = "LocalAdapter"
            self.ret_dir = config["options"]["output_dir"]

            self.metadata_man = metadata_man
            if self.metadata_man is None:
                raise KeyError

            logger.debug("Creating Local Adapter")
        except KeyError:
            logger.error("Invalid configuration for Local Adapter")
            raise KeyError

    def store(self, r_id: str) -> str:
        """
        Store a copy of a resource in this adapter.

        Store assumes that the file is in the `dropbox_dir`.
        AdapterManager will always verify that this is the case.

        :param r_id - the resource to store's UUID
        """
        logger.debug(f"Storing object {r_id} to adapter {self.adapter_id}")
        file_metadata = self.metadata_man.get_resource_info(r_id)[0]
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
            new_location = "{}_{}_{}".format("/".join(new_location.split('/')[:-1]), name, r_id)

        other_copies = self.metadata_man.get_copy_info(
            r_id, self.adapter_id)

        if len(other_copies) != 0:
            logger.debug(
                f"Other copies of {r_id} from {self.adapter_id} exist")
            return

        if sha1Hashed == checksum:
            copyfile(current_location, new_location)
        else:
            logger.error(f"Checksum Mismatch on object {r_id}")
            raise ChecksumMismatchException

        self.metadata_man.add_copy(
            r_id,
            self.adapter_id,
            new_location,
            sha1Hashed,
            self.adapter_type,
            canonical=False)

    def retrieve(self, r_id: str) -> str:
        """
        Retrieve a copy of a resource from this adapter.

        Retrieve assumes that the file can be stored to the `output_dir`.
        AdapterManager will always verify that this is the case.

        Returns the path to the resource.

        May overwrite files in the `output_dir`

        :param r_id - the resource to retrieve's UUID
        """
        logger.debug(
            f"Retrieving object {r_id} from adapter {self.adapter_id}")
        try:
            filename = self.metadata_man.get_resource_info(r_id)[0][3]
        except IndexError:
            logger.error(f"Cannot Retrieve object {r_id}. Not ingested.")
            raise ResourceNotIngestedException
        try:
            copy_info = self.metadata_man.get_copy_info(
                r_id, self.adapter_id)[0]
        except IndexError:
            logger.error(
                f"Tried to retrieve a nonexistent copy of {r_id} from {self.adapter_id}")
            raise NoCopyExistsException
        expected_hash = copy_info[4]
        copy_path = copy_info[3]
        real_hash = copy_info[4]

        new_location = "{}/{}".format(self.ret_dir, filename)

        if real_hash == expected_hash:
            copyfile(copy_path, new_location)
        else:
            logger.error(f"Checksum Mismatch on object {r_id}")
            raise ChecksumMismatchException

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
        logger.debug(
            f"Storing canonical copy of object {r_id} to {self.adapter_id}")
        current_location = current_path
        new_location = os.path.expanduser(
            "{}/canonical_{}".format(self.storage_dir, filename))
        new_dir = os.path.expanduser("/".join(new_location.split("/")[:-1]))

        sha1Hash = hashlib.sha1(open(current_location, "rb").read())
        sha1Hashed = sha1Hash.hexdigest()

        if not os.path.isdir(new_dir):
            os.makedirs(new_dir)

        if os.path.isfile(new_location):
            new_location = "{}_{}".format(new_location, r_id)

        other_copies = self.metadata_man.get_canonical_copy_metadata(
            r_id)
        if len(other_copies) != 0:
            logger.error(
                f"Other canonical copies of {r_id} from {self.adapter_id} exist")
            raise StorageFailedException

        if sha1Hashed == checksum:
            copyfile(current_location, new_location)
        else:
            logger.error(f"Checksum Mismatch on object {r_id}")
            raise ChecksumMismatchException

        self.metadata_man.add_copy(
            r_id,
            self.adapter_id,
            new_location,
            sha1Hashed,
            self.adapter_type,
            canonical=True)

        return new_location

    def delete(self, r_id: str) -> None:
        """
        Delete a copy of a resource from this adapter.
        Delete the corresponding entry in the `copies` table.

        :param r_id - the resource to retrieve's UUID
        """
        logger.debug(f"Deleting copy of object {r_id} from {self.adapter_id}")
        copy_info = self.metadata_man.get_copy_info(
            r_id, self.adapter_id)

        if len(copy_info) == 0:
            # We've already deleted, probably as part of another level
            return

        copy_info = copy_info[0]

        copy_path = copy_info[3]

        os.remove(copy_path)

        self.metadata_man.delete_copy_metadata(copy_info[0])

    def _delete_canonical(self, r_id: str) -> None:
        """
        Delete a canonical copy of a resource from this adapter.
        Delete the corresponding entry in the `copies` table.

        :param r_id - the resource to retrieve's UUID
        """
        logger.debug(
            f"Deleting canonical copy of object {r_id} from {self.adapter_id}")
        copy_info = self.metadata_man.get_canonical_copy_metadata(
            r_id)[0]
        copy_path = copy_info[3]

        os.remove(copy_path)

        self.metadata_man.delete_copy_metadata(copy_info[0])

    def get_actual_checksum(self, r_id: str) -> str:
        """
        Returns an exact checksum of a resource, not relying on the metadata db.

        If possible, implementations of get_actual_checksum should do no file I/O.
            In the case of LocalAdapter, we're able to do this without copying files
            around.

        :param r_id - resource we want the checksum of
        """
        logger.debug(
            f"Getting actual checksum of object {r_id} from adapter {self.adapter_id}")
        copy_info = self.metadata_man.get_copy_info(r_id, self.adapter_id)
        if len(copy_info) == 0:
            raise NoCopyExistsException
        copy_info = copy_info[0]
        path = copy_info[3]
        hash_obj = hashlib.sha1(open(path, "rb").read())
        checksum = hash_obj.hexdigest()
        return checksum
