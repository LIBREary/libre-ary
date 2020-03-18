import json
import os
import hashlib
import logging

try:
    import boto3
    from botocore.exceptions import ClientError

except ImportError:
    _boto_enabled = False
else:
    _boto_enabled = True

from libreary.exceptions import ResourceNotIngestedException, ChecksumMismatchException, NoCopyExistsException
from libreary.exceptions import StorageFailedException, ConfigurationError, OptionalModuleMissingException

logger = logging.getLogger(__name__)


class S3Adapter:
    """
        An Adapter allows LIBREary to save copies of digital objects
            to different places across cyberspace. Working with many
            adapters in concert, one should be able do save sufficient
            copies to places they want them.

        S3Adapter allows users to store objects in AWS S3.
    """

    def __init__(self, config: dict, metadata_man: object = None):
        """
        Constructor for S3Adapter. Expects a python dict :param `config`
            in the following format:

        You must have already created the S3 bucket you wish to use for this to work.

        ```{json}
        {
        "metadata": {
            "db_file": "path to metadata db"
        },
        "adapter": {
            "bucket_name": "name of S3 bucket",
            "adapter_identifier": "friendly identifier",
            "adapter_type": "S3Adapter",
            "region": "AWS Region",
            "key_file":"Path to optional AWS key file. See create_session docs for more"
        },
        "options": {
            "dropbox_dir": "path to dropbox directory",
            "output_dir": "path to output directory"
        },
        "canonical":"(boolean) true if this is the canonical adapter"
        }
        """
        self.config = config
        try:
            self.adapter_id = config["adapter"]["adapter_identifier"]
            self.adapter_type = "S3Adapter"
            self.dropbox_dir = config["options"]["dropbox_dir"]
            self.ret_dir = config["options"]["output_dir"]

            if not _boto_enabled:
                raise OptionalModuleMissingException(
                    ['boto3'], "S3 adapter requires the boto3 module.")

            self.profile = self.config["adapter"].get("profile")
            self.key_file = self.config["adapter"].get("key_file")
            self.bucket_name = self.config["adapter"].get("bucket_name")
            self.region = self.config["adapter"].get("region", "us-west-2")

            self.env_specified = os.getenv("AWS_ACCESS_KEY_ID") is not None and os.getenv(
                "AWS_SECRET_ACCESS_KEY") is not None

            self.metadata_man = metadata_man
            if self.metadata_man is None:
                raise KeyError

            logger.debug("Creating S3 Adapter")
        except KeyError:
            logger.error("Invalid configuration for S3 Adapter")
            raise KeyError

        if self.profile is None and self.key_file is None and not self.env_specified:
            raise ConfigurationError("Must specify either profile', 'key_file', or "
                                     "'AWS_ACCESS_KEY_ID' and 'AWS_SECRET_ACCESS_KEY' environment variables.")

        try:
            self.initialize_boto_client()
        except Exception as e:
            logger.error(f"Could not create AWS session")
            raise e
        except ClientError as e:
            logger.error(f"Could not create AWS session - Boto3 client failed.")
            raise e

        self._create_bucket_if_nonexistent()

    def initialize_boto_client(self) -> None:
        """Initialize the boto client."""

        self.session = self.create_session()
        # self.client = self.session.client('s3')
        # self.s3 = self.session.resource('s3')
        self.client = boto3.client('s3')
        self.s3 = boto3.resource('s3')

    def create_session(self) -> boto3.session.Session:
        """Create a session.

        First we look in self.key_file for a path to a json file with the
        credentials. The key file should have 'AWSAccessKeyId' and 'AWSSecretKey'.
        Next we look at self.profile for a profile name and try
        to use the Session call to automatically pick up the keys for the profile from
        the user default keys file ~/.aws/config.
        Finally, boto3 will look for the keys in environment variables:
        AWS_ACCESS_KEY_ID: The access key for your AWS account.
        AWS_SECRET_ACCESS_KEY: The secret key for your AWS account.
        AWS_SESSION_TOKEN: The session key for your AWS account.
        This is only needed when you are using temporary credentials.
        The AWS_SECURITY_TOKEN environment variable can also be used,
        but is only supported for backwards compatibility purposes.
        AWS_SESSION_TOKEN is supported by multiple AWS SDKs besides python.
        """

        session = None

        if self.key_file is not None:
            credfile = os.path.expandvars(os.path.expanduser(self.key_file))

            try:
                with open(credfile, 'r') as f:
                    creds = json.load(f)
            except json.JSONDecodeError as e:
                logger.error(f"Could not create AWS session")
                raise e

            except Exception as e:
                logger.error(f"Could not create AWS session")
                raise e

            session = boto3.session.Session(region_name=self.region, **creds)
        elif self.profile is not None:
            session = boto3.session.Session(
                profile_name=self.profile, region_name=self.region
            )
        else:
            session = boto3.session.Session(region_name=self.region)
        logger.error(f"Created AWS session")
        return session

    def _create_bucket_if_nonexistent(self) -> None:
        """
        Create the S3 bucket we will need if it doesn't already exist
        """
        location = {'LocationConstraint': self.region}
        response = self.client.list_buckets()
        buckets = [bucket['Name'] for bucket in response['Buckets']]

        if self.bucket_name in buckets:
            logger.debug(f"Found existing bucket. ID: {self.bucket_name}")
            return

        logger.debug(f"No existing bucket. Creating: {self.bucket_name}")
        self.s3.create_bucket(Bucket=self.bucket_name,
                              CreateBucketConfiguration=location)

    def store(self, r_id: str) -> None:
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

        sha1Hash = hashlib.sha1(open(current_location, "rb").read())
        sha1Hashed = sha1Hash.hexdigest()

        other_copies = self.metadata_man.get_copy_info(
            r_id, self.adapter_id)
        if len(other_copies) != 0:
            logger.debug(
                f"Other copies of {r_id} from {self.adapter_id} exist")
            return

        if sha1Hashed == checksum:
            locator = '{}_{}'.format(r_id, name)
            self.s3.Bucket(
                self.bucket_name).upload_file(
                current_location,
                locator)
        else:
            logger.error(f"Checksum Mismatch on {r_id} from {self.adapter_id}")
            raise ChecksumMismatchException

        self.metadata_man.add_copy(
            r_id,
            self.adapter_id,
            locator,
            sha1Hashed,
            self.adapter_type,
            canonical=False)

    def _store_canonical(self, current_path: str, r_id: str,
                         checksum: str, filename: str) -> str:
        """
            Store a canonical copy of a resource in this adapter.

            If we're using the S3Adapter as a canonical adapter, we need
            to be able to store from a current path, taking in a generated UUID,
            rather than looking info up from the database.

            :param current_path - current path to object
            :param r_id - UUID of resource you're storing
            :param checksum - checksum of resource
            :param filename - filename of resource you're storing

        """
        logger.debug(
            f"Storing canonical object {r_id} to adapter {self.adapter_id}")
        current_location = current_path

        sha1Hash = hashlib.sha1(open(current_location, "rb").read())
        sha1Hashed = sha1Hash.hexdigest()

        locator = "canonical_{}_{}".format(r_id, filename)

        other_copies = self.metadata_man.get_canonical_copy_metadata(
            r_id)
        if len(other_copies) != 0:
            logger.error(
                f"Other canonical copies of {r_id} from {self.adapter_id} exist")
            raise StorageFailedException

        if sha1Hashed == checksum:
            self.s3.Bucket(self.bucket_name).upload_file(current_path, locator)
        else:
            logger.error(f"Checksum Mismatch on {r_id} from {self.adapter_id}")
            raise ChecksumMismatchException

        self.metadata_man.add_copy(
            r_id,
            self.adapter_id,
            locator,
            sha1Hashed,
            self.adapter_type,
            canonical=True)

        return locator

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
        copy_locator = copy_info[3]
        real_hash = copy_info[4]

        new_location = "{}/{}".format(self.ret_dir, filename)

        if real_hash == expected_hash:
            self.s3.Bucket(
                self.bucket_name).download_file(
                copy_locator,
                new_location)
        else:
            logger.error(f"Checksum Mismatch on object {r_id}")
            raise ChecksumMismatchException

        return new_location

    def update(resource_id: str, updated_path: str) -> None:
        """
        Update a resource with a new object. Preserves UUID and all other metadata (levels, etc.)

        :param r_id - the UUID of the object you'd like to update
        :param updated_path - path to the contents of the updated object.

        """
        pass

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
        locator = copy_info[3]

        self.client.delete_object(
            Bucket=self.bucket_name, Key=locator)
        self.cursor.execute("delete from copies where copy_id=?",
                            [copy_info[0]])
        self.conn.commit()

    def _delete_canonical(self, r_id: str) -> None:
        """
        Delete a canonical copy of a resource from this adapter.
        Delete the corresponding entry in the `copies` table.

        :param r_id - the resource to retrieve's UUID
        """
        logger.debug(
            f"Deleting canonical copy of object {r_id} from {self.adapter_id}")
        try:
            copy_info = self.metadata_man.get_canonical_copy_metadata(
                r_id)[0]
        except IndexError:
            logger.debug(
                f"Canonical copy of {r_id} on {self.adapter_id} has already been deleted.")
            return

        locator = copy_info[3]

        self.client.delete_object(
            Bucket=self.bucket_name, Key=locator)

        self.metadata_man.delete_copy_metadata(copy_info[0])

    def get_actual_checksum(self, r_id: str,
                            delete_after_download: bool = True) -> str:
        """
        Returns an exact checksum of a resource, not relying on the metadata db.

        If possible, implementations of get_actual_checksum should do no file I/O.
            For S3, we need to download and checksum manually. :/

        :param r_id - resource we want the checksum of
        :param delete_after_download - True if the file should be downloaded after the
            checksum is calculated
        """
        logger.debug(
            f"Getting actual checksum of object {r_id} from adapter {self.adapter_id}")
        new_path = self.retrieve(r_id)

        sha1Hash = hashlib.sha1(open(new_path, "rb").read())
        sha1Hashed = sha1Hash.hexdigest()

        if delete_after_download:
            logger.debug(f"Delete after download enabled on {self.adapter_id}")
            os.remove(new_path)

        return sha1Hashed
