import json
import os
import sqlite3
import hashlib

try:
    import boto3
    from botocore.exceptions import ClientError

except ImportError:
    _boto_enabled = False
else:
    _boto_enabled = True

CONFIG_DIR = "../config"

class S3Adapter:

    def __init__(self, config):
        """
        This should handle configuration (auth, etc.) and set up the
        metadata db connection

        You must have already created the S3 bucket for this to work.
        """
        self.config = config

        self.metadata_db = os.path.realpath(config['metadata'].get("db_file"))
        self.adapter_id = config["adapter"]["adapter_identifier"]
        self.conn = sqlite3.connect(self.metadata_db)
        self.cursor = self.conn.cursor()
        self.adapter_type = "S3Adapter"
        self.dropbox_dir = config["options"]["dropbox_dir"]
        self.ret_dir = config["options"]["output_dir"]


        if not _boto_enabled:
            raise OptionalModuleMissingException(['boto3'], "S3 adapter requires the boto3 module.")

        self.profile = self.config["adapter"].get("profile")
        self.key_file = self.config["adapter"].get("key_file")
        self.bucket_name = self.config["adapter"].get("bucket_name")
        self.region = self.config["adapter"].get("bucket_name")

        self.env_specified = os.getenv("AWS_ACCESS_KEY_ID") is not None and os.getenv("AWS_SECRET_ACCESS_KEY") is not None
        if self.profile is None and self.key_file is None and not self.env_specified:
            raise ConfigurationError("Must specify either profile', 'key_file', or "
                                     "'AWS_ACCESS_KEY_ID' and 'AWS_SECRET_ACCESS_KEY' environment variables.")

        try:
            self.initialize_boto_client()
        except Exception as e:
            raise e


    def initialize_boto_client(self):
        """Initialize the boto client."""

        self.session = self.create_session()
        #self.client = self.session.client('s3')
        #self.s3 = self.session.resource('s3')
        self.client = boto3.client('s3')
        self.s3 = boto3.resource('s3')

    def create_session(self):
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
             
                raise e

            except Exception as e:
           
                raise e

            session = boto3.session.Session(region_name=self.region, **creds)
        elif self.profile is not None:
            session = boto3.session.Session(
                profile_name=self.profile, region_name=self.region
            )
        else:
            session = boto3.session.Session(region_name=self.region)

        return session


    def store(self, r_id):
        """
        Given a resource id, saves resource and returns confirmation.

        Assumes file is stored as `name` in `dropbox_dir`. AdapterManager will verify this
        """
        file_metadata = self.load_metadata(r_id)[0]
        dropbox_path = file_metadata[1]
        checksum = file_metadata[4]
        name = file_metadata[3]
        current_location = "{}/{}".format(self.dropbox_dir, name)

        sha1Hash = hashlib.sha1(open(current_location,"rb").read())
        sha1Hashed = sha1Hash.hexdigest()


        other_copies = self.cursor.execute(
            "select * from copies where resource_id='{}' and adapter_identifier='{}' and not canonical = 1 limit 1".format(
            r_id, self.adapter_id)).fetchall()
        if len(other_copies) != 0:
            print("Other copies from this adapter exist")
            raise StorageFailedException

        if sha1Hashed == checksum:
            locator = '{}_{}'.format(name, r_id)
            self.s3.Bucket(self.bucket_name).upload_file(current_location, locator)
        else:
            print("Checksum Mismatch")
            raise Exception

        

        self.cursor.execute(
            "insert into copies values ( ?,?, ?, ?, ?, ?, ?)",
            [None, r_id, self.adapter_type, locator, sha1Hashed, self.adapter_id, False])
        self.conn.commit()

    def _store_canonical(self, current_path, r_id, checksum, filename):
        """
        Ingest files as the canonical adapter. Don't run this function. Ingester will call it.
        """
        current_location = current_path

        sha1Hash = hashlib.sha1(open(current_location,"rb").read())
        sha1Hashed = sha1Hash.hexdigest()
           
        locator = "{}_{}".format(filename, r_id) 

        sql = "select * from copies where resource_id='{}' and adapter_identifier='{}' and canonical = 1 limit 1".format( str(r_id), self.adapter_id)
        other_copies = self.cursor.execute(sql).fetchall()
        if len(other_copies) != 0:
            print("Other copies from this adapter exist")
            raise StorageFailedException

        if sha1Hashed == checksum:
            self.s3.Bucket(self.bucket_name).upload_file(current_path, locator)
        else:
            print("Checksum Mismatch")
            raise ChecksumMismatchException

        self.cursor.execute(
            "insert into copies values ( ?,?, ?, ?, ?, ?, ?)",
            [None, r_id, self.adapter_id,  locator, sha1Hashed, self.adapter_type, True])
        self.conn.commit()

        return locator


    def retrieve(self, r_id):
        copy_info = self.cursor.execute(
            "select * from copies where resource_id=? and adapter_identifier=? limit 1",
            (r_id, self.adapter_id)).fetchall()[0]
        expected_hash = copy_info[4]
        copy_locator = copy_info[3]
        real_hash = copy_info[4]

        new_location = "{}/{}".format(self.ret_dir, r_id)

        if real_hash == expected_hash:
            self.s3.Bucket(self.bucket_name).download_file(copy_locator, new_location)
        else:
            print("Checksum Mismatch")
            
        return new_location

    def update(resource_id, updated):
        """
        Overwrite the remote resource specified with what's passed into :param updated.
        """
        pass

    def delete(self, r_id):
        copy_info = self.cursor.execute(
            "select * from copies where resource_id=? and adapter_identifier=? and not canonical = 1 limit 1",
            (r_id, self.adapter_id)).fetchall()[0]
        expected_hash = copy_info[4]
        locator = copy_info[3]

        response = self.client.delete_object(Bucket=self.bucket_name, Key=locator)
        self.cursor.execute("delete from copies where copy_id=?",
                            [copy_info[0]])
        self.conn.commit()

    def delete_canonical(self, r_id):
        copy_info = self.cursor.execute(
            "select * from copies where resource_id=? and adapter_identifier=? and canonical = 1 limit 1",
            (r_id, self.adapter_id)).fetchall()[0]
        expected_hash = copy_info[4]
        locator = copy_info[3]

        response = self.client.delete_object(Bucket=self.bucket_name, Key=locator)

        self.cursor.execute("delete from copies where copy_id=?",
                            [copy_info[0]])
        self.conn.commit()

    def get_actual_checksum(self, r_id, delete_after_download=True):
        """
        Return an exact checksum of a resource, not relying on the metadata db

        If possible, this should be done with no file I/O

        For S3, we need to download and checksum manually. :/
        """
        new_path = self.retrieve("r_id")

        sha1Hash = hashlib.sha1(open(new_path,"rb").read())
        sha1Hashed = sha1Hash.hexdigest()

        if delete_after_download:
            os.remove(new_path)

        return sha1Hashed

    def load_metadata(self, r_id):
        return self.cursor.execute(
            "select * from resources where uuid='{}'".format(r_id)).fetchall()