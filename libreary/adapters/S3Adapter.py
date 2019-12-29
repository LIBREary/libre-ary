import json
import os
import sqlite3

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
        self.client = self.session.client('s3')
        self.s3 = self.session.resource('s3')


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


    def store(resource_id):
        """
        Given a resource id, saves resource and returns confirmation
        """
        pass

    def _store_canonical(current_path, r_id, delete_after_store=False):
        """
        Ingest files as the canonical adapter. Don't run this function. Ingester will call it.
        """
        pass


    def retrieve(resource_id):
        """
        Given a resource id, load it from the adapter's repository

        Downloads file to local dir, returns path.
        """
        pass

    def update(resource_id, updated):
        """
        Overwrite the remote resource specified with what's passed into :param updated.
        """
        pass

    def delete(resource_id):
        """
        delete a resource
        """
        pass

    def get_actual_checksum(self, r_id):
        """
        Return an exact checksum of a resource, not relying on the metadata db

        If possible, this should be done with no file I/O
        """
        pass

    def delete_canonical(self, r_id):
        pass


if __name__ == '__main__':
    def create_config_for_adapter(adapter_id, adapter_type):
        base_config = json.load(open("{}/{}_config.json".format(CONFIG_DIR, adapter_id)))
        general_config = json.load(open("{}/agent_config.json".format(CONFIG_DIR)))
        full_adapter_conf = {}
        full_adapter_conf["adapter"] = base_config["adapter"]
        full_adapter_conf["adapter"]["adapter_type"] = adapter_type
        full_adapter_conf["metadata"] = general_config["metadata"]
        full_adapter_conf["options"] = general_config["options"]

        return full_adapter_conf

    config = create_config_for_adapter("s3", "S3Adapter")
    adapter = S3Adapter(config)