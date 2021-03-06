import os
import hashlib
import pickle
from pathlib import Path
import logging


try:
    from googleapiclient.discovery import build
    from google_auth_oauthlib.flow import InstalledAppFlow
    from google.auth.transport.requests import Request
    from apiclient.http import MediaFileUpload, MediaIoBaseDownload

except ImportError:
    _google_enabled = False
else:
    _google_enabled = True

from libreary.exceptions import ResourceNotIngestedException, ChecksumMismatchException
from libreary.exceptions import StorageFailedException, NoCopyExistsException, OptionalModuleMissingException

# Google Drive Scope
SCOPES = ['https://www.googleapis.com/auth/drive']

logger = logging.getLogger(__name__)


class GoogleDriveAdapter():
    """docstring for GoogleDriveAdapter

        An Adapter allows LIBREary to save copies of digital objects
            to different places across cyberspace. Working with many
            adapters in concert, one should be able do save sufficient
            copies to places they want them.

        DriveAdapter allows you to store objects in Google Drive

    """

    def __init__(self, config: dict, metadata_man: object = None):
        """
        Constructor for GoogleDriveAdapter. Expects a python dict :param `config`
            in the following format:

        You must have already created the Google Drive directory you wish to use for this to work.

        ```{json}
        {
        "metadata": {
            "db_file": "path to metadata db"
        },
        "adapter": {
            "folder_path": "Name of google drive folder for storage. LIBREary will create this folder",
            "adapter_identifier": "friendly identifier",
            "adapter_type": "GoogleDriveAdapter",
            "credentials_file":"Path to credentials file. See get_google_client docs for more",
            "token_file":"Path to place you want to save a token file",
        },
        "options": {
            "dropbox_dir": "path to dropbox directory",
            "output_dir": "path to output directory"
        },
        "canonical":"(boolean) true if this is the canonical adapter"
        }
        """
        try:
            self.adapter_id = config["adapter"]["adapter_identifier"]
            self.token_file = config["adapter"]["token_file"]
            self.dropbox_dir = config["options"]["dropbox_dir"]
            self.folder_path = config["adapter"]["folder_path"]
            self.adapter_type = "GoogleDriveAdapter"
            self.ret_dir = config["options"]["output_dir"]
            self.credentials_file = config["adapter"]["credentials_file"]

            self.metadata_man = metadata_man
            if self.metadata_man is None:
                raise KeyError

            logger.debug("Creating Drive Adapter")
        except KeyError:
            logger.error("Invalid configuration for Drive Adapter")
            raise KeyError

        if not _google_enabled:
            logger.error(
                "Google Drive adapter requires the googleapiclient module.")
            raise OptionalModuleMissingException(
                ['googleapiclient'], "Google Drive adapter requires the googleapiclient module.")

        self.get_google_client()
        self.dir_id = self._get_or_create_folder()

    def get_google_client(self) -> None:
        """
        Build a Google Drive client object.

        Important to note that this uses an OAUTH flow, so you'll
        need to run it from a computer that has a web browser you can use.

        Store the creds JSON file in the place you note in `config["adapter"]["credentials_file"]`
        A token will be stored in `config["adapter"]["token_file"]`.

        If you are running LIBREary on a headless server, I recommend getting a token first,
        and saving the token file on the server, so that you don't need to mess around with
        headless browsers etc.

        Get creds JSON file from here:
            https://developers.google.com/drive/api/v3/quickstart/python?authuser=3
        """
        creds = None
        # The file token.pickle stores the user's access and refresh tokens, and is
        # created automatically when the authorization flow completes for the first
        # time.

        logger.debug("Attempting to acquire google credentials")
        # We save this in the self.config["token_file"] file
        if os.path.exists(self.token_file):
            with open(self.token_file, 'rb') as token:
                creds = pickle.load(token)
        # If there are no (valid) credentials available, let the user log in.
        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                logger.debug("Google credentials expired. Refreshing")
                creds.refresh(Request())
            else:
                logger.debug(
                    "Google credentials not found. Acquiring new token")
                flow = InstalledAppFlow.from_client_secrets_file(
                    self.credentials_file, SCOPES)
                creds = flow.run_local_server(port=0)
            # Save the credentials for the next run
            with open(self.token_file, 'wb') as token:
                logger.debug(f"Saving token to file {self.token_file}")
                pickle.dump(creds, token)

        logger.debug("Building GoogleDrive API service")
        self.service = build('drive', 'v3', credentials=creds)

    def _list_objects(self) -> None:
        """
        Sanity-check method for devs to use. Lists top 1000 items in drive
        """
        logger.debug("Testing drive connection")
        results = self.service.files().list(
            pageSize=1000, fields="nextPageToken, files(id, name)").execute()
        items = results.get('files', [])

        if not items:
            logger.info('No files found.')
        else:
            for item in items:
                logger.info(item)

    def _get_or_create_folder(self) -> str:
        """
        If the folder specified in config exists, get its id.
        If not, create it.
        """
        page_token = None
        dir_id = None
        response = self.service.files().list(q="mimeType='application/vnd.google-apps.folder' and name='{}'".format(self.folder_path),
                                             spaces='drive',
                                             fields='nextPageToken, files(id, name)',
                                             pageToken=page_token).execute()
        for file in response.get('files', []):
            dir_id = file.get('id')
            page_token = response.get('nextPageToken', None)
            if page_token is None:
                logger.debug(f"Found existing directory. ID: {dir_id}")
                break

        if not dir_id:

            file_metadata = {
                'name': self.folder_path,
                'mimeType': 'application/vnd.google-apps.folder'
            }
            file = self.service.files().create(body=file_metadata,
                                               fields='id').execute()
            dir_id = file.get('id')
            logger.debug(
                f"Could not find existing directory. 9io9o0Created new one - 9io9o0ID: {dir_id}")

        return dir_id

    def _upload_file(self, filename: str, current_path: str) -> str:
        """
        Helper method to upload a file to drive, in the directory
        LIBRE-ary is configured to use.

        :param filename - name of file to upload
        :param current_path - place where the file is right now
        """
        logger.debug(f"Uploading file {filename} to Drive.")
        file_metadata = {'name': filename,
                         'parents': [self.dir_id]}
        media = MediaFileUpload(current_path,
                                mimetype='image/jpeg')
        file = self.service.files().create(body=file_metadata,
                                           media_body=media,
                                           fields='id').execute()
        f_id = file.get('id')
        return f_id

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

        sha1Hash = hashlib.sha1(open(current_location, "rb").read())
        sha1Hashed = sha1Hash.hexdigest()

        new_name = "{}_{}".format(r_id, name)

        other_copies = self.metadata_man.get_copy_info(
            r_id, self.adapter_id)

        if len(other_copies) != 0:
            logger.debug(
                f"Other copies of {r_id} from {self.adapter_id} exist")
            return

        if sha1Hashed == checksum:
            locator = self._upload_file(new_name, current_location)
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
        copy_locator = copy_info[3]
        real_hash = copy_info[4]

        new_location = "{}/{}".format(self.ret_dir, filename)

        if real_hash == expected_hash:
            self._download_file(copy_locator, new_location)
        else:
            logger.error(f"Checksum Mismatch on object {r_id}")
            raise ChecksumMismatchException

        return new_location

    def _download_file(self, locator: str, new_loc: str) -> None:
        """
        Helper method to download a file from drive, in the directory
        LIBRE-ary is configured to use.

        :param locator - google drive ID
        :param new_loc - place the file should go
        """
        request = self.service.files().get_media(fileId=locator)
        Path(new_loc).touch()
        fh = open(new_loc, "wb")
        downloader = MediaIoBaseDownload(fh, request)
        done = False
        while done is False:
            status, done = downloader.next_chunk()
            if not status:
                raise ChecksumMismatchException

    def update(self, r_id: str, updated: str) -> None:
        """
        Update a resource with a new object. Preserves UUID and all other metadata (levels, etc.)

        :param r_id - the UUID of the object you'd like to update
        :param updated_path - path to the contents of the updated object.

        """
        pass

    def _store_canonical(self, current_path: str, r_id: str,
                         checksum: str, filename: str) -> str:
        """
            If we're using the GoogleDrive as a canonical adapter, we need
            to be able to store from a current path, taking in a generated UUID,
            rather than looking info up from the database.

            :param current_path - current path to object
            :param r_id - UUID of resource you're storing
            :param checksum - checksum of resource
            :param filename - filename of resource you're storing
        """
        logger.debug(
            f"Storing canonical copy of object {r_id} to {self.adapter_id}")

        new_name = "canonical_{}_{}".format(r_id, filename)

        sha1Hash = hashlib.sha1(open(current_path, "rb").read())
        sha1Hashed = sha1Hash.hexdigest()
        other_copies = self.metadata_man.get_canonical_copy_metadata(
            r_id)
        if len(other_copies) != 0:
            logger.error(
                f"Other canonical copies of {r_id} from {self.adapter_id} exist")
            raise StorageFailedException

        if sha1Hashed == checksum:
            locator = self._upload_file(new_name, current_path)
        else:
            logger.error(f"Checksum Mismatch on object {r_id}")
            raise ChecksumMismatchException

        self.metadata_man.add_copy(
            r_id,
            self.adapter_id,
            locator,
            sha1Hashed,
            self.adapter_type,
            canonical=True)

        return locator

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
        copy_locator = copy_info[3]

        self.service.files().delete(fileId=copy_locator).execute()

        self.metadata_man.delete_copy_metadata(copy_info[0])

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

        copy_locator = copy_info[3]

        self.service.files().delete(fileId=copy_locator).execute()

        self.metadata_man.delete_copy_metadata(copy_info[0])

    def get_actual_checksum(self, r_id: str,
                            delete_after_download: bool = True) -> str:
        """
        Return an exact checksum of a resource, not relying on the metadata db.

        The :param deep trusts the tag we've given google drive on ingestion,
        if True, it will retrieve and recompute
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
