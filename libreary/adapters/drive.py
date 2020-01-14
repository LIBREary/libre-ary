import sqlite3
import os
from shutil import copyfile
import hashlib
import json
import pickle

try:
    from googleapiclient.discovery import build
    from google_auth_oauthlib.flow import InstalledAppFlow
    from google.auth.transport.requests import Request
    from pydrive.auth import GoogleAuth
    from apiclient.http import MediaFileUpload,MediaIoBaseDownload

except ImportError:
    _google_enabled = False
else:
    _google_enabled = True

from libreary.exceptions import ResourceNotIngestedException, ChecksumMismatchException, NoCopyExistsException, OptionalModuleMissingException
from libreary.exceptions import RestorationFailedException, AdapterCreationFailedException, AdapterRestored, StorageFailedException, ConfigurationError

SCOPES = ['https://www.googleapis.com/auth/drive']

class GoogleDriveAdapter():
    """docstring for GoogleDriveAdapter
        
        An Adapter allows LIBREary to save copies of digital objects
            to different places across cyberspace. Working with many
            adapters in concert, one should be able do save sufficient
            copies to places they want them.

        DriveAdapter allows you to store objects in Google Drive
        
    """

    def __init__(self, config:dict):
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
            "credentials_file":"Path to credentials file. See get_google_client docs for more"
        },
        "options": {
            "dropbox_dir": "path to dropbox directory",
            "output_dir": "path to output directory"
        },
        "canonical":"(boolean) true if this is the canonical adapter"
        }
        """
        self.metadata_db = os.path.realpath(config['metadata'].get("db_file"))
        self.adapter_id = config["adapter"]["adapter_identifier"]
        self.conn = sqlite3.connect(self.metadata_db)
        self.cursor = self.conn.cursor()
        self.token_file = config["adapter"]["token_file"]
        self.dropbox_dir = config["options"]["dropbox_dir"]
        self.folder_path = config["adapter"]["folder_path"]
        self.adapter_type = "LocalAdapter"
        self.ret_dir = config["options"]["output_dir"]
        self.credentials_file = config["adapter"]["credentials_file"]

        if not _google_enabled:
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

        #gauth = GoogleAuth()
        # Create local webserver and auto handles authentication.
        #gauth.LocalWebserverAuth()
        creds = None
        # The file token.pickle stores the user's access and refresh tokens, and is
        # created automatically when the authorization flow completes for the first
        # time.

        # We save this in the self.config["token_file"] file
        if os.path.exists(self.token_file):
            with open(self.token_file, 'rb') as token:
                creds = pickle.load(token)
        # If there are no (valid) credentials available, let the user log in.
        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                creds.refresh(Request())
            else:
                flow = InstalledAppFlow.from_client_secrets_file(self.credentials_file, SCOPES)
                creds = flow.run_local_server(port=0)
            # Save the credentials for the next run
            with open(self.token_file, 'wb') as token:
                pickle.dump(creds, token)

        self.service = build('drive', 'v3', credentials=creds)

    def _list_objects(self):
        """
        Sanity-check method for devs to use. Lists top 1000 items in drive
        """
        results = self.service.files().list(
            pageSize=1000, fields="nextPageToken, files(id, name)").execute()
        items = results.get('files', [])

        if not items:
            print('No files found.')
        else:
            for item in items:
                print(item)

    def _get_or_create_folder(self):

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
                break

        if not dir_id:
            file_metadata = {
                'name': self.folder_path,
                'mimeType': 'application/vnd.google-apps.folder'
                }
            file = self.service.files().create(body=file_metadata,
                                    fields='id').execute()
            dir_id = file.get('id')

        return dir_id


    def _upload_file(self, filename, current_path):
        """
        Helper method to upload a file to drive, in the directory
        LIBRE-ary is configured to use.
        """
        file_metadata = {'name': filename,
                        'parents': [self.dir_id]}
        media = MediaFileUpload(current_path,
                        mimetype='image/jpeg')
        file = self.service.files().create(body=file_metadata,
                                    media_body=media,
                                    fields='id').execute()
        f_id = file.get('id')
        return f_id

        

    def store(self, r_id:str) -> str:
        """
        Store a copy of a resource in this adapter.

        Store assumes that the file is in the `dropbox_dir`.
        AdapterManager will always verify that this is the case.

        :param r_id - the resource to store's UUID
        """
        file_metadata = self.load_metadata(r_id)[0]
        dropbox_path = file_metadata[1]
        checksum = file_metadata[4]
        name = file_metadata[3]
        current_location = "{}/{}".format(self.dropbox_dir, name)

        sha1Hash = hashlib.sha1(open(current_location, "rb").read())
        sha1Hashed = sha1Hash.hexdigest()

        new_name = "{}_{}".format(r_id, name)

        other_copies = self.cursor.execute(
            "select * from copies where resource_id='{}' and adapter_identifier='{}' and not canonical = 1 limit 1".format(
                r_id, self.adapter_id)).fetchall()
        if len(other_copies) != 0:
            print("Other copies from this adapter exist")
            return

        if sha1Hashed == checksum:
            locator = self._upload_file(filename, current_path)
        else:
            print("Checksum Mismatch")
            raise Exception

        self.cursor.execute(
            "insert into copies values ( ?,?, ?, ?, ?, ?, ?)",
            [None, r_id, self.adapter_id, locator, sha1Hashed, self.adapter_type, False])
        self.conn.commit()

    def retrieve(self, r_id):
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

    def update(self, r_id, updated):
        pass

    def _store_canonical(self, current_path, r_id, checksum, filename):
        """
            If we're using the GoogleDrive as a canonical adapter, we need
            to be able to store from a current path, taking in a generated UUID,
            rather than looking info up from the database.
        """

        new_name = "{}_{}".format(r_id, filename)

        sha1Hash = hashlib.sha1(open(current_path, "rb").read())
        sha1Hashed = sha1Hash.hexdigest()

        sql = "select * from copies where resource_id='{}' and adapter_identifier='{}' and canonical = 1 limit 1".format(
            str(r_id), self.adapter_id)
        other_copies = self.cursor.execute(sql).fetchall()
        if len(other_copies) != 0:
            print("Other canonical copies from this adapter exist")
            raise StorageFailedException

        if sha1Hashed == checksum:
            locator = self._upload_file(new_name, current_path)
        else:
            raise ChecksumMismatchException

        self.cursor.execute(
            "insert into copies values ( ?,?, ?, ?, ?, ?, ?)",
            [None, r_id, self.adapter_id, locator, sha1Hashed, self.adapter_type, True])
        self.conn.commit()

        return locator

    def delete(self, r_id):
        copy_info = self.cursor.execute(
            "select * from copies where resource_id=? and adapter_identifier=? and not canonical = 1 limit 1",
            (r_id, self.adapter_id)).fetchall()

        if len(copy_info) == 0:
            # We've already deleted, probably as part of another level
            return

        copy_info = copy_info[0]
        expected_hash = copy_info[4]
        copy_locator = copy_info[3]

        self.service.files().delete(fileId=copy_locator).execute()

        self.cursor.execute("delete from copies where copy_id=?",
                            [copy_info[0]])
        self.conn.commit()

    def _delete_canonical(self, r_id):
        copy_info = self.cursor.execute(
            "select * from copies where resource_id=? and adapter_identifier=? and canonical = 1 limit 1",
            (r_id, self.adapter_id)).fetchall()[0]
        expected_hash = copy_info[4]
        copy_locator = copy_info[3]

        self.service.files().delete(fileId=copy_locator).execute()

        self.cursor.execute("delete from copies where copy_id=?",
                            [copy_info[0]])
        self.conn.commit()

    def load_metadata(self, r_id):
        return self.cursor.execute(
            "select * from resources where uuid='{}'".format(r_id)).fetchall()

    def get_actual_checksum(self, r_id, delete_after_download: bool = True):
        """
        Return an exact checksum of a resource, not relying on the metadata db.

        The :param deep trusts the tag we've given google drive on ingestion,
        if True, it will retrieve and recompute
        """
        new_path = self.retrieve(r_id)

        sha1Hash = hashlib.sha1(open(new_path, "rb").read())
        sha1Hashed = sha1Hash.hexdigest()

        if delete_after_download:
            os.remove(new_path)

        return sha1Hashed
