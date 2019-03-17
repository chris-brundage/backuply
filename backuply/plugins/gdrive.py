import argparse
import backoff
import errno
import googleapiclient.http
import mimetypes
import multiprocessing
import httplib2
import shutil
import pytz
import os
import threading
from abc import ABC, abstractmethod
from datetime import datetime
from dateutil.parser import parse as dtp
from googleapiclient.http import MediaFileUpload
from queue import Queue
from pathlib import Path
from apiclient import discovery, errors
from oauth2client import file, client, tools
from tzlocal import get_localzone
from backuply.jobs import BackupJob
from backuply.errors import InvalidConfigurationError


def upload_giveup(e):
    """Tells our retry decorator on resumable Google Drive uploads when to give up.

    :param e: (apiclient.errors.HttpError) The exception raised by the Google API client.
    :return: (bool)
    """
    if e.resp.status not in (500, 502, 503, 504):
        return True
    return False


class GoogleDriveBackupJob(BackupJob):
    backup_type = 'gdrive'

    def __init__(self, source, backup_target, exclude_file=None, verbose=False,
                 debug=False, *args, **kwargs):
        super(GoogleDriveBackupJob, self).__init__(verbose, debug,
                                                   *args, **kwargs)

        self.backup_target = backup_target
        if not os.path.exists(source):
            raise IOError(errno.ENOENT,
                          'The backup source {} does not exist.'.format(
                              source))
        self.source = Path(source)

        if exclude_file is not None:
            if not os.path.exists(exclude_file):
                raise IOError(errno.ENOENT,
                              'The exclude file {} does not exist.'.format(
                                  exclude_file))
            self.exclude_file = Path(exclude_file)
        else:
            self.exclude_file = None

        self.overwrite = self.settings.get('overwrite', False)
        self.update = not self.overwrite

        conf_dir = self.settings.conf_dir
        self.drive_client = GoogleDrivePlugin(conf_dir, self.backup_target,
                                              None, update=self.update)

        self.num_workers = self.settings.get('num_workers',
                                             multiprocessing.cpu_count())
        self.thread_pool = []
        self.queue = Queue(maxsize=self.num_workers * 100)
        for i in range(self.num_workers):
            t = threading.Thread(target=self.backup_worker)
            t.start()
            self.thread_pool.append(t)

    def backup_worker(self):
        while True:
            job = self.queue.get()
            if job is None:
                break
            self.drive_client.upload_file(job['filename'], job['parent_dir'])
            self.queue.task_done()

    def backup(self, *args, **kwargs):
        for f in self.source.glob('**/*'):
            if not f.is_file():
                continue
            # TODO: Remove this conditional when testing is done.
            if f.stat().st_size >= 100 * 1024 * 1024:
                print(f, 'is too big for testing', sep=' ')
                continue
            job = {
                'filename': f,
                'parent_dir': None,
            }
            self.queue.put_nowait(job)
            if self.queue.qsize() + self.num_workers >= self.queue.maxsize:
                self.queue.join()
        self.queue.join()
        for i in range(self.num_workers):
            self.queue.put(None)
        for t in self.thread_pool:
            t.join()

    @staticmethod
    def add_arguments(parser: argparse.ArgumentParser):
        gdrive_group = parser.add_argument_group('GOOGLE DRIVE BACKUPS')
        gdrive_group.add_argument('--install-secrets',
                                  dest='client_secrets_file',
                                  metavar='/path/to/client_secrets.json',
                                  help='Path to a client_secrets.json file you wish to install to the Backuply configuration directory.')
        return parser


class GoogleApiClient(ABC):
    @property
    @abstractmethod
    def credentials_file(self): pass

    def __init__(self, conf_dir: Path, cmd_flags):
        """Constructor.

        :param conf_dir: (pathlib.Path) The directory where our Google API config files are.
        :param cmd_flags: (argparse.Namespace) Flags passed from command line scripts.
        """
        if not conf_dir.parent.is_dir():
            raise IOError(errno.ENOTDIR,
                          'The parent directory for {} does not exist or is not a directory.'.format(
                              conf_dir))

        self.conf_dir = conf_dir
        self.cmd_flags = cmd_flags

        if not conf_dir.exists():
            conf_dir.mkdir()

        self.http = httplib2.Http()
        self.client_secrets = self._client_secrets()

        self.flow = client.flow_from_clientsecrets(self.client_secrets,
                                                   self.scope,
                                                   message=tools.message_if_missing(
                                                       self.client_secrets))
        self.credentials = self.set_credentials()

    def _client_secrets(self):
        """If provided on the command line, installs our client secrets file in its desired location.

        Otherwise it just returns the path to the client secrets file.

        :return: (pathlib.Path) The client secrets filename.
        """
        client_secrets_file = self.conf_dir.joinpath('client_secrets.json')
        try:
            if self.cmd_flags.client_secrets_file:
                new_client_secret = Path(self.cmd_flags.client_secrets_file)
                if not new_client_secret.exists():
                    raise IOError(errno.ENOENT,
                                  'The client secrets file {} does not exist.'.format(
                                      new_client_secret))
                if new_client_secret.parent != self.conf_dir:
                    shutil.copyfile(new_client_secret, client_secrets_file)
        except AttributeError:
            pass

        return client_secrets_file

    def set_credentials(self):
        """Retrieves, validates, and stores OAuth credentials.

        :return: The credentials.
        """
        credentials = file.Storage(self.credentials_file).get()

        if credentials is None or credentials.invalid:
            storage_file = file.Storage(self.credentials_file)

            credentials = tools.run_flow(self.flow, storage_file,
                                         self.cmd_flags)

        return credentials

    def set_service(self, api_name, api_version):
        """Builds an API service against our authorized credentials.

        :param api_name: (str) The name of the Google API.
        :param api_version: (str) The API version.
        :return: The appropriate Google API client object.
        """
        self.http = self.credentials.authorize(self.http)
        return discovery.build(api_name, api_version, http=self.http)


class GoogleDrivePlugin(GoogleApiClient):
    default_scope = [
        'https://www.googleapis.com/auth/drive.file',
    ]

    @property
    def credentials_file(self):
        return self.conf_dir.joinpath('backuply_gdrive_credentials')

    def __init__(self, conf_dir, base_dir, cmd_flags, *args, **kwargs):
        """Constructor.

        :param conf_dir: (pathlib.Path) The directory where our Google API config files are.
        :param base_dir: (str) The base directory name on Google Drive.
        :param cmd_flags: (argparse.Namespace) Flags passed from command line scripts.
        :param args:
        :param kwargs:
        """
        try:
            self.scope = kwargs['scope']
        except KeyError:
            self.scope = GoogleDrivePlugin.default_scope
        try:
            self.default_chunk_size = kwargs['default_chunk_size']
        except KeyError:
            self.default_chunk_size = googleapiclient.http.DEFAULT_CHUNK_SIZE

        super(GoogleDrivePlugin, self).__init__(conf_dir, cmd_flags)

        self.service = self.set_service('drive', 'v3')
        self.base_dir = self._validate_base_dir(base_dir)
        self.update = kwargs.get('update', True)

    def _validate_base_dir(self, dir_name):
        """Makes sure the base directory for our directory structure
            exists and is accessible on Google Drive.

        :param dir_name: (str) The name of the directory.
        :return:
        """
        r = self.service.files().list(orderBy='folder,name',
                                      q='name=\'{}\''.format(dir_name))
        drive_dirs = r.execute()
        matched_dir = None
        if 'nextPageToken' not in drive_dirs:
            matched_dir = self._match_dirname(drive_dirs, dir_name)
        else:
            while drive_dirs.get('nextPageToken', False):
                matched_dir = self._match_dirname(drive_dirs, dir_name)
                if matched_dir is not None:
                    break
                r = self.service.files().list_next(previous_request=r,
                                                   previous_response=drive_dirs)
                drive_dirs = r.execute()

        if matched_dir is not None:
            return matched_dir
        return self._create_directory(dir_name)

    def should_update_file(self, src: Path, parents=None):
        """Indicates whether the provided file has been updated since its last upload.

        :param src: (pathlib.Path) The path to the file that's being uploaded.
        :param parents: (list) The Google Drive parent directories of said file.
        :return: (bool)
        """
        # In order to compare against the Drive API mtime,
        # we need a timezone aware mtime that's converted to UTC
        mtime = datetime.fromtimestamp(src.stat().st_mtime, tz=get_localzone())
        mtime = mtime.astimezone(pytz.UTC)
        # Build the file search query string
        q = 'name=\'{}\''.format(src.name)
        if parents is not None:
            for p in parents:
                q += ' and \'{}\' in parents'.format(p)
        # The list request
        r = self.service.files().list(orderBy='name,modifiedTime desc', q=q,
                                      fields='nextPageToken, files(id, name, mimeType, modifiedTime)')
        matched_files = r.execute()
        try:
            if 'nextPageToken' not in matched_files:
                for f in matched_files['files']:
                    # Skip trashed files
                    if f.get('trashed'):
                        continue
                    # We have a match, but is it newer?
                    if f['name'] == src.name:
                        drive_mtime = dtp(f['modifiedTime'])
                        if mtime > drive_mtime:
                            return True
                        return False
            else:
                # Loop through all pages of the list and do the same as above.
                while matched_files.get('nextPageToken'):
                    r = self.service.files().list_next(previous_request=r,
                                                       previous_response=matched_files)
                    matched_files = r.execute()
                    for f in matched_files['files']:
                        if f.get('trashed'):
                            continue
                        if f['name'] == src.name:
                            drive_mtime = dtp(f['modifiedTime'])
                            if mtime > drive_mtime:
                                return True
                            return False
        # There were no results from the Google Drive API
        except (AttributeError, TypeError, KeyError):
            pass
        return True

    def _match_dirname(self, drive_dirs, filename):
        """Loops through listed files in Google Drive API response,
            and finds the one matching filename

        :param drive_dirs: (dict) The Google Drive API response.
        :param filename: (str) The filename.
        :return: (dict) The matching Google Drive file.
        """
        if len(drive_dirs['files']) <= 0:
            return
        for f in drive_dirs['files']:
            try:
                if f.get('trashed', False):
                    continue
                if f['name'] == filename:
                    return f
            except KeyError as e:
                import json
                print(json.dumps(f, indent=4))
                raise e

    def _create_directory(self, dir_name, parents=None):
        """Creates a directory on Google Drive.

        :param dir_name: (str) The name of the directory.
        :param parents: (list) The parent directories.
        :return: (dict) The Google Drive API response.
        """
        metadata = {
            'name': dir_name,
            'mimeType': 'application/vnd.google-apps.folder',
        }
        if parents is not None:
            metadata['parents'] = [p['id'] for p in parents]
        directory = self.service.files().create(body=metadata).execute()
        return directory

    def _directory_structure(self, filename: Path):
        """Gets the immediate parent directory of the file to be uploaded.

        The entire structure will be created if it doesn't already exist.

        :param filename: (pathlib.Path) The file to be uploaded.
        :return: (dict) The Google Drive API response representing the directory.
        """
        parent_paths = [self.base_dir]
        for file_part in filename.parts[1:-1]:
            q = 'name=\'{}\''.format(file_part)
            if len(parent_paths) > 0:
                q += ' and \'{}\' in parents'.format(parent_paths[-1]['id'])
            r = self.service.files().list(orderBy='folder,name', q=q)
            drive_dirs = r.execute()
            if 'nextPageToken' not in drive_dirs:
                matched_dir = self._match_dirname(drive_dirs, file_part)
            else:
                matched_dir = None
                while drive_dirs.get('nextPageToken', False):
                    matched_dir = self._match_dirname(drive_dirs, file_part)
                    if matched_dir is not None:
                        break
                    r = self.service.files().list_next(previous_request=r,
                                                       previous_response=drive_dirs)
                    drive_dirs = r.execute()
            if matched_dir is None:
                matched_dir = self._create_directory(file_part, parent_paths)
            parent_paths = [matched_dir]
        return parent_paths

    @backoff.on_exception(backoff.expo, errors.HttpError, giveup=upload_giveup)
    def _resumable_upload(self, request, response=None):
        """Does the actual work of uploading the file while retrying errors.

        :param request: The Google API client request object
        :param response: The Google API client response object
        :return: The Google API client response object
        """
        while response is None:
            status, response = request.next_chunk()
            if status:
                print('Uploaded {:.2f}%%.'.format(status.progress() * 100))
        return response

    def upload_chunk_size(self, filename: Path):
        """Sets a Google approved chunk size for uploading files.

        https://developers.google.com/api-client-library/python/guide/media_upload#resumable-media-chunked-upload
        See note about chunk_size restrictions

        :param filename: (pathlib.Path) The file to be uploaded.
        :return: (int) The upload chunk size in bytes.
        """
        file_size = filename.stat().st_size
        chunk_limit = 256 * 1024

        if file_size >= chunk_limit:
            if self.default_chunk_size % chunk_limit == 0:
                return self.default_chunk_size
            else:
                return self.default_chunk_size - (
                            self.default_chunk_size % chunk_limit)
        else:
            return self.default_chunk_size

    def upload_file(self, src: Path, parent_dir=None, tries=0):
        """Uploads a file to Google Drive, and preserves its directory structure.

        :param src: (pathlib.Path) The file to be uploaded.
        :param parent_dir: (dict) The parent directory on Google Drive.
        :param tries: (int) The number of times we've already tried to do this.
        :return: (tuple) A list of parent folder ids,
            and the Google Drive API response for uploading the file.
        """
        if not src.exists():
            raise IOError(errno.ENOENT,
                          'The source file {} does not exist'.format(src))

        if src.stat().st_size > 0:
            mimetype, _ = mimetypes.guess_type(str(src), strict=False)
            resumable = True
        else:
            mimetype = 'application/octet-stream'
            resumable = False
        chunk_size = self.upload_chunk_size(src)
        print(chunk_size)

        if parent_dir is None:
            parents = self._directory_structure(src)
            parents.reverse()
            parents = [p['id'] for p in parents]
        else:
            parents = [parent_dir['id']]
        if not self.update:
            upload = True
        elif self.update and self.should_update_file(src, parents):
            upload = True
        else:
            upload = False
        if not upload:
            print(src, 'is already up to date on Google Drive', sep=' ')
            return
        upload_file = MediaFileUpload(str(src), mimetype=mimetype,
                                      resumable=resumable, chunksize=chunk_size)
        metadata = {
            'name': src.name,
            'parents': parents,
        }
        print(metadata)
        r = self.service.files().create(body=metadata, media_body=upload_file)
        try:
            if resumable:
                return parents, self._resumable_upload(r)
            else:
                return parents, r.execute()
        except errors.HttpError as e:
            if e.resp.status == 404:
                tries += 1
                if tries >= 5:
                    raise e
                return self.upload_file(src, parent_dir, tries)
            else:
                raise e


class GoogleDriveDirectoryNotFound(IOError):
    def __init__(self, message, parents=None):
        super(GoogleDriveDirectoryNotFound, self).__init__(errno.ENOENT,
                                                           message)
        self.parents = parents


if __name__ == '__main__':
    import os
    home_dir = os.path.expanduser('~')
    home_dir = os.path.join(home_dir, 'test')
    b = GoogleDriveBackupJob(home_dir, 'Backuply')
    print(b.backup())
    # conf_dir = Path(home_dir).joinpath('.backuply')
    # g = GoogleDrivePlugin(conf_dir, 'Backuply', None, None)
    # ul_file = Path(home_dir).joinpath('foo1.txt')
    # print(g.upload_file(ul_file))
