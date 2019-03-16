import backoff
import errno
import logging
import mimetypes
import googleapiclient
import httplib2
import shutil
from abc import ABC, abstractmethod
from googleapiclient.http import MediaFileUpload
from pathlib import Path
from apiclient import discovery, errors
from oauth2client import file, client, tools
from backuply.errors import InvalidConfigurationError


def upload_giveup(e):
    if e.resp.status not in (500, 502, 503, 504):
        return True
    return False


class GoogleApiClient(ABC):
    @property
    @abstractmethod
    def credentials_file(self): pass

    def __init__(self, conf_dir: Path, cmd_flags, logger):
        if logger is None:
            logger = logging.basicConfig()
        self.logger = logger

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
        credentials = file.Storage(self.credentials_file).get()

        if credentials is None or credentials.invalid:
            storage_file = file.Storage(self.credentials_file)

            credentials = tools.run_flow(self.flow, storage_file,
                                         self.cmd_flags)

        return credentials

    def set_service(self, api_name, api_version):
        self.http = self.credentials.authorize(self.http)
        return discovery.build(api_name, api_version, http=self.http)


class GoogleDrivePlugin(GoogleApiClient):
    default_scope = [
        'https://www.googleapis.com/auth/drive.file',
    ]

    @property
    def credentials_file(self):
        return self.conf_dir.joinpath('backuply_gdrive_credentials')

    def __init__(self, conf_dir, base_dir, cmd_flags, logger, scope=None):
        if scope is None:
            self.scope = GoogleDrivePlugin.default_scope
        else:
            self.scope = scope

        super(GoogleDrivePlugin, self).__init__(conf_dir, cmd_flags, logger)

        self.service = self.set_service('drive', 'v3')
        self.base_dir = self._validate_base_dir(base_dir)

    def _validate_base_dir(self, dir_name):
        # r = self.service.files().list(orderBy='folder,name',
        #                               q='name={}'.format(dir_name))
        r = self.service.files().list(orderBy='folder,name')
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
        # raise GoogleDriveDirectoryNotFound('{} was not found.'.format(dir_name))

    def _match_dirname(self, drive_dirs, filename):
        if len(drive_dirs['files']) <= 0:
            return
        for f in drive_dirs['files']:
            try:
                import json
                print(json.dumps(f, indent=4))
                if f.get('trashed', False):
                    continue
                if f['name'] == filename:
                    return f
            except KeyError as e:
                import json
                print(json.dumps(f, indent=4))
                raise e

    def _create_directory(self, dir_name, parents=None):
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
        matched_dir = None
        for file_part in filename.parts[1:-1]:
            q = 'name={}'.format(file_part)
            if len(parent_paths) > 0:
                q += ' and {} in parents'.format(parent_paths[-1])
            r = self.service.files().list(orderBy='folder,name')
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
        while response is None:
            status, response = request.next_chunk()
        return response

    def upload_file(self, src: Path, tries=0, parent_dir=None):
        if not src.exists():
            raise IOError(errno.ENOENT,
                          'The source file {} does not exist'.format(src))
        mimetype, _ = mimetypes.guess_type(str(src), strict=False)
        if parent_dir is None:
            parents = self._directory_structure(src)
            parents.reverse()
            parents = [p['id'] for p in parents]
            # parents = parents[-1]['id']
        else:
            parents = [parent_dir['id']]
        upload_file = MediaFileUpload(str(src), mimetype=mimetype,
                                      resumable=True)
        metadata = {
            'name': src.name,
            'parents': parents,
        }
        print(metadata)
        r = self.service.files().create(body=metadata, media_body=upload_file)
        try:
            return parents, self._resumable_upload(r)
        except errors.HttpError as e:
            if e.resp.status == 404:
                tries += 1
                if tries >= 5:
                    raise e
                return self.upload_file(src, tries)
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
    conf_dir = Path(home_dir).joinpath('.backuply')
    g = GoogleDrivePlugin(conf_dir, 'Backuply', None, None)
    ul_file = Path(home_dir).joinpath('baz.txt')
    print(g.upload_file(ul_file))
