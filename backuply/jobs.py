from abc import ABC, abstractmethod 
import datetime
import errno
import logging
import os
import re
import subprocess
from abc import ABC, abstractmethod
from logging.handlers import SysLogHandler
from .errors import InvalidBackupTarget
from .settings import Settings


class BackupJob(ABC):
    @abstractmethod
    def backup(self, *args, **kwargs): pass

    @staticmethod
    @abstractmethod
    def add_arguments(parser): pass

    @staticmethod
    def setup_logging(debug, verbose, logger_name='backuply'):
        pass

    def __init__(self, verbose=False, debug=False, *args, **kwargs):
        if kwargs.get('conf_dir') is not None:
            self.settings = Settings(kwargs['conf_dir'])
        else:
            self.settings = Settings()
        self.backup_type = self.__class__.backup_type

        self.verbose = verbose
        self.debug = debug

        self.logger = kwargs.get('logger')
        if self.logger is None:
            self.logger = logging.getLogger('backuply')
            self.logger.addHandler(SysLogHandler())

        if debug:
            self.logger.setLevel(logging.DEBUG)
        elif verbose:
            self.logger.setLevel(logging.INFO)
        elif kwargs.get('quiet', False):
            self.logger.setLevel(logging.CRITICAL)
        else:
            self.logger.setLevel(logging.WARNING)


class ShellBackupJob(BackupJob):
    @property
    @abstractmethod
    def backup_command(self): pass

    def backup(self, *args, **kwargs):
        """

        :param args:
        :param kwargs:
        :return:
        """
        now = datetime.datetime.now()
        old_backup = None
        overwrite = getattr(self, 'overwrite', False)

        if os.path.isfile(self.backup_target):
            if not overwrite:
                raise InvalidBackupTarget(errno.EEXIST,
                                          'The backup target {} already exists. Please specify a new filename, the --force switch, or delete it.'.format(
                                              self.backup_target),
                                          self.backup_target,
                                          self.backup_type)
            # If we're overwriting the old backup,
            # move it out of the way until the backup completes successfully.
            else:
                old_backup = os.path.split(self.backup_target)
                old_backup_filename = old_backup[-1].replace('.tar.gz',
                                                             '.{:%Y.%m.%d.%H.%M.%S}.tar.gz'.format(
                                                                 now))
                old_backup = os.path.join(old_backup[0], old_backup_filename)
                os.rename(self.backup_target, old_backup)

        job_retval = subprocess.check_call(self.backup_command, *args, **kwargs)
        print(f'tar command exited with code {job_retval}')
        if job_retval in (0, 2) and old_backup is not None:
            try:
                os.remove(old_backup)
            except IOError as e:
                if e.errno != errno.ENOENT:
                    raise e
                if self.verbose:
                    print(e)

        return job_retval

    def __init__(self, source, backup_target, exclude_file=None, verbose=False,
                 debug=False, backup_target_file_required=False, *args, **kwargs):
        """

        :param source:
        :param backup_target:
        :param exclude_file:
        :param verbose:
        :param debug:
        :param backup_target_file_required:
        :param args:
        :param kwargs:
        """
        super().__init__(verbose, debug, *args, **kwargs)

        if not os.path.exists(source):
            raise IOError(errno.ENOENT,
                          'The backup source {} does not exist.'.format(source))
        # Make sure source ends with a trailing slash
        self.source = source
        if not source.endswith('/'):
            self.source += '/'

        # Validate the state of the backup target
        self.backup_target = ShellBackupJob.validate_backup_target(backup_target,
                                                                   backup_target_file_required)

        if exclude_file is not None:
            if not os.path.exists(exclude_file):
                raise IOError(errno.ENOENT,
                              'The exclude file {} does not exist.'.format(
                                  exclude_file))
        self.exclude_file = exclude_file

    @staticmethod
    def validate_backup_target(backup_target, file_required=False, cur_dir=None, *args, **kwargs):
        """Recursive function that walks up a path to check if it's in /etc/fstab
            and then verifies that it's mounted and that the path exists.

        :param backup_target: (str) The full path to the backup target.
        :param file_required: (bool) Whether or not the backup target can be a file.
        :param cur_dir: (str) The current part of the path we have traversed.
        :param args:
        :param kwargs:
        :return (str) The full path to the backup target.
        """
        # Get the backup target's directory if the target is a file.
        backup_dir = None
        if file_required:
            if os.path.isdir(backup_target):
                raise InvalidBackupTarget(errno.EISDIR, 'backup_target must be a file', backup_target)
            if os.path.isfile(backup_target):
                backup_dir = os.path.split(backup_target)[0]
        elif os.path.isfile(backup_target):
            raise InvalidBackupTarget(errno.ENOTDIR,
                                      'backup_target must be a directory',
                                      backup_target)

        # On the first pass cur_dir should be the backup target or the target's directory
        if cur_dir is None:
            cur_dir = backup_target if backup_dir is None else backup_dir

        # Strip trailing slashes off the path
        cur_dir = cur_dir.rstrip('/')

        # Look for an entry in /etc/fstab matching cur_dir
        with open('/etc/fstab', 'r') as f:
            found = False
            for line in f:
                if re.search(cur_dir, line):
                    logging.info('{} was found in /etc/fstab'.format(cur_dir))
                    found = True
                    break

        # Chop the end of the path off and try again
        if not found:
            try:
                cur_dir = os.path.split(cur_dir)
                # If we've reached the point where we're checking /,
                # then it's safe enough to assume the path is not a mount point
                if cur_dir[-1] is not None and cur_dir[-1] != '':
                    cur_dir = cur_dir[0]

                    return ShellBackupJob.validate_backup_target(backup_target,
                                                                 file_required, cur_dir,
                                                                 *args, **kwargs)
            except (IndexError, InvalidBackupTarget):
                pass
        # Check that the path is mounted and exists
        else:
            if not os.path.ismount(cur_dir):
                raise InvalidBackupTarget(errno.ENXIO,
                                          '{} is not mounted.'.format(cur_dir),
                                          backup_target)

        # Now that everything else checks out, let's make sure the path actually exists.
        if not os.path.isdir(backup_target) and not file_required:
            raise InvalidBackupTarget(errno.ENOTDIR,
                                      '{} is not a directory'.format(
                                          backup_target), backup_target)
        elif backup_dir is not None and not os.path.isdir(backup_dir):
            raise InvalidBackupTarget(errno.ENOTDIR,
                                      'The directory {} for backup target {} does not exist or is not a directory.'.format(
                                          backup_dir, backup_target),
                                      backup_target)

        # Make sure the backup target ends with /
        if os.path.isdir(backup_target) and not backup_target.endswith('/'):
            backup_target += '/'

        return backup_target


class RsyncBackupJob(ShellBackupJob):
    backup_type = 'rsync'

    def __init__(self, source, backup_target, exclude_file=None, dry_run=False,
                 incremental=False, verbose=False, debug=False,
                 rsync_extra_args=None, *args, **kwargs):
        """

        :param source:
        :param backup_target:
        :param exclude_file:
        :param dry_run:
        :param incremental:
        :param verbose:
        :param debug:
        :param rsync_extra_args:
        :param args:
        :param kwargs:
        """
        super(RsyncBackupJob, self).__init__(source, backup_target,
                                             exclude_file, verbose, debug,
                                             backup_target_file_required=False,
                                             *args, **kwargs)

        self.dry_run = dry_run
        self.incremental = incremental
        self.rsync_extra_args = rsync_extra_args

    @staticmethod
    def add_arguments(parser):
        """

        :param parser:
        :return:
        """
        rsync_group = parser.add_argument_group('RSYNC BACKUPS')
        rsync_group.add_argument('--incremental', '-i', dest='incremental',
                                 action='store_true',
                                 help='Specify if this is an incremental backup (adds the -u switch to rsync).')

        return parser

    @property
    def backup_command(self):
        """

        :return:
        """
        rsync_args = [
            'rsync',
            '-a',
            '-A',
            '-X',
        ]
        if self.verbose:
            rsync_args.append('-v')
            rsync_args.append('-P')
        else:
            rsync_args.append('-q')

        if self.dry_run:
            rsync_args.append('-n')

        if self.exclude_file is not None:
            rsync_args.append('--exclude-from=%s' % self.exclude_file)

        # Incremental backups only grab updated files
        if self.incremental:
            rsync_args.append('-u')
        # Full backups clean up deleted files
        else:
            rsync_args.append('--delete-after')

        # Add any extra args passed by the caller
        try:
            rsync_args += self.rsync_extra_args
        except TypeError:
            pass

        rsync_args.append(self.source)
        rsync_args.append(self.backup_target)

        return rsync_args


class TarBackupJob(ShellBackupJob):
    backup_type = 'tar'

    def __init__(self, source, backup_target, exclude_file=None, verbose=False,
                 debug=False, tar_extra_args=None, *args, **kwargs):
        """

        :param source:
        :param backup_target:
        :param exclude_file:
        :param verbose:
        :param debug:
        :param tar_extra_args:
        :param args:
        :param kwargs:
        """
        super(TarBackupJob, self).__init__(source, backup_target, exclude_file,
                                           verbose, debug,
                                           backup_target_file_required=True,
                                           *args, **kwargs)

        self.tar_extra_args = tar_extra_args
        self.dry_run = kwargs.get('dry_run', False)
        self.overwrite = kwargs.get('overwrite', False)

        self.compress = kwargs.get('compress', True)
        self.compression_type = kwargs.get('compression_type', 'gzip')

        self.extra_excludes = kwargs.get('extra_excludes', [])

    @staticmethod
    def add_arguments(parser):
        """

        :param parser:
        :return:
        """
        tar_group = parser.add_argument_group('TAR BACKUPS')
        tar_group.add_argument('--overwrite', '-f', dest='overwrite',
                               action='store_true',
                               help='Overwrite existing backup targets.')

        return parser

    @property
    def backup_command(self):
        """

        :return:
        """
        tar_args = [
            'tar',
            '--create',
            '--acls',
            '--selinux',
            '--xattrs',
            '--exclude=%s' % self.backup_target,
        ]

        if self.verbose:
            tar_args.append('--verbose')

        for exclude in self.extra_excludes:
            tar_args.append('--exclude=%s' % exclude)

        if self.exclude_file is not None:
            tar_args.append('--exclude-from=%s' % self.exclude_file)

        if self.compress:
            tar_args.append('--%s' % self.compression_type)
        else:
            tar_args.append('--verify')

        # Add any extra args passed by the caller
        try:
            tar_args += self.tar_extra_args
        except TypeError:
            pass

        tar_args.append('--file=%s' % self.backup_target)
        tar_args.append(self.source)

        return tar_args


class InvalidBackupTarget(Exception):
    def __init__(self, message, backup_target=None, backup_type=None):
        """

        :param message:
        :param backup_target:
        :param backup_type:
        """
        super(InvalidBackupTarget, self).__init__(message)
        self.backup_type = backup_type
        self.backup_target = backup_target

    def __str__(self):
        out_str = 'Backup target %s is not valid for the type %s. The error was "%s"' % (
            self.backup_target, self.backup_type, self.args[0])

        return out_str

