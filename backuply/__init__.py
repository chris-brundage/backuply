
from .jobs import BackupJob, RsyncBackupJob, TarBackupJob, InvalidBackupTarget


def create_backup_job(backup_type, *args, **kwargs):
    """

    :param backup_type:
    :param args:
    :param kwargs:
    :return:
    """
    if backup_type == RsyncBackupJob.backup_type:
        return RsyncBackupJob(*args, **kwargs)
    if backup_type == TarBackupJob.backup_type:
        return TarBackupJob(*args, **kwargs)

    raise TypeError('No job defined for backup_type %s' % backup_type)


def add_arguments(parser):
    """

    :param parser:
    :return:
    """
    backup_types = [
        RsyncBackupJob.backup_type,
        TarBackupJob.backup_type,
    ]

    parser.add_argument('--backup-type', '-t', dest='backup_type',
                        choices=backup_types,
                        default=RsyncBackupJob.backup_type,
                        help='The type of backup job. Defaults to %s' % RsyncBackupJob.backup_type)
    parser.add_argument('--dry-run', '-n', dest='dry_run',
                        action='store_true',
                        help='Simulate the backup instead of performing it.')
    parser.add_argument('--exclude-file', dest='exclude_file',
                        metavar='/path/to/exclude/file',
                        help='The path to a list of files to exclude from the backup. Should follow the format of rsync\'s --exclude-from files.',
                        required=True)
    parser.add_argument('--debug', '-d', dest='debug', action='store_true',
                        help='Print debug and verbose output.')
    parser.add_argument('--verbose', '-v', dest='verbose',
                        action='store_true',
                        help='Print verbose output.')
    parser.add_argument('--source', '-s', dest='source',
                        metavar='/path/to/backup', default='/',
                        help='The source files to backup. Defaults to /')
    parser.add_argument('backup_target', metavar='/path/to/backup/target',
                        help='The path to backup file storage.')

    try:
        parser = RsyncBackupJob.add_arguments(parser)
        parser = TarBackupJob.add_arguments(parser)
    except AttributeError:
        pass

    return parser
