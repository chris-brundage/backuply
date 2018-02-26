#!/usr/bin/env python
from __future__ import absolute_import, division
import argparse
import backuply
import errno
import os
import subprocess


def main():
    if os.geteuid() != 0:
        print 'This script must be run with root privileges.'
        return errno.EPERM
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--simulate', dest='simulate', action='store_true',
                        help='Print the backup command that would execute and then exit.')
    parser = backuply.add_arguments(parser)
    args = parser.parse_args()
    # Debug implies verbose
    if args.debug:
        args.verbose = True

    # The parsed arguments contains the keyword arguments
    backup_job_args = dict(args.__dict__)
    try:
        del backup_job_args['simulate']
    except KeyError:
        pass

    try:
        if args.backup_type == backuply.TarBackupJob.backup_type:
            if 'extra_excludes' not in backup_job_args:
                backup_job_args['extra_excludes'] = ['/home/*']

        backup_job = backuply.create_backup_job(**backup_job_args)
        if args.simulate:
            print ' '.join(backup_job.backup_command)
            return 0
        backup_job.backup()

        return 0
    except backuply.InvalidBackupTarget as e:
        print e
        return 1
    except IOError as e:
        print e.strerror
        return e.errno
    except subprocess.CalledProcessError as e:
        print e
        return e.returncode
    except KeyboardInterrupt:
        return 1


if __name__ == '__main__':
    exit(main())
