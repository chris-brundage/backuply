#!/usr/bin/env python
"""Simple rsync backup of a filesystem to a backup disk.
"""
from __future__ import absolute_import, division
import argparse
import errno
import os
import re
import subprocess


def validate_backup_volume(backup_volume, cur_dir=None, *args, **kwargs):
    if cur_dir is None:
        cur_dir = backup_volume

    slash_regex = r'(.*)(/[ \t]?$)'
    regex_check = re.sub(slash_regex, r'\1/?', cur_dir, re.IGNORECASE)

    with open('/etc/fstab', 'r') as f:
        found = False
        for line in f:
            if re.search(regex_check, line):
                if kwargs.get('verbose', False):
                    print '%s was found in /etc/fstab' % cur_dir
                found = True
                break

    if not found:
        try:
            cur_dir = cur_dir.split('/')
            del cur_dir[-1]
            if len(cur_dir) <= 1:
                return 0

            cur_dir = '/'.join(cur_dir)

            return validate_backup_volume(backup_volume, cur_dir)
        except (IndexError, TypeError):
            pass
    else:
        if not os.path.ismount(cur_dir):
            raise IOError(errno.ENXIO, '%s is not mounted.' % cur_dir)
        elif not os.path.isdir(backup_volume):
            raise IOError(errno.ENOTDIR, '%s was not found or is not a directory.' % backup_volume)

    return 0


def backup(backup_volume, exclude_file=None, *args, **kwargs):
    validate_backup_volume(backup_volume, *args, **kwargs)
    if exclude_file is not None:
        if not os.path.exists(exclude_file):
            raise IOError(errno.ENOENT, 'The exclude file %s was not found.' % exclude_file)

    rsync_args = [
        'rsync',
        '-a',
        '-H',
        '-A',
        '-X',
        '--safe-links',
        '--delete-after',
    ]
    if kwargs.get('verbose', False):
        rsync_args.append('-v')
        rsync_args.append('-P')
    else:
        rsync_args.append('-q')

    if kwargs.get('dry_run', False):
        rsync_args.append('-n')

    if exclude_file is not None:
        rsync_args.append('--exclude-from=%s' % exclude_file)

    if kwargs.get('incremental', False):
        rsync_args.append('-u')

    rsync_args.append('/')
    rsync_args.append(backup_volume)

    return subprocess.check_call(rsync_args)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--dry-run', '-n', dest='dry_run', action='store_true', help='Simulate the backup instead of performing it.')
    parser.add_argument('--exclude-file', dest='exclude_file', metavar='/path/to/exclude/file', help='The path to a list of files to exclude from the backup. Should follow the format of rsync\'s --exclude-from files.')
    parser.add_argument('--incremental', '-i', dest='incremental', action='store_true', help='Specify if this is an incremental backup (adds the -u switch to rsync).')
    parser.add_argument('--verbose', '-v', dest='verbose', action='store_true', help='Print verbose output.')
    parser.add_argument('backup_volume', metavar='/path/to/backup/volume', help='The path to the backup volume.')
    args = parser.parse_args()

    try:
        return backup(**args.__dict__)
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

