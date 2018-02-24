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
    """Recursive function that walks up a path to check if it's in /etc/fstab and then verifies that it's mounted.

    Also, verifies that the path exists.
    """
    if cur_dir is None:
        cur_dir = backup_volume

    # Strip trailing slashes off the path
    cur_dir = cur_dir.rstrip('/')

    # Look for an entry in /etc/fstab matching cur_dir
    with open('/etc/fstab', 'r') as f:
        found = False
        for line in f:
            if re.search(cur_dir, line):
                if kwargs.get('verbose', False):
                    print '%s was found in /etc/fstab' % cur_dir
                found = True
                break

    # Chop the end of the path off and try again
    if not found:
        try:
            cur_dir = os.path.split(cur_dir)
            # If we've reached the point where we're checking /, 
            # then it's safe enough to assume the path is not a mount point
            if cur_dir[-1] is None or cur_dir[-1] == '':
                return 0

            cur_dir = cur_dir[0] 

            return validate_backup_volume(backup_volume, cur_dir)
        except (IndexError, TypeError):
            pass
    # Check that the path is mounted and exists
    else:
        if not os.path.ismount(cur_dir):
            raise IOError(errno.ENXIO, '%s is not mounted.' % cur_dir)
    # Now that everything else checks out, let's make sure the path actually exists.
    if not os.path.isdir(backup_volume):
        raise IOError(errno.ENOTDIR, '%s was not found or is not a directory.' % backup_volume)

    return 0


def backup(backup_volume, exclude_file=None, *args, **kwargs):
    """Constructs arguments to pass to rsync based on arguments passed to the script.

    Raises a subprocess.CalledProcessError if anything goes wrong with the rysnc
    """
    # Make sure the backup destination is valid
    validate_backup_volume(backup_volume, *args, **kwargs)
    # Make sure any specified exclude files actually exist.
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

