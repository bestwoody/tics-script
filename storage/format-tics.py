#!/usr/bin/python3
import argparse
import os
import subprocess
from os import path

from git import Repo


def main():
    default_suffix = ['.cpp', '.h', '.cc', '.hpp']
    parser = argparse.ArgumentParser(description='TiFlash Code Format',
                                     formatter_class=argparse.RawTextHelpFormatter)
    parser.add_argument('--repo_path', help='path of tics repository', required=True)
    parser.add_argument('--suffix',
                        help='suffix of files to format, split by space, default: {}'.format(' '.join(default_suffix)),
                        default=' '.join(default_suffix))
    parser.add_argument('--ignore_suffix', help='ignore files with suffix, split by space')
    args = parser.parse_args()
    default_suffix = args.suffix.strip().split(' ') if args.suffix else []
    ignore_suffix = args.ignore_suffix.strip().split(' ') if args.ignore_suffix else []
    tics_repo_path = args.repo_path
    if not os.path.isabs(tics_repo_path):
        raise Exception("path of repo should be absolute")
    repo = Repo(tics_repo_path)
    files_to_check = {a.a_path for a in repo.index.diff(None)}
    files_to_check.update({a.a_path for a in repo.index.diff('HEAD')})
    files_to_check.update(repo.untracked_files)
    files_to_format = []
    for f in files_to_check:
        if not any([f.endswith(e) for e in default_suffix]):
            continue
        if any([f.endswith(e) for e in ignore_suffix]):
            continue
        file_path = '{}/{}'.format(tics_repo_path, f)
        if not path.exists(file_path):
            continue
        if ' ' in file_path:
            print('file {} can not be formatted'.format(file_path))
            continue
        files_to_format.append(file_path)

    if files_to_format:
        print('Files to format:\n  {}'.format('\n  '.join(files_to_format)))
        cmd = 'clang-format -i {}'.format(' '.join(files_to_format))
        child = subprocess.Popen(cmd, shell=True, cwd=tics_repo_path)
        child.wait()
    else:
        print('No file to format')


if __name__ == '__main__':
    main()
