#!/usr/bin/env python
from argparse import ArgumentParser
from pymongo import MongoClient
from os.path import isfile, isdir, join, split
from os import listdir, stat
from hashlib import md5
from base64 import b64encode
from collections import defaultdict

"""
work_paths: _id=full_path, action
files: _id=full_path, parent, name, type, size, signature(_id, head, tail size)
"""

def drop_files(db):
    db.files.drop()
    db.work_paths.drop()

def make_work(db, path, action):
    task = db.work_paths
    try:
        task.insert({'_id':path, 'path':path, 'action':action})
    except Exception:
        print "%s %s is already in progress" % (action, path)
    else:
        print "Added %s as work" % path

def add_file(db, path):
    files = db.files
    try:
        stats = stat(path)
        size = stats.st_size
        f = open(path, 'r')
        head = f.read(4096)
        f.seek(size-4096)
        tail = f.read(4096)
        (parent, name) = split(path)
        files.remove({'_id':path})
        signature = md5()
        signature.update(name.encode('utf-8'))
        signature.update(str(size))
        signature.update(head)
        signature.update(tail)
        file = {'_id':path, 'size':size, 'parent':parent, 'name':name, 'signature':b64encode(signature.digest())}
        files.insert(file)
    except IOError:
        pass
    else:
        print "Indexed %s" % path

def find_dups(db):
    for sig in db.files.distinct("signature"):
        files = list(db.files.find({'signature':sig}))
        if len(files) > 1:
            paths = map(lambda file: file['_id'], files)
            dup = (sig, paths)
            yield dup

def work_until_done(db):
    while True:
        num_worked = 0
        for task in db.work_paths.find():
            num_worked+=1
            path = task['path']
            action = task['action']
            work(db, path, action)
            db.work_paths.remove(task)
        if num_worked == 0:
            print "Done working"
            return

def work(db, path, action):
    if action == 'rem':
        raise Exception("Not implemented")
        pass #TODO
    elif action == 'add':
        index_path(db, path)
    else:
        raise ValueError(action)

def index_path(db, path):
    if isfile(path):
        add_file(db, path)
    elif isdir(path):
        for f in listdir(path):
            new_path = join(path, f)
            make_work(db, new_path, 'add')
    else:
        print "%s is missing", path

def remove_path(db, path, action):
    raise Exception("Not done yet")
    pass

def get_path_segments(path):
    segments = [path]
    while True:
        (path, tail) = split(path)
        segments.append(path)
        if tail == '':
            break
    return segments

def process_dups(dups):
    cum_segments = defaultdict(lambda : 0)
    for (sig, paths) in dups:
        print sig
        for path in paths:
            print "\t", path
            segments = get_path_segments(path)
            for segment in segments:
                cum_segments[segment] += 1
    seg_cnt = cum_segments.items()
    cnt_seg = map(lambda x: list(reversed(x)), seg_cnt)
    sorted_segs = reversed(sorted(cnt_seg))
    for cnt,seg in sorted_segs:
        print seg,cnt

parser = ArgumentParser(description='Tool for managing file deduplication.')
parser.add_argument('--drop', action='store_true', help='clear the know files database')
parser.add_argument("--add", nargs='*', default=[])
parser.add_argument("--rem", nargs='*', default=[])

def main():
    args = parser.parse_args()
    mc = MongoClient("localhost", 27017)
    db = mc.dedup
    if args.drop:
        print "Dropping files database"
        drop_files(db)
    for path in args.rem:
        print "Will remove %s" % path
        make_work(db, path, 'rem')
    for path in args.add:
        print "Will index %s" % path
        make_work(db, path, 'add')
    print "Indexing..."
    work_until_done(db)
    print "Fixing dups"
    dups = find_dups(db)
    process_dups(dups)

    print "Done"
    
if __name__ == "__main__":
    main()

