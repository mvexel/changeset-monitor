import bz2file
import xml.etree.ElementTree as ET
import argparse
import os
from dateutil.parser import parse
import simplejson as json
import datetime
from time import mktime
from sys import stdout

VERBOSITY = 1000
PG_CONNECTION = "dbname=test user=postgres"

uids = set()
users = {}


class MyEncoder(json.JSONEncoder):

    def default(self, obj):
        if isinstance(obj, datetime.datetime):
            return int(mktime(obj.timetuple()))

        return json.JSONEncoder.default(self, obj)


def resolve_uid(elem):
    if not "uid" in elem.attrib:
        return "anonymous"
    else:
        return elem.attrib["uid"]


def upsert_user(elem):
    uid = resolve_uid(elem)
    closed = parse(elem.attrib["closed_at"])
    if uid in users:
        if closed < users[uid]["first_edit"]:
            users[uid]["first_edit"] = closed
        if closed > users[uid]["last_edit"]:
            users[uid]["last_edit"] = closed
        users[uid]["changeset_count"] += 1
    else:
        users[uid] = {"changeset_count": 1,
                      "first_edit": closed,
                      "last_edit": closed}


def parse_xml_file(path, limit=None):
    counter = 0
    with bz2file.open(args.changesetfile) as changesetxml:
        for event, elem in ET.iterparse(changesetxml):
            if event == "end" and elem.tag == "changeset":
                counter += 1
                upsert_user(elem)
            if limit and counter == limit:
                break
            if not counter % VERBOSITY:
                stdout.write(".")
                stdout.flush()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description='Parse the big changesets file from http://planet.'
                    'openstreetmap.org/planet/changesets-latest.osm.bz2.')
    parser.add_argument('changesetfile',
                        help='The location of the changesets bz2 file')
    parser.add_argument('output_json',
                        help='The location of the output JSON file')
    parser.add_argument('-l', dest='limit', type=int, help='Limit')
    args = parser.parse_args()

    stdout.write("working.")

    if os.path.exists(args.changesetfile):
        parse_xml_file(args.changesetfile, args.limit)
    else:
        print 'no such file: ', args.changesetfile

    with open(args.output_json, 'w') as outfile:
        outfile.write(json.dumps(users,
                                 cls=MyEncoder,
                                 sort_keys=True,
                                 indent=4,
                                 separators=(',', ': ')))

    print "done."
