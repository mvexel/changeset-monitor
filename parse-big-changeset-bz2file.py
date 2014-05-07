import bz2file
import xml.etree.ElementTree as ET
import argparse
import os
from dateutil.parser import parse
import simplejson as json
import datetime
from time import mktime
from sys import stdout
import psycopg2
from psycopg2.extras import register_hstore

VERBOSITY = 2000
PG_CONNECTION = "dbname=changesets user=martijnv host=localhost"
TABLENAME = "changesets"

# assumes:
#
# CREATE TABLE changesets (
#     id bigint,
#     osm_uid integer,
#     osm_user character varying,
#     created_at timestamp without time zone,
#     closed_at timestamp without time zone,
#     num_changes integer,
#     min_lon double precision,
#     max_lon double precision,
#     min_lat double precision,
#     max_lat double precision,
#     tags hstore
# );

changesets_values = []

conn = psycopg2.connect(PG_CONNECTION)
register_hstore(conn)
cursor = conn.cursor()

keys = ["id",
        "osm_uid",
        "osm_user",
        "closed_at",
        "created_at",
        "num_changes",
        "min_lon",
        "min_lat",
        "max_lon",
        "max_lat",
        "tags"]


class MyEncoder(json.JSONEncoder):

    def default(self, obj):
        if isinstance(obj, datetime.datetime):
            return int(mktime(obj.timetuple()))

        return json.JSONEncoder.default(self, obj)


def resolve_user(elem):
    if not "uid" in elem.attrib:
        return [0, "anonymous"]
    else:
        return [int(elem.attrib["uid"]), elem.attrib["user"]]


def pg_insert(values):
    base_string = "({placeholders})".format(
        placeholders=", ".join(("%s " * len(keys)).split()))
    arguments = ", ".join(cursor.mogrify(base_string, v) for v in values)
    cursor.execute("INSERT INTO {tablename} VALUES {arguments}".format(
        tablename=TABLENAME,
        arguments=arguments))
    conn.commit()


def get_changeset_values_as_tuple(elem):
    tags = {}
    # add the changeset id
    values = [int(elem.attrib["id"])]
    # add user id and username
    values.extend(resolve_user(elem))
    # add dates and change count
    values.extend([
        parse(elem.attrib["created_at"]),
        parse(elem.attrib["closed_at"]),
        int(elem.attrib["num_changes"])])
    # add bbox if present
    if "min_lon" and "max_lon" and "min_lat" and "max_lat" in elem.attrib:
        values.extend([
            float(elem.attrib["min_lon"]),
            float(elem.attrib["max_lon"]),
            float(elem.attrib["min_lat"]),
            float(elem.attrib["max_lat"])])
    else:
        values.extend([0.0, 0.0, 0.0, 0.0])
    # add tags if present
    for child in elem:
        tags[child.attrib["k"]] = child.attrib["v"]
    values.append(tags)
    return tuple(values)


def parse_xml_file(path, limit=None):
    counter = 0
    with bz2file.open(path) as changesetxml:
        for event, elem in ET.iterparse(changesetxml):
            if event == "end" and elem.tag == "changeset":
                counter += 1
                changesets_values.append(get_changeset_values_as_tuple(elem))
            if limit and counter == limit:
                break
            if counter > 0 and not counter % VERBOSITY:
                stdout.write(".")
                stdout.flush()
                pg_insert(changesets_values)
                changesets_values[:] = []


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description='Parse the big changesets file from http://planet.'
                    'openstreetmap.org/planet/changesets-latest.osm.bz2.')
    parser.add_argument('changesetfile',
                        help='The location of the changesets bz2 file')
    parser.add_argument('-l', dest='limit', type=int, help='Limit')
    args = parser.parse_args()

    stdout.write("working.")

    if os.path.exists(args.changesetfile):
        parse_xml_file(args.changesetfile, args.limit)
        # push any remaining changesets
        if len(changesets_values) > 0:
            pg_insert(changesets_values)
    else:
        print 'no such file: ', args.changesetfile
    cursor.close()
    conn.close()
    print "\ndone."
