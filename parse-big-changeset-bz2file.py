import bz2file
import xml.etree.ElementTree as ET
import argparse
import os
from sys import stdout, exit
import helpers
import database

VERBOSITY = 1000

changesets_values = []


def parse_xml_file(path, limit=None):
    counter = 0
    with bz2file.open(path) as changesetxml:
        context = ET.iterparse(changesetxml, events=('start', 'end'))
        context = iter(context)
        event, root = context.next()
        for event, elem in context:
            if event == "end" and elem.tag == "changeset":
                counter += 1
                changesets_values.append(
                    helpers.get_changeset_values_as_tuple(elem))
                root.clear()
            if limit and counter == limit:
                break
            if counter > 0 and not counter % VERBOSITY:
                database.insert_changesets(changesets_values)
                changesets_values[:] = []
    return counter


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description='Parse the big changesets file from http://planet.'
                    'openstreetmap.org/planet/changesets-latest.osm.bz2.')
    parser.add_argument('changesetfile',
                        help='The location of the changesets bz2 file')
    parser.add_argument('-l', dest='limit', type=int, help='Limit')
    parser.add_argument('-t',
                        dest='test',
                        action='store_true',
                        help='shoot a test')
    args = parser.parse_args()

    if args.test:
        print 'testing'
        import testdata
        database.insert_changesets(testdata.sample1)
        exit(0)

    stdout.write("working.")

    if os.path.exists(args.changesetfile):
        processed = parse_xml_file(args.changesetfile, args.limit)
        # push any remaining changesets
        if len(changesets_values) > 0:
            database.insert_changesets(changesets_values)
    else:
        print 'no such file: ', args.changesetfile
    print "\ndone. {counter} changesets processed.".format(counter=processed)
