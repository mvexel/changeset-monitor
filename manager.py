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


def init_database(args):
    if "wipe" in args:
        print "going to wipe database..."
        raw_input("press ^C now if you don't want "
                  "this or enter to continue.\n")
        print "wiping..."
        database.wipe(args.host, args.port, args.user, args.database)
    else:
        print "going to initialize database..."
        database.init(args.host, args.port, args.user, args.database)


def load_database(args):
    print "going to load database...sit back and relax..."

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
    print "\ndone. {counter} changesets"
    " processed.".format(counter=processed)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Parse the big changesets file from http://planet."
                    "openstreetmap.org/planet/changesets-latest.osm.bz2.")
    subparsers = parser.add_subparsers(help="choose a command")

    # the initialization subcommand
    parser_init = subparsers.add_parser(
        "init",
        help="initialize the database (needs to exist)")
    parser_init.set_defaults(func=init_database)
    parser_init.add_argument(
        "wipe",
        help="wipe the database")
    parser_init.add_argument(
        "--host",
        default="localhost",
        help="database host")
    parser_init.add_argument(
        "--port",
        default=5432,
        help="database port")
    parser_init.add_argument(
        "--user",
        help="database user")
    parser_init.add_argument(
        "--database",
        default="changesets",
        help="database name")

    # the loading subcommand
    parser_load = subparsers.add_parser(
        "load",
        help="load the database with the initial changesets-latest.bz2")
    parser_load.set_defaults(func=load_database)
    parser_load.add_argument(
        "changesetfile",
        help='The location of the changesets bz2 file')
    parser_load.add_argument(
        '--limit',
        type=int,
        help='limit -- for testing')
    parser_load.add_argument(
        '-test',
        action='store_true',
        help='small sample data test')

    args = parser.parse_args()
    args.func(args)
