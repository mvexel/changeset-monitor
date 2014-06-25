import argparse
import os
from sys import stdout, exit
from changesetstore import ChangesetStore
from helpers import handle_error

def wipe_database(args):
    print "going to wipe database..."
    raw_input("press ^C now if you don't want "
              "this or enter to continue.\n")
    print "wiping..."
    ChangesetStore.wipe_database()


def init_database(args):
    print "going to initialize database..."
    ChangesetStore.initialize_postgres()


def load_database(args):
    print "going to load database...sit back and relax..."

    if args.test:
        print 'testing'
        import testdata
        ChangesetStore.insert_changesets(testdata.sample1)
        exit(0)

    stdout.write("working.")

    try:
        processed = ChangesetStore.parse_xml_file(
            args.changesetfile, args.limit)
    except IOError as e:
        handle_error(e)
    print "\ndone. {counter} changesets processed.".format(counter=processed)


def make_database_catch_up():
    pass

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Various database management commands to manually"
                    "initialize and control the changesets application")
    subparsers = parser.add_subparsers(
        help="choose between database initialization and loading.")

    # the initialization subcommand
    parser_init = subparsers.add_parser(
        "init",
        help="This will create your database and initialize the "
        "changesets table. Please make sure you have PostgreSQL "
        "running and accepting connections from you on the host "
        "and port specified in config.py")
    parser_init.set_defaults(func=init_database)

    # the wipe subcommand
    parser_wipe = subparsers.add_parser(
        "wipe",
        help="This command wipes your changesets table clean.")
    parser_wipe.set_defaults(func=wipe_database)

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
