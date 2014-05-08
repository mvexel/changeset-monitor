import psycopg2
from psycopg2.extras import register_hstore
import config
from sys import stdout
import helpers

TABLENAME = "changesets"

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


# assumes:
#
# CREATE TABLE changesets (
#     id bigint,
#     osm_uid integer,
#     osm_user character varying,
#     created_at timestamp with time zone,
#     closed_at timestamp with time zone,
#     num_changes integer,
#     min_lon double precision,
#     max_lon double precision,
#     min_lat double precision,
#     max_lat double precision,
#     tags hstore
# );

def init(host, port, user, database):
    conn = psycopg2.connect(
        "host={host} port={port} user={user} dbname={database}".format(
            host=host,
            port=port,
            user=user,
            database=database))
    cursor = conn.cursor()
    try:
        cursor.execute("""CREATE TABLE changesets (
        id bigint,
        osm_uid integer,
        osm_user character varying,
        created_at timestamp with time zone,
        closed_at timestamp with time zone,
        num_changes integer,
        min_lon double precision,
        max_lon double precision,
        min_lat double precision,
        max_lat double precision,
        tags hstore);""")
        conn.commit()
    except psycopg2.Error as e:
        helpers.handle_error(e)


def wipe(host, port, user, database):
    conn = psycopg2.connect(
        "host={host} port={port} user={user} dbname={database}".format(
            host=host,
            port=port,
            user=user,
            database=database))
    cursor = conn.cursor()
    try:
        cursor.execute("DELETE FROM changesets")
        conn.commit()
    except psycopg2.Error as e:
        helpers.handle_error(e)


def insert_changesets(values):
    conn = psycopg2.connect(config.PG_CONNECTION)
    register_hstore(conn)
    cursor = conn.cursor()
    try:
        base_string = "({placeholders})".format(
            placeholders=", ".join(("%s " * len(keys)).split()))
        arguments = ", ".join(cursor.mogrify(base_string, v) for v in values)
        if len(arguments) == 0:
            return False
        cursor.execute("INSERT INTO {tablename} VALUES {arguments}".format(
            tablename=TABLENAME,
            arguments=arguments))
        conn.commit()
        stdout.write(".")
        stdout.flush()
    except psycopg2.Error as e:
        stdout.write("x")
        stdout.flush()
        return False
    return True
    cursor.close()
    conn.close()
