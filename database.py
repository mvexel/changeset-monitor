import psycopg2
from psycopg2.extras import register_hstore
import config
from sys import stdout

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

conn = psycopg2.connect(config.PG_CONNECTION)
register_hstore(conn)
cursor = conn.cursor()


def insert_changesets(values):
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


def close():
    cursor.close()
    conn.close()
