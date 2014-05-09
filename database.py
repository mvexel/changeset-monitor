import psycopg2
from psycopg2.extras import register_hstore
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
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

def get_connection_string(with_database=True):
    connstr = ""
    for key, val in config.PG_CONNECTION.iteritems():
        if not with_database and key == "dbname":
            continue
        connstr += " {key}={val}".format(
            key=key,
            val=val)
    return connstr


def init():
    try:
        conn = psycopg2.connect(get_connection_string(with_database=False))
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cursor = conn.cursor()
        # create the database
        print "creating database"
        cursor.execute("""CREATE DATABASE changesets""")
        conn.close()

        conn = psycopg2.connect(get_connection_string())
        cursor = conn.cursor()
        cursor.execute("""CREATE EXTENSION hstore""")
        conn.commit()
        cursor.execute("""CREATE TABLE changesets (
            id bigint PRIMARY KEY,
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
        cursor.execute("""CREATE INDEX idx_changesets_id
            ON changesets USING btree(id)""")
        cursor.execute("""CREATE INDEX idx_changesets_closed_at
            ON changesets USING btree(closed_at)""")
        cursor.execute("""CREATE INDEX idx_changesets_closed_min_lon
            ON changesets USING btree(min_lon)""")
        cursor.execute("""CREATE INDEX idx_changesets_closed_max_lon
            ON changesets USING btree(max_lon)""")
        cursor.execute("""CREATE INDEX idx_changesets_closed_min_lat
            ON changesets USING btree(min_lat)""")
        cursor.execute("""CREATE INDEX idx_changesets_closed_max_lat
            ON changesets USING btree(max_lat)""")
        conn.commit()
    except psycopg2.Error as e:
        helpers.handle_error(e)
    finally:
        conn.close()


def wipe():
    conn = psycopg2.connect(get_connection_string())
    cursor = conn.cursor()
    try:
        cursor.execute("DELETE FROM changesets")
        conn.commit()
    except psycopg2.Error as e:
        helpers.handle_error(e)
    finally:
        conn.close()


def insert_changesets(values):
    conn = psycopg2.connect(get_connection_string())
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
        return True
    except:
        stdout.write("x")
        stdout.flush()
        return False
    finally:
        cursor.close()
        conn.close()


def get_latest_changeset():
    """get the timestamp at which the last changeset in the local
    database was closed."""
    import psycopg2
    conn = psycopg2.connect(get_connection_string())
    cursor = conn.cursor()
    try:
        cursor.execute(
            "SELECT closed_at FROM changesets ORDER BY id DESC LIMIT 1")
        query_result = cursor.fetchone()
        result = query_result[0]
    except Exception:
        print 'something went wrong'
        result = None
    finally:
        conn.close()
        return result
