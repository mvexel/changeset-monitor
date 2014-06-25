import psycopg2
from psycopg2.extras import register_hstore
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import config
from sys import stdout
import helpers


class ChangesetStore:

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

    @classmethod
    def initialize_postgres(cls):
        conn = None
        try:
            conn = psycopg2.connect(helpers.get_connection_string(
                with_database=False))
            conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
            cursor = conn.cursor()
            # create the database
            print "creating database"
            cursor.execute(("""CREATE DATABASE {database}""").format(
                database=config.PG_CONNECTION['dbname']))
            conn.close()

            conn = psycopg2.connect(helpers.get_connection_string())
            cursor = conn.cursor()
            cursor.execute("""CREATE EXTENSION hstore""")
            conn.commit()
            cursor.execute("""CREATE TABLE {table} (
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
                tags hstore);""".format(
                table=config.TABLENAME))
            cursor.execute(("""CREATE INDEX idx_changesets_id
                ON {table} USING btree(id)""").format(
                table=config.TABLENAME))
            cursor.execute(("""CREATE INDEX idx_changesets_closed_at
                ON {table} USING btree(closed_at)""").format(
                table=config.TABLENAME))
            cursor.execute(("""CREATE INDEX idx_changesets_closed_min_lon
                ON {table} USING btree(min_lon)""").format(
                table=config.TABLENAME))
            cursor.execute(("""CREATE INDEX idx_changesets_closed_max_lon
                ON {table} USING btree(max_lon)""").format(
                table=config.TABLENAME))
            cursor.execute(("""CREATE INDEX idx_changesets_closed_min_lat
                ON {table} USING btree(min_lat)""").format(
                table=config.TABLENAME))
            cursor.execute(("""CREATE INDEX idx_changesets_closed_max_lat
                ON {table} USING btree(max_lat)""").format(
                table=config.TABLENAME))
            conn.commit()
        except psycopg2.Error as e:
            helpers.handle_error(e)
        finally:
            if conn is not None:
                conn.close()

    @classmethod
    def wipe_database(cls):
        conn = psycopg2.connect(helpers.get_connection_string())
        cursor = conn.cursor()
        try:
            cursor.execute("DELETE FROM {table}".format(
                table=config.TABLENAME))
            conn.commit()
        except psycopg2.Error as e:
            helpers.handle_error(e)
        finally:
            conn.close()

    @classmethod
    def insert_changesets(cls, values):
        conn = psycopg2.connect(helpers.get_connection_string())
        register_hstore(conn)
        cursor = conn.cursor()
        try:
            base_string = "({placeholders})".format(
                placeholders=", ".join(("%s " * len(cls.keys)).split()))
            print base_string
            arguments = ", ".join(
                cursor.mogrify(base_string, v) for v in values)
            if len(arguments) == 0:
                return False
            cursor.execute("INSERT INTO {tablename} VALUES {arguments}".format(
                tablename=cls.TABLENAME,
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

    @classmethod
    def get_latest_changeset(cls):
        """get the timestamp at which the last changeset in the local
        database was closed."""
        import psycopg2
        conn = psycopg2.connect(helpers.get_connection_string())
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

    @classmethod
    def parse_xml_file(cls, path, limit=None):
        import bz2file
        import xml.etree.ElementTree as ET
        changesets_values = []
        counter = 0
        with bz2file.open(path) as changesetxml:
            context = ET.iterparse(changesetxml, events=('start', 'end'))
            context = iter(context)
            event, root = context.next()
            for event, elem in context:
                if event == "end" and elem.tag == "changeset":
                    counter += 1
                    changesets_values.append(
                        elem.attrib.values())
                    root.clear()
                if limit and counter == limit:
                    break
                if counter > 0 and not counter % config.VERBOSITY:
                    ChangesetStore.insert_changesets(changesets_values)
                    changesets_values[:] = []
            # push any remaining changesets
            if len(changesets_values) > 0:
                ChangesetStore.insert_changesets(
                    changesets_values)

        return counter
