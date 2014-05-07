import datetime
from dateutil.tz import tzutc

PG_CONNECTION = "dbname=changesets user=martijnv host=localhost"

OSM_DAY_ZERO = datetime.datetime(2012, 10, 28, 19, 36, tzinfo=tzutc())

TMP_DIR = '/tmp'
