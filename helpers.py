import config
import requests
import os
import xml.etree.ElementTree as ET

osmtypes = ["node", "way", "relation"]
actions = ["create", "modify", "delete"]


def get_latest_local_changeset():
    """get the timestamp at which the last changeset in the local
    database was closed."""
    import psycopg2
    connection = psycopg2.connect(config.PG_CONNECTION)
    cursor = connection.cursor()
    try:
        cursor.execute(
            "SELECT closed_at FROM changesets ORDER BY id DESC LIMIT 1")
        query_result = cursor.fetchone()
        result = query_result[0]
    except Exception:
        print 'something went wrong'
        result = None
    finally:
        return result


def get_current_state_from_server():
    """Get the current state from the OSM server, including the current
    delta sequence number, and the last run time."""
    # I dislike PyYAML, that is why
    from dateutil.parser import parse
    current_state_file_url =\
        "http://planet.openstreetmap.org/replication/changesets/state.yaml"
    state_file = requests.get(current_state_file_url).text
    current_state = {}
    for line in state_file.split('\n'):
        elems = line.split(':')
        if len(elems) > 1:
            current_state[elems[0].strip()] = ":".join(elems[1:]).strip()
    if not 'last_run' and 'sequence' in current_state:
        return {}
    current_state['sequence'] = int(current_state['sequence'])
    current_state['last_run'] = parse(current_state['last_run'])
    return current_state


def get_changeset_path_for(utctime):
    """Given an UTC timestamp, get the path for what is likely to be
    the corresponding changeset minutely delta. This may break down when
    the minutely updates were interrupted, but it will always return a
    path to a file that has changesets for the given timestamp or before,
    guaranteed not after."""
    from math import floor
    current_state = get_current_state_from_server()
    if 'last_run' not in current_state:
        return ""
    parts = []
    minutes = int(floor(
        (current_state['last_run'] - utctime).total_seconds() / 60))
    rem = current_state['sequence'] - minutes
    if rem < 0:
        rem = 0
    while rem > 0:
        parts.insert(0, (str(rem % 1000).zfill(3)))
        rem = (rem - rem % 1000) / 1000
    while len(parts) < 3:
        parts.insert(0, "000")
    parts.insert(0, "http://planet.osm.org/replication/changesets/")
    print parts
    return '{path}.osm.gz'.format(path=os.path.join(*parts))


def changesets_for_minutely(path):
    """Read a minutely changeset file and resurn the changesets
    containted within it as an elementtree object"""
    import gzip
    chunk_size = 1024
    tempfile = os.path.join(config.TMP_DIR, 'temp.gz')
    changesets = []
    with open(tempfile, 'wb') as fd:
        for chunk in requests.get(path).iter_content(chunk_size):
            fd.write(chunk)
    with gzip.open(tempfile) as changeset_xml:
        for event, elem in ET.iterparse(changeset_xml):
            if event == "end" and elem.tag == "changeset":
                changesets.append(
                    get_changeset_values_as_tuple(elem))
    return changesets


def backfill_changeset_database():
    """Make the changeset database catch up."""
    # get latest changeset in db
    latest_changeset_in_local_db = get_latest_local_changeset()
    # figure out which changeset minutely to fetch first
    first_changeset_to_fetch =\
        get_changeset_path_for(latest_changeset_in_local_db)
    # enqueue fetching changeset minutelies to process
    print first_changeset_to_fetch


def get_changeset_values_as_tuple(elem):
    """Parse a changeset XML element from the OSM changeset
    metadata file into a tuple of values ready to insert into
    the changeset database schema"""
    from dateutil.parser import parse
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


def analyze_changeset(elementtree):
    """get created, modified, deleted nodes, ways, relations
    for a changeset xml object"""
    result = {}
    for action in actions:
        action_breakdown = {}
        # reorganize the xml
        mock_xml = ET.Element(action)
        for elem in elementtree.findall(action):
            mock_xml.extend(list(elem))
        for osmtype in osmtypes:
            action_breakdown[osmtype] = len(mock_xml.findall(osmtype))
        result[action] = action_breakdown
    return result


def get_changeset_details_from_osm(changeset_id):
    """gets the full changeset from the OSM API and returns it as a dict"""
    url = os.path.join(
        config.OSM_API_BASE_URL,
        'changeset',
        changeset_id,
        'download')
    response = requests.get(url)
    return analyze_changeset(ET.fromstring(response.content))


def resolve_user(elem):
    """given a changeset xml element, resolve the uid and user name,
    recognizing they can be empty because of early anonymous editing"""
    if not "uid" in elem.attrib:
        return [0, "anonymous"]
    else:
        return [int(elem.attrib["uid"]), elem.attrib["user"]]
