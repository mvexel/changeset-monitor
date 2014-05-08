import config
import requests
import os
import sys
import xml.etree.ElementTree as ET
import database

osmtypes = ["node", "way", "relation"]
actions = ["create", "modify", "delete"]


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
    minutes = int(floor(
        (current_state['last_run'] - utctime).total_seconds() / 60))
    rem = current_state['sequence'] - minutes
    return path_for_sequence(rem)


def path_for_sequence(sequence_number):
    parts = []
    if sequence_number < 0:
        sequence_number = 0
    while sequence_number > 0:
        parts.insert(0, (str(sequence_number % 1000).zfill(3)))
        sequence_number = (sequence_number - sequence_number % 1000) / 1000
    while len(parts) < 3:
        parts.insert(0, "000")
    parts.insert(0, "http://planet.osm.org/replication/changesets/")
    return '{path}.osm.gz'.format(path=os.path.join(*parts))


def changesets_for_minutely(sequence_number):
    """Read a minutely changeset file and resurn the changesets
    containted within it as an elementtree object"""
    import gzip
    chunk_size = 1024
    tempfile = os.path.join(config.TMP_DIR, 'temp.gz')
    path = path_for_sequence(sequence_number)
    changesets = []
    with open(tempfile, 'wb') as fd:
        for chunk in requests.get(path).iter_content(chunk_size):
            fd.write(chunk)
    with gzip.open(tempfile) as changeset_xml:
        for event, elem in ET.iterparse(changeset_xml):
            if event == "end" and elem.tag == "changeset":
                changesets.append(
                    get_changeset_values_as_dict(elem))
    return changesets


def backfill_changeset_database():
    """Make the changeset database catch up."""
    # get latest changeset in db
    latest_changeset_in_local_db = database.get_latest_changeset()
    # figure out which changeset minutely to fetch first
    first_changeset_to_fetch =\
        get_changeset_path_for(latest_changeset_in_local_db)
    # enqueue fetching changeset minutelies to process
    print first_changeset_to_fetch


def get_changeset_values_as_dict(elem):
    """Parse a changeset XML element from the OSM changeset
    metadata file into a tuple of values ready to insert into
    the changeset database schema"""
    from dateutil.parser import parse
    tags = {}
    changeset = {}
    for key in elem.attrib:
        if key in ['created_at', 'closed_at']:
            changeset[key] = parse(elem.attrib[key])
        else:
            changeset[key] = elem.attrib[key]
    for child in elem:
        tags[child.attrib["k"]] = child.attrib["v"]
    changeset["tags"] = tags
    return changeset


def as_tuple(changeset):
    """get an insert-ready tuple from a changeset dict"""
    # add the changeset id
    if not "id" in changeset:
        # changeset is borked
        return []
    values = [int(changeset["id"])]
    # add user id and username
    values.extend(resolve_user(changeset))
    # add dates and change count
    if "created_at" in changeset:
        values.append(changeset["created_at"])
    else:
        values.append(None)
    if "closed_at" in changeset:
        values.append(changeset["closed_at"])
    else:
        values.append(None)
    if "num_changes" in changeset:
        values.append(int(changeset["num_changes"]))
    else:
        values.append(None)
    # add bbox if present
    if "min_lon" and "max_lon" and "min_lat" and "max_lat" in changeset:
        values.extend([
            float(changeset["min_lon"]),
            float(changeset["max_lon"]),
            float(changeset["min_lat"]),
            float(changeset["max_lat"])])
    else:
        values.extend([0.0, 0.0, 0.0, 0.0])
    # add tags if present
    if "tags" in changeset:
        values.append(changeset["tags"])

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


def resolve_user(changeset):
    """given a changeset xml element, resolve the uid and user name,
    recognizing they can be empty because of early anonymous editing"""
    if not "uid" in changeset:
        return [0, "anonymous"]
    else:
        return [int(changeset["uid"]), changeset["user"]]


def handle_error(e):
    print hilite("uh oh", 0, 1)
    print hilite(e.message, 0, 0)
    sys.exit(1)


# shameless paste from http://stackoverflow.com/a/2330297
def hilite(string, status, bold):
    if sys.stdout.isatty():
        attr = []
        if status:
            # green
            attr.append('32')
        else:
            # red
            attr.append('31')
        if bold:
            attr.append('1')
        return '\x1b[%sm%s\x1b[0m' % (';'.join(attr), string)
    else:
        return string
