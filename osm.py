from dateutil.parser import parse
import requests
import os
import xmltodict
import gzip
import config
from collections import OrderedDict


class Changeset(object):

    def __init__(self, **kwargs):
        self.id = kwargs.get("id", 0)
        self.uid = kwargs.get("uid", "")
        self.user = kwargs.get("user", 0)
        self.created_at = kwargs.get("created_at", None)
        self.closed_at = kwargs.get("closed_at", None)
        self.num_changes = kwargs.get("num_changes", None)
        self.min_lat = kwargs.get("min_lat", 0.0)
        self.min_lon = kwargs.get("min_lon", 0.0)
        self.max_lat = kwargs.get("max_lat", 0.0)
        self.max_lon = kwargs.get("max_lon", 0.0)
        self.tags = kwargs.get("tags", {})

        @property
        def id(self):
            return self._id

        @id.setter
        def id(self, value):
            self._id = int(value)

        @property
        def uid(self):
            return self._uid

        @uid.setter
        def uid(self, value):
            self._uid = int(value)

        @property
        def user(self):
            return self._user

        @user.setter
        def user(self, value):
            self._user = value

        @property
        def created_at(self):
            return self._created_at

        @created_at.setter
        def created_at(self, value):
            self._created_at = parse(value)

        @property
        def closed_at(self):
            return self._closed_at

        @closed_at.setter
        def closed_at(self, value):
            self._closed_at = parse(value)

        @property
        def num_changes(self):
            return self._num_changes

        @num_changes.setter
        def num_changes(self, value):
            self._num_changes = int(value)

        @property
        def min_lat(self):
            return self._min_lat

        @min_lat.setter
        def min_lat(self, value):
            self._min_lat = float(value)

        @property
        def min_lon(self):
            return self._min_lon

        @min_lon.setter
        def min_lon(self, value):
            self._min_lon = float(value)

        @property
        def max_lat(self):
            return self._max_lat

        @max_lat.setter
        def max_lat(self, value):
            self._max_lat = float(value)

        @property
        def max_lon(self):
            return self._max_lon

        @max_lon.setter
        def max_lon(self, value):
            self._max_lon = float(value)

        @property
        def tags(self):
            return self._tags

        @tags.setter
        def tags(self, value):
            self._tags = value

    def as_tuple(self):
        """get an insert-ready tuple from a changeset dict"""
        # add the changeset id
        return (
            self.id,
            self.uid,
            self.user,
            self.created_at,
            self.closed_at,
            self.num_changes,
            self.min_lat,
            self.max_lat,
            self.min_lon,
            self.max_lon,
            self.tags)


class API(object):

    base_url = 'http://api.osm.org/api/0.6/'

    @classmethod
    def osmchange(cls, changeset_id):
        url = os.path.join(
            cls.base_url,
            'changeset',
            str(changeset_id),
            'download')
        response = requests.get(url)
        if response.status_code != 200:
            return {"error": response.status_code}
        return xmltodict.parse(response.content)

    @classmethod
    def changeset(cls, changeset_id):
        url = os.path.join(
            cls.base_url,
            'changeset',
            str(changeset_id))
        response = requests.get(url)
        if response.status_code != 200:
            return {"error": response.status_code}
        return xmltodict.parse(response.content)

    @classmethod
    def element(cls, osm_type, id):
        if osm_type not in ['node', 'way', 'relation']:
            return False
        url = os.path.join(
            cls.base_url,
            osm_type,
            str(id))
        print url
        response = requests.get(url)
        if len(response.content) == 0:
            return {"error": "no content"}
        if response.status_code != 200:
            return {"error": response.status_code}
        return xmltodict.parse(response.content)


class Planet(object):

    base_url = 'http://planet.osm.org/'

    @classmethod
    def changesets_for_minutely(cls, sequence_number):
        """Read a minutely changeset file and resurn the changesets
        containted within it as an elementtree object"""
        chunk_size = 1024
        tempfile = os.path.join(config.TMP_DIR, 'temp.gz')
        path = cls.path_for_sequence(sequence_number)
        with open(tempfile, 'wb') as fd:
            for chunk in requests.get(path).iter_content(chunk_size):
                fd.write(chunk)
        with gzip.open(tempfile) as changeset_xml:
            return xmltodict.parse(changeset_xml)

    @classmethod
    def changeset_path_for_sequence(cls, sequence_number):
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

    @classmethod
    def get_changeset_path_for(cls, utctime):
        """Given an UTC timestamp, get the path for what is likely to be
        the corresponding changeset minutely delta. This may break down when
        the minutely updates were interrupted, but it will always return a
        path to a file that has changesets for the given timestamp or before,
        guaranteed not after."""
        from math import floor
        current_state = cls.get_current_state()
        if 'last_run' not in current_state:
            return ""
        minutes = int(floor(
            (current_state['last_run'] - utctime).total_seconds() / 60))
        rem = current_state['sequence'] - minutes
        return cls.path_for_sequence(rem)

    @classmethod
    def get_current_state(cls):
        """Get the current state from the OSM server, including the current
        delta sequence number, and the last run time."""
        # I dislike PyYAML, that is why
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
        return OrderedDict(current_state)
