from helpers import changesets_for_minutely
from osm import Changeset, API, Planet

# changesets = [Changeset(**c) for c in changesets_for_minutely(456789)]

# for changeset in changesets:
#     print changeset
#     print changeset.id
#     print changeset.as_tuple()

# print API.changeset(18004384)
print API.osmchange(18004384)
# print "node"
# node = API.element("node", 123456)
# print node["osm"].keys()
# print "way"
# print API.element("way", 234567)
# print "relation"
# print API.element("relation", 123456)
# print Planet.changesets_for_minutely(123456)
# print Planet.get_current_state_from_server()
print Planet.get_current_state()
