from libreary import Libreary
import libreary

l = Libreary("/Users/ben/Desktop/libre-ary/example/config")
levels_dict = [
{
"id": "local1",
"type":"LocalAdapter"
},
{
"id": "local2",
"type":"LocalAdapter"
}
]
l.add_level("low", "1", levels_dict, copies=1)
l.ingest("/Users/ben/Desktop/dropbox/helppls.txt", ["low"], "Test File for Level System Design")
print(l.search("Test"))