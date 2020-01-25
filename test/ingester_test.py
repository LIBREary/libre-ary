import json

from libreary.ingester import Ingester
from libreary import Libreary

l = Libreary("/Users/ben/desktop/libre-ary/config")
i = l.ingester
print(i.metadata_man)
# i.ingest("/Users/ben/Desktop/dropbox/helppls.txt", ["low","medium"], "Test File for Level System Design")