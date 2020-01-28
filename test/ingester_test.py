import json

from libreary.ingester import Ingester
from libreary import Libreary
import libreary

libreary.set_stream_logger()
l = Libreary("/Users/glick/desktop/libre-ary/example/config")
i = l.ingester


def test_ingester_ingest_delete():
    obj_uuid = i.ingest("/Users/glick/Desktop/grace.jpg", ["low"], "Test File for ingester tests")
    assert type(obj_uuid) == str
    
    object_list = i.list_resources()
    uuids = [j[5] for j in object_list]
    assert obj_uuid in uuids
    
    i.delete_resource(obj_uuid)

    object_list = i.list_resources()
    uuids = [j[5] for j in object_list]
    assert obj_uuid not in uuids

