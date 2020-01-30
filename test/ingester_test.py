import json

from libreary.ingester import Ingester
from libreary import Libreary
import libreary

libreary.set_stream_logger()
l = Libreary("test_run_dir/config")
i = l.ingester


def test_ingester_ingest_delete():
    obj_uuid = i.ingest("test_run_dir/dropbox/grace.jpg", ["low"], "Test File for ingester tests")
    assert type(obj_uuid) == str
    
    object_list = i.list_resources()
    uuids = [j[5] for j in object_list]
    assert obj_uuid in uuids
    
    i.delete_resource(obj_uuid)

    object_list = i.list_resources()
    uuids = [j[5] for j in object_list]
    assert obj_uuid not in uuids

