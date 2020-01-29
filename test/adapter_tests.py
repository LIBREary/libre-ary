import json
import libreary
from libreary.adapter_manager import AdapterManager
from libreary import Libreary

CONFIG_DIR = "/Users/glick/Desktop/libre-ary/example/config"


CONFIG_DIR = "/Users/glick/Desktop/libre-ary/example/config"

libreary.set_stream_logger()
l = Libreary("/Users/glick/desktop/libre-ary/example/config")
am = l.adapter_man


def test_adapter_drive():
    a = am.set_additional_adapter("drive","GoogleDriveAdapter" )
    assert am.verify_adapter("drive") == True

def test_adapter_local():
    a = am.set_additional_adapter("local1", "LocalAdapter")
    assert am.verify_adapter("local1") == True

def test_adapter_s3():
    a = am.set_additional_adapter("s3", "S3Adapter")
    assert am.verify_adapter("s3") == True


