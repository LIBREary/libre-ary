from libreary.metadata.sqlite3 import SQLite3MetadataManager
import sqlite3
import libreary

libreary.set_stream_logger()

config = {
        "db_file": "/Users/glick/Desktop/libre-ary/test/test_run_dir/md_index.db"
        }

db_conn = sqlite3.connect("/Users/glick/Desktop/libre-ary/test/test_run_dir/md_index.db")
cursor = db_conn.cursor()

mm = SQLite3MetadataManager(config)

def test_metadata_db_initially_empty():
    assert len(mm.list_resources()) == 0
    assert len(mm.get_levels()) == 0

def test_metadata_add_level():
    a = [{"type": "LocalAdapter", "id":"local1"}]
    mm.add_level("test", "2", ",".join([str(l) for l in a]), copies=1)
    assert len(mm.get_levels()) == 1

"""
def test_metadata_delete_level():
"""

def test_metadata_add_delete_resource():
    a=["test"]
    mm.ingest_to_db("No Locator", ",".join([str(l) for l in a]), "test filename", "sha1 hash", "test-hell", "test-object")
    assert len(mm.list_resources()) == 1
    mm.delete_resource("test-hell")
    assert len(mm.list_resources()) == 0 