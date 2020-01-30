from libreary.metadata.sqlite3 import SQLite3MetadataManager
import sqlite3
import libreary
import json

libreary.set_stream_logger()

config = {
        "db_file": "test_run_dir/md_index.db"
        }

db_conn = sqlite3.connect("test_run_dir/md_index.db")
cursor = db_conn.cursor()

mm = SQLite3MetadataManager(config)

def test_metadata_db_initially_empty():
    assert len(mm.list_resources()) == 0
    init_levels = len(mm.get_levels())
    assert len(mm.get_levels()) == init_levels

def test_metadata_add_level():
    a = [{"type": "LocalAdapter", "id":"local1"}]
    init_levels = len(mm.get_levels())
    mm.add_level("test", "2", a, copies=1)
    assert len(mm.get_levels()) == init_levels + 1

"""
def test_metadata_delete_level():
"""

def test_metadata_add_delete_resource():
    a=["test"]
    init_resources = len(mm.list_resources())
    mm.ingest_to_db("No Locator", ",".join([str(l) for l in a]), "test filename", "sha1 hash", "test-hell", "test-object")
    assert len(mm.list_resources()) == init_resources + 1
    mm.delete_resource("test-hell")
    assert len(mm.list_resources()) == init_resources