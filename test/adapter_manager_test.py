import libreary
from libreary import Libreary
from libreary.adapters.BaseAdapter import BaseAdapter
from libreary.adapters.local import LocalAdapter
from libreary import AdapterManager


libreary.set_stream_logger()
l = Libreary("test_run_dir/config")
am = l.adapter_man

def test_initial_levels():
    num_initial_levels = len(am.metadata_man.get_levels())
    assert len(am.levels.items()) <= num_initial_levels

def test_level_add():
    # requires at least one adapter to be set up already
    am.set_additional_adapter("test_local5", "LocalAdapter")
    am.metadata_man.add_level("test_low", 1, [{"id":"test_local5", "type":"LocalAdapter"}], copies=1)
    am.reload_levels_adapters()
    assert "test_low" in am.levels.keys()
    # am.metadata_man.delete_level("test_low")

def test_add_adapters():
    am.set_additional_adapter("test_local5", "LocalAdapter")
    am.reload_levels_adapters()
    assert "test_local5" in am.adapters.keys()

def test_verify_adapters():
    for adapter in am.adapters.keys():
        assert am.verify_adapter(adapter) == True

def test_create_adapter():
    adapter = AdapterManager.create_adapter(
                    "LocalAdapter", "test_local5", "test_run_dir/config", am.config["metadata"])
    assert type(adapter) == LocalAdapter