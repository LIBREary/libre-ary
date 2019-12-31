import json

from libreary.adapter_manager import AdapterManager
from libreary.config_parser import ConfigParser

CONFIG_DIR = "/Users/ben/Desktop/libre-ary/libreary/config"


config = json.load(
        open("{}/{}".format(CONFIG_DIR, "adapter_manager_config.json")))
am = AdapterManager(config)

print(am.adapters)
print(am.send_resource_to_adapters("1277ccb6-051c-458d-9250-570b6e085d79"))
print(am.retrieve_by_preference("1277ccb6-051c-458d-9250-570b6e085d79"))
am.delete_resource_from_adapters("1277ccb6-051c-458d-9250-570b6e085d79") 