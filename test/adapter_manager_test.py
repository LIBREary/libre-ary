import json

from libreary.adapter_manager import AdapterManager
from libreary.config_parser import ConfigParser

CONFIG_DIR = "/Users/ben/Desktop/libre-ary/libreary/config"


config = json.load(
        open("{}/{}".format(CONFIG_DIR, "adapter_manager_config.json")))
am = AdapterManager(config)

print(am.adapters)
print(am.send_resource_to_adapters("b68c6bdb-8355-4ae9-bdba-078f87727ee8"))