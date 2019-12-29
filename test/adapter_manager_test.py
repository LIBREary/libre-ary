import json

from libreary.adapter_manager import AdapterManager
from libreary.config_parser import ConfigParser

CONFIG_DIR = "/Users/ben/Desktop/libre-ary/libreary/config"


config = json.load(
        open("{}/{}".format(CONFIG_DIR, "adapter_manager_config.json")))
am = AdapterManager(config)

print(am.reload_levels_adapters())