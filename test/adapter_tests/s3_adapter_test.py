import json

from libreary.adapter_manager import AdapterManager
from libreary.config_parser import ConfigParser

CONFIG_DIR = "/Users/ben/Desktop/libre-ary/config"


config = json.load(
        open("{}/{}".format(CONFIG_DIR, "config.json")))
am = AdapterManager(config)
am.set_additional_adapter("s3", "S3Adapter")

print(am.verify_adapter("s3"))