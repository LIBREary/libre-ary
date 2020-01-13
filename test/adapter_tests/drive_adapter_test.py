import json

from libreary.adapter_manager import AdapterManager

CONFIG_DIR = "/Users/ben/Desktop/libre-ary/config"


config = json.load(
        open("{}/{}".format(CONFIG_DIR, "config.json")))
am = AdapterManager(config)
a = am.set_additional_adapter("drive","GoogleDriveAdapter" )
#print(a._store_canonical("/Users/ben/Desktop/grace.jpg", "34", "6b4f683d08d5431b5f8d1c8f4071610d5cab758d", "grace.jpg"))
a.delete("34")


#print(am.verify_adapter("drive"))