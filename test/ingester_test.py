import json

CONFIG_DIR = "../libreary/config"

from libreary.ingester import Ingester

config = json.load(open("{}/{}".format(CONFIG_DIR, "ingester_config.json")))
i = Ingester(config)
i.ingest("/Users/ben/Desktop/dropbox/helppls.txt", ["low","medium"], "Test File for Level System Design")