import json

from libreary.ingester import Ingester

config = json.load(open("{}/{}".format(CONFIG_DIR, "ingester_config.json")))
i = Ingester(config)
i.ingest("/Users/ben/Desktop/dropbox/helppls.txt", [1,])