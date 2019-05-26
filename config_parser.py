import json
import os

CONFIG_DIR = "config"

class ConfigParser:


    def __init__(self):
        #self.metadata_db = os.path.realpath(config['metadata'].get("db_file"))
        #self.conn = sqlite3.connect(self.metadata_db)
        #self.cursor = self.conn.cursor()
        pass

    def create_config_for_adapter(self, adapter_id, adapter_type):
        base_config = json.load(open("{}/{}_config.json".format(CONFIG_DIR, adapter_id)))
        general_config = json.load(open("{}/agent_config.json".format(CONFIG_DIR)))
        full_adapter_conf = {}
        full_adapter_conf["adapter"] = base_config["adapter"]
        full_adapter_conf["adapter"]["adapter_type"] = adapter_type
        full_adapter_conf["metadata"] = general_config["metadata"]
        full_adapter_conf["options"] = general_config["options"]

        return full_adapter_conf


    def add_new_adapter(self, adapter_config):
        pass


if __name__ == '__main__':
    parser = ConfigParser()

    print(parser.create_config_for_adapter("local1", "local"))
