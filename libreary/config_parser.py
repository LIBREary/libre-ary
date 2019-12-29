import json
import os



class ConfigParser:


    def __init__(self, config_dir):
        #self.metadata_db = os.path.realpath(config['metadata'].get("db_file"))
        #self.conn = sqlite3.connect(self.metadata_db)
        #self.cursor = self.conn.cursor()
        self.config_dir = config_dir

    def create_config_for_adapter(self, adapter_id, adapter_type):
        base_config = json.load(open("{}/{}_config.json".format(self.config_dir, adapter_id)))
        general_config = json.load(open("{}/agent_config.json".format(self.config_dir)))
        full_adapter_conf = {}
        full_adapter_conf["adapter"] = base_config["adapter"]
        full_adapter_conf["adapter"]["adapter_type"] = adapter_type
        full_adapter_conf["metadata"] = general_config["metadata"]
        full_adapter_conf["options"] = general_config["options"]

        return full_adapter_conf


    def add_new_adapter(self, adapter_config):
        pass