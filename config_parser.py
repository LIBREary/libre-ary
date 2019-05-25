import json

CONFIG_DIR = "config"

class ConfigParser:


	def __init__(self):
		self.metadata_db = os.path.realpath(config['metadata'].get("db_file"))
        self.conn = sqlite3.connect(self.metadata_db)
        self.cursor = self.conn.cursor()

	def create_config_for_adapter(self, adapter_id):
		base_config = json.read(open("{}/{}_config.json".format(CONFIG_DIR, adapter_id)))
		general_config = json.read(open("{}/agent_config.json".format(CONFIG_DIR)))
		full_adapter_conf = {}
		full_adapter_conf["adapter"] = base_config
		full_adapter_conf["metadata"] = general_config["metadata"]
		full_adapter_conf["options"] = general_config["options"]

		return full_adapter_conf


	def add_new_adapter(self, adapter_config):
		pass

