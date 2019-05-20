from adapters import *
from ingester import Ingester

adapter_translate = {
	"local": LocalAdapter.LocalAdapter
}

class LibrearyAgent:


	def __init__(self, config):
		self.metadata_db = os.path.realpath(config['metadata'].get("db_file"))
        self.conn = sqlite3.connect(self.metadata_db)
        self.cursor = self.conn.cursor()
        self.storage_dir = config["adapter"]["storage_dir"]
        self.dropbox_dir = config["options"]["dropbox_dir"]
        self.adapter_type = "local"
        self.ret_dir = config["options"]["output_dir"]
        self.ingester = Ingester(config=self.config)


    def run_check(self):
    	resources = self.ingester.get_all_copies()
        for resource in resources:
            check_single_resource(resource[0])

    def check_single_resource(self, r_id):
        pass

    def get_all_copies(self, r_id):
    	copies = self.cursor.execute("select * from copies where resource_id=?", (r_id))
        return copies.fetchall()


    def ingest_to_db(self, file):
    	pass

    def create_adapter(self, adapter_type, adapter_id):
    	adapter = adapter_translate[adapter_type](config=self.config["adapters"][adapter_id])
        return adapter





if __name__ == '__main__':
	config = {
        "metadata": {
            "db_file": "metadata/md_index.db",
        },
        "adapters": {

        "local1": {
            "storage_dir": "~/Desktop/librelocal",
            "adapter_identifier": "local1",
            "adapter_type": "local"
        },
        "local2": {
        	"storage_dir": "~/Desktop/librelocal2",
            "adapter_identifier": "local2"

        }

        },
        "options": {
            "dropbox_dir": "dropbox",
            "output_dir": "retrieval"
        }
    }

    agent = LibrearyAgent()
