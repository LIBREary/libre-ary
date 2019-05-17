

class LibrearyAgent:


	def __init__():
		self.metadata_db = os.path.realpath(config['metadata'].get("db_file"))
        self.conn = sqlite3.connect(self.metadata_db)
        self.cursor = self.conn.cursor()
        self.storage_dir = config["adapter"]["storage_dir"]
        self.dropbox_dir = config["options"]["dropbox_dir"]
        self.adapter_type = "local"
        self.ret_dir = config["options"]["output_dir"]


    def run_check():
    	pass


    def get_all_copies(r_id):
    	pass


    def ingest_to_db(file):
    	pass

    def create_adapter(adapter_id):
    	pass






if __name__ == '__main__':
	config = {
        "metadata": {
            "db_file": "metadata/md_index.db",
        },
        "adapters": {

        "local1": {
            "storage_dir": "~/Desktop/librelocal",
            "adapter_identifier": "local1"
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
