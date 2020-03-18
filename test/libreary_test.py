from libreary.libreary import Libreary

libreary = Libreary("test_run_dir/config")

def test_config():
	assert type(libreary.config) == dict
	assert libreary.config_dir != None

def test_add_level():
	levels_dict = [
		{
		"id": "local1",
		"type":"LocalAdapter"
		},
		{
		"id": "local2",
		"type":"LocalAdapter"
		}
		]
	assert libreary.add_level("low", "1", levels_dict, copies=1) == None
	libreary.metadata_man.delete_level("low")

def test_libreary_ingest():
	obj_id = libreary.ingest("test_run_dir/dropbox/grace.jpg", ["low"],
               "cat", delete_after_store=False)
	assert type(obj_id) == str
	libreary.delete(obj_id)

def test_libreary_metadata():
	obj_id = libreary.ingest("test_run_dir/dropbox/grace.jpg", ["low"],
               "cat", delete_after_store=False)
	metadata = libreary.metadata_man.get_resource_info(obj_id)
	assert metadata[0][5] == obj_id
	assert metadata[0][4] == "6b4f683d08d5431b5f8d1c8f4071610d5cab758d"
	libreary.delete(obj_id)

def test_libreary_retrieve():
	obj_id = libreary.ingest("test_run_dir/dropbox/grace.jpg", ["low"],
               "cat", delete_after_store=False)
	new_path = libreary.retrieve(obj_id)
	assert new_path == "test_run_dir/retrieval/grace.jpg"
	libreary.delete(obj_id)

def test_libreary_delete():
    obj_id = libreary.ingest("test_run_dir/dropbox/grace.jpg", ["low"],"cat", delete_after_store=False)
    libreary.delete(obj_id) 

def test_search():
	o_id = libreary.ingest("test_run_dir/dropbox/grace.jpg", ["low"], "Test File for Level System Design", delete_after_store=False)
	search_results = libreary.search("Test File")
	assert type(search_results) == list
	assert len(search_results) >= 1
	libreary.delete(o_id)