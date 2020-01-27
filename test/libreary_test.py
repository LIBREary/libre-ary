from libreary.libreary import Libreary

libreary = Libreary("/Users/glick/desktop/libre-ary/example/config")

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

def test_libreary_ingest():
	obj_id = libreary.ingest("/Users/glick/Desktop/grace.jpg", ["low"],
               "cat", delete_after_store=False)
	assert type(obj_id) == str
	libreary.delete(obj_id)

def test_libreary_metadata():
	obj_id = libreary.ingest("/Users/glick/Desktop/grace.jpg", ["low"],
               "cat", delete_after_store=False)
	metadata = libreary.metadata_man.get_resource_info(obj_id)
	assert metadata[0][5] == obj_id
	assert metadata[0][4] == "6b4f683d08d5431b5f8d1c8f4071610d5cab758d"
	libreary.delete(obj_id)

def test_libreary_retrieve():
	obj_id = libreary.ingest("/Users/glick/Desktop/grace.jpg", ["low"],
               "cat", delete_after_store=False)
	new_path = libreary.retrieve(obj_id)
	assert new_path == "/Users/glick/Desktop/retrieval/grace.jpg"
	libreary.delete(obj_id)

def test_libreary_delete():
    obj_id = libreary.ingest("/Users/glick/Desktop/grace.jpg", ["low"],"cat", delete_after_store=False)
    libreary.delete(obj_id)

test_libreary_ingest()