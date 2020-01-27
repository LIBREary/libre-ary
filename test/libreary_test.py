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

