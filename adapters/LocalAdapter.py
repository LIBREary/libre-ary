from BaseAdapter import BaseAdapter

class LocalAdapter(BaseAdapter):
	"""docstring for LocalAdapter
		
		LocalAdapter is a basic adapter which saves files 
		to a local directory specified in the adapter's config

		Later in this project's plan, the LocalAdapter will be used
		for ingesting the master copies as well as as a (probably)
		commonly used adapter.

		It's also very nice to use for testing, as saving files is easy (ish)
		to debug and doesn't cost any money (unlike a cloud service)
	"""

	def __init__(self, config):
		pass