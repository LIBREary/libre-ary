class BaseAdapter:
    """
	Class definition for the base adapter for libre-ary resource adapters.

	Use this mostly as a reference for what other adapters need to implement.
	"""

    def __init__(config):
        """
		This should handle configuration (auth, etc.) and set up the
		metadata db connection
		"""
        pass

    def store(resource_id):
        """
		Given a resource id, saves resource and returns confirmation
		"""
        pass

    def retrieve(resource_id):
        """
		Given a resource id, load it from the adapter's repository

		Downloads file to local dir, returns path.
		"""
        pass

    def update(resource_id, updated):
        """
		Overwrite the remote resource specified with what's passed into :param updated.
		"""
        pass

    def delete(resource_id):
        """
		delete a resource
    	"""
        pass
