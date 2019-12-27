class IngestionFailedException(Exception):
	pass

class ChecksumMismatchException(Exception):
	pass

class MetadataUnavailableException(Exception):
	pass

class AdapterConnectionFailureException(Exception):
	pass