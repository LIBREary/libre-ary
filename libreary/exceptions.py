class IngestionFailedException(Exception):
	pass

class ChecksumMismatchException(Exception):
	pass

class MetadataUnavailableException(Exception):
	pass

class AdapterConnectionFailureException(Exception):
	pass

class RestorationFailedException:
	pass

class AdapterCreationFailedException:
	pass

class StorageFailedException:
	pass

class OptionalModuleMissingException:
	pass

class ResourceNotIngestedException:
	pass