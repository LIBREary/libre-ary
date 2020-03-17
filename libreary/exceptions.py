class IngestionFailedException(Exception):
    pass


class ChecksumMismatchException(Exception):
    pass


class MetadataUnavailableException(Exception):
    pass


class AdapterConnectionFailureException(Exception):
    pass


class RestorationFailedException(Exception):
    pass


class AdapterCreationFailedException(Exception):
    pass


class StorageFailedException(Exception):
    pass


class OptionalModuleMissingException(Exception):
    pass


class ResourceNotIngestedException(Exception):
    pass


class NoCopyExistsException(Exception):
    pass


class AdapterRestored(Exception):
    pass


class ConfigurationError(Exception):
    pass


class NoSuchMetadataFieldExeption(Exception):
    pass
