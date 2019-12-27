

class Libreary:
    """
    Main class which will abstract interaction. Instantiating this class should set up all of the stuff
    """

    def __init__(self, config_dir):
        """
        Set up all of the necessary tooling - We need to get an:
        - metadata manager
        - set of adapters (adapterman)
        - ingester
        """
        self.adapters = []

    def run_check(deep=False):
        """
        A deep check uses actual checksums, while a shallow check trusts the metadata database
        """
        pass

    def ingest():
        pass

    def retrieve():
        """
        strategy: tell adapter manager 'i want this object'. 
        THe adapter manager will sort out which copy to retrieve
        """
        pass

    def delete():
        """
        Deletes from adapters
        Deletes from canonical
        verifies deletion
        optionally, returns a copy to the retrieval directory
        """
        pass

    def update():
        pass

    def search():
        pass

    def check_single_resource():
        pass