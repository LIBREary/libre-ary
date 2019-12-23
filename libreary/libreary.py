

class Libreary:
    """
    Main class which will abstract interaction. Instantiating this class should set up all of the stuff
    """

    def __init__(self, config_dir):
        """
        Set up all of the necessary tooling - We need to get an:
        - agent
        - metadata manager
        - set of adapters
        - ingester
        """
        self.adapters = []

    def run_check():
        pass

    def ingest():
        pass

    def retrieve():
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

    def check_copies():
        pass
