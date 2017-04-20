class Datastore:

    WEI_ETH_FACTOR = 1000000000000000000.0

    def __init__(self):
        self.actions = list()

    def extract(self, rpc_block):
        raise NotImplementedError

    def save(self):
        raise NotImplementedError

