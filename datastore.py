class Datastore:

    WEI_ETH_FACTOR = 1000000000000000000.0

    def __init__(self):
        self.actions = list()

    @classmethod
    def config(cls, es_url, es_maxsize):
        raise NotImplementedError

    def extract(self, rpc_block):
        raise NotImplementedError

    def save(self):
        raise NotImplementedError

