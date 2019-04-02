from clients.custom_clickhouse import CustomClickhouse
from unittest.mock import MagicMock
from operations.indices import ClickhouseIndices
import socket
from config import TEST_PARITY_NODE

def parity(test_function):
    def wrap(*args, **kwargs):
        port_is_open = True
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            host, port = TEST_PARITY_NODE.split("//")[-1][:-1].split(":")
            result = sock.connect_ex((host, int(port)))
            port_is_open = result == 0
        except Exception as e:
            print("Can't validate parity port acceptance: ")
            print(e)
            test_function(*args, **kwargs)
        if not port_is_open:
            raise Exception("Parity port is not open")
        test_function(*args, **kwargs)

    wrap.__setattr__("__name__", test_function.__name__)
    return wrap

def mockify(object, mocks, not_mocks):
    def cat(x=None, *args, **kwargs):
        return x

    for attr in dir(object):
        if not attr.startswith('__'):
            if attr in mocks.keys():
                setattr(object, attr, mocks[attr])
            elif attr not in not_mocks:
                value = getattr(object, attr)
                if callable(value):
                    setattr(object, attr, MagicMock(side_effect=cat))


class TestClickhouse(CustomClickhouse):
    def prepare_indices(self, indices):
        for index in indices.values():
            self.send_sql_request("DROP TABLE IF EXISTS {}".format(index))
        ClickhouseIndices(indices).prepare_indices()
        self._prepare_views_as_indices(indices)

    def _prepare_views_as_indices(self, indices):
        engine = 'ENGINE = ReplacingMergeTree() ORDER BY id'
        contract_fields = """
            id String, 
            address String, 
            blockNumber Int64, 
            test UInt8, 
            standards Array(Nullable(String)), 
            standard_erc20 UInt8,
            standard_bancor_converter UInt8
        """
        if "contract" in indices:
            self.send_sql_request(
                "CREATE TABLE IF NOT EXISTS {} ({}) {}".format(indices["contract"], contract_fields, engine))

    def index(self, index, doc, id):
        doc['id'] = id
        self.bulk_index(index=index, docs=[doc])
