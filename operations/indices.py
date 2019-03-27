from config import INDICES
from clients.custom_clickhouse import CustomClickhouse
from schema.schema import SCHEMA

INDEX_FIELDS = SCHEMA

PRIMARY_KEYS = {
    "block_flag": ["id", "name"],
    "contract_block": ["id", "name"]
}


class ClickhouseIndices:
    def __init__(self, indices=INDICES):
        self.client = CustomClickhouse()
        self.indices = indices

    def _create_index(self, index, fields={}, primary_key=["id"]):
        """
        Create specified index in database with specified field types and primary key

        Parameters
        ----------
        index : str
            Name of index
        fields : dict
            Fields and their types and index
        primary_key : list
            All possible primary keys in index
        """
        fields["id"] = "String"
        fields_string = ", ".join(["{} {}".format(name, type) for name, type in fields.items()])
        primary_key_string = ",".join(primary_key)
        create_sql = """
            CREATE TABLE IF NOT EXISTS {} ({}) ENGINE = ReplacingMergeTree() ORDER BY ({})
        """.format(index, fields_string, primary_key_string)
        self.client.send_sql_request(create_sql)

    def prepare_indices(self):
        """
        Create all indices specified in schema/schema.py

        This function is an entry point for prepare-indices operation
        """
        for key, index in self.indices.items():
            if key in INDEX_FIELDS:
                self._create_index(index, INDEX_FIELDS[key], PRIMARY_KEYS.get(key, ["id"]))
