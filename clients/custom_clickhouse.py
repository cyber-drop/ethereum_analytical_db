from clickhouse_driver import Client
from utils import split_on_chunks
from config import NUMBER_OF_JOBS, MAX_CHUNK_SIZE
from clients.custom_client import CustomClient
from tqdm import tqdm
import json
from config import MAX_MEMORY_USAGE
import sys

class CustomClickhouse(CustomClient):
    def _create_client(self):
        """
        Create clickhouse connection and set initial parameters (for example, max memory usage)

        Returns
        -------
        client : clickhouse_driver.Client
            Initialized connection to a clickhouse
        """
        # TODO wait for clickhouse port
        client = Client('localhost', send_receive_timeout=10000)
        client.execute("SET max_memory_usage = {}".format(MAX_MEMORY_USAGE))
        return client

    def __init__(self):
        self.client = self._create_client()

    def __del__(self):
        self.client.disconnect()

    def _create_sql_query(self, index, query, fields, final=True):
        fields_string = ",".join(fields)
        sql = 'SELECT {} FROM {}'.format(fields_string, index)
        if final:
            sql += ' FINAL'
        if query:
            sql += ' ' + query
        return sql

    def _convert_values_to_dict(self, values, fields):
        converted_fields = [field.split(" AS ")[-1] for field in fields]
        documents = [{"_source": dict(zip(converted_fields, value))} for value in values]
        for document in documents:
            if "id" in document["_source"]:
                document["_id"] = document["_source"]["id"]
                del document["_source"]["id"]
        return documents

    def search(self, index, fields, query=None, **kwargs):
        """
        Search records in a given table
        Each record will be represented only with given fields

        Parameters
        -------
        index : str
            Name of table
        fields : list
            List with field names
        query : str
            Last part of query

        Returns
        -------
        list
            List of records returned by given conditions
        """
        fields += ["id"]
        sql = self._create_sql_query(index, query, fields)
        values = self.client.execute(sql)
        return self._convert_values_to_dict(values, fields)

    def count(self, index, query=None, final=True, **kwargs):
        """
        Count records in a given table

        Parameters
        -------
        index : str
            Name of table
        query : str
            Last part of query
        final : bool
            To skip or not to skip repeating records in tables with updated records

        Returns
        -------
        int
            Number of records in database
        """
        sql = self._create_sql_query(index, query, ["COUNT(*)"], final)
        return self.client.execute(sql)[0][0]

    def iterate(self, index, fields, query=None, per=NUMBER_OF_JOBS, return_id=True, final=True):
        """
        Iterate over records in a table

        Parameters
        -------
        index : str
            Name of table
        fields : list
            List with field names
        query : str
            Last part of query
        per : int
            Size of page
        return_id : bool
            To return id in _id field of document
        final : bool
            To skip or not to skip repeating records in tables with updated records

        Returns
        -------
        int
            Number of records in database
        """
        iterate_client = self._create_client()
        if return_id:
            fields += ["id"]
        settings = {'max_block_size': per}
        sql = self._create_sql_query(index, query, fields, final)
        generator = iterate_client.execute_iter(sql, settings=settings)
        count = self.count(index, query, final)
        progress_bar = tqdm(total=count)
        for chunk in split_on_chunks(generator, per):
            progress_bar.update(per)
            yield self._convert_values_to_dict(chunk, fields)

    def _prepare_fields(self, docs, fields):
        for document in docs:
            for field in fields:
                if field not in document:
                    document[field] = None
                elif type(document[field]) == dict:
                    document[field] = json.dumps(document[field])

    def _set_id(self, docs, id_field):
        for document in docs:
            id = str(document[id_field])
            del document[id_field]
            document["id"] = id

    def _filter_schema(self, docs, index):
        fields = self.client.execute("DESCRIBE TABLE {}".format(index))
        whitelist = [field[0] for field in fields]
        for document in docs:
            blacklisted_keys = set([key for key in document if key not in whitelist])
            for key in blacklisted_keys:
                del document[key]

    def _split_records(self, records, max_bytes=MAX_CHUNK_SIZE):
        buffer = []
        current_bytes = 0
        for record in records:
            record_bytes = sys.getsizeof(record)
            if current_bytes + record_bytes > max_bytes:
                if current_bytes >= 2 * max_bytes:
                    print("The size of chunk is much bigger then the limit")
                yield buffer
                buffer = []
                current_bytes = 0
            current_bytes += record_bytes
            buffer.append(record)
        yield buffer

    def bulk_index(self, index, docs, id_field="id", **kwargs):
        """
        Add given records to a table within one query

        Parameters
        -------
        index : str
            Name of table
        docs : list
            List of records
        id_field : str
            Name of field with record id
        """
        self._set_id(docs, id_field)
        self._filter_schema(docs, index)
        fields = list(set([field for doc in docs for field in doc.keys()]))
        self._prepare_fields(docs, fields)
        fields_string = ",".join(fields)
        for chunk in self._split_records(docs):
            self.client.execute(
                'INSERT INTO {} ({}) VALUES'.format(index, fields_string),
                chunk
            )

    def send_sql_request(self, sql):
        """
        Send sql query and return result as scalar table

        Parameters
        -------
        sql : str
            Query to send

        Returns
        -------
        Content of the first cell of returned table
        """
        result = self.client.execute(sql)
        if result:
            return result[0][0]
