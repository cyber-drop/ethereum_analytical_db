from clickhouse_driver import Client
from utils import split_on_chunks
from config import NUMBER_OF_JOBS
from clients.custom_client import CustomClient
from tqdm import tqdm
import json


class CustomClickhouse(CustomClient):
    def _create_client(self):
        return Client('localhost', send_receive_timeout=10000)

    def __init__(self):
        self.client = self._create_client()

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
        fields += ["id"]
        sql = self._create_sql_query(index, query, fields)
        values = self.client.execute(sql)
        return self._convert_values_to_dict(values, fields)

    def count(self, index, query=None, final=True, **kwargs):
        sql = self._create_sql_query(index, query, ["COUNT(*)"], final)
        return self.client.execute(sql)[0][0]

    def iterate(self, index, fields, query=None, per=NUMBER_OF_JOBS, return_id=True, final=True):
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

    def bulk_index(self, index, docs, id_field="id", **kwargs):
        self._set_id(docs, id_field)
        self._filter_schema(docs, index)
        fields = list(set([field for doc in docs for field in doc.keys()]))
        self._prepare_fields(docs, fields)
        fields_string = ",".join(fields)
        self.client.execute(
            'INSERT INTO {} ({}) VALUES'.format(index, fields_string),
            docs
        )

    def send_sql_request(self, sql):
        result = self.client.execute(sql)
        if result:
            return result[0][0]
