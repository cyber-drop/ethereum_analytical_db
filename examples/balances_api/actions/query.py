from clickhouse_driver import Client

class Query():
    def __init__(self, table):
        self.client = Client('localhost')
        self.table = table

    def _get_addresses_string(self, addresses):
        addresses_string = ",".join(["'{}'".format(address) for address in addresses])
        return addresses_string

    def _send_sql_request(self, addresses, sql):
        addresses_string = self._get_addresses_string(addresses)
        sql = sql.format(self.table, addresses_string) 
        result = self.client.execute(sql)
        return dict(result)