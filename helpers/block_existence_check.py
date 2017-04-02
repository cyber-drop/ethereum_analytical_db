import sys
import elasticsearch

ELASTICSEARCH = elasticsearch.Elasticsearch()

for i in range(0, 500000):
    try:
        res = ELASTICSEARCH.get(index="ethereum-block", doc_type='b', id=i)
    except elasticsearch.exceptions.NotFoundError:
        sys.stdout.write(i)
