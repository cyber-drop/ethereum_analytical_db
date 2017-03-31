import sys
import elasticsearch

es = elasticsearch.Elasticsearch()

for i in range(0, 500000):
    try:
        res = es.get(index="ethereum-block", doc_type='b', id=i)
    except elasticsearch.exceptions.NotFoundError as e:
        sys.stdout.write(i)

