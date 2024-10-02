import pandas as pd
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk, BulkIndexError


csv_file = './Employee Sample Data 1.csv'  


employee_data = pd.read_csv(csv_file, encoding='ISO-8859-1')



employee_data = employee_data.fillna(0)
employee_data_json = employee_data.to_dict(orient='records')


es = Elasticsearch(
    [{'host': 'localhost', 'port': 9200, 'scheme': 'http'}]
)


index_name = 'employees'

if not es.indices.exists(index=index_name):
    es.indices.create(index=index_name)


def create_employee_doc(data):
    for record in data:
        yield {
            "_index": index_name,
            "_source": record
        }


try:
    success, failed = bulk(es, create_employee_doc(employee_data_json))
    print(f"Indexed {success} documents successfully.")
    print(f"Failed to index {len(failed)} documents.")
    for item in failed:
        print(item)
except BulkIndexError as e:
    print(f"Failed to index {len(e.errors)} documents.")
    i=0
    for error in e.errors:
        if(i==5):
            break
        print(error) 
        i+=1

#for verifying indexed data
# response = es.search(index=index_name, body={"query": {"match_all": {}}})
# print("Search results:", response)
