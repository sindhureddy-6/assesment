import pandas as pd
from elasticsearch.helpers import bulk
from elasticsearch import Elasticsearch
from elasticsearch.helpers import BulkIndexError
from datetime import datetime
from elasticsearch.exceptions import NotFoundError


es = Elasticsearch([{'host': 'localhost', 'port': 9200, 'scheme': 'http'}])
csv_file = './Employee Sample Data 1.csv'

def createCollection(p_collection_name):
    p_collection_name = p_collection_name.lower()  
    if es.indices.exists(index=p_collection_name):
   
        es.indices.delete(index=p_collection_name)
        print(f"Existing collection '{p_collection_name}' deleted.")
    
   
    mappings = {
        "mappings": {
            "properties": {
                "Exit Date": {
                    "type": "date",
                    "format": "yyyy-MM-dd||MM/dd/yyyy"  # Specify the date formats
                },
           
            }
        }
    }
    es.indices.create(index=p_collection_name, body=mappings)
    print(f"Collection '{p_collection_name}' created.")

def convert_date_format(date_str):
    """Convert date strings from 'MM/DD/YYYY' to 'YYYY-MM-DD' format."""
    if date_str==0:  
        return None  
    if isinstance(date_str, str):  
        try:
            return datetime.strptime(date_str, '%m/%d/%Y').strftime('%Y-%m-%d')
        except ValueError:
            print(f"Warning: Could not convert date '{date_str}'. Returning original value.")
            return date_str 
    print(f"Warning: Invalid date type for {date_str}. Returning None.")
    return None  

def indexData(p_collection_name, p_exclude_column):
    
    employee_data = pd.read_csv(csv_file, encoding='ISO-8859-1', dtype=str).fillna(0)  # Treat all columns as strings


    if p_exclude_column in employee_data.columns:
        employee_data = employee_data.drop(columns=[p_exclude_column])

  
    if 'Exit Date' in employee_data.columns:
        employee_data['Exit Date'] = employee_data['Exit Date'].apply(convert_date_format)

   
    employee_data_json = employee_data.to_dict(orient='records')

    
    def create_employee_doc(data):
        for record in data:
            yield {
                "_index": p_collection_name,
                "_source": record
            }

    try:
        # Use the bulk helper to index documents
        success, failed = bulk(es, create_employee_doc(employee_data_json))
        print(f"Indexed {success} documents successfully.")
        print(f"Failed to index {len(failed)} documents.")
        for item in failed:
            print(item)  # Print the error details for each failed document
    except BulkIndexError as e:
        print(f"Failed to index {len(e.errors)} documents.")
        for error in e.errors:
            print(error)

def searchByColumn(p_collection_name, p_column_name, p_column_value):
    query = {
        "query": {
            "match": {p_column_name: p_column_value}
        }
    }
    
    # Execute the search query
    results = es.search(index=p_collection_name, body=query)
    
    # Extract total count and hits
    total_count = results['hits']['total']['value']
    hits = results['hits']['hits']
    
    print(f"Found {total_count} records where '{p_column_name}' is '{p_column_value}' in collection '{p_collection_name}'")

    # Format results
    formatted_results = []
    for hit in hits:
        record = {
            "Employee ID": hit['_source'].get("Employee ID"),
            "Full Name": hit['_source'].get("Full Name"),
            "Job Title": hit['_source'].get("Job Title"),
            "Business Unit": hit['_source'].get("Business Unit"),
            "Gender": hit['_source'].get("Gender"),
            "Ethnicity": hit['_source'].get("Ethnicity"),
            "Age": hit['_source'].get("Age"),
            "Hire Date": hit['_source'].get("Hire Date"),
            "Annual Salary": hit['_source'].get("Annual Salary"),
            "Bonus %": hit['_source'].get("Bonus %"),
            "Country": hit['_source'].get("Country"),
            "City": hit['_source'].get("City"),
            "Exit Date": hit['_source'].get("Exit Date")
        }
        formatted_results.append(record)

    return {
        "total_found": total_count,
        "records": formatted_results
    }

def getEmpCount(p_collection_name):
    count = es.count(index=p_collection_name)["count"]
    print(f"Collection '{p_collection_name}' contains {count} employees.")
    return count

def delEmpById(p_collection_name, p_employee_id):
    # First, search for the document using the Employee ID
    query = {
        "query": {
            "match": {
                "Employee ID": p_employee_id
            }
        }
    }
    
    # Search for the employee document
    search_results = es.search(index=p_collection_name, body=query)
    
    
    if search_results['hits']['total']['value'] > 0:
       
        doc_id = search_results['hits']['hits'][0]['_id']
        
        
        es.delete(index=p_collection_name, id=doc_id)
        print(f"Deleted employee with Employee ID {p_employee_id} from collection '{p_collection_name}'.")
    else:
        print(f"No employee found with Employee ID {p_employee_id} in collection '{p_collection_name}'.")

def getDepFacet(p_collection_name):
    query = {
        "aggs": {
            "department_count": {
                "terms": {
                    "field": "Department.keyword", 
                    "size": 1000  
                }
            }
        },
        "size": 0 
    }
    
    # Execute the search query with aggregation
    results = es.search(index=p_collection_name, body=query)
    
    # Extract department counts from results
    buckets = results['aggregations']['department_count']['buckets']
    
    print(f"Department facet for collection '{p_collection_name}':")
    department_counts = []
    
    for bucket in buckets:
        department_info = {
            "Department": bucket['key'],
            "Count": bucket['doc_count']
        }
        department_counts.append(department_info)
        print(f"Department: {bucket['key']}, Count: {bucket['doc_count']}")
    
    return department_counts




v_nameCollection = "hash_sindhu"
v_phoneCollection = "hash_0120"


createCollection(v_nameCollection)
createCollection(v_phoneCollection)
getEmpCount(v_nameCollection)
indexData(v_nameCollection, 'Department') 
indexData(v_phoneCollection,'Gender')
delEmpById(v_nameCollection, 'E02003')
getEmpCount(v_nameCollection)
searchByColumn(v_nameCollection,'Department','IT')
searchByColumn(v_nameCollection,'Gender' ,'Male')
searchByColumn(v_phoneCollection,'Department','IT')
getDepFacet(v_nameCollection)
getDepFacet(v_phoneCollection)
