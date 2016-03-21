from sodapy import Socrata
import time
from dataStorage import upload_to_Elasticsearch

def get_open_data(url,endpoint,api_key,limit=1000,query=''):
    #input: Socrata endpoint for data, and OPTIONAL query
    client = Socrata(url,api_key)
    out = []
    idx=0
    time.sleep(5)#sleep 20 seconds, to allow time to connect
    data = client.get(endpoint,limit=limit,offset=idx,where=query)
    time.sleep(20)#sleep 20 seconds, to allow time to connect
    while len(data) > 0:
        #page through the results, appending to the out list
        out+=data
        idx+=limit
        data = client.get(endpoint,limit=limit,offset=idx,where=query)

    client.close()
    return out
        
        
def upload_open_data_to_Elasticsearch(url,endpoint,api_key,query=None,kwargs={}):
    #input: Socrata url, endpoint, API key, OPTIONAL query, and ES bulk upload kwargs
    #output: uploads data to ES index
    client = Socrata(url,api_key)
    idx=0
    time.sleep(5)#sleep 20 seconds, to allow time to connect
    docs = client.get(endpoint,limit=10000,offset=0,where=query)
    upload_to_Elasticsearch.bulk_upload_docs_to_ES_cURL(docs,**kwargs)
    #time.sleep(20)#sleep 20 seconds, to allow time to connect
    while len(docs) > 0:
        #page through the results, appending to the out list
        idx+=10000
        docs = client.get(endpoint,limit=10000,offset=idx,where=query)
        upload_to_Elasticsearch.update_ES_records_curl(docs,**kwargs)
    client.close()
