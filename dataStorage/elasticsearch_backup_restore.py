import os
#import zipfile
import gzip
import datetime as dt
import json
import requests
from elasticsearch import Elasticsearch
from elasticsearch import helpers
from elasticsearch.exceptions import NotFoundError
import ConfigParser
from pprint import pprint
from copy import deepcopy
from dataStorage import upload_to_Elasticsearch

#read in the config file
config = ConfigParser.ConfigParser()
config.read('./config/capstone_config.ini')

ES_url = config.get('ElasticSearch','host')
ES_password = config.get('ElasticSearch','password')
ES_username= config.get('ElasticSearch','username')


def dump_index_to_json(index,doc_type,dump_loc,q={}):
    #input: Index name, document type, location to store the data dump, OPTIONAL query
    #output: dumps the index to a zipped json file stored in dump_loc
    es_full_url = 'http://' + ES_username + ':' + ES_password + '@' + ES_url + ':9200'
    es = Elasticsearch(es_full_url)

    start=dt.datetime.now()

    #export the index mapping
    r = requests.get('%s/%s/_mapping' % (es_full_url,index))
    with open(os.path.join(dump_loc,'%s_MAPPING.json' % index),'w') as outfile:
        json.dump(r.json(),outfile)

    #export the index data
    gz_f = os.path.join(dump_loc,'%s_%s_DUMP.gz' % (index,doc_type))
    with gzip.open(gz_f, 'wb') as outfile:
        for doc in helpers.scan(es,index=index,doc_type=doc_type,query=q):
            outfile.write(json.dumps(doc['_source']) + '\n')

    print "Completed data dump. Took %s" % str(dt.datetime.now()-start)

    
def restore_index_from_json(index,doc_type,dump_loc,**kwargs):
    #input: file name of ES data output from 'dump_index_to_json', Index name, document type, OPTIONAL mapping file
    #output: builds the index in ES

    #define keyword inputs
    id_field = kwargs.get('id_field', False)
    delete_index = kwargs.get('delete_index', False)
    
    es_full_url = 'http://' + ES_username + ':' + ES_password + '@' + ES_url + ':9200'

    #create the index, set the replicas so uploads will not err out
    settings = {"settings": {"number_of_replicas" : 1} }
    r = requests.post('%s/%s' % (es_full_url,index),data=json.dumps(settings))
    
    #if there is a mapping file, post the mapping
    mapping = os.path.join(dump_loc,'%s_MAPPING.json' % index)
    if os.path.exists(mapping):
        with open(mapping,'r') as mapping_file:
            data=json.dumps(json.load(mapping_file)[index]['mappings'][doc_type])
            r = requests.post('%s/%s/_mapping/%s' % (es_full_url,index,doc_type),data=data)
            
    #check to see if the file is gzipped
    f = os.path.join(dump_loc,'%s_%s_DUMP.gz' % (index,doc_type))
    ext = os.path.basename(f).split('.')[-1]
    
    with gzip.open(f,'rb') as upload:
        docs=[]
        bulk=0    
        for line in upload:
            bulk+=1
            docs.append(json.loads(line.strip()))
            #upload 10k records at a time
            if bulk == 10000:
                upload_to_Elasticsearch.bulk_upload_docs_to_ES_cURL(docs,index=index,doc_type=doc_type,id_field=id_field,delete_index=delete_index)
                bulk=0
                docs=[]
        #upload the remaining records        
        upload_to_Elasticsearch.bulk_upload_docs_to_ES_cURL(docs,index=index,doc_type=doc_type,id_field=id_field,delete_index=delete_index)     


