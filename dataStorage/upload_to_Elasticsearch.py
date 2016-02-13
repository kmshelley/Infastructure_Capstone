import os
import geojson
from elasticsearch import Elasticsearch
from elasticsearch import helpers
import ConfigParser
from pprint import pprint

#read in the config file
config = ConfigParser.ConfigParser()
config.read('./config/capstone_config.ini')
ES_url = config.get('ElasticSearch','host')


def upload_docs_to_ES(docs,index,doc_type,id_field,geopoint=False):
    #input: list of JSON documents, an index name, document type, ID field, and name of an (OPTIONAL) geopoint field.
    #uploads each feature element to ElasticSearch
    es = Elasticsearch(ES_url)
    try:
        es.indices.create(index)
    except:
        #do not try to recreate the index
        pass
    
    #if the data has a location field, set the geo_point mapping
    if geopoint:
        mapping = {doc_type:{'properties':{geopoint:{'type':'geo_point','store':'yes'}}}}
        es.indices.put_mapping(index=index, doc_type=doc_type, body=mapping)

    actions = []
    #build the list of ElasticSearch uploads for bulk command
    for doc in docs:
        action = {
            '_index': index,
            '_type': doc_type,
            }
        #check if the document is a geojson document
        validation = geojson.is_valid(doc)
        if validation['valid'].lower() == 'yes':
            #add the point to the document properties
            doc['properties'][geopoint] = list(geojson.utils.coords(doc))[0]

            #load the document properties into ES
            action['_source'] = doc['properties']
            
            #get id from geojson properties document
            action['_id'] = doc['properties'][id_field]        
        else:
            #assign id for typical json document
            action['_id'] = doc[id_field]
            action['_source'] = doc
            
        actions.append(action)
    try:
        helpers.bulk(es, actions)
        print 'Sucessfully uploaded %s records!' % str(len(actions))
    except Exception as e:
        print '#### ERROR:s'
        pprint(e)
    

    
def delete_ES_records(index,doc_type):
    #deletes all ElasticSearch records for an index (recrusively runs until index is empty
    es = Elasticsearch(ES_url) #connect to ElasticSearch instance

    try:
        records = [res['_id'] for res in es.search(index)['hits']['hits']] #list of all WBAN station ID's
        if len(records) > 0:
            deleted = 0
            for rec in records:
                es.delete(index=index,doc_type=doc_type,id=rec)
                deleted+=1

            print 'Sucessfully deleted: %s' % deleted
            delete_ES_records(index,doc_type)
        else:
            return
            
    except Exception as e:
        print '#### ERROR: %s' % e
        
        
   
