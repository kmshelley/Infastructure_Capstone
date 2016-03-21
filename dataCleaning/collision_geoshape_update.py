import os
import geojson, json
from elasticsearch import Elasticsearch
from elasticsearch import helpers
import ConfigParser
from pprint import pprint
from copy import deepcopy
import subprocess
from pprint import pprint
from dataStorage import upload_to_Elasticsearch
from shapely.geometry import Polygon
from pyproj import Proj

#read in the config file
config = ConfigParser.ConfigParser()
config.read('./config/capstone_config.ini')
#getting config file two folders up so we don't check in password by mistake
#config.read(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))) + '/capstone_config.ini')


ES_url = config.get('ElasticSearch','host')
ES_password = config.get('ElasticSearch','password')
ES_username= config.get('ElasticSearch','username')

def add_collision_geoshape_point(index, doc_type):
    #input: index name, doc_type
    #Searches ES collisions data, updates records with gepshape mapping
    es_url = 'http://%s:%s@%s:9200' % (ES_username,ES_password,ES_url)
    es = Elasticsearch(es_url)


    try:
        mapping = {}
        mapping['properties'] = {}

        #iset the geo_shape mapping
        mapping['properties']['collision_GEOSHAPE_C'] = {'type':'geo_shape','tree':'quadtree', 'precision': '1m'}
        
        #use cURL to put the mapping
        p = subprocess.Popen(['curl','%s/%s/_mapping/%s' % (es_url,index,doc_type),'-d','%s' % json.dumps(mapping)],stderr=subprocess.PIPE)
        out, err = p.communicate()
        if err: print '\n' + err
        
    except Exception as e:
        #do not try to recreate the index
        print "Error creating index:"
        print e
        
    
    idx=0
    updates=[]
    for result in helpers.scan(es,index=index,doc_type=doc_type): 
        if 'collision_GEOJSON_C' in result['_source']:
            idx+=1
            _id = result['_id']
            #make copy of geopoint field as a geoshape point
            result['_source']['collision_GEOSHAPE_C'] = {
                                                            "type":"point",
                                                             "coordinates": result['_source']['collision_GEOJSON_C'] 
                                                        }

            updates.append(result['_source'])

        if idx >= 10000:
            upload_to_Elasticsearch.update_ES_records_curl(updates,index=index,doc_type=doc_type,id_field='collision_UNIQUE KEY')
            idx=0
            updates=[]
        
    #upload the remaining records
    upload_to_Elasticsearch.update_ES_records_curl(updates,index=index,doc_type=doc_type,id_field='collision_UNIQUE KEY')
