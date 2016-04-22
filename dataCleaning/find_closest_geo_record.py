#lat/lon grid class
import os
import math
from shapely.geometry import Point,Polygon
from pyproj import Proj
from elasticsearch import Elasticsearch
from elasticsearch import helpers
from elasticsearch.client import indices
from dataStorage import upload_to_Elasticsearch
import ConfigParser
from pprint import pprint
from copy import deepcopy

#read in the config file
config = ConfigParser.ConfigParser()
config.read('./config/capstone_config.ini')

ES_url = config.get('ElasticSearch','host')
ES_password = config.get('ElasticSearch','password')
ES_username= config.get('ElasticSearch','username')


def find_closest(index1,doc_type1,geo_field1,id_field1,index2,doc_type2,geo_field2,proj=None):
    #input: 2 indexes, doc_types, and geospatial fields
    #output: updates index 1 with record of closest item from index2

    ##O(N^2) RUNTIME. ONLY RUN ON SMALL INDEXES!!
    
    es_url = 'http://%s:%s@%s:9200' % (ES_username,ES_password,ES_url)
    es = Elasticsearch(es_url)

    mapping1 = es.indices.get_field_mapping(index=index1,doc_type=doc_type1,fields=geo_field1)
    type1 = mapping1[index1]['mappings'][doc_type1][geo_field1]['mapping'][geo_field1]['type']
    #print type1
    
    mapping2 = es.indices.get_field_mapping(index=index2,doc_type=doc_type2,fields=geo_field2)
    type2 = mapping2[index2]['mappings'][doc_type2][geo_field2]['mapping'][geo_field2]['type']
    #print type2
    
    updates = []
    idx=0
    for res1 in helpers.scan(es,index=index1,doc_type=doc_type1):
        _id = res1['_id']
        if type1 == 'geo_point':
            if proj:
                poly1 = Point(proj(res1['_source'][geo_field1][0],res1['_source'][geo_field1][1]))
            else:
                poly1 = Point(res1['_source'][geo_field1])
        else:
            if res1['_source'][geo_field1]['type'].lower() == 'point':
                if proj:
                    poly1 = Point(proj(res1['_source'][geo_field1]['coordinates'][0],res1['_source'][geo_field1]['coordinates'][1]))
                else:
                    poly1 = Point(res1['_source'][geo_field1]['coordinates'])
            else:
                if proj:
                    coords = [proj(lng,lat) for lng,lat in res1['_source'][geo_field1]['coordinates'][0]]
                    poly1 = Polygon(coords)
                else:
                    poly1 = Polygon(res1['_source'][geo_field1]['coordinates'][0])
        
        idx+=1
        min_dist = float('inf')
        closest = None
        for res2 in list(helpers.scan(es,index=index2,doc_type=doc_type2))[:5]: 
            if type2 == 'geo_point':
                if proj:
                    poly2 = Point(proj(res2['_source'][geo_field2][0],res2['_source'][geo_field2][1]))
                else:
                    poly2 = Point(res2['_source'][geo_field2])
            else:
                if res2['_source'][geo_field2]['type'].lower() == 'point':
                    if proj:
                        poly2 = Point(proj(res2['_source'][geo_field2]['coordinates'][0],res2['_source'][geo_field2]['coordinates'][1]))
                    else:
                        poly2 = Point(res2['_source'][geo_field2]['coordinates'])
                else:
                    if proj:
                        coords = [proj(lng,lat) for lng,lat in res2['_source'][geo_field2]['coordinates'][0]]
                        poly2 = Polygon(coords)
                    else:
                        poly2 = Polygon(res2['_source'][geo_field2]['coordinates'][0])

            c1,c2 = poly1.centroid.coords[0],poly2.centroid.coords[0]
            dist = math.sqrt((c1[0]-c2[0])**2 + (c1[1]-c2[1])**2)
            if dist < min_dist:
                min_dist = dist
                closest = res2['_id']
            
        new_doc = deepcopy(res1['_source'])
        new_doc['closest_%s' % doc_type2] = closest
        updates.append(new_doc)
        if idx >= 10000:
            upload_to_Elasticsearch.update_ES_records_curl(updates,index=index1,doc_type=doc_type1,id_field=id_field1)
            idx=0
            updates=[]
    #upload remaining records        
    upload_to_Elasticsearch.update_ES_records_curl(updates,index=index1,doc_type=doc_type1,id_field=id_field1)        

    
