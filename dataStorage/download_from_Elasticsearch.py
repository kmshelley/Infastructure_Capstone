import os
import geojson, json
from elasticsearch import Elasticsearch
from elasticsearch import helpers
from elasticsearch.exceptions import NotFoundError
import ConfigParser
from pprint import pprint
from copy import deepcopy

#read in the config file
config = ConfigParser.ConfigParser()
config.read('./config/capstone_config.ini')

ES_url = config.get('ElasticSearch','host')
ES_password = config.get('ElasticSearch','password')
ES_username= config.get('ElasticSearch','username')

def es_geoshape_to_geojson(index,doc_type,geoshape_field,outfile,query={}):
    #input: ES index, doctype, and field that is mapped as a geoshape
    #output: Geojson feature collection
    es_full_url = 'http://' + ES_username + ':' + ES_password + '@' + ES_url + ':9200'
    es = Elasticsearch(es_full_url)

    features = []
    docs = helpers.scan(es,index=index,doc_type=doc_type,query=json.dumps(query))
    for doc in docs:
        #define the geojson shape type based on ES geoshape type
        if doc['_source'][geoshape_field]['type'].lower() == 'point':
            shape=geojson.Point(doc['_source'][geoshape_field]['coordinates'])
        if doc['_source'][geoshape_field]['type'].lower() == 'linestring':
            shape=geojson.LineString(doc['_source'][geoshape_field]['coordinates'])
        if doc['_source'][geoshape_field]['type'].lower() == 'polygon':
            shape=geojson.Polygon(doc['_source'][geoshape_field]['coordinates'])
        if doc['_source'][geoshape_field]['type'].lower() == 'multipolygon':
            shape=geojson.MultiPolygon(doc['_source'][geoshape_field]['coordinates'])

        #print geojson.is_valid(shape)
        
        props = deepcopy(doc['_source'])
        props.pop(geoshape_field,None)
        features.append(geojson.Feature(geometry=shape,properties=props))

    
    with open(outfile,'w') as out:
        geojson.dump(geojson.FeatureCollection(features),out)


