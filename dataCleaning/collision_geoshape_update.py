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
    

def add_zcta_zip_to_collisions():
    #iterates through zcta zip code polygons, updates collisions records with zip code id they are contained within
    #if no lat/lng for collision adds 'NA' to field
    #input: index name, doc_type
    #Searches ES collisions data, updates records with gepshape mapping
    es_url = 'http://%s:%s@%s:9200' % (ES_username,ES_password,ES_url)
    es = Elasticsearch(es_url)

    proj = Proj(init='epsg:2263') #NY/Long Island UTM projection

    try:
        mapping = {}
        mapping['properties'] = {}

        #set the ZCTA zip fieldmapping
        mapping['properties']['collision_ZCTA_ZIP'] = {'type':'string'}
        mapping['properties']['collision_ZCTA_ZIP_NoSuffix'] = {'type':'string'}
        
        #use cURL to put the mapping
        p = subprocess.Popen(['curl','%s/%s/_mapping/%s' % (es_url,'saferoad','collisions'),'-d','%s' % json.dumps(mapping)],stderr=subprocess.PIPE)
        out, err = p.communicate()
        if err: print '\n' + err
        
    except Exception as e:
        #do not try to recreate the index
        print "Error creating index:"
        print e
        
    
    idx=0
    updates=[]
    for result in helpers.scan(es,index='saferoad',doc_type='collisions'): 
        idx+=1
        _id = result['_id']
        #Add placeholder for ZCTA zip code
        result['_source']['collision_ZCTA_ZIP'] = 'NA'

        if 'collision_GEOSHAPE_C' in result['_source']:
            query = '{ \
                    "query":{ \
                            "bool":{ \
                                    "must":{"match_all": {}}, \
                                    "filter":{ \
                                            "geo_shape":{ \
                                                    "coords":{ \
                                                            "indexed_shape": { \
                                                                    "index": "saferoad", \
                                                                    "type": "collisions", \
                                                                    "id": "%s", \
                                                                    "path": "collision_GEOSHAPE_C" \
                                                                    } \
                                                            } \
                                                    } \
                                            } \
                                    } \
                            } \
                    }' % result['_id']
            max_area = 0
            max_zip = None
            #query the zip codes, finding all zip shapes that contain the current colision
            for shape in helpers.scan(es,query=query,index='nyc_zip_codes',doc_type='zip_codes'):
                coords = [proj(lng,lat) for lng,lat in shape['_source']['coords']['coordinates'][0]]
                poly = Polygon(coords)
                if poly.area > max_area:
                    #get the largest zip code by geographic area
                    max_area = poly.area
                    max_zip = shape['_id']
            
            result['_source']['collision_ZCTA_ZIP'] = max_zip
            result['_source']['collision_ZCTA_ZIP_NoSuffix'] =  max_zip
        updates.append(result['_source'])

        if idx >= 10000:
            upload_to_Elasticsearch.update_ES_records_curl(updates,index='saferoad',doc_type='collisions',id_field='collision_UNIQUE KEY')
            idx=0
            updates=[]
        
    #upload the remaining records
    upload_to_Elasticsearch.update_ES_records_curl(updates,index='saferoad',doc_type='collisions',id_field='collision_UNIQUE KEY')


def add_zcta_zip_to_collision_rec(record):
    #performs geoquery on nyc_zip_codes to assign ZCTA zip to single collision record
    #if no lat/lng for collision adds 'NA' to field
    #input: collision record
    #Searches ES collisions data, updates record with ZCTA zip
    es_url = 'http://%s:%s@%s:9200' % (ES_username,ES_password,ES_url)
    es = Elasticsearch(es_url)

    proj = Proj(init='epsg:2263') #NY/Long Island UTM projection
    
    #Add placeholder for ZCTA zip code
    record['collision_ZCTA_ZIP'] = 'NA'

    if 'collision_GEOSHAPE_C' in record:
        query = '{ \
                "query":{ \
                        "bool":{ \
                                "must":{"match_all": {}}, \
                                "filter":{ \
                                        "geo_shape":{ \
                                                "coords":{ \
                                                        "shape": { \
                                                                "type": "point", \
                                                                "coordinates": %s \
                                                                } \
                                                        } \
                                                } \
                                        } \
                                } \
                        } \
                }' % record['collision_GEOSHAPE_C']['coordinates']
        max_area = 0
        max_zip = 'NA'
        #query the zip codes, finding all zip shapes that contain the current colision
        for shape in helpers.scan(es,query=query,index='nyc_zip_codes',doc_type='zip_codes'):
            coords = [proj(lng,lat) for lng,lat in shape['_source']['coords']['coordinates'][0]]
            poly = Polygon(coords)
            if poly.area > max_area:
                #get the largest zip code by geographic area
                max_area = poly.area
                max_zip = shape['_id']
                
        record['collision_ZCTA_ZIP'] = max_zip
    return record
        
