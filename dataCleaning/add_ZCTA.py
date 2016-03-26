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


    
def add_zcta_zip_to_index(index,doc_type,loc_field,id_field,prefix=None):
    #iterates through zcta zip code polygons, updates index records with zip code id they are contained within
    #if no lat/lng adds 'NA' to field
    #input: index name, doc_type
    #Searches ES collisions data, updates records with gepshape mapping
    es_url = 'http://%s:%s@%s:9200' % (ES_username,ES_password,ES_url)
    es = Elasticsearch(es_url)

    proj = Proj(init='epsg:2263') #NY/Long Island UTM projection

    if prefix:
        zip_field1 = prefix + '_ZCTA_ZIP'
        zip_field2 = prefix + '_ZCTA_ZIP_NoSuffix'
    else:
        zip_field1 = 'ZCTA_ZIP'
        zip_field2 = 'ZCTA_ZIP_NoSuffix'
        
    try:
        mapping = {}
        mapping['properties'] = {}

        #set the ZCTA zip fieldmapping
        mapping['properties'][zip_field1] = {'type':'string'}
        mapping['properties'][zip_field2] = {'type':'string'}
        
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
        idx+=1
        _id = result['_id']
        #Add placeholder for ZCTA zip code
        result['_source'][zip_field1] = 'NA'
        result['_source'][zip_field2] = 'NA'


        if loc_field in result['_source']:
            query = '''{
                        "query":{
                                "bool":{
                                        "must":{"match_all": {}},
                                        "filter":{
                                                "geo_shape":{
                                                        "coords":{
                                                                "indexed_shape": {
                                                                        "index": "%s",
                                                                        "type": "%s",
                                                                        "id": "%s",
                                                                        "path": "%s"
                                                                        },
                                                                "relation": "intersects"
                                                                }
                                                        }
                                                }
                                        }
                                }
                        }''' % (index,doc_type,_id,loc_field)
            max_area = 0
            max_zip = False
            #query the zip codes, finding all zip shapes that contain the current colision
            for shape in helpers.scan(es,query=query,index='nyc_zip_codes',doc_type='zip_codes'):
                coords = [proj(lng,lat) for lng,lat in shape['_source']['coords']['coordinates'][0]]
                poly = Polygon(coords)
                if poly.area > max_area:
                    #get the largest zip code by geographic area
                    max_area = poly.area
                    max_zip = shape['_id']
            if max_zip:
                result['_source'][zip_field1] = max_zip
                result['_source'][zip_field2] =  max_zip.split('-')[0]
        updates.append(result['_source'])

        if idx >= 10000:
            upload_to_Elasticsearch.update_ES_records_curl(updates,index=index,doc_type=doc_type,id_field=id_field)
            idx=0
            updates=[]
        
    #upload the remaining records
    upload_to_Elasticsearch.update_ES_records_curl(updates,index=index,doc_type=doc_type,id_field=id_field)



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
            max_zip = False
            #query the zip codes, finding all zip shapes that contain the current colision
            for shape in helpers.scan(es,query=query,index='nyc_zip_codes',doc_type='zip_codes'):
                coords = [proj(lng,lat) for lng,lat in shape['_source']['coords']['coordinates'][0]]
                poly = Polygon(coords)
                if poly.area > max_area:
                    #get the largest zip code by geographic area
                    max_area = poly.area
                    max_zip = shape['_id']
            
            if max_zip:
                result['_source']['collision_ZCTA_ZIP'] = max_zip
                result['_source']['collision_ZCTA_ZIP_NoSuffix'] =  max_zip.split('-')[0]
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
        max_zip = None
        #query the zip codes, finding all zip shapes that contain the current colision
        for shape in helpers.scan(es,query=query,index='nyc_zip_codes',doc_type='zip_codes'):
            coords = [proj(lng,lat) for lng,lat in shape['_source']['coords']['coordinates'][0]]
            poly = Polygon(coords)
            if poly.area > max_area:
                #get the largest zip code by geographic area
                max_area = poly.area
                max_zip = shape['_id']
        if max_zip:
            record['collision_ZCTA_ZIP'] = max_zip.split('-')[0]
    return record
        
