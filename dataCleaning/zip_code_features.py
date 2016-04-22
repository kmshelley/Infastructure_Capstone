import sys
sys.path.append('/root/Infrastructure_Capstone')
import os
import subprocess
import math
from shapely.geometry import Polygon,LineString
from pyproj import Proj
from pprint import pprint
from copy import deepcopy
import datetime as dt
import time
from dateutil.parser import parse
import json, csv
from itertools import izip, product
import requests

from dataStorage import upload_to_Elasticsearch

from elasticsearch import Elasticsearch
from elasticsearch import helpers
from elasticsearch.client import indices

import ConfigParser

#read in the config file
config = ConfigParser.ConfigParser()
config.read('./config/capstone_config.ini')

ES_url = config.get('ElasticSearch','host')
ES_hosts = config.get('ElasticSearch','hostlist')
ES_password = config.get('ElasticSearch','password')
ES_username= config.get('ElasticSearch','username')

def zip_traffic_data(zipcode):
    #returns median and average Annualized Average Daily Traffic counts from 2013 accross all streets in the zip code
    #(Does not include streets with no AADT information)
    output = {}
    #adds data about streets in the zip code
    es_url = 'http://%s:%s@%s:9200' % (ES_username,ES_password,ES_url)
    es = Elasticsearch(es_url)
    query = '''{
                "fields": ["AvgTraffic"],
                "query":{
                        "bool":{
                                "must":{"match_all": {}},
                                "filter":{
                                        "geo_shape":{
                                                "geometry":{
                                                        "indexed_shape": {
                                                                "index": "nyc_zip_codes",
                                                                "type": "zip_codes",
                                                                "id": "%s",
                                                                "path": "coords"
                                                                }
                                                        }
                                                }
                                        }
                                }
                        }
                }''' % zipcode
    counts = []
    for doc in helpers.scan(es,query=query,index='nyc_street_traffic',doc_type='AADT13'): #iterate through all the streets in the AADT13 index
        #only add traffic count data > 0 (0 == missing data)
        if doc['fields']['AvgTraffic'][0] > 0: counts.append(doc['fields']['AvgTraffic'][0])
    if len(counts)>0:
        output['Median AADT'] = counts[len(counts)/2]
        output['Average AADT'] = sum(counts)/len(counts)
    else:
        output['Median AADT']=0
        output['Average AADT']=0

    return output

        
def zip_street_data(zipcode,p=Proj(init='epsg:2263')):
    #returns a dict of data about streets in the zipcode
    
    output = {}
    output['total_road_length']=0
    output['total_road_count']=0
    #speed limits
    speeds=[]
    output['5mph']=0
    output['15mph']=0
    output['25mph']=0
    output['35mph']=0
    output['45mph']=0
    output['55mph']=0
    output['65mph']=0
    output['85mph']=0
    #artertial classification code counts
    output['ACC_1']=0
    output['ACC_2']=0
    output['ACC_3']=0
    output['ACC_4']=0
    output['ACC_5']=0
    output['ACC_6']=0
    #count of tunnels/bridges/
    output['tunnels']=0
    output['bridges']=0
    
    
    #adds data about streets in the zip code
    es_url = 'http://%s:%s@%s:9200' % (ES_username,ES_password,ES_url)
    es = Elasticsearch(es_url)
    query = '''{
                "query" : {
                    "bool": {
                        "should": [
                            { "term" : { "LeftPostal" : "%s" } },
                            { "term" : { "RightPosta" : "%s" } }
                        ]
                    }
                }
            }''' % (zipcode,zipcode)

    for doc in helpers.scan(es,query=query,index='nyc_streets',doc_type='complete_segments'): #iterate through all the streets in the zip code
        street = doc['_source']
        output['total_road_count']+=1
        
        line = LineString([p(lng,lat) for lng,lat in street['segment']['coordinates']])
        output['total_road_length']+=(line.length/1609.34) #length in statute miles

        #speed limits
        speeds.append(float(street['SPEED']))
        if float(street['SPEED'])==5:
            output['5mph']+=1.0
        if float(street['SPEED'])==15:
            output['15mph']+=1.0
        if float(street['SPEED'])==25:
            output['25mph']+=1.0
        if float(street['SPEED'])==35:
            output['35mph']+=1.0
        if float(street['SPEED'])==45:
            output['45mph']+=1.0
        if float(street['SPEED'])==55:
            output['55mph']+=1.0
        if float(street['SPEED'])==65:
            output['65mph']+=1.0
        if float(street['SPEED'])==85:
            output['85mph']+=1.0
        
        #arterial classification
        if street['PostType'].lower()=='tunl':output['tunnels']+=1.0
        if street['PostType'].lower()=='brg':output['bridges']+=1.0

    if len(speeds) > 0:
        speeds.sort()
        output['median_speed_limit']=speeds[len(speeds)/2]
        #convert count of speed limits to % of roads
        output['5mph']/=len(speeds)
        output['15mph']/=len(speeds)
        output['25mph']/=len(speeds)
        output['35mph']/=len(speeds)
        output['45mph']/=len(speeds)
        output['55mph']/=len(speeds)
        output['65mph']/=len(speeds)
        output['85mph']/=len(speeds)
    
    return output


def update_zip_code_level_data(index,doc_type,p=Proj(init='epsg:2263')):
    #updates the zip code collection with zip-specific data
    es_url = 'http://%s:%s@%s:9200' % (ES_username,ES_password,ES_url)
    es = Elasticsearch(es_url)
    to_update=[]
    for doc in helpers.scan(es,index=index,doc_type=doc_type): #iterate through all the streets in the zip code
        zipcode = doc['_source']
        zipcode['area'] = Polygon([p(lng,lat) for lng,lat in zipcode['coords']['coordinates'][0]]).area/(1609.34**2)
        zipcode.update(zip_street_data(zipcode['zipcode'].split('-')[0],p))
        zipcode.update(zip_traffic_data(zipcode['zipcode']))
        to_update.append(zipcode)
        kwargs={"index":"nyc_zip_codes","doc_type":"zip_codes","id_field":"zipcode"}
    upload_to_Elasticsearch.update_ES_records_curl(to_update,**kwargs)
    
                               



                       
