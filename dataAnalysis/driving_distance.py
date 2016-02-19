import os
from pprint import pprint
import geojson
import csv
import LatLon
import ConfigParser
import datetime as dt
from elasticsearch import Elasticsearch
from elasticsearch import helpers
from dataAcquisition import address_geocoding


#read in the config file
config = ConfigParser.ConfigParser()
#config.read('E:/GoogleDrive/DataSciW210/Final/20160214/Infrastructure_Capstone/config/capstone_config.ini')
config.read('./config/capstone_config.ini')

ES_url = config.get('ElasticSearch','host')
ES_password = config.get('ElasticSearch','password')
ES_username= config.get('ElasticSearch','username')



def great_circle_distance_to_collision(loc_file,closest_field,dist_field):
    #input: filename of locations (can be geojson or CSV with 'Latitude', 'Longitude', and 'Name' fields,
    #string representing the fieldname to add for the closest location, string representing fieldname of GCD field
    #output: updates fields in collisions table with closest_field and dist_field

    es = Elasticsearch(['http://' + ES_username + ':' + ES_password + '@' + ES_url + ':9200/'])

    accidents = helpers.scan(es,
        query={'query': {'match_all': {}}},
        index='saferoad',
        doc_type='collisions'
    )
    
    index = 0
    actions = []
    with open(loc_file,'r') as locs:
        for hit in accidents:
            if hit['_source']['LONGITUDE'] <> '':
                ll1 = LatLon.LatLon(float(hit['_source']['LATITUDE']),float(hit['_source']['LONGITUDE']))
                
                min_dist = float('inf')
                nearest_loc = None

                locs.seek(0)#move the pointer to the beginning of the file
                ext = os.path.basename(loc_file).split('.')[-1] #extension of the file
                if ext == 'csv':
                    reader = csv.DictReader(locs)
                    for row in reader:
                        ll2 = LatLon.LatLon(float(row['Latitude']),float(row['Longitude']))
                        loc_name = row['Name']
                        dist = float(ll1.distance(ll2)) #great circle distance in KM
                        if dist < min_dist:
                            min_dist = dist
                            nearest_loc = loc_name
                        
                elif ext == 'json' or ext =='geojson':
                    geo = geojson.load(locs)
                    for feature in geo['features']:
                        ll2 = LatLon.LatLon(float(feature['geometry']['coordinates'][1]),float(feature['geometry']['coordinates'][0]))
                        loc_name = feature['properties']['Name']
                        dist = float(ll1.distance(ll2)) #great circle distance in KM
                        if dist < min_dist:
                            min_dist = dist
                            nearest_loc = loc_name
                    
                
                hit['_source'][closest_field] = nearest_loc
                hit['_source'][dist_field] = min_dist
                
                #temporarily added for updating the geojson_c field
                #hit['_source']['GEOJSON_C'] = [float(hit['_source']['LONGITUDE']),float(hit['_source']['LATITUDE'])]
                
                action = {
                           '_op_type': 'update',
                           '_index' : 'saferoad',
                           '_type':'collisions',
                           '_id' : hit['_source']['ID'],
                           'doc' : hit['_source']                               
                        }
                actions.append(action)
                index+=1

                #update records 10,000 at a time
                if index == 10000:
                    try:
                        helpers.bulk(es, actions)
                        #pprint(actions)
                        print 'Successfully updated %s records!' % index
                        actions = []
                        index=0
                    except Exception as e:
                        print '### ERRROR: %s' % e
        #push final set of actions
        try:
            helpers.bulk(es, actions)
            print 'Successfully updated %s records!' % index
        except Exception as e:
            print '### ERRROR: %s' % e
                    
                        


