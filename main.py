from classes.SearchGrid import SearchGrid
from dataAcquisition import acquire_QCLCD_data, acquire_NYC_Collisions, address_geocoding
from dataAnalysis import driving_distance
from dataStorage import upload_to_Elasticsearch
import geojson,sys

__author__ = 'Katherine'

if __name__ == '__main__':
    '''Main Entry Point to the Program'''

    # Upload Weather Stations to ElasticSearch #
##    geofile = 'E:/GoogleDrive/DataSciW210/Final/datasets/NY_Area_WBAN_Stations.geojson'
##    import geojson
##    with open(geofile,'r') as geo:
##        stations = geojson.load(geo)['features']
##    upload_to_Elasticsearch.upload_docs_to_ES(stations,'weather_stations','weather_stations','WBAN','loc')

    #   Upload weather data to Elasticsearch   #
    ###CHANGE THESE FIELDS###
##    months = range(12,11,-1)
##    years = range(2011,2010,-1)
##    acquire_QCLCD_data.collect_and_store_weather_data(months,years)
    ######


    #   Upload EMS/FDNY station data to Elasticsearch   #
    #import geojson
    #stations = address_geocoding.geojson_from_address_file('E:/GoogleDrive/DataSciW210/Final/datasets/NYC_EMS_Locations.csv','Address')
    #with open('E:/GoogleDrive/DataSciW210/Final/datasets/NYC_EMS_Locations.json','w') as geo:
    #    geojson.dump(stations,geo)
        
    #upload_to_Elasticsearch.upload_docs_to_ES(stations,'emergency_stations','fdny','FacilityName','loc')

    

    #   Upload collision data to Elasticsearch   #
##    collisions = 'E:/GoogleDrive/DataSciW210/Final/datasets/collisions.csv'
    #collisions = '../../colSmall.csv'
    #collisions = '../../collisions.csv'
    #acquire_NYC_Collisions.upload_collision_data(collisions,index="saferoad",doc_type="collisions")

    
    #   Define driving distances, upload to ElasticSearch    #
##    driving_distance.great_circle_distance_to_collision('./flatDataFiles/NYC_EMS_Locations.json',closest_field='Closest EMS Station',dist_field='GCD to EMS KM')
##    driving_distance.great_circle_distance_to_collision('./flatDataFiles/NYC_EMS_Locations.json',closest_field='Closest Trauma Center',dist_field='GCD to Trauma Center KM')


    ### GRID DEVLOPMENT ###

    #Connect to ElasticSearch
    import ConfigParser
    from elasticsearch import Elasticsearch

    #read in the config file
    config = ConfigParser.ConfigParser()
    config.read('./config/capstone_config.ini')
    
    ES_url = config.get('ElasticSearch','host')
    ES_password = config.get('ElasticSearch','password')
    ES_username= config.get('ElasticSearch','username')

    es = Elasticsearch(['http://' + ES_username + ':' + ES_password + '@' + ES_url + ':9200/'])

    from total_size import total_size
    #Lambert Conformal Conic projection centered on the polygon definition
    lcc = '+proj=lcc +lat_1=%s +lat_2=%s +lat_0=%s +lon_0=%s +preserve_units = True +ellps=clrk66' % (33,45,40.6795535,-74.0006865)
    with open('./flatDataFiles/NYC_polygon.json','r') as geo_file:
        geo = geojson.load(geo_file)
        geo_poly = geo['features'][0]
        poly = list(geojson.utils.coords(geo_poly))
        #print len(poly)
        
        

    from datetime import datetime as dt
    start_time = dt.now()

    data_start = dt(2012,6,30,17,0)
    data_end = dt(2016,2,5,16,0)
    total_hours = (data_end - data_start).days * 24
    
    data_bounds = [-74.25909,40.477399,-73.700009,40.917577]
    
    grid = SearchGrid(data_bounds,50,filter_poly=poly,proj_str=lcc).temporal_grid(data_start,data_end)

    try :
        es.indices.delete(index='test')
        es.indices.delete(index='nyc_grid')
    except :
        pass

    try:
        es.indices.create(index='nyc_grid')
    except:
        #do not try to recreate the index
        pass
    
    grid_mapping = {'grid':
                       {'properties':
                        {'grid_center':
                         {
                             'type':'geo_point',
                             'store':'yes'
                          },
                        'grid_boundary':
                         {
                             'type':'geo_shape',
                             'tree':'quadtree',
                             'precision': '1m'
                          }
                         }
                        }
                    }
    #define the mapping
    es.indices.put_mapping(index='nyc_grid', doc_type='grid', body=grid_mapping)


    succeeded=0
    failed=0
    for g in grid:
        try:
            es.index(index="nyc_grid", doc_type='grid', id=g['grid_id'], body=g)
            succeeded+=1
        except:
            failed+=1

    print "Temporal Grid defined. Took %s" % str(dt.now() - start_time)
    print "%s records uploaded. %s uploads failed." % (str(succeeded),str(failed))

