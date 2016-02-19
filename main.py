from classes.SearchGrid import SearchGrid
from dataAcquisition import acquire_QCLCD_data, acquire_NYC_Collisions, address_geocoding, mta_status
from dataAnalysis import driving_distance
from dataStorage import upload_to_Elasticsearch
import geojson

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

    #since we are just testing, delete index everytime this is run
    try :
        es.indices.delete(index='nyc_grid', ignore=400)
    except :
        pass


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
    data_bounds = [-74.25909,40.477399,-73.700009,40.917577]
    
    grid = SearchGrid(data_bounds,50,filter_poly=poly).temporal_grid()
    index = 0
    docs=[]
    for g in grid:
        index+=1
        docs.append(g)
        if index == 10000:
            #upload 10k grid records at a time
            upload_to_Elasticsearch.upload_docs_to_ES(docs,index='nyc_grid',doc_type='grid',geopoint='grid_center',geoshape='grid_boundary')
            index = 0
            docs=[]
    #upload remainder of grid
    upload_to_Elasticsearch.upload_docs_to_ES(docs,index='nyc_grid',doc_type='grid',geopoint='grid_center',geoshape='grid_boundary')
    print "Temporal Grid defined. Took %s" % str(dt.now() - start_time)

