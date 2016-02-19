from classes.SearchGrid import SearchGrid
from dataAcquisition import acquire_QCLCD_data, acquire_NYC_Collisions, address_geocoding, mta_status
from dataStorage import upload_to_Elasticsearch

__author__ = 'Katherine'

if __name__ == '__main__':
    '''Main Entry Point to the Program'''

    # Upload Weather Stations to ElasticSearch #
##    geofile = 'E:/GoogleDrive/DataSciW210/Final/datasets/NY_Area_WBAN_Stations.geojson'
##    import geojson
##    with open(geofile,'r') as geo:
##        stations = geojson.load(geo)['features']
##    upload_to_Elasticsearch.upload_docs_to_ES(stations,'weather_stations','weather_stations','WBAN','loc')

    
    #import geojson
    #stations = address_geocoding.geojson_from_address_file('E:/GoogleDrive/DataSciW210/Final/datasets/NYC_EMS_Locations.csv','Address')
    #with open('E:/GoogleDrive/DataSciW210/Final/datasets/NYC_EMS_Locations.json','w') as geo:
    #    geojson.dump(stations,geo)
        
    #upload_to_Elasticsearch.upload_docs_to_ES(stations,'emergency_stations','fdny','FacilityName','loc')

    # Weather data collection
    ###CHANGE THESE FIELDS###
##    months = range(12,11,-1)
##    years = range(2011,2010,-1)
##    acquire_QCLCD_data.collect_and_store_weather_data(months,years)
    ######

    #collision data processing and upload
##    collisions = 'E:/GoogleDrive/DataSciW210/Final/datasets/collisions.csv'
    #collisions = '../../colSmall.csv'
    #collisions = '../../collisions.csv'
    #acquire_NYC_Collisions.upload_collision_data(collisions,index="saferoad",doc_type="collisions")

    #define the grid of NYC, upload to ES
    import datetime as dt
    start = dt.datetime(2012,6,30,17,0)
    end = dt.datetime(2016,2,5,16,0)
     
    bounds = [-74.25909,40.477399,-73.700009,40.917577]
    grid = SearchGrid(bounds,50).temporal_grid(start,end)
    upload_to_Elasticsearch.upload_docs_to_ES(grid,index='nyc_grid_complete',doc_type='grid',geopoint='grid_center',geoshape='grid_boundary')
    
    
