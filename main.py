from classes.SearchGrid import SearchGrid
from dataAcquisition import acquire_QCLCD_data, acquire_NYC_Collisions, address_geocoding
from dataAnalysis import driving_distance
from dataStorage import upload_to_Elasticsearch
from dataCleaning import filter_shapefile
import geojson,sys

__author__ = 'Katherine'

if __name__ == '__main__':
    '''Main Entry Point to the Program'''

    # Upload Weather Stations to ElasticSearch #
##    geofile = './flatDataFiles/NY_Area_WBAN_Stations.json'
##    with open(geofile,'r') as geo:
##        stations = geojson.load(geo)['features']
##    upload_to_Elasticsearch.bulk_upload_docs_to_ES_cURL(stations,index='weather_stations',doc_type='wban',delete_index=True,id_field='WBAN',geopoint='loc')

    #   Upload weather data to Elasticsearch   #
    ###CHANGE THESE FIELDS###
##    months = range(12,0,-1)
##    years = range(2016,2010,-1)
##    acquire_QCLCD_data.collect_and_store_weather_data(months,years)
    ######


    #   Upload EMS/FDNY station data to Elasticsearch   #
    #import geojson
    #stations = address_geocoding.geojson_from_address_file('E:/GoogleDrive/DataSciW210/Final/datasets/NYC_EMS_Locations.csv','Address')
    #with open('E:/GoogleDrive/DataSciW210/Final/datasets/NYC_EMS_Locations.json','w') as geo:
    #    geojson.dump(stations,geo)
        
    #upload_to_Elasticsearch.upload_docs_to_ES(stations,'emergency_stations','fdny','FacilityName','loc')

    from classes.SearchGrid import SearchGrid
from dataAcquisition import acquire_QCLCD_data, acquire_NYC_Collisions, address_geocoding
from dataAnalysis import driving_distance
from dataStorage import upload_to_Elasticsearch
import geojson,sys

__author__ = 'Katherine'

if __name__ == '__main__':
    '''Main Entry Point to the Program'''

    # Upload Weather Stations to ElasticSearch #
##    geofile = './flatDataFiles/NY_Area_WBAN_Stations.json'
##    with open(geofile,'r') as geo:
##        stations = geojson.load(geo)['features']
##    upload_to_Elasticsearch.bulk_upload_docs_to_ES_cURL(stations,index='weather_stations',doc_type='wban',delete_index=True,id_field='WBAN',geopoint='loc')

    #   Upload weather data to Elasticsearch   #
    ###CHANGE THESE FIELDS###
    #months = range(12,0,-1)
    #years = range(2016,2010,-1)
    #acquire_QCLCD_data.collect_and_store_weather_data(months,years)
    ######


    #   Upload streetmap data to Elasticsearch   #
##    from pyproj import Proj
##    p = Proj(init='epsg:2030')
##    with open('./flatDataFiles/NYC_streets_complete.json','r') as geo:
##        streets = filter_shapefile.street_map_centers(geojson.load(geo),p)
##
##    with open('./flatDataFiles/NYC_streets_centers.json','w') as geo:
##        geojson.dump(streets,geo)

    #complete street segments
##    with open('./flatDataFiles/NYC_streets_complete.json','r') as geo:
##        streets = filter_shapefile.street_map_centers(geojson.load(geo),p) 
##    upload_to_Elasticsearch.bulk_upload_docs_to_ES_cURL(streets,index='nyc_streets',doc_type='complete_segments',geoshape='points',shape_type='linestring')

    #complete street segments
    with open('./flatDataFiles/NYC_streets_starts.json','r') as geo:
        streets = filter_shapefile.street_map_centers(geojson.load(geo),p) 
    upload_to_Elasticsearch.bulk_upload_docs_to_ES_cURL(streets,index='nyc_street_start',doc_type='start_points',geopoint='coords')

    #complete street segments
    with open('./flatDataFiles/NYC_streets_centers.json','r') as geo:
        streets = filter_shapefile.street_map_centers(geojson.load(geo),p) 
    upload_to_Elasticsearch.bulk_upload_docs_to_ES_cURL(streets,index='nyc_street_center',doc_type='center_points',geopoint='coords')

