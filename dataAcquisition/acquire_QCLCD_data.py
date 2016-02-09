#-------------------------------------------------------------------------------
# Name:        acquire_QCLCD_data.py
# Purpose:     Downloads weather data from NOAA into MongoDB
#
# Author:      Katherine Shelley
#
# Created:     3/16/2015
#-------------------------------------------------------------------------------

import urllib
import os
import zipfile
import csv
import pprint
import datetime as dt
import sys
import contextlib
import ast
import pandas as pd
import geojson

QCLCD_url = 'http://www.ncdc.noaa.gov/orders/qclcd/'
#http://cdo.ncdc.noaa.gov/qclcd_ascii/199607.tar.gz <- filename format before 7/2007

def clean_up_files():
    import glob
    try:
        if os.path.isfile(os.path.join(temp_data_dir,'wbanmasterlist.psv.zip')):
            #if the WBAN file exists, delete it
            os.remove(os.path.join(temp_data_dir,'wbanmasterlist.psv.zip'))
        #get a list of existing weather observation files
        weather_files = glob.glob(temp_data_dir + '/QCLCD*')
        for file in weather_files: os.remove(file)
    except:
        "Error deleting weather files!"
        return

def download_QCLCD_data(url,filename):
    #input: URL of NOAA QCLCD zipped file
    
    outFilePath = os.path.join(temp_data_dir,filename)
    month = filename[5:-4]

    urllib.urlretrieve(url + filename,outFilePath)
    return outFilePath
    

def extract_hourly_records(outFilePath):
    #input: Location of zipped NOAA QCLCD data file
    #output: Pandas dataframe of weather records
    if os.path.isfile(outFilePath) and zipfile.is_zipfile(outFilePath):
        #if the url passed to the def exists and is a valid zip file
        #added for Linux (was creating an empty file for non-existent url downloads)
        z = zipfile.ZipFile(outFilePath)
        
        for f in z.namelist():
            ##LOAD HOURLY WEATHER DATA INTO PANDAS DATAFRAME
            if f.find('hourly.txt') > -1:
                #get observation info
                with contextlib.closing(z.open(f,'r')) as hourlyFile:
                    df = pd.read_csv(hourlyFile,parse_dates=[[1,2]],keep_date_col=True,low_memory=False)
                    #df['_id'] = '%s_%s_%s' % (df['WBAN'],df['Date'],df['Time']) #custom id

        z.close()
        return df #return the weather observation dataframe

def extract_station_records(outFilePath,station_file=None):
    #input: Location of zipped NOAA QCLCD data file
    #output: geojson feature collection of weather station locations, updates existing station geojson file if passed
    if os.path.isfile(outFilePath) and zipfile.is_zipfile(outFilePath):
        #if the url passed to the def exists and is a valid zip file
        #added for Linux (was creating an empty file for non-existent url downloads)
        z = zipfile.ZipFile(outFilePath)
        
        for f in z.namelist():
            ##UPDATE THE WEATHER STATION INFORMATION       
            if f.find('station.txt') > -1:
                csv.register_dialect('WBAN_dialect', delimiter='|') #WBAN file is PSV
                #update station info
                with contextlib.closing(z.open(f,'r')) as stationFile:
                    csv_dict = csv.DictReader(stationFile,dialect='WBAN_dialect') #read the PSV as list of row dicts
                    curr_stations = {} #dict of current stations
                    for row in csv_dict:
                        decode_row = {}
                        for k in row: decode_row[k] = row[k].decode('utf-8','ignore') #decode text, I was getting utf-8 errors without this
                        #create geojson FeatureCollection of stations
                        curr_stations[row['WBAN']] = geojson.Feature(geometry=geojson.Point((float(row['Longitude']),float(row['Latitude']))),properties=row)
                if os.path.isfile(station_file):
                    with open(station_file,'r')as geo:
                        station_geojson = geojson.load(geo)
                        
                    for station in station_geojson['features']:
                        _id = station['properties']['WBAN']
                        if _id in curr_stations:
                            #update the station geojson feature information with stations listed in the current file
                            station['geometry'] = curr_stations[_id]['geometry']
                            station['properties'] = curr_stations[_id]['properties']
                else:
                    #If the stations geojson feature collection doesn't already exist, create it from the current stations dict
                    station_geojson = geojson.FeatureCollection([curr_stations[wban] for wban in curr_stations]) 
                    
##                with open(station_file,'w')as geo:
##                     geojson.dump(station_geojson,geo)
        z.close()
        return station_geojson #return the weather observation dataframe
    
def collect_and_store_weather_data(months=range(2,0,-1),years=range(2016,2015,-1)):
    #input: list of months and years
    #output: downloads and extracts hourly weather observations and WBAN station location information
    try:
        total_start = dt.datetime.now()

        for year in years:
            for month in months:
                local_start = dt.datetime.now()
                
                #download monthly zipped file
                qclcd = download_QCLCD_data(QCLCD_url,'QCLCD%04d%02d.zip' % (year,month))
                #get hourly weather records for all stations
                df = extract_hourly_records(qclcd)
                ## OPTIONAL ## write df to CSV
                df.to_csv(os.path.join(temp_data_dir,'QCLCD%04d%02d.csv' % (year,month)),index=False) #write out the CSV

                #get station locatio information
                geo_file = os.path.join(temp_data_dir,'WBAN_stations.json')
                stations = extract_station_records(qclcd,geo_file)
                ## OPTIONAL ## write the geojson file to flat file
                with open(geo_file,'w') as geo:
                    geojson.dump(stations,geo)

                
                
                
                ####KASANE: This is where we would load the data into ElasticSearch. My code updates a flat GeoJSON file for the
                ## station locations, but we will possibly want to push that to ElasticSearch as well.
                ####



                os.remove(qclcd)
                
                print "Finished collecting weather data for %04d%02d." % (year,month)
                print "Total Runtime: %s " % (dt.datetime.now() - local_start)
        print "Finished!\nTotal Run Time: %s " % (dt.datetime.now() - total_start)

    except Exception as e:
        print "#####ERROR: %s" % e

###CHANGE THESE FIELDS###
temp_data_dir = 'E:/GoogleDrive/DataSciW210/Final/datasets'
months = range(2,0,-1)
years = range(2016,2015,-1)
######

collect_and_store_weather_data(months,years)
