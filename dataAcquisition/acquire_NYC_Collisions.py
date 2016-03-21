__author__='Kasane'

#For now, this will upload a static file to ES 
#Big ToDo: support downloading  file everyday and add only new data to ES

import os
import geojson
import datetime as dt
import sys
import csv
from dateutil.parser import parse
from elasticsearch import Elasticsearch,helpers
from dataStorage import upload_to_Elasticsearch
from dataCleaning import add_ZCTA

def upload_collision_data_from_socrata(docs,index,doc_type,new_mapping=False):
    #input: list of collisions documents
    #output: updates collisions index with new documents
    records = []
    for row in docs:
        row = {(' ').join(k.upper().split('_')):row[k] for k in row} #fix the field names from socrata
        #Only store data in ES if it has lat/lon data
        if 'LATITUDE' in row and 'LONGITUDE' in row and row['LATITUDE'] != "" and row['LONGITUDE'] != "":
            row["GEOJSON_C"] = (float(row["LONGITUDE"]),float(row['LATITUDE']))
            row["GEOSHAPE_C"] = { "type": "point", "coordinates": [float(row["LONGITUDE"]),float(row['LATITUDE'])]}
            row['LOCATION'] = '(%s,%s)' % (row['LOCATION']['latitude'],row['LOCATION']['longitude'])
            #convert date and time into one variable
            coll_date = parse(row['DATE']).replace(tzinfo=None)
            coll_time = parse(row['TIME']).replace(tzinfo=None)
            row["DATETIME_C"] = dt.datetime.strftime(coll_date + dt.timedelta(hours=coll_time.hour,seconds=coll_time.minute*60), "%Y-%m-%dT%H:%M:%S")
            #assign a unique id based on date, time, and location
            #row["ID"] = hashlib.sha224(dt.datetime.strftime(coll_date,"%m/%d/%Y") + dt.datetime.strftime(coll_time,"%H:%M:%S") + row["LATITUDE"] + row["LONGITUDE"]).hexdigest()

            # append "collision_" in front of each column
            #print row
            newrow = dict()
            for key, value in row.iteritems():
                newrow["collision_" + key] = value

            #add ZCTA zip code fields
            newrow = add_ZCTA.add_zcta_zip_to_collision_rec(newrow)
            newrow['collision_ZCTA_ZIP_NoSuffix'] =  newrow['collision_ZCTA_ZIP'].split('-')[0]
                    
            records.append(newrow)	

    if new_mapping:
        #if this is a new index, use function that creates the mapping
        upload = {'index':index,'doc_type':doc_type,'id_field':'collision_UNIQUE KEY','geopoint':'collision_GEOJSON_C','geoshape':'collision_GEOSHAPE_C'}
        upload_to_Elasticsearch.bulk_upload_docs_to_ES_cURL(records,**upload)
    else:
        #update existing index
        upload = {'index':index,'doc_type':doc_type,'id_field':'collision_UNIQUE KEY'}
        upload_to_Elasticsearch.update_ES_records_curl(records,**upload) 




def upload_collision_data_from_flatfile(docs,index,doc_type,new_mapping=False):
    #input: list of collisions documents
    #output: updates collisions index with new documents
    records = []
    for row in docs:
        #Only store data in ES if it has lat/lon data
        if row['LATITUDE'] != "" and row['LONGITUDE'] != "":
            row["GEOJSON_C"] = (float(row["LONGITUDE"]),float(row['LATITUDE']))
            row["GEOSHAPE_C"] = { "type": "point", "coordinates": [float(row["LONGITUDE"]),float(row['LATITUDE'])] }
            #convert date and time into one variable
            coll_date = parse(row['DATE']).replace(tzinfo=None)
            coll_time = parse(row['TIME']).replace(tzinfo=None)
            row["DATETIME_C"] = dt.datetime.strftime(coll_date + dt.timedelta(hours=coll_time.hour,seconds=coll_time.minute*60), "%Y-%m-%dT%H:%M:%S")
            #row["DATETIME_C"] = dt.datetime.strptime(row["DATE"] + " " + row["TIME"]+ ':00', "%m/%d/%Y %H:%M:%S")
            #assign a unique id based on date, time, and location
            #row["ID"] = hashlib.sha224(row["DATE"] + row["TIME"] + row["LATITUDE"] + row["LONGITUDE"]).hexdigest()

            # append "collision_" in front of each column
            #print row
            newrow = dict()
            for key, value in row.iteritems():
                    newrow["collision_" + key] = value

            #add ZCTA zip code fields
            newrow = add_ZCTA.add_zcta_zip_to_collision_rec(newrow)
            #newrow['collision_ZCTA_ZIP_NoSuffix'] =  newrow['collision_ZCTA_ZIP'].split('-')[0]
            
            records.append(newrow)	

    if new_mapping:
        #if this is a new index, use function that creates the mapping
        upload = {'index':index,'doc_type':doc_type,'id_field':'collision_UNIQUE KEY','geopoint':'collision_GEOJSON_C','geoshape':'collision_GEOSHAPE_C'}
        upload_to_Elasticsearch.bulk_upload_docs_to_ES_cURL(records,**upload)
    else:
        #update existing index
        upload = {'index':index,'doc_type':doc_type,'id_field':'collision_UNIQUE KEY'}
        upload_to_Elasticsearch.update_ES_records_curl(records,**upload) 
