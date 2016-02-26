__author__='Kasane'

#For now, this will upload a static file to ES 
#Big ToDo: support downloading  file everyday and add only new data to ES

import os
import hashlib
import geojson
from datetime import datetime as dt
import sys
import csv
from datetime import datetime
from elasticsearch import Elasticsearch,helpers
from dataStorage import upload_to_Elasticsearch


def upload_collision_data(flatfile,index,doc_type):
    with open(flatfile) as csvfile:  
        csv_reader = csv.DictReader(csvfile)
        records = []
        for row in csv_reader:
                #add GEOJSON if lat and long exists
                if row['LATITUDE'] != "" and row['LONGITUDE'] != "":
                        row["GEOJSON_C"] = (float(row["LONGITUDE"]),float(row['LATITUDE']))
			#row["GEOJSON_C"] = { "lat" : float(row['LATITUDE']), "lon": float(row["LONGITUDE"]) }
                #convert date and time into one variable
                row["DATETIME_C"] = datetime.strptime(row["DATE"] + " " + row["TIME"]+ ':00', "%m/%d/%Y %H:%M:%S")
                #assign a unique id based on date, time, and location
                row["ID"] = hashlib.sha224(row["DATE"] + row["TIME"] + row["LATITUDE"] + row["LONGITUDE"]).hexdigest()

		# append "collision_" in front of each column
		#print row
		newrow = dict()
		for key, value in row.iteritems():
			newrow["collision_" + key] = value
                #print newrow
		
			
	  	records.append(newrow)	

        upload_to_Elasticsearch.upload_docs_to_ES(records,index,doc_type,id_field="collision_ID",geopoint="collision_GEOJSON_C") 
