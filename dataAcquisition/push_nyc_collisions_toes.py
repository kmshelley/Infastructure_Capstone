__author__='Katherine'

#For now, this will upload a static file to ES 
#Big ToDo: support downloading  file everyday and add only new data to ES

import os
import geojson
from datetime import datetime as dt
import sys
import csv
import json
from datetime import datetime
from elasticsearch import Elasticsearch,helpers
import pandas as pd



def upload_docs_to_ES(docs,index,type):
    #input: list of JSON documents
    #uploads each feature element to ElasticSearch
    actions = []
    #build the list of ElasticSearch uploads for bulk command
    for doc in docs:
        action = {
            "_index": index,
            "_type": type,
            "_source": doc
            }
        actions.append(action)
    try:
        helpers.bulk(es, actions)
        print "Sucessfully uploaded %s records!" % str(len(actions))
    except Exception as e:
        print '#### ERROR:s'
        print(e)

#First argument is ES username
#Second argument is ES password
#Thrid arg is server IP
#es = ElasticSearch('http://' + sys.argv[3] + ':9200/',username=sys.argv[1], password=sys.argv[2])
es = Elasticsearch(['http://' + sys.argv[1] + ':' + sys.argv[2] + '@' + sys.argv[3] + ':9200/'])

indexName = "saferoad" #this is like a group of tables under common theme
typeName = "collisions" #this is like table name

#since we are just testing, delete index everytime this is run
try :
    es.indices.delete(index=indexName, ignore=400)
except :
    pass

es.indices.create(index=indexName, ignore=400)
#es.index(index=indexName, doc_type=typeName, id=42, body={"any": "data", "timestamp": datetime.now()})

#with open('colSmall.csv') as csvfile:

with open('collisions.csv') as csvfile:  
	csv_reader = csv.DictReader(csvfile)
   
    	records = []
 
    	for row in csv_reader:
        
        	#add GEOJSON if lat and long exists
        	if row['LATITUDE'] != "" and row['LONGITUDE'] != "":
            		#row["GEOJSON_C"] = geojson.Feature(geometry=geojson.Point((float(row['LONGITUDE']),float(row['LATITUDE']))))
	
			row["GEOJSON_C"] = { "lat" : float(row['LATITUDE']), "lon": float(row["LONGITUDE"]) }
    		#convert date and time into one variable
        	row["DATETIME_C"] = datetime.strptime(row["DATE"] + " " + row["TIME"]+ ':00', "%m/%d/%Y %H:%M:%S") 
        
        	records.append(row)

	upload_docs_to_ES(records,indexName,typeName) 
	#helpers.bulk(es,list_records)

