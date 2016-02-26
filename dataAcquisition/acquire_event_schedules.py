
import zipfile
import urllib
import os
import contextlib
import pandas as pd
import numpy
from elasticsearch import Elasticsearch,helpers
from upload_to_Elasticsearch import upload_docs_to_ES



def get_baseballSchedule(year):
    #input: URL of NOAA QCLCD zipped file
    
    url = "http://www.retrosheet.org/gamelogs/gl%s.zip"
    
    outFilePath = os.path.join("temp","temp" + str(year) + ".zip")
 
    urllib.urlretrieve((url % year),outFilePath)
    #return outFilePath

    z = zipfile.ZipFile(outFilePath)
    for f in z.namelist():
        print f
        with contextlib.closing(z.open(f,'r')) as gameschedules:
            
            
            #load into numpy
            #df = numpy.loadtxt(gameschedules,delimiter=",",skiprows=0,dtype="str")
            #dictionar here http://www.retrosheet.org/gamelogs/glfields.txt
            #0 - date, 1 - number of game, 2 - Day of the week, 11 - length of game in outs, 12 - DAy/Night indicator, 
            #16 - stadisum, 17 - attendance count
          
            df = pd.read_csv(gameschedules,parse_dates=[0],low_memory=False,header=None, usecols=[0,1,2,11,12,16,17])
            df.columns = ["EventDate", "EventGameNumber", "EventDayOfWeek", 
                          "EventLengthOfGame", "EventDayOrNightGame", "EventStadium", "EventAttendance"]
            
            #filter out all of the other stadiums. 
            stadiums = {"NYC20": (-73.9264,40.8292) ,"NYC21": (-73.846043,40.756337)}
            df = df[df['EventStadium'].isin(stadiums.keys())]
            
            #add latlong
            df["EventLongLat"] = ""
            
            #df.ix[df.EventStadium==stadiums.keys()[0], "EventLongLat"] = stadiums[stadiums.keys()[0]]
            
            for index, row in df.iterrows():
                df.set_value(index,"EventLongLat",stadiums[stadiums.keys()[0]] if row["EventStadium"] == stadiums.keys()[0] \
                                        else stadiums[stadiums.keys()[1]])
                #df.loc[index, "EventLongLat"] = stadiums[stadiums.keys()[0]] if row["EventStadium"] == stadiums.keys()[0] \
                #                        else stadiums[stadiums.keys()[1]]
                
            eventsArray = []
            
            for index, row in df.iterrows():
                eventsArray.append(row.to_dict())
            
    upload_docs_to_ES(eventsArray,"events","public", "", geopoint="EventLongLat") 
                        
    os.remove(outFilePath)

get_baseballSchedule(2015)
