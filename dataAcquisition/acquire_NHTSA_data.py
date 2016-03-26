__author__='Katherine'
import os
import zipfile
from ftplib import FTP
import urllib
import geojson
from sas7bdat import SAS7BDAT
import pandas as pd
from datetime import datetime as dt

NHTSA_ftp = 'ftp.nhtsa.dot.gov'
#make temp directory for debug file
try:
    os.makedirs(os.path.join(os.getcwd(),'tmp'))
except:
    pass

def convert_SAS_to_df(filename):
    with SAS7BDAT(filename) as f:
        return f.to_data_frame()

def extract_GeoJSON(row,lat_field,lng_field,timestamp=False):
    #input: FARS DataFrame row, name of latitude field, longitude field, and (optional) time field
    #output: GeoJSON Point Feature with (optional) time property
    pt = geojson.Point((row[lng_field],row[lat_field]))
    props = {}
    if timestamp:props['timestamp'] = timestamp
    return geojson.Feature(geometry=pt,properties=props)

def download_NHTSA_data(year,type='FARS'):
    debug = open(os.path.join(os.getcwd(),'tmp','debug.txt'),'a')
    #input: year to download data, type of file 'FARS' or 'GES'
    if type.lower()=='fars':
        filename = 'FARS%s.zip' % str(year)
        outfile = os.path.join(os.getcwd(),'tmp',filename)
    
        #FARS data have a couple different file names, try downloading both
        try:
            url = 'ftp://%s/%s/%s/SAS/FARS%s.zip' % (NHTSA_ftp,type.upper(),str(year),str(year))
            urllib.urlretrieve(url,outfile)
        except:
            try:
                url = 'ftp://%s/%s/%s/SAS/FSAS%s.zip' % (NHTSA_ftp,type.upper(),str(year),str(year))
                urllib.urlretrieve(url,outfile)
            except:
                debug.write("%s ERROR: Could not find FARS file for %s\n" % (dt.now(),year))
                outfile = False
                pass


                
    elif type.lower()=='ges':
        filename = 'GES%s.zip' % str(year)
        outfile = os.path.join(os.getcwd(),'tmp',filename)
                 
        ftp = FTP(NHTSA_ftp)
        ftp.login()
        ftp.cwd('GES/GES' + str(year)[2:])
        
        for filename in ftp.nlst():
            if filename.lower().split('.')[-1] == 'zip' and filename.lower().find('flat')==-1:
                 #if the file is a zipped file and not the "flat" version, download it
                 try:
                     url = 'ftp://%s/GES/%s%s/%s' % (NHTSA_ftp,type.upper(),str(year)[2:],filename)
                     urllib.urlretrieve(url,outfile)
                 except Exception as e:
                    print e
                    debug.write("%s ERROR: Could not find GES file for %s\n" % (dt.now(),year))
                    outfile = False
                    pass
            else:
                 debug.write("%s ERROR: Could not find GES file for %s\n" % (dt.now(),year))
                 outfile = False
                 
    debug.close()
    return outfile


def acquire_and_clean_NHSTA_data(year,type='FARS'):
    #outfile = download_NHTSA_data(year,type)
    outfile ='E:\\GoogleDrive\\DataSciW210\\Final\\Infastructure_Capstone\\dataAcquisition\\tmp\\FARS2014.zip'
    if os.path.isfile(outfile) and zipfile.is_zipfile(outfile):
        #if the url passed to the def exists and is a valid zip file
        #added for Linux (was creating an empty file for non-existent url downloads)
        z = zipfile.ZipFile(outfile)
        for f in z.namelist():
            #convert each SAS file to a pandas data frame
            if f.lower().split('.')[-1] == 'sas7bdat':
                unzipped = z.extract(f) #extract the file
                df = convert_SAS_to_df(unzipped) #convert to pandas dataframe
                os.remove(unzipped) #remove extracted file

                ###KASANE'S ES CODE
                ## 
                ##
                ##
                ###


                ##This is the code for extracting geojson point records from the ACCIDENT file
##                points = []
##
##                if type.lower()=='fars' and f == 'accident.sas7bdat':
##                #Extract location and timestamp out of ACCIDENT file and load into GeoJSON point feature collection
##                    print df.columns
##                    for row_tup in df.iterrows():
##                        row = row_tup[1]
##                        timestamp = dt(int(row['YEAR']),int(row['MONTH']),int(row['DAY']),int(row['HOUR']),int(row['MINUTE']))
##                        points.append(extract_GeoJSON(row,'LATITUDE','LONGITUD',timestamp))
##                        
##                pointsfile = f.split('.')[0] + '_geojson.json' #filename for GEOJSON file
##                geojson.dump(geojson.FeatureCollection(points),os.path.join(os.getcwd(),'tmp',pointsfile))
     
                        
                    



