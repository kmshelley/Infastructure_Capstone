
# coding: utf-8

# # Serious Accident Prediction with PySpark and Sci-Kit Learn

from pyspark import SparkContext, SparkConf
conf = SparkConf().setAppName('sklearn_RandomForest')
sc = SparkContext(conf=conf)

from pyspark.sql import SQLContext
sqlContext = SQLContext(sc)


#Scikit learn libraries
#classifiers
from sklearn.ensemble import RandomForestClassifier

#metrics/cross-validation
from sklearn.cross_validation import cross_val_score
from sklearn.utils import shuffle

#Python Libraries
import sys
sys.path.append('../Infrastructure_Capstone')
import os
import random
from copy import deepcopy
from pyproj import Proj
import datetime as dt
from dateutil.parser import parse
import numpy as np
import pandas as pd

from elasticsearch import Elasticsearch
from elasticsearch import helpers
from elasticsearch.client import indices

import ConfigParser

#read in the config file
config = ConfigParser.ConfigParser()
config.read('../Infrastructure_Capstone/config/capstone_config.ini')

ES_url = config.get('ElasticSearch','host')
ES_hosts = config.get('ElasticSearch','hostlist')
ES_password = config.get('ElasticSearch','password')
ES_username= config.get('ElasticSearch','username')

print ES_hosts
seed = random.seed(0)


#Configuration for reading from and writing to Elasticsearch
es_read_conf = { 
        "es.resource" : "nyc_grid/rows", 
        "es.nodes" : ES_hosts,
        "es.port" : "9200",
        "es.net.http.auth.user" : ES_username,
        "es.net.http.auth.pass" : ES_password
    }

pred_read_conf = { 
        "es.resource" : "prediction_results/rows", 
        "es.nodes" : ES_hosts,
        "es.port" : "9200",
        "es.net.http.auth.user" : ES_username,
        "es.net.http.auth.pass" : ES_password
    }

pred_write_conf = {
        "es.resource" : "prediction_results/rows",
        "es.nodes" : ES_hosts,
        "es.port" : "9200",
        "es.net.http.auth.user" : ES_username, 
        "es.net.http.auth.pass" : ES_password,
        "es.mapping.id" : "grid_id"
    } 

feat_write_conf = {
        "es.resource" : "prediction_features/importance",
        "es.nodes" : ES_hosts,
        "es.port" : "9200",
        "es.net.http.auth.user" : ES_username, 
        "es.net.http.auth.pass" : ES_password,
        "es.mapping.id" : "Rank"
    }

# The following functions perform feature engineering and data preparation for reading and writing to/from the model

def trainingDict(row):
    #returns a dict of label and features for an ML algorithm
    #input: SparkSQL dataframe row element
    #output: Spark LabeledPoint for training
    #id field
    if 'median_speed_limit' in row:
        #only keep records with street information
        output = {}

        output['id'] = row['grid_id']

        #zip code
        output['zipcode'] = row['grid_zipcode']

        #date time fields
        output['dayOfMonth'] = long(row['grid_day'])
        output['dayOfWeek'] = long(row['grid_dayOfWeek'])
        output['hour'] = long(row['grid_hourOfDay'])
        output['month'] = long(row['grid_month'])

        #weather fields
        output['fog'] = row['weather_Fog_Dummy']
        output['rain'] = row['weather_Rain_Dummy']
        output['snow'] = row['weather_SnowHailIce_Dummy']

        output['temp'] = row['weather_WetBulbFarenheit']
        output['precip'] = row['weather_HourlyPrecip']
        #vis = row.weather_Visibility  ## NOT AVAILABLE IN WEATHER FORECAST
        output['windspeed'] = row['weather_WindSpeed']

        #zipcode-specific data
        output['zip_area'] = row['zip_area']
        output['speed5'] = row["5mph"]
        output['speed15'] = row["15mph"]
        output['speed25'] = row["25mph"]
        output['speed35'] = row["35mph"]
        output['speed45'] = row["45mph"]
        output['speed55'] = row["55mph"]
        output['speed65'] = row["65mph"]
        output['speed85'] = row["85mph"]
        output['med_speed'] = row["median_speed_limit"]
        output['road_len'] = row["total_road_length"]
        output['rd_count'] = row["total_road_count"]
        output['bridges'] = row["bridges"]
        output['tunnels'] = row["tunnels"]

        #road condition requests
        output['rd_cond'] = (row['road_cond_requests'] + 0.0)/output['road_len'] # road repair requests per mile of road

        #liquor licenses
        output['liquor'] = (row['liquor_licenses'] + 0.0)/output['zip_area'] # Number of liquor licenses per square mile

        #truth label
        output['label'] = 1 if (float(row['grid_totalInjuries'])>0 or float(row['grid_totalFatalities'])>0) else 0

        return output

def predictionDict(row):
    #returns a dict of label and features for an ML algorithm
    #input: SparkSQL dataframe row element
    #output: Spark LabeledPoint for training
    #id field
    if 'median_speed_limit' in row:
        output = {}

        output['id'] = row['grid_id']

        #zip code
        output['zipcode'] = row['grid_zipcode']

        #date time fields
        output['dayOfMonth'] = long(row['grid_day'])
        output['dayOfWeek'] = long(row['grid_dayOfWeek'])
        output['hour'] = long(row['grid_hourOfDay'])
        output['month'] = long(row['grid_month'])

        #weather fields
        output['fog'] = row['weather_Fog_Dummy']
        output['rain'] = row['weather_Rain_Dummy']
        output['snow'] = row['weather_SnowHailIce_Dummy']

        output['temp'] = row['weather_Temp']
        output['precip'] = row['weather_Precip']
        #vis = row.weather_Visibility  ## NOT AVAILABLE IN WEATHER FORECAST
        output['windspeed'] =  row['weather_WindSpeed']

        #zipcode-specific data
        output['zip_area'] = row['zip_area']
        output['speed5'] = row["5mph"]
        output['speed15'] = row["15mph"]
        output['speed25'] = row["25mph"]
        output['speed35'] = row["35mph"]
        output['speed45'] = row["45mph"]
        output['speed55'] = row["55mph"]
        output['speed65'] = row["65mph"]
        output['speed85'] = row["85mph"]
        output['med_speed'] = row["median_speed_limit"]
        output['road_len'] = row["total_road_length"]
        output['rd_count'] = row["total_road_count"]
        output['bridges'] = row["bridges"]
        output['tunnels'] = row["tunnels"]

        #road condition requests
        output['rd_cond'] = (row['road_cond_requests'] + 0.0)/output['road_len'] # road repair requests per mile of road

        #liquor licenses
        output['liquor'] = (row['liquor_licenses'] + 0.0)/output['zip_area'] # Number of liquor licenses per square mile

        return output
    
def update_probability(row):
    #inpur: key,val pair of id, es recrod dict, and predicted probability
    #updates probability in dict, returns (id,dict) pair
    (_id,(d,prob)) = row
    d['probability'] = prob
    return (_id,dict(d))

def conv_feat_rank(f):
    #converts the feature importance rating from a numpy float to a python native float
    try:
        f['Rating'] = f['Rating'].item()
    except:
        pass
    return f


# ### Get Evenly Distributed Training Data
# 
# The following cells build a DataFrame with approximately the same number of records with and without a serious accident (injury or fatality).


#get set of accident and non-accident records
#defines an ES query for accidents with injury or fatality
q = '''
    {
        "query": {
            "bool":{
                "should":[
                    { "range": 
                        { "grid_totalFatalities": { "gt": 0 } } 
                    },
                    { "range": 
                        { "grid_totalInjuries": { "gt": 0 } } 
                    }
                ]
            }
        }
    }
'''
acc_conf = deepcopy(es_read_conf)
acc_conf['es.query'] = q
#accidents = grid_rdd.filter(lambda row: row['grid_isAccident'] == 1)
accidents = sc.newAPIHadoopRDD(
            inputFormatClass="org.elasticsearch.hadoop.mr.EsInputFormat",
            keyClass="org.apache.hadoop.io.NullWritable", 
            valueClass="org.elasticsearch.hadoop.mr.LinkedMapWritable", 
            conf=acc_conf).map(lambda row: row[1])
accidents.cache()
accN = accidents.count()



q = '''
    {
        "query": {
            "bool":{
                "must":[
                    { "term": 
                        { "grid_totalFatalities": 0 } 
                    },
                    { "term": 
                        { "grid_totalInjuries": 0 } 
                    }
                ]
            }
        }
    }
'''

nonAcc_conf = deepcopy(es_read_conf)
nonAcc_conf['es.query'] = q
nonAccidents = sc.newAPIHadoopRDD(
            inputFormatClass="org.elasticsearch.hadoop.mr.EsInputFormat",
            keyClass="org.apache.hadoop.io.NullWritable", 
            valueClass="org.elasticsearch.hadoop.mr.LinkedMapWritable", 
            conf=nonAcc_conf).map(lambda row: row[1])
nonAccidents.cache()
nonAccN = nonAccidents.count()



print "%s out of %s grid elements have a serious accident." % (str(accN),str(nonAccN + accN))


# ### Training Data
# We will now randomly sample from the accidents and non-accidents RDD's to get approximately 50-50 accidents and non-accidents

fraction = 0.9

sub_acc = accidents.sample(withReplacement=True,fraction=fraction,seed=seed)
sub_noacc = nonAccidents.sample(withReplacement=True,fraction=fraction*accN/nonAccN,seed=seed)

print "Total accidents: %s" % str(sub_acc.count())
print "Total non-accidents: %s" % str(sub_noacc.count())
full_rdd = sub_acc.union(sub_noacc)
full_rdd.cache()



df = pd.DataFrame(full_rdd.map(trainingDict).filter(lambda row: row).collect())
df.head(3)


Y = np.array(df['label']) #extract the labels
X = df.drop(['id','label'],1).as_matrix()

X, Y = shuffle(X,Y,random_state=0) #shuffle the labels and features

print X.shape
print Y.shape


# ### Train and Run the Model
# We will use scikit-learn's Random Forest model and use 5-fold cross validation to validate the accuracy of the model.

#Let's check an anticipated accuracy using 5-fold cross-validation
rf = RandomForestClassifier(n_estimators=25,min_samples_split=20,max_features=None)#,class_weight={0:0.98,1:0.02})
rf.fit(X,Y)
print 'Average accuracy: %s' % (np.mean(cross_val_score(rf,X,Y,cv=5))*100)

# Get the feature importance ratings from the RF model
importances = rf.feature_importances_
indices = np.argsort(importances)[::-1]

features = df.drop(['id','label'],1).columns.values

feature_idx = []
for f in range(X.shape[1]):
    feature_idx.append({
            'Rank': f+1,
            'Feature': df.columns.values[f],
            'Rating': importances[indices[f]]
        })

#Write the feature importance ratings to Elasticsearch
feature_ranks = sc.parallelize(feature_idx).map(lambda feat: (feat['Rank'],conv_feat_rank(feat)))
feature_ranks.saveAsNewAPIHadoopFile(
        path='-', 
        outputFormatClass="org.elasticsearch.hadoop.mr.EsOutputFormat",
        keyClass="org.apache.hadoop.io.NullWritable", 
        valueClass="org.elasticsearch.hadoop.mr.LinkedMapWritable", 
        conf=feat_write_conf)


# ## Generate Future Predictions
# 
# Now we will use the predictions Elasticsearch Index to create predictions for the next 10 days, and push the results back up to Elasticsearch.

predictions = sc.newAPIHadoopRDD(
            inputFormatClass="org.elasticsearch.hadoop.mr.EsInputFormat",
            keyClass="org.apache.hadoop.io.NullWritable", 
            valueClass="org.elasticsearch.hadoop.mr.LinkedMapWritable", 
            conf=pred_read_conf).map(lambda row: row[1])
predictions.cache()


df_pred = pd.DataFrame(predictions.map(predictionDict).filter(lambda row: row).collect())
df_pred.head(3)

ids = np.array(df_pred['id']) #extract the ids
X_pred = df_pred.drop(['id'],1).as_matrix()


#predict the probabilities, match id's with probability of serious collision (1) (convert probability from numpy.float to python native)
probabilities = sc.parallelize(zip(list(ids),list(rf.predict_proba(X_pred)))).map(lambda (k,v): (k,v[1].item()))
probabilities.take(3)


# ### Write predictions to Elasticsearch
# 
# Write the RF predictions back to the SafeRoad results Elasticsearch index.
predWithProb = predictions.map(lambda row: (row['grid_id'],row)).join(probabilities).map(update_probability)
predWithProb.saveAsNewAPIHadoopFile(
        path='-', 
        outputFormatClass="org.elasticsearch.hadoop.mr.EsOutputFormat",
        keyClass="org.apache.hadoop.io.NullWritable", 
        valueClass="org.elasticsearch.hadoop.mr.LinkedMapWritable", 
        conf=pred_write_conf)

