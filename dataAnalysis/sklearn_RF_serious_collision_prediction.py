# # Serious Accident Prediction with PySpark and Sci-Kit Learn
from pyspark import SparkContext, SparkConf
conf = SparkConf().setAppName('sklearn_RandomForest_serious_collisions')
sc = SparkContext(conf=conf)

from pyspark.sql import SQLContext
sqlContext = SQLContext(sc)


#Scikit learn libraries
#classifiers
from sklearn.neighbors import KNeighborsClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.ensemble import RandomForestClassifier, ExtraTreesClassifier

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

data_grid = config.get('indexes','grid')
results = config.get('indexes','results')
update = config.get('indexes','pred_update')
features = config.get('indexes','features')

seed = random.seed(0)

#Configuration for reading from and writing to Elasticsearch
es_read_conf = { 
        "es.resource" : data_grid, 
        "es.nodes" : ES_hosts,
        "es.port" : "9200",
        "es.net.http.auth.user" : ES_username,
        "es.net.http.auth.pass" : ES_password
    }

pred_read_conf = { 
        "es.resource" : results, 
        "es.nodes" : ES_hosts,
        "es.port" : "9200",
        "es.net.http.auth.user" : ES_username,
        "es.net.http.auth.pass" : ES_password
    }

pred_write_conf = {
        "es.resource" : results,
        "es.nodes" : ES_hosts,
        "es.port" : "9200",
        "es.net.http.auth.user" : ES_username, 
        "es.net.http.auth.pass" : ES_password,
        "es.mapping.id" : "grid_id"
    } 

feat_write_conf = {
        "es.resource" : features,
        "es.nodes" : ES_hosts,
        "es.port" : "9200",
        "es.net.http.auth.user" : ES_username, 
        "es.net.http.auth.pass" : ES_password,
        "es.mapping.id" : "id"
    } 


# ### Training Points
# 
# The following will build the feature arrays for the ML algorithm.

def trainingDict(row):
    #returns a dict of label and features for an ML algorithm
    #input: SparkSQL dataframe row element
    #output: Dict of label and features (1=any injury/fatality 0=no injury/fatality) for training
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

def ped_trainingDict(row):
    #returns a dict of label and features for an ML algorithm
    #input: SparkSQL dataframe row element
    #output: Dict of label (1=pedestrian injury/fatality 0=no ped injury/fatality)
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
        output['label'] = 1 if (float(row['grid_pedestrianInjuries'])>0 or float(row['grid_pedestrianFatalities'])>0) else 0

        return output
    
def cyc_trainingDict(row):
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
        output['label'] = 1 if (float(row['grid_cyclistInjuries'])>0 or float(row['grid_cyclistFatalities'])>0) else 0

        return output
    
def mot_trainingDict(row):
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
        output['label'] = 1 if (float(row['grid_motoristInjuries'])>0 or float(row['grid_motoristFatalities'])>0) else 0

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
    
def update_all_probability(row):
    #inpur: key,val pair of id, es recrod dict, and predicted probability
    #updates probability in dict, returns (id,dict) pair
    (_id,(d,prob)) = row
    d['all_probability'] = prob
    return (_id,dict(d))

def update_ped_probability(row):
    #inpur: key,val pair of id, es recrod dict, and predicted probability
    #updates probability in dict, returns (id,dict) pair
    (_id,(d,prob)) = row
    d['pedestrian_probability'] = prob
    return (_id,dict(d))

def update_cyc_probability(row):
    #inpur: key,val pair of id, es recrod dict, and predicted probability
    #updates probability in dict, returns (id,dict) pair
    (_id,(d,prob)) = row
    d['cyclist_probability'] = prob
    return (_id,dict(d))

def update_mot_probability(row):
    #inpur: key,val pair of id, es recrod dict, and predicted probability
    #updates probability in dict, returns (id,dict) pair
    (_id,(d,prob)) = row
    d['motorist_probability'] = prob
    return (_id,dict(d))

def clean_feat_rank(f):
    #converts the rating from a numpy float to a python native float
    try:
        f['Rating'] = f['Rating'].item()
    except:
        pass
    return f

def feature_id(f,pref):
    #adds feature ID with prefix and rank
    f['id'] = pref + '_' + str(f['Rank'])
    return f


def fit_and_predict_model(train_data,pred_data,weights={1:0.5,0:0.5}):
    #input: Spark RDD for training data, and data to predict, and OPTIONAL weights
    #output: fits a Random Forest model and returns the model, RDD of (id,probability) based on pred_data RDD, and feature importances
    df = pd.DataFrame(train_data.collect())

    Y = np.array(df['label']) #extract the labels
    train_df = df.drop(['id','label','dayOfMonth'],1)
    X = train_df.as_matrix()

    X, Y = shuffle(X,Y,random_state=0) #shuffle the labels and features

    #Train the model
    rf = RandomForestClassifier(n_estimators=25,min_samples_split=20,max_features=None,class_weight=weights)
    rf.fit(X,Y)
    
    df_pred = pd.DataFrame(pred_data.collect())
    ids = np.array(df_pred['id']) #extract the ids
    X_pred = df_pred.drop(['id','dayOfMonth'],1).as_matrix()
    
    #predict the probabilities, match id's with probability of serious collision (1) (convert probability from numpy.float to python native)
    probabilities = sc.parallelize(zip(list(ids),list(rf.predict_proba(X_pred)))).map(lambda (k,v): (k,v[1].item()))
    
    #get feature importance json
    importances = rf.feature_importances_
    indices = np.argsort(importances)[::-1]

    features = train_df.columns.values

    feature_idx = []
    for f in range(X.shape[1]):
        feature_idx.append({
                'Rank': f+1,
                'Feature': train_df.columns.values[f],
                'Rating': importances[indices[f]]
            })

    return rf,probabilities,feature_idx


# ## Generic Model
# 
# The following cells use a generic function to train and run a random forest.

#get data for predicting future probabilities
predictions = sc.newAPIHadoopRDD(
            inputFormatClass="org.elasticsearch.hadoop.mr.EsInputFormat",
            keyClass="org.apache.hadoop.io.NullWritable", 
            valueClass="org.elasticsearch.hadoop.mr.LinkedMapWritable", 
            conf=pred_read_conf)

predictions.cache()

#empty feature importance RDD
featuresRDD = sc.parallelize([]) 

# ### All Vicitms

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
                ],
                "minimum_should_match": 1
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
                ],
                "minimum_should_match": 1
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


truth_frac = accN/(nonAccN + accN)
weights = {1 : truth_frac, 0 : 1-truth_frac} #define the weights

print "%s out of %s grid elements have a serious accident." % (str(accN),str(nonAccN + accN))

#randomly sample equal amount of 1-0 records for training
fraction = 0.9
sub_acc = accidents.sample(withReplacement=True,fraction=fraction,seed=seed)
sub_noacc = nonAccidents.sample(withReplacement=True,fraction=fraction*accN/nonAccN,seed=seed)

trainRDD = sub_acc.union(sub_noacc).map(trainingDict).filter(lambda row: row)
trainRDD.cache()

#get RDD for fitting predictions
predRDD = predictions.map(lambda row: row[1]).map(predictionDict).filter(lambda row: row)
predRDD.cache()

model,probabilities,all_features = fit_and_predict_model(trainRDD,predRDD)
predictions = predictions.join(probabilities).map(update_all_probability)

all_feat = sc.parallelize(all_features).map(lambda f: feature_id(f,'all'))
featuresRDD = featuresRDD.union(all_feat)


# ### Pedestrians

#get set of accident and non-accident records
#defines an ES query for accidents with injury or fatality
q = '''
    {
        "query": {
            "bool":{
                "should":[
                    { "range": 
                        { "grid_pedestrianFatalities": { "gt": 0 } } 
                    },
                    { "range": 
                        { "grid_pedestrianInjuries": { "gt": 0 } } 
                    }
                ],
                "minimum_should_match": 1
            }
        }
    }
'''
acc_conf = deepcopy(es_read_conf)
acc_conf['es.query'] = q
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
                        { "grid_pedestrianFatalities": 0 } 
                    },
                    { "term": 
                        { "grid_pedestrianInjuries": 0 } 
                    }
                ],
                "minimum_should_match": 1
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

truth_frac = accN/(nonAccN + accN)
weights = {1 : truth_frac, 0 : 1-truth_frac} #define the weights

print "%s out of %s grid elements have an accident involving pedestrian injury or fatality." % (str(accN),str(nonAccN + accN))

#randomly sample equal amount of 1-0 records for training
fraction = 0.9
sub_acc = accidents.sample(withReplacement=True,fraction=fraction,seed=seed)
sub_noacc = nonAccidents.sample(withReplacement=True,fraction=fraction*accN/nonAccN,seed=seed)

trainRDD = sub_acc.union(sub_noacc).map(ped_trainingDict).filter(lambda row: row)
trainRDD.cache()


#get RDD for fitting predictions
predRDD = predictions.map(lambda row: row[1]).map(predictionDict).filter(lambda row: row)
predRDD.cache()

model,probabilities,ped_features = fit_and_predict_model(trainRDD,predRDD)
predictions = predictions.join(probabilities).map(update_ped_probability)

ped_feat = sc.parallelize(ped_features).map(lambda f: feature_id(f,'pedestrian'))
featuresRDD = featuresRDD.union(ped_feat)


# ### Cyclists

#get set of accident and non-accident records
#defines an ES query for accidents with injury or fatality
q = '''
    {
        "query": {
            "bool":{
                "should":[
                    { "range": 
                        { "grid_cyclistFatalities": { "gt": 0 } } 
                    },
                    { "range": 
                        { "grid_cyclistInjuries": { "gt": 0 } } 
                    }
                ],
                "minimum_should_match": 1
            }
        }
    }
'''
acc_conf = deepcopy(es_read_conf)
acc_conf['es.query'] = q
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
                        { "grid_cyclistFatalities": 0 } 
                    },
                    { "term": 
                        { "grid_cyclistInjuries": 0 } 
                    }
                ],
                "minimum_should_match": 1
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

truth_frac = accN/(nonAccN + accN)
weights = {1 : truth_frac, 0 : 1-truth_frac} #define the weights

print "%s out of %s grid elements have an accident involving cyclist injury or fatality." % (str(accN),str(nonAccN + accN))

#randomly sample equal amount of 1-0 records for training
fraction = 0.9
sub_acc = accidents.sample(withReplacement=True,fraction=fraction,seed=seed)
sub_noacc = nonAccidents.sample(withReplacement=True,fraction=fraction*accN/nonAccN,seed=seed)

trainRDD = sub_acc.union(sub_noacc).map(cyc_trainingDict).filter(lambda row: row)
trainRDD.cache()

#get RDD for fitting predictions
predRDD = predictions.map(lambda row: row[1]).map(predictionDict).filter(lambda row: row)
predRDD.cache()

model,probabilities,cyc_features = fit_and_predict_model(trainRDD,predRDD)
predictions = predictions.join(probabilities).map(update_cyc_probability)

cyc_feat = sc.parallelize(cyc_features).map(lambda f: feature_id(f,'cyclist'))
featuresRDD = featuresRDD.union(cyc_feat)


# ### Motorists

#get set of accident and non-accident records
#defines an ES query for accidents with injury or fatality
q = '''
    {
        "query": {
            "bool":{
                "should":[
                    { "range": 
                        { "grid_motoristFatalities": { "gt": 0 } } 
                    },
                    { "range": 
                        { "grid_motoristInjuries": { "gt": 0 } } 
                    }
                ],
                "minimum_should_match": 1
            }
        }
    }
'''
acc_conf = deepcopy(es_read_conf)
acc_conf['es.query'] = q
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
                        { "grid_motoristFatalities": 0 } 
                    },
                    { "term": 
                        { "grid_motoristInjuries": 0 } 
                    }
                ],
                "minimum_should_match": 1
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

truth_frac = accN/(nonAccN + accN)
weights = {1 : truth_frac, 0 : 1-truth_frac} #define the weights

print "%s out of %s grid elements have an accident involving motorist injury or fatality." % (str(accN),str(nonAccN + accN))

#randomly sample equal amount of 1-0 records for training
fraction = 0.9
sub_acc = accidents.sample(withReplacement=True,fraction=fraction,seed=seed)
sub_noacc = nonAccidents.sample(withReplacement=True,fraction=fraction*accN/nonAccN,seed=seed)

trainRDD = sub_acc.union(sub_noacc).map(mot_trainingDict).filter(lambda row: row)
trainRDD.cache()

#get RDD for fitting predictions
predRDD = predictions.map(lambda row: row[1]).map(predictionDict).filter(lambda row: row)
predRDD.cache()

model,probabilities,mot_features = fit_and_predict_model(trainRDD,predRDD)
predictions = predictions.join(probabilities).map(update_mot_probability)

mot_feat = sc.parallelize(mot_features).map(lambda f: feature_id(f,'motorist'))
featuresRDD = featuresRDD.union(mot_feat)


# ### Write predictions to Elasticsearch
# 
# Write the RF predictions back to the SafeRoad results Elasticsearch index.

predictions.saveAsNewAPIHadoopFile(
        path='-', 
        outputFormatClass="org.elasticsearch.hadoop.mr.EsOutputFormat",
        keyClass="org.apache.hadoop.io.NullWritable", 
        valueClass="org.elasticsearch.hadoop.mr.LinkedMapWritable", 
        conf=pred_write_conf)


##featuresRDD = (sc.parallelize([])
##               .union(all_feat)
##               .union(ped_feat)
##               .union(cyc_feat)
##               .union(mot_feat))

featuresRDD = featuresRDD.map(lambda f: (f['id'],clean_feat_rank(f)))


featuresRDD.saveAsNewAPIHadoopFile(
        path='-', 
        outputFormatClass="org.elasticsearch.hadoop.mr.EsOutputFormat",
        keyClass="org.apache.hadoop.io.NullWritable", 
        valueClass="org.elasticsearch.hadoop.mr.LinkedMapWritable", 
        conf=feat_write_conf)



