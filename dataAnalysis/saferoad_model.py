# # Serious Accident Prediction with PySpark and Sci-Kit Learn
from pyspark import SparkContext, SparkConf
conf = SparkConf().setAppName('SafeRoad Model')
sc = SparkContext(conf=conf)

from pyspark.sql import SQLContext
sqlContext = SQLContext(sc)


#Scikit learn libraries
#classifiers
from sklearn.neighbors import KNeighborsClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.ensemble import RandomForestClassifier, ExtraTreesClassifier

#metrics/cross-validation
from sklearn.cross_validation import cross_val_score, ShuffleSplit
from sklearn.utils import shuffle
from sklearn.metrics import f1_score, accuracy_score, precision_score, recall_score, roc_auc_score, roc_curve, auc

#Python Libraries
import sys
import os
import random
import json
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

#custom modules
sys.path.append('/root/Infrastructure_Capstone')
from dataStorage import upload_to_Elasticsearch



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
diag = config.get('indexes','diagnostics')
collisions_idx = config.get('indexes','collisions')

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

diag_write_conf = {
        "es.resource" : diag,
        "es.nodes" : ES_hosts,
        "es.port" : "9200",
        "es.net.http.auth.user" : ES_username, 
        "es.net.http.auth.pass" : ES_password,
        "es.mapping.id" : "id"
    }

# ### Training Points
# 
# The following will build the feature arrays for the ML algorithm.

def trainingDict(row,victim='all'):
    #returns a dict of label and features for an ML algorithm, victim type to set as the truth label
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
        output['med_aadt'] = row["Median AADT"]
        output['avg_aadt'] = row["Average AADT"]

        #road condition requests
        output['rd_cond'] = (row['road_cond_requests'] + 0.0)/output['road_len'] # road repair requests per mile of road

        #liquor licenses
        output['liquor'] = (row['liquor_licenses'] + 0.0)/output['zip_area'] # Number of liquor licenses per square mile

        #truth label
        if victim.lower()[:3] == 'all':
            output['label'] = 1 if (float(row['grid_totalInjuries'])>0 or float(row['grid_totalFatalities'])>0) else 0
        if victim.lower()[:3] == 'ped':
            output['label'] = 1 if (float(row['grid_pedestrianFatalities'])>0 or float(row['grid_pedestrianInjuries'])>0) else 0
        if victim.lower()[:3] == 'cyc':
            output['label'] = 1 if (float(row['grid_cyclistFatalities'])>0 or float(row['grid_cyclistInjuries'])>0) else 0
        if victim.lower()[:3] == 'mot':
            output['label'] = 1 if (float(row['grid_motoristFatalities'])>0 or float(row['grid_motoristInjuries'])>0) else 0
            
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
        output['med_aadt'] = row["Median AADT"]
        output['avg_aadt'] = row["Average AADT"]

        #road condition requests
        output['rd_cond'] = (row['road_cond_requests'] + 0.0)/output['road_len'] # road repair requests per mile of road

        #liquor licenses
        output['liquor'] = (row['liquor_licenses'] + 0.0)/output['zip_area'] # Number of liquor licenses per square mile

        return output
    
def update_probability(row,victim='all'):
    #inpur: key,val pair of id, es recrod dict, and predicted probability, victim type (all, pedestrian, motorist, cyclist)
    #updates probability in dict, returns (id,dict) pair
    (_id,(d,prob)) = row
    d[victim.lower() + '_probability'] = prob
    return (_id,dict(d))

def clean_feat_rank(f):
    #converts the rating from a numpy float to a python native float
    for key in f:
        try:
            f[key] = f[key].item()
        except:
            pass
    return f

##
## Functions to get training data and train model
##
def get_latest_record(index,doc_type,datetime_field):
    #input: index, doc_type, and a date-time field
    #output: returns the latest ES record based on the datetime field
    
    es_url = 'http://%s:%s@%s:9200' % (ES_username,ES_password,ES_url)
    es = Elasticsearch(es_url)

    query = '{"sort": [ { "%s":   { "order": "desc" }} ] }' % datetime_field
    try:
        return helpers.scan(es,index=index,doc_type=doc_type,query=query,preserve_order=True).next()['_source']
    except:
        return None

def mean_decrease_accuracy(model,X,Y,feats,victim='all'):
    #Taken from http://blog.datadive.net/selecting-good-features-part-iii-random-forests/
    #crossvalidate the scores on a number of different random splits of the data
    scores = {}
    #iterate 20 times, with random splits of the training/test data
    for train_idx, test_idx in ShuffleSplit(len(X), 20, .3):
        X_train, X_test = X[train_idx], X[test_idx]
        Y_train, Y_test = Y[train_idx], Y[test_idx]
        r = model.fit(X_train, Y_train)
        
        #get overall accuracy
        acc = accuracy_score(Y_test, model.predict(X_test))
        for i in range(X.shape[1]):
            #make a copy of test data
            X_t = X_test.copy()
            #randomly shuffle the ith feature
            np.random.shuffle(X_t[:, i])
            #test the new accuracy
            shuff_acc = accuracy_score(Y_test, model.predict(X_t))
            
            if feats[i] not in scores: scores[feats[i]] = []
            scores[feats[i]].append((acc-shuff_acc)/acc) #% change in accuracy
            
    #print "Features sorted by their score:"
    importances = np.array([round(np.mean(scores[f]), 4) for f in feats])
    indices = np.argsort(importances)[::-1]
    
    f_sorted = zip(np.array(feats)[indices],importances[indices],np.arange(1,len(indices)+1))
    feature_idx = []
    for f,r,i in f_sorted:
        feature_idx.append({
                'Rank': i,
                'Feature': f,
                'Rating': r
            })

##    order=np.array(feats)feats[indices]
##    print order
##    df = pd.DataFrame(f_sorted)
##    df.columns=['Features','Rating','Rank']
##    #print df
##    fig = plt.figure()
##    ax = sns.barplot(x="Features", y="Rating", data=df, order=order)
##    ax.set_xticklabels(order,rotation='vertical',horizontalalignment='center')
    
    return feature_idx

def get_training_test_data(test_date,victim='all',fraction=1.0):
    #input: start date, OPTIONAL victim type, OPTIONAL sampling fraction and victim type (all, pedestrian, cyclist, motorist)
    #output: training RDD, test RDD, dict of training class weights

    #formatted query date
    datestr = dt.datetime.strftime(test_date,'%m/%d/%Y')
    
    #define the injury/fatality fields to query
    if victim.lower()[:3] == 'all':
        fatal = 'grid_totalFatalities'
        injury = 'grid_totalInjuries'
    if victim.lower()[:3] == 'ped':
        fatal = 'grid_pedestrianFatalities'
        injury = 'grid_pedestrianInjuries'
    if victim.lower()[:3] == 'cyc':
        fatal = 'grid_cyclistFatalities'
        injury = 'grid_cyclistInjuries'
    if victim.lower()[:3] == 'mot':
        fatal = 'grid_motoristFatalities'
        injury = 'grid_motoristInjuries'

    #define non-accident test data and count    
    no_acc_test_q = '''{
        "query" : {
            "bool": {
                "must" :[
                            { "term": 
                                { "%s": 0 } 
                            },
                            { "term": 
                                { "%s": 0 } 
                            },
                            { "range" : 
                                { "grid_fullDate" : 
                                    { "gte": "%s",
                                      "format": "MM/dd/yyyy" }
                                }
                            }
                        ] 
                    }         
                }      
    }''' % (fatal,injury,datestr)


    nonAcc_test_conf = deepcopy(es_read_conf)
    nonAcc_test_conf['es.query'] = no_acc_test_q

    nonAcc_testRDD = sc.newAPIHadoopRDD(
            inputFormatClass="org.elasticsearch.hadoop.mr.EsInputFormat",
            keyClass="org.apache.hadoop.io.NullWritable", 
            valueClass="org.elasticsearch.hadoop.mr.LinkedMapWritable", 
            conf=nonAcc_test_conf).map(lambda row: row[1])
    nonAcc_testRDD.cache()
    
    nonAcc_testN = nonAcc_testRDD.count()

    #define non-accident training data and count    
    no_acc_train_q = '''{
        "query" : {
            "bool": {
                "must" :[
                            { "term": 
                                { "%s": 0 } 
                            },
                            { "term": 
                                { "%s": 0 } 
                            },
                            { "range" : 
                                { "grid_fullDate" : 
                                    { "lt": "%s",
                                      "format": "MM/dd/yyyy" }
                                }
                            }
                        ] 
                    }         
                }      
    }''' % (fatal,injury,datestr)


    nonAcc_train_conf = deepcopy(es_read_conf)
    nonAcc_train_conf['es.query'] = no_acc_train_q

    nonAcc_trainRDD = sc.newAPIHadoopRDD(
            inputFormatClass="org.elasticsearch.hadoop.mr.EsInputFormat",
            keyClass="org.apache.hadoop.io.NullWritable", 
            valueClass="org.elasticsearch.hadoop.mr.LinkedMapWritable", 
            conf=nonAcc_train_conf).map(lambda row: row[1])
    nonAcc_trainRDD.cache()
    
    nonAcc_trainN = nonAcc_trainRDD.count()

    #define accident test data and count
    acc_test_q = '''{
        "query" : {
            "bool": {
                "should":[
                    { "range": 
                        { "%s": { "gt": 0 } } 
                    },
                    { "range": 
                        { "%s": { "gt": 0 } } 
                    }
                ],
                "minimum_should_match": 1,
                "must" : { "range" : 
                    { "grid_fullDate" : 
                        { "gte": "%s",
                           "format": "MM/dd/yyyy" }
                        }
                } 
            }         
        }      
    }''' % (fatal,injury,datestr)

    Acc_test_conf = deepcopy(es_read_conf)
    Acc_test_conf['es.query'] = acc_test_q

    Acc_testRDD = sc.newAPIHadoopRDD(
            inputFormatClass="org.elasticsearch.hadoop.mr.EsInputFormat",
            keyClass="org.apache.hadoop.io.NullWritable", 
            valueClass="org.elasticsearch.hadoop.mr.LinkedMapWritable", 
            conf=Acc_test_conf).map(lambda row: row[1])
    Acc_testRDD.cache()
    
    Acc_testN = Acc_testRDD.count()

    #define accident training data and count
    acc_train_q = '''{
        "query" : {
            "bool": {
                "should":[
                    { "range": 
                        { "%s": { "gt": 0 } } 
                    },
                    { "range": 
                        { "%s": { "gt": 0 } } 
                    }
                ],
                "minimum_should_match": 1,
                "must" : { "range" : 
                    { "grid_fullDate" : 
                        { "lt": "%s",
                           "format": "MM/dd/yyyy" }
                        }
                } 
            }         
        }      
    }''' % (fatal,injury,datestr)

    Acc_train_conf = deepcopy(es_read_conf)
    Acc_train_conf['es.query'] = acc_train_q

    Acc_trainRDD = sc.newAPIHadoopRDD(
            inputFormatClass="org.elasticsearch.hadoop.mr.EsInputFormat",
            keyClass="org.apache.hadoop.io.NullWritable", 
            valueClass="org.elasticsearch.hadoop.mr.LinkedMapWritable", 
            conf=Acc_train_conf).map(lambda row: row[1])
    Acc_trainRDD.cache()
    
    Acc_trainN = Acc_trainRDD.count()


    truth_frac = (0.0 + Acc_trainN)/(nonAcc_trainN + Acc_trainN)
    weights = {1 : truth_frac, 0 : 1-truth_frac} #define the weights of the training data

    #randomly sample equal amount of 1-0 records for training
    fraction = fraction
    #sub_acc = Acc_trainRDD.sample(withReplacement=True,fraction=fraction,seed=seed)
    #randomly sample (with replacement) from the non-accidents
    sub_noacc = nonAcc_trainRDD.sample(withReplacement=True,fraction=fraction*Acc_trainN/nonAcc_trainN,seed=seed)

    trainRDD = Acc_trainRDD.union(sub_noacc).map(lambda row: trainingDict(row,victim)).filter(lambda row: row)
    trainRDD.cache()

    testRDD = Acc_testRDD.union(nonAcc_testRDD).map(lambda row: trainingDict(row,victim)).filter(lambda row: row)
    testRDD.cache()
    
    return trainRDD, testRDD, weights

def fit_and_predict_model(model,train_data,test_data,pred_data,weights={1:0.5,0:0.5},victim='all'):
    #input: Spark RDD for training data, and data to predict, and OPTIONAL weights, optional victim type (all, pedestrian, cyclist, motorist)
    #output: fits a Random Forest model and returns an RDD of (id,probability) based on pred_data RDD, and feature importances
    features = ['avg_aadt',
                'bridges',
                'dayOfWeek',
                'fog',
                'hour',
                'liquor',
                'med_aadt',
                'med_speed',
                'month',
                'rain',
                'rd_cond',
                'rd_count',
                'road_len',
                'snow',
                'speed15',
                'speed25',
                'speed35',
                'speed45',
                'speed5',
                'speed55',
                'speed65',
                'speed85',
                'tunnels',
                'zip_area']
    #training data
    df = pd.DataFrame(train_data.collect())
    Y_train = np.array(df['label']) #extract the labels
    #df_train = df.drop(['id','label','dayOfMonth','zipcode','temp'],1)
    df_train = df[features]
    X_train = df_train.as_matrix()
    #shuffle the training data and labels
    X_train, Y_train = shuffle(X_train,Y_train,random_state=0) #shuffle the labels and features

    #test data
    df = pd.DataFrame(test_data.collect())
    Y_test = np.array(df['label']) #extract the labels
    #df_test = df.drop(['id','label','dayOfMonth','zipcode','temp'],1)
    df_test = df[features]
    X_test = df_test.as_matrix()
    #shuffle the test data and labels
    X_test, Y_test = shuffle(X_test,Y_test,random_state=0) #shuffle the labels and features
    
    #Train the model
    #rf = RandomForestClassifier(n_estimators=100,min_samples_split=50,max_features=None,max_depth=15,class_weight=weights)
    model.fit(X_train,Y_train)
    
    df_pred = pd.DataFrame(pred_data.collect())
    ids = np.array(df_pred['id']) #extract the ids
    #X_pred = df_pred.drop(['id','dayOfMonth','zipcode','temp'],1).as_matrix()
    X_pred = df_pred[features].as_matrix()
    
    #predict the probabilities, match id's with probability of serious collision (1) (convert probability from numpy.float to python native)
    probabilities = sc.parallelize(zip(list(ids),list(model.predict_proba(X_pred)))).map(lambda (k,v): (k,v[1].item()))
    
    #get feature importance json
##    importances = model.feature_importances_
##    features = df_train.columns.values
##    indices = np.argsort(importances)[::-1]
##
##    f_sorted = zip(features[indices],importances[indices],np.arange(1,len(indices)+1))
##    feature_idx = []
##    for f,r,i in f_sorted:
##        feature_idx.append({
##                'id' : victim.lower() + '_' + str(i),
##                'Rank': i,
##                'Feature': f,
##                'Rating': r,
##                'entityType': victim.lower()
##            })

    feature_idx = mean_decrease_accuracy(model,X_train,Y_train,feats=features)
    for f in feature_idx:
        f['id'] = victim.lower() + '_' + str(f['Rank'])
        f['entityType'] = victim.lower()
    #feature_idx = {}

    #################
    ## Diagnostics ##
    #################
    #dictionary of diagnostics
    diagnostics = {}

    test_predictions = model.predict(X_test)

    percent_accidents = np.mean(test_predictions)
        
    fp = 1.0*np.sum(np.logical_and(test_predictions==1,Y_test==0))
    false_positive_rate = fp/(np.sum(Y_test==0))

    tp = 1.0*np.sum(np.logical_and(test_predictions==1,Y_test==1))
    true_positive_rate = tp/(np.sum(Y_test==1))

    fn = 1.0*np.sum(np.logical_and(test_predictions==0,Y_test==1))
    false_negative_rate = fn/(np.sum(Y_test==1))

    tn = 1.0*np.sum(np.logical_and(test_predictions==0,Y_test==0))
    true_negative_rate= tn/(np.sum(Y_test==0))

    accuracy = (tp+tn)/(tp+fp+tn+fn)

    diagnostics[victim.lower() + '_percent_accidents'] = percent_accidents
    diagnostics[victim.lower() + '_false_postive_rate'] = false_positive_rate
    diagnostics[victim.lower() + '_true_postive_rate'] = true_positive_rate
    diagnostics[victim.lower() + '_false_negative_rate'] = false_negative_rate
    diagnostics[victim.lower() + '_true_negative_rate'] = true_negative_rate
    diagnostics[victim.lower() + '_F1']  = f1_score(Y_test, test_predictions)
    diagnostics[victim.lower() + '_Precision'] = precision_score(Y_test, test_predictions)
    diagnostics[victim.lower() + '_Recall'] = recall_score(Y_test, test_predictions)
    diagnostics[victim.lower() + '_Test_set_accuracy'] = accuracy

    predict_test_probs = model.predict_proba(X_test)[:,1]
    diagnostics[victim.lower() + '_AUC'] = roc_auc_score(Y_test, predict_test_probs)

    fpr, tpr, thresholds = roc_curve(Y_test, predict_test_probs, pos_label=1)
    diagnostics[victim.lower() + '_ROC'] = {'FPR': list(fpr), 'TPR': list(tpr)}

    return probabilities,feature_idx,diagnostics


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
diagnostics = {}


#################
# ### All Vicitms
#################
#get latest collision record from saferoad index
last_coll = get_latest_record(index=collisions_idx.split('/')[0],doc_type=collisions_idx.split('/')[1],datetime_field='collision_DATETIME_C')
coll_time = parse(last_coll['collision_DATETIME_C']).replace(tzinfo=None) #get the date of the latest collision record

#we will build training data set from the start of the collisions data to 90 days prior to the latest record
test_date = coll_time - dt.timedelta(days=90)
trainRDD, testRDD, weights = get_training_test_data(test_date,victim='all',fraction=1.0)

#get RDD for fitting predictions
predRDD = predictions.map(lambda row: row[1]).map(predictionDict).filter(lambda row: row)
predRDD.cache()

#fit RF and predict
rf = RandomForestClassifier(n_estimators=100,min_samples_split=50,max_features=None,max_depth=15,class_weight="balanced",n_jobs=4,bootstrap=True)
probabilities,all_features,all_diagnostics = fit_and_predict_model(rf,trainRDD,testRDD,predRDD,victim='all')
diagnostics.update(all_diagnostics)

#add all victim probabilities to predictions RDD
predictions = predictions.join(probabilities).map(lambda row: update_probability(row,victim='all'))

#identify each feature rating based on victim type
all_feat = sc.parallelize(all_features)#.map(lambda f: feature_id(f,'all'))
featuresRDD = featuresRDD.union(all_feat)

#################
# ### Pedestrians
#################
#get latest collision record from saferoad index
last_coll = get_latest_record(index=collisions_idx.split('/')[0],doc_type=collisions_idx.split('/')[1],datetime_field='collision_DATETIME_C')
coll_time = parse(last_coll['collision_DATETIME_C']).replace(tzinfo=None) #get the date of the latest collision record

#we will build training data set from the start of the collisions data to 90 days prior to the latest record
test_date = coll_time - dt.timedelta(days=90)
trainRDD, testRDD, weights = get_training_test_data(test_date,victim='pedestrian',fraction=1.0)

#get RDD for fitting predictions
predRDD = predictions.map(lambda row: row[1]).map(predictionDict).filter(lambda row: row)
predRDD.cache()

#fit RF and predict
rf = RandomForestClassifier(n_estimators=100,min_samples_split=50,max_features=None,max_depth=15,class_weight="balanced",n_jobs=4,bootstrap=True)
probabilities,ped_features,ped_diagnostics = fit_and_predict_model(rf,trainRDD,testRDD,predRDD,victim='pedestrian')
diagnostics.update(ped_diagnostics)

#add pedestrian probabilities to predictions RDD
predictions = predictions.join(probabilities).map(lambda row: update_probability(row,victim='pedestrian'))

#identify each feature rating based on victim type
ped_feat = sc.parallelize(ped_features)#.map(lambda f: feature_id(f,'pedestrian'))
featuresRDD = featuresRDD.union(ped_feat)

#################
# ### Cyclists
#################
#get latest collision record from saferoad index
last_coll = get_latest_record(index=collisions_idx.split('/')[0],doc_type=collisions_idx.split('/')[1],datetime_field='collision_DATETIME_C')
coll_time = parse(last_coll['collision_DATETIME_C']).replace(tzinfo=None) #get the date of the latest collision record

#we will build training data set from the start of the collisions data to 90 days prior to the latest record
test_date = coll_time - dt.timedelta(days=90)
trainRDD, testRDD, weights = get_training_test_data(test_date,victim='cyclist',fraction=1.0)

#get RDD for fitting predictions
predRDD = predictions.map(lambda row: row[1]).map(predictionDict).filter(lambda row: row)
predRDD.cache()

#fit RF and predict
rf = RandomForestClassifier(n_estimators=100,min_samples_split=50,max_features=None,max_depth=15,class_weight="balanced",n_jobs=4,bootstrap=True)
probabilities,cyc_features,cyc_diagnostics = fit_and_predict_model(rf,trainRDD,testRDD,predRDD,victim='cyclist')
diagnostics.update(cyc_diagnostics)

#add cyclist probabilities to predictions RDD
predictions = predictions.join(probabilities).map(lambda row: update_probability(row,victim='cyclist'))

#identify each feature rating based on victim type
cyc_feat = sc.parallelize(cyc_features)#.map(lambda f: feature_id(f,'cyclist'))
featuresRDD = featuresRDD.union(cyc_feat)

#################
# ### Motorists
#################
#get latest collision record from saferoad index
last_coll = get_latest_record(index=collisions_idx.split('/')[0],doc_type=collisions_idx.split('/')[1],datetime_field='collision_DATETIME_C')
coll_time = parse(last_coll['collision_DATETIME_C']).replace(tzinfo=None) #get the date of the latest collision record

#we will build training data set from the start of the collisions data to 90 days prior to the latest record
test_date = coll_time - dt.timedelta(days=90)
trainRDD, testRDD, weights = get_training_test_data(test_date,victim='motorist',fraction=1.0)

#get RDD for fitting predictions
predRDD = predictions.map(lambda row: row[1]).map(predictionDict).filter(lambda row: row)
predRDD.cache()

#fit RF and predict
rf = RandomForestClassifier(n_estimators=100,min_samples_split=50,max_features=None,max_depth=15,class_weight="balanced",n_jobs=4,bootstrap=True)
probabilities,mot_features,mot_diagnostics = fit_and_predict_model(rf,trainRDD,testRDD,predRDD,victim='motorist')
diagnostics.update(mot_diagnostics)

#add motorist probabilities to predictions RDD
predictions = predictions.join(probabilities).map(lambda row: update_probability(row,victim='motorist'))

#identify each feature rating based on victim type
mot_feat = sc.parallelize(mot_features)#.map(lambda f: feature_id(f,'motorist'))
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



    
#convert ratings from numpy float to python native float, convert to tuple for loading into ES
featuresRDD = featuresRDD.map(lambda f: (f['id'],clean_feat_rank(f)))

featuresRDD.saveAsNewAPIHadoopFile(
        path='-', 
        outputFormatClass="org.elasticsearch.hadoop.mr.EsOutputFormat",
        keyClass="org.apache.hadoop.io.NullWritable", 
        valueClass="org.elasticsearch.hadoop.mr.LinkedMapWritable", 
        conf=feat_write_conf)


#
# ### Write Model diagnostics to Elasticsearch
#
index,doc_type=diag.split('/')
diagnostics.update({ 'id' : '1' })

mapping = {'properties':
               {
                   'all_percent_accidents': { 'type': 'double' },
                   'pedestrian_percent_accidents': { 'type': 'double' },
                   'cyclist_percent_accidents': { 'type': 'double' },
                   'motorist_percent_accidents': { 'type': 'double' },
                   'all_false_postive_rate': { 'type': 'double' },
                   'pedestrian_false_postive_rate': { 'type': 'double' },
                   'cyclist_false_postive_rate': { 'type': 'double' },
                   'motorist_false_postive_rate': { 'type': 'double' },
                   'all_true_postive_rate': { 'type': 'double' },
                   'pedestrian_true_postive_rate': { 'type': 'double' },
                   'cyclist_true_postive_rate': { 'type': 'double' },
                   'motorist_true_postive_rate': { 'type': 'double' },
                   'all_false_negative_rate': { 'type': 'double' },
                   'pedestrian_false_negative_rate': { 'type': 'double' },
                   'cyclist_false_negative_rate': { 'type': 'double' },
                   'motorist_false_negative_rate': { 'type': 'double' },
                   'all_true_negative_rate': { 'type': 'double' },
                   'pedestrian_true_negative_rate': { 'type': 'double' },
                   'cyclist_true_negative_rate': { 'type': 'double' },
                   'motorist_true_negative_rate': { 'type': 'double' },
                   'all_F1': { 'type': 'double' },
                   'pedestrian_F1': { 'type': 'double' },
                   'cyclist_F1': { 'type': 'double' },
                   'motorist_F1': { 'type': 'double' },
                   'all_Precision': { 'type': 'double' },
                   'pedestrian_Precision': { 'type': 'double' },
                   'cyclist_Precision': { 'type': 'double' },
                   'motorist_Precision': { 'type': 'double' },
                   'all_Recall': { 'type': 'double' },
                   'pedestrian_Recall': { 'type': 'double' },
                   'cyclist_Recall': { 'type': 'double' },
                   'motorist_Recall': { 'type': 'double' },
                   'all_Test_set_accuracy': { 'type': 'double' },
                   'pedestrian_Test_set_accuracy': { 'type': 'double' },
                   'cyclist_Test_set_accuracy': { 'type': 'double' },
                   'motorist_Test_set_accuracy': { 'type': 'double' },
                   'all_AUC': { 'type': 'double' },
                   'pedestrian_AUC': { 'type': 'double' },
                   'cyclist_AUC': { 'type': 'double' },
                   'motorist_AUC': { 'type': 'double' },
                   'all_ROC': { 'type': 'object' },
                   'pedestrian_ROC': { 'type': 'object' },
                   'cyclist_ROC': { 'type': 'object' },
                   'motorist_ROC': { 'type': 'object' }
                 }
              }
try:
    es_url = 'http://%s:%s@%s:9200' % (ES_username,ES_password,ES_url)
    es = Elasticsearch(es_url)
    
    #use cURL to put the mapping
    p = subprocess.Popen(['curl','%s/%s/_mapping/%s' % (es_url,index,doc_type),'-d','%s' % json.dumps(mapping)],stderr=subprocess.PIPE)
    out, err = p.communicate()
    if err: print '\n' + err + '\n\n'
except:
    pass

upload_to_Elasticsearch.update_ES_records_curl([diagnostics],index=index,doc_type=doc_type,id_field="id")
