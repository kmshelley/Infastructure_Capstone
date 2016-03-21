# # Random Forest Classifier with PySpark MLLib
# 
from pyspark import SparkContext, SparkConf
conf = SparkConf().setAppName('RandomForest')
sc = SparkContext(conf=conf)

#ML Lib libraries
from pyspark.mllib.tree import RandomForest, RandomForestModel, DecisionTreeModel
from pyspark.mllib.util import MLUtils
from pyspark.mllib.regression import LabeledPoint
from pyspark.ml.feature import OneHotEncoder
from pyspark.ml.feature import StringIndexer

#Python Libraries
import sys
sys.path.append('/root/Infrastructure_Capstone')
import os
import random
from copy import deepcopy
import datetime as dt

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

seed = random.seed(0)
start = dt.datetime.now()

#Configuration for reading from and writing from/to Elasticsearch training data index
es_read_conf = { 
        "es.resource" : "nyc_dataframe/rows", 
        "es.nodes" : ES_url,
        "es.port" : "9200",
        "es.net.http.auth.user" : ES_username,
        "es.net.http.auth.pass" : ES_password
    }

es_write_conf = {
        "es.resource" : "saferoad_results/rows",
        "es.nodes" : ES_url,
        "es.port" : "9200",
        "es.net.http.auth.user" : ES_username, 
        "es.net.http.auth.pass" : ES_password,
        "es.mapping.id" : "grid_id"
    } 

#Configuration for reading from and writing from/to Elasticsearch results index
pred_read_conf = { 
        "es.resource" : "saferoad_results/rows", 
        "es.nodes" : ES_url,
        "es.port" : "9200",
        "es.net.http.auth.user" : ES_username,
        "es.net.http.auth.pass" : ES_password
    }

pred_write_conf = {
        "es.resource" : "saferoad_results/rows",
        "es.nodes" : ES_url,
        "es.port" : "9200",
        "es.net.http.auth.user" : ES_username, 
        "es.net.http.auth.pass" : ES_password,
        "es.mapping.id" : "grid_id"
    } 


# ### Functions for MapReduce

#Function to convert dataframe rows to Labeled Points (label, features)
def trainingLabeledPoint(row): 
    #input: SparkSQL dataframe row element
    #output: Spark LabeledPoint for training
    
    #zip code
    zipcode = row.grid_zipcodeIdx
    
    #date time fields
    dayOfMonth = long(row.grid_day)
    dayOfWeek = long(row.grid_dayOfWeek)
    hour = long(row.grid_hourOfDay)
    month = long(row.grid_month)
    
    #weather fields
    if row.weather_Fog>0:
        fog = 1
    else:
        fog = 0
        
    if row.weather_Rain>0:
        rain = 1
    else:
        rain = 0
        
    if row.weather_SnowHailIce>0:
        snow = 1
    else:
        snow = 0
    
    temp = row.weather_WetBulbFarenheit 
    precip = row.weather_HourlyPrecip  
    #vis = row.weather_Visibility  ## NOT AVAILABLE IN WEATHER FORECAST
    windspeed = row.weather_WindSpeed  

    #truth label
    label = long(row.grid_isAccident)    

    return LabeledPoint(label,[zipcode,dayOfMonth,dayOfWeek,hour,month,fog,rain,snow,temp,precip,windspeed])

def predictionLabeledPoint(row): 
    #input: SparkSQL dataframe row element
    #output: Spark LabeledPoint for prediction
    
    #zip code
    zipcode = row.grid_zipcodeIdx
    
    #date time fields
    dayOfMonth = long(row.grid_day)
    dayOfWeek = long(row.grid_dayOfWeek)
    hour = long(row.grid_hourOfDay)
    month = long(row.grid_month)
    
    #weather fields
    
    fog = row.weather_Fog_Dummy
    rain = row.weather_Rain_Dummy
    snow = row.weather_SnowHailIce_Dummy
    temp = row.weather_Temp 
    precip = row.weather_Precip  
    #vis = row.weather_Visibility  ## NOT AVAILABLE IN WEATHER FORECAST
    windspeed = row.weather_WindSpeed  

    return LabeledPoint(0,[zipcode,dayOfMonth,dayOfWeek,hour,month,fog,rain,snow,temp,precip,windspeed])


#took from here http://stackoverflow.com/questions/28818692/pyspark-mllib-class-probabilities-of-random-forest-predictions
def predict_proba(rf_model, data):
    '''
    This wrapper overcomes the "binary" nature of predictions in the native
    RandomForestModel. 
    '''
    # Collect the individual decision tree models by calling the underlying
    # Java model. These are returned as JavaArray defined by py4j.
    trees = rf_model._java_model.trees()
    ntrees = rf_model.numTrees()
    scores = DecisionTreeModel(trees[0]).predict(data.map(lambda x: x.features))

    # For each decision tree, apply its prediction to the entire dataset and
    # accumulate the results using 'zip'.
    featsAndPredictions = sc.parallelize([]) #empty RDD
    for i in range(1,ntrees):
        dtm = DecisionTreeModel(trees[i])
        predictions = dtm.predict(data.map(lambda x: x.features))
        featsAndPredictions=featsAndPredictions.union(data.map(lambda lp: lp.features).zip(predictions))

        #scores = scores.zip(dtm.predict(data.map(lambda x: x.features)))
        #scores = scores.map(lambda x: x[0] + x[1])
        
    #add up the predictions and divide the accumulated scores over the number of trees
    return featsAndPredictions.reduceByKey(lambda a,b: a+b).map(lambda (key,val): (key,val/ntrees)) #add up the predictions
    
    # Divide the accumulated scores over the number of trees
    #return scores.map(lambda x: x/ntrees)


def resultID(res):
    #converts RF output to format that can be used to associate with original data RDD
    key,val=res
    zipcode = zipKey[key[0]]
    day = int(key[1])
    hour = int(key[3])
    month = int(key[4])
    return ('%s%s%s_%s' % (str(month),str(day),str(hour),zipcode),val)

def predID(row):
    #defines id to match prediction output id
    zipcode = row['grid_zipcode']
    #date time fields
    day = int(row['grid_day'])
    hour = int(row['grid_hourOfDay'])
    month = int(row['grid_month'])
    return ('%s%s%s_%s' % (str(month),str(day),str(hour),zipcode),row)

def addProbability(e):
    (_id,(row,p))=e
    row['probability'] = float(p)
    return (_id,row)


# ### Gather data from Elasticsearch grid index

#get RDD of the collisions grid
##grid_rdd = sc.newAPIHadoopRDD(
##    inputFormatClass="org.elasticsearch.hadoop.mr.EsInputFormat",
##    keyClass="org.apache.hadoop.io.NullWritable", 
##    valueClass="org.elasticsearch.hadoop.mr.LinkedMapWritable", 
##    conf=es_read_conf).map(lambda row: row[1])
##
##grid_rdd.cache()


# ### Get Evenly Distributed Training Data
# 
# The following cells build a SparkSQL DataFrame with approximately the same number of accident and non-accident records

#get set of accident and non-accident records

#grid records with accident
acc_conf = deepcopy(es_read_conf)
acc_conf['es.query'] = '{ "query" : { "term" : { "grid_isAccident" : "1" } }  }'

accidents = sc.newAPIHadoopRDD(
            inputFormatClass="org.elasticsearch.hadoop.mr.EsInputFormat",
            keyClass="org.apache.hadoop.io.NullWritable", 
            valueClass="org.elasticsearch.hadoop.mr.LinkedMapWritable", 
            conf=acc_conf).map(lambda row: row[1])
accidents.cache()
accN = accidents.count()

#grid records with no accident
no_acc_conf = deepcopy(es_read_conf)
no_acc_conf['es.query'] = '{ "query" : { "term" : { "grid_isAccident" : "0" } }  }'

no_accidents = sc.newAPIHadoopRDD(
            inputFormatClass="org.elasticsearch.hadoop.mr.EsInputFormat",
            keyClass="org.apache.hadoop.io.NullWritable", 
            valueClass="org.elasticsearch.hadoop.mr.LinkedMapWritable", 
            conf=no_acc_conf).map(lambda row: row[1])
no_accidents.cache()
noaccN = no_accidents.count()
##no_accidents = grid_rdd.filter(lambda row: row['grid_isAccident'] == 0)
##no_accidents.cache()
##noaccN = no_accidents.count()


print "%s out of %s grid elements have an accident." % (str(accN),str(noaccN))


# ### Sample Data
# We will now randomly sample from the accidents and non-accidents RDD's to get approximately 50-50 accidents and non-accidents
fraction = 0.75
sub_acc = accidents.sample(withReplacement=False,fraction=fraction,seed=seed)
sub_noacc = no_accidents.sample(withReplacement=False,fraction=fraction*accN/noaccN,seed=seed)

print "Total accidents: %s" % str(sub_acc.count())
print "Total non-accidents: %s" % str(sub_noacc.count())
full_rdd = sub_acc.union(sub_noacc)
full_rdd.cache()


#create a dataframe for encoding categorical variables
df = sqlContext.createDataFrame(full_rdd) #complete dataset
#df = sqlContext.createDataFrame(full_rdd.sample(withReplacement=False,fraction=0.25,seed=seed)) #let's start with a 1/4 of the data


# ## Convert categorical features
# 
# The following cells train a Spark StringIndexer to index the zip codes in the data set.

#define categorical indexers for the data
zipIndexer =  StringIndexer(inputCol='grid_zipcode', outputCol='grid_zipcodeIdx')#,handleInvalid='skip')
zipIdxModel = zipIndexer.fit(df)
indexed = zipIdxModel.transform(df)

indexed.cache()
#zipEncoder = OneHotEncoder(dropLast=False, inputCol="grid_zipcodeIdx", outputCol="grid_zipcodeVec")
#zipEncoded = zipEncoder.transform(td1)

#save the zip code labels for rewriting predictions to Elasticsearch index
zipCodeLables = zipIdxModel._call_java("labels")
zipKey = {i:zipCodeLables[i] for i in range(len(zipCodeLables))}


# ## Labeled Points
# 
# Spark MLLib algorithms take LabeledPoints, a special object tuple of (label, [features]). Before training the model we will run a simple map job to convert the SparkSQL DataFrame rows to LabeledPoints.

data = indexed.map(trainingLabeledPoint)

# ### Train and Run the Model
# The following Random Forest code comes directly from the Spark MLLib programming guide: 
# http://spark.apache.org/docs/latest/mllib-ensembles.html#random-forests

# Split the data into training and test sets (30% held out for testing)
(trainingData, testData) = data.randomSplit([0.7, 0.3])

# Train a RandomForest model.
#  Empty categoricalFeaturesInfo indicates all features are continuous.
#  Note: Use larger numTrees in practice.
#  Setting featureSubsetStrategy="auto" lets the algorithm choose.
model = RandomForest.trainClassifier(trainingData, numClasses=2, categoricalFeaturesInfo={},
                                     numTrees=100, featureSubsetStrategy="auto",
                                     impurity='gini', maxDepth=4, maxBins=32)

# Evaluate model on test instances and compute test error
predictions = model.predict(testData.map(lambda x: x.features))
labelsAndPredictions = testData.map(lambda lp: lp.label).zip(predictions)
testErr = labelsAndPredictions.filter(lambda (v, p): v != p).count() / float(testData.count())
print('Test Error = ' + str(testErr))



# ## Generate Future Predictions
# 
# Now we will use the predictions Elasticsearch Index to create predictions for the next 10 days, and push the results back up to Elasticsearch.

predictions = sc.newAPIHadoopRDD(
            inputFormatClass="org.elasticsearch.hadoop.mr.EsInputFormat",
            keyClass="org.apache.hadoop.io.NullWritable", 
            valueClass="org.elasticsearch.hadoop.mr.LinkedMapWritable", 
            conf=pred_read_conf).map(lambda row: row[1])
predictions.cache()

# Write predictions RDD to SparkSQL DataFrame, index zip codes to match indexed training data
df = sqlContext.createDataFrame(predictions) 
pred_indexed = zipIdxModel.transform(df)
pred_indexed.cache()

# Convert DataFrame to RDD of LabeledPoints
pred_data = pred_indexed.map(predictionLabeledPoint)

# Create RDD of (features,probability) tuples
probabilities = predict_proba(model,pred_data)
probabilities.cache()

#create unique ID's to join the predicted probabilites back to the results grid
prob_adj = probabilities.map(resultID)
pred_adj = predictions.map(predID)


# ### Write predictions to Elasticsearch
# 
# Write the RF predictions back to the SafeRoad results Elasticsearch index.
predWithProb = pred_adj.join(prob_adj).map(addProbability)
predWithProb.saveAsNewAPIHadoopFile(
        path='-', 
        outputFormatClass="org.elasticsearch.hadoop.mr.EsOutputFormat",
        keyClass="org.apache.hadoop.io.NullWritable", 
        valueClass="org.elasticsearch.hadoop.mr.LinkedMapWritable", 
        conf=pred_write_conf)


print "Completed predictions! Took %s" % str(dt.datetime.now() - start)
