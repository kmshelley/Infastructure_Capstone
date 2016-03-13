import os
import geojson, json
from elasticsearch import Elasticsearch
from elasticsearch import helpers
from elasticsearch.exceptions import NotFoundError
import ConfigParser
from pprint import pprint
from copy import deepcopy
import subprocess
from pprint import pprint

#read in the config file
config = ConfigParser.ConfigParser()
config.read('./config/capstone_config.ini')
#getting config file two folders up so we don't check in password by mistake
#config.read(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))) + '/capstone_config.ini')


ES_url = config.get('ElasticSearch','host')
ES_password = config.get('ElasticSearch','password')
ES_username= config.get('ElasticSearch','username')

temp_data_dir = config.get('MISC','temp_data_dir')

def create_es_index_and_mapping_cURL(index,doc_type,time_field=False,time_type=False,date_field=False,date_type=False,datetime_field=False,datetime_type=False,geopoint=False,geoshape=False,delete_index=False):
    #input: an index name, document type, and optional field names to map.
    #creates ElasticSearch index using subprocess and cURL
    es = 'http://%s:%s@%s:9200' % (ES_username,ES_password,ES_url)

    if delete_index:
        try :
            p = subprocess.Popen(['curl','-XDELETE','%s/%s' % (es,index)])
            #subprocess.call('curl -XDELETE %s/%s' % (es,index))
            out, err = p.communicate()
            if err: print '\n' + err + '\n\n'
        except Exception as e:
            print "Error deleting index:"
            print e
    try:
        #create the index, set the replicas so uploads will not err out
        settings = {"settings":
                    {"number_of_replicas" : 1}
                    }
        p = subprocess.Popen(['curl','-XPUT','%s/%s' % (es,index),'-d',json.dumps(settings)])
        out, err = p.communicate()
        if err: print '\n' + err + '\n\n'
        
        #build the mapping document
        mapping = {}
        mapping['properties'] = {}

        #if the data has a certain mapping fields
        if time_field: mapping['properties'][time_field] = {'type':time_type}
        if date_field: mapping['properties'][date_field] = {'type':date_type}
        if datetime_field: mapping['properties'][datetime_field] = {'type':datetime_type}
        if geopoint: mapping['properties'][geopoint] = {'type':'geo_point','store':'yes'}
        if geoshape: mapping['properties'][geoshape] = {'type':'geo_shape','tree':'quadtree', 'precision': '1m'}

        #use cURL to put the mapping
        p = subprocess.Popen(['curl','%s/%s/_mapping/%s' % (es,index,doc_type),'-d','%s' % json.dumps(mapping)],stderr=subprocess.PIPE)
        out, err = p.communicate()
        if err: print '\n' + err + '\n\n'
        
    except Exception as e:
        #do not try to recreate the index
        print "Error creating index:"
        print e
        
    #double-check that the correct number of replicas are being used
    p = subprocess.Popen(['curl','-XPUT','%s/%s/_settings' % (es,index),'-d','{"number_of_replicas": 1}'])
    out, err = p.communicate()
    if err: print '\n' + err + '\n\n'

    
def upload_individual_docs_to_ES_cURL(docs,index,doc_type,id_field=False,geopoint=False,geoshape=False,delete_index=False):
    #input: list of JSON documents, an index name, document type, ID field, and name of an (OPTIONAL) geopoint field.
    #uploads each feature element to ElasticSearch using subprocess and cURL
    #es = Elasticsearch(ES_url)

    es = 'http://%s:%s@%s:9200' % (ES_username,ES_password,ES_url)

    if delete_index:
        try :
            p = subprocess.Popen(['curl','-XDELETE','%s/%s' % (es,index)])
            #subprocess.call('curl -XDELETE %s/%s' % (es,index))
            out, err = p.communicate()
            if err: print '\n' + err
        except Exception as e:
            print "Error deleting index:"
            print e
    print "\n\nPast Delete code\n\n"
    try:
        #create the index, set the replicas so uploads will not err out
        settings = {"settings":
                    {"number_of_replicas" : 1}
                    }
        p = subprocess.Popen(['curl','-XPUT','%s/%s' % (es,index),'-d',json.dumps(settings)])
        out, err = p.communicate()
        if err: print '\n' + err
        print "\n\nPast Create code\n\n"
        #build the mapping document
        mapping = {}
        mapping['properties'] = {}

        #if the data has a location field, set the geo_point mapping
        if geopoint: mapping['properties'][geopoint] = {'type':'geo_point','store':'yes'}
        if geoshape: mapping['properties'][geoshape] = {'type':'geo_shape','tree':'quadtree', 'precision': '1m'}

        #use cURL to put the mapping
        p = subprocess.Popen(['curl','%s/%s/_mapping/%s' % (es,index,doc_type),'-d','%s' % json.dumps(mapping)],stderr=subprocess.PIPE)
        out, err = p.communicate()
        if err: print '\n' + err
        print "\n\nPast Mapping code"
    except Exception as e:
        #do not try to recreate the index
        print "Error creating index:"
        print e


    #double-check that the correct number of replicas are being used
    p = subprocess.Popen(['curl','-XPUT','%s/%s/_settings' % (es,index),'-d','{"number_of_replicas": 1}'])
    out, err = p.communicate()
    if err: print '\n' + err

    
    #iterate through and upload individual documents
    succeeded=0
    failed=0
    idx=0
    for doc in docs:
        idx+=1
        #check if the document is a geojson document
        validation = geojson.is_valid(doc)
        if validation['valid'].lower() == 'yes':
            #add the point/shape to the document properties
            if geopoint: doc['properties'][geopoint] = list(geojson.utils.coords(doc))[0]
            if geoshape: doc['properties'][geoshape] = doc['geometry']
            #load the document properties into ES
            to_upload = deepcopy(doc['properties'])
        else:
            to_upload = deepcopy(doc)

        if id_field:
            _id = to_upload[id_field]
        else:
            _id=idx
        #up_doc = json.dumps(to_upload)
        #print up_doc
        #upload the document
        p = subprocess.Popen(['curl','-XPUT','%s/%s/%s/%s' % (es,index,doc_type,_id,),'-d','%s' % json.dumps(to_upload)],stderr=subprocess.PIPE)
        out, err = p.communicate()
        if not err:            
            succeeded+=1
        else:
            failed+=1
        
    print "Finished uploading documents. %s succeeded. %s failed." % (succeeded,failed)


def bulk_upload_docs_to_ES_cURL(docs,index,doc_type,id_field=False,geopoint=False,geoshape=False,delete_index=False):
    #input: list of JSON documents, an index name, document type, ID field, and name of an (OPTIONAL) geopoint field.
    #uploads each feature element to ElasticSearch using subprocess and cURL
    #es = Elasticsearch(ES_url)
    
    es = 'http://%s:%s@%s:9200' % (ES_username,ES_password,ES_url)

    if delete_index:
        try :
            p = subprocess.Popen(['curl','-XDELETE','%s/%s' % (es,index)])
            #subprocess.call('curl -XDELETE %s/%s' % (es,index))
            out, err = p.communicate()
            if err: print '\n' + err + '\n\n'
        except Exception as e:
            print "Error deleting index:"
            print e
    try:
        #create the index, set the replicas so uploads will not err out
        settings = {"settings": {"number_of_replicas" : 1} }
        p = subprocess.Popen(['curl','-XPUT','%s/%s' % (es,index),'-d',json.dumps(settings)])
        out, err = p.communicate()
        if err: print '\n' + err + '\n\n'
        
        #build the mapping document
        mapping = {}
        mapping['properties'] = {}

        #if the data has a location field, set the geo_point mapping
        if geopoint: mapping['properties'][geopoint] = {'type':'geo_point','store':'yes'}
        if geoshape: mapping['properties'][geoshape] = {'type':'geo_shape','tree':'quadtree', 'precision': '1m'}

        #use cURL to put the mapping
        p = subprocess.Popen(['curl','%s/%s/_mapping/%s' % (es,index,doc_type),'-d','%s' % json.dumps(mapping)],stderr=subprocess.PIPE)
        out, err = p.communicate()
        if err: print '\n' + err + '\n\n'
        
    except Exception as e:
        #do not try to recreate the index
        print "Error creating index:"
        print e


    #double-check that the correct number of replicas are being used
    p = subprocess.Popen(['curl','-XPUT','%s/%s/_settings' % (es,index),'-d','{"number_of_replicas": 1}'])
    out, err = p.communicate()
    if err: print '\n' + err + '\n\n'
        
    #iterate through and upload individual documents
    actions = []
    idx = 0
    bulk = 0
    for doc in docs:
        bulk+=1
        idx+=1
        #check if the document is a geojson document
        validation = geojson.is_valid(doc)
        if validation['valid'].lower() == 'yes':
            #add the point/shape to the document properties
            if geopoint: doc['properties'][geopoint] = list(geojson.utils.coords(doc))[0]
            if geoshape: doc['properties'][geoshape] = doc['geometry']
            #load the document properties into ES
            to_upload = deepcopy(doc['properties'])
        else:
            to_upload = deepcopy(doc)

        if id_field:
            _id = to_upload[id_field]
        else:
            _id=idx
        actions.append('{ "create" : {"_id" : "%s", "_type" : "%s", "_index" : "%s"} }\n' % (_id,doc_type,index))
        actions.append('%s\n' % json.dumps(to_upload))

        #upload 10k records at a time
        if bulk >= 10000:
            with open('bulk.txt','w') as bulk_file:
                bulk_file.writelines(actions) #write the actions to a file to be read by the bulk cURL command
            #upload the remaining records
            p = subprocess.Popen(['curl','-XPOST','%s/_bulk' % es,'--data-binary','@bulk.txt'],stderr=subprocess.PIPE)
            #p = subprocess.Popen(['curl','-XPOST','%s/_bulk' % es,'-d','%s' % ('').join(actions)],stderr=subprocess.PIPE)
            out, err = p.communicate()
            if err:            
                print err + '\n\n'
            bulk=0
            actions=[]
        
    #upload the remaining records
    with open('bulk.txt','w') as bulk_file:
        bulk_file.writelines(actions) #write the actions to a file to be read by the bulk cURL command
    p = subprocess.Popen(['curl','-XPOST','%s/_bulk' % es,'--data-binary','@bulk.txt'],stderr=subprocess.PIPE)
    #p = subprocess.Popen(['curl','-XPOST','%s/_bulk' % es,'-d','%s' % ('').join(actions)],stderr=subprocess.PIPE)
    out, err = p.communicate()
    if err:            
        print err
    os.remove('bulk.txt')
        
    
def upload_individual_docs_to_ES(docs,index,doc_type,id_field=False,geopoint=False,geoshape=False,delete_index=False):
    #input: list of JSON documents, an index name, document type, ID field, and name of an (OPTIONAL) geopoint field.
    #uploads each feature element to ElasticSearch
    #es = Elasticsearch(ES_url)

    es = Elasticsearch(['http://' + ES_username + ':' + ES_password + '@' + ES_url + ':9200/'])

    if delete_index:
        try :
            es.indices.delete(index=index, ignore=400)
        except :
            pass
    
    try:
        es.indices.create(index)
    except:
        #do not try to recreate the index
        pass
    
    #if the data has a location field, set the geo_point mapping
    if geopoint:
        try:
            mapping = {doc_type:{'properties':{geopoint:{'type':'geo_point','store':'yes'}}}}
            es.indices.put_mapping(index=index, doc_type=doc_type, body=mapping)
        except:
            pass

    if geoshape:
        try:
            mapping = {doc_type:{'properties':{geoshape:{'type':'geo_shape','tree':'quadtree', 'precision': '1m'}}}}
            es.indices.put_mapping(index=index, doc_type=doc_type, body=mapping)
        except:
            pass

    #iterate through and upload individual documents
    succeeded=0
    failed=0
    for doc in docs:
        #check if the document is a geojson document
        validation = geojson.is_valid(doc)
        if validation['valid'].lower() == 'yes':
            #add the point/shape to the document properties
            if geopoint: doc['properties'][geopoint] = list(geojson.utils.coords(doc))[0]
            if geoshape: doc['properties'][geoshape] = doc['geometry']
            #load the document properties into ES
            to_upload = deepcopy(doc['properties'])
        else:
            to_upload = deepcopy(doc)

        if id_field: _id = to_upload[id_field]
        #upload the document
        try:
            res = es.index(index=index, doc_type=doc_type, id=_id, body=to_upload)
            succeeded+=1
        except Exception as e:
            print "ERROR: " % str(e)
            failed+=1
        
    print "Finished uploading documents. %s succeeded. %s failed." % (succeeded,failed)

    
def upload_docs_to_ES(docs,index,doc_type,id_field=False,geopoint=False,geoshape=False,delete_index=False):
    #input: list of JSON documents, an index name, document type, ID field, and name of an (OPTIONAL) geopoint field.
    #uploads each feature element to ElasticSearch
    #es = Elasticsearch(ES_url)

    es = Elasticsearch(['http://' + ES_username + ':' + ES_password + '@' + ES_url + ':9200/'])

    if delete_index:
        try :
            es.indices.delete(index=index, ignore=400)
        except :
            pass
    
    try:
        es.indices.create(index)
    except:
        #do not try to recreate the index
        pass
    
    #if the data has a location field, set the geo_point mapping
    if geopoint:
        try:
            mapping = {doc_type:{'properties':{geopoint:{'type':'geo_point','store':'yes'}}}}
            es.indices.put_mapping(index=index, doc_type=doc_type, body=mapping)
        except:
            pass

    if geoshape:
        try:
            mapping = {doc_type:{'properties':{geoshape:{'type':'geo_shape','tree':'quadtree', 'precision': '1m'}}}}
            es.indices.put_mapping(index=index, doc_type=doc_type, body=mapping)
        except:
            pass

    actions = []
    #build the list of ElasticSearch uploads for bulk command
    index=0
    for doc in docs:
        action = {
            '_index': index,
            '_type': doc_type,
            }
        #check if the document is a geojson document
        validation = geojson.is_valid(doc)
        if validation['valid'].lower() == 'yes':
            #add the point/shape to the document properties
            if geopoint: doc['properties'][geopoint] = list(geojson.utils.coords(doc))[0]
            if geoshape: doc['properties'][geoshape] = doc['geometry']
            #get id from geojson properties document
            if id_field: action['_id'] = doc['properties'][id_field]      
            #load the document properties into ES
            action['_source'] = doc['properties']
        else:
            #assign id for typical json document
            if id_field: action['_id'] = doc[id_field]
            action['_source'] = doc
            
        actions.append(action)
        index +=1

        #upload 10k records at a time
        if index == 10000:
            try:
                helpers.bulk(es, actions)
                print 'Sucessfully uploaded %s records!' % str(len(actions))
                actions = []
                index=0
            except Exception as e:
                print '#### ERROR:s'
                pprint(e)
            
        
    
    #upload remaining 10k records        
    try:
        helpers.bulk(es, actions)
        print 'Sucessfully uploaded %s records!' % str(len(actions))
    except Exception as e:
        print '#### ERROR:'
        pprint(e)
    
def update_ES_records_curl(docs,index,doc_type,id_field):
    #Input: index name, doc type, existing record ID, and a document to post to ES
    #Output: posts the updated document to ES with cURL; uses upsert to add new docs

    es = 'http://%s:%s@%s:9200' % (ES_username,ES_password,ES_url)
    
##    #double-check that the correct number of replicas are being used
##    try:
##        p = subprocess.Popen(['curl','-XPOST','%s/%s/_settings' % (es,index),'-d','{"number_of_replicas": 1}'])
##        out, err = p.communicate()
##    except:
##        pass
##        
    #iterate through and upload individual documents
    actions = []
    idx = 0
    bulk = 0
    for doc in docs:
        bulk+=1
        idx+=1

        if id_field:
            _id = doc[id_field]
        else:
            _id=idx
        actions.append('{ "update" : {"_id" : "%s", "_type" : "%s", "_index" : "%s"} }\n' % (_id,doc_type,index))
        actions.append('{ "doc": %s, "doc_as_upsert" : true }\n' % json.dumps(doc))

        #upload 10k records at a time
        if idx >= 10000:
            with open('bulk.txt','w') as bulk_file:
                bulk_file.writelines(actions) #write the actions to a file to be read by the bulk cURL command
            #upload the remaining records
            p = subprocess.Popen(['curl','-XPOST','%s/_bulk' % es,'--data-binary','@bulk.txt'],stderr=subprocess.PIPE)
            out, err = p.communicate()
            if err:            
                print err + '\n\n'
            bulk=0
            actions=[]
        
    #upload the remaining records
    with open('bulk.txt','w') as bulk_file:
        bulk_file.writelines(actions) #write the actions to a file to be read by the bulk cURL command
    p = subprocess.Popen(['curl','-XPOST','%s/_bulk' % es,'--data-binary','@bulk.txt'],stderr=subprocess.PIPE)
    out, err = p.communicate()
    if err:            
        print err
    #os.remove('bulk.txt')
        
    
    
def delete_ES_records(index,doc_type):
    #deletes all ElasticSearch records for an index (recrusively runs until index is empty
    es = Elasticsearch(ES_url) #connect to ElasticSearch instance

    try:
        records = [res['_id'] for res in es.search(index)['hits']['hits']] #list of all WBAN station ID's
        if len(records) > 0:
            deleted = 0
            for rec in records:
                es.delete(index=index,doc_type=doc_type,id=rec)
                deleted+=1

            print 'Sucessfully deleted: %s' % deleted
            delete_ES_records(index,doc_type)
        else:
            return
            
    except Exception as e:
        print '#### ERROR: %s' % e
        
        
   
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


def get_first_record(index,doc_type,datetime_field):
    #input: index, doc_type, and a date-time field
    #output: returns the latest ES record based on the datetime field
    
    es_url = 'http://%s:%s@%s:9200' % (ES_username,ES_password,ES_url)
    es = Elasticsearch(es_url)

    query = '{"sort": [ { "%s":   { "order": "asc" }} ] }' % datetime_field

    try:
        return helpers.scan(es,index=index,doc_type=doc_type,query=query,preserve_order=True).next()['_source']
    except:
        return None
