var express = require('express');
var elasticsearch = require('elasticsearch');
var http = require('http');


var router = express.Router();
var hostsIP = [
                        'http://accident:Dav1dC0C0@169.53.138.92:9200'
                ];

/* GET home page. */
router.get('/', function(req, res, next) {
  res.render('index', { title: 'Express' });
});

router.get('/predict', function (req, res) {

	var client = new elasticsearch.Client({
		hosts: hostsIP,
  		apiVersion: '2.2'
	});

client.search({
  index: 'saferoad_results',
  type: 'rows',
  from: 0,
  size: 250,
  body: {
    query: {
      match: {
        grid_dateHourStr: req.query.dateHourStr
      }
    },
    sort: [
        { "probability" : {"order" : "desc"}}
    ]
  }
}).then(function (resp) {

   res.setHeader('Content-Type', 'application/json');
   index = 0
   var arr = new Array();

   for (i = 0; i < resp.hits.hits.length; i++) {
	var item = resp.hits.hits[i]["_source"];
	item["rank"] = i;
	arr.push(item);
   }

   //res.send(JSON.stringify(resp.hits.hits));
   res.send(JSON.stringify(arr));
   //console.log(resp.hits.hits.length);
}, function (err) {
    console.trace(err.message);
});
/*
	client.ping({
  requestTimeout: 30000,

  // undocumented params are appended to the query string
  hello: "elasticsearch"
}, function (error) {
  if (error) {
    console.error('elasticsearch cluster is down!');
  } else {
    console.log('All is well');
  }
});
*/
 
});

router.get('/tenDayTrend', function (req, res) {

        var client = new elasticsearch.Client({
                hosts: hostsIP,
                apiVersion: '2.2'
        });

client.search({
  index: 'saferoad_results',
  type: 'rows',
  body:
{
    "size" : 0,
    "query" : {
        "filtered": {
            "filter": {
               "and" : [
                  {
                   "range": {
                     "grid_fullDate": {
                         "gte": req.query.lower,
                         "lte": req.query.upper
                      }
                    }
                  },
                  {
                       "range": {
                          "probability": {
                             "gt": req.query.threshold
                           }
                        }
                  } 
                  
                ]
            }
        }
    },
    "aggs" : {
        "dateHour" : {
            "date_histogram" : {
                "field" : "grid_fullDate",
                "interval" : "hour",
                "min_doc_count" : 0,
                "extended_bounds" : { 
                    "min" : req.query.lower,
                    "max" : req.query.upper
                 }
            }
        }
    }
} 
}).then(function (resp) {
   //console.log(resp["aggregations"]["dateHour"]["buckets"])
   res.setHeader('Content-Type', 'application/json');
   //res.send(JSON.stringfy(resp));
   var arr = new Array();

   for (i = 0; i < resp["aggregations"]["dateHour"]["buckets"].length; i++) {
        arr.push(resp["aggregations"]["dateHour"]["buckets"][i]);
   }

   res.send(JSON.stringify(arr));
}, function (err) {
    console.trace(err.message);
});
});


router.get('/getZipcodeInfo', function (req, res) {
        var client = new elasticsearch.Client({
                hosts: hostsIP,
                apiVersion: '2.2'
        });

client.search({
  index: 'saferoad_results',
  type: 'rows',
  body: 
{
    "query" : {
        "filtered": {
            "filter": {
               "and" : [
                  {
                   "term": {
                     "grid_zipcode": req.query.zipcode
                    }
                  },
                  {	
                   "term": {
                     "grid_dateHourStr": req.query.dateHourStr
                    }
                  }  
                ]
            }
        }
    }
}
}).then(function (resp) {
   res.setHeader('Content-Type', 'application/json');
   res.send(JSON.stringify(resp.hits.hits[0]["_source"]));
}, function (err) {
    console.trace(err.message);
});

});

module.exports = router;
