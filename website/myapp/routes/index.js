var express = require('express');
var elasticsearch = require('elasticsearch');
var http = require('http');


var router = express.Router();

/* GET home page. */
router.get('/', function(req, res, next) {
  res.render('index', { title: 'Express' });
});

router.get('/predict', function (req, res) {

var lineReader = require('readline').createInterface({
  input: require('fs').createReadStream('temp.txt')
});

lineReader.on('line', function (line) {
  console.log('Line from file:', line);
});
	var client = new elasticsearch.Client({
		hosts: [
    			'',
    			''
  		],
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

module.exports = router;
