var express = require('express');
var elasticsearch = require('elasticsearch');
var http = require('http');
var Q = require("q");

var router = express.Router();
var hostsIP = [
		'es1',
'es2'

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

	var probaType = req.query.accidentType;

	switch (req.query.accidentType) {
	    case "PEDS":
	        probaType = "pedestrian_probability";
	        break;
	    case "CYC":
	      	probaType = "cyclist_probability";
	        break;
	    case "MOTO":
	        probaType = "motorist_probability";
	        break;
	    default:
		probaType = "all_probability";
	        break;
	}

// default is get average of 24 hours
var filterBody = {
                   "wildcard": {
                     "grid_dateHourStr": req.query.dateStr + "*"
                    }
                  }

if (req.query.fromHour != undefined) {

     //they specified custom date/hour
	filterBody = {
                   "range": {
                     "grid_fullDate": {
                         "gte": req.query.fromHour,
                         "lt": req.query.toHour
                      }
                    }
                  }

}

client.search({
  index: 'saferoad_results',
  type: 'rows',
  body:
{
    "size" : 0,
    "query" : {
        "filtered": {
            "filter": {
               "and": [
			filterBody
                ]
            }
        }
    },
    "aggs": {
        "group_by_zipcode": {
           "terms": {
                "field": "grid_zipcode",
                "size" : 0,
                "order": {
                     "average_probability": "desc"
                 }
            },
            "aggs": {
                 "average_probability": {
                     "avg": {
                        "field": probaType
                     }
                  }
            }
        }
    }
}
}).then(function (resp) {

	try {
   var arr = new Array();

   res.setHeader('Content-Type', 'application/json');
   index = 0
   var arr = new Array();
   for (i = 0; i < resp["aggregations"]["group_by_zipcode"]["buckets"].length; i++) {
	var item = resp["aggregations"]["group_by_zipcode"]["buckets"][i];
	var newItem = { "rank" : i, "probability" : item["average_probability"]["value"], "grid_zipcode" : item["key"]}
	arr.push(newItem);
   }

   //res.send(JSON.stringify(resp.hits.hits));
   res.send(JSON.stringify(arr));
   //console.log(resp.hits.hits.length);
 } catch (e) {
	 console.log(e);
 }
}, function (err) {
    console.trace(err.message);
});

});

router.get('/tenDayTrend', function (req, res) {


	var probaType = req.query.accidentType;

	switch (req.query.accidentType) {
	    case "PEDS":
	        probaType = "pedestrian_probability";
	        break;
	    case "CYC":
	        probaType = "cyclist_probability";
	        break;
	    case "MOTO":
	        probaType = "motorist_probability";
	        break;
	    default:
	        probaType = "all_probability";
	        break;
	}

	//console.log(probaType);
	        var client = new elasticsearch.Client({
	                hosts: hostsIP,
	                apiVersion: '2.2'
	        });


  var rangeContent = {};
rangeContent[probaType] = {
                             "gt": req.query.threshold
                           };


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
                       "range": rangeContent /*{
                          probaType: {
                             "gt": req.query.threshold
                           }
                        }*/
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
	try {
   res.send(JSON.stringify(resp["aggregations"]["dateHour"]["buckets"]));
 } catch (e) {
	 console.log(e);
 }

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

		try {
   res.setHeader('Content-Type', 'application/json');
   res.send(JSON.stringify(resp.hits.hits[0]["_source"]));

 } catch (e) {

	 console.log(e);
 }
}, function (err) {
    console.trace(err.message);
});

});

router.get('/collisionsByZipcode', function (req, res) {

        var client = new elasticsearch.Client({
                hosts: hostsIP,
                apiVersion: '2.2'
        });


				var filterBody = {};

				switch (req.query.accidentType) {
				    case "PEDS":
				        filterBody = {
										"or" : [
										  {
												"range": {
														 "collision_NUMBER OF PEDESTRIANS INJURED": {
																"gt": 0
															}
													 }
										  },
											{
												"range": {
														 "collision_NUMBER OF PEDESTRIANS KILLED": {
																"gt": 0
															}
													 }
										  }
									]
								 }
				        break;
				    case "CYC":
							filterBody = {
									"or" : [
										{
											"range": {
													 "collision_NUMBER OF CYCLIST INJURED": {
															"gt": 0
														}
												 }
										},
										{
											"range": {
													 "collision_NUMBER OF CYCLIST KILLED": {
															"gt": 0
														}
												 }
										}
								]
							};
				        break;
				    case "MOTO":
							filterBody = {
									"or" : [
										{
											"range": {
													 "collision_NUMBER OF MOTORIST INJURED": {
															"gt": 0
														}
												 }
										},
										{
											"range": {
													 "collision_NUMBER OF MOTORIST KILLED": {
															"gt": 0
														}
												 }
										}
								]
							};
				      break;
				    default: //all
							filterBody = {
								"range" : {
								 "collision_injured_or_killed": {
										"gt": 0
									}
								}
							 };
							 break;
				}

var allTitles = [];

	console.log(filterBody);

// first we do a search, and specify a scroll timeout
client.search({
  index: 'saferoad',
  type: 'collisions',
  // Set to 30 seconds because we are calling right back
  scroll: '30s',
  search_type: 'scan',
  body: {
  	size: 10000,
  	"_source": ["collision_GEOSHAPE_C"],
  	"query" : {
        "filtered": {
            "filter": {
               "and" : [
								 filterBody,
                  {
                     "term": {
                          collision_ZCTA_ZIP_NoSuffix: req.query.zipcode //"11101"
                       }
                  }


                ]
            }
        }
    }}
}, function getMoreUntilDone(error, response) {

	try {
	  // collect the title from each response
	  response.hits.hits.forEach(function (hit) {
	    //allTitles.push({ "type": "Point", "coordinates": hit["_source"]["collision_GEOSHAPE_C"]["coordinates"]});
	   //allTitles.push(hit["_source"]["collision_GEOSHAPE_C"]["coordinates"]);
	       allTitles.push([hit["_source"]["collision_GEOSHAPE_C"]["coordinates"][1],hit["_source"]["collision_GEOSHAPE_C"]["coordinates"][0]]);
	//console.log(hit);
	  });
	  //console.log(response.hits.total)
	  //console.log(allTitles.length)
	  if (response.hits.total !== allTitles.length) {
	    // now we can call scroll over and over
	    client.scroll({
	      scrollId: response._scroll_id,
	      scroll: '30s'
	    }, getMoreUntilDone);
	  } else {
			res.setHeader('Content-Type', 'application/json');
	 		res.send(JSON.stringify(allTitles));
	  }
	} catch (e) {
		console.log(e);
	}
});
}, function (err) {
    console.trace(err.message);
});

router.get('/getCurrentStats', function (req, res) {
        var client = new elasticsearch.Client({
                hosts: hostsIP,
                apiVersion: '2.2'
        });

				client.search({
					index: 'saferoad',
				  type: 'collisions',
				  body:
					{
				  "size": 1,
				  "sort": [
				    {
				      "collision_DATETIME_C": {
				        "order": "desc"
				      }
				    }
				  ],
				  fields : ["collision_DATETIME_C"]

					}

				}).then(function (resp) {

					 //must concert to this format: 2016-04-15T00:00:00
				   var uc = resp["hits"]["hits"][0]["fields"]["collision_DATETIME_C"][0];
           var year = uc.split("-")[0];
					 var lc = year + "-01-01T00:00:00"
					 var lastyear = parseInt(year,10) -1;
					 var ul = new String(lastyear) + uc.substring(uc.indexOf("-"));
					 var ll = lastyear + "-01-01T00:00:00"

					 //console.log(uc);
					 //console.log(lc);
					 //console.log(ul);
					 //console.log(ll);
					 //ll:2015-01-01T00:00:00

					 //uc="2016-04-14T00:00:00";
					 //lc="2016-01-01T00:00:00"
					 //ul="2015-04-14T00:00:00";
					 //ll="2015-01-01T00:00:00";

					client.search({
					  index: 'nyc_grid',
					  type: 'rows',
					  body:
					{
					    "size" : 0,
					    "query" : {
					        "filtered": {
					            "filter": {
					               "or": [
					                   {
					                      "range": {
					                         "grid_fullDate": {
					                             "gte": lc,
					                             "lte": uc
					                          }
					                       }
					                  },
					                  {
					                      "range": {
					                         "grid_fullDate": {
					                             "gte": ll,
					                             "lt": ul
					                          }
					                       }
					                  }
					                ]
					            }
					        }
					    },
					    "aggs" : {

					             "group_by_year": {
					                  "terms": {
					                       "field": "grid_year"
					                   },
					                   "aggs": {
					                       "moto_injuries" : { "sum" : { "field" : "grid_motoristInjuries" } },
					                       "moto_fatalities" : { "sum" : { "field" : "grid_motoristFatalities" } },
					                       "cyc_injuries" : { "sum" : { "field" : "grid_cyclistInjuries" } },
					                       "cyc_fatalities" : { "sum" : { "field" : "grid_cyclistFatalities" } },
					                       "ped_injuries" : { "sum" : { "field" : "grid_pedestrianInjuries" } },
					                       "ped_fatalities" : { "sum" : { "field" : "grid_pedestrianFatalities" } }
					                   }
							}
					        }
					   }

					}).then(function (resp2) {

						try {
					   res.setHeader('Content-Type', 'application/json');
						 var result = {latest: uc, buckets: resp2["aggregations"]["group_by_year"]["buckets"]}
					   res.send(result);
					 } 	catch (e) {
					 		console.log(e);
					 	}
					}, function (err) {
					    console.trace(err.message);
					});

}, function (err) {
		console.trace(err.message);
})
});

router.get('/getUpdateDate', function (req, res) {
        var client = new elasticsearch.Client({
                hosts: hostsIP,
                apiVersion: '2.2'
        });

client.search({
  index: 'saferoad_update',
  type: 'update_date',
}).then(function (resp) {

	try {
   console.log(resp);
   res.setHeader('Content-Type', 'application/json');
   res.send(JSON.stringify(resp.hits.hits[0]["_source"]["Model_Update_FullDate"]));
 } 	catch (e) {
		console.log(e);
	}
}, function (err) {
    console.trace(err.message);
});

});

router.get('/diag', function (req, res) {
        var client = new elasticsearch.Client({
                hosts: hostsIP,
                apiVersion: '2.2'
        });

client.search({
  index: 'saferoad_diagnostics',
  type: 'diagnostics',
}).then(function (resp) {

	try {
   //console.log(resp);
   res.setHeader('Content-Type', 'application/json');

		var hits = resp.hits.hits[0]["_source"];
		hits = mergeROCData(hits,"all");
		hits = mergeROCData(hits,"cyclist");
		hits = mergeROCData(hits,"pedestrian");
		hits = mergeROCData(hits,"motorist");
		res.send(JSON.stringify(hits));
		}	catch (e) {
	 		console.log(e);
 		}

}, function (err) {
    console.trace(err.message);
});

});

function mergeROCData(hits, type)
{
 var allROC = [];
for (var i = 0; i < hits[type + "_ROC"]["TPR"].length; i++) {
//for (var i = 0; i < 10; i++) {
        allROC.push({tpr: hits[type + "_ROC"]["TPR"][i], fpr: hits[type + "_ROC"]["FPR"][i]});
   }
//   res.send(JSON.stringify(allROC));
//   console.log(allROC);
delete hits[type + "_ROC"];
hits[type + "_ROC_merged"] = allROC;

	return hits;
}

router.get('/importance', function (req, res) {
        var client = new elasticsearch.Client({
                hosts: hostsIP,
                apiVersion: '2.2'
        });

client.search({
  index: 'prediction_features',
  type: 'importance',
  body:
{
  "size": 0,
  "aggs":{
    "by_type":{
      "terms": {
        "field": "entityType",
        "size": 0
      },
      "aggs": {
        "tops": {
          "top_hits": {
            "size": 100,
            "sort" : [
                { "Rank" : {"order" : "asc"}}
             ]
          }
        }
      }
    }
  }
}
}).then(function (resp) {

	try {
   	//console.log(resp);
   	res.setHeader('Content-Type', 'application/json');
   	res.send(JSON.stringify(resp["aggregations"]["by_type"]["buckets"]));
 	} 	catch (e) {
		console.log(e);
	}

}, function (err) {
    console.trace(err.message);
});

});


function getWeather(client,zipcode, dateStr,fromHour,toHour) {
	var filterBody = {
                   "wildcard": {
                     "pred_id": "2016" + dateStr + "*"
                    }}

	if (fromHour != undefined) {

	     //they specified custom date/hour
		filterBody = { "range": {
                     "pred_fullDate": {
                         "gte": fromHour,
                         "lt": toHour
                      }
                    }}
	}

	return client.search({
		index: 'wunderground',
		type: 'hourly',
		body:
			{
	    "query" : {
	        "filtered": {
	            "filter": {
	               "and": [
	                    filterBody,
	                    {
	                    "term" : {
	                     "pred_zipcode" : zipcode
	                    }}

	                ]
	            }
	        }
	    }
		}
	});
}

function getPredictionRows(client,zipcode, dateStr,fromHour,toHour) {
	var filterBody = {
                   "wildcard": {
                     "grid_dateHourStr": dateStr + "*"
                    }}

	if (fromHour != undefined) {

	     //they specified custom date/hour
		filterBody = { "range": {
                     "grid_fullDate": {
                         "gte": fromHour,
                         "lt": toHour
                      }
                    }}
	}

	return client.search({
		index: 'saferoad_results',
		type: 'rows',
		body:
			{
	    "query" : {
	        "filtered": {
	            "filter": {
	               "and": [
	                    filterBody,
	                    {
	                    "term" : {
	                     "grid_zipcode" : zipcode
	                    }}

	                ]
	            }
	        }
	    }
		}
	});
}

//returns most frequent
function mode(array)
{
    if(array.length == 0)
    	return null;
    var modeMap = {};
    var maxEl = array[0], maxCount = 1;
    for(var i = 0; i < array.length; i++)
    {
    	var el = array[i];
    	if(modeMap[el] == null)
    		modeMap[el] = 1;
    	else
    		modeMap[el]++;
    	if(modeMap[el] > maxCount)
    	{
    		maxEl = el;
    		maxCount = modeMap[el];
    	}
    }
    return maxEl;
}

function findMedian(data) {

    // extract the .values field and sort the resulting array
    var m = data.sort(function(a, b) {
        return a - b;
    });

    var middle = Math.floor((m.length - 1) / 2); // NB: operator precedence
    if (m.length % 2) {
        return m[middle];
    } else {
        return (m[middle] + m[middle + 1]) / 2.0;
    }
}


//woohoo check out parallel elastic search calls!
//sample    dateStr=0415&zipcode=10007
router.get('/getInfoForZipcode', function (req, res) {
        var client = new elasticsearch.Client({
                hosts: hostsIP,
                apiVersion: '2.2'
        });

				var promise = Q.all([
					getWeather(client, req.query.zipcode, req.query.dateStr, req.query.fromHour,req.query.toHour),
					getPredictionRows(client, req.query.zipcode, req.query.dateStr, req.query.fromHour,req.query.toHour)
				]);

				promise.spread(function (weather,preds) {

					/*following prop needs to be retrieved, most of them can be retrieved from first row, but "*" ones we need to take avg/most frequent
					Datetime of Wunderground(pred_fullDate)
*Weather - logo 	Wunderground (icon_url)
*Weather - condition 	Wunderground (conditionl)
*Weather - temperature 	Wunderground (temp)
*Weather - wind speed  	Wunderground (windspeed)

Speed breakdown  saferoad_results(all of them)
Bridges		saferoad_results(bridges)
Tunnels	 saferoad_results(tunnels)
*311 results saferoad_results(road_cond_requests)
*Liquor licenss saferoad_results(liquor_licenses)
Total road count 	saferoad_results(total_road_count)
Total road length 		saferoad_results(total_road_length)
Zip_area  saferoad_results(zip_area)
Average AADT saferoad_results(Average AADT)
Median AADT saferoad_results(Average AADT)

					*/
					try {
							var result;
							var roadRequestArray = new Array();
							var liquorLicensesArray = new Array();

							//get median road consition requests and liquor licenses
							for (var i =0; i< preds["hits"]["hits"].length; i++) {
								  var currItem = preds["hits"]["hits"][i];
									//console.log(currItem);
									if (i ==0){
											result = currItem["_source"];

											//put together speed limits into array
											speedLimitArray = []; //format  { "speed": "5mph", 	"percentage" : 0.035}
											speedLimitArray.push({"speed": "5mph",	"percentage": result["5mph"] });
											speedLimitArray.push({"speed": "15mph",	"percentage": result["15mph"] });
											speedLimitArray.push({"speed": "25mph",	"percentage": result["25mph"] });
											speedLimitArray.push({"speed": "35mph",	"percentage": result["35mph"] });
											speedLimitArray.push({"speed": "45mph",	"percentage": result["45mph"] });
											speedLimitArray.push({"speed": "55mph",	"percentage": result["55mph"] });
											speedLimitArray.push({"speed": "65mph",	"percentage": result["65mph"] });
											result["speed_limits"] = speedLimitArray;
									}

									try {
										roadRequestArray.push(parseInt(currItem["_source"]["road_cond_requests"],10));
									} catch (e){

									}

									try {
										liquorLicensesArray.push(parseInt(currItem["_source"]["liquor_licenses"],10));
									} catch (e){

									}
							}

							result["road_cond_requests"] = findMedian(roadRequestArray);
							result["liquor_licenses"] = findMedian(liquorLicensesArray);

							//now get average/mode weather conditions
							var logoArray = new Array(); 	//Wunderground (icon_url)
							var conditionArray = new Array(); //	Wunderground (conditionl)
							var temperatureArray = new Array(); 	//Wunderground (temp)
							var windArray = new Array(); // 	Wunderground (windspeed)
							for (var i =0; i< weather["hits"]["hits"].length; i++) {
									var currItem = weather["hits"]["hits"][i]["_source"];
									logoArray.push(currItem["icon_url"]);
									conditionArray.push(currItem["condition"]);

									try { //just in case temperature is NA or sth
										temperatureArray.push(parseInt(currItem["temp"],10));
									} catch (e){

									}

									try { //just in case temperature is NA or sth
										windArray.push(parseInt(currItem["windspeed"],10));
									} catch (e){

									}
							}

							result["icon_url"] = mode(logoArray);
							result["condition"] = mode(conditionArray);
							result["temp"] = findMedian(temperatureArray);
							result["windspeed"] = findMedian(windArray);

							res.setHeader('Content-Type', 'application/json');
							res.send(JSON.stringify(result));
						} catch (e){
							console.log(e);
						}
				})
				.done();


});



module.exports = router;
