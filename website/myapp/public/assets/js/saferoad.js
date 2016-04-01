jQuery.fn.d3MouseOver = function () {
  this.each(function (i, e) {
    var evt = new MouseEvent("mouseover");
    e.dispatchEvent(evt);
  });
};

jQuery.fn.d3MouseOut = function () {
  this.each(function (i, e) {
    var evt = new MouseEvent("mouseout");
    e.dispatchEvent(evt);
  });
};


//var data = [{'zipcode': '11433', 'rank': 1, 'prob': 70.18460447877358}, {'zipcode': '11224', 'rank': 2, 'prob': 70.18460447877358}, {'zipcode': '11434', 'rank': 3, 'prob': 70.18460447877358}, {'zipcode': '11229', 'rank': 4, 'prob': 70.18460447877358}, {'zipcode': '11230', 'rank': 5, 'prob': 70.18460447877358}, {'zipcode': '11411', 'rank': 6, 'prob': 70.18460447877358}, {'zipcode': '11232', 'rank': 7, 'prob': 70.18460447877358}, {'zipcode': '10305', 'rank': 8, 'prob': 70.18460447877358}, {'zipcode': '11235', 'rank': 9, 'prob': 70.18460447877358}, {'zipcode': '11236', 'rank': 10, 'prob': 70.18460447877358}, {'zipcode': '11416', 'rank': 11, 'prob': 70.18460447877358}, {'zipcode': '10006', 'rank': 12, 'prob': 70.18460447877358}, {'zipcode': '11207', 'rank': 13, 'prob': 70.18460447877358}, {'zipcode': '11208', 'rank': 14, 'prob': 70.18460447877358}, {'zipcode': '11692', 'rank': 15, 'prob': 70.18460447877358}, {'zipcode': '11422', 'rank': 16, 'prob': 70.18460447877358}, {'zipcode': '11423', 'rank': 17, 'prob': 70.18460447877358}, {'zipcode': '11004', 'rank': 18, 'prob': 70.18460447877358}, {'zipcode': '11697', 'rank': 19, 'prob': 70.18460447877358}, {'zipcode': '11428', 'rank': 20, 'prob': 70.18460447877358}, {'zipcode': '11218', 'rank': 21, 'prob': 70.18460447877358}, {'zipcode': '11219', 'rank': 22, 'prob': 70.18460447877358}, {'zipcode': '11102', 'rank': 23, 'prob': 69.13856809350749}, {'zipcode': '10024', 'rank': 24, 'prob': 69.13856809350749}, {'zipcode': '11109', 'rank': 25, 'prob': 69.13856809350749}, {'zipcode': '10031', 'rank': 26, 'prob': 69.13856809350749}, {'zipcode': '10002', 'rank': 27, 'prob': 69.13856809350749}, {'zipcode': '10453', 'rank': 28, 'prob': 69.13856809350749}, {'zipcode': '10456', 'rank': 29, 'prob': 69.13856809350749}, {'zipcode': '10038', 'rank': 30, 'prob': 69.13856809350749}, {'zipcode': '10309', 'rank': 31, 'prob': 69.13856809350749}, {'zipcode': '10009', 'rank': 32, 'prob': 69.13856809350749}, {'zipcode': '10013', 'rank': 33, 'prob': 69.13856809350749}, {'zipcode': '10464', 'rank': 34, 'prob': 69.13856809350749}, {'zipcode': '11217', 'rank': 35, 'prob': 69.13856809350749}, {'zipcode': '11369', 'rank': 36, 'prob': 69.13856809350749}, {'zipcode': '10021', 'rank': 37, 'prob': 65.3830683307725}, {'zipcode': '11221', 'rank': 38, 'prob': 65.3830683307725}, {'zipcode': '11372', 'rank': 39, 'prob': 65.3830683307725}, {'zipcode': '10173', 'rank': 40, 'prob': 65.3830683307725}, {'zipcode': '10475', 'rank': 41, 'prob': 65.3830683307725}, {'zipcode': '11225', 'rank': 42, 'prob': 65.3830683307725}, {'zipcode': '11106', 'rank': 43, 'prob': 65.3830683307725}, {'zipcode': '10028', 'rank': 44, 'prob': 65.3830683307725}, {'zipcode': '10001', 'rank': 45, 'prob': 65.3830683307725}, {'zipcode': '10034', 'rank': 46, 'prob': 65.3830683307725}, {'zipcode': '11415', 'rank': 47, 'prob': 65.3830683307725}, {'zipcode': '10065', 'rank': 48, 'prob': 65.3830683307725}, {'zipcode': '11355', 'rank': 49, 'prob': 65.3830683307725}, {'zipcode': '10457', 'rank': 50, 'prob': 65.3830683307725}, {'zipcode': '11359', 'rank': 51, 'prob': 65.3830683307725}, {'zipcode': '10010', 'rank': 52, 'prob': 65.3830683307725}, {'zipcode': '11361', 'rank': 53, 'prob': 65.3830683307725}, {'zipcode': '10312', 'rank': 54, 'prob': 65.3830683307725}, {'zipcode': '10162', 'rank': 55, 'prob': 65.3830683307725}, {'zipcode': '10012', 'rank': 56, 'prob': 65.3830683307725}, {'zipcode': '11213', 'rank': 57, 'prob': 65.3830683307725}, {'zipcode': '10103', 'rank': 58, 'prob': 65.3830683307725}, {'zipcode': '11365', 'rank': 59, 'prob': 65.3830683307725}, {'zipcode': '10168', 'rank': 60, 'prob': 65.3830683307725}, {'zipcode': '10468', 'rank': 61, 'prob': 65.3830683307725}, {'zipcode': '10020', 'rank': 62, 'prob': 62.05641746686686}, {'zipcode': '10470', 'rank': 63, 'prob': 62.05641746686686}, {'zipcode': '11430', 'rank': 64, 'prob': 62.05641746686686}, {'zipcode': '11370', 'rank': 65, 'prob': 62.05641746686686}, {'zipcode': '11220', 'rank': 66, 'prob': 62.05641746686686}, {'zipcode': '10170', 'rank': 67, 'prob': 62.05641746686686}, {'zipcode': '10110', 'rank': 68, 'prob': 62.05641746686686}, {'zipcode': '10171', 'rank': 69, 'prob': 62.05641746686686}, {'zipcode': '11101', 'rank': 70, 'prob': 62.05641746686686}, {'zipcode': '10112', 'rank': 71, 'prob': 62.05641746686686}, {'zipcode': '10022', 'rank': 72, 'prob': 62.05641746686686}, {'zipcode': '10023', 'rank': 73, 'prob': 62.05641746686686}, {'zipcode': '11223', 'rank': 74, 'prob': 62.05641746686686}, {'zipcode': '11373', 'rank': 75, 'prob': 62.05641746686686}, {'zipcode': '10474', 'rank': 76, 'prob': 62.05641746686686}, {'zipcode': '11374', 'rank': 77, 'prob': 62.05641746686686}, {'zipcode': '10174', 'rank': 78, 'prob': 62.05641746686686}, {'zipcode': '11105', 'rank': 79, 'prob': 62.05641746686686}, {'zipcode': '11435', 'rank': 80, 'prob': 62.05641746686686}, {'zipcode': '11375', 'rank': 81, 'prob': 62.05641746686686}, {'zipcode': '10027', 'rank': 82, 'prob': 62.05641746686686}, {'zipcode': '11228', 'rank': 83, 'prob': 62.05641746686686}, {'zipcode': '11379', 'rank': 84, 'prob': 62.05641746686686}, {'zipcode': '10451', 'rank': 85, 'prob': 62.05641746686686}, {'zipcode': '10271', 'rank': 86, 'prob': 62.05641746686686}, {'zipcode': '10301', 'rank': 87, 'prob': 62.05641746686686}, {'zipcode': '11231', 'rank': 88, 'prob': 62.05641746686686}, {'zipcode': '10152', 'rank': 89, 'prob': 62.05641746686686}, {'zipcode': '10032', 'rank': 90, 'prob': 62.05641746686686}, {'zipcode': '11412', 'rank': 91, 'prob': 62.05641746686686}, {'zipcode': '10302', 'rank': 92, 'prob': 62.05641746686686}, {'zipcode': '11203', 'rank': 93, 'prob': 62.05641746686686}, {'zipcode': '10033', 'rank': 94, 'prob': 62.05641746686686}, {'zipcode': '11413', 'rank': 95, 'prob': 62.05641746686686}, {'zipcode': '11234', 'rank': 96, 'prob': 62.05641746686686}, {'zipcode': '11204', 'rank': 97, 'prob': 62.05641746686686}, {'zipcode': '10004', 'rank': 98, 'prob': 62.05641746686686}, {'zipcode': '10454', 'rank': 99, 'prob': 62.05641746686686}, {'zipcode': '11354', 'rank': 100, 'prob': 62.05641746686686}, {'zipcode': '10005', 'rank': 101, 'prob': 62.05641746686686}, {'zipcode': '10455', 'rank': 102, 'prob': 62.05641746686686}, {'zipcode': '11385', 'rank': 103, 'prob': 62.05641746686686}, {'zipcode': '10036', 'rank': 104, 'prob': 62.05641746686686}, {'zipcode': '10307', 'rank': 105, 'prob': 62.05641746686686}, {'zipcode': '10037', 'rank': 106, 'prob': 62.05641746686686}, {'zipcode': '10308', 'rank': 107, 'prob': 62.05641746686686}, {'zipcode': '10128', 'rank': 108, 'prob': 62.05641746686686}, {'zipcode': '10458', 'rank': 109, 'prob': 62.05641746686686}, {'zipcode': '11358', 'rank': 110, 'prob': 62.05641746686686}, {'zipcode': '11239', 'rank': 111, 'prob': 62.05641746686686}, {'zipcode': '10279', 'rank': 112, 'prob': 62.05641746686686}, {'zipcode': '10310', 'rank': 113, 'prob': 62.05641746686686}, {'zipcode': '11210', 'rank': 114, 'prob': 62.05641746686686}, {'zipcode': '10040', 'rank': 115, 'prob': 62.05641746686686}, {'zipcode': '11691', 'rank': 116, 'prob': 62.05641746686686}, {'zipcode': '10011', 'rank': 117, 'prob': 62.05641746686686}, {'zipcode': '11362', 'rank': 118, 'prob': 62.05641746686686}, {'zipcode': '10282', 'rank': 119, 'prob': 62.05641746686686}, {'zipcode': '10462', 'rank': 120, 'prob': 62.05641746686686}, {'zipcode': '11363', 'rank': 121, 'prob': 62.05641746686686}, {'zipcode': '11424', 'rank': 122, 'prob': 62.05641746686686}, {'zipcode': '11364', 'rank': 123, 'prob': 62.05641746686686}, {'zipcode': '10044', 'rank': 124, 'prob': 62.05641746686686}, {'zipcode': '10314', 'rank': 125, 'prob': 62.05641746686686}, {'zipcode': '11214', 'rank': 126, 'prob': 62.05641746686686}, {'zipcode': '11694', 'rank': 127, 'prob': 62.05641746686686}, {'zipcode': '10465', 'rank': 128, 'prob': 62.05641746686686}, {'zipcode': '11215', 'rank': 129, 'prob': 62.05641746686686}, {'zipcode': '10016', 'rank': 130, 'prob': 62.05641746686686}, {'zipcode': '11427', 'rank': 131, 'prob': 62.05641746686686}, {'zipcode': '11367', 'rank': 132, 'prob': 62.05641746686686}, {'zipcode': '10167', 'rank': 133, 'prob': 62.05641746686686}, {'zipcode': '11368', 'rank': 134, 'prob': 62.05641746686686}, {'zipcode': '10469', 'rank': 135, 'prob': 62.05641746686686}, {'zipcode': '10471', 'rank': 136, 'prob': 41.240159510237376}, {'zipcode': '11104', 'rank': 137, 'prob': 41.240159510237376}, {'zipcode': '10026', 'rank': 138, 'prob': 41.240159510237376}, {'zipcode': '11378', 'rank': 139, 'prob': 41.240159510237376}, {'zipcode': '10119', 'rank': 140, 'prob': 41.240159510237376}, {'zipcode': '10278', 'rank': 141, 'prob': 41.240159510237376}, {'zipcode': '11211', 'rank': 142, 'prob': 41.240159510237376}, {'zipcode': '10461', 'rank': 143, 'prob': 41.240159510237376}, {'zipcode': '10466', 'rank': 144, 'prob': 41.240159510237376}, {'zipcode': '10111', 'rank': 145, 'prob': 28.615594479547653}, {'zipcode': '10473', 'rank': 146, 'prob': 28.615594479547653}, {'zipcode': '11103', 'rank': 147, 'prob': 28.615594479547653}, {'zipcode': '10177', 'rank': 148, 'prob': 28.615594479547653}, {'zipcode': '10029', 'rank': 149, 'prob': 28.615594479547653}, {'zipcode': '11351', 'rank': 150, 'prob': 28.615594479547653}, {'zipcode': '10452', 'rank': 151, 'prob': 28.615594479547653}, {'zipcode': '11205', 'rank': 152, 'prob': 28.615594479547653}, {'zipcode': '11356', 'rank': 153, 'prob': 28.615594479547653}, {'zipcode': '11357', 'rank': 154, 'prob': 28.615594479547653}, {'zipcode': '11238', 'rank': 155, 'prob': 28.615594479547653}, {'zipcode': '10459', 'rank': 156, 'prob': 28.615594479547653}, {'zipcode': '10069', 'rank': 157, 'prob': 28.615594479547653}, {'zipcode': '11419', 'rank': 158, 'prob': 28.615594479547653}, {'zipcode': '10280', 'rank': 159, 'prob': 28.615594479547653}, {'zipcode': '11420', 'rank': 160, 'prob': 28.615594479547653}, {'zipcode': '11216', 'rank': 161, 'prob': 28.615594479547653}, {'zipcode': '10018', 'rank': 162, 'prob': 28.615594479547653}, {'zipcode': '11429', 'rank': 163, 'prob': 28.615594479547653}, {'zipcode': '11371', 'rank': 164, 'prob': 9.588846553401405}, {'zipcode': '10472', 'rank': 165, 'prob': 9.588846553401405}, {'zipcode': '11432', 'rank': 166, 'prob': 9.588846553401405}, {'zipcode': '10172', 'rank': 167, 'prob': 9.588846553401405}, {'zipcode': '10115', 'rank': 168, 'prob': 9.588846553401405}, {'zipcode': '11226', 'rank': 169, 'prob': 9.588846553401405}, {'zipcode': '11436', 'rank': 170, 'prob': 9.588846553401405}, {'zipcode': '11377', 'rank': 171, 'prob': 9.588846553401405}, {'zipcode': '10030', 'rank': 172, 'prob': 9.588846553401405}, {'zipcode': '11201', 'rank': 173, 'prob': 9.588846553401405}, {'zipcode': '11233', 'rank': 174, 'prob': 9.588846553401405}, {'zipcode': '10153', 'rank': 175, 'prob': 9.588846553401405}, {'zipcode': '10003', 'rank': 176, 'prob': 9.588846553401405}, {'zipcode': '10303', 'rank': 177, 'prob': 9.588846553401405}, {'zipcode': '11414', 'rank': 178, 'prob': 9.588846553401405}, {'zipcode': '10154', 'rank': 179, 'prob': 9.588846553401405}, {'zipcode': '10035', 'rank': 180, 'prob': 9.588846553401405}, {'zipcode': '10306', 'rank': 181, 'prob': 9.588846553401405}, {'zipcode': '11206', 'rank': 182, 'prob': 9.588846553401405}, {'zipcode': '11237', 'rank': 183, 'prob': 9.588846553401405}, {'zipcode': '10007', 'rank': 184, 'prob': 9.588846553401405}, {'zipcode': '11417', 'rank': 185, 'prob': 9.588846553401405}, {'zipcode': '11418', 'rank': 186, 'prob': 9.588846553401405}, {'zipcode': '11209', 'rank': 187, 'prob': 9.588846553401405}, {'zipcode': '10039', 'rank': 188, 'prob': 9.588846553401405}, {'zipcode': '11360', 'rank': 189, 'prob': 9.588846553401405}, {'zipcode': '10460', 'rank': 190, 'prob': 9.588846553401405}, {'zipcode': '10311', 'rank': 191, 'prob': 9.588846553401405}, {'zipcode': '11451', 'rank': 192, 'prob': 9.588846553401405}, {'zipcode': '11421', 'rank': 193, 'prob': 9.588846553401405}, {'zipcode': '11212', 'rank': 194, 'prob': 9.588846553401405}, {'zipcode': '11693', 'rank': 195, 'prob': 9.588846553401405}, {'zipcode': '10463', 'rank': 196, 'prob': 9.588846553401405}, {'zipcode': '10014', 'rank': 197, 'prob': 9.588846553401405}, {'zipcode': '11425', 'rank': 198, 'prob': 9.588846553401405}, {'zipcode': '10165', 'rank': 199, 'prob': 9.588846553401405}, {'zipcode': '10075', 'rank': 200, 'prob': 9.588846553401405}, {'zipcode': '11005', 'rank': 201, 'prob': 9.588846553401405}, {'zipcode': '11426', 'rank': 202, 'prob': 9.588846553401405}, {'zipcode': '11366', 'rank': 203, 'prob': 9.588846553401405}, {'zipcode': '10467', 'rank': 204, 'prob': 9.588846553401405}, {'zipcode': '10019', 'rank': 205, 'prob': 9.588846553401405}, {'zipcode': '10169', 'rank': 206, 'prob': 9.588846553401405}]

//var dataLen = data.length;

var map = L.map('map').setView([40.66391877278807,-73.93834608174572], 10);

L.tileLayer('https://api.tiles.mapbox.com/v4/{id}/{z}/{x}/{y}.png?access_token={accessToken}', {
  attribution: 'Map data &copy; <a href="http://openstreetmap.org">OpenStreetMap</a> contributors, <a href="http://creativecommons.org/licenses/by-sa/2.0/">CC-BY-SA</a>, Imagery © <a href="http://mapbox.com">Mapbox</a>',
  maxZoom: 18,
  id: 'mapbox.light',
  accessToken: 'pk.eyJ1Ijoia21zaGVsbGV5IiwiYSI6ImNpbDh2Nmg3NTBmZ2N1a20wNjR0c2pka3oifQ.Z7q1QzUQYFKFXN9JSALt9w'
}).addTo(map);

var geojsonLayer = null;

var map2 = L.map('smallMap').setView([40.66391877278807,-73.93834608174572], 10);

L.tileLayer('https://api.tiles.mapbox.com/v4/{id}/{z}/{x}/{y}.png?access_token={accessToken}', {
  attribution: 'Map data &copy; <a href="http://openstreetmap.org">OpenStreetMap</a> contributors, <a href="http://creativecommons.org/licenses/by-sa/2.0/">CC-BY-SA</a>, Imagery © <a href="http://mapbox.com">Mapbox</a>',
  maxZoom: 18,
  id: 'mapbox.light',
  accessToken: 'pk.eyJ1Ijoia21zaGVsbGV5IiwiYSI6ImNpbDh2Nmg3NTBmZ2N1a20wNjR0c2pka3oifQ.Z7q1QzUQYFKFXN9JSALt9w'
}).addTo(map2);

var focusedZip = null;
var collisionsInZip = null;
var heatLayer = null;

var probData = [];


function pad(s) { return (s < 10) ? '0' + s : s; }

function convertDate(inputFormat,withSlash) {

  var d = new Date(inputFormat);
  if (withSlash){
    return [pad(d.getMonth()+1),pad(d.getDate()), d.getFullYear()].join('/');
  } else {
  return [pad(d.getMonth()+1),pad(d.getDate())].join('');
 }
}

function convertDateTime(input) { //2016-03-23T00:00:00
 return [input.getFullYear(),pad(input.getMonth()+1),pad(input.getDate())].join('-')
            + "T"
            + [pad(input.getHours()),pad(input.getMinutes()),pad(input.getSeconds())].join(':')
}

$(function() {
  //fill date and time dropdowns
  var today = new Date();
  var currentDay = today;
  var currentHour = currentDay.getHours();

  var optionsStr = ""
  var selected = "selected"; //select first item

  for (var i = 0; i<10; i++){ //for next 10 days
     optionsStr += "<option value='" + convertDate(currentDay,false) +  "' " + selected + ">" + convertDate(currentDay,true) + "</option>";
     currentDay =  new Date(currentDay.getTime() + 86400000 * 1);
     selected = "";
  }

  $("#dateSelector").append(optionsStr);

  var optionsStrHours = "";

  for (var i = 0; i<24; i++){ //for next 10 days
     var selected = (currentHour == i) ? "selected" : "";

     //put AM/PM
     displayHour = i + "AM";
     if (i == 12){
          displayHour = "12PM";
     } else if (i>12){
          displayHour = i-12 + "PM";
     }

     optionsStrHours += "<option value='" + pad(i) + "' " + selected + ">" + displayHour + "</option>";
  }

  $("#hourSelector").append(optionsStrHours);

  $("#updateButton").click(function() {
    updateData();
  });

  $(".zipInfo button").click(function() {
    $(".zipInfo").hide();
  });



  updateData();

  // now get stuff for 10 day forecast
  today.setHours(0);
  today.setMinutes(0);
  today.setSeconds(0);

  var lowerBound = convertDateTime(today);
  //upper bound  - add 10 day and  minus 1 hour
  after10Day = new Date(today.getTime() + 86400000 * 10 - 60*60*1000);
  var upperBound = convertDateTime(after10Day);

  updateData2(lowerBound,upperBound);

  $("#updateThresholdButton").click(function() {
    updateData2(lowerBound,upperBound);
  });



});

//linechart starts here


var margin = {top: 20, right: 20, bottom: 50, left: 50},
  width = 700 - margin.left - margin.right,
  height = 500 - margin.top - margin.bottom;


var x = d3.scale.linear()
          .range([0,width]);
          //.domain([0,210]);

var y = d3.scale.linear()
              .range([height, 0])
              .domain([0,100]);


var xAxis = d3.svg.axis()
                  .scale(x)
                  .orient("bottom")
                  .ticks(4);

var yAxis = d3.svg.axis()
                  .scale(y)
                  .orient("left");

var line = d3.svg.line()
  .x(function(d) { return x(d.rank); })
  .y(function(d) { return y(d.probability * 100); });

  var tip = d3.tip()
    .attr('class', 'd3-tip')
    .offset([-10, 0])
    .html(function(d) {
      return "<strong>Zipcode:</strong>&nbsp;<span style='color:white'>" + d.grid_zipcode + "</span><br><strong>Probability:</strong>&nbsp;<span style='color:white'>" + Math.round(d.probability * 1000) / 10 + "%</span>";
    })


var svg = d3.select("#lineChart").append("svg")
  .attr("width", width + margin.left + margin.right)
  .attr("height", height + margin.top + margin.bottom)
  .append("g")
  .attr("transform", "translate(" + margin.left + "," + margin.top + ")");

  svg.call(tip);

// add the tooltip area to the webpage
var tooltip = d3.select("body").append("div")
  .attr("class", "tooltip")
  .style("opacity", 0);
/*		24-Apr-07	93.24
25-Apr-07	95.35
26-Apr-07	98.84
*/

/*
var focus = svg.append("g")
      .attr("class", "focus")
      .style("display", "none");

focus.append("circle")
      .attr("r", 4.5);

focus.append("text")
      .attr("x", 9)
      .attr("dy", ".35em");
*/
//bisectDate = d3.bisector(function(d) { return d.rank; }).left;

function dotMouseOver(d)
{
  tip.show(d);
  geojsonLayer.eachLayer(function (layer) {
    if(layer.feature.properties.zipcode == d.grid_zipcode) {
      layer.setStyle({fillColor :'#00000'});
    }
  });
}

function getProbability(zipcode){
  for (var i = 0; i< probData.length; i ++){
    var thisItem = probData[i];
    if(zipcode== thisItem.grid_zipcode) {
        return thisItem.probability;
    }
  }
}

function getOpacity(feature){
  return getColor(getProbability(feature.properties.zipcode));
}

function dotMouseOut(d) {
  tip.hide(d);
  geojsonLayer.eachLayer(function (layer) {
    if(layer.feature.properties.zipcode == d.grid_zipcode) {
      layer.setStyle({fillColor : getOpacity(layer.feature)});
    }
  });
}

function getColor(d) {
		return d > 0.9 ? '#800026' :
			   d > 0.8 ? '#bd0026' :
			   d > 0.7 ? '#e31a1c' :
			   d > 0.6 ? '#fc4e2a' :
			   d > 0.5 ? '#fd8d3c' :
			   d > 0.4 ? '#feb24c' :
			   d > 0.3 ? '#fed976' :
         d > 0.2 ? '#ffeda0' :
						  '#ffffcc';
	}


function updateData()
{

var selectedDateHrString  =  "?dateHourStr=" + $("#dateSelector").val() + $("#hourSelector").val();

d3.json("/predict" + selectedDateHrString , function(error, data) {

 function style(feature) {
    return {
      fillColor: getOpacity(feature),
      weight: 1,
      opacity: 1,
      fillOpacity: 1,
      color: 'black'//,
      //fillOpacity: getOpacity(feature) //feature.properties.probability //,
      //fillColor: 'red'
    }
 };

 probData = data;

 if (geojsonLayer == null) {
  //load entire geojsonlayer here, as there seemed to have been a race condition in which
  //if the json came back before layer loaded coloring wouldn't work sometimes
    geojsonLayer = L.geoJson(zip_codes, {
      style: 	style,
      onEachFeature: function(feature, layer) {
              //layer.bindPopup('<strong>Science Hall</strong><br>Where the GISC was born.');
              //layer.on('mouseover', function() { layer.openPopup();  }):
              //if (feature.properties && feature.properties.popupContent) {

/*
                  layer.bindPopup("<h5>Zipcode:" + feature.properties.zipcode + "<h5>Probability:" +
                        getProbability(feature.properties.zipcode), {autoPan:false, closeButton: false});
                        */
                  //layer.on('mouseover', function() { layer.openPopup();  });

                  //layer.on('mouseout', function() { layer.closePopup(); });
              //}

              layer.on('mouseover', function() {
                $(".zipcode_" + feature.properties.zipcode).d3MouseOver();
              });

              layer.on('mouseout', function() {
                $(".zipcode_" + feature.properties.zipcode).d3MouseOut();
              });

              layer.on('click', function() {
                $("#zipInfoPopupCode").text(feature.properties.zipcode);
                $(".zipInfo").show();
                map2.invalidateSize()

                /*var polygon = L.polygon([
                    [51.509, -0.08],
                    [51.503, -0.06],
                    [51.51, -0.047]
                ]).addTo(map2);
                map2.fitBounds([
                    [51.509, -0.08],
                    [51.503, -0.06],
                    [51.51, -0.047]
                ]);*/

                if (focusedZip != null) //remove existing
                {
                    map2.removeLayer(focusedZip);
                }

                if (heatLayer != null) //remove existing
                {
                    map2.removeLayer(heatLayer);
                }

                for (var i = 0; i< zip_codes.features.length; i++)
                {
                    var currentItem = zip_codes.features[i];
                    if (currentItem.properties.zipcode  == feature.properties.zipcode) {

                        focusedZip = L.geoJson(currentItem,{
                          style: 	{
                            fillColor: 'white',
                            weight: 1,
                            opacity: 1,
                            fillOpacity: 1,
                            color: 'black',
                            fillOpacity:.2//,
                            //fillOpacity: getOpacity(feature) //feature.properties.probability //,
                            //fillColor: 'red'
                          }}
                        ).addTo(map2);
                        map2.fitBounds(focusedZip.getBounds());

                        /*var myIcon = L.divIcon({
                            html: 'X'
                        });*/

                        $.getJSON( "/collisionsByZipcode?zipcode=" + feature.properties.zipcode, function( data ) {
                          //collisionsInZip  = L.geoJson(data,{icon: myIcon}).addTo(map2);
                          heatLayer = L.heatLayer(data, {radius: 10}).addTo(map2);
                          //collisionsInZip.setIcon(myIcon);
                        });



                        //http://169.53.138.92:3000/collisionsByZipcode?zipcode=11101
                    }

                }
                /*
                map2.whenReady(function () {
                  window.setTimeout(function () {
                    var polygon = L.polygon([
                        [51.509, -0.08],
                        [51.503, -0.06],
                        [51.51, -0.047]
                    ]).addTo(map2);
                    map2.fitBounds([
                        [51.509, -0.08],
                        [51.503, -0.06],
                        [51.51, -0.047]
                    ]);
                  }.bind(this), 1000);


                }, this);
*/

                $.getJSON( "/getZipcodeInfo?zipcode=" + feature.properties.zipcode + "&dateHourStr=" +  $("#dateSelector").val() + $("#hourSelector").val(),
                    function( data ) {

                        //alert(data);
                });

              });

      }
    })
    geojsonLayer.addTo(map);

    //add legend
    var legend = L.control({position: 'bottomright'});

    legend.onAdd = function (map) {

        var div = L.DomUtil.create('div', 'info legend'),
            grades = [.15, .25,.35,.45,.55,.65,.75,.85,.95],
            labels = [];

        // loop through our density intervals and generate a label with a colored square for each interval
        for (var i = 0; i < grades.length; i++) {
            var text = (i+1)/10 + '&ndash;' + (i+2)/10;
            if (i == 0 ) text = "< .2";
            if (i == (grades.length -1) ) text = "> .9";

            div.innerHTML +=
                '<i style="background:' + getColor(grades[i]) + '"></i> ' +text + '<br>';

                //grades[i] + (grades[i + 1] ? '&ndash;' + grades[i + 1] + '<br>' : '+');
        }

        return div;
    };

    legend.addTo(map);
} else { //just change color
  //update shading of the map

  geojsonLayer.eachLayer(function (layer) {

    for (var i = 0; i< data.length; i ++){
      var thisItem = data[i];
      if(layer.feature.properties.zipcode == thisItem.grid_zipcode) {
        layer.setStyle({fillColor :getColor(thisItem.probability)})
        }
    }
  });


}

      svg.selectAll("path").remove();
      svg.selectAll(".dot").remove();

      //y.domain(d3.extent(data, function(d) { return d.close; }));
      x.domain(d3.extent(data, function(d) { return d.rank; }));

      svg.append("g")
        .attr("class", "x axis")
        .attr("transform", "translate(0," + height + ")")
        .call(xAxis)
        .append("text")
        .attr("y",30)
        .attr("x",width/2+10)
        .attr("dy", ".71em")
        .style("text-anchor", "end")
        .text("Ranking");

      svg.append("g")
        .attr("class", "y axis")
        .call(yAxis)
        .append("text")
        .attr("transform", "rotate(-90)")
        .attr("y", 6)
        .attr("dy", ".71em")
        .style("text-anchor", "end")
        .text("Probability(%)");

/*
function mousemove() {
    var x0 = x.invert(d3.mouse(this)[0]),
        i = bisectDate(data, x0, 1),
        d0 = data[i - 1],
        d1 = data[i],
        d = x0 - d0.rank > d1.rank - x0 ? d1 : d0;

    focus.attr("transform", "translate(" + x(d.rank) + "," + y(d.probability * 100) + ")");
    //focus.select("text").text(d.zipcode);


  }
  */

/*
  svg.append("rect")
          .attr("class", "overlay")
          .attr("width", width)
          .attr("height", height)
          .on("mouseover", function() {
            focus.style("display", null);

          })
          .on("mouseout", function() { focus.style("display", "none");
              geojsonLayer.eachLayer(function (layer) {


            layer.setStyle({fillColor :'#000000',
                          fillOpacity:  layer.feature.properties.probability})
            });


          })
          .on("mousemove", mousemove);
          */

  svg.append("path")
      .datum(data)
      .attr("class", "line")
      .attr("d", line)
      /*
      .on("mouseover", function(d) {
          /alert(d);
          /*tooltip.transition()
               .duration(200)
               .style("opacity", .9);
          tooltip.html(d["Cereal Name"] + "<br/> (" + xValue(d)
          + ", " + yValue(d) + ")")
               .style("left", (d3.event.pageX + 5) + "px")
               .style("top", (d3.event.pageY - 28) + "px");
               */
      //});


  svg.selectAll(".dot")
      .data(data)
      .enter().append("circle")
      .attr("class", function(d) { return "dot zipcode_" + d.grid_zipcode; })
      .attr("r", 1)
      .attr("cx", function(d) { return x(d.rank); })
      .attr("cy", function(d) { return y(d.probability * 100); })
      .on("mouseover", function(d) {
        dotMouseOver(d);
/*
          tooltip.transition()
               .duration(200)
               .style("opacity", .9);
          tooltip.html("Zipcode:" + d.grid_zipcode+ "<br/> Probability:" + d.probability)
               .style("left", (d3.event.pageX + 5) + "px")
               .style("top", (d3.event.pageY - 28) + "px");
               */

      })
      .on("mouseout", function(d) {
        dotMouseOut(d);

/*
         tooltip.transition()
         .duration(500)
         .style("opacity", 0);
         */


});;




<!-- line chart ends here-->

//update currente datetime
$("#currentDate").text($("#dateSelector option:selected").text());
$("#currentHour").text($("#hourSelector option:selected").text());

});

}

/**************
10 day trend linechart start here
**************/

width2 = 1000 - margin.left - margin.right,
height2 = 500 - margin.top - margin.bottom;

var x2 =  d3.time.scale()
          .range([0,width2])

var y2 = d3.scale.linear()
              .range([height2, 0])

var xAxis2 = d3.svg.axis()
                  .scale(x2)
                  .orient("bottom");

var yAxis2 = d3.svg.axis()
                  .scale(y2)
                  .orient("left");

var line2 = d3.svg.line()
  .x(function(d) { return x2(d.key_as_string); })
  .y(function(d) { return y2(d.doc_count); });

var svg2 = d3.select("#highProbCounChart").append("svg")
  .attr("width", width2 + margin.left + margin.right)
  .attr("height", height2 + margin.top + margin.bottom)
  .append("g")
  .attr("transform", "translate(" + margin.left + "," + margin.top + ")");



/*var focus = svg.append("g")
      .attr("class", "focus")
      .style("display", "none");

focus.append("circle")
      .attr("r", 4.5);

focus.append("text")
      .attr("x", 9)
      .attr("dy", ".35em");
*/

function updateData2(lower, upper)
{

  var threshold = $("#probThreshold").val()

  d3.json("http://169.53.138.92:3000/tenDayTrend?lower="
                      +  lower + "&upper=" + upper + "&threshold=" + threshold,
                       function(error, data2) {

    var formatDate = d3.time.format("%Y-%m-%dT%H:%M:%S.%LZ");

    for (var i = 0; i < data2.length; i++) {
       data2[i].key_as_string = formatDate.parse(data2[i].key_as_string);
  }

  svg2.selectAll("path").remove();

  y2.domain([0,211]);
  x2.domain(d3.extent(data2, function(d) {
                    return d.key_as_string;
                              }));

      //y.domain(d3.extent(data, function(d) { return d.close; }));
      svg2.append("g")
        .attr("class", "x axis")
        .attr("transform", "translate(0," + height2 + ")")
        .call(xAxis2)
        .append("text")
        .attr("y",30)
        .attr("x",width2/2+10)
        .attr("dy", ".71em")
        .style("text-anchor", "end")
        .text("Date and Time");

      svg2.append("g")
        .attr("class", "y axis")
        .call(yAxis2)
        .append("text")
        .attr("transform", "rotate(-90)")
        .attr("y", 6)
        .attr("dy", ".71em")
        .style("text-anchor", "end")
        .text("High Risk Count");

  svg2.append("path")
      .datum(data2)
      .attr("class", "line")
      .attr("d", line2)
      /*
      .on("mouseover", function(d) {
          /alert(d);
          /*tooltip.transition()
               .duration(200)
               .style("opacity", .9);
          tooltip.html(d["Cereal Name"] + "<br/> (" + xValue(d)
          + ", " + yValue(d) + ")")
               .style("left", (d3.event.pageX + 5) + "px")
               .style("top", (d3.event.pageY - 28) + "px");
               */
      //});





<!-- line chart ends here-->

});

}
