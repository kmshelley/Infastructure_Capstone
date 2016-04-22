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

var policeLayer = null;
var councilLayer = null;
var schoolLayer = null;


function pad(s) { return (s < 10) ? '0' + s : s; }

function convertDate(inputFormat,withSeparator,separatorChar) {

  var d = new Date(inputFormat);
  if (withSeparator){
    if (separatorChar == "/") {
      return [pad(d.getMonth()+1),pad(d.getDate()), d.getFullYear()].join(separatorChar);
    } else { //separate is "-", return 2016-03-23
      return [d.getFullYear(), pad(d.getMonth()+1),pad(d.getDate())].join(separatorChar);
    }
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

  //check for chrome
  var isChrome = /Chrome/.test(navigator.userAgent) && /Google Inc/.test(navigator.vendor);
  if (!isChrome){
    $('#myModal').modal()
  }

  //get update date
  $.get( "/getUpdateDate", function( data ) {

    //convert date time to formatted string
    var udpateD = new Date(data);
    var newStr = new Array(udpateD.getFullYear(),pad(udpateD.getMonth()+1),pad(udpateD.getDate())).join("/");
    newStr += " " + pad(udpateD.getUTCHours()) + ":" + pad(udpateD.getUTCMinutes());
    $("#updateDate").text(newStr);
  });

  //fill date and time dropdown
  var today = new Date();
  var currentDay = today;
  var currentHour = currentDay.getHours();

  var optionsStr = ""
  var selected = "selected"; //select first item

  for (var i = 0; i<10; i++){ //for next 10 days
     optionsStr += "<option formattedDate=" +convertDate(currentDay,true,"-") + " value='" + convertDate(currentDay,false) +  "' " + selected + ">" + convertDate(currentDay,true,"/") + "</option>";
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

  $("#fromHourSelector").append(optionsStrHours);
  $("#toHourSelector").append(optionsStrHours);

  $('#fromHourSelector').selectpicker({
    size: 10,
    width:"100px"
  });

  $('#fromHourSelector').selectpicker('val', $('#fromHourSelector :selected').val());

  $('#toHourSelector').selectpicker({
    size: 10,
    width:"100px"
  });

  $('#toHourSelector').selectpicker('val', $('#toHourSelector :selected').val());


  $("#updateButton").click(function() {
    updateData();
  });

  $(".zipInfo button").click(function() {
    $(".zipInfo").hide();
  });

  $("#allDay").click(function() {
    $(".hourSelector").hide();
  });

  $("#customHours").click(function() {
    $(".hourSelector").show();
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

  //no update current injury/fatalities stats
  //get all accidents before this date
  var getNumbersBefore = new Date(today.getTime() + 86400000 * 1);
  var startOfYear = new Date(getNumbersBefore.getFullYear(),0,1); //2016-01-01
  var upperBoundForThisYear = convertDateTime(getNumbersBefore);
  var lowerBoundForThisYear = convertDateTime(startOfYear);
  var upperBoundForLastYear = convertDateTime(new Date(getNumbersBefore.setFullYear(getNumbersBefore.getFullYear()-1)));
  var lowerBoundForLastYear = convertDateTime(new Date(startOfYear.setFullYear(startOfYear.getFullYear()-1)));

  var queryStr = "uc=" +  upperBoundForThisYear + "&lc=" + lowerBoundForThisYear + "&ul=" + upperBoundForLastYear + "&ll=" + lowerBoundForLastYear;


  //fill quick stats numbers
  $.getJSON( "/getCurrentStats?" + queryStr, function( data ) {

     $(".currentDate").text(convertDate(new Date(data.latest),true,"/")) ;

    //determine which year was highest to determine display up/down arrow
    var motoFatalitiesHighest = {year: 0, value:0};
    var cycFatalitiesHighest = {year: 0, value:0};
    var pedFatalitiesHighest = {year: 0, value:0};
    var allFatalitiesHighest = {year: 0, value:0};
    var motoInjuriesHighest = {year: 0, value:0};
    var cycInjuriesHighest = {year: 0, value:0};
    var pedInjuriesHighest = {year: 0, value:0};
    var allInjuriesHighest = {year: 0, value:0};

    for (var i = 0; i< data.buckets.length; i++){
      var item = data.buckets[i];
      var year = item["key"];

      var motoFatal = item["moto_fatalities"]["value"];
      $("#moto_fatalities" + year).text(motoFatal);
      if (motoFatal > motoFatalitiesHighest["value"])
      {
          motoFatalitiesHighest["value"] = motoFatal;
          motoFatalitiesHighest["year"] = year;
      }

      var motoInj = item["moto_injuries"]["value"];
      $("#moto_injuries" + year).text(motoInj);
      if (motoInj > motoInjuriesHighest["value"])
      {
          motoInjuriesHighest["value"] = motoInj;
          motoInjuriesHighest["year"] = year;
      }

      var pedFatal = item["ped_fatalities"]["value"];
      $("#ped_fatalities" + year).text(pedFatal);
      if (pedFatal > pedFatalitiesHighest["value"])
      {
          pedFatalitiesHighest["value"] = pedFatal;
          pedFatalitiesHighest["year"] = year;
      }

      var pedInj = item["ped_injuries"]["value"];
      $("#ped_injuries" + year).text(pedInj);
      if (pedInj > pedInjuriesHighest["value"])
      {
          pedInjuriesHighest["value"] = pedInj;
          pedInjuriesHighest["year"] = year;
      }

      var cycFatal = item["cyc_fatalities"]["value"];
      $("#cyc_fatalities" + year).text(cycFatal);
      if (cycFatal > cycFatalitiesHighest["value"])
      {
          cycFatalitiesHighest["value"] = cycFatal;
          cycFatalitiesHighest["year"] = year;
      }

      var cycInj = item["cyc_injuries"]["value"];
      $("#cyc_injuries" + year).text(item["cyc_injuries"]["value"]);
      if (cycInj > cycInjuriesHighest["value"])
      {
          cycInjuriesHighest["value"] = cycInj;
          cycInjuriesHighest["year"] = year;
      }

      var allFatal = item["moto_fatalities"]["value"] + item["cyc_fatalities"]["value"] + item["ped_fatalities"]["value"];
      $("#total_fatalities" + year).text(allFatal);
      if (allFatal > allFatalitiesHighest["value"])
      {
          allFatalitiesHighest["value"] = allFatal;
          allFatalitiesHighest["year"] = year;
      }

      var allInj = item["moto_injuries"]["value"] + item["cyc_injuries"]["value"] + item["ped_injuries"]["value"];
      $("#total_injuries" + year).text(allInj);
      if (allInj > allInjuriesHighest["value"])
      {
          allInjuriesHighest["value"] = allInj;
          allInjuriesHighest["year"] = year;
      }

    }

    //now put red arrow or blue arrow
    assignArrows(motoFatalitiesHighest, "motoFatal");
    assignArrows(cycFatalitiesHighest,"cycFatal");
    assignArrows(pedFatalitiesHighest,"pedFatal");
    assignArrows(allFatalitiesHighest,"allFatal");
    assignArrows(motoInjuriesHighest,"motoInj");
    assignArrows(cycInjuriesHighest,"cycInj");
    assignArrows(pedInjuriesHighest,"pedInj");
    assignArrows(allInjuriesHighest,"allInj");

  });

  $("#updateThresholdButton").click(function() {
    updateData2(lowerBound,upperBound);
  });

  //write out divs for diagnostics for all, cyc, ped and motor
  addDiagnosticsDivs("all","All injuries/fatalities")
  addDiagnosticsDivs("cyclist","Cyclist injuries/fatalities");
  addDiagnosticsDivs("motorist","Motorist injuries/fatalities");
  addDiagnosticsDivs("pedestrian","Pedestrian injuries/fatalities");

  $.getJSON( "/diag", function( data ) {
    addDiagInfoPerType("all",data);
    addDiagInfoPerType("pedestrian",data);
    addDiagInfoPerType("cyclist",data);
    addDiagInfoPerType("motorist",data);
  });

  $.getJSON( "/importance", function( data ) {

    for (var i=0; i< data.length; i ++) {

      var curData = data[i];

        var type = curData["key"];

        var title = "All Injuries/Fatalities";
        if (type=="cyclist") title = "Cyclist Injuries/Fatalities";
        else if (type=="pedestrian") title = "Pedestrian Injuries/Fatalities";
        else if (type=="motorist") title = "Motorist Injuries/Fatalities";

        var divs = '<div class="float-left divideByFour"> \
        <div class="zipCodeHeader">' + title + '</div> \
        <div id="' + type + 'Importance"></div></div>';

        $("#importance").append(divs);

        var barchart = featureBarChart();
        barchart("#" + type  + "Importance", curData["tops"]["hits"]["hits"]);


    }

  });

});

function assignArrows(ent, divId){
  if (ent["year"] == 2016) //collisions went up
  {
    $("#" + divId + "Arrow").addClass("glyphicon glyphicon-arrow-up text-danger");
  } else {
    $("#" + divId + "Arrow").addClass("glyphicon glyphicon-arrow-down text-primary");
  }
}

function addDiagInfoPerType(type, data){
  var rocCurve = rocLineChart();
  rocCurve("#" + type + "ROC", data[type + "_ROC_merged"]);
  $("#" + type + "AUC").text(parseFloat(data[type+"_AUC"].toFixed(4)));
  $("#" + type + "F1").text(parseFloat(data[type+"_F1"].toFixed(4)));
  $("#" + type + "Precision").text(parseFloat(data[type+"_Precision"].toFixed(4)));
  $("#" + type + "Recall").text(parseFloat(data[type+"_Recall"].toFixed(4)));
  $("#" + type + "FPR").text(parseFloat(data[type+"_false_postive_rate"].toFixed(4)));
  $("#" + type + "FNR").text(parseFloat(data[type+"_false_negative_rate"].toFixed(4)));
  $("#" + type + "TPR").text(parseFloat(data[type+"_true_postive_rate"].toFixed(4)));
  $("#" + type + "TNR").text(parseFloat(data[type+"_true_negative_rate"].toFixed(4)));

}

function addDiagnosticsDivs(type,title){

  /*
  "all_AUC",
"all_F1",
"all_Precision",
"all_Recall",
"all_Test_set_accuracy",
"all_false_negative_rate",
"all_false_postive_rate",
"all_percent_accidents",
"all_true_negative_rate",
"all_true_negative_rate",
  */
  var divs = '<div class="float-left divideByFour"> \
  <div class="zipCodeHeader">' + title + '</div> \
  <div id="' + type + 'ROC"></div> \
  <div> \
    <div style="text-align: left;"> \
      <ul style="list-style-type: none;" class="zipUl"> \
        <li><span  class="liKeyNarrower">AUC:</span><span id="' + type + 'AUC"></span></li> \
        <li><span  class="liKeyNarrower">F1:</span><span id="' + type + 'F1"></span></li> \
        <li><span  class="liKeyNarrower">Precision:</span><span id="' + type + 'Precision"></span></li> \
        <li><span  class="liKeyNarrower">Recall:</span><span id="' + type + 'Recall"></span></li> \
        <li><span  class="liKeyNarrower">False Positive Rate:</span><span id="' + type + 'FPR"></span></li> \
        <li><span  class="liKeyNarrower">False Negative Rate:</span><span id="' + type + 'FNR"></span></li> \
        <li><span  class="liKeyNarrower">True Positive Rate:</span><span id="' + type + 'TPR"></span></li> \
        <li><span  class="liKeyNarrower">True Negative Rate:</span><span id="' + type + 'TNR"></span></li> \
      </ul> \
    </div> \
  </div> \
  </div>';
  $("#diagnostics").append(divs);

}



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
    if(layer.feature.properties.ZCTA5CE10 == d.grid_zipcode) {
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
  return getColor(getProbability(feature.properties.ZCTA5CE10));
}

function dotMouseOut(d) {
  tip.hide(d);
  geojsonLayer.eachLayer(function (layer) {
    if(layer.feature.properties.ZCTA5CE10 == d.grid_zipcode) {
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


function getSelectedDateHourString(){
  var selectedDateHrString  =  "?dateStr=" + $("#dateSelector").val() + "&accidentType=" + $("#accidentType").val();

  var hourRangeSelection = $("input[type='radio'][name='hourRangeSelector']:checked").val();
  if (hourRangeSelection !="allDay")
  {
    var fromHour = $("#fromHourSelector").val();
    var fromHourInt = parseInt(fromHour,10);
    var toHour = $("#toHourSelector").val()
    var toHourInt = parseInt(toHour,10);

    //see if toHour is after fromHour, then assume it's day after
    if (toHourInt == fromHourInt)
    {
      alert("Please choose a valid hour range");
      return;

    } else if (fromHourInt < toHourInt) { //within the same day
      fromDateHour = $("#dateSelector :selected").attr("formattedDate") + "T" + fromHour + ":00:00"
      toDateHour = $("#dateSelector :selected").attr("formattedDate") + "T" + toHour + ":00:00"
    } else {
      //#add one day to toDateHour
      var toDayTmr = moment($("#dateSelector :selected").attr("formattedDate"),'YYYY-MM-DD', true).add(1,'day').format('YYYY-MM-DD')
      fromDateHour = $("#dateSelector :selected").attr("formattedDate") + "T" + fromHour + ":00:00";
      toDateHour = toDayTmr + "T" + toHour + ":00:00";
    }

    selectedDateHrString += "&fromHour=" + fromDateHour + "&toHour=" + toDateHour;


  }
  return selectedDateHrString;

}


function updateData()
{

  var selectedDateHrString = getSelectedDateHourString();

d3.json("/predict" + selectedDateHrString , function(error, data) {

 function style(feature) {
    return {
      fillColor: getOpacity(feature),
      weight: 1,
      opacity: 1,
      fillOpacity: .8,
      color: 'gray'
    }
 };

 probData = data;

 if (geojsonLayer == null) {
  //load entire geojsonlayer here, as there seemed to have been a race condition in which
  //if the json came back before layer loaded coloring wouldn't work sometimes
    geojsonLayer = L.geoJson(zip_codes, {
      style: 	style,
      onEachFeature: function(feature, layer) {

              layer.on('mouseover', function() {
                $(".zipcode_" + feature.properties.ZCTA5CE10).d3MouseOver();
              });

              layer.on('mouseout', function() {
                $(".zipcode_" + feature.properties.ZCTA5CE10).d3MouseOut();
              });

              layer.on('click', function() {
                $("#zipInfoPopupCode").text(feature.properties.ZCTA5CE10);
                $(".zipInfo").show();
                map2.invalidateSize()

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
                    if (currentItem.properties.ZCTA5CE10  == feature.properties.ZCTA5CE10) {

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

                        $.getJSON( "/collisionsByZipcode?zipcode=" + feature.properties.ZCTA5CE10 + "&accidentType=" + $("#accidentType").val() , function( data ) {
                          //collisionsInZip  = L.geoJson(data,{icon: myIcon}).addTo(map2);
                          heatLayer = L.heatLayer(data, {radius: 10}).addTo(map2);
                          //collisionsInZip.setIcon(myIcon);
                          switch ($("#accidentType").val()) {
                            case "PEDS":
                              $("#zipHeatType").html("&nbsp;Pedestrian");
                              break;
                            case "CYC":
                              $("#zipHeatType").html("&nbsp;Cyclist");
                              break;
                            case "CYC":
                              $("#zipHeatType").html("&nbsp;MOTO");
                              break;
                            default:
                              $("#zipHeatType").html("");
                              break;
                          }


                        });

                    }

                }

                //get currentely selected date/time
                var selectedDateHrString  =  "&dateStr=" + $("#dateSelector").val();

                var hourRangeSelection = $("input[type='radio'][name='hourRangeSelector']:checked").val();
                if (hourRangeSelection !="allDay")
                {
                  fromDateHour = $("#dateSelector :selected").attr("formattedDate") + "T" + $("#fromHourSelector").val() + ":00:00"
                  toDateHour = $("#dateSelector :selected").attr("formattedDate") + "T" + $("#toHourSelector").val() + ":00:00"
                  selectedDateHrString += "&fromHour=" + fromDateHour + "&toHour=" + toDateHour;
                }

                var selectedDateHrString = getSelectedDateHourString();

                $.getJSON( "/getInfoForZipcode" + selectedDateHrString + "&zipcode=" + feature.properties.ZCTA5CE10,
                    function( data ) {

                        //update speed limit dist.
                        drawZipcodeBar(data["speed_limits"]);

                        //populate all of the data
                        //$("#zipLogo").attr("src",data["icon_url"]);
                        $("#zipCondition").text(data["condition"]);
                        $("#zipTemp").text(data["temp"]);
                        $("#zipWind").text(data["windspeed"]);
                        $("#zipBridges").text(data["bridges"]);
                        $("#zipTunnels").text(data["tunnels"]);
                        $("#zip311").text(parseFloat(data["road_cond_requests"].toFixed(0)));
                        $("#zipNumRoads").text(data["total_road_count"]);
                        $("#zipRoadLen").text(parseFloat(data["total_road_length"].toFixed(1)));

                        $("#zipAvgTrafficCount").text(parseFloat(data["Average AADT"].toFixed(0)));
                        $("#zipMedTrafficCount").text(parseFloat(data["Median AADT"].toFixed(0)));
                        $("#zipLiquor").text(parseFloat(data["liquor_licenses"].toFixed(0)));
                        $("#zipArea").text(parseFloat(data["zip_area"].toFixed(4)));

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
                '<i style="background:' + getColor(grades[i]) + '; opacity:.8"></i> ' +text + '<br>';

                //grades[i] + (grades[i + 1] ? '&ndash;' + grades[i + 1] + '<br>' : '+');
        }

        return div;
    };

    legend.addTo(map);

    var legend2 = L.control({position: 'topleft'});

    legend2.onAdd = function (map) {

        var div = L.DomUtil.create('div', 'info legend layerSelector')

            div.innerHTML += ' \
  <label style="font-weight:normal; color:green;"><input type="checkbox" class="addLayer" id="police"> Police Precincts</label><br>  \
  <label style="font-weight:normal;color:blue;"><input type="checkbox" class="addLayer" id="council"> City Council Districts</label><br> \
    <label style="font-weight:normal;color:purple"><input type="checkbox" class="addLayer" id="schools"> Schools Districts</label><br>';

                //grades[i] + (grades[i + 1] ? '&ndash;' + grades[i + 1] + '<br>' : '+');


        return div;
    };

    legend2.addTo(map);

    $(".addLayer").click(function() {

      if ($(this).attr("id") == "police")
      {
        if ($(this).is(":checked")) {
            if (policeLayer != null) {
              policeLayer.addTo(map);
            } else {
              policeLayer = L.geoJson(police_precincts,{
                 style: {
                   weight: 2,
                   opacity: 1,
                   fillOpacity: 0,
                   color: 'green',
                   pointerEvents: 'none'
                 }

              });
              policeLayer.addTo(map);
            }

        } else {
            map.removeLayer(policeLayer);
        }

      } else if ($(this).attr("id") == "council")
      {
        if ($(this).is(":checked")) {
          if (councilLayer != null) {
            councilLayer.addTo(map);
          } else {
            councilLayer = L.geoJson(city_council_districts,{
               style: {
                 weight: 2,
                 opacity: 1,
                 fillOpacity: 0,
                 color: 'blue',
                 pointerEvents: 'none'
               }

            });
            councilLayer.addTo(map);
          }

        } else {
            map.removeLayer(councilLayer);
        }

      } else if ($(this).attr("id") == "schools")
      {

        if ($(this).is(":checked")) {
          if (schoolLayer != null) {
            schoolLayer.addTo(map);
          } else {
            schoolLayer = L.geoJson(school_districts,{
               style: {
                 weight: 2,
                 opacity: 1,
                 fillOpacity: 0,
                 color: 'purple',
                 pointerEvents: 'none'

               }

            });
            schoolLayer.addTo(map);
          }

        } else {
            map.removeLayer(schoolLayer);
        }
      }
    });


} else { //just change color
  //update shading of the map

  geojsonLayer.eachLayer(function (layer) {

    for (var i = 0; i< data.length; i ++){
      var thisItem = data[i];
      if(layer.feature.properties.ZCTA5CE10 == thisItem.grid_zipcode) {
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
        .attr("y", -35)
        .attr("x", -170)
        .attr("dy", ".71em")
        .style("text-anchor", "end")
        .text("Probability(%)");


        /*svg.append("g")
          .attr("class", "y axis")
          .call(yAxis)
          .append("text")
          .attr("transform", function() {
              return d3.svg.transform()
                  .rotate(-90) //.translate(200, 100)
          })
          .attr("y", 6)
          .attr("dy", ".71em")
          .style("text-anchor", "end")
          .text("Probability(%)");
          */



  svg.append("path")
      .datum(data)
      .attr("class", "line")
      .attr("d", line)

  svg.selectAll(".dot")
      .data(data)
      .enter().append("circle")
      .attr("class", function(d) { return "dot zipcode_" + d.grid_zipcode; })
      .attr("r", 1)
      .attr("cx", function(d) { return x(d.rank); })
      .attr("cy", function(d) { return y(d.probability * 100); })
      .on("mouseover", function(d) {
        dotMouseOver(d);
      })
      .on("mouseout", function(d) {
        dotMouseOut(d);

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
height2 = 200 - margin.top - margin.bottom;

var x2 =  d3.time.scale()
          .range([0,width2])

var y2 = d3.scale.linear()
              .range([height2, 0])

var xAxis2 = d3.svg.axis()
                  .scale(x2)
                  .orient("bottom")

var yAxis2 = d3.svg.axis()
                  .scale(y2)
                  .orient("left")
                  .ticks(4);


var line2 = d3.svg.line()
  .x(function(d) { return x2(d.key_as_string); })
  .y(function(d) { return y2(d.doc_count); });

var svg2 = d3.select("#highProbCounChart").append("svg")
  .attr("width", width2 + margin.left + margin.right)
  .attr("height", height2 + margin.top + margin.bottom)
  .append("g")
  .attr("transform", "translate(" + margin.left + "," + margin.top + ")");

var tip2 = d3.tip()
    .attr('class', 'd3-tip')
    .offset([-10, 0])
    .html(function(d) {
      return "<strong>Date:</strong>&nbsp;<span style='color:white'>" + d.key_as_string + "</span><br><strong>Count:</strong>&nbsp;<span style='color:white'>" + d.doc_count + "</span>";
    })

svg2.call(tip2);

function updateData2(lower, upper)
{

  var threshold = $("#probThreshold").val()

  d3.json("/tenDayTrend?lower="
                      +  lower + "&upper=" + upper + "&threshold=" + threshold,
                       function(error, data2) {

    var formatDate = d3.time.format("%Y-%m-%dT%H:%M:%S.%LZ");

    for (var i = 0; i < data2.length; i++) {
       data2[i].key_as_string = formatDate.parse(data2[i].key_as_string);
  }

  svg2.selectAll("path").remove();
  svg2.selectAll(".dot2").remove();

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
        .attr("y", -35)
        .attr("x", -30)
        .attr("dy", ".71em")
        .style("text-anchor", "end")
        .text("High Risk Count");

  svg2.append("path")
      .datum(data2)
      .attr("class", "line")
      .attr("d", line2)

  svg2.selectAll(".dot2")
      .data(data2)
      .enter().append("circle")
      .attr("class", "dot2")
      .attr("r", 2)
      .attr("cx", function(d) { return x2(d.key_as_string); })
      .attr("cy", function(d) { return y2(d.doc_count); })
      .on("mouseover", function(d) {
            tip2.show(d);
      })
      .on("mouseout", function(d) {
            tip2.hide(d);
      });
<!-- line chart ends here-->

});

}


/**************
speed limit distribution bar chart
**************/

var marginBar = {top: 10, right:10, bottom: 20, left: 50};

/*var fakeData = [   { "speed": "5mph", 	"percentage" : 0.035},
            { "speed": "15mph", 	"percentage" : 0.033},
                { "speed": "25mph", 	"percentage" : 0.55},
                { "speed": "35mph", 	"percentage" : 0.3},
                { "speed": "45mph", 	"percentage" : 0.02},
                { "speed": "55mph", 	"percentage" : 0},
              { "speed": "65mph", 	"percentage" : 0},
              { "speed": "75mph", 	"percentage" : 0} ];
              */


var widthBar = 350 - marginBar.left - marginBar.right;
var heightBar = 155 - marginBar.top - marginBar.bottom;


var svgBar = d3.select("#zipcodeSpeeds").append("svg")
  .attr("width", widthBar + marginBar.left + marginBar.right)
  .attr("height", heightBar + marginBar.top + marginBar.bottom)
  .append("g")
  .attr("transform", "translate(" + marginBar.left + "," + marginBar.top + ")");

function drawZipcodeBar(fakeData)
{

  var xBar = d3.scale.ordinal()
      .rangeRoundBands([0, widthBar], .1);


  var yBar = d3.scale.linear()
                .range([heightBar, 0])

  var xAxisBar = d3.svg.axis()
                    .scale(xBar)
                    .orient("bottom")

  var yAxisBar = d3.svg.axis()
                        .scale(yBar)
                        .orient("left")
                        .ticks(5, "%");

  xBar.domain(fakeData.map(function(d) { return d.speed; }));
  yBar.domain([0, 1]);

  for (var i = 0; i < fakeData.length; i++) {
       fakeData[i].percentage = type(fakeData[i].percentage);
  }

  svgBar.selectAll(".bar").remove();
  svgBar.selectAll(".x.axis.bar").remove();
  svgBar.selectAll(".y.axis").remove();

  svgBar.append("g")
      .attr("class", "x axis bar")
      .attr("transform", "translate(0," + heightBar + ")")
      .call(xAxisBar);

  svgBar.append("g")
      .attr("class", "y axis")
      .call(yAxisBar)
    .append("text")
      .attr("transform", "rotate(-90)")
      .attr("y", 6)
      .attr("dy", ".71em")
      .style("text-anchor", "end")
      .text("Pecentage");

  svgBar.selectAll(".bar")
      .data(fakeData)
    .enter().append("rect")
      .attr("class", "bar")
      .attr("x", function(d) { return xBar(d.speed); })
      .attr("width", xBar.rangeBand())
      .attr("y", function(d) { return yBar(d.percentage); })
      .attr("height", function(d) { return heightBar - yBar(d.percentage); });

}

function type(d) {
  d.frequency = +d.frequency;
  return d;
}


/**************
ROC linechart start here
**************/

function rocLineChart() {

  var marginROC = {top: 20, right:20, bottom: 40, left: 40};

  var widthROC = 250 - marginROC.left - marginROC.right,
      heightROC = 250 - marginROC.top - marginROC.bottom;

  function my(divId, dataROC) {

    var xROC =  d3.scale.linear()
              .range([0,widthROC])

    var yROC = d3.scale.linear()
                  .range([heightROC, 0])

    var xAxisROC = d3.svg.axis()
                      .scale(xROC)
                      .orient("bottom")
                      .ticks(4);

    var yAxisROC = d3.svg.axis()
                      .scale(yROC)
                      .orient("left")
                      .ticks(4);

    var dataForDiagonal = [{x:0, y:0}, {x:1,y:1}];

    var lineROC = d3.svg.line()
      .x(function(d) { return xROC(d.fpr); })
      .y(function(d) { return yROC(d.tpr); });

    var diagonal = d3.svg.line()
        .x(function(d) { return xROC(d.x); })
        .y(function(d) { return yROC(d.y); });

    var svgROC = d3.select(divId).append("svg")
      .attr("width", widthROC + marginROC.left + marginROC.right)
      .attr("height", heightROC + marginROC.top + marginROC.bottom)
      .append("g")
      .attr("transform", "translate(" + marginROC.left + "," + marginROC.top + ")");

      //d3.json("/roc", function(error, dataROC) {

      svgROC.selectAll("path").remove();

      yROC.domain(d3.extent(dataROC, function(d) {
                        return d.tpr;
                                  }));
      xROC.domain(d3.extent(dataROC, function(d) {
                        return d.fpr;
                                  }));

          //y.domain(d3.extent(data, function(d) { return d.close; }));
          svgROC.append("g")
            .attr("class", "x axis")
            .attr("transform", "translate(0," + heightROC + ")")
            .call(xAxisROC)
            .append("text")
            .attr("y",30)
            .attr("x",widthROC/2+10)
            .attr("dy", ".71em")
            .style("text-anchor", "end")
            .text("False Positive Rate");

          svgROC.append("g")
            .attr("class", "y axis")
            .call(yAxisROC)
            .append("text")
            .attr("transform", "rotate(-90)")
            .attr("y", -40)
            .attr("x", -20)
            .attr("dy", ".71em")
            .style("text-anchor", "end")
            .text("True Positive Rate");

      svgROC.append("path")
          .datum(dataROC)
          .attr("class", "line")
          .attr("d", lineROC)

      svgROC.append("path")
                      .datum(dataForDiagonal)
                      .attr("class", "line")
                      .attr("d", diagonal)
                      .style("stroke", "black")
                      .style("stroke-dasharray", ("3, 3"));

      //right boundaries
      svgROC.append("line")
                      .attr("x1", widthROC )
                      .attr("y1", 0)
                      .attr("x2", widthROC)
                      .attr("y2", heightROC)
                      .attr("stroke-width", 1)
                      .attr("stroke", "black");

      svgROC.append("line")
                      .attr("x1", 0)
                      .attr("y1", 0)
                      .attr("x2", widthROC)
                      .attr("y2", 0)
                      .attr("stroke-width", 1)
                      .attr("stroke", "black");

    //});

  }

  my.width = function(value) {
    if (!arguments.length) return width;
    width = value;
    return my;
  };

  my.height = function(value) {
    if (!arguments.length) return height;
    height = value;
    return my;
  };

  return my;
}

/**************
feature importancee
**************/

function featureBarChart() {

  var marginBar = {top: 10, right:10, bottom: 50, left: 90};

  var widthBar = 280 - marginBar.left - marginBar.right,
      heightBar = 500 - marginBar.top - marginBar.bottom;

  function my(divId, dataBar) {

    var yBar = d3.scale.ordinal()
        .rangeRoundBands([0, heightBar], .1);

    var xBar = d3.scale.linear()
                  .range([0,widthBar]);

    var yAxisBar = d3.svg.axis()
                      .scale(yBar)
                      .orient("left")
                      .tickFormat(function(d) { return convertText(d); });

    var xAxisBar = d3.svg.axis()
                          .scale(xBar)
                          .orient("bottom")
                          .ticks(5);

    var svgBar = d3.select(divId).append("svg")
      .attr("width", widthBar + marginBar.left + marginBar.right)
      .attr("height", heightBar + marginBar.top + marginBar.bottom)
      .append("g")
      .attr("transform", "translate(" + marginBar.left + "," + marginBar.top + ")");

      yBar.domain(dataBar.map(function(d) {
        return d._source.Feature;

      }));
      xBar.domain([0, 1]);

      svgBar.append("g")
          .attr("class", "x axis")
          .attr("transform", "translate(0," + heightBar + ")")
          .call(xAxisBar)

      svgBar.append("g")
          .attr("class", "y axis")
          .call(yAxisBar)

      svgBar.selectAll(".bar")
          .data(dataBar)
        .enter().append("rect")
          .attr("class", "bar")
          .attr("y", function(d) { return yBar(d._source.Feature); })
          .attr("height", yBar.rangeBand())
          .attr("x", function(d) { return xBar(0); })
          .attr("width", function(d) { return xBar(d._source.Rating); });


  }

  return my;
}

function convertText(featureText)
{
  switch (featureText) {
  case "hour":
    return "Hour of Day";
  case "liquor":
    return "Liquor Lic.";
  case "rd_cond":
      return "311 Requests";
  case "temp":
      return "Temperature";
  case "rd_count":
      return "Road Count";
  case "dayOfWeek":
      return "Day of Week";
  case "zipcode":
      return "Zipcode";
  case "zip_area":
      return "Zipcode Area";
  case "zip_area":
      return "Zipcode Area";
  case "windspeed":
      return "Wind Speed";
  case "month":
      return "Month";
  case "speed35":
      return "% of 35mph";
  case "speed5":
    return "% of 5mph";
  case "speed15":
    return "% of 15mph";
  case "speed25":
    return "% of 25mph";
  case "speed45":
    return "% of 45mph";
  case "speed55":
    return "% of 55mph";
  case "speed65":
    return "% of 65mph";
  case "speed85":
    return "% of 85mph";
  case "avg_aadt":
    return "Av. Traffic Ct";
  case "med_aadt":
    return "Md. Traffic Ct";
  case "rain":
    return "Raining";
  case "snow":
    return "Snow";
  case "fog":
      return "Foggy";
  case "med_speed":
    return "Md. Speed";
  case "bridges":
    return "# of Bridges";
  case "tunnels":
    return "# of Tunnels";
  case "precip":
    return "Precipit.";
  case "road_len":
    return "Road Length";
  }
}
