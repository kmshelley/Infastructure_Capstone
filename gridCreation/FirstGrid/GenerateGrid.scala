import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark._
// To make some of the examples work we will also need RDD
import org.apache.spark.rdd.RDD
import org.elasticsearch.spark._ 
import scala.collection.JavaConversions._
import org.elasticsearch.spark.rdd.EsSpark
import java.util.Date
//import java.util.Calendar
import org.joda.time._
import org.joda.time.format.DateTimeFormatter
import org.joda.time.format.DateTimeFormat

/*
Execute below in this folder to run:
sbt clean assembly && $SPARK_HOME/bin/spark-submit   --master spark://spark1:7077 $(find target -iname "*assembly*.jar") ESIP2:9200,ESIP2:9200 ESUsername ESpassword
*/

object Main extends App {

	//there must be better way of doing this... 
	val nodes = args(0)
	val user = args(1)
        val password = args(2)

  	val conf = new SparkConf().setAppName("Simple Application")
		.set("es.nodes", nodes )
		.set("es.net.http.auth.user",user) 
          	.set("es.net.http.auth.pass",password) 
	val sc = new SparkContext(conf)

	//set start and end time
	//set timezone to Eastern so we are accounting for "lost" hour when DST started
	val nyczone:DateTimeZone = DateTimeZone.forID("America/New_York");

	//val d1: org.joda.time.DateTime =new DateTime("2012-07-01T00:00:00-00:00")
	val d1: org.joda.time.DateTime =new DateTime(2012,7,1,0,0,nyczone)
	//LocalDateTime(int year, int monthOfYear, int dayOfMonth, int hourOfDay, int minuteOfHour) 
	
	//final date is 2016-02-22 so have end date to one day later but it will not be included in the final list of dates
	//val d2: org.joda.time.DateTime = new DateTime("2012-07-03T00:00:00-00:00")
	//val d2: org.joda.time.DateTime = new DateTime("2016-02-23T00:00:00-00:00")
	val d2: org.joda.time.DateTime = new DateTime(2016,2,23,0,0,nyczone)
	print(d1)

	//find out hours inbetween
	val hours = Hours.hoursBetween(d1, d2).getHours()
	println("num of hours")
	println(hours)

	//create an array of datetimes in hour inteval between beginning and end date 
	val dates:Array[DateTime] = new Array[DateTime](hours)
	for ( i <- 0 to (hours-1)) {
		dates(i) = d1.withFieldAdded(DurationFieldType.hours(), i);
	}

	//class for defining schema, we will start with only three attributes
	 case class GridRow(grid_id: String, grid_zipcode: Integer, grid_fullDate: Date, grid_dayOfWeek: Integer,grid_hourOfDay: Integer,grid_isAccident:Integer, grid_month:Integer, grid_day:Integer)

	//function to merge zipcode with each hour intervals, which returns arrray of GridRow, which gets flatmapped
	def mergeWithZipCodes(dt:DateTime): Array[GridRow] = {

val zipCodes = Array(10271,10278,10279,10280,10282,10803,10301,10302,10303,10304,10305,10306,10307,10308,10309,10310,10311,10312,10314,11351,11354,11355,11356,11357,11358,11359,11360,11361,11362,11363,11364,11365,11366,11367,11368,11369,11370,11371,11372,11373,11374,11375,11377,11378,11379,11385,11201,11411,11412,11413,11414,11415,11416,11417,11418,11419,11420,11421,11422,11423,11424,11425,11426,11427,11428,11429,11430,11432,11433,11434,11435,11436,11451,11040,10451,10452,10453,10454,10455,10456,10457,10458,10459,10460,10461,10462,10463,10464,10465,10466,10467,10468,10469,10470,10471,10472,10473,10474,10475,11001,11004,11005,10001,10002,10003,10004,10005,10006,10007,10009,10010,10011,10012,10013,10014,10016,10017,10018,10019,10020,10021,10022,10023,10024,10025,10026,10027,10028,10029,10030,10031,10032,10033,10034,10035,10036,10037,10038,10039,10040,10044,10065,10069,10075,11101,11102,11103,11104,11105,11106,11109,10103,10110,10111,10112,10115,10119,10128,10152,10153,10154,11691,11692,11693,11694,11697,10162,10165,10167,10168,10169,10170,10171,10172,10173,10174,10177,11203,11204,11205,11206,11207,11208,11209,11210,11211,11212,11213,11214,11215,11216,11217,11218,11219,11220,11221,11222,11223,11224,11225,11226,11228,11229,11230,11231,11232,11233,11234,11235,11236,11237,11238,11239)

		val gr:Array[GridRow] = new Array[GridRow](zipCodes.length)

		//id format is:
		//2012-07-01T00:00:00_zipcode
		val  dtfOut = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss");
		val dateInString = dtfOut.print(dt)

	 	for ( i <- 0 to (zipCodes.length - 1)) {
	     		gr(i) = GridRow(dateInString+ "_" +zipCodes(i).toString,zipCodes(i),dt.toDate(), dt.getDayOfWeek(),dt.getHourOfDay(),0,dt.getMonthOfYear(),dt.getDayOfMonth())
			//gr(i) = GridRow(dt.getMillis().toString+ "_" +zipCodes(i).toString,zipCodes(i),dt.toDate(), dt.getDayOfWeek())
      		}
		return gr
	}

	val dateRdd = sc.parallelize(dates).flatMap(d => mergeWithZipCodes(d))
	//println(dateRdd.getClass.getName)

	//dateRdd.collect().foreach(println)

	//println(dateRdd.first())
	//save to ES
	EsSpark.saveToEs(dateRdd, "dataframe/rows", Map("es.mapping.id" -> "grid_id"))
}


/*old code*/
/*
        case class Trip(departure: String, arrival: String, date: Date)

val upcomingTrip = Trip("OTP", "SFO",Calendar.getInstance.getTime )
val lastWeekTrip = Trip("MUC", "OTP",Calendar.getInstance.getTime)

val rdd = sc.makeRDD(Seq(upcomingTrip, lastWeekTrip))
EsSpark.saveToEs(rdd, "spark/docs")
*/
