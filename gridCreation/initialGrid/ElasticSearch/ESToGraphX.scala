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
	//val d1: org.joda.time.DateTime =new DateTime("2012-07-01T00:00:00-00:00")
	val d1: org.joda.time.DateTime =new DateTime(2012,7,1,0,0,DateTimeZone.UTC)
	//LocalDateTime(int year, int monthOfYear, int dayOfMonth, int hourOfDay, int minuteOfHour) 
	
	//final date is 2016-02-22 so have end date to one day later but it will not be included in the final list of dates
	//val d2: org.joda.time.DateTime = new DateTime("2012-07-03T00:00:00-00:00")
	//val d2: org.joda.time.DateTime = new DateTime("2016-02-23T00:00:00-00:00")
	val d2: org.joda.time.DateTime = new DateTime(2016,2,23,0,0,DateTimeZone.UTC)
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
	case class GridRow(grid_id: String, grid_zipcode: Integer, grid_fullDate: Date, grid_dayOfWeek: Integer,grid_isAccident:Integer)
	//case class GridRow(grid_id: String, grid_zipcode: Integer, grid_fullDate: Date,grid_isAccident:Integer)

	//function to merge zipcode with each hour intervals, which returns arrray of GridRow, which gets flatmapped
	def mergeWithZipCodes(dt:DateTime): Array[GridRow] = {

val zipCodes = Array(10310,10311,10312,10005,10014,10018,10474,11235,11356,10453,11416,11425,11426,11430,11104,11212,11213,11214,10309,10454,10306,10452,10458,11232,11236,10459,10462,10463,11351,11354,11231,11357,11362,11368,10455,10456,10460,11210,11217,11223,10152,10154,10165,10172,10280,10457,10465,11234,11237,10467,10466,10472,10029,10039,11233,10115,10119,10169,10303,11373,10461,10471,11414,11003,11004,11238,11413,10031,10037,10044,11377,10032,10033,10034,11412,11101,10069,10111,10162,10167,11228,11229,11369,11005,11105,10170,10171,10174,10308,10451,10468,11364,11365,11355,11359,11360,11366,11367,10473,10009,11375,11358,11361,11363,11371,10110,11379,11411,11001,11040,10010,10011,10279,10282,10302,10065,10173,10177,10271,10278,10305,10019,10027,10040,11225,11230,10470,10006,10016,11106,10002,10003,10012,10013,10475,11226,10103,10112,10153,10168,10023,10030,10036,10038,10024,10028,10075,11372,11374,11378,11102,11109,11204,11206,11216,11224,11697,11417,11429,11436,11434,11201,11205,11207,11219,11221,11581,11422,11432,11433,11435,11691,11420,11421,11424,11427,11451,11385,11203,11209,11215,11218,11220,11692,11694,11415,11418,11419,11428)

		val gr:Array[GridRow] = new Array[GridRow](zipCodes.length)

		//id format is:
		//2012-07-01T00:00:00_zipcode
		val  dtfOut = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss");
		val dateInString = dtfOut.print(dt)

	 	for ( i <- 0 to (zipCodes.length - 1)) {
	     		gr(i) = GridRow(dateInString+ "_" +zipCodes(i).toString,zipCodes(i),dt.toDate(), dt.getDayOfWeek(),0)
			//gr(i) = GridRow(dt.getMillis().toString+ "_" +zipCodes(i).toString,zipCodes(i),dt.toDate(), dt.getDayOfWeek())
      		}
		return gr
	}

	val dateRdd = sc.parallelize(dates).flatMap(d => mergeWithZipCodes(d))


	//dateRdd.collect().foreach(println)

	//println(dateRdd.first())
	//save to ES
	EsSpark.saveToEs(dateRdd, "grids/rows", Map("es.mapping.id" -> "grid_id"))
}


/*old code*/
/*
        case class Trip(departure: String, arrival: String, date: Date)

val upcomingTrip = Trip("OTP", "SFO",Calendar.getInstance.getTime )
val lastWeekTrip = Trip("MUC", "OTP",Calendar.getInstance.getTime)

val rdd = sc.makeRDD(Seq(upcomingTrip, lastWeekTrip))
EsSpark.saveToEs(rdd, "spark/docs")
*/
