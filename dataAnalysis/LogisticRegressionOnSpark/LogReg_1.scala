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
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.classification.{LogisticRegressionWithLBFGS, LogisticRegressionModel}
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import com.github.fommil.netlib.BLAS._

/*
Execute below in this folder to run:
sbt clean assembly && $SPARK_HOME/bin/spark-submit   --master spark://spark1:7077 $(find target -iname "*assembly*.jar") 169.53.138.92:9200,169.53.138.84:9200 accident Dav1dC0C0
*/

//object Main extends App {

object Main {

   def main (args : Array[String]) {
	//there must be better way of doing this... 
	val nodes = args(0)
	val user = args(1)
        val password = args(2)

  	val conf = new SparkConf().setAppName("Simple Application")
		.set("es.nodes", nodes )
		.set("es.net.http.auth.user",user) 
          	.set("es.net.http.auth.pass",password) 
	val sc = new SparkContext(conf)

	val esRDD = sc.esRDD("dataframe/rows")

	//convert to LabeledPoint
	//function to merge zipcode with each hour intervals, which returns arrray of GridRow, which gets flatmapped
	def resetZipcodeLabel(row:Tuple2[String,scala.collection.Map[String,AnyRef]]): LabeledPoint = {
		
		val zipcode = row._2("grid_zipcode").asInstanceOf[Long].toDouble
		val dayOfMonth = row._2("grid_day").asInstanceOf[Long].toDouble
                val dayOfWeek = row._2("grid_dayOfWeek").asInstanceOf[Long].toDouble
                val hour = row._2("grid_hourOfDay").asInstanceOf[Long].toDouble
                val month = row._2("grid_month").asInstanceOf[Long].toDouble
                val label = row._2("grid_isAccident").asInstanceOf[Long].toDouble

                //val dv: Vector = Vectors.dense(zipcode, dayOfMonth,dayOfWeek,hour,month)
				val dv: Vector = Vectors.dense(zipcode,dayOfWeek,hour,month)

		return LabeledPoint(label,dv)
	}
	
	//println(esRDD.first()._2("grid_zipcode").asInstanceOf[Long].toDouble.getClass.getName)

	//test mapping with small portion of the data 
	//val smallData = sc.parallelize(esRDD.take(100)).map(d => resetZipcodeLabel(d))
	//println(smallData.first())

	val data = esRDD.map(d => resetZipcodeLabel(d))
	//val data = sc.parallelize(esRDD.take(100)).map(d => resetZipcodeLabel(d))

	// Split data into training (60%) and test (40%).
	val splits = data.randomSplit(Array(0.6, 0.4), seed = 11L)
	val training = splits(0).cache()
	val test = splits(1)
	//println(training.first())
	// Run training algorithm to build the model
	
	val model =sc.broadcast(new LogisticRegressionWithLBFGS()
  		.run(training))

	//println(model)
	//println(model.getNumFeatures())
	

	/*val pt = test.first()
	println(model.predict(pt.features))
	println(pt)
	*/
	// Compute raw scores on the test set.
	val predictionAndLabels =test.map { d =>
  		val prediction = model.value.predict(d.features)
  		(prediction, d.label)
		//(d.label,d.features)
		//model
	}
	
	//predictionAndLabels.foreach(println)
	
	val total = predictionAndLabels.count()
	println(total)
	val correct = predictionAndLabels.filter(line => line._1 == line._2).count() 
	println(correct/total.toFloat)
	
	/*

	// Get evaluation metrics.
	val metrics = new MulticlassMetrics(predictionAndLabels)
	val precision = metrics.precision
	println("Precision = " + precision)
	*/
   }
}


/*old code*/
/*
        case class Trip(departure: String, arrival: String, date: Date)

val upcomingTrip = Trip("OTP", "SFO",Calendar.getInstance.getTime )
val lastWeekTrip = Trip("MUC", "OTP",Calendar.getInstance.getTime)

val rdd = sc.makeRDD(Seq(upcomingTrip, lastWeekTrip))
EsSpark.saveToEs(rdd, "spark/docs")
*/
