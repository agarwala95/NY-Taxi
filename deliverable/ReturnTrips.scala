import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Column
import org.apache.spark.sql.Dataset
import math._
import org.apache.spark.sql.Row

object ReturnTrips {
  def compute(trips : Dataset[Row], dist : Double, spark : SparkSession) : Dataset[Row] = {

        import spark.implicits._
        val R = 6371 //radius in km
     
    	def haversine(lat1:Double, lon1:Double, lat2:Double, lon2:Double)={
            val dLat=(lat2 - lat1).toRadians
            val dLon=(lon2 - lon1).toRadians
         
            val a = math.pow(math.sin(dLat/2),2) + math.pow(math.sin(dLon/2),2) * math.cos(lat1.toRadians) * math.cos(lat2.toRadians)
            val c = 2 * math.asin(math.sqrt(a))
            math.abs(R * c * 1000)
        }
    	
    	val dist_func = udf(haversine(_:Double,_:Double,_:Double,_:Double))
    	val round = udf(math.floor _)
    	
    	val trips_DF = trips.select('tpep_pickup_datetime,'tpep_dropoff_datetime,'pickup_longitude,'pickup_latitude,'dropoff_longitude,'dropoff_latitude);
    	
    	val interval = (dist*0.001)/111.2;

    	val trips_time = trips_DF.withColumn("pick_time",unix_timestamp($"tpep_pickup_datetime")).withColumn("drop_time",unix_timestamp($"tpep_dropoff_datetime"));
    
    	val trips_buck = trips_time.withColumn("Bucket_ptime",round($"pick_time"/(28800))).withColumn("Bucket_dtime",round($"drop_time"/(28800))).withColumn("Bucket_PickLat",round($"pickup_latitude"/interval)).withColumn("Bucket_DropLat",round($"dropoff_latitude"/interval));
    
    	val trips_buckNeighbors = trips_buck.withColumn("Bucket_ptime", explode(array($"Bucket_ptime"-1, $"Bucket_ptime"))).withColumn("Bucket_PickLat", explode(array($"Bucket_PickLat" - 1, $"Bucket_PickLat", $"Bucket_PickLat" + 1))).withColumn("Bucket_DropLat",explode(array($"Bucket_DropLat" - 1, $"Bucket_DropLat", $"Bucket_DropLat" + 1)));
    
    	val return_trips = trips_buck.as("a").join(trips_buckNeighbors.as("b"),
    	($"b.Bucket_PickLat" === $"a.Bucket_DropLat")
    	&& ($"a.Bucket_PickLat" === $"b.Bucket_DropLat")
    	&& ($"a.Bucket_dtime" === $"b.Bucket_ptime") 
    	&& (dist_func($"a.dropoff_latitude",$"a.dropoff_longitude",$"b.pickup_latitude",$"b.pickup_longitude") < dist)
    	&& (dist_func($"b.dropoff_latitude",$"b.dropoff_longitude",$"a.pickup_latitude",$"a.pickup_longitude") < dist)
    	&& ($"b.pick_time" > $"a.drop_time")
    	&& (($"a.drop_time" + 28800) > $"b.pick_time"));
    	
    	return_trips
  }
}