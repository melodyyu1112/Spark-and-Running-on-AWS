import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.*;
import scala.Tuple2;

import java.io.Serializable;
import java.util.Comparator;
import java.util.Collections;

public class HW6 {

  // the full input data file is at s3://us-east-1.elasticmapreduce.samples/flightdata/input
  public static void main(String[] args) {

    if (args.length < 2)
      throw new RuntimeException("Usage: HW6 <datafile location> <output location>");

    String dataFile = args[0];
    String output = args[1];

    // turn off logging except for error messages
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR);
    Logger.getLogger("org.apache.spark.storage.BlockManager").setLevel(Level.ERROR);

    // for running locally
    //  SparkSession spark = SparkSession.builder().appName("HW6").config("spark.master", "local").getOrCreate();

    // for running on ec2
    SparkSession spark = SparkSession.builder().appName("HW6").getOrCreate();

   //    Dataset<Row> r = warmup(spark, dataFile);
   //    r.javaRDD().repartition(1).saveAsTextFile(output);

    /* Problem 1 */
    Dataset<Row> r1 = Q1(spark, dataFile);
    r1.javaRDD().repartition(1).saveAsTextFile(output);


    /* Problem 2 */
    /*Implement the same query as above, but use the RDD API.
      convert a Dataset to a JavaRDD by calling javaRDD() in the skeleton code.*/
      JavaRDD<Row> r2 = Q2(spark, dataFile);
      r2.repartition(1).saveAsTextFile(output);

    /* Problem 3 */
     JavaPairRDD<Tuple2<String, Integer>, Integer> r3 = Q3(spark, dataFile);
     r3.repartition(1).saveAsTextFile(output);

    /* Problem 4 */
     Tuple2<String, Integer> r4 = Q4(spark, dataFile);
     spark.createDataset(Collections.singletonList(r4), Encoders.tuple(Encoders.STRING(), Encoders.INT()))
          .javaRDD().saveAsTextFile(output);

    /* Problem 5 */
     JavaPairRDD<String, Double> r5 = Q5(spark, dataFile);
     r5.repartition(1).saveAsTextFile(output);


    // saves the results to an output file in parquet format (useful to generate a test dataset on an even smaller dataset)
     r.repartition(1).write().parquet(output);

    // shut down
    spark.stop();
  }

  // offsets into each Row from the input data read
  public static final int MONTH = 2;
  public static final int ORIGIN_CITY_NAME = 15;
  public static final int DEST_CITY_NAME = 24;
  public static final int DEP_DELAY = 32;
  public static final int CANCELLED = 47;

   public static Dataset<Row> warmup (SparkSession spark, String dataFile) {

    Dataset<Row> df = spark.read().parquet(dataFile);

    // create a temporary table based on the data that we read
    df.createOrReplaceTempView("flights");

    // run a SQL query
    Dataset<Row> r = spark.sql("SELECT * FROM flights LIMIT 10");

    // this prints out the results
    r.show();

    // this uses the RDD API to project a column from the read data and print out the results
    r.javaRDD()
     .map(t -> t.get(DEST_CITY_NAME))
     .foreach(t -> System.out.println(t));

    return r;
  }



  /* Select all flights that leave from Seattle, WA, and return the destination city names.
  Only return each destination city name once. Implement this using the Dataset API.
  This should be trivial and is intended for you to learn about the Dataset API. */
 
    public static Dataset<Row> Q1 (SparkSession spark, String dataFile) {

    Dataset<Row> df = spark.read().parquet(dataFile);

    df.createOrReplaceTempView("flights");
    Dataset<Row> r = spark.sql("SELECT distinct destcityname FROM flights WHERE origincityname = 'Seattle, WA'");
    r.show();

    return r;
  }

*/
  /*Implement the same query as above, but use the RDD API.
   We convert a Dataset to a JavaRDD by calling javaRDD() in the skeleton code.*/
/*
  public static JavaRDD<Row> Q2 (SparkSession spark, String dataFile) {

    JavaRDD<Row> d = spark.read().parquet(dataFile).javaRDD();
    JavaRDD<Row> seattle = d.filter(a -> a.get(ORIGIN_CITY_NAME).toString().equals("Seattle, WA"));
    JavaRDD<Row> dest_city = seattle.map(c -> RowFactory.create(c.get(DEST_CITY_NAME)));
    d = dest_city.distinct();

    return d;
  }
*/

  /*Find the number of non-cancelled flights per month that departs from each city,
  return the results in a RDD where the key is a pair (i.e., a Tuple2 object), consisting of a String
  for the departing city name, and an Integer for the month.
  The value should be the number of non-cancelled flights. */

/*  public static JavaPairRDD<Tuple2<String, Integer>, Integer> Q3 (SparkSession spark, String dataFile) {
    JavaRDD<Row> d = spark.read().parquet(dataFile).javaRDD();
    JavaRDD<Row> cancelled = d.filter(a -> a.get(CANCELLED).equals(0));
    JavaPairRDD<Tuple2<String, Integer>, Integer> ones = cancelled.mapToPair(t -> new Tuple2<Tuple2<String, Integer>, Integer> (new Tuple2<String, Integer>(t.getString(ORIGIN_CITY_NAME), t.getInt(MONTH)), 1));
    ones = ones.reduceByKey((v1, v2) -> v1 + v2);
    return ones;
  }
*/
/*
  public static Tuple2<String, Integer> Q4 (SparkSession spark, String dataFile) {

    JavaRDD<Row> d = spark.read().parquet(dataFile).javaRDD();
    JavaRDD<Tuple2<String,String>> mostDest = d.map(f -> new Tuple2<String, String>(f.getString(ORIGIN_CITY_NAME), f.getString(DEST_CITY_NAME))).distinct();
    JavaPairRDD<String,Integer> x = mostDest.mapToPair(t -> new Tuple2<String, Integer>(t._1, 1)).reduceByKey((v1, v2) -> v1 + v2);
    Tuple2<String,Integer> y = x.max(new MaxDestComparator());

    JavaRDD<Row> filter1 = d.filter(f -> f.get(ORIGIN_CITY_NAME).equals(y._1));
    JavaPairRDD<String, Integer> result = filter1.mapToPair(t -> new Tuple2<>(t.getString(ORIGIN_CITY_NAME), 1)).reduceByKey((v1, v2) -> v1 + v2);
    return result.first();
  }

  private static class MaxDestComparator implements Comparator<Tuple2<String,Integer>>, Serializable {

    @Override
    public int compare(Tuple2<String, Integer> va1, Tuple2<String, Integer> va2) {
      return va1._2 - va2._2;
    }

  }
*/
/*
Compute the average delay from all departing flights for each city.
Flights with NULL delay values (due to cancellation or otherwise) should not be counted.
Return the results in a RDD where the key is a String for the city name, and the value is a Double for the average delay in minutes.  */
  public static JavaPairRDD<String, Double> Q5 (SparkSession spark, String dataFile) {

    JavaRDD<Row> d = spark.read().parquet(dataFile).javaRDD();
    JavaRDD<Row> no_null_cancel = d.filter(t -> t.get(DEP_DELAY) != null);

    JavaPairRDD<String,Double> total_delay = no_null_cancel
            .mapToPair(t -> new Tuple2<String,Double> (t.getString(ORIGIN_CITY_NAME), Double.valueOf(t.get(DEP_DELAY).toString())))
            .reduceByKey((v1,v2) -> v1 + v2);

    JavaPairRDD<String,Double> num_flights = no_null_cancel
            .mapToPair(t -> new Tuple2<String,Double> (t.getString(ORIGIN_CITY_NAME), 1.0))
            .reduceByKey((v1,v2) -> v1 + v2);

    JavaPairRDD<String, Tuple2<Double, Double>> delayAndNumCount = total_delay.join(num_flights);

    JavaPairRDD<String, Double> average_delay = delayAndNumCount.mapToPair(t -> new Tuple2<String, Double>(t._1, (t._2._1 / t._2._2)));

    return average_delay;
  }
