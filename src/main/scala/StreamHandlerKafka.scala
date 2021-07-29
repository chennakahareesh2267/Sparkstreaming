import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming._
import org.apache.spark.sql.types._
import java.sql.{Connection, DriverManager, Statement, Timestamp}
import java.util.Properties
case class DeviceData(device: String, temp: Double, humd: Double, pres: Double,timestamp:Timestamp)
class JDBCSink(url:String,user:String,pwd:String) extends ForeachWriter[DeviceData]{
  val driver = "org.postgresql.Driver"
  var connection:Connection = _
  var statement:Statement = _
  def open(partitionId: Long, version: Long): Boolean = {
    Class.forName(driver)
    connection = DriverManager.getConnection(url,user,pwd)
    statement = connection.createStatement
    true
  }

  def process(record: DeviceData) = {
    val sqlQuery = "INSERT INTO trace.table_name(device,temp,humd,pres,timestamp) values('"+record.device+"','"+record.temp+"','"+record.humd+"','"+record.pres+"','"+record.timestamp+"')"
    statement.executeUpdate(sqlQuery)
  }

  def close(errorOrNull: Throwable): Unit = {
    connection.close()
  }
}

object StreamHandlerKafka {

  val prop = new Properties()
  prop.setProperty("url","jdbc:postgresql://validhost:5432/sysdb")
  prop.setProperty("user","username")
  prop.setProperty("password","password")
  prop.setProperty("hostname","validhost")
  prop.setProperty("port","5432")
  prop.setProperty("database","sysdb")
  prop.setProperty("driver","org.postgresql.Driver")

  def main(args: Array[String]): Unit = {
    val spark= SparkSession.builder().master("local[*]").appName("Stream Handler Kafka").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    import spark.implicits._
    val inputDF= spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers","localhost:9092")
      .option("startingOffsets","latest")
      .option("subscribe","weather1")
      .load()


    /*
          inputDF example
        +----+--------------------+-------+---------+------+--------------------+-------------+
        | key|               value|  topic|partition|offset|           timestamp|timestampType|
        +----+--------------------+-------+---------+------+--------------------+-------------+
        |null|[32 30 32 31 2D 3...|weather|        0|  6218|2021-07-28 15:34:...|            0|
        |null|[32 30 32 31 2D 3...|weather|        0|  6219|2021-07-28 15:34:...|            0|
        +----+--------------------+-------+---------+------+--------------------+-------------+
        use the following you get the values columns as csv line

    val rawDF = inputDF.selectExpr("CAST(value as STRING)").as[String]
    val expandedDF = rawDF.map(row => row.split(","))
      .map(row => DeviceData(
        row(1),
        row(2).toDouble,
        row(3).toDouble,
        row(4).toDouble,
        Timestamp.valueOf(row(0))
      )).withWatermark("timestamp","10 minutes")
      */
    val rawDF = inputDF.select(
      get_json_object(($"value").cast("string"),"$.profile_name").alias("device"),
      get_json_object(($"value").cast("string"),"$.temp").alias("temp").cast(DoubleType),
      get_json_object(($"value").cast("string"),"$.humd").alias("humd").cast(DoubleType),
      get_json_object(($"value").cast("string"),"$.pres").alias("pres").cast(DoubleType),
      get_json_object(($"value").cast("string"),"$.current_time").alias("timestamp")
    ).withColumn("timestamp",to_timestamp(col("timestamp")))
      .withWatermark("timestamp","10 minutes").as[DeviceData]

  /*
    Complete Mode - The entire updated Result Table will be written to the external storage.
                    It is up to the storage connector to decide how to handle writing of the entire table.

    Append Mode - Only the new rows appended in the Result Table since the last trigger will be written to the external storage.
                  This is applicable only on the queries where existing rows in the Result Table are not expected to change.

    Update Mode - Only the rows that were updated in the Result Table since the last trigger will be written to the external
                  storage (available since Spark 2.1.1). Note that this is different from the Complete Mode in that this mode only
                  outputs the rows that have changed since the last trigger. If the query doesn’t contain aggregations,
                  it will be equivalent to Append mode.
  */

//    // console
//    val consoleQuery = rawDF
//      .writeStream
//      .format("console")
//      .outputMode("update")
//      .start()
//    consoleQuery.awaitTermination()
//
//    // foreach writer
//
//    val customWriter = new JDBCSink(prop.getProperty("url"),prop.getProperty("user"),prop.getProperty("password"))
//    val foreachQuery=rawDF.writeStream.foreach(customWriter).outputMode("append").start()
//    foreachQuery.awaitTermination()

    // foreachbatch
    val foreachBatchQuery = rawDF.writeStream.foreachBatch(forEachBatchSink).outputMode("append").start()
    foreachBatchQuery.awaitTermination()



  }
  def forEachBatchSink = (df:Dataset[DeviceData],batchId:Long) => {
    df.write.mode(SaveMode.Append).jdbc(prop.getProperty("url"),"trace.table_name",prop)
  }

}
