import org.apache.spark.sql.streaming
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import scala.concurrent.duration._
import org.apache.spark.sql.streaming.ProcessingTime
import org.apache.spark.sql.streaming.OutputMode.Append
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object PartBQuestion2 {

  def main(args: Array[String]) {

       val conf  = new SparkConf()
      .set("spark.executor.memory", "1G")
      .set("spark.driver.memory", "1G")
      .set("spark.executor.cores","4")
      .set("spark.task.cpus","1")
      .set("spark.eventLog.enabled","true")
      .set("spark.eventLog.dir","hdfs://10.254.0.178/logs")
      .set("spark.executor.instances","5")
      .setMaster("spark://10.254.0.178:7077")

    val spark = SparkSession
      .builder.config(conf=conf)
      .appName("CS-838-Assignment2-PartB-2")
      .getOrCreate()

  import spark.implicits._
  import org.apache.spark.sql.types._

  val userSchema = new StructType().add("userA", "String").add("userB", "String").add("timestamp","Timestamp").add("interaction","String")
    val lines = spark.readStream
      .option("sep", ",")
      .schema(userSchema)
      .csv("hdfs://10.254.0.178:8020/monitor/")
    
   val windowedCounts = lines.filter("interaction='MT'")
       .select("userB")

    val query = windowedCounts.writeStream
        .format("parquet")
//        .format("console")
        .trigger(ProcessingTime(10.seconds))
        .option("numRows", "100000")
        .option("truncate", "false")
      .option("checkpointLocation", "hdfs://10.254.0.178:8020/check")
      .start("hdfs://10.254.0.178:8020/outputparquet")
//        .start()
    query.awaitTermination()
  }
}

