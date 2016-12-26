import java.sql.Timestamp
import org.apache.spark.sql.streaming
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import scala.concurrent.duration._
import org.apache.spark.sql.streaming.ProcessingTime
import org.apache.spark.sql.streaming.OutputMode.Append

object PartBQuestion3 {

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
      .appName("CS-838-Assignment2-PartB-3")
      .getOrCreate()

  import spark.implicits._
  import org.apache.spark.sql.types._

  val userSchema = new StructType().add("userA", "String").add("userB", "String").add("timestamp","Timestamp").add("interaction","String")
    val lines = spark.readStream
      .option("sep", ",")
      .schema(userSchema)
      .csv("hdfs://10.254.0.178:8020/monitor/")
 val userSchema2 = new StructType().add("userA", "String")
val user = spark.read
            .option("sep", ",")
      .schema(userSchema2)
        .csv("hdfs://10.254.0.178:8020/input/user.csv")

   val windowedCounts = lines 
     .join(user, "userA")
//     .groupBy(window($"timestamp", "5 seconds", "5 seconds"),$"userA")
     .groupBy($"userA")
//   .agg(concat_ws(",",collect_list($"userB")))
//     .orderBy("window")
//      .sort(asc("window"))
        .count().orderBy($"userA").select("userA","count")
    val query = windowedCounts.writeStream
//      .outputMode("append")
        .outputMode("complete")
//      .format("parquet")
        .format("console")
//        .queryName("table")
        .trigger(ProcessingTime(5.seconds))
        .option("numRows", "100000")
        .option("truncate", "false")
//      .option("checkpointLocation", "hdfs://10.254.0.178:8020/")
//      .start("hdfs://10.254.0.178:8020/")
        .start()
    query.awaitTermination()
  }
}

