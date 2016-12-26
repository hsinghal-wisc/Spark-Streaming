import java.sql.Timestamp

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object PartBQuestion1 {

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
      .appName("StructuredNetworkWordCountWindowed")
      .getOrCreate()

  import spark.implicits._
  import org.apache.spark.sql.types._

  val userSchema = new StructType().add("userA", "String").add("userB", "String").add("timestamp","Timestamp").add("interaction","String")
    val lines = spark.readStream
      .option("sep", ",")
      .schema(userSchema)
      .csv("hdfs://10.254.0.178:8020/monitor/")
    
lines.isStreaming
lines.printSchema

      val windowedCounts = lines.groupBy(
      window($"timestamp", "60 minutes", "30 minutes"), $"interaction"
    ).count().orderBy("window")


    val query = windowedCounts.writeStream
      .outputMode("complete")
      .format("console")
.option("numRows", "100000")      
.option("truncate", "false")
      .start()

    query.awaitTermination()
  }
}

