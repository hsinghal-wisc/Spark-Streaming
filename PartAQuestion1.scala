import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.Partitioner
import org.apache.spark.HashPartitioner
import org.apache.spark.RangePartitioner

object PartAQuestion1 {
  def main(args: Array[String]) {

    val conf  = new SparkConf()
      .set("spark.executor.memory", "1G")
      .set("spark.driver.memory", "1G")
      .set("spark.executor.cores","4")
      .set("spark.task.cpus","1")
      .set("spark.eventLog.enabled","true")
      .set("spark.eventLog.dir","hdfs://10.254.0.178/logs")
      .set("spark.executor.instances","5")
//      .set("spark.master","SPARK_MASTER_IP:7077")
      .setMaster("spark://10.254.0.178:7077")

    val spark = SparkSession
      .builder.config(conf=conf)
      .appName("CS-838-Assignment2-PartA-1")
      .getOrCreate()
        
    val iters = args(1).toInt
    val lines = spark.read.textFile(args(0)).rdd
    val links = lines.map{ s =>
      val parts = s.split("\\s+")
      (parts(0), parts(1))
       }.distinct().groupByKey()
//.partitionBy(new HashPartitioner(80))
    var ranks = links.mapValues(v => 1.0)
    for (i <- 1 to iters) {
      val contribs = links.join(ranks).values.flatMap { case (urls, rank) =>
        val size = urls.size
        urls.map(url => (url, rank / size))
      }
      ranks = contribs.reduceByKey(_ + _).mapValues(0.15 + 0.85 * _)
    }
    val output = ranks.collect()
    output.foreach(tup => println(tup._1 + " has rank: " + tup._2 + "."))
    spark.stop()
  }
}

