package spark_streaming

import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.SparkConf

/**
  * Created by lzz on 6/28/16.
  */
object Demo extends App{
  val sparkConf = new SparkConf().setMaster("local")
    .setAppName("My App")
  val ssc = new StreamingContext(sparkConf, Seconds(1))
  val lines = ssc.socketTextStream("localhost", 9999)
  val words = lines.flatMap(_.split(" "))
  val pairs = words.map(word => (word, 1))
  val wordCounts = pairs.reduceByKey(_ + _)
  wordCounts.print()
  ssc.start()
  ssc.awaitTermination()
}
