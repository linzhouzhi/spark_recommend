package spark_streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Minutes, Seconds, StreamingContext}

/**
  * Created by lzz on 6/28/16.
  */
object Kafka_wc extends App{
  val sparkConf = new SparkConf().setMaster("local")
    .setAppName("My App")
  val ssc = new StreamingContext(sparkConf, Seconds(1))

  //val Array(zkQuorum, group, topics, numThreads) = args
  val zkQuorum = "192.168.1.221:2181,192.168.1.222:2181,192.168.1.223:2181"
  val group = "group001"
  val topics = "my-replicated-topic"
  val numThreads = 2

  ssc.checkpoint("checkpoint")
  val topicMap = topics.split(",").map((_, numThreads.toInt)).toMap
  val lines = KafkaUtils.createStream(ssc, zkQuorum, group, topicMap).map(_._2)
  val words = lines.flatMap(_.split(" "))
  val wordCounts = words.map(x => (x, 1L))
    .reduceByKeyAndWindow(_ + _, _ - _, Minutes(10), Seconds(2), 2)
  wordCounts.print()

  ssc.start()
  ssc.awaitTermination()

}
