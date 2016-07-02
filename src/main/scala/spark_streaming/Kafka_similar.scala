package spark_streaming

import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{ConnectionFactory, Put}
import org.apache.spark.SparkConf
import org.apache.spark.mllib.linalg._
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Minutes, Seconds, StreamingContext}

import scala.collection.immutable.IndexedSeq

/**
  * Created by lzz on 6/28/16.
  */
object Kafka_similar extends App{

  var conf = HBaseConfiguration.create()
  conf.set("hbase.master", "hadoop006:16010" )
  conf.addResource( "/home/lzz/work/idea_work/spark_work/spark_recommend/src/main/resources/hbase-site.xml" )
  //Connection 的创建链接
  val conn = ConnectionFactory.createConnection(conf)
  val testTable = TableName.valueOf("test")
  val table = conn.getTable(testTable)

  val ad_tags: Matrix = Matrices.dense(3, 2, Array(1.0, 3.0, 5.0, 2.0, 4.0, 6.0))
  //val dm_sparse: Matrix = Matrices.sparse( 3,2, Array(0, 1,6), Array(0, 0, 1,1,2,2), Array(3, 4, 1,2,4,5))
  val user_arr = Array( 1.0, 3.0 )
  val userTags: Vector = Vectors.dense( user_arr )
  ad_tags.multiply( userTags ).toArray.foreach( println )

  val sparkConf = new SparkConf().setMaster("local")
    .setAppName("My App")
  val ssc = new StreamingContext(sparkConf, Seconds(5))

  //val Array(zkQuorum, group, topics, numThreads) = args
  val zkQuorum = "192.168.1.221:2181,192.168.1.222:2181,192.168.1.223:2181"
  val group = "group001"
  val topics = "my-replicated-topic"
  val numThreads = 2

  ssc.checkpoint("checkpoint")
  val topicMap = topics.split(",").map((_, numThreads.toInt)).toMap
  val lines = KafkaUtils.createStream(ssc, zkQuorum, group, topicMap).map(_._2)
  val user_tags = lines.map(_.split('|'))
  val ad_user = user_tags.map(x => (x(0), format_tag( x )))
      .map( x => (sim_write( x._1, x._2) ) )
    .print()

  //  .reduceByKeyAndWindow(_ + _, _ - _, Minutes(10), Seconds(2), 2)
 // wordCounts.print()

  ssc.start()
  ssc.awaitTermination()

  def format_tag( x: Array[String] ): DenseVector ={
    val tag_arr = Array( x(1).toDouble, x(2).toDouble   )
    val tag_vector = Vectors.dense( tag_arr )

    //ad_tags.multiply( tag_vector ).toJson.zip( Array("ad1","ad2","ad3") )
    ad_tags.multiply( tag_vector )
  }

  def sim_write( key: String, value: Vector ): Unit ={
    val ads = Array( "ad1","ad2","ad3" )
    val ads_size = ads.size

    for( i <- 0 to ads_size ){
      //准备插入一条 key 为 rk001 的数据
      val rowkey = key + ads(i)
      val p = new Put( rowkey.getBytes )
      //为put操作指定 column 和 value
      p.addColumn("cf".getBytes, "value".getBytes(), value(i).toString.getBytes )
      table.put(p)
      val tmp = key + "-" + ads(i) + ":" + value(i)
      println( tmp )
    }
  }
}
