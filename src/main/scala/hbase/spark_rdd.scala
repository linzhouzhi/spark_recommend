package hbase

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Scan
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.util.{Base64, Bytes}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by lzz on 6/28/16.
  */
object spark_rdd extends App{
  val sparkConf = new SparkConf().setMaster("local")
    .setAppName("My App")
    .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
  val sc = new SparkContext(sparkConf)

  var hConf = HBaseConfiguration.create()
  hConf.set("hbase.zookeeper.property.clientPort", "2181" )
  hConf.set("hbase.zookeeper.quorum", "192.168.1.221,192.168.1.222,192.168.1.223" )
  hConf.set("hbase.master", "hadoop006:16010" )
  hConf.set(TableInputFormat.INPUT_TABLE, "user_tags")


  var scan = new Scan();
  scan.addFamily(Bytes.toBytes("cf"))
  var proto = ProtobufUtil.toScan(scan);
  var ScanToString = Base64.encodeBytes(proto.toByteArray());
  hConf.set(TableInputFormat.SCAN, ScanToString);

  val usersRDD = sc.newAPIHadoopRDD( hConf, classOf[TableInputFormat],
    classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
    classOf[org.apache.hadoop.hbase.client.Result])

  usersRDD.map( x => x._2 )
    .map( result => ( result.getRow,  List( result.getValue( Bytes.toBytes("cf"),Bytes.toBytes("h1")),result.getValue( Bytes.toBytes("cf"),Bytes.toBytes("name") ) ) ))
    .map( row => ( new String(row._1), row._2 ) )
    .foreach(
      r => ( println( r._1 + "----" + r._2 ) )
    )
}
