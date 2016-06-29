package tags

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{Result, Scan}
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.util.{Base64, Bytes}
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by lzz on 6/29/16.
  */
object HbaseMatrix extends App{
  val sparkConf = new SparkConf().setMaster("local")
    .setAppName("My App")
    .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
  val sc = new SparkContext(sparkConf)

  var conf = HBaseConfiguration.create()
  conf.set("hbase.master", "hadoop006:16010" )
  conf.addResource( "/home/lzz/work/idea_work/spark_work/spark_recommend/src/main/resources/hbase-site.xml" )
  conf.set(TableInputFormat.INPUT_TABLE, "user_tags")


  var scan = new Scan();
  scan.addFamily(Bytes.toBytes("cf"))
  var proto = ProtobufUtil.toScan(scan);
  var ScanToString = Base64.encodeBytes(proto.toByteArray());
  conf.set(TableInputFormat.SCAN, ScanToString);

  val usersRDD = sc.newAPIHadoopRDD( conf, classOf[TableInputFormat],
    classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
    classOf[org.apache.hadoop.hbase.client.Result])

  val rows: RDD[Vector] = usersRDD.map( x => x._2 )
    .map( result => ( result.getRow, result.getValue( Bytes.toBytes("cf"),Bytes.toBytes("total")) ))
    .map( row => ( new String(row._1), new String ( row._2 ) ) )
    .map( row =>( row._1.toString.split( '+' )(0),row._1.toString.split( '+' )(1) + ":" + row._2 ) )
    .groupByKey
    .map( row => ( row._1, row._2.toList ))
    .sortBy( row => row._1)
    .map( row => tag_vector( row._2 ) )

  val mat: RowMatrix = new RowMatrix( rows )
  mat.rows.foreach( println )

  def tag_vector( list: List[String] ): Vector ={
    val tags = Array( "tag16", "tag18", "tag9", "tag15", "tag19", "tag6", "tag13", "tag7", "tag23", "tag11", "tag24", "tag27", "tag4", "tag8", "tag12", "tag3" )
    var row_tag = new Array[ Double ]( tags.size )
    for( l <- list ){
      val tag_map = l.split( ":" )
      val tag_i = tags.indexOf( tag_map( 0 ) )
      row_tag(tag_i) = tag_map( 1 ).toDouble
    }
    Vectors.dense( row_tag )
  }

}

