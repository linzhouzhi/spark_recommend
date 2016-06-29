package tags

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{Result, Scan}
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.util.{Base64, Bytes}
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.mllib.linalg.{Matrices, Matrix, Vector, Vectors}
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
  conf.addResource( "main/resources/hbase-site.xml" )
  conf.set(TableInputFormat.INPUT_TABLE, "user_tags")


  //标签库，可以改成数据库中直接读取
  val tags = Array( "tag16", "tag18", "tag9", "tag15", "tag19", "tag6", "tag13", "tag7", "tag23", "tag11", "tag24", "tag27", "tag4", "tag8", "tag12", "tag3" )
  var scan = new Scan();
  scan.addFamily(Bytes.toBytes("cf"))
  var proto = ProtobufUtil.toScan(scan);
  var ScanToString = Base64.encodeBytes(proto.toByteArray());
  conf.set(TableInputFormat.SCAN, ScanToString);
  //从数据库中读取（用户-标签）表的数据
  val usersRDD = sc.newAPIHadoopRDD( conf, classOf[TableInputFormat],
    classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
    classOf[org.apache.hadoop.hbase.client.Result])

  //用户-标签
  val user_tags = usersRDD.map( x => x._2 )
    .map( result => ( result.getRow, result.getValue( Bytes.toBytes("cf"),Bytes.toBytes("total")) ))
    .map( row => ( new String(row._1), new String ( row._2 ) ) )
    .map( row =>( row._1.toString.split( '+' )(0),row._1.toString.split( '+' )(1) + ":" + row._2 ) )
    .groupByKey
    .map( row => ( row._1, row._2.toList ))
    .sortBy( row => row._1)


  //每一行转化为 Local dense vector  用于生成矩阵
  val rows: RDD[Vector] = user_tags
    .map( row => tag_vector( row._2 ) )
  //生成分布式 rowMatrix 矩阵
  val mat: RowMatrix = new RowMatrix( rows )
  //某个广告的标签，其中顺序跟 tags的顺序是一样的
  val ad_tags = Array(0.0,0.0,0.0,0.0,0.0,0.0,8.0,9.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0)
  //转化为矩阵用于和用户标签矩阵进行相乘
  val dm: Matrix = Matrices.dense( tags.size, 1, ad_tags )
  //相乘得到广告和用户的相识度矩阵（这边不应该这样直接相乘应该要优化）
  val ad_recommoned = mat.multiply( dm ).rows.map( x => ( x(0) ))
  //获取用户rdd
  val user = user_tags.map( x => x._1 )
  //获取topN用户，这些用户就是该广告要投放的
  ad_recommoned.zip( user ).sortBy( x => x._1 ).top( 3 ).foreach( println )

  def tag_vector( list: List[String] ): Vector ={
    var row_tag = new Array[ Double ]( tags.size )
    for( l <- list ){
      val tag_map = l.split( ":" )
      val tag_i = tags.indexOf( tag_map( 0 ) )
      row_tag(tag_i) = tag_map( 1 ).toDouble
    }
    Vectors.dense( row_tag )
  }

}

