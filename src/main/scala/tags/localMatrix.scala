package tags
import matrix.TRowMatrix
import org.apache.spark.mllib.linalg.{Vectors, _}
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by lzz on 6/28/16.
  */
object localMatrix extends App{
  val sparkConf = new SparkConf().setMaster("local")
    .setAppName("My App")
    .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
  val sc = new SparkContext(sparkConf)

  //标签数据每个元素对应的是标签中的id
  val tags = Array(1, 2, 3, 5, 6, 8, 9, 11, 12, 13, 14, 17)
  //用户id 数据
  val users = Array(2, 3, 4, 5, 6, 8, 9, 12, 13, 14, 15, 16, 18, 19, 22, 24, 25, 26, 29)
  //广告id数据
  val ads = Array( 3, 5, 6, 7, 8 )
  // （广告id,标签id,权重）
  val ad_tags = Seq(
    Vector( 3, 1,1),
    Vector( 5, 2,1),
    Vector( 6, 3,1),
    Vector( 7, 5,1),
    Vector( 8, 6,1),
    Vector( 6, 8,1),
    Vector( 8, 9,1),
    Vector( 3, 5,1),
    Vector( 3, 5, 1),
    Vector( 5, 6, 1),
    Vector( 6, 8, 1),
    Vector( 7, 9, 1),
    Vector( 8, 11,1),
    Vector( 6, 12,1),
    Vector( 8, 13,1)
  )
  // （用户id, 标签id, 投放次数 ）
  val user_tags = Seq(
    Vector( 2,1,1),
    Vector( 2,2,1),
    Vector( 3,3,1),
    Vector( 4,5,1),
    Vector( 5,6,1),
    Vector( 6,8,1),
    Vector( 8,9,1),
    Vector( 9,5,1),
    Vector( 12,5, 1),
    Vector( 12,6, 1),
    Vector( 13,8, 1),
    Vector( 14,9, 1),
    Vector( 15,11,1),
    Vector( 16,12,1),
    Vector( 18,13,1),
    Vector( 19,14,1),
    Vector( 22,17,1),
    Vector( 24,1, 1),
    Vector( 25,2, 1),
    Vector( 26,3, 1),
    Vector( 22,5, 1),
    Vector( 24,6, 1),
    Vector( 6, 8, 1),
    Vector( 8, 9, 1),
    Vector( 9, 11,1),
    Vector( 12,12,1),
    Vector( 13,13,1),
    Vector( 14,14,1),
    Vector( 15,17,1),
    Vector( 16,1,1),
    Vector( 18,1,1),
    Vector( 19,1,1),
    Vector( 3,1,1),
    Vector( 4,3,1),
    Vector( 5,6,1),
    Vector( 6,5,1),
    Vector( 12,1, 1),
    Vector( 13,2, 1),
    Vector( 14,3, 1),
    Vector( 15,5, 1),
    Vector( 16,6, 1),
    Vector( 18,8, 1),
    Vector( 19,9, 1),
    Vector( 22,11,1),
    Vector( 24,12,1),
    Vector( 25,13,3)
  )

  //创建一个用户和标签的空矩阵
  var ut_matrix = Array.ofDim[Double](users.size, tags.size)
  // 将user_tags 中的数据添加到矩阵中
  for( user_tag <- user_tags ){
    val user_i = users.indexOf( user_tag(0) )
    val tag_i = tags.indexOf( user_tag(1) )
    ut_matrix(user_i)(tag_i) = user_tag(2)
  }

  //把二维数组转化为，类型为vector的一维数组
  var userSeq = new Array[ Vector ]( users.size )
  for( i <- 0 to (users.size - 1) ){
    val ut_vector: Vector = Vectors.dense( ut_matrix( i ) )
    userSeq( i ) = ut_vector
  }
  //转化为行矩阵
  val userM = new RowMatrix( sc.parallelize( userSeq.toSeq )  )
  println( "用户-标签矩阵××××××××××××" )
  userM.rows.foreach( println )

  //创建标签-广告矩阵
  var at_matrix = Array.ofDim[Double]( tags.size, ads.size )
  for( ad_tag <- ad_tags ){
    val ad_i = ads.indexOf( ad_tag(0) )
    val tag_i = tags.indexOf( ad_tag(1) )
    at_matrix(tag_i)(ad_i) = ad_tag(2)
  }

  //按顺序将 标签-广告 矩阵中的数据写入到 adSeq 中用于建立稠密矩阵
  var adSeq = new Array[ Double ]( tags.size * ads.size )
  for( i <- 0 to (tags.size - 1) ){
    for( item <- 0 to ( ads.size - 1) ){
      adSeq( i*ads.size + item ) = at_matrix(i)(item)
    }
  }
  //建立稠密矩阵
  val adM: DenseMatrix = new DenseMatrix(tags.size, ads.size, adSeq )
  println( "标签-广告矩阵××××××××××××××××××" )
  adM.colIter.foreach( println )

  //矩阵相乘得到相识矩阵
  val rM = TRowMatrix.transposeRowMatrix( userM multiply( adM ) )
  println( "相识矩阵×××××××××××××××" )
  rM.rows.foreach( println )

}
