package hbase

import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.hadoop.hbase.client._
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.JavaConversions._

/**
  * Created by lzz on 6/28/16.
  * 相关例子 https://wiki.apache.org/hadoop/Hbase/Scala
  */
object Hbase_CRUD extends App{
  val sparkConf = new SparkConf().setMaster("local")
    .setAppName("My App")
    .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
  val sc = new SparkContext(sparkConf)

  var conf = HBaseConfiguration.create()
  conf.set("hbase.master", "hadoop006:16010" )
  conf.addResource( "/home/lzz/work/idea_work/spark_work/spark_recommend/src/main/resources/hbase-site.xml" )
  //Connection 的创建链接
  val conn = ConnectionFactory.createConnection(conf)
  //从Connection获得 Admin
  val admin = conn.getAdmin

  //创建 test 表
  val testTable = TableName.valueOf("test")
  val tableDescr = new HTableDescriptor(testTable)
  tableDescr.addFamily(new HColumnDescriptor("cf".getBytes))
  println("Creating table `test`. ")
  if (admin.tableExists(testTable)) {
    admin.disableTable(testTable)
    admin.deleteTable(testTable)
  }
  admin.createTable(tableDescr)
  println("Done!")
  
  try{
    //获取 test 表
    val table = conn.getTable(testTable)
    try{
      //准备插入一条 key 为 rk001 的数据
      val p = new Put("rk001".getBytes)
      //为put操作指定 column 和 value 
      p.addColumn("cf".getBytes,"name".getBytes, "linzhouzhi".getBytes)
      p.addColumn("cf".getBytes,"password".getBytes, "111111".getBytes)
      //提交
      table.put(p)

      //查询某条数据
      val g = new Get("rk001".getBytes)
      val result = table.get(g)
      val value = Bytes.toString(result.getValue("cf".getBytes,"name".getBytes))
      println("get rk001: "+value)

      //扫描数据
      val s = new Scan()
      s.addColumn("cf".getBytes,"name".getBytes)
      val scanner = table.getScanner(s)

      try{
        for(r <- scanner){
          println("found row: "+r)
          println("found value: "+Bytes.toString(r.getValue("cf".getBytes,"name".getBytes)))
        }
      }finally {
        //关闭scanner
        scanner.close()
      }

      //删除
      val d = new Delete("rk001".getBytes)
      d.addColumn("cf".getBytes,"name".getBytes)
      table.delete(d)

    }finally {
      if(table != null) table.close()
    }

  }finally {
    //关闭链接
    conn.close()
  }

}
