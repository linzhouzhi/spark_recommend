# spark_recommend
   * java 部分    
    ..src/main/java/hbase  //hbase 增删改查例子，以后用于标签提取和保存
    
    ..src/main/java/kafka  //kafka produce 和 consumer 用于对接spark_streaming
    
   * scala 部分
    
    ..src/main/scala/hbase                   //hbase crud操作 和 rdd操作
          
    ..src/main/scala/matrix                  //矩阵处理目录
          
    ..src/main/scala/spark_streaming         //spark streaming 实时处理部分
    
    ..src/main/scala/tags                    //标签投放模块
    
    ..src/main/scala/tags/localMatrix.scala  //本地向量和矩阵的使用
                     
    ..src/main/scala/tags/HbaseMatrix.scla   //模拟广告投放模块（分布式矩阵的应用）
    