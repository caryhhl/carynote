package com.dt.spark.sql

import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{SparkContext, SparkConf}

/**
  * 使用Scala开发集群运行的Spark WordCount程序
  * @author DT大数据梦工厂
  * 新浪微博：http://weibo.com/ilovepains/
  * Created by hp on 2016/3/31.
  * 通过案例实战Spark SQL下的UDF和UDAF的具体使用：
  * UDF: User Defined Function，用户自定义的函数，函数的输入是一条具体的数据记录，实现上讲就是普通的Scala函数；
  * UDAF：User Defined Aggregation Function，用户自定义的聚合函数，函数本身作用于数据集合，能够在聚合操作的基础上进行自定义操作；
  *
  * 实质上讲，例如说UDF会被Spark SQL中的Catalyst封装成为Expression，最终会通过eval方法来计算输入的数据Row（此处的Row和DataFrame
  * 中的Row没有任何关系）
  *
  */
object SparkSQLUDFUDAF {

  def main (args: Array[String]) {
    /**
      * 第1步：创建Spark的配置对象SparkConf，设置Spark程序的运行时的配置信息，
      * 例如说通过setMaster来设置程序要链接的Spark集群的Master的URL,如果设置
      * 为local，则代表Spark程序在本地运行，特别适合于机器配置条件非常差（例如
      * 只有1G的内存）的初学者       *
      */
    val conf = new SparkConf() //创建SparkConf对象
    conf.setAppName("SparkSQLUDFUDAF") //设置应用程序的名称，在程序运行的监控界面可以看到名称
    //    conf.setMaster("spark://Master:7077") //此时，程序在Spark集群
    conf.setMaster("local[4]")
    /**
      * 第2步：创建SparkContext对象
      * SparkContext是Spark程序所有功能的唯一入口，无论是采用Scala、Java、Python、R等都必须有一个SparkContext
      * SparkContext核心作用：初始化Spark应用程序运行所需要的核心组件，包括DAGScheduler、TaskScheduler、SchedulerBackend
      * 同时还会负责Spark程序往Master注册程序等
      * SparkContext是整个Spark应用程序中最为至关重要的一个对象
      */
    val sc = new SparkContext(conf) //创建SparkContext对象，通过传入SparkConf实例来定制Spark运行的具体参数和配置信息

    val sqlContext = new SQLContext(sc) //构建SQL上下文

    //模拟实际使用的数据
    val bigData = Array("Spark", "Spark", "Hadoop", "Spark", "Hadoop", "Spark", "Spark", "Hadoop", "Spark", "Hadoop")

    /**
      * 基于提供的数据创建DataFrame
      */
    val bigDataRDD =  sc.parallelize(bigData)
    val bigDataRDDRow = bigDataRDD.map(item => Row(item))
    val structType = StructType(Array(StructField("word", StringType, true)))
    val bigDataDF = sqlContext.createDataFrame(bigDataRDDRow,structType)

    bigDataDF.registerTempTable("bigDataTable") //注册成为临时表

    /**
      * 通过SQLContext注册UDF，在Scala 2.10.x版本UDF函数最多可以接受22个输入参数
      */
    sqlContext.udf.register("computeLength", (input: String) => input.length)

    //直接在SQL语句中使用UDF，就像使用SQL自动的内部函数一样
    sqlContext.sql("select word, computeLength(word) as length from bigDataTable").show

    sqlContext.udf.register("wordCount", new MyUDAF)

    sqlContext.sql("select word,wordCount(word) as count,computeLength(word) as length" +
      " from bigDataTable group by word").show()


    while(true)()



  }

}

/**
  * 按照模板实现UDAF
  */
class  MyUDAF extends UserDefinedAggregateFunction {
  /**
    * 该方法指定具体输入数据的类型
    * @return
    */
  override def inputSchema: StructType = StructType(Array(StructField("input", StringType, true)))

  /**
    * 在进行聚合操作的时候所要处理的数据的结果的类型
    * @return
    */
  override def bufferSchema: StructType = StructType(Array(StructField("count", IntegerType, true)))

  /**
    * 指定UDAF函数计算后返回的结果类型
    * @return
    */
  override def dataType: DataType = IntegerType

  override def deterministic: Boolean = true

  /**
    * 在Aggregate之前每组数据的初始化结果
    * @param buffer
    */
  override def initialize(buffer: MutableAggregationBuffer): Unit = {buffer(0) =0}

  /**
    * 在进行聚合的时候，每当有新的值进来，对分组后的聚合如何进行计算
    * 本地的聚合操作，相当于Hadoop MapReduce模型中的Combiner
    * @param buffer
    * @param input
    */
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    buffer(0) = buffer.getAs[Int](0) + 1
  }

  /**
    * 最后在分布式节点进行Local Reduce完成后需要进行全局级别的Merge操作
    * @param buffer1
    * @param buffer2
    */
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1(0) = buffer1.getAs[Int](0) + buffer2.getAs[Int](0)
  }


  /**
    * 返回UDAF最后的计算结果
    * @param buffer
    * @return
    */
  override def evaluate(buffer: Row): Any = buffer.getAs[Int](0)

}
