package com.dt.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext



object SparkSQL2Hive {
  def main(args: Array[String]): Unit = {
      val conf = new SparkConf() //创建SparkConf对象
      conf.setAppName("SparkSQL2Hive") //设置应用第69课：Spark SQL通过Hive数据源实战程序的名称，在程序运行的监控界面可以看到名称
      conf.setMaster("spark://Master:7077") //此时，程序在Spark集群
      
      val sc = new SparkContext(conf) //创建SparkContext对象，通过传入SparkConf实例来定制Spark运行的具体参数和配置信息
      
      /**
       * 第一：在目前企业级大数据Spark开发的时候绝大多数情况下是采用Hive作为数据仓库的；
       * Spark提供了Hive的支持功能，Spark通过HiveContext可以直接操作Hive中的数据；
       * 基于HiveContext我们可以使用sql/hql两种方式才编写SQL语句对Hive进行操作，包括
       * 创建表、删除表、往表里导入数据以及用SQL语法构造各种SQL语句对表中的数据进行CRUD操作
       * 第二：我们也可以直接通过saveAsTable的方式把DataFrame中的数据保存到Hive数据仓库中；
       * 第三：可以直接通过HiveContext.table方法来直接加载Hive中的表而生成DataFrame 
       */
      val hiveContext = new HiveContext(sc)	
      hiveContext.sql("use hive")  //使用Hive数据仓库中的hive数据库
      hiveContext.sql("DROP TABLE IF EXISTS people")  //删除同名的Table
      hiveContext.sql("CREATE TABLE IF NOT EXISTS people(name STRING, age INT) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\t' LINES TERMINATED BY '\\n'" )  //创建自定义的Table
      /**
       * 把本地数据加载到Hive数据仓库中（背后实际上发生了数据的拷贝）
       * 当然也可以通过LOAD DATA INPATH去获得HDFS等上面的数据到Hive（此时发生了数据的移动）
       */
      hiveContext.sql("LOAD DATA LOCAL INPATH '/root/Documents/SparkApps/resources/people.txt' INTO TABLE people")
      
      hiveContext.sql("DROP TABLE IF EXISTS peoplescores")
      hiveContext.sql("CREATE TABLE IF NOT EXISTS peoplescores(name STRING, score INT) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\t' LINES TERMINATED BY '\\n'")
      hiveContext.sql("LOAD DATA LOCAL INPATH '/root/Documents/SparkApps/resources/peoplescores.txt' INTO TABLE peoplescores")
      
      /**
       * 通过HiveContext使用join直接基于Hive中的两张表进行操作获得大于90分的人的name、age、score
       */
      val resultDF = hiveContext.sql("SELECT  pi.name, pi.age, ps.score " 
          + "FROM people pi JOIN peoplescores ps ON pi.name=ps.name WHERE ps.score>90")
          
      /**
       * 通过saveAsTable创建一张Hive Managed Table，数据的元数据和数据即将房的具体的位置都是
       * 由Hive数据仓库进行管理的，当删除该表的时候，数据也会一起被删除（磁盘上的数据不再存在）
       */
      hiveContext.sql("DROP TABLE IF EXISTS peopleinformationresult")
      resultDF.saveAsTable("peopleinformationresult")
      
      /**
       * 使用HiveContext的table方面可以直接去读Hive数据仓库中的Table并生成DataFrame，接下来就可以
       * 进行机器学习、图计算、各种复杂ETL等操作；
       */
      val dataFromHive = hiveContext.table("peopleinformationresult")
      dataFromHive.show()              
      
      
  }
}