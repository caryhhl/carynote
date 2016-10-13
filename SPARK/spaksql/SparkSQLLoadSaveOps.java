package com.dt.spark.SparkApps.sql;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class SparkSQLLoadSaveOps {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setMaster("local").setAppName("RDD2DataFrameByProgrammatically");
		JavaSparkContext sc = new JavaSparkContext(conf);
		SQLContext sqlContext = new SQLContext(sc);
		
		DataFrame peopleDF = sqlContext.read().format("json").load("D:\\Big_Data_Software"
				+ "\\spark-1.6.0-bin-hadoop2.6\\examples\\src\\main\\resources\\people.json");
		
		peopleDF.select("name").write().mode(SaveMode.Append).save("D:\\Big_Data_Software\\spark-1.6.0-bin-hadoop2.6\\examples\\src\\main\\resources\\usersNames");
		
		
	}
}
