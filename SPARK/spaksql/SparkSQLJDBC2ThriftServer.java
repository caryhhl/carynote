package com.dt.spark.SparkApps.sql;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
/**
 * 实战演示Java通过JDBC访问Thrift Server，进而访问Spark SQL，进而访问Hive，这是企业级开发中最为常见的方式；
 * @author DT大数据梦工厂
 * 新浪微博：http://weibo.com/ilovepains/
 * Created by hp on 2016/4/1.
 *	 
 *	
 */
public class SparkSQLJDBC2ThriftServer {
	public static void main(String[] args) {
		String sql = "select name from people where age = ?";
		Connection conn = null;
		ResultSet resultSet = null;
		try {
			Class.forName("org.apache.hive.jdbc.HiveDriver");
			conn = DriverManager.getConnection("jdbc:hive2://Master:10001/hive?"
					+ "hive.server2.transport.mode=http;hive.server2.thrift.http.path=cliservice",
					"root", "");
			
			PreparedStatement preparedStatement = conn.prepareStatement(sql);
			preparedStatement.setInt(1,30);
			
			resultSet = preparedStatement.executeQuery();
			
			while(resultSet.next()){
				System.out.println(resultSet.getString(1));//此处的数据应该保存在Parquet中等
			}
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			try {
				resultSet.close();
				conn.close();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
}
