package com.mpinfo.database.utils;

import java.sql.*;
import org.apache.log4j.Logger;

public class DatabaseConnectionHandler {
	
	private static Logger logger = Logger.getLogger(DatabaseConnectionHandler.class.getName());
	
	public Connection getConnection(String DBUrl, String userName, String password){
		Connection conn = null;
		try {
			// Check database type through jdbc connection string
			if(DBUrl.indexOf("oracle") > 0){
				DriverManager.registerDriver(new oracle.jdbc.driver.OracleDriver());
			}else if(DBUrl.indexOf("sybase") > 0){
				DriverManager.registerDriver(new com.sybase.jdbc4.jdbc.SybDriver());
			}else if(DBUrl.indexOf("mysql") > 0){
				DriverManager.registerDriver(new  com.mysql.jdbc.Driver());
			}else if(DBUrl.indexOf("sqlserver") > 0){
				DriverManager.registerDriver(new com.microsoft.sqlserver.jdbc.SQLServerDriver());
			}
			// Create database connection
			conn = DriverManager.getConnection(DBUrl,userName,password);	
		}catch(Exception e){
			logger.error("Retrive database connection failure!!");
			logger.error(e.getMessage());
			System.exit(0);			
		}
		return conn;
	}
}
