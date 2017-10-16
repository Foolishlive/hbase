package com.mpinfo.hadoop.utils;

import java.sql.*;
import org.apache.log4j.Logger;
import com.mpinfo.hadoop.hbase.HBaseOperator;

/**
 * The HBaseManipulateHandler program implements a application that handle and process command to manipulate data
 * in HBase Table. It support many operation method, such insert, delete and update data to database.
 * It also support to connect to database directly through JDBC Driver, it support to create hbase table with 
 * column family and cell that reference from source table metadata or assigned mapping file. 
 * It can retrieve rowid from oracle table.
 * It support to import Oracle, Sybase, MS SQL Server and MySQL database data into hadoop hbase.
 * It required following arguments:
 * 		-DBUrl:						JDBC Connection String		  
 * 		-username:					Database login username
 * 		-password:					Database login user's password
 * 		-table:						Import source database's table name
 * 		-hbaseurl:					HBase service port
 * 		-hbasetablename:			Import target HBase's table name
 * 		-hbasetablereplace			Drop exist HBase Table and import data to a new HBase Table  
 * 		-hbasetablerowkey			Assign which source field as the target HBase's table row key
 * 		-columnfamilyfile			Define the source database's field map to target HBase's column family and qualifier
 * 		-hbasetableappend			Append importing data into exist HBase Table
 * 		-hbasesinglecolumnfamily	Specific single column family in HBase Table
 * 		-operation					Operation manipulate HBase Table
 * Copyright(C) 2000 - 2016 MPower Information Co., LTD. All Rights Reserved.
 * @author Edward Chen
 *
 */
public class HBaseManipulateHandler {

	private static Logger logger = Logger.getLogger(ImportHandler.class.getName());
	private String DBUrl = null;						//Relational Database JDBC URL Connection String
	private String userName = null;						//Relational Database login user name
	private String password = null;						//Relational Database login user password
	private String tableName = null;					//Import source relational database's table name
	private String hbaseUrl = null;						//Import target HBase zookeeper url
	private String hbaseTableName = null;				//Import target HBase table name
	private boolean hbaseTableReplace = false;			//The flag control import data to overwrite exist hbase table, it will drop exist table and create the same table again 
	private boolean hbaseTableAppend = false;			//The flag control import data to append to exist hbase table
	private String hbaseTableRowKey = null;				//Assign database field name as Hbase Table Row Key
	private HBaseOperator hbaseOperator = null;			//Hbase manipulation program execute insert, delete and update data to hbase table 
	private String operation = null;					//Operation manipulate HBase Table
	private int totalRecordCount = 0, 
			    deleteFailureCount = 0;					//Source table total record count, this variable is use for showing import progress
	private String hbaseSingleColumnFamily = null;      //Only one column family in Hbase Table
	private String columnFamilyByFile = null;	
	
	public String getColumnFamilyByFile() {
		return columnFamilyByFile;
	}

	public void setColumnFamilyByFile(String columnFamilyByFile) {
		this.columnFamilyByFile = columnFamilyByFile;
	}

	public String getHBaseUrl() {
		return hbaseUrl;
	}

	public void setHBaseUrl(String hbaseUrl) {
		this.hbaseUrl = hbaseUrl;
	}

	public String getHBaseTableName() {
		return hbaseTableName;
	}

	public void setHBaseTableName(String hbaseTableName) {
		this.hbaseTableName = hbaseTableName;
	}

	public String getHBaseTableRowKey() {
		return hbaseTableRowKey;
	}

	public void setHBaseTableRowKey(String hbaseTableRowKey) {
		this.hbaseTableRowKey = hbaseTableRowKey;
	}
	
	public String getOperation() {
		return operation;
	}

	public void setOperation(String operation) {
		this.operation = operation;
	}
	
	/**
	 * Show manipulatee arguments
	 */
	public void Helper(){
		System.out.println("Here is help for this HBase Manipulate tool:");
		System.out.println("You must provide following arguments:");
		System.out.println("\t-operation :\t\t which operation you want to manipulate hbase table");
		System.out.println("\t\tdelete :\t\t delete one hbase row");
		System.out.println("\t\tinsert :\t\t insert one hbase row");
		System.out.println("\t\tupdate :\t\t update one hbase row");
		System.out.println("\t\timport :\t\t import one record into hbase");
		System.out.println("\t\timportImageFile : \t import Image File as a hbase qualifier");
		System.out.println("\t-hbaseurl :\t\t target hbase connection url ");
		System.out.println("\t-hbasetablename :\t target hbase table name ");
		System.out.println("\t-hbasetablerowkey :\t target hbase table's row key, it also can be define mapping file ");
	}
	
	/**
	 * execute manipulation hbase task 
	 */
	public void execute(){
		
		if(operation.toUpperCase().equals("DELETE") && DBUrl == null){
			deleteRowKey();
		}else if(operation.toUpperCase().equals("DELETE") && DBUrl != null){
			deleteRowFromDBList();
		}
	}
	
	public void deleteRowFromDBList(){
		
		Statement 			stmt		= null;
		ResultSet 			rs 			= null, 
				  			rsCount		= null;
		PreparedStatement 	pstmt 		= null;	
		Connection 			con 		= null;
		ResultSetMetaData 	rsMetaData 	= null;				//Retrieve Table Meta data
		float 				progressCount = 0;				//Execution progress percentage
		int 				progressNumber = 0; 			//Execution progress number
		long 				start_time, 
							end_time;						//start_time and end_time variable are use for execution time
		
		
		start_time 			= System.currentTimeMillis();		//Importing start time
		hbaseOperator 		= new HBaseOperator(this.hbaseUrl);	//Initial hbase manipulation program
		
		try {
			// Check database type through jdbc connection string
			if(this.DBUrl.indexOf("oracle") > 0){
				DriverManager.registerDriver(new oracle.jdbc.driver.OracleDriver());
			}else if(this.DBUrl.indexOf("sybase") > 0){
				DriverManager.registerDriver(new com.sybase.jdbc4.jdbc.SybDriver());
			}else if(this.DBUrl.indexOf("mysql") > 0){
				DriverManager.registerDriver(new  com.mysql.jdbc.Driver());
			}else if(this.DBUrl.indexOf("sqlserver") > 0){
				DriverManager.registerDriver(new com.microsoft.sqlserver.jdbc.SQLServerDriver());
			}
			
			// Create database connection
			con = DriverManager.getConnection(this.DBUrl,this.userName,this.password);
			
			//Retrieve source Database table metadata
			if(this.DBUrl.indexOf("oracle") > 0 && this.hbaseTableRowKey.indexOf("rowid") >=0){
				//The database is oracle with source table's rowid
				pstmt = con.prepareStatement("Select rowid, t.* from " + this.tableName + " t ");
				rsMetaData = pstmt.getMetaData();
			} else {
				//The database is oracle without source table's rowid and source database is non-oracle 
				pstmt = con.prepareStatement("Select * from " + this.tableName );
				rsMetaData = pstmt.getMetaData();
			}
			
			//Check RowKey is correct field
			boolean rowKeyIsExist = false;
			for(int colIdx=0;colIdx<rsMetaData.getColumnCount();colIdx++){
				if(this.hbaseTableRowKey.toUpperCase().equals(rsMetaData.getColumnName(colIdx+1).toUpperCase())){
					rowKeyIsExist = true;
					break;
				}
			}
			
			if(!rowKeyIsExist){
				System.out.println("Please confirm row key is existing field in source table!!");
				logger.error("Please confirm row key is existing field in source table!!");
				System.exit(-1);
			}
			
			if(!hbaseOperator.isTableExist(this.hbaseTableName)){
				System.out.println("HBase table " + this.hbaseTableName + 
						" doesn't exist, please create target HBase table first before using this tool!!");
				logger.info("HBase table " + this.hbaseTableName + 
						" doesn't exist, please create target HBase table first before using this tool!!");
				System.exit(-1);
			}
			
			stmt = con.createStatement();
			rsCount = stmt.executeQuery("Select count(*) from " + this.tableName );
			while(rsCount.next()){
				totalRecordCount = rsCount.getInt(1);
			}
			stmt.close();
			
			pstmt.setFetchSize(1000);
			rs = pstmt.executeQuery();
			logger.info("+++++++++++++++++++++++Delete progress starting++++++++++++++++++++++++++");
			logger.info("Start to delete data, plese wait!!");
			System.out.println("Start to delete data, plese wait!!");
			System.out.println("It will delete total " + totalRecordCount + " record from HBase table " + 
					this.hbaseTableName + ".");
			logger.info("It will delete total " + totalRecordCount + " record from HBase table " + 
					this.hbaseTableName + ".");
			System.out.println("####--------------------------------------------------####");
			System.out.print("..0%");
			deleteFailureCount=0;
			while(rs.next()){
				//retrieve source table data and import to target hbase table
				//To retrive rowkey value from source table record
				String hbaseTableRowKeyVal = rs.getString(this.hbaseTableRowKey);

				hbaseOperator.deleteRow(this.hbaseTableName, hbaseTableRowKeyVal);
				
				progressCount++;
				int tempProgressNumber = (int)Math.round((progressCount/totalRecordCount) * 100);
				if(progressNumber != tempProgressNumber && tempProgressNumber % 2 == 0){
					if(tempProgressNumber > 2){
						for(int i=0;i<(tempProgressNumber-progressNumber)/2;i++){
							System.out.print("*");
						}
						progressNumber += tempProgressNumber-progressNumber;
						logger.info("******Current delete record counts is " + progressCount + "*****");
					}else{
						System.out.print("*");
						progressNumber += 2;
					}
				}
				logger.info("Current delete rowkkey is " + hbaseTableRowKeyVal);
			}
			
		}catch(Exception e){
			logger.error("delete data failure!! Cause by :");
			logger.error(e.getMessage());
			System.exit(-1);
		}
		System.out.println("100%");
		end_time = System.currentTimeMillis();
		System.out.println("Finish delete record total " + (totalRecordCount - deleteFailureCount) + 
				" record to HBase, it is " + deleteFailureCount + " records delete failure, it takes : " + (end_time - start_time) + "ms");
		logger.info("Finish importing total " + (totalRecordCount - deleteFailureCount) + 
				" record to HBase, it is " + deleteFailureCount + " records delete failure, it takes : " + (end_time - start_time) + "ms");
		logger.info("+++++++++++++++++++++++Importing progress complete++++++++++++++++++++++++++");
	}
	
	public void deleteRowKey(){
		long start_time, end_time;							//start_time and end_time variable are use for execution time
		start_time = System.currentTimeMillis();			//Importing start time
		hbaseOperator = new HBaseOperator(this.hbaseUrl);	//Initial hbase manipulation program
		
		logger.info("+++++++++++++++++++++++" + this.operation + " progress starting++++++++++++++++++++++++++");
		logger.info("Start to " + this.operation + " data, plese wait!!");
		System.out.println("Start to " + this.operation + " data, plese wait!!");
		
		totalRecordCount = 0;
		deleteFailureCount = 0;
		
		System.out.println("It will delete 1 record in HBase table " + this.hbaseTableName + ".");
		logger.info("It will delete 1 record in HBase table " + this.hbaseTableName + ".");
		try{
			hbaseOperator.deleteRow(getHBaseTableName(), getHBaseTableRowKey());
			logger.info("delete row key " + getHBaseTableRowKey() + " in hbase table "
					+ getHBaseTableName() + ".");
			System.out.println("delete row key " + getHBaseTableRowKey() + " in hbase table "
					+ getHBaseTableName() + ".");
			totalRecordCount++;
		}catch(Exception e){
			logger.error("delete row key " + getHBaseTableRowKey() + " in hbase table " 
					+ getHBaseTableName() + "Failure!!");
			System.out.println("delete row key " + getHBaseTableRowKey() + " in hbase table " 
					+ getHBaseTableName() + "Failure!!");
			logger.error(e.getMessage());
			deleteFailureCount++;
		}
		
		end_time = System.currentTimeMillis();
		System.out.println("Finish to " + this.operation + " total " + (totalRecordCount - deleteFailureCount) + 
				" record from HBase table" + this.tableName + ", it is " + deleteFailureCount + " records " + this.operation + 
				" failure, it takes : " + (end_time - start_time) + "ms");
		logger.info("Finish to " + this.operation + " total " + (totalRecordCount - deleteFailureCount) + 
				" record from HBase table" + this.tableName + ", it is " + deleteFailureCount + " records " + this.operation + 
				" failure, it takes : " + (end_time - start_time) + "ms");
		logger.info("+++++++++++++++++++++++" + this.operation + " progress complete++++++++++++++++++++++++++");
	}

	public int getTotalRecordCount() {
		return totalRecordCount;
	}

	public void setTotalRecordCount(int totalRecordCount) {
		this.totalRecordCount = totalRecordCount;
	}

	public int getDeleteFailureCount() {
		return deleteFailureCount;
	}

	public void setDeleteFailureCount(int deleteFailureCount) {
		this.deleteFailureCount = deleteFailureCount;
	}

	public static void main(String[] args){
		
		HBaseManipulateHandler hbaseManipulateHandler = new HBaseManipulateHandler();

		ImportHandler importHandler = new ImportHandler();
		ImportImageFileHandler importImageFileHandler = new ImportImageFileHandler();
		
		if(args.length == 0){
			hbaseManipulateHandler.Helper();
			System.exit(0);
		}
		
		for(int loc = 0;loc < args.length;loc++){
			if(args[loc].equals("-dburl")){
				hbaseManipulateHandler.setDBUrl(args[++loc]);
			}else if(args[loc].equals("-username")){
				hbaseManipulateHandler.setUserName(args[++loc]);
			}else if(args[loc].equals("-password")){
				hbaseManipulateHandler.setPassword(args[++loc]);
			}else if(args[loc].equals("-table")){
				hbaseManipulateHandler.setTableName(args[++loc]);
			}else if(args[loc].equals("-hbaseurl")){
				hbaseManipulateHandler.setHBaseUrl(args[++loc]);
			}else if(args[loc].equals("-hbasetablename")){
				hbaseManipulateHandler.setHBaseTableName(args[++loc]);
			}else if(args[loc].equals("-hbasetablereplace")){
				hbaseManipulateHandler.setHbaseTableReplace(true);
			}else if(args[loc].equals("-hbasetablerowkey")){
				hbaseManipulateHandler.setHBaseTableRowKey(args[++loc]);
			}else if(args[loc].equals("-columnfamilymappingfile")){
				hbaseManipulateHandler.setColumnFamilyByFile(args[++loc]);
			}else if(args[loc].equals("-hbasetableappend")){
				hbaseManipulateHandler.setHbaseTableAppend(true);
			}else if(args[loc].equals("-hbasesinglecolumnfamily")){
				hbaseManipulateHandler.setHbaseSingleColumnFamily(args[++loc]);
			}else if(args[loc].equals("-operation")){
				hbaseManipulateHandler.setOperation(args[++loc]);
			}else if(args[loc].equals("-dburl")){
				hbaseManipulateHandler.setDBUrl(args[++loc]);
			}
		}
		
		int processCount = 0;
		if(hbaseManipulateHandler.getOperation().toUpperCase().equals("IMPORT")){
			importHandler.execute(args);
			processCount = importHandler.getTotalRecordCount() - importHandler.getImportingFailureCount();
		}else if(hbaseManipulateHandler.getOperation().toUpperCase().equals("IMPORTIMAGEFILE")){
			importImageFileHandler.execute(args);
			processCount = importImageFileHandler.getTotalRecordCount() - importImageFileHandler.getImportingFailureCount();
		}else{
			hbaseManipulateHandler.execute();
			processCount = hbaseManipulateHandler.getTotalRecordCount() - hbaseManipulateHandler.getDeleteFailureCount();
		}
		System.exit(processCount);
	}

	public String getHbaseSingleColumnFamily() {
		return hbaseSingleColumnFamily;
	}

	public void setHbaseSingleColumnFamily(String hbaseSingleColumnFamily) {
		this.hbaseSingleColumnFamily = hbaseSingleColumnFamily;
	}

	public String getDBUrl() {
		return DBUrl;
	}

	public void setDBUrl(String dBUrl) {
		DBUrl = dBUrl;
	}

	public String getUserName() {
		return userName;
	}

	public void setUserName(String userName) {
		this.userName = userName;
	}

	public String getPassword() {
		return password;
	}

	public void setPassword(String password) {
		this.password = password;
	}

	public String getTableName() {
		return tableName;
	}

	public void setTableName(String tableName) {
		this.tableName = tableName;
	}

	public boolean isHbaseTableReplace() {
		return hbaseTableReplace;
	}

	public void setHbaseTableReplace(boolean hbaseTableReplace) {
		this.hbaseTableReplace = hbaseTableReplace;
	}

	public boolean isHbaseTableAppend() {
		return hbaseTableAppend;
	}

	public void setHbaseTableAppend(boolean hbaseTableAppend) {
		this.hbaseTableAppend = hbaseTableAppend;
	}
}
