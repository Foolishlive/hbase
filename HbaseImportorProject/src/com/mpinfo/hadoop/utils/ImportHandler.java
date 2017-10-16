package com.mpinfo.hadoop.utils;

import java.io.BufferedReader;
import java.io.FileReader;
import java.sql.*;
import java.util.*;
import org.apache.log4j.Logger;
import com.mpinfo.hadoop.hbase.*;
import com.mpinfo.database.utils.DatabaseConnectionHandler;

/**
 * The ImportHandler program implements a application that handle and process command to import data from database into 
 * hadoop hbase. It connect to database directly through JDBC Driver, it support to create hbase table with column family
 * and cell that reference from source table metadata or assigned mapping file. It also support to retrieve rowid from 
 * oracle table.
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
 * 		-hbasesinglecolumnfamily	Specific single column family in HBase Table"
 * Copyright(C) 2000 - 2016 MPower Information Co., LTD. All Rights Reserved.
 * @author Edward Chen
 *
 */
public class ImportHandler {
	
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
	private boolean importByMappingFile = false;		//The flag control source database table field map to hbase table column family
	private int totalRecordCount = 0, 
			    importingFailureCount = 0;				//Source table total record count, this variable is use for showing import progress
	private String hbaseSingleColumnFamily = null;      //Only one column family in Hbase Table
	private ArrayList<ColumnFamilyMapping> columnFamilyMappingList 
			= new ArrayList<ColumnFamilyMapping>();		//The Database table field and hbase table column family mapping list
	private Scanner scan;

	private boolean isImageField(String fieldType){
		boolean result = false;
		if(fieldType.toUpperCase().indexOf("IMAGE") >= 0){
			result = true;
		}else if(fieldType.toUpperCase().indexOf("BLOB") >= 0){
			result = true;
		}else if(fieldType.toUpperCase().indexOf("CLOB") >= 0){
			result = true;
		}
		return result;
	}
	
	/**
	 * Initial importing, it check all arguments are properly given then
	 *  to start importing data from source database to hbase.
	 *  
	 */
	public void execute(String[] args){
		
		int loc;
		
		logger.info("Checking needed arguments!!");
		if(args.length == 0){
			importHelper();
			System.exit(0);
		}
		for(loc = 0;loc < args.length;loc++){
			if(args[loc].equals("-dburl")){
				setDBUrl(args[++loc]);
			}else if(args[loc].equals("-username")){
				setUserName(args[++loc]);
			}else if(args[loc].equals("-password")){
				setPassword(args[++loc]);
			}else if(args[loc].equals("-table")){
				setTableName(args[++loc]);
			}else if(args[loc].equals("-hbaseurl")){
				setHBaseUrl(args[++loc]);
			}else if(args[loc].equals("-hbasetablename")){
				setHBaseTableName(args[++loc]);
			}else if(args[loc].equals("-hbasetablereplace")){
				setHbaseTableReplace(true);
			}else if(args[loc].equals("-hbasetablerowkey")){
				setHBaseTableRowKey(args[++loc]);
			}else if(args[loc].equals("-columnfamilymappingfile")){
				setColumnFamilyByFile(args[++loc]);
			}else if(args[loc].equals("-hbasetableappend")){
				setHbaseTableAppend(true);
			}else if(args[loc].equals("-hbasesinglecolumnfamily")){
				setHbaseSingleColumnFamily(args[++loc]);
			}
		}
		
		if(this.DBUrl == null){
			System.out.println("Please provide source database connection url!!");
			logger.error("Please provide source database connection url!!");
			importHelper();
			System.exit(0);
		}
		if(this.userName == null || this.password == null){
			System.out.println("Please provide source database's username or password  !!");
			logger.error("Please provide source database's username or password  !!");
			importHelper();
			System.exit(0);
		}
		if(this.tableName == null){
			System.out.println("Please provide source database table name !!");
			logger.error("Please provide source database table name !!");
			importHelper();
			System.exit(0);
		}
		if(this.hbaseUrl == null){
			System.out.println("Please provide target HBase connection url!!");
			logger.error("Please provide target HBase connection url!!");
			importHelper();
			System.exit(0);
		}
		if(this.hbaseTableName == null){
			System.out.println("Please provide target HBase Table Name!!");
			logger.error("Please provide target HBase Table Name!!");
			importHelper();
			System.exit(0);
		}
		if(this.hbaseTableRowKey == null && !this.importByMappingFile){
			System.out.println("Please provide target HBase Table Row Key or Column Family Mapping File !!");
			logger.error("Please provide target HBase Table Row Key or Column Family Mapping File !!");
			importHelper();
			System.exit(0);
		}
		if(this.importByMappingFile){
			executeImportByMappingFile();
		}else{
			executeImport();
		}
		
	}
	
	/**
	 * It show this ImportHandler required arguments and description.
	 */
	public void importHelper(){
		System.out.println("Here is help for this importor tool:");
		System.out.println("You must provide following arguments:");
		System.out.println("\t-dburl :\t\t source database jdbc connection url, for example: ");
		System.out.println("\t\t Oracle\t:\t\t jdbc:oracle:thin:@<HOST>:<PORT>:<SID>");
		System.out.println("\t\t MS SQL Server\t:\t jdbc:microsoft:sqlserver://<HOST>:<PORT>[;DatabaseName=<DB>]");
		System.out.println("\t\t Sybase\t:\t\t jdbc:sybase:Tds:<HOST>:<PORT>");
		System.out.println("\t\t MySQL\t:\t\t jdbc:mysql://<HOST>:<PORT>/<DB>");
		System.out.println("\t-username :\t\t source database login user");
		System.out.println("\t-password :\t\t source database login user's password");
		System.out.println("\t-table :\t\t import table name in source database");
		System.out.println("\t-hbaseurl :\t\t target hbase connection url ");
		System.out.println("\t-hbasetablename :\t target hbase table name ");
		System.out.println("\t-hbasetablereplace : \t Replace exist HBase Table and Data");
		System.out.println("\t-hbasetableappend : \t Append data to exist HBase Table");
		System.out.println("\t-hbasetablerowkey :\t target hbase table's row key, it also can be define mapping file ");
		System.out.println("\t-columnfamilyfile :\t database field mapping to hbase column family and qualifier definition");
		System.out.println("\t-hbasetableappend : \t Append importing data into exist HBase Table");
		System.out.println("\t-hbasesinglecolumnfamily : \t Specific single column family in HBase Table");
	}
	
	/**
	 * It refer to mapping file to import data from database into hbase. 
	 */
	public void executeImportByMappingFile(){
		
		logger.info("Import data into hbase via Mapping File");
		Connection con = null;								//HBase connection
		String[] familyNames = null;						//HBase table column family name list
		String createTableAns = "N", 						//Flag control to create target hbase table
				dropTableAns = "N", 						//Flag control to drop target hbase table
				appDataAns = "N";							//Flag control to append data to hbase table
		hbaseOperator = new HBaseOperator(this.hbaseUrl);	//Initial hbase manipulation program
		long start_time, end_time;							//start_time and end_time variable are use for execution time
		float progressCount = 0;							//Execution progress percentage
		int progressNumber = 0;								//Execution progress number
		start_time = System.currentTimeMillis();			//Importing start time
		String hbaseTableRowKeyVal = null;
		
		try {
			// Check database type through jdbc connection string
			/*if(this.DBUrl.indexOf("oracle") > 0){
				DriverManager.registerDriver(new oracle.jdbc.driver.OracleDriver());
			}else if(this.DBUrl.indexOf("sybase") > 0){
				DriverManager.registerDriver(new com.sybase.jdbc4.jdbc.SybDriver());
			}else if(this.DBUrl.indexOf("mysql") > 0){
				DriverManager.registerDriver(new  com.mysql.jdbc.Driver());
			}else if(this.DBUrl.indexOf("sqlserver") > 0){
				DriverManager.registerDriver(new com.microsoft.sqlserver.jdbc.SQLServerDriver());
			}*/
			// Create database connection
			DatabaseConnectionHandler DBConnHandler = new DatabaseConnectionHandler();
			con = DBConnHandler.getConnection(this.DBUrl, this.userName, this.password);
			//con = DriverManager.getConnection(this.DBUrl,this.userName,this.password);
			
			scan = new Scanner(System.in);
			scan.useDelimiter("\n");
			if(hbaseOperator.isTableExist(this.hbaseTableName) && !hbaseTableReplace && !hbaseTableAppend){
				System.out.println("The target HBase Table exist, do you want to drop it first ?(Y/N)");
				dropTableAns = scan.next();
				if(dropTableAns.indexOf("Y") >= 0 || dropTableAns.indexOf("y") >= 0){
					hbaseOperator.dropTable(this.hbaseTableName);
				}else{
					System.out.println("The target HBase Table exist, do you want to append data to target hbase table ?(Y/N)");
					appDataAns = scan.next();
				}
			}else if (!hbaseOperator.isTableExist(this.hbaseTableName) && !hbaseTableReplace && !hbaseTableAppend){
				System.out.println("The target HBase Table doesn't exist, do you want to create it ?(Y/N)");
				createTableAns = scan.next();
			}else{
				if(hbaseOperator.isTableExist(this.hbaseTableName) && !hbaseTableAppend){
					hbaseOperator.dropTable(this.hbaseTableName);
				}else if(!hbaseOperator.isTableExist(this.hbaseTableName) || hbaseTableReplace){
					createTableAns = "Y";
				}
			}

			if(!hbaseOperator.isTableExist(this.hbaseTableName) || 
					(createTableAns.indexOf("Y") >=0 || createTableAns.indexOf("y") >= 0)){
				//Create HBase Table
				String priorCFName = null;
				ArrayList<String> CFList = new ArrayList<String>();
				int columnFamilyCount = 0;
		
				for(int i=0;i<columnFamilyMappingList.size();i++){
					if(priorCFName == null){
						columnFamilyCount++;
						priorCFName = ((ColumnFamilyMapping)columnFamilyMappingList.get(i)).getColumnFamilyName();
						CFList.add(((ColumnFamilyMapping)columnFamilyMappingList.get(i)).getColumnFamilyName());
					}else {
						if(!priorCFName.equals(
								((ColumnFamilyMapping)columnFamilyMappingList.get(i)).getColumnFamilyName())){
							columnFamilyCount++;
							priorCFName = ((ColumnFamilyMapping)columnFamilyMappingList.get(i)).getColumnFamilyName();
							CFList.add(((ColumnFamilyMapping)columnFamilyMappingList.get(i)).getColumnFamilyName());
						}
					}
				}
				familyNames = CFList.toArray(new String[columnFamilyCount]);
				hbaseOperator.createTable(this.hbaseTableName, familyNames);
			}else if (!hbaseTableAppend && (appDataAns.indexOf("N") >= 0 || appDataAns.indexOf("n") >= 0)){
				if(hbaseOperator.isTableExist(this.hbaseTableName)){
					logger.info("Please confirm exists target HBase table first before using this import tool!!");
					System.out.println("Please confirm exists target HBase table first before using this import tool!!");
				}else{
					logger.info("Please create target HBase table first before using this import tool!!");
					System.out.println("Please create target HBase table first before using this import tool!!");
				}
				return;
			}
	
			
			PreparedStatement pstmt = con.prepareStatement("Select count(*) from " + this.tableName );
			ResultSet rs = pstmt.executeQuery();
			while(rs.next()){
				totalRecordCount = rs.getInt(1);
			}
			rs.close();
			pstmt.close();
			pstmt.setFetchSize(1000);
			pstmt = con.prepareStatement("Select * from " + this.tableName );
			rs = pstmt.executeQuery();
			logger.info("+++++++++++++++++++++++Importing progress starting++++++++++++++++++++++++++");
			logger.info("Start to import data, plese wait!!");
			System.out.println("Start to import data, plese wait!!");
			System.out.println("It will import total " + totalRecordCount + " record into HBase.");
			logger.info("It will import total " + totalRecordCount + " record into HBase.");
			System.out.println("####--------------------------------------------------####");
			System.out.print("..0%");
			while(rs.next()){
				hbaseTableRowKeyVal = rs.getString(this.hbaseTableRowKey);
				ColumnFamilyMapping columnFamilyMapping = null;
				ArrayList<ColumnFamilyValue> columnFamilyValueList = new ArrayList<ColumnFamilyValue>();
				for(int CFIdx = 0;CFIdx < columnFamilyMappingList.size();CFIdx++){
					columnFamilyMapping = (ColumnFamilyMapping)columnFamilyMappingList.get(CFIdx);
					byte[] value = rs.getBytes(columnFamilyMapping.getDbFieldName());
					if(value==null){
						value="".getBytes();
					}
					columnFamilyValueList.add(new ColumnFamilyValue(value, columnFamilyMapping.getColumnFamilyName(),
							columnFamilyMapping.getQualifierName()));
				/*	hbaseOperator.insertData(this.hbaseTableName, hbaseTableRowKeyVal, 
							columnFamilyMapping.getColumnFamilyName(), 
							columnFamilyMapping.getQualifierName(), value); */
				}
				hbaseOperator.insertRowData(this.hbaseTableName, hbaseTableRowKeyVal,columnFamilyValueList);
				columnFamilyValueList = null;
				progressCount++;
				int tempProgressNumber = (int)Math.round((progressCount/totalRecordCount) * 100);
				if(progressNumber != tempProgressNumber && tempProgressNumber % 2 == 0){
					if(tempProgressNumber > 2){
						for(int i=0;i<(tempProgressNumber-progressNumber)/2;i++){
							System.out.print("*");
						}
						progressNumber += tempProgressNumber-progressNumber;
						logger.info("******Current import file counts is " + progressCount + "*****");
					}else{
						System.out.print("*");
						progressNumber += 2;
					}
				}
				logger.info("Current import rowkkey is " + hbaseTableRowKeyVal);
			}
			
		} catch (Exception e) {
			logger.error("==================Import Image file failure========================");
			logger.error("Import rowkey failure " + hbaseTableRowKeyVal  + " failure, it encounter problem as follow error message :");
			logger.error(e.getMessage());
			logger.error("==================Import Image file failure========================");
			
    		progressCount++;
			System.exit(-1);
		}
		System.out.println("100%");
		end_time = System.currentTimeMillis();
		System.out.println("Finish importing total " + (totalRecordCount - importingFailureCount) + 
				" record to HBase, it is " + importingFailureCount + " records import failure, it takes : " + (end_time - start_time) + "ms");
		logger.info("Finish importing total " + (totalRecordCount - importingFailureCount) + 
				" record to HBase, it is " + importingFailureCount + " records import failure, it takes : " + (end_time - start_time) + "ms");
		logger.info("+++++++++++++++++++++++Importing progress complete++++++++++++++++++++++++++");
		
	}
		
	/**
	 * It direct import source database directly, it use field name as a column family name and 
	 * the column family has only qualifier "val". 
	 */
	public void executeImport(){
		Connection 	con = null;
		String[] 	fieldNames = null,							//Database table field name list
					fieldTypes = null;							//Database table field type list
		String 		createTableAns = "N",						//Flag control to create target hbase table  
					dropTableAns = "N", 						//Flag control to drop target hbase table
					appDataAns = "N";							//Flag control to append data to hbase table
		
		long 		start_time, 
					end_time;									//start_time and end_time variable are use for execution time
		float 		progressCount = 0;							//Execution progress percentage
		int 		progressNumber = 0; 						//Execution progress number
		
		start_time 			= System.currentTimeMillis();		//Importing start time
		hbaseOperator 		= new HBaseOperator(this.hbaseUrl);	//Initial hbase manipulation program
		Statement stmt 		= null;
		ResultSet rs 		= null, 
				  rsCount 	= null;
		PreparedStatement pstmt 		= null;	
		ResultSetMetaData rsMetaData 	= null;				//Retrieve Table Meta data
				
		try {
			// Check database type through jdbc connection string
			/*if(this.DBUrl.indexOf("oracle") > 0){
				DriverManager.registerDriver(new oracle.jdbc.driver.OracleDriver());
			}else if(this.DBUrl.indexOf("sybase") > 0){
				DriverManager.registerDriver(new com.sybase.jdbc4.jdbc.SybDriver());
			}else if(this.DBUrl.indexOf("mysql") > 0){
				DriverManager.registerDriver(new  com.mysql.jdbc.Driver());
			}else if(this.DBUrl.indexOf("sqlserver") > 0){
				DriverManager.registerDriver(new com.microsoft.sqlserver.jdbc.SQLServerDriver());
			}*/
			
			// Create database connection
			//con = DriverManager.getConnection(this.DBUrl,this.userName,this.password);
			DatabaseConnectionHandler DBConnHandler = new DatabaseConnectionHandler();
			con = DBConnHandler.getConnection(this.DBUrl, this.userName, this.password);
			
			scan = new Scanner(System.in);
			scan.useDelimiter("\n");
			if(hbaseOperator.isTableExist(this.hbaseTableName) && !hbaseTableReplace && !hbaseTableAppend){
				//The target hbase table is exist, confirm to drop it or not
				System.out.println("The target HBase Table is exist, do you want to drop it first ?(Y/N)");
				dropTableAns = scan.next();
				
				if(dropTableAns.indexOf("Y") >= 0 || dropTableAns.indexOf("y") >= 0){
					//drop exist hbase table
					hbaseOperator.dropTable(this.hbaseTableName);
				}else{
					//Does source data append to exist hbase table ?
					System.out.println("The target HBase Table is exist, do you want to append data to target hbase table ?(Y/N)");
					appDataAns = scan.next();
				}
			}else if(!hbaseOperator.isTableExist(this.hbaseTableName) && !hbaseTableReplace && !hbaseTableAppend){
				//The target hbase table is not exist, confirm to create a new one
				System.out.println("The target HBase Table doesn't exist, do you want to create it ?(Y/N)");
				createTableAns = scan.next();
			}else {
				if(hbaseOperator.isTableExist(this.hbaseTableName) && !hbaseTableAppend){
					//Drop exist hbase table via argument
					hbaseOperator.dropTable(this.hbaseTableName);
				}else if(!hbaseOperator.isTableExist(this.hbaseTableName) || hbaseTableReplace){
					//Does it need to create hbase table 
					createTableAns = "Y";
				}
			}
			
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
			
			fieldNames = new String[rsMetaData.getColumnCount()-1];
			fieldTypes = new String[rsMetaData.getColumnCount()-1];
			
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
			
			//retrieve the source table's meta information
			for(int colIdx=0, fieldNameLoc=0;colIdx<rsMetaData.getColumnCount();colIdx++){
				if(!this.hbaseTableRowKey.toUpperCase().equals(rsMetaData.getColumnName(colIdx+1).toUpperCase())){
					fieldNames[fieldNameLoc] = rsMetaData.getColumnName(colIdx+1);
					fieldTypes[fieldNameLoc] = rsMetaData.getColumnTypeName(colIdx+1);
					fieldNameLoc++;
				}
			}
			
			if(!hbaseOperator.isTableExist(this.hbaseTableName) || 
					(createTableAns.indexOf("Y") >=0 || createTableAns.indexOf("y") >= 0)){
				//Create HBase Table
				if(this.hbaseSingleColumnFamily == null){
					hbaseOperator.createTable(this.hbaseTableName, fieldNames);
				}else{
					hbaseOperator.createTable(this.hbaseTableName, this.hbaseSingleColumnFamily);
				}
			}else if (!hbaseTableAppend && (appDataAns.indexOf("N") >= 0 || appDataAns.indexOf("n") >= 0)){
				if(hbaseOperator.isTableExist(this.hbaseTableName)){
					System.out.println("Please confirm exists target HBase table first before using this import tool!!");
					logger.info("Please confirm exists target HBase table first before using this import tool!!");
				}else{
					System.out.println("Please create target HBase table first before using this import tool!!");
					logger.info("Please create target HBase table first before using this import tool!!");
				}
				return;
			}

			stmt = con.createStatement();
			rsCount = stmt.executeQuery("Select count(*) from " + this.tableName );
			while(rsCount.next()){
				totalRecordCount = rsCount.getInt(1);
			}
			stmt.close();
			
			pstmt.setFetchSize(1000);
			rs = pstmt.executeQuery();
			logger.info("+++++++++++++++++++++++Importing progress starting++++++++++++++++++++++++++");
			logger.info("Starting import data, plese wait!!");
			System.out.println("Starting import data, plese wait!!");
			System.out.println("It will import total " + totalRecordCount + " record into HBase table " + 
					this.hbaseTableName + ".");
			logger.info("It will import total " + totalRecordCount + " record into HBase table " + 
					this.hbaseTableName + ".");
			System.out.println("####--------------------------------------------------####");
			System.out.print("..0%");
			importingFailureCount=0;
			while(rs.next()){
				//retrieve source table data and import to target hbase table
				//To retrive rowkey value from source table record
				String hbaseTableRowKeyVal = rs.getString(this.hbaseTableRowKey);
				
				ArrayList<ColumnFamilyValue> columnFamilyValueList = new ArrayList<ColumnFamilyValue>();
				byte[] value = null;
				String fieldName = null;
				
				try {
					for(int colIdx=0;colIdx<rs.getMetaData().getColumnCount();colIdx++){
					
						fieldName = rs.getMetaData().getColumnName(colIdx+1);
						if(!fieldName.toUpperCase()
								.equals(this.hbaseTableRowKey.toUpperCase())){
							if(isImageField(rs.getMetaData().getColumnTypeName(colIdx+1))){
								if(rs.getBytes(fieldName)==null){
									value = "".getBytes();
								}else{
									value = rs.getBytes(fieldName);
								}
							}else{
								if(rs.getString(fieldName)==null){
									value = "".getBytes();
								}else{
									value = rs.getString(fieldName).getBytes();
								}
							}
							
							if(this.hbaseSingleColumnFamily == null){
								columnFamilyValueList.add(new ColumnFamilyValue(value, 
										fieldName, "val"));
							}else{
								columnFamilyValueList.add(new ColumnFamilyValue(value, 
										this.hbaseSingleColumnFamily, fieldName));
							}
						}
					}					
				} catch (Exception e) {	
					logger.error("==================Import Image file failure========================");
					logger.error("Failure import record key is " + hbaseTableRowKeyVal + ", failure on field = " + fieldName + ", it encounter problem as follow error message :");
					logger.error(e.getMessage());
					logger.error("==================Import Image file failure========================");
					importingFailureCount++;
					
				}

				hbaseOperator.insertRowData(this.hbaseTableName, hbaseTableRowKeyVal,columnFamilyValueList);
				
				columnFamilyValueList = null;
				progressCount++;
				int tempProgressNumber = (int)Math.round((progressCount/totalRecordCount) * 100);
				if(progressNumber != tempProgressNumber && tempProgressNumber % 2 == 0){
					if(tempProgressNumber > 2){
						for(int i=0;i<(tempProgressNumber-progressNumber)/2;i++){
							System.out.print("*");
						}
						progressNumber += tempProgressNumber-progressNumber;
						logger.info("******Current import record counts is " + progressCount + "*****");
					}else{
						System.out.print("*");
						progressNumber += 2;
					}
				}
				logger.info("Current import rowkkey is " + hbaseTableRowKeyVal);
			}
			
		} catch (Exception e) {
			logger.error("Importing data failure!! Cause by :");
			logger.error(e.getMessage());
			System.exit(-1);
		}
		System.out.println("100%");
		end_time = System.currentTimeMillis();
		System.out.println("Finish importing total " + (totalRecordCount - importingFailureCount) + 
				" record to HBase, it is " + importingFailureCount + " records import failure, it takes : " + (end_time - start_time) + "ms");
		logger.info("Finish importing total " + (totalRecordCount - importingFailureCount) + 
				" record to HBase, it is " + importingFailureCount + " records import failure, it takes : " + (end_time - start_time) + "ms");
		logger.info("+++++++++++++++++++++++Importing progress complete++++++++++++++++++++++++++");
	}
	
	private void setColumnFamilyByFile(String FileName){
		
		logger.info("Process Mapping file!!");
		String sCurrentLine;

		try (BufferedReader br = new BufferedReader(new FileReader(FileName)))
		{
			while ((sCurrentLine = br.readLine()) != null) {
				if(sCurrentLine.indexOf("rowkey")>=0){
					this.hbaseTableRowKey = sCurrentLine.substring(0, sCurrentLine.indexOf(" "));
				}else if(sCurrentLine.indexOf("#") < 0){
					ColumnFamilyMapping columnFamilyMapping = 
						new ColumnFamilyMapping(sCurrentLine.substring(0, sCurrentLine.indexOf(" ")),
								sCurrentLine.substring(sCurrentLine.indexOf(" ")+1, sCurrentLine.indexOf(":")), 
								sCurrentLine.substring(sCurrentLine.indexOf(":")+1, sCurrentLine.length()));
					this.columnFamilyMappingList.add(columnFamilyMapping);
				}
			}
			this.importByMappingFile = true;

		} catch (Exception e) {
			logger.error("ColumnFamily Mapping File not correct!!");
			System.out.println("ColumnFamily Mapping File not correct!!");
			e.printStackTrace();
			System.exit(-1);
		} 
	}
	
	public static void main(String args[]){
		
		logger.info("Starting Importor tool to import data from database to HBase!!");
		
		logger.info("Initial Importor tool!!");
		ImportHandler importHandler = new ImportHandler();
		
		importHandler.execute(args);
		System.exit(importHandler.getTotalRecordCount() - importHandler.getImportingFailureCount());
	}

	public String getPassword() {
		return password;
	}

	public void setPassword(String password) {
		this.password = password;
	}

	public void setDBUrl(String dBUrl) {
		DBUrl = dBUrl;
	}

	public void setUserName(String userName) {
		this.userName = userName;
	}
	
	public void setHBaseUrl(String hBaseUrl) {
		hbaseUrl = hBaseUrl;
	}

	public void setTableName(String tableName) {
		this.tableName = tableName;
	}

	public void setHBaseTableName(String hBaseTableName) {
		hbaseTableName = hBaseTableName;
	}

	public void setHBaseTableRowKey(String hbaseTableRowKey) {
		this.hbaseTableRowKey = hbaseTableRowKey;
	}

	public boolean isHbaseTableReplace() {
		return hbaseTableReplace;
	}

	public void setHbaseTableReplace(boolean hbaseTableReplace) {
		this.hbaseTableReplace = hbaseTableReplace;
	}

	public int getTotalRecordCount() {
		return totalRecordCount;
	}

	public void setTotalRecordCount(int totalRecordCount) {
		this.totalRecordCount = totalRecordCount;
	}

	public boolean isHbaseTableAppend() {
		return hbaseTableAppend;
	}

	public void setHbaseTableAppend(boolean hbaseTableAppend) {
		this.hbaseTableAppend = hbaseTableAppend;
	}

	public String getHbaseSingleColumnFamily() {
		return hbaseSingleColumnFamily;
	}

	public void setHbaseSingleColumnFamily(String hbaseSingleColumnFamily) {
		this.hbaseSingleColumnFamily = hbaseSingleColumnFamily;
	}

	public int getImportingFailureCount() {
		return importingFailureCount;
	}

}
