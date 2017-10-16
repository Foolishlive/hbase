package com.mpinfo.hadoop.utils;

import org.apache.log4j.Logger;
import java.io.*;
import java.net.URL;
import java.nio.file.*;
import java.util.Scanner;
import javax.imageio.*;
import java.awt.image.*;
import java.sql.*;
import com.mpinfo.database.utils.DatabaseConnectionHandler;
import com.mpinfo.hadoop.hbase.*;

public class ImportImageFileHandler {

	private static Logger logger = Logger.getLogger(ImportImageFileHandler.class.getName());
	private String filePath = null;
	private String fileURL = null;
	private String DBUrl = null;						//Relational Database JDBC URL Connection String
	private String userName = null;						//Relational Database login user name
	private String password = null;						//Relational Database login user password
	private String tableName = null;					//Import source relational database's table name
	private String imageFieldName = null;
	private String hbaseTableRowKey = null;
	private String hbaseUrl = null;
	private String hbaseTableName = null;private String hbaseTableColumnFamily = null;
	private String hbaseTableColumnQualifier = null;
	private int totalRecordCount = 0, importingFailureCount = 0;					//Counting total importing image files and failure importing image files
	private boolean hbaseTableReplace = false;			//The flag control import data to overwrite exist hbase table, it will drop exist table and create the same table again 
	private boolean hbaseTableAppend = false;			//The flag control import data to append to exist hbase table
	private HBaseOperator hbaseOperator = null;			//Hbase manipulation program execute insert, delete and update data to hbase table
	private String rowKeyReplaceSign = null,
			       rowKeyReplacedBySign = null;
	private Scanner scan;
	
	public void helper(){
		System.out.println("Here is some help for this importor tool:");
		System.out.println("You must provide following arguments:");
		System.out.println("\t-filepath :\t\t\t source image files path, for example: /home/user/");
		System.out.println("\t-url :\t\t\t source image files url, for example: http://www.mpinfo.com.tw/product/mpinfo.jpg");
		System.out.println("\t-dburl :\t\t source database jdbc connection url, for example: ");
		System.out.println("\t\t Oracle\t:\t\t jdbc:oracle:thin:@<HOST>:<PORT>:<SID>");
		System.out.println("\t\t MS SQL Server\t:\t jdbc:microsoft:sqlserver://<HOST>:<PORT>[;DatabaseName=<DB>]");
		System.out.println("\t\t Sybase\t:\t\t jdbc:sybase:Tds:<HOST>:<PORT>");
		System.out.println("\t\t MySQL\t:\t\t jdbc:mysql://<HOST>:<PORT>/<DB>");
		System.out.println("\t-username :\t\t source database login user");
		System.out.println("\t-password :\t\t source database login user's password");
		System.out.println("\t-table :\t\t import table name in source database");
		System.out.println("\t-imagefieldname :\t\t table field name record image file path");
		System.out.println("\t-hbasetablerowkey :\t target hbase table's row key, it also can be define mapping file ");
		System.out.println("\t-hbaseurl :\t\t\t target hbase connection url ");
		System.out.println("\t-hbasetablename :\t\t target hbase table name ");
		System.out.println("\t-hbasetablecolumnfamily : \t Specific column family in HBase Table");
		System.out.println("\t-hbasetablecolumnqualifier : \t Specific column qualifier in HBase Table");
		System.out.println("\t-hbasetablereplace : \t\t Replace exist HBase Table and Data");
		System.out.println("\t-hbasetableappend : \t\t Append data to exist HBase Table");
		System.out.println("\t-rowkeyreplacesign : \t\t The sign in hbase rokey");
		System.out.println("\t-rowkeyreplacedbysign : \t The sign replace sign in thefile name");
	}
	
	public void execute(String[] args){
		
		logger.info("Checking needed arguments!!");
		if(args.length == 0){
			helper();
			System.exit(0);
		}
		for(int loc = 0;loc < args.length;loc++){
			if(args[loc].equals("-filepath")){
				setFilePath(args[++loc]);
			}else if(args[loc].equals("-hbaseurl")){
				setHbaseUrl(args[++loc]);
			}else if(args[loc].equals("-hbasetablename")){
				setHbaseTableName(args[++loc]);
			}else if(args[loc].equals("-hbasecolumnfamily")){
				setHbaseTableColumnFamily(args[++loc]);
			}else if(args[loc].equals("-hbasecolumnqualifier")){
				setHbaseTableColumnQualifier(args[++loc]);
			}else if(args[loc].equals("-hbasetableappend")){
				setHbaseTableAppend(true);
			}else if(args[loc].equals("-hbasetablereplace")){
				setHbaseTableReplace(true);
			}else if(args[loc].equals("-rowkeyreplacesign")){
				setRowKeyReplaceSign(args[++loc]);
			}else if(args[loc].equals("-rowkeyreplacedbysign")){
				setRowKeyReplacedBySign(args[++loc]);
			}else if(args[loc].equals("-dburl")){
				setDBUrl(args[++loc]);
			}else if(args[loc].equals("-username")){
				setUserName(args[++loc]);
			}else if(args[loc].equals("-password")){
				setPassword(args[++loc]);
			}else if(args[loc].equals("-table")){
				setTableName(args[++loc]);
			}else if(args[loc].equals("-imagefieldname")){
				setImageFieldName(args[++loc]);
			}else if(args[loc].equals("-hbasetablerowkey")){
				setHbaseTableRowKey(args[++loc]);
			}else if(args[loc].equals("-url")){
				setHbaseTableRowKey(args[++loc]);
			}
		}
		
		if(this.filePath != null){
			importFromFilePath();
		}else if(this.DBUrl != null){
			importFromDatabase();
		}else if(this.fileURL != null){
			importFromFileURL();
		}
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

	public void importFromDatabase(){
		
		String 		createTableAns = "N",						//Flag control to create target hbase table  
					dropTableAns = "N", 						//Flag control to drop target hbase table
					appDataAns = "N";							//Flag control to append data to hbase table
		String hbaseTableRowKeyVal = null;
		long 		start_time, 
					end_time;									//start_time and end_time variable are use for execution time
		float 		progressCount = 0;							//Execution progress percentage
		int 		progressNumber = 0; 						//Execution progress number
		File image = null;
		Connection 	conn 	= null;
		Statement 	stmt	= null;
		ResultSet 	rs 	 	= null, 
				  	rsCount = null;
		PreparedStatement pstmt 		= null;	
	
		start_time 			= System.currentTimeMillis();		//Importing start time
		hbaseOperator 		= new HBaseOperator(this.hbaseUrl);	//Initial hbase manipulation program
			
		if(this.DBUrl == null){
			System.out.println("Please provide source database connection string!!");
			logger.error("Please provide source database connection string!!");
			helper();
			System.exit(0);
		}
		if(this.userName == null || this.password == null){
			System.out.println("Please provide source database's username or password  !!");
			logger.error("Please provide source database's username or password  !!");
			helper();
			System.exit(0);
		}
		if(this.tableName == null){
			System.out.println("Please provide source database table name !!");
			logger.error("Please provide source database table name !!");
			helper();
			System.exit(0);
		}
		if(this.imageFieldName == null){
			System.out.println("Please provide source table field name that record image path!!");
			logger.error("Please provide source table field name that record image path!!");
			helper();
			System.exit(0);
		}
		if(this.hbaseTableRowKey == null){
			System.out.println("Please provide source table field name which as target rowkey!!");
			logger.error("Please provide source table field name which as target rowkey!!");
			helper();
			System.exit(0);
		}
		if(this.hbaseUrl == null){
			System.out.println("Please provide target HBase connection url!!");
			logger.error("Please provide target HBase connection url!!");
			helper();
			System.exit(0);
		}
		if(this.hbaseTableName == null){
			System.out.println("Please provide target HBase Table Name!!");
			logger.error("Please provide target HBase Table Name!!");
			helper();
			System.exit(0);
		}
		if(this.hbaseTableColumnFamily == null){
			System.out.println("Please provide target HBase Table Column Family Name !!");
			logger.error("Please provide target HBase Table Column Family Name !!");
			helper();
			System.exit(0);
		}
		if( this.hbaseTableColumnQualifier == null){
			System.out.println("Please provide target HBase Table Column Qualifier Name !!");
			logger.error("Please provide target HBase Table Column Qualifier Name !!");
			helper();
			System.exit(0);
		}
		
		try{
			DatabaseConnectionHandler DBConnHandler = new DatabaseConnectionHandler();
			conn = DBConnHandler.getConnection(this.DBUrl, this.userName, this.password);
			
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
			
			if(!hbaseOperator.isTableExist(this.hbaseTableName) || 
					(createTableAns.indexOf("Y") >=0 || createTableAns.indexOf("y") >= 0)){
				//Create HBase Table
					hbaseOperator.createTable(this.hbaseTableName, this.hbaseTableColumnFamily);
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
			
			stmt = conn.createStatement();
			rsCount = stmt.executeQuery("Select count(*) from " + this.tableName );
			while(rsCount.next()){
				totalRecordCount = rsCount.getInt(1);
			}
			stmt.close();
			
			pstmt = conn.prepareStatement("Select * from " + this.tableName );
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
			progressCount = 1;
			int tempProgressNumber = 0;
			while(rs.next()){
				//retrieve source table data and import to target hbase table
				//To retrive rowkey value from source table record
				try{
					hbaseTableRowKeyVal = rs.getString(this.hbaseTableRowKey);
					byte[] value = null;
					if(rs.getBytes(this.imageFieldName)==null){
						value = "".getBytes();
					}else if(rs.getString(this.imageFieldName).indexOf("http")>=0){
						URL url = new URL(rs.getString(this.imageFieldName));
						BufferedImage bufferedImage = ImageIO.read(url);
						ByteArrayOutputStream byteArrayOut = new ByteArrayOutputStream();
						String extension = null;
						String imageFileName = rs.getString(this.imageFieldName);
						while(imageFileName.indexOf("/") >= 0){
							imageFileName = imageFileName.substring(imageFileName.indexOf("/")+1, imageFileName.length());
						}
						if(imageFileName.indexOf(".") >= 0){
							extension = imageFileName.substring(imageFileName.indexOf(".")+1, imageFileName.length());
						}else{
							extension = "jpg";
						}
						logger.info("import rowkey " + hbaseTableRowKeyVal + " image file " + 
								rs.getString(this.imageFieldName) + " into hbase!");
						ImageIO.write(bufferedImage, extension, byteArrayOut);
		        		value = byteArrayOut.toByteArray();
					}else{
						image = new File(rs.getString(this.imageFieldName));
						BufferedImage bufferedImage = ImageIO.read(image);
						ByteArrayOutputStream byteArrayOut = new ByteArrayOutputStream();
						logger.info("import rowkey " + hbaseTableRowKeyVal + " image file " + 
								rs.getString(this.imageFieldName) + " into hbase!");
						String extension = null;
						if(image.getName().indexOf(".") >= 0){
							extension = image.getName().substring(image.getName().indexOf(".")+1, image.getName().length());
						}
						if(extension==null){
							extension = "jpg";
						}
						ImageIO.write(bufferedImage, extension, byteArrayOut);
		        		value = byteArrayOut.toByteArray();
					}
					hbaseOperator.insertData(this.hbaseTableName, hbaseTableRowKeyVal, this.hbaseTableColumnFamily,
	        				this.hbaseTableColumnQualifier, value);
					
					tempProgressNumber = (int)Math.round((progressCount/totalRecordCount) * 100);
	        		if(progressNumber != tempProgressNumber && tempProgressNumber % 2 == 0){
	        			if(tempProgressNumber > 2){
	        				for(int j=0;j<(tempProgressNumber-progressNumber)/2;j++){
	        					System.out.print("*");
	        				}
	        				progressNumber += tempProgressNumber-progressNumber;
	        				logger.info("******Current import file counts is " + progressCount + "*****");
	        			}else{
	        				System.out.print("*");
	        				progressNumber += 2;
	        			}
	        		}
	        		progressCount++;
				}catch(Exception e){
					// TODO Auto-generated catch block
					logger.error("==================Import Image file failure========================");
					logger.error("Import rowkey " + hbaseTableRowKeyVal + " image file " + image.getName() + " failure, it encounter problem as follow error message :");
					logger.error(e.getMessage());
					logger.error("==================Import Image file failure========================");
					
					importingFailureCount++;
		    		progressCount++;
				}
			}
			
		}catch(SQLException sqlex){
			// TODO Auto-generated catch block
			logger.error("==================Import Image file failure========================");
			logger.error("Connect to database failure, this importing procedure stop now!");
			logger.error(sqlex.getMessage());
			logger.error("==================Import Image file failure========================");
			
			importingFailureCount++;
    		progressCount++;
			//e.printStackTrace();
		}
		System.out.println("100%");
		end_time = System.currentTimeMillis();
		System.out.println("Finish importing total " +totalRecordCount + " image files to HBase, there is " + 
				importingFailureCount +" image files are importing failure, it takes : " + (end_time - start_time) + "ms");
		logger.info("Finish importing total " +totalRecordCount + " image files to HBase, there is " + 
				importingFailureCount +" image files are importing failure, it takes : " + (end_time - start_time) + "ms");
		logger.info("+++++++++++++++++++++++Importing progress complete++++++++++++++++++++++++++");
				
	}
	
	public String getDBUrl() {
		return DBUrl;
	}

	public void setDBUrl(String dBUrl) {
		DBUrl = dBUrl;
	}

	public void importFromFilePath(){
		
		String 		createTableAns = "N",						//Flag control to create target hbase table  
					dropTableAns = "N", 						//Flag control to drop target hbase table
					appDataAns = "N";							//Flag control to append data to hbase table

		long 		start_time, 
					end_time;									//start_time and end_time variable are use for execution time
		float 		progressCount = 0;							//Execution progress percentage
		int 		progressNumber = 0; 						//Execution progress number
		File folder = null;
		File image = null;
		
		start_time 			= System.currentTimeMillis();		//Importing start time
		hbaseOperator 		= new HBaseOperator(this.hbaseUrl);	//Initial hbase manipulation program
				
		if(this.filePath == null){
			System.out.println("Please provide source image files path!!");
			logger.error("Please provide source image files path!!!");
			helper();
			System.exit(0);
		}
		if(this.hbaseUrl == null){
			System.out.println("Please provide target HBase connection url!!");
			logger.error("Please provide target HBase connection url!!");
			helper();
			System.exit(0);
		}
		if(this.hbaseTableName == null){
			System.out.println("Please provide target HBase Table Name!!");
			logger.error("Please provide target HBase Table Name!!");
			helper();
			System.exit(0);
		}
		if(this.hbaseTableColumnFamily == null){
			System.out.println("Please provide target HBase Table Column Family Name !!");
			logger.error("Please provide target HBase Table Column Family Name !!");
			helper();
			System.exit(0);
		}
		if( this.hbaseTableColumnQualifier == null){
			System.out.println("Please provide target HBase Table Column Qualifier Name !!");
			logger.error("Please provide target HBase Table Column Qualifier Name !!");
			helper();
			System.exit(0);
		}
		
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
			hbaseOperator.createTable(this.hbaseTableName, this.hbaseTableColumnFamily);
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
		
		folder = new File(filePath);
		
		if(folder.listFiles().length <= 0){
			System.out.println("Please provide source image files path!! There is no images file in the path!! ");
			logger.error("Please provide source image files path!!! There is no images file in the path!!");
			System.exit(0);
		}
		start_time = System.currentTimeMillis();			//Importing start time
		
		totalRecordCount = folder.listFiles().length;
		
		progressCount = 1;
		int tempProgressNumber = 0;
		Path dir = FileSystems.getDefault().getPath(filePath);
		DirectoryStream<Path> stream = null;
		try {
			stream = Files.newDirectoryStream(dir);
		} catch (IOException e1) {
			logger.error("This folder is not exist, please check your path !!");
			logger.error(e1.getMessage());
			System.out.println("This folder is not exist, please check your path !!");
			System.out.println(e1.getMessage());
		}
		
		logger.info("+++++++++++++++++++++++Importing progress starting++++++++++++++++++++++++++");
		logger.info("Starting import data, plese wait!!");
		System.out.println("Starting import data, plese wait!!");
		System.out.println("It will import approximately " + totalRecordCount + " image files into HBase.");
		logger.info("It will import approximately " + totalRecordCount + " image files into HBase");
		System.out.println("####--------------------------------------------------####");
		System.out.print("..0%");
				
		//for(int i=0;i<folder.listFiles().length;i++){
		for(Path path : stream){
			try {
				//fileEntry = fileLists[i];
			//for (final File fileEntry : folder.listFiles()) {
				
				//fileEntry = path.getFileName().toFile();
				if (path.toFile().isFile()) {
					
					//image = new File(fileEntry.getAbsolutePath());
					image = path.toFile();
					BufferedImage bufferedImage = ImageIO.read(image);
					ByteArrayOutputStream byteArrayOut = new ByteArrayOutputStream();
					logger.info("Current import image file " + image.getName() + " into hbase!");
					String extension = image.getName().substring(image.getName().indexOf(".")+1, image.getName().length());
					String rowID = image.getName().substring(0, image.getName().indexOf("."));
					while(rowKeyReplacedBySign != null && rowID.indexOf(rowKeyReplacedBySign)>=0){
						rowID = rowID.substring(0, rowID.indexOf(rowKeyReplacedBySign)) + rowKeyReplaceSign +
								rowID.substring(rowID.indexOf(rowKeyReplacedBySign)+1, rowID.length());
					}
					
	        		ImageIO.write(bufferedImage, extension, byteArrayOut);
	        		byte[] imageBytes = byteArrayOut.toByteArray();
	        		hbaseOperator.insertData(this.hbaseTableName, rowID, this.hbaseTableColumnFamily,
	        				this.hbaseTableColumnQualifier, imageBytes);
				
	        		tempProgressNumber = (int)Math.round((progressCount/totalRecordCount) * 100);
	        		if(progressNumber != tempProgressNumber && tempProgressNumber % 2 == 0){
	        			if(tempProgressNumber > 2){
	        				for(int j=0;j<(tempProgressNumber-progressNumber)/2;j++){
	        					System.out.print("*");
	        				}
	        				progressNumber += tempProgressNumber-progressNumber;
	        				logger.info("******Current import file counts is " + progressCount + "*****");
	        			}else{
	        				System.out.print("*");
	        				progressNumber += 2;
	        			}
	        		}
	        		progressCount++;
				}else{
					totalRecordCount--;
				}
	        
			} catch (Exception e) {
				// TODO Auto-generated catch block
				logger.error("==================Import Image file failure========================");
				logger.error("Import image file " + image.getName() + " failure, it encounter problem as follow error message :");
				logger.error(e.getMessage());
				logger.error("==================Import Image file failure========================");
				
				importingFailureCount++;
        		progressCount++;
				//e.printStackTrace();
			}
		}
		System.out.println("100%");
		end_time = System.currentTimeMillis();
		System.out.println("Finish importing total " +totalRecordCount + " image files to HBase, there is " + 
				importingFailureCount +" image files are importing failure, it takes : " + (end_time - start_time) + "ms");
		logger.info("Finish importing total " +totalRecordCount + " image files to HBase, there is " + 
				importingFailureCount +" image files are importing failure, it takes : " + (end_time - start_time) + "ms");
		logger.info("+++++++++++++++++++++++Importing progress complete++++++++++++++++++++++++++");
	}
	
	public void importFromFileURL(){
		
		String 		createTableAns = "N",						//Flag control to create target hbase table  
					dropTableAns = "N", 						//Flag control to drop target hbase table
					appDataAns = "N";							//Flag control to append data to hbase table

		long 		start_time, 
					end_time;									//start_time and end_time variable are use for execution time
		
		start_time 			= System.currentTimeMillis();		//Importing start time
		hbaseOperator 		= new HBaseOperator(this.hbaseUrl);	//Initial hbase manipulation program
				
		if(this.fileURL == null){
			System.out.println("Please provide source image files URL!!");
			logger.error("Please provide source image files URL!!!");
			helper();
			System.exit(0);
		}
		if(this.hbaseUrl == null){
			System.out.println("Please provide target HBase connection url!!");
			logger.error("Please provide target HBase connection url!!");
			helper();
			System.exit(0);
		}
		if(this.hbaseTableName == null){
			System.out.println("Please provide target HBase Table Name!!");
			logger.error("Please provide target HBase Table Name!!");
			helper();
			System.exit(0);
		}
		if(this.hbaseTableColumnFamily == null){
			System.out.println("Please provide target HBase Table Column Family Name !!");
			logger.error("Please provide target HBase Table Column Family Name !!");
			helper();
			System.exit(0);
		}
		if( this.hbaseTableColumnQualifier == null){
			System.out.println("Please provide target HBase Table Column Qualifier Name !!");
			logger.error("Please provide target HBase Table Column Qualifier Name !!");
			helper();
			System.exit(0);
		}
		
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
			hbaseOperator.createTable(this.hbaseTableName, this.hbaseTableColumnFamily);
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
		
		
		start_time = System.currentTimeMillis();			//Importing start time
		
		logger.info("+++++++++++++++++++++++Importing progress starting++++++++++++++++++++++++++");
		logger.info("Starting import data, plese wait!!");
		System.out.println("Starting import data, plese wait!!");
		System.out.println("It will import image from " + this.fileURL + " into HBase.");
		logger.info("It will import image from " + this.fileURL + " into HBase.");
		try {
			URL url = new URL(this.fileURL);
			BufferedImage bufferedImage = ImageIO.read(url);
			ByteArrayOutputStream byteArrayOut = new ByteArrayOutputStream();
			String extension = this.fileURL.substring(this.fileURL.indexOf(".")+1, this.fileURL.length());
			String rowID = this.fileURL.substring(0, this.fileURL.indexOf("."));
			while(rowKeyReplacedBySign != null && rowID.indexOf(rowKeyReplacedBySign)>=0){
				rowID = rowID.substring(0, rowID.indexOf(rowKeyReplacedBySign)) + rowKeyReplaceSign +
						rowID.substring(rowID.indexOf(rowKeyReplacedBySign)+1, rowID.length());
			}
			ImageIO.write(bufferedImage, extension, byteArrayOut);
	        byte[] imageBytes = byteArrayOut.toByteArray();
	        hbaseOperator.insertData(this.hbaseTableName, rowID, this.hbaseTableColumnFamily,
	        		this.hbaseTableColumnQualifier, imageBytes);
				
			} catch (Exception e) {
				// TODO Auto-generated catch block
				logger.error("==================Import Image file failure========================");
				logger.error("Import image file " + this.fileURL + " failure, it encounter problem as follow error message :");
				logger.error(e.getMessage());
				logger.error("==================Import Image file failure========================");
				//e.printStackTrace();
			}
		end_time = System.currentTimeMillis();
		System.out.println("Finish importing image file to HBase, it takes : " + (end_time - start_time) + "ms");
		logger.info("Finish importing image file to HBase, it takes : " + (end_time - start_time) + "ms");
		logger.info("+++++++++++++++++++++++Importing progress complete++++++++++++++++++++++++++");
	}
	
	public int getImportingFailureCount() {
		return importingFailureCount;
	}

	public void setImportingFailuireCount(int importingFailureCount) {
		this.importingFailureCount = importingFailureCount;
	}

	public static void main(String[] args) {
		logger.info("Starting Importor tool to import data from database to HBase!!");
		/*if(args.length ==0){
			System.out.println("You need to provide parameters to import data to hbase!!");
		}else{*/
		logger.info("Initial Importor tool!!");
		ImportImageFileHandler importImageFileHandler = new ImportImageFileHandler();
		
		importImageFileHandler.execute(args);
		System.exit(importImageFileHandler.getTotalRecordCount());

	}

	public void setFilePath(String filePath) {
		this.filePath = filePath;
	}

	public void setHbaseUrl(String hbaseUrl) {
		this.hbaseUrl = hbaseUrl;
	}

	public void setHbaseTableName(String hbaseTableName) {
		this.hbaseTableName = hbaseTableName;
	}

	public void setHbaseTableColumnFamily(String hbaseTableColumnFamily) {
		this.hbaseTableColumnFamily = hbaseTableColumnFamily;
	}

	public void setHbaseTableColumnQualifier(String hbaseTableColumnQualifier) {
		this.hbaseTableColumnQualifier = hbaseTableColumnQualifier;
	}

	public void setHbaseTableReplace(boolean hbaseTableReplace) {
		this.hbaseTableReplace = hbaseTableReplace;
	}

	public void setHbaseTableAppend(boolean hbaseTableAppend) {
		this.hbaseTableAppend = hbaseTableAppend;
	}

	public int getTotalRecordCount() {
		return totalRecordCount;
	}

	public void setTotalRecordCount(int totalRecordCount) {
		this.totalRecordCount = totalRecordCount;
	}

	public String getRowKeyReplaceSign() {
		return rowKeyReplaceSign;
	}

	public void setRowKeyReplaceSign(String rowKeyReplaceSign) {
		this.rowKeyReplaceSign = rowKeyReplaceSign;
	}

	public String getRowKeyReplacedBySign() {
		return rowKeyReplacedBySign;
	}

	public void setRowKeyReplacedBySign(String rowKeyReplacedBySign) {
		this.rowKeyReplacedBySign = rowKeyReplacedBySign;
	}
	
	public String getImageFieldName() {
		return imageFieldName;
	}

	public void setImageFieldName(String imageFieldName) {
		this.imageFieldName = imageFieldName;
	}

	public String getHbaseTableRowKey() {
		return hbaseTableRowKey;
	}

	public void setHbaseTableRowKey(String hbaseTableRowKey) {
		this.hbaseTableRowKey = hbaseTableRowKey;
	}

	public String getHbaseUrl() {
		return hbaseUrl;
	}

	public String getTableName() {
		return tableName;
	}

	public void setTableName(String tableName) {
		this.tableName = tableName;
	}
	
	public String getFileURL() {
		return fileURL;
	}

	public void setFileURL(String fileURL) {
		this.fileURL = fileURL;
	}

}
