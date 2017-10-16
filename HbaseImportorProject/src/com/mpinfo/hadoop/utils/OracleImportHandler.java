package com.mpinfo.hadoop.utils;

import java.io.BufferedReader;
import java.io.FileReader;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.util.*;
import java.util.concurrent.*;
import org.apache.log4j.Logger;
import com.mpinfo.database.utils.DatabaseConnectionHandler;
import com.mpinfo.hadoop.hbase.*;
import com.mpinfo.thread.*;

public class OracleImportHandler implements Runnable {
	
	public 	static int 				RecordCountPerThread = 500000;
	private static Logger 			logger = Logger.getLogger(ImportHandler.class.getName());
	private final  ThreadPool 		pool = new ThreadPool();
	private String 					pstmtStr = null;
	private static String 			DBUrl = null;						//Relational Database JDBC URL Connection String
	private static String 			userName = null;					//Relational Database login user name
	private static String 			password = null;					//Relational Database login user password
	private static String 			tableName = null;					//Import source relational database's table name
	private static String 			hbaseUrl = null;					//Import target HBase zookeeper url
	private static String 			hbaseTableName = null;				//Import target HBase table name
	private boolean 				hbaseTableReplace = false;			//The flag control import data to overwrite exist hbase table, it will drop exist table and create the same table again 
	private boolean 				hbaseTableAppend = false;			//The flag control import data to append to exist hbase table
	private static String 			hbaseTableRowKey = null;			//Assign database field name as Hbase Table Row Key
	private static HBaseOperator 	hbaseOperator = null;				//Hbase manipulation program execute insert, delete and update data to hbase table 
	private boolean 				importByMappingFile = false;		//The flag control source database table field map to hbase table column family
	private static long				totalRecordCount = 0, 
			    					importingFailureCount = 0;			//Source table total record count, this variable is use for showing import progress
	private static String 			hbaseSingleColumnFamily = null;   	//Only one column family in Hbase Table
	private static ArrayList<ColumnFamilyMapping> columnFamilyMappingList
								= new ArrayList<ColumnFamilyMapping>();	//The Database table field and hbase table column family mapping list
	private static String[] 		fieldNames = null,					//Database table field name list
									fieldTypes = null;					//Database table field type list
	private static float 			progressCount;					//Execution progress percentage
	private static float			progressNumber; 				//Execution progress number
	private Scanner scan;
	private static Connection 		con = null;
	private int 					threadID = -1;
	private long 					start_time,
									end_time;
	
	public OracleImportHandler(){
		
	}
	
	public OracleImportHandler(String pstmtStr){
		this.pstmtStr = pstmtStr;
	}
	
	@Override
	public void run() {
		
		//HBaseOperator hbaseOperator 		=  null;	//new HBaseOperator(hbaseUrl);	//Initial hbase manipulation program
		
		String name = Thread.currentThread().getName();
		PreparedStatement pstmt = null;
		ResultSet rs = null;
        try
        {	  
        	threadID = pool.obtainThread();
        	/*if(pool.getHBaseOperator(threadID) != null){
        		hbaseOperator = pool.getHBaseOperator(threadID);
        	}else{
        		pool.setHBaseOperator(threadID, hbaseUrl);
        		hbaseOperator = pool.getHBaseOperator(threadID);
        	}*/
        	
            logger.info(name + " acquiring " + threadID);
              
            DatabaseConnectionHandler DBConnHandler = new DatabaseConnectionHandler();
            Connection conn = DBConnHandler.getConnection(DBUrl, userName, password);
              
            pstmt = conn.prepareStatement(pstmtStr);
            logger.info(pstmtStr);
            rs = pstmt.executeQuery();
            pstmt.setFetchSize(10000);
            importingFailureCount=0;
              
            while(rs.next()){
            	//retrieve source table data and import to target hbase table
            	//To retrive rowkey value from source table record
            	String hbaseTableRowKeyVal = rs.getString(hbaseTableRowKey);
  				
            	ArrayList<ColumnFamilyValue> columnFamilyValueList = new ArrayList<ColumnFamilyValue>();
            	byte[] value = null;
            	String fieldName = null;
  				
            	try {
            		for(int colIdx=0;colIdx<rs.getMetaData().getColumnCount();colIdx++){
  					
            			fieldName = rs.getMetaData().getColumnName(colIdx+1);
            			if(!fieldName.toUpperCase()
  							.equals(hbaseTableRowKey.toUpperCase())){
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
  							
            				if(hbaseSingleColumnFamily == null){
            					columnFamilyValueList.add(new ColumnFamilyValue(value, 
  										fieldName, "val"));
            				}else{
            					columnFamilyValueList.add(new ColumnFamilyValue(value, 
  										hbaseSingleColumnFamily, fieldName));
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
            	  
            	hbaseOperator.insertRowData(hbaseTableName, hbaseTableRowKeyVal,columnFamilyValueList);
  				
            	columnFamilyValueList = null;
            	progressCount++;
            	  
            	logger.info("Current import rowkkey is " + hbaseTableRowKeyVal);
            }              
              
            logger.info(name + " putting back " + threadID);
            pool.releaseThread(threadID);
        }
        catch (Exception e){
        	logger.error("Parallel executing failure on threadID " + threadID);
        	logger.error(e.getMessage());
        }
	}
	
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
	
	public int getThreadID() {
		return threadID;
	}

	public void importFromDatabase(){
		
		start_time 			= System.currentTimeMillis();		//Importing start time
		int parallelTasks = 0;
		try{
			Statement stmt = con.createStatement();
			ResultSet rs = stmt.executeQuery("select count(*) from " + tableName);
			while(rs.next()){
				totalRecordCount = rs.getLong(1);
			}
			
			rs.close();
			parallelTasks = (int)(totalRecordCount / RecordCountPerThread);
			if(totalRecordCount % RecordCountPerThread != 0){
				parallelTasks++;
			}
			
			logger.info("+++++++++++++++++++++++Importing progress starting++++++++++++++++++++++++++");
			logger.info("Start to import data, plese wait!!");
			System.out.println("Start to import data, plese wait!!");
			System.out.println("It will import total " + totalRecordCount + " record into HBase.");
			logger.info("It will import total " + totalRecordCount + " record into HBase.");
			System.out.println("####--------------------------------------------------####");
			System.out.print("..0%");
			
			ExecutorService executor = Executors.newFixedThreadPool(ThreadPool.MAX_AVAILABLE);
			//ExecutorService[] executors = new ExecutorService[10];
			OracleImportHandler oracleImportHandler = null;
			progressCount=0;
			progressNumber=0;
			for(int parallel_thread_id=0;parallel_thread_id <parallelTasks;parallel_thread_id++){
				String temppstmtStr = "Select t.rowid, t.* from " + tableName + " t," + 
	  					"(select rid from (select rowid rid, rownum rn from " + tableName + 
	  					" where rownum <= " + (parallel_thread_id + 1)* RecordCountPerThread + 
	  					" ) where rn > " + parallel_thread_id * RecordCountPerThread + 
	  					" ) b where t.rowid = b.rid";
				//executors[parallel_thread_id % ThreadPool.MAX_AVAILABLE] = 
				//		Executors.newSingleThreadExecutor();
				//executors[parallel_thread_id % executors.length] =
				//	Executors.newFixedThreadPool(ThreadPool.MAX_AVAILABLE / executors.length);
				oracleImportHandler = new OracleImportHandler(temppstmtStr);
				executor.execute(oracleImportHandler);
				//executors[parallel_thread_id % ThreadPool.MAX_AVAILABLE].execute(oracleImportHandler);
				//executors[parallel_thread_id % executors.length].execute(oracleImportHandler);
			}
			executor.shutdown();
			/*for(int i=0;i<executors.length;i++){
				if(executors[i]!= null){
					executors[i].shutdown();
				}
			}*/
			while(!executor.isTerminated()){
			//while(true){
				int tempProgressNumber = (int)((progressCount/totalRecordCount) * 100);
				//System.out.println("progressCount = " + progressCount);
				//System.out.println("totalRecordCount = " + totalRecordCount);
				//System.out.println("progressNumber = " + progressNumber);
				//System.out.println("tempprogressNumber = " + tempProgressNumber);
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
				/*boolean isTerminated = true;
				for(int i=0;i<executors.length;i++){
					if(executors[i]!=null && !executors[i].isTerminated()){
						isTerminated = false;
					}
				}
				if(isTerminated){
					break;
				}*/
			}

			System.out.println("100%");
			end_time = System.currentTimeMillis();
			System.out.println("Finish importing total " + (totalRecordCount - importingFailureCount) + 
					" record to HBase, it is " + importingFailureCount + " records import failure, it takes : " + (end_time - start_time) + "ms");
			logger.info("Finish importing total " + (totalRecordCount - importingFailureCount) + 
					" record to HBase, it is " + importingFailureCount + " records import failure, it takes : " + (end_time - start_time) + "ms");
			logger.info("+++++++++++++++++++++++Importing progress complete++++++++++++++++++++++++++");
		}catch(Exception e){
			logger.error("Import from database failure!!");
			logger.error(e.getMessage());
		}
				
	}
	
	public String getPstmtStr() {
		return pstmtStr;
	}

	public void setPstmtStr(String pstmtStr) {
		this.pstmtStr = pstmtStr;
	}

	public void importByMappingFile(){ 
		logger.info("Test");
	}
	
	public void parseArguments(String[] args){
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
				setHBaseTableReplace(true);
			}else if(args[loc].equals("-hbasetablerowkey")){
				setHBaseTableRowKey(args[++loc]);
			}else if(args[loc].equals("-columnfamilymappingfile")){
				setColumnFamilyByFile(args[++loc]);
			}else if(args[loc].equals("-hbasetableappend")){
				setHBaseTableAppend(true);
			}else if(args[loc].equals("-hbasesinglecolumnfamily")){
				setHBaseSingleColumnFamily(args[++loc]);
			}else if(args[loc].equals("-recordcountperthread")){
				OracleImportHandler.RecordCountPerThread = new Integer(args[++loc]).intValue();
			}
		}
		
		if(DBUrl == null){
			System.out.println("Please provide source database connection url!!");
			logger.error("Please provide source database connection url!!");
			importHelper();
			System.exit(0);
		}
		if(userName == null || password == null){
			System.out.println("Please provide source database's username or password  !!");
			logger.error("Please provide source database's username or password  !!");
			importHelper();
			System.exit(0);
		}
		if(tableName == null){
			System.out.println("Please provide source database table name !!");
			logger.error("Please provide source database table name !!");
			importHelper();
			System.exit(0);
		}
		if(hbaseUrl == null){
			System.out.println("Please provide target HBase connection url!!");
			logger.error("Please provide target HBase connection url!!");
			importHelper();
			System.exit(0);
		}
		if(hbaseTableName == null){
			System.out.println("Please provide target HBase Table Name!!");
			logger.error("Please provide target HBase Table Name!!");
			importHelper();
			System.exit(0);
		}
		if(hbaseTableRowKey == null && !importByMappingFile){
			System.out.println("Please provide target HBase Table Row Key or Column Family Mapping File !!");
			logger.error("Please provide target HBase Table Row Key or Column Family Mapping File !!");
			importHelper();
			System.exit(0);
		}
	}
	
	public void checkImportData(){
		String 		createTableAns = "N",						//Flag control to create target hbase table  
					dropTableAns = "N", 						//Flag control to drop target hbase table
					appDataAns = "N";							//Flag control to append data to hbase table
		
		hbaseOperator 		= new HBaseOperator(hbaseUrl);	//Initial hbase manipulation program
		PreparedStatement pstmt 		= null;	
		ResultSetMetaData rsMetaData 	= null;				//Retrieve Table Meta data
				
		try {
			
			DatabaseConnectionHandler DBConnHandler = new DatabaseConnectionHandler();
			con = DBConnHandler.getConnection(DBUrl, userName, password);
			
			scan = new Scanner(System.in);
			scan.useDelimiter("\n");
			if(hbaseOperator.isTableExist(hbaseTableName) && !hbaseTableReplace && !hbaseTableAppend){
				//The target hbase table is exist, confirm to drop it or not
				System.out.println("The target HBase Table is exist, do you want to drop it first ?(Y/N)");
				dropTableAns = scan.next();
				
				if(dropTableAns.indexOf("Y") >= 0 || dropTableAns.indexOf("y") >= 0){
					//drop exist hbase table
					hbaseOperator.dropTable(hbaseTableName);
				}else{
					//Does source data append to exist hbase table ?
					System.out.println("The target HBase Table is exist, do you want to append data to target hbase table ?(Y/N)");
					appDataAns = scan.next();
				}
			}else if(!hbaseOperator.isTableExist(hbaseTableName) && !hbaseTableReplace && !hbaseTableAppend){
				//The target hbase table is not exist, confirm to create a new one
				System.out.println("The target HBase Table doesn't exist, do you want to create it ?(Y/N)");
				createTableAns = scan.next();
			}else {
				if(hbaseOperator.isTableExist(hbaseTableName) && !hbaseTableAppend){
					//Drop exist hbase table via argument
					hbaseOperator.dropTable(hbaseTableName);
				}else if(!hbaseOperator.isTableExist(hbaseTableName) || hbaseTableReplace){
					//Does it need to create hbase table 
					createTableAns = "Y";
				}
			}
			
			//Retrieve source Database table metadata
			if(DBUrl.indexOf("oracle") > 0 && hbaseTableRowKey.indexOf("rowid") >=0){
				//The database is oracle with source table's rowid
				pstmt = con.prepareStatement("Select rowid, t.* from " + tableName + " t ");
				rsMetaData = pstmt.getMetaData();
			} else {
				//The database is oracle without source table's rowid and source database is non-oracle 
				pstmt = con.prepareStatement("Select * from " + tableName );
				rsMetaData = pstmt.getMetaData();
			}
			
			fieldNames = new String[rsMetaData.getColumnCount()-1];
			fieldTypes = new String[rsMetaData.getColumnCount()-1];
			
			//Check RowKey is correct field
			boolean rowKeyIsExist = false;
			for(int colIdx=0;colIdx<rsMetaData.getColumnCount();colIdx++){
				if(hbaseTableRowKey.toUpperCase().equals(rsMetaData.getColumnName(colIdx+1).toUpperCase())){
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
				if(!hbaseTableRowKey.toUpperCase().equals(rsMetaData.getColumnName(colIdx+1).toUpperCase())){
					fieldNames[fieldNameLoc] = rsMetaData.getColumnName(colIdx+1);
					fieldTypes[fieldNameLoc] = rsMetaData.getColumnTypeName(colIdx+1);
					fieldNameLoc++;
				}
			}
			
			if(!hbaseOperator.isTableExist(hbaseTableName) || 
					(createTableAns.indexOf("Y") >=0 || createTableAns.indexOf("y") >= 0)){
				//Create HBase Table
				if(hbaseSingleColumnFamily == null){
					hbaseOperator.createTable(hbaseTableName, fieldNames);
				}else{
					hbaseOperator.createTable(hbaseTableName, hbaseSingleColumnFamily);
				}
			}else if (!hbaseTableAppend && (appDataAns.indexOf("N") >= 0 || appDataAns.indexOf("n") >= 0)){
				if(hbaseOperator.isTableExist(hbaseTableName)){
					System.out.println("Please confirm exists target HBase table first before using this import tool!!");
					logger.info("Please confirm exists target HBase table first before using this import tool!!");
				}else{
					System.out.println("Please create target HBase table first before using this import tool!!");
					logger.info("Please create target HBase table first before using this import tool!!");
				}
				System.exit(0);
			}
		}catch(Exception e){
			logger.error("Check import data failure!!");
			logger.error(e.getMessage());
		}
	}
	
	public void execute(String[] args){
		
		parseArguments(args);
		checkImportData();		
		if(this.importByMappingFile){
			importByMappingFile();
		}else{
			importFromDatabase();
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
	
	private void setColumnFamilyByFile(String FileName){
		
		logger.info("Process Mapping file!!");
		String sCurrentLine;

		try (BufferedReader br = new BufferedReader(new FileReader(FileName)))
		{
			while ((sCurrentLine = br.readLine()) != null) {
				if(sCurrentLine.indexOf("rowkey")>=0){
					hbaseTableRowKey = sCurrentLine.substring(0, sCurrentLine.indexOf(" "));
				}else if(sCurrentLine.indexOf("#") < 0){
					ColumnFamilyMapping columnFamilyMapping = 
						new ColumnFamilyMapping(sCurrentLine.substring(0, sCurrentLine.indexOf(" ")),
								sCurrentLine.substring(sCurrentLine.indexOf(" ")+1, sCurrentLine.indexOf(":")), 
								sCurrentLine.substring(sCurrentLine.indexOf(":")+1, sCurrentLine.length()));
								columnFamilyMappingList.add(columnFamilyMapping);
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
	
	public static String getDBUrl() {
		return DBUrl;
	}

	public static void setDBUrl(String dBUrl) {
		DBUrl = dBUrl;
	}

	public static String getUserName() {
		return userName;
	}

	public static void setUserName(String userName) {
		OracleImportHandler.userName = userName;
	}

	public static String getPassword() {
		return password;
	}

	public static void setPassword(String password) {
		OracleImportHandler.password = password;
	}

	public static String getTableName() {
		return tableName;
	}

	public static void setTableName(String tableName) {
		OracleImportHandler.tableName = tableName;
	}

	public static String getHBaseUrl() {
		return hbaseUrl;
	}

	public static void setHBaseUrl(String hbaseUrl) {
		OracleImportHandler.hbaseUrl = hbaseUrl;
	}

	public static String getHBaseTableName() {
		return hbaseTableName;
	}

	public static void setHBaseTableName(String hbaseTableName) {
		OracleImportHandler.hbaseTableName = hbaseTableName;
	}

	public boolean isHBaseTableReplace() {
		return hbaseTableReplace;
	}

	public void setHBaseTableReplace(boolean hbaseTableReplace) {
		this.hbaseTableReplace = hbaseTableReplace;
	}

	public boolean isHBaseTableAppend() {
		return hbaseTableAppend;
	}

	public void setHBaseTableAppend(boolean hbaseTableAppend) {
		this.hbaseTableAppend = hbaseTableAppend;
	}

	public static String getHBaseTableRowKey() {
		return hbaseTableRowKey;
	}

	public static void setHBaseTableRowKey(String hbaseTableRowKey) {
		OracleImportHandler.hbaseTableRowKey = hbaseTableRowKey;
	}

	public boolean isImportByMappingFile() {
		return importByMappingFile;
	}

	public void setImportByMappingFile(boolean importByMappingFile) {
		this.importByMappingFile = importByMappingFile;
	}

	public static String getHBaseSingleColumnFamily() {
		return hbaseSingleColumnFamily;
	}

	public static void setHBaseSingleColumnFamily(String hbaseSingleColumnFamily) {
		OracleImportHandler.hbaseSingleColumnFamily = hbaseSingleColumnFamily;
	}

	public static void main(String[] args){
		OracleImportHandler oracleImportHandler = new OracleImportHandler();
		oracleImportHandler.execute(args);
		System.exit((int) (new Long(oracleImportHandler.totalRecordCount).intValue()-oracleImportHandler.importingFailureCount));
	}

}
