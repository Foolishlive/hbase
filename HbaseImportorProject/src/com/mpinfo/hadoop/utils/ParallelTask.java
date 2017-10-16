package com.mpinfo.hadoop.utils;

import java.sql.*;
import java.util.*;

import org.apache.log4j.Logger;

import com.mpinfo.database.utils.*;
import com.mpinfo.hadoop.hbase.*;

public class ParallelTask {
	
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
	
	public static void main(String[] args){
		String tableName = "emp";
		int totalRecordCount=16;
		int sparate_count = 0;
		Connection con = null;
		PreparedStatement pstmt = null;
		Statement stmt = null;
		
		try{
		DatabaseConnectionHandler DBConnHandler = new DatabaseConnectionHandler();
		con = DBConnHandler.getConnection("jdbc:oracle:thin:@192.168.200.213:1521:DB11G","scott","scott");
		stmt = con.createStatement();
		ResultSet rs = stmt.executeQuery("select count(*) from employee");
		while(rs.next()){
			totalRecordCount = rs.getInt(1);
		}
		rs.close();
		
		sparate_count = totalRecordCount / 50000;
		
		pstmt = con.prepareStatement("Select t.rowid, t.* from " + tableName + " t," + 
					"(select rid from (select rowid rid, rownum rn from " + tableName + 
					" where rownum <= ? ) where rn > ? ) b where t.rowid = b.rid");
		
		if(totalRecordCount%500000 !=0 ){
			sparate_count++;
		}
		
		for(int parallel_thread_id=0;parallel_thread_id <sparate_count;parallel_thread_id++){
			pstmt.setInt(1, (parallel_thread_id + 1) * 50000);
			pstmt.setInt(2, parallel_thread_id * 50000);
			rs = pstmt.executeQuery();
			while(rs.next()){
				System.out.println(rs.getString(1));
			}
			rs.close();
		}
		
		}catch(Exception e){
			e.printStackTrace();
		}
	}
}
