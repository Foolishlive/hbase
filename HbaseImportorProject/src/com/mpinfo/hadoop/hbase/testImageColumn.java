package com.mpinfo.hadoop.hbase;

import java.io.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

public class testImageColumn {
	
	private static BufferedOutputStream out;

	public static void main(String[] args) throws Exception {
		
		if(args.length == 0){
			System.out.println("Here is some help for this importor tool:");
			System.out.println("You must provide following arguments:");
			System.out.println("\t-filepath :\t\t\t source image files path, for example: /home/user/");
			System.out.println("\t-hbaseurl :\t\t\t target hbase connection url ");
			System.out.println("\t-hbasetablename :\t\t target hbase table name ");
			System.out.println("\t-hbasetablecolumnfamily : \t Specific column family in HBase Table");
			System.out.println("\t-hbasetablecolumnqualifier : \t Specific column qualifier in HBase Table");
			System.out.println("\t-rowkeyreplacesign : \t\t The sign in hbase rokey");
			System.out.println("\t-rowkeyreplacedbysign : \t The sign replace sign in thefile name");
			System.exit(-1);
		}
		
		String hbaseUrl = null,
			   hbaseTableName = null,
			   hbaseColumnQualifier = null,
			   filePath = null,
			   rowKeyReplaceSign = null,
			   rowKeyReplacedBySign = null;
		
		//Parsing arguments
		for(int loc = 0;loc < args.length;loc++){
			if(args[loc].equals("-filepath")){
				filePath = args[++loc];
			}else if(args[loc].equals("-hbaseurl")){
				hbaseUrl = args[++loc];
			}else if(args[loc].equals("-hbasetablename")){
				hbaseTableName = args[++loc];
			}else if(args[loc].equals("-hbasecolumnqualifier")){
				hbaseColumnQualifier = args[++loc];
			}else if(args[loc].equals("-rowkeyreplacesign")){
				rowKeyReplaceSign = args[++loc];
			}else if(args[loc].equals("-rowkeyreplacedbysign")){
				rowKeyReplacedBySign = args[++loc];
			}
		}
				
		Configuration configuration = null;					//HBase connection configuration
		Connection connection = null;						//HBase connection
		configuration = HBaseConfiguration.create();
		configuration.set("hbase.zookeeper.quorum", hbaseUrl);
		try {
			connection = ConnectionFactory.createConnection(configuration);
		} catch (IOException e) {
			System.out.println("Can't connect to HBase zookeeper !! Please checking hbase zookeeper is in running status!!");
			e.printStackTrace();
		}
		Table table = connection.getTable(TableName.valueOf(hbaseTableName));
		ResultScanner rs = null;
		try {
			rs = table.getScanner(new Scan());
			for (Result r : rs) {
				for (Cell keyValue : r.rawCells()) {
					if(new String(CellUtil.cloneQualifier(keyValue)).toUpperCase()
							.equals(hbaseColumnQualifier.toUpperCase())){
						String fileName = Bytes.toString(r.getRow());
						
						while(rowKeyReplaceSign != null && 
								fileName.indexOf(rowKeyReplaceSign)>=0){
							fileName = fileName.substring(0, fileName.indexOf(rowKeyReplaceSign)) + rowKeyReplacedBySign +
									fileName.substring(fileName.indexOf(rowKeyReplaceSign)+1, fileName.length());
						}
				
						out = new BufferedOutputStream(new FileOutputStream(
								filePath + "/" + fileName+".gif"));
					    out.write(CellUtil.cloneValue(keyValue));
						
					}
				}
			}
			rs.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}
