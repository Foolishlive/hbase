package com.mpinfo.hadoop.hbase;

import java.io.*;
import java.util.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.CompareFilter.*;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.*;
import org.apache.log4j.Logger;

public class HBaseOperator {
	
	private static Logger logger = 
			Logger.getLogger(HBaseOperator.class.getName());
	private Configuration configuration = null;					//HBase connection configuration
	private Connection connection = null;						//HBase connection
		
	public HBaseOperator(){
		
	}
		
	/**
	 * Initialize instance and assign HBase's zookeeper service port
	 * @param zookeeperQuorm	HBase Zoo Keeper URL
	 */
	public HBaseOperator(String zookeeperQuorm){
		configuration = HBaseConfiguration.create();
		logger.info("hbase.zookeeper.quorum = " + zookeeperQuorm);
		configuration.set("hbase.zookeeper.quorum", zookeeperQuorm);
		//configuration.set("zookeeper.znode.parent","/hbase-unsecure");
		try {
			connection = ConnectionFactory.createConnection(configuration);
		} catch (IOException e) {
			logger.error("Can't connect to HBase zookeeper !! Please checking hbase zookeeper is in running status!!");
			logger.error(e.getMessage());
		}
	}
	
	/**
	 * Initialize instance via properties 
	 * @param prop HBase connection information
	 */
	public HBaseOperator(Properties prop) {
		configuration = HBaseConfiguration.create();
		logger.info("hbase.zookeeper.quorum = " + prop.getProperty("hbase.zookeeper.quorum"));
		configuration.set("hbase.zookeeper.quorum", prop.getProperty("hbase.zookeeper.quorum"));
		try {
			connection = ConnectionFactory.createConnection(configuration);
		} catch (IOException e) {
			logger.error("Can't connect to HBase zookeeper !! Please checking hbase zookeeper is in running status!!");
			logger.error(e.getMessage());
		}
	}
	
	/**
	 * create table
	 * To Create a new HBase Table
	 * @param tableName		Table Name
	 */
	public void changeColumnFamily(String tableStr, String[] familyNames) {
		
		HTableDescriptor tableDescriptor = null;
		HColumnDescriptor[] hColumnDescriptors = null;
		try {
			Admin admin = connection.getAdmin();
			TableName tableName = TableName.valueOf(tableStr);
			
			tableDescriptor = admin.getTableDescriptor(tableName);
			hColumnDescriptors = tableDescriptor.getColumnFamilies();
			// insert data to hbase
			if (familyNames != null && familyNames.length > 0 
					&& familyNames.length > hColumnDescriptors.length) {
				logger.info("add new column family to " + tableStr + "......");
				for (String familyName : familyNames) {
					if(familyName == null){
						continue;
					}					
					if(!tableDescriptor.hasFamily(familyName.getBytes())){
						admin.addColumn(tableName, new HColumnDescriptor(familyName));
					}
				}
				logger.info("end add column family ......");
			}else if(familyNames != null && familyNames.length > 0 
					&& familyNames.length < hColumnDescriptors.length){
				logger.info("delete column family from " + tableStr + "......");
				for(HColumnDescriptor hColumnDescriptor : hColumnDescriptors){
					if(hColumnDescriptor == null){
						continue;
					}
					int loc;
					for(loc = 0;loc < familyNames.length;loc++){
						if(familyNames[loc].equals(hColumnDescriptor.getNameAsString())){
							break;
						}
					}
					if(loc==familyNames.length){
						admin.deleteColumn(tableName, hColumnDescriptor.getName());
					}
				}
				logger.info("end delete column family ......");
			}
			admin.close();
		} catch (Exception e) {
			if (familyNames != null && familyNames.length > 0 
					&& familyNames.length > hColumnDescriptors.length) {
				logger.error("To add a new column family failure!!");
			}else if(familyNames != null && familyNames.length > 0 
					&& familyNames.length < hColumnDescriptors.length){
				logger.error("To remove a column family failure!!");
			}
			logger.error(e.getMessage());
		}
		
	}
	
	public boolean isTableExist(String tableStr){
		Admin admin = null;
		TableName tableName = null;
		try{
			admin = connection.getAdmin();
			tableName = TableName.valueOf(tableStr);
			return admin.tableExists(tableName);
		}catch(Exception e){
			logger.error("To check hbase status failure!!");
			logger.error(e.getMessage());
		}
		return false;
	}
	
	/**
	 * create table
	 * 
	 * @param tableName
	 */
	public void createTable(String tableStr, String[] familyNames) {
		logger.info("start create table ......");
		try {
			Admin admin = connection.getAdmin();
			TableName tableName = TableName.valueOf(tableStr);
			HTableDescriptor tableDescriptor = new HTableDescriptor(tableName);
			// create hbase table metadata
			if (familyNames != null && familyNames.length > 0) {
				for (String familyName : familyNames) {
					tableDescriptor.addFamily(new HColumnDescriptor(familyName));
				}
			}
			admin.createTable(tableDescriptor);
			admin.close();
		} catch (Exception e) {
			logger.error("To create hbase status failure!!");
			logger.error(e.getMessage());
		} 
		logger.info("end create table ......");
	}
	
	/**
	 * create table
	 * 
	 * @param tableName
	 */
	public void createTable(String tableStr, String familyName) {
		logger.info("start create table ......");
		try {
			Admin admin = connection.getAdmin();
			TableName tableName = TableName.valueOf(tableStr);
			HTableDescriptor tableDescriptor = new HTableDescriptor(tableName);
			// create hbase table metadata
			if (familyName != null) {
				tableDescriptor.addFamily(new HColumnDescriptor(familyName));
			}
			admin.createTable(tableDescriptor);
			admin.close();
		} catch (Exception e) {
			logger.error("To create hbase status failure!!");
			logger.error(e.getMessage());
		} 
		logger.info("end create table ......");
	}
		
	/**
	 * insert one qualifier data into hbase
	 * 
	 * @param tableName
	 * @throws Exception
	 */
	public void insertData(String tableName, String rowId, String familyName,String qualifier, String value) throws Exception {
		Table table = connection.getTable(TableName.valueOf(tableName));
		Put put = new Put(rowId.getBytes());  // To insert data to hbase, it only support one cell add into hbase table
		put.addColumn(familyName.getBytes(), qualifier.getBytes(), value.getBytes());
		try {
			table.put(put);
		} catch (IOException e) {
			logger.error("To insert data into hbase table failure!!");
			logger.error(e.getMessage());
		}
	}
	
	/**
	 * insert one qualifier data into hbase
	 * 
	 * @param tableName
	 * @throws Exception
	 */
	public void insertData(String tableName, String rowId, String familyName,String qualifier, byte[] value) throws Exception {
		Table table = connection.getTable(TableName.valueOf(tableName));
		Put put = new Put(rowId.getBytes());  // To insert data to hbase, it only support one cell add into hbase table
		put.addColumn(familyName.getBytes(), qualifier.getBytes(), value);
		try {
			table.put(put);
		} catch (IOException e) {
			logger.error("To insert data into hbase table failure!!");
			logger.error(e.getMessage());
		}
	}
	
	/**
	 * insert one rowkey data into hbase
	 */
	public void insertRowData(String tableName, String rowId, ArrayList<ColumnFamilyValue> columnFamilyValueList){
		try {
			Table table = connection.getTable(TableName.valueOf(tableName));

			Put put= new Put(rowId.getBytes());
			for(int idx=0;idx < columnFamilyValueList.size();idx++){
				put.addColumn(((ColumnFamilyValue)columnFamilyValueList.get(idx)).getColumnFamilyName().getBytes(), 
						((ColumnFamilyValue)columnFamilyValueList.get(idx)).getQualifierName().getBytes(),
						((ColumnFamilyValue)columnFamilyValueList.get(idx)).getValue());
			}
			table.put(put);
		} catch (Exception e) {
			logger.error("Importing data failure!! Cause by :");
			logger.error(e.getMessage());
			logger.error("Failure insert record key is " + rowId);
		
		}		
	}
	
	/**
	 * update one record into hbase
	 * 
	 * @param tableName
	 * @throws Exception
	 */
	public void updateData(String tableName, String rowId, String familyName,String qualifier, String value) throws Exception {
		Table table = connection.getTable(TableName.valueOf(tableName));
		Put put = new Put(rowId.getBytes()); // To update date to hbase, it only support append a new data to cell
		put.addColumn(familyName.getBytes(), qualifier.getBytes(), value.getBytes());
		try {
			table.put(put);
		} catch (IOException e) {
			logger.error("To update a data from hbase table failure!!");
			logger.error(e.getMessage());
		}
	}
	
	/**
	 * update one record into hbase
	 * 
	 * @param tableName
	 * @throws Exception
	 */
	public void updateData(String tableName, String rowId, String familyName,String qualifier, byte[] value) throws Exception {
		Table table = connection.getTable(TableName.valueOf(tableName));
		Put put = new Put(rowId.getBytes()); // To update date to hbase, it only support append a new data to cell
		put.addColumn(familyName.getBytes(), qualifier.getBytes(), value);
		try {
			table.put(put);
		} catch (IOException e) {
			logger.error("To update a data from hbase table failure!!");
			logger.error(e.getMessage());
		}
	}
	
		/**
	 * drop one record
	 * 
	 * @param tablename
	 * @param rowkey
	 */
	public void deleteRow(String tablename, String rowkey) {
		try {
			Table table = connection.getTable(TableName.valueOf(tablename));
			Delete d1 = new Delete(rowkey.getBytes());
			table.delete(d1); // To delete data from hbase table, it support to delete the same row key data
		} catch (IOException e) {
			logger.error("To delete a data from hbase table failure!!");
			logger.error(e.getMessage());
		}
	}
		/**
	 * query all data
	 * 
	 * @param tableName
	 * @throws Exception
	 */
	public ResultScanner queryAll(String tableName) throws Exception {
		Table table = connection.getTable(TableName.valueOf(tableName));
		ResultScanner rs = null;
		try {
			rs = table.getScanner(new Scan());
			for (Result r : rs) {
				for (Cell keyValue : r.rawCells()) {
					logger.info("Row ID : " + new String(CellUtil.cloneFamily(keyValue))+":"+new String(CellUtil.cloneQualifier(keyValue)) + "====嚙踝蕭:" + new String(CellUtil.cloneValue(keyValue)));
				}
			}
			rs.close();
		} catch (IOException e) {
			logger.error("To Query all data from hbase table failure!!");
			e.printStackTrace();
		}
		return rs;
	}

	/**
	 * Query by rowId
	 * 
	 * @param tableName
	 * @throws Exception
	 */
	public Result queryByRowId(String tableName, String rowId) throws Exception {
		Table table = connection.getTable(TableName.valueOf(tableName));
		Result r = null;
		try {
			Get scan = new Get(rowId.getBytes());// query by rowid
			r = table.get(scan);
			logger.info("rowkey:" + new String(r.getRow()));
			for (Cell keyValue : r.rawCells()) {
				logger.info("ROW ID : " + new String(CellUtil.cloneFamily(keyValue))+":"+new String(CellUtil.cloneQualifier(keyValue)) + "====嚙踝蕭:" + new String(CellUtil.cloneValue(keyValue)));
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		return r;
	}

	/**
	 * Query by one condition
	 * 
	 * @param tableName
	 */
	public ResultScanner queryByCondition(String tableName, String familyName,String qualifier,String value) {
		ResultScanner rs = null;
		try {
			Table table = connection.getTable(TableName.valueOf(tableName));
			Filter filter = new SingleColumnValueFilter(Bytes.toBytes(familyName), Bytes.toBytes(qualifier), CompareOp.EQUAL, Bytes.toBytes(value)); // ��嚙踐ⅣamilyName嚙踝蕭��蕭瞏value嚙踐ㄙ擗�蕭�貔�蕭
			Scan s = new Scan();
			s.setFilter(filter);
			rs = table.getScanner(s);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return rs;
	}

	/**
	 * Query by multipul Query Conditions
	 * 
	 * @param tableName
	 */
	public ResultScanner queryByConditions(String tableName, String[] familyNames, String[] qualifiers,String[] values) {
		ResultScanner rs = null;
		try {
			Table table = connection.getTable(TableName.valueOf(tableName));
			List<Filter> filters = new ArrayList<Filter>();
			if (familyNames != null && familyNames.length > 0) {
				int i = 0;
				for (String familyName : familyNames) {
					Filter filter = new SingleColumnValueFilter(Bytes.toBytes(familyName), Bytes.toBytes(qualifiers[i]), CompareOp.EQUAL, Bytes.toBytes(values[i]));
					filters.add(filter);
					i++;
				}
			}
			FilterList filterList = new FilterList(filters);
			Scan scan = new Scan();
			scan.setFilter(filterList);
			rs = table.getScanner(scan);
			for (Result r : rs) {
				logger.info("rowkey : " + new String(r.getRow()));
				for (Cell keyValue : r.rawCells()) {
					logger.info("ROW ID : " + new String(CellUtil.cloneFamily(keyValue))+":"+new String(CellUtil.cloneQualifier(keyValue)) + "====嚙踝蕭:" + new String(CellUtil.cloneValue(keyValue)));
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return rs;
	}

	/**
	 * Deop Table
	 * 
	 * @param tableName
	 */
	public void dropTable(String tableStr) {
		logger.info("start drop hbase table ......");
		try {
			Admin admin = connection.getAdmin();
			TableName tableName = TableName.valueOf(tableStr);
			admin.disableTable(tableName);
			admin.deleteTable(tableName);
			admin.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
		logger.info("end drop table ......");
	}
	
}
