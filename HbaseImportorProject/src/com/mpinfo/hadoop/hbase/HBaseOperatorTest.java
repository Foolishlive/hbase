package com.mpinfo.hadoop.hbase;

import java.text.DecimalFormat;
import java.text.NumberFormat;

import org.apache.log4j.Logger;

public class HBaseOperatorTest {

	private static Logger logger = 
			Logger.getLogger(HBaseOperator.class.getName());
	
	public static void main(String[] args) throws Exception {
		
		HBaseOperator hbaseOperator = new HBaseOperator();
		//Create Table
		hbaseOperator.createTable("Employee", new String[]{"EMPNO","ENAME","JOB","MGR","HIREDATE","SAL","COMM","DEPTNO","CREATE_DATE"});
		//Insert Data
		long start_time, end_time;
		start_time = System.currentTimeMillis();
		NumberFormat formatter = new DecimalFormat("0000");
		for(int i = 0;i < 1;i++){
			hbaseOperator.insertData("Employee", "row-"+formatter.format(i+1), "EMPNO","val", "MP0147");
			hbaseOperator.insertData("Employee", "row-"+formatter.format(i+1), "ENAME", "val","Edward");
			hbaseOperator.insertData("Employee", "row-"+formatter.format(i+1), "JOB", "val","Manager");
			hbaseOperator.insertData("Employee", "row-"+formatter.format(i+1), "MGR", "val","none");
			hbaseOperator.insertData("Employee", "row-"+formatter.format(i+1), "HIREDATE", "val","2004");
			hbaseOperator.insertData("Employee", "row-"+formatter.format(i+1), "SAL", "val","60000");
			hbaseOperator.insertData("Employee", "row-"+formatter.format(i+1), "COMM", "val","TEST");
			hbaseOperator.insertData("Employee", "row-"+formatter.format(i+1), "DEPTNO", "val","TEST");
			hbaseOperator.insertData("Employee", "row-"+formatter.format(i+1), "CREATE_DATE", "val","TEST");
		}
		hbaseOperator.changeColumnFamily("Employee", new String[]{"EMPNO","ENAME","JOB","MGR","HIREDATE","SAL","COMM","DEPTNO","CREATE_DATE"});//,"Test"});
		end_time = System.currentTimeMillis();
		logger.info("Total execution time : " + (end_time - start_time) + "ms");
		//Query all Data
		hbaseOperator.queryAll("Employee");
		//query row id
		hbaseOperator.queryByRowId("Employee", "row-0001");
		//query data by condition
		//queryByCondition("t_table", "f1","a", "eeeeee");
		//query multipal condition
		//queryByConditions("t_table", new String[]{"f1","f3"},new String[]{"a","c"}, new String[]{"fffaaa","fffccc"});
		//delete record
		//deleteRow("t_table", "row-0001");
		//drop table
		//dropTable("t_table");
		hbaseOperator = null;
	}

}
