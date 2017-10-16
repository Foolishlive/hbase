package com.mpinfo.hadoop.hbase;

/**
 * This ColumnFamilyMapping implements for mapping database field to ColumnFamily and it's Qualifier. 
 * @author Edward Chen
 *
 */
public class ColumnFamilyMapping {
	
	private String dbFieldName = null;
	private String columnFamilyName = null;
	private String qualifierName = null;
	
	/**
	 * To construct this mapping class
	 * @param dbFieldName		Source database table field name
	 * @param columnFamilyName	Target hbase table column family name
	 * @param qualifierName		Target hbase table column family qualifier Name
	 */
	public ColumnFamilyMapping(String dbFieldName, String columnFamilyName, String qualifierName){
		this.dbFieldName = dbFieldName;
		this.columnFamilyName = columnFamilyName;
		this.qualifierName = qualifierName;
	}
	
	/**
	 * Retrieve database field name
	 * @return String
	 */
	public String getDbFieldName() {
		return dbFieldName;
	}
	
	/**
	 * Assign database field Name
	 * @param dbFieldName
	 */
	public void setDbFieldName(String dbFieldName) {
		this.dbFieldName = dbFieldName;
	}
	
	/**
	 * Retrieve ColumnFamily Name
	 * @return String
	 */
	public String getColumnFamilyName() {
		return columnFamilyName;
	}
	
	/**
	 * Assign Column Family Name
	 * @param columnFamilyName
	 */
	public void setColumnFamilyName(String columnFamilyName) {
		this.columnFamilyName = columnFamilyName;
	}
	
	/**
	 * Retrieve Column Family's Qualifier Name
	 * @return String
	 */
	public String getQualifierName() {
		return qualifierName;
	}
	
	/**
	 * Assign Column Family's Qualifier Name
	 * @param qualifierName
	 */
	public void setQualifierName(String qualifierName) {
		this.qualifierName = qualifierName;
	}

}
