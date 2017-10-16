package com.mpinfo.hadoop.hbase;

/**
 * This ColumnFamilyMapping implements for mapping database field to ColumnFamily and it's Qualifier. 
 * @author Edward Chen
 *
 */
public class ColumnFamilyValue {
	
	private byte[] value = null;
	private String columnFamilyName = null;
	private String qualifierName = null;
	
	/**
	 * To construct this mapping class
	 * @param dbFieldName		Source database table field name
	 * @param columnFamilyName	Target hbase table column family name
	 * @param qualifierName		Target hbase table column family qualifier Name
	 */
	public ColumnFamilyValue(byte[] value, String columnFamilyName, String qualifierName){
		this.value = value;
		this.columnFamilyName = columnFamilyName;
		this.qualifierName = qualifierName;
	}
	
	/**
	 * Retrieve value
	 * @return String
	 */
	public byte[] getValue() {
		return value;
	}
	
	/**
	 * Assign value 
	 * @param value
	 */
	public void setValue(byte[] value) {
		this.value = value;
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
