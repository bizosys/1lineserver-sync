package com.bizosys.oneline.common;

import java.util.ArrayList;
import java.util.List;

public class SyncTable 
{
	private String name;
	private String className;
	private String tableType;
	private List<SyncColumn> syncColumns;
	
	public static final String TYPE_MASTER = "Master";
	public static final String TYPE_TRANSACTION = "Transaction";
	
	public void addColumn(SyncColumn column)
	{
		if(syncColumns == null)
			syncColumns = new ArrayList<SyncColumn>();
		syncColumns.add(column);
	}
	
	/**
	 * @return the name
	 */
	public String getName() {
		return name;
	}
	
	/**
	 * @param name the name to set
	 */
	public void setName(String name) {
		this.name = name;
	}
	
	/**
	 * @return the className
	 */
	public String getClassName() {
		return className;
	}
	
	/**
	 * @param className the className to set
	 */
	public void setClassName(String className) {
		this.className = className;
	}
	
	/**
	 * @return the tableType
	 */
	public String getTableType() {
		return tableType;
	}
	
	/**
	 * @param tableType the tableType to set
	 */
	public void setTableType(String tableType) {
		this.tableType = tableType;
	}
	
	/**
	 * @return the syncColumns
	 */
	public List<SyncColumn> getSyncColumns() {
		return syncColumns;
	}
	
	/**
	 * @param syncColumns the syncColumns to set
	 */
	public void setSyncColumns(List<SyncColumn> syncColumns) {
		this.syncColumns = syncColumns;
	}
}