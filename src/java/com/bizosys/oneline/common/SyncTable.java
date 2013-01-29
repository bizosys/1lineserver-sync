package com.bizosys.oneline.common;


public class SyncTable 
{
	public static String UP = "up";
	public static String DOWN = "down";

	public String tableName = null;
	public String syncDirection = null;
	
	public String toString() {
		return tableName + " -> " + syncDirection;
	}
}