package com.bizosys.oneline.common;

public class SyncColumn 
{
	private String name;
	private String variableName;
	
	public SyncColumn(String name, String vName)
	{
		this.name = name;
		this.variableName = vName;
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
	 * @return the variableName
	 */
	public String getVariableName() {
		return variableName;
	}
	
	/**
	 * @param variableName the variableName to set
	 */
	public void setVariableName(String variableName) {
		this.variableName = variableName;
	}
}
