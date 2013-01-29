package com.bizosys.oneline.common;

import java.io.FileInputStream;
import java.util.ArrayList;
import java.util.List;

import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import org.xml.sax.Attributes;

public class SyncTableXMLContentHandler extends XMLContentHandler
{
	private SyncTable syncTable;
	private static List<SyncTable> syncTables = new ArrayList<SyncTable>();
	
	public static void main(String[] args)
	{
		try 
		{
			parseXML("F:\\work\\JavaLabs\\DatabaseUtil.xml");
		} 
		catch (Exception e) 
		{			
			e.printStackTrace();
		}
	}
	
	public static List<SyncTable> parseXML(String filePath) throws Exception
	{
		SAXParser parser = SAXParserFactory.newInstance().newSAXParser();
		SyncTableXMLContentHandler handler = new SyncTableXMLContentHandler();
		parser.setProperty("http://xml.org/sax/properties/lexical-handler", handler);		
		parser.parse(new FileInputStream(filePath), handler);
		return syncTables;
	}
	
	public SyncTable getSyncTable() {
		return syncTable;
	}

	protected XMLElement createElement(XMLElement parent, String name, Attributes attributes) throws Exception 
	{		
		XMLElement element = null;
		if( name.compareToIgnoreCase("synctable") == 0 )
			element = newXMLElement(parent, createSyncTable(attributes));
		else if( name.compareToIgnoreCase("synccolumn") == 0 )
			element = newXMLElement(parent, createSyncColumn((SyncTable)parent.value(), attributes));
		else if( name.compareToIgnoreCase("tablelist") == 0 )
			element = nullXMLElement(parent);
//		System.out.println("Name is: " +name);
		
		return element;
	}

	protected void processText(XMLElement parentElement, XMLElement element, String name, String str) throws Exception 
	{	
	}
	
	protected void processCDATA(XMLElement parentElement, XMLElement element, String str) throws Exception {		
	}

	private SyncTable createSyncTable(Attributes attributes)
	{
//		System.out.println("Creating Sync Table: " +attributes.getValue("className"));
		syncTable = new SyncTable();
		syncTable.setTableType(attributes.getValue("type"));
		syncTable.setName(attributes.getValue("name"));
		syncTable.setClassName(attributes.getValue("className"));
		syncTables.add(syncTable);
		return syncTable;
	}
	
	private SyncColumn createSyncColumn(SyncTable table, Attributes attributes)
	{
		String name = attributes.getValue("name");
		String variableName = attributes.getValue("variableName");
		
		SyncColumn sColumn = new SyncColumn(name, variableName);
		syncTable.addColumn(sColumn);
		
		return sColumn;
	}
	
}