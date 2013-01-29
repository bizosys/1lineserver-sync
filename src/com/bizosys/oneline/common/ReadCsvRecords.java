package com.bizosys.oneline.common;

import java.io.PrintWriter;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.log4j.Logger;

import com.oneline.dao.ReadBase;

public class ReadCsvRecords<T> extends ReadBase<String> 
{
	public String docName = null; 
	public static final String READ_INSERT = "READ_INSERT";
	public static final String READ_UPDATE = "READ_UPDATE";
	public String tableName = null;
	
	private final static Logger LOG = Logger.getLogger(ReadCsvRecords.class);
	
	private PrintWriter out = null;
	private Boolean writeAttributes = Boolean.FALSE;
	private Class<T> classToFill = null;
	private String readType = null;
	
	private static final HashMap<String,String> sqlTokens;
	private static Pattern sqlTokenPattern;
	
	static
	{           
	    //MySQL escape sequences: http://dev.mysql.com/doc/refman/5.1/en/string-syntax.html
	    String[][] search_regex_replacement = new String[][]
	    {
	                //search string     search regex        sql replacement regex
	            {   "\u0000"    ,       "\\x00"     ,       "\\\\0"     },
	            {   "'"         ,       "'"         ,       "\\\\'"     },
	            {   "\""        ,       "\""        ,       "\\\\\""    },
	            {   "\b"        ,       "\\x08"     ,       "\\\\b"     },
	            {   "\n"        ,       "\\n"       ,       "\\\\n"     },
	            {   "\r"        ,       "\\r"       ,       "\\\\r"     },
	            {   "\t"        ,       "\\t"       ,       "\\\\t"     },
	            {   "\u001A"    ,       "\\x1A"     ,       "\\\\Z"     },
	            {   "\\"        ,       "\\\\"      ,       "\\\\\\\\"  }
	    };

	    sqlTokens = new HashMap<String,String>();
	    String patternStr = "";
	    for (String[] srr : search_regex_replacement)
	    {
	        sqlTokens.put(srr[0], srr[2]);
	        patternStr += (patternStr.isEmpty() ? "" : "|") + srr[1];            
	    }
	    sqlTokenPattern = Pattern.compile('(' + patternStr + ')');
	}

	public ReadCsvRecords(Class<T> classToFill)
	{
		super();
		this.classToFill = classToFill;
	}
	
	public ReadCsvRecords(PrintWriter out, Class<T> classToFill) 
	{
		this(classToFill);
		this.out = out;
	}

	public ReadCsvRecords(Boolean writeAttributes, Class<T> classToFill) 
	{
		this(classToFill);
		this.writeAttributes = writeAttributes;
	}

	public ReadCsvRecords(PrintWriter out, Boolean writeAttributes, Class<T> classToFill) 
	{
		this(classToFill);
		this.out = out;
		this.writeAttributes = writeAttributes;
	}
	
	/**
	 * 
	 * @param out
	 * @param readType Possible values READ_INSERT, READ_UPDATE
	 * @param classToFill
	 */
	public ReadCsvRecords(PrintWriter out, String readType, Class<T> classToFill) 
	{
		this(classToFill);
		this.out = out;
		this.readType = readType;
	}

	@Override
	protected List<String> populate() throws SQLException 
	{
		if ( this.rs == null ) 
		{
			throw new SQLException("Rs is not initialized.");
		}

		ResultSetMetaData md = rs.getMetaData() ;
		int totalCol = md.getColumnCount();
		String[] cols = new String[totalCol];
		int[] types = new int[totalCol];
		
		for ( int i=0; i<totalCol; i++ ) 
		{
			cols[i] = md.getColumnLabel(i+1);
			types[i] = md.getColumnType(i+1);
		}
		
		List<String> records = null;
		StringBuilder strBuf = new StringBuilder();
		String className = null;
		if ( null != docName ) className = docName;
		else className = classToFill.getName();

		if (this.out == null) 
		{
			records = new ArrayList<String>();
		}

		while (this.rs.next()) 
		{
			if (this.writeAttributes) 
			{
				this.recordAsAttributes(totalCol, cols, strBuf, className);
			} 
			else if (this.readType == READ_INSERT)
			{
				this.recordAsInsertQuery(totalCol, cols, strBuf, className);
			}
			else if (this.readType == READ_UPDATE)
			{
				this.recordAsUpdateQuery(totalCol, cols, strBuf, className);
			}
			else 
			{
				this.recordAsTags(totalCol, cols, types, strBuf, className);
			}

			if ( LOG.isDebugEnabled()) LOG.debug(strBuf.toString());
			
			if (this.out == null) 
			{
				records.add(strBuf.toString());
			} 
			else 
			{
				this.out.println(strBuf.toString());
			}
				
			strBuf.delete(0, strBuf.length());
		}
		return records;		
	}

	@Override
	protected String getFirstRow() throws SQLException 
	{
		if ( this.rs == null ) 
		{
			throw new SQLException("Rs is not initialized.");
		}

		ResultSetMetaData md = rs.getMetaData() ;
		int totalCol = md.getColumnCount();
		String[] cols = new String[totalCol];
		int[] types = new int[totalCol];
		for ( int i=0; i<totalCol; i++ ) 
		{
			cols[i] = md.getColumnLabel(i+1);
			types[i] = md.getColumnType(i+1);
		}
		
		StringBuilder strBuf = new StringBuilder();
		String className = null;
		if ( null != docName ) className = docName;
		else className = classToFill.getName();

		if (! this.rs.next()) return null; 
		
		if (this.writeAttributes) 
		{
			this.recordAsAttributes(totalCol, cols, strBuf, className);
		} 
		else 
		{
			this.recordAsTags(totalCol, cols, types, strBuf, className);
		}

		if ( LOG.isDebugEnabled()) LOG.debug(strBuf.toString());
		String xmlRec = strBuf.toString();
		if (this.out != null) this.out.println(xmlRec);
		return xmlRec;
	}	

	private void recordAsTags(int totalCol, String[] cols, int[] types,
		StringBuilder strBuf, String className) throws SQLException {
		
		strBuf.append('<').append(className).append(">\n");

		for ( int i=0; i<totalCol; i++ )  {
			Object obj = rs.getObject(i+1);
			if ( null == obj) continue;
			
			strBuf.append('<').append(cols[i]).append('>');
			switch (types[i]) {
			case java.sql.Types.VARCHAR:
			case java.sql.Types.LONGVARCHAR:
			case java.sql.Types.NCHAR:
			case java.sql.Types.CHAR:
				strBuf.append("<![CDATA[").append(rs.getObject(i+1).toString()).append("]]>");
				break;
			default:
				strBuf.append(rs.getObject(i+1).toString());
				break;
			}
			strBuf.append("</").append(cols[i]).append(">\n");
		}

		strBuf.append("</").append(className).append(">\n");
	}
	
	private void recordAsAttributes(int totalCol, String[] cols,  
		StringBuilder strBuf, String className) throws SQLException	{

		strBuf.append('<').append(className).append(" \n");
		for ( int i=0; i<totalCol; i++ ) {
			Object obj = rs.getObject(i+1);
			if ( null == obj) continue;
			
			strBuf.append(cols[i]).append('=');
			strBuf.append(rs.getObject(i+1).toString()).append(' ');
		}

		strBuf.append("/>\n");
	}

	private void recordAsInsertQuery(int totalCol, String[] cols, StringBuilder strBuf, String className) throws SQLException	
	{
			strBuf.append("INSERT INTO " +tableName +" (");
			for ( int i=0; i<totalCol; i++ ) 
			{
				strBuf.append(cols[i]);
				if(i ==  (totalCol - 1))
					strBuf.append(") VALUES ");
				else
					strBuf.append(',');
			}

			strBuf.append('(');
			for ( int i=0; i<totalCol; i++ ) 
			{
				Object obj = rs.getObject(i+1);
				if ( null == obj) continue;
				
				strBuf.append("\"").append(escape(rs.getObject(i+1).toString())).append("\"");
				if(i <  (totalCol - 1))
					strBuf.append(", ");
			}
			strBuf.append(");");
	}

	private void recordAsUpdateQuery(int totalCol, String[] cols, StringBuilder strBuf, String className) throws SQLException	
	{
			strBuf.append("UPDATE " +tableName +" SET ");
			for ( int i=1; i<totalCol-1; i++ ) 
			{
				Object obj = rs.getObject(i+1);
				if ( null == obj) continue;
				
				strBuf.append(cols[i]).append('=');
				strBuf.append("\"").append(escape(rs.getObject(i+1).toString())).append("\"");
				if(i <  (totalCol - 2))
					strBuf.append(", ");
			}
			strBuf.append(" WHERE ");
			strBuf.append(cols[0]).append('=');
			strBuf.append("\"").append(escape(rs.getObject(1).toString())).append("\"");
			strBuf.append(";");
	}

	public static String escape(String s)
	{
	    Matcher matcher = sqlTokenPattern.matcher(s);
	    StringBuffer sb = new StringBuffer();
	    while(matcher.find())
	    {
	        matcher.appendReplacement(sb, sqlTokens.get(matcher.group(1)));
	    }
	    matcher.appendTail(sb);
	    return sb.toString();
	}
}
