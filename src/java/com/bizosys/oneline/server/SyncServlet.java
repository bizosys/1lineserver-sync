package com.bizosys.oneline.server;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.sql.SQLException;
import java.util.List;

import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.bizosys.oneline.common.Compressor;
import com.bizosys.oneline.common.SyncTypes;
import com.oneline.dao.PoolFactory;
import com.oneline.dao.ReadAsDML;
import com.oneline.dao.ReadQuadruplet;
import com.oneline.dao.WriteBase;
import com.oneline.util.FileReaderUtil;

/**
 * Servlet implementation class DBServlet
 */
public class SyncServlet extends HttpServlet 
{
	private static final long serialVersionUID = 1L;
	
	public void init(ServletConfig config)
	{
		System.out.println (FileReaderUtil.toString("jdbc.conf"));
		PoolFactory.getInstance().setup(FileReaderUtil.toString("jdbc.conf"));
	}
	
    /**
     * @see HttpServlet#HttpServlet()
     */
    public SyncServlet() {
        super();
        // TODO Auto-generated constructor stub
        
    }

	/**
	 * @see HttpServlet#doGet(HttpServletRequest request, HttpServletResponse response)
	 */
	protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		// TODO Auto-generated method stub
	}

	/**
	 * @see HttpServlet#doPost(HttpServletRequest request, HttpServletResponse response)
	 */
	protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException
	{
		ObjectInputStream resultStream = null;
		String results = null;
		try 
		{
			System.out.println("Sync Start......");
			resultStream = new ObjectInputStream(request.getInputStream());
			results = Compressor.decompressToString((byte[]) resultStream.readObject());
			
			resultStream.close();
			
			//Read the results for type of sync
			String[] input = results.split("\t");
			int syncType = Integer.parseInt(input[0]);
			String syncData = "";
			if(input.length > 1) syncData = input[1];

			//Based on the type of the sync operation do the necessary
			syncDatabase(syncType, syncData, response.getOutputStream());
			
		} 
		catch (Exception e) 
		{
			e.printStackTrace(System.out);
			response.sendError(501, e.getMessage());
		} 
	}

	public void sendResponse(OutputStream response, String results) throws IOException 
	{
		ObjectOutputStream sendStream = null;
		sendStream = new ObjectOutputStream(response);
		sendStream.write(results.getBytes());
		sendStream.flush();
		sendStream.close();
	}

	public void syncDatabase(int syncType, String syncData, OutputStream response) throws Exception
	{
		try
		{
			if(syncType == SyncTypes.SYNC_UP)
			{
				// IF TRANSACTION IS SYNCED THEN RUN THE DATA QUERY IN THE SERVER TABLES
				new WriteBase().execute(syncData, new Object[]{});
				sendResponse(response, Boolean.TRUE.toString());
			}
			else if( syncType == SyncTypes.SYNC_DOWN)
			{
				StringBuilder sqlDump = new StringBuilder();
				List<ReadQuadruplet.Quadruplet> downTableInserts = new ReadQuadruplet().execute(
						"select destination_table, mark_inserts, find_inserts,complete_inserts  from sync_config where direction = 'd'");
				System.out.println(syncDownSteps(downTableInserts, true));
				sqlDump.append(syncDownSteps(downTableInserts, true));

				List<ReadQuadruplet.Quadruplet> downTableUpdates = new ReadQuadruplet().execute(
						"select destination_table, mark_updates, find_updates,complete_updates from sync_config where direction = 'd'");
				sqlDump.append(syncDownSteps(downTableUpdates, false));
				
				sendResponse(response, sqlDump.toString());
			}
			else if (syncType == SyncTypes.SYNC_SCHEMA)
			{
				// IF SCHEMA IS BEING SYNCED CHECK CHANGES TO THE SCHEMA AND DO THE NECESSARY UPDATES
			}
		}
		catch(Exception ex)
		{
			ex.printStackTrace(System.out);
			throw ex;
		}
	}
	
	private String syncDownSteps(List<ReadQuadruplet.Quadruplet> upTables, boolean isInsert) throws SQLException
	{
		String sql = null;
		StringBuilder sqlDump = new StringBuilder();
		try {
			for (ReadQuadruplet.Quadruplet syncTable : upTables)
			{
				//Mark
				sql = syncTable.second.toString();
				sql = sql + ";";
				new WriteBase().execute(sql, new Object[]{});
				
				//Update
				sql = syncTable.third.toString();
				 sqlDump.append((isInsert) ? sqlDumpInserts(syncTable.first.toString(),sql) : 
					 sqlDumpUpdates(syncTable.first.toString(), sql));
			}
			return sqlDump.toString();			
		} catch (Exception ex) {
			System.out.println("Error :" + sql);
			throw new SQLException(ex);
		}

	}
	
	private String sqlDumpInserts(String destinationTable, String query)
	{
		StringWriter sw = new StringWriter();
		PrintWriter writer = new PrintWriter(sw);
		ReadAsDML reader = new ReadAsDML(writer, ReadAsDML.READ_INSERT, destinationTable);
		
		try {
			reader.execute(query);
		
		} catch (Exception e) {
			System.out.println("Getting Insert data for table " + destinationTable +" failed.\n");
			e.printStackTrace(System.out);
		}
		return sw.toString();
	}

	private String sqlDumpUpdates(String detinationTable, String query) throws SQLException
	{
		System.out.println(query);
		StringWriter sw = new StringWriter();
		PrintWriter writer = new PrintWriter(sw);
		ReadAsDML reader = new ReadAsDML(writer, ReadAsDML.READ_UPDATE, detinationTable);
		reader.execute(query);
		return sw.toString();
	}
	
	public static void main(String[] args) throws Exception {
		SyncServlet s = new SyncServlet();
		s.init(null);
		
		OutputStream stream = new FileOutputStream("d:\\out.txt");
		s.syncDatabase(3, "", stream);
	}
}
