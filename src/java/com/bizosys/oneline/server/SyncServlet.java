package com.bizosys.oneline.server;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
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
import com.bizosys.oneline.common.ReadAsDML;
import com.bizosys.oneline.common.SyncTable;
import com.bizosys.oneline.common.SyncConfigLoader;
import com.bizosys.oneline.common.SyncTypes;
import com.oneline.dao.DbConfig;
import com.oneline.dao.IPool;
import com.oneline.dao.PoolFactory;
import com.oneline.dao.WriteBase;
import com.oneline.util.FileReaderUtil;

/**
 * Servlet implementation class DBServlet
 */
public class SyncServlet extends HttpServlet 
{
	private static final long serialVersionUID = 1L;
	private static List<SyncTable> tablesToSync;
	
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
			System.out.println("Got Data......");
			resultStream = new ObjectInputStream(request.getInputStream());
			System.out.println("Got Result Stream......");
			results = Compressor.decompressToString((byte[]) resultStream.readObject());
			resultStream.close();
			
			//Read the results for type of sync
			String[] input = results.split("\t");
			int syncType = Integer.parseInt(input[0]);
			String syncData = "";
			if(input.length > 1)
				syncData = input[1];

			//Based on the type of the sync operation do the necessary
			syncDatabase(syncType, syncData, response);
			
		} 
		catch (IOException e) 
		{
			e.printStackTrace(); 
		} 
		catch (ClassNotFoundException e) 
		{
			e.printStackTrace();
		}
	}

	public void sendResponse(HttpServletResponse response, String results) throws IOException 
	{
		ObjectOutputStream sendStream = null;
		sendStream = new ObjectOutputStream(response.getOutputStream());
		sendStream.writeObject(results);
		sendStream.flush();
		sendStream.close();
	}

	public void syncDatabase(int syncType, String syncData, HttpServletResponse response)
	{
		try
		{
			if(syncType == SyncTypes.SYNC_DOWN)
			{
				// IF TRANSACTION IS SYNCED THEN RUN THE DATA QUERY IN THE SERVER TABLES
				new WriteBase().execute(syncData, new Object[]{});
				sendResponse(response, Boolean.TRUE.toString());
			}
			else if( syncType == SyncTypes.SYNC_UP)
			{
				// IF MASTER IS BEING SYNCED THEN CHECK TABLE FOR LATEST INFO. IF THERE THEN SEND INCREMENTAL UPDATE/INSERT
				System.out.println("Syncing Master Table");
				
				String updateQuery = "";
				for (SyncTable syncTable : tablesToSync)
				{
					if( SyncTable.DOWN.equals(syncTable.syncDirection) ) continue;
					
					String selectQuery = "Select * from " +syncTable.tableName;
//					String selectQuery = "Select * from " +syncTable.getName() +" WHERE client_version != server_version AND server_version > 0;";
					updateQuery = getMasterData(selectQuery, syncTable.tableName);

					if (!updateQuery.equals(""))
					{
						sendResponse(response, updateQuery);
					}
				}
				
			}
			else if (syncType == SyncTypes.SYNC_SCHEMA)
			{
				// IF SCHEMA IS BEING SYNCED CHECK CHANGES TO THE SCHEMA AND DO THE NECESSARY UPDATES
			}
		}
		catch(SQLException se)
		{
			se.printStackTrace();
			
			try {
				sendResponse(response, null);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		catch (IOException e) 
		{
			e.printStackTrace();
		}
	}
	
	private String getMasterData(String selectQuery, String tableName) throws SQLException
	{
		System.out.println("Getting Updated Master Data");
		StringWriter sw = new StringWriter();
		PrintWriter writer = new PrintWriter(sw);
		ReadAsDML reader = new ReadAsDML(writer, ReadAsDML.READ_UPDATE, Object.class);
		reader.tableName = tableName;
		reader.execute(selectQuery);
		
		return sw.toString();
	}

}
