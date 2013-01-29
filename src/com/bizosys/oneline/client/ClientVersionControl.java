package com.bizosys.oneline.client;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.InetAddress;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

import com.bizosys.oneline.common.Compressor;
import com.bizosys.oneline.common.ReadCsvRecords;
import com.bizosys.oneline.common.SyncTable;
import com.bizosys.oneline.common.SyncTableXMLContentHandler;
import com.bizosys.oneline.common.SyncTypes;
import com.oneline.dao.DbConfig;
import com.oneline.dao.IPool;
import com.oneline.dao.PoolFactory;
import com.oneline.dao.WriteBase;
import com.oneline.util.StringUtils;

public class ClientVersionControl 
{
	private static List<SyncTable> tablesToSync;
	
	static
	{
		try {
			tablesToSync = SyncTableXMLContentHandler.parseXML("F:\\work\\JavaLabs\\DatabaseUtil.xml");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public void syncFull() throws SQLException
	{
		createDatabaseConnection();
		syncSchemaChanges();
		syncClientData();
		syncMasterData();
	}
	
	public void syncTransactions() throws SQLException
	{
		createDatabaseConnection();
		syncClientData();
	}
	
	public void syncMaster() throws SQLException
	{
		createDatabaseConnection();
//		syncSchemaChanges();
		syncMasterData();
	}
	

	private void syncSchemaChanges() throws SQLException
	{
		
		ResultSet rs = null;
		StringBuilder sb = new StringBuilder();
		try
		{
			DatabaseMetaData metaData = PoolFactory.getDefaultPool().getConnection().getMetaData();
			rs = metaData.getColumns(null, null, "dbdata", null);
			while (rs.next())
			{
				sb.append(rs.getString("COLUMN_NAME"));
				sb.append(rs.getString("TYPE_NAME"));
				sb.append(rs.getInt("COLUMN_SIZE"));
			}
		}
		catch(Exception e)
		{
		}
		finally
		{
			try 
			{
				rs.close();
			} 
			catch (SQLException ex) 
			{
			}
		}
	
		String dataToSend = SyncTypes.SYNC_SCHEMA +"\t" +sb.toString();
		String updateSchemaStatement = callServlet(dataToSend);
		new WriteBase().execute(updateSchemaStatement, new Object[]{});
	}
	
	private void syncClientData() throws SQLException
	{
		syncInsertions();
		syncUpdations();
	}
	
	private void syncInsertions() throws SQLException
	{
		String insertQuery = "";
		for (SyncTable syncTable : tablesToSync)
		{
			if(!syncTable.getTableType().equals("Transaction"))
				continue;
			
			String selectQuery = "Select * from " +syncTable.getName() +" WHERE client_version != server_version AND server_version = 0;";
			insertQuery = getNewTransactionData(selectQuery, syncTable.getName());

			if (!insertQuery.equals(""))
			{
				System.out.println("Insert Query: " +insertQuery);
				String syncSuccess = callServlet(SyncTypes.SYNC_TRANSACTION +"\t" +insertQuery +';');
				if(syncSuccess == "true")
					updateLocalDataOnSuccess(selectQuery);
				else
					System.out.println("Syncing Insertion Data for table \"" +syncTable.getName() +"\" Failed.");
			}
		}
	}

	private void syncUpdations() throws SQLException
	{
		String updateQuery = "";
		for (SyncTable syncTable : tablesToSync)
		{
			if(!syncTable.getTableType().equals("Transaction"))
				continue;
			
			String selectQuery = "Select * from " +syncTable.getName() +" WHERE client_version != server_version AND server_version > 0;";
			updateQuery = getUpdatedTransactionData(selectQuery, syncTable.getName());

			if (!updateQuery.equals(""))
			{
				String syncSuccess = callServlet(SyncTypes.SYNC_TRANSACTION +"\t" +updateQuery);

				if(syncSuccess == "true")
					updateLocalDataOnSuccess(selectQuery);
				else
					System.out.println("Syncing Update Data for table \"" +syncTable.getName() +"\" Failed.");
			}
		}
	}

	private void updateLocalDataOnSuccess(String selectQuery) throws SQLException 
	{
		StringBuilder updateString = new StringBuilder("UPDATE dbdata SET 'server_version' = 'client_version WHERE 'id' IN ( " +selectQuery + ");");
		new WriteBase().execute(updateString.toString(), new Object[]{});
	}

	private void syncMasterData() throws SQLException
	{
		String dataToUpdate = callServlet(String.valueOf(SyncTypes.SYNC_MASTER));
		System.out.println(dataToUpdate);
		String[] updateQueries = dataToUpdate.split(";");
		System.out.println("\n\nNo of queries: " +updateQueries.length +"\n\n");
		for (int i = 0; i < updateQueries.length-1; i++) 
		{
			System.out.println("Executing Query: " +updateQueries[i]);
			new WriteBase().execute(updateQueries[i], new Object[]{});
		}
	}

	private String callServlet(String dataToSend)
	{
		String responseData = "";
		try 
		{
			URL serverURL = new URL("http://localhost:8080/1linesync/DBServlet");
			URLConnection serverConnection = serverURL.openConnection();
			serverConnection.setDoInput(true);
			serverConnection.setDoOutput(true);
			serverConnection.setUseCaches(false);
			serverConnection.setDefaultUseCaches(false);
			serverConnection.setRequestProperty("Content-Type", "text/plain");
			
			ObjectOutputStream out = new ObjectOutputStream(serverConnection.getOutputStream());
			out.writeObject(Compressor.compress(dataToSend));
			out.flush();
			out.close();
			ObjectInputStream in = new ObjectInputStream(serverConnection.getInputStream());
			responseData = (String) in.readObject();
			System.out.println("Records received...");
		} 
		catch (MalformedURLException e) 
		{
			e.printStackTrace();
		} 
		catch (IOException e) 
		{
			e.printStackTrace();
		} 
		catch (ClassNotFoundException e) 
		{
			e.printStackTrace();
		}
		return responseData;
	}

	private void createDatabaseConnection()
	{
//        PoolFactory.getInstance().setup(FileReaderUtil.toString("db.conf"));
		DbConfig config2 = new DbConfig();
		config2.driverClass = "com.mysql.jdbc.Driver";
		config2.connectionUrl = "jdbc:mysql://localhost/sampledb";
		config2.login = "root";
		config2.password = "root";
		
		PoolFactory.getInstance().setup(config2);
		IPool pool = PoolFactory.getDefaultPool();
		if(pool == null)
		{
			System.out.println("No db instance pool");
		}
	}
	
	private String getNewTransactionData(String query, String tableName)
	{
		StringWriter sw = new StringWriter();
		PrintWriter writer = new PrintWriter(sw);
		ReadCsvRecords<Object> reader = new ReadCsvRecords<Object>(writer, ReadCsvRecords.READ_INSERT, Object.class);
		reader.tableName = tableName;
		try 
		{
			reader.execute(query);
		} 
		catch (SQLException e) 
		{
			System.out.println("Getting Insert data for table " +tableName +" failed.\n");
			e.printStackTrace();
		}
		
		System.out.println(sw.toString());
		return sw.toString();
	}

	private String getUpdatedTransactionData(String query, String tableName) throws SQLException
	{
		System.out.println("Getting Update Transaction Data");
		StringWriter sw = new StringWriter();
		PrintWriter writer = new PrintWriter(sw);
		ReadCsvRecords<Object> reader = new ReadCsvRecords<Object>(writer, ReadCsvRecords.READ_UPDATE, Object.class);
		reader.tableName = tableName;
		reader.execute(query);
		
		return sw.toString() +';';
	}

	public static void main (String args[]) throws SQLException
	{ 
		ClientVersionControl cvc = new ClientVersionControl();
//		cvc.syncFull();
		cvc.syncMaster();
//		cvc.syncTransactions();
	} 
}
