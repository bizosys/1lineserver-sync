package com.bizosys.oneline.client;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;
import java.sql.SQLException;
import java.util.List;

import com.bizosys.oneline.common.Compressor;
import com.bizosys.oneline.common.SyncTypes;
import com.oneline.dao.IPool;
import com.oneline.dao.PoolFactory;
import com.oneline.dao.ReadAsDML;
import com.oneline.dao.ReadQuadruplet;
import com.oneline.dao.WriteBase;
import com.oneline.util.FileReaderUtil;
import com.oneline.util.StringUtils;

public class SyncAgent 
{
	public void syncUpAndDown() throws SQLException {
		syncUp();
		syncDown();
	}
	
	public void syncUp() throws SQLException {
		List<ReadQuadruplet.Quadruplet> upTableInserts = new ReadQuadruplet().execute(
			"select destination_table, mark_inserts, find_inserts,complete_inserts  from sync_config where direction = 'u'");
		syncUpSteps(upTableInserts, true);

		List<ReadQuadruplet.Quadruplet> upTableUpdates = new ReadQuadruplet().execute(
				"select destination_table, mark_updates, find_updates,complete_updates from sync_config where direction = 'u'");
		syncUpSteps(upTableUpdates, false);
	}
	
	private void syncUpSteps(List<ReadQuadruplet.Quadruplet> upTables, boolean isInsert) throws SQLException {
		
		for (ReadQuadruplet.Quadruplet syncTable : upTables)
		{
			new WriteBase().execute(syncTable.second.toString());
			
			String sqlDump = (isInsert) ? 
				sqlDumpInserts(syncTable.first.toString(), syncTable.third.toString()) :
				sqlDumpUpdates(syncTable.first.toString(), syncTable.third.toString());
				
			if ( null == sqlDump) continue;
			if ( sqlDump.length() == 0 ) continue;
			
			String syncSuccess = callServlet(SyncTypes.SYNC_UP + "\t" + sqlDump +';');
			
			if(syncSuccess.equals("true") ) {
				new WriteBase().execute(syncTable.fourth.toString(), WriteBase.EMPTY_ARRAY);
			} else {	
				System.out.println("Syncing Data for table \"" + syncTable.first.toString() +"\" Failed.");
				break;
			}
		}
	}

	private void syncDown() throws SQLException
	{
		String sqlUpdateText = callServlet(String.valueOf(SyncTypes.SYNC_DOWN));
		
		if (StringUtils.isEmpty(sqlUpdateText)) return;
		
		WriteBase writer = new WriteBase();
		
		try {
			int index1 = 0;
			int index2 = sqlUpdateText.indexOf(';');
			String token = null;

			writer.beginTransaction();
			
			while (index2 >= 0) {
				token = sqlUpdateText.substring(index1, index2);
				writer.execute(token);
				index1 = index2 + 1;
				index2 = sqlUpdateText.indexOf(';', index1);
			}
        
			if (index1 < sqlUpdateText.length() - 1) {
				writer.execute(sqlUpdateText.substring(index1));
			}			
			
			writer.commitTransaction();
			writer = null;
		
		} catch (Exception ex) {
			writer.rollbackTransaction();
			writer = null;
			
		} finally {
			if ( null != writer) {
				writer.rollbackTransaction();
			}
		}
	}

	private String callServlet(String dataToSend)
	{
		System.out.println(dataToSend);
		if ( 1 == 1) return null;
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

	/**
	 * All Insert statements as SQL String
	 * @param query
	 * @param detinationTable
	 * @return
	 */
	private String sqlDumpInserts(String detinationTable, String query)
	{
		StringWriter sw = new StringWriter();
		PrintWriter writer = new PrintWriter(sw);
		ReadAsDML reader = new ReadAsDML(writer, ReadAsDML.READ_INSERT, detinationTable);
		
		try {
			reader.execute(query);
		
		} catch (Exception e) {
			System.out.println("Getting Insert data for table " + detinationTable +" failed.\n");
			e.printStackTrace(System.out);
		}
		return sw.toString();
	}

	private String sqlDumpUpdates(String query, String detinationTable) throws SQLException
	{
		System.out.println("Getting Update Transaction Data");
		StringWriter sw = new StringWriter();
		PrintWriter writer = new PrintWriter(sw);
		ReadAsDML reader = new ReadAsDML(writer, ReadAsDML.READ_UPDATE, detinationTable);
		reader.execute(query);
		
		return sw.toString() +';';
	}

	public static void main (String args[]) throws SQLException
	{ 
		System.out.println(FileReaderUtil.toString("jdbc.conf"));
        PoolFactory.getInstance().setup(FileReaderUtil.toString("jdbc.conf"));
        
		IPool pool = PoolFactory.getInstance().getPool("dictionary", false);
		if(pool == null) {
			System.out.println("No db instance pool");
		}
		
		SyncAgent cvc = new SyncAgent();
//		cvc.syncFull();
		cvc.syncUp();
//		cvc.syncTransactions();
	} 
}
