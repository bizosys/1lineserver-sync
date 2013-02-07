/*
* Copyright 2010 Bizosys Technologies Limited
*
* Licensed to the Bizosys Technologies Limited (Bizosys) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The Bizosys licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/

package com.bizosys.oneline.common;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URISyntaxException;
import java.net.URL;

import org.apache.log4j.Logger;

import com.oneline.ApplicationFault;
import com.oneline.util.StringUtils;

public class FileReaderUtil {

	private final static Logger LOG = Logger.getLogger(FileReaderUtil.class);

	/**
	 * Give in Strings
	 * @param fileName
	 * @return
	 * @throws ApplicationFault
	 */
	public static String toString(String fileName) 
	{
		
		File aFile = getFile(fileName);
		BufferedReader reader = null;
		InputStream stream = null;
		StringBuilder sb = new StringBuilder();
		try {
			stream = new FileInputStream(aFile); 
			reader = new BufferedReader ( new InputStreamReader (stream) );
			String line = null;
			String newline = StringUtils.getLineSeaprator();
			while((line=reader.readLine())!=null) {
				if (line.length() == 0) continue;
				sb.append(line).append(newline);	
			}
			return sb.toString();
		} 
		catch (Exception ex) 
		{
			throw new RuntimeException(ex);
		} 
		finally 
		{
			try {if ( null != reader ) reader.close();
			} catch (Exception ex) {LOG.error("FileReaderUtil", ex);}
			try {if ( null != stream) stream.close();
			} catch (Exception ex) {LOG.error("FileReaderUtil", ex);}
		}
	}
	
	/**
	 * Resolves a file from various location..
	 * @param fileName
	 * @return
	 * @throws ApplicationFault
	 */
    public static File getFile(String fileName) 
    {
		File aFile = new File(fileName);
		System.out.println("File Path 1: " +aFile.getAbsolutePath());
		if (aFile.exists()) return aFile;
		
		aFile = new File("/" + fileName);
		System.out.println("File Path 2: " +aFile.getAbsolutePath());
		if (aFile.exists()) return aFile;

		aFile = new File("conf/" + fileName);
		System.out.println("File Path 3: " +aFile.getAbsolutePath());
		if (aFile.exists()) return aFile;
		
		aFile = new File("resources/" + fileName);
		System.out.println("File Path 4: " +aFile.getAbsolutePath());
		if (aFile.exists()) return aFile;
		
		try {
			URL resource = FileReaderUtil.class.getClassLoader().getResource(fileName);
			if ( resource != null) aFile = new File(resource.toURI());
		} 
		catch (URISyntaxException ex) {
			throw new RuntimeException(ex);
		}

		if (aFile.exists()) return aFile;

		throw new RuntimeException("FileResourceUtil > File does not exist :" + fileName);
	}
    
    public static String getHostNeutralFileName(String strUrl) {
    	if ( strUrl.indexOf(':') < 0) return  strUrl;
    	strUrl = strUrl.replace("D:", "/D:");
    	strUrl = strUrl.replace("C:", "/C:");
    	strUrl = strUrl.replace("d:", "/d:");
    	strUrl = strUrl.replace("c:", "/c:");
    	return strUrl;
    }
}
