package com.runnerup;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class Property {
	
	private static String host;
	
	private static int port;
	
	private static String clusterName;
	
	private static String indexAlias;
	
	private static String oldIndex;
	
	private static String newIndex;
	
	private static String mappingFileUrl;
	
	private static Long documentsCount;
	
	static{
		InputStream stream = Object.class.getResourceAsStream("/setting.properties");
		Properties p = new Properties();
		
		try {
			p.load(stream);
			host = p.getProperty("host");
			port = Integer.parseInt(p.getProperty("port"));
			clusterName = p.getProperty("cluster_name");
			indexAlias = p.getProperty("index_alias");
			oldIndex = p.getProperty("old_index");
			newIndex = p.getProperty("new_index");
			mappingFileUrl = p.getProperty("mapping_file_url");
			EsClient.initClient(clusterName, host, port);  
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static String getHost() {
		return host;
	}

	public static int getPort() {
		return port;
	}

	public static String getIndexAlias() {
		return indexAlias;
	}

	public static String getOldIndex() {
		return oldIndex;
	}

	public static String getNewIndex() {
		return newIndex;
	}

	public static String getMappingFileUrl() {
		return mappingFileUrl;
	}

	public static String getClusterName() {
		return clusterName;
	}

	public static Long getDocumentsCount() {
		return documentsCount;
	}

	public static void setDocumentsCount(Long documentsCount) {
		Property.documentsCount = documentsCount;
	}

}
