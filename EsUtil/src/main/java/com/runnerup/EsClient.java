package com.runnerup;

import java.net.InetAddress;

import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.xpack.client.PreBuiltXPackTransportClient;

public class EsClient {
	
	private static TransportClient client = null;
	
	public static void initClient(String clusterName,String host,int port){
		if (client != null)
			return;
		Settings settings = Settings.builder()
				.put("cluster.name", clusterName)
				.put("client.transport.sniff", true)
				.put("xpack.security.user", "elastic:changeme").build();
		try {
			client = new PreBuiltXPackTransportClient(settings)
					.addTransportAddress(new InetSocketTransportAddress(
							InetAddress.getByName(host), port));
		} catch (Exception e) {
		}
	}
	
	public static TransportClient getInstance(){
		if(client == null){
			new Property();
		}
		return client;
	}

}
