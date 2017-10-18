package com.runnerup;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Iterator;

import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequestBuilder;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.IndicesAdminClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSONObject;

public class ReMapping {

	private final static Logger LOG = LoggerFactory.getLogger(ReMapping.class);
	
	public static void resetMapping(String newIndex,String mappingUrl){
		Client client = EsClient.getInstance();
		IndicesAdminClient iac = client.admin().indices();
		IndicesExistsRequestBuilder exists = iac.prepareExists(newIndex); 
		boolean exsit = exists.execute().actionGet().isExists();
		if(exsit){
			client.admin().indices().prepareDelete(newIndex).execute().actionGet();
		}
		CreateIndexRequestBuilder cirb = iac.prepareCreate(newIndex);
		JSONObject mapping = parseMapping(mappingUrl);
		Iterator<String> keys = mapping.keySet().iterator();
		while(keys.hasNext()){
			String key = keys.next();
			JSONObject value = mapping.getJSONObject(key);
			cirb.addMapping(key, value.toJSONString());
		}
		cirb.get();
		LOG.info("初始化mapping成功！");
	}
	
	public static JSONObject parseMapping(String mappingUrl){
		File file = new File(mappingUrl);
		BufferedReader reader = null;
		StringBuffer buffer = new StringBuffer();
		try {
			reader = new BufferedReader(new FileReader(file));
			String tempString = null;
			while((tempString = reader.readLine())!=null){
				buffer = buffer.append(tempString);
			}
			reader.close();
		} catch (FileNotFoundException e) {
			LOG.error("mapping 配置文件没有找到!");
		} catch (IOException e) {
			LOG.error("读取mapping配置文件错误!");
		}
		
		return JSONObject.parseObject(buffer.toString());
	}
}
