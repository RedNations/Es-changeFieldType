package com.runnerup.changeMapping;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.search.SearchHit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.runnerup.EsClient;
import com.runnerup.Property;
import com.runnerup.ReMapping;

public class ChangeMapping {
	
	private final static Logger log = LoggerFactory.getLogger(ChangeMapping.class);
	
	private static final ArrayList<Map<String,Object>> buffer = new ArrayList<Map<String,Object>>();
	   
	private static Client client = EsClient.getInstance();  
	
	public static void changeMapping() throws IOException{
		//初始化mapping
		ReMapping.resetMapping(Property.getNewIndex(), Property.getMappingFileUrl());
		
		//读取旧数据
		Scan();
		
		//建立索引别名
		alias();
		
		//删除旧的索引
		deleteOldIndex();
	}
	
	/**
	 * @throws IOException 
	 * @Description: 读取旧索引信息
	 * @param: tags
	 * @return: return_type
	 * @throws
	 */ 
	public static void Scan() throws IOException{
		
		SearchResponse scrollResp = client
				.prepareSearch(Property.getOldIndex())
				.setScroll(new TimeValue(60000))
				.setSize(100)//每次读取100
				.execute().actionGet();
		Long totalRecord = scrollResp.getHits().getTotalHits();
		Property.setDocumentsCount(totalRecord);
		log.info("读取到索引："+Property.getOldIndex()+",总共数量:"+totalRecord);
		
		for(SearchHit hit:scrollResp.getHits()){
			executeESBulk(hit);
		}
		if(totalRecord.intValue() >100){
			while(true){
				scrollResp = client
						.prepareSearchScroll(scrollResp.getScrollId())
						.setScroll(new TimeValue(600000))
						.execute().actionGet();
				if(scrollResp.getHits().getHits().length == 0)
					break;
				for(SearchHit hit:scrollResp.getHits()){
					executeESBulk(hit);
				}
			}
		}
		close();
	}
	
	public static void executeESBulk(SearchHit hit) throws IOException {
		Map<String, Object> map = new HashMap<String, Object>();
		String routing = hit.field("_routing").getValue();
		map.put("_routing", routing);

		String id = hit.getId();
		map.put("id", id);
		String parent = "";
		if (hit.field("_parent") != null) {
			parent = hit.field("_parent").getValue();
			map.put("_parent", parent);
		}
		map.put("_type", hit.getType());
		map.put("source", hit.getSource());
		buffer.add(map);
		if(buffer.size()>=100){
			flushBatch();
		}
	}
	
	public static void flushBatch() throws IOException{
		log.info("开始刷新数据到新索引！");
		BulkRequestBuilder bulkRequest = client.prepareBulk();
		for(Map<String,Object> hit : buffer){
			@SuppressWarnings("unchecked")
			Map<String,Object> source =  (Map<String, Object>) hit.get("source");
			IndexRequest indexRequest = new IndexRequest(Property.getNewIndex(),
	    			   hit.get("_type").toString(),  hit.get("id").toString())
				.source(source)
				.routing(hit.get("_routing").toString());
			if (hit.get("_parent")!=null)
				indexRequest.parent(hit.get("_parent").toString());
			bulkRequest.add(indexRequest);
		}
		
		Property.setDocumentsCount(Property.getDocumentsCount()- (long) buffer.size());
		System.out.println("Flushed a batch of " + buffer.size() + " - Remaining " + Property.getDocumentsCount() + " documents");
		
		BulkResponse bulkResponse;
		bulkResponse = bulkRequest.execute().actionGet();
		if(bulkResponse.hasFailures()){
			throw new IOException(bulkResponse.buildFailureMessage());
		}
		else buffer.clear();
	}
	
	public static void close() throws IOException {
        if(buffer.size() > 0)
            flushBatch();
        buffer.clear();
        log.info("数据迁移完毕！");
        
    }
	
	public static void alias(){
		client.admin().indices().prepareAliases()
		.addAlias(Property.getNewIndex(), Property.getIndexAlias())
		.execute().actionGet();
	}
	
	public static void deleteOldIndex(){
		client.admin().indices().prepareDelete(Property.getOldIndex()).execute().actionGet();
	}
	
	public static void main(String[] args) throws IOException {
		alias();
	}
}
