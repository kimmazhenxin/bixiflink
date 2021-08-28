package com.nx.es;

import org.apache.http.HttpHost;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;



/**
 * @Author: kim
 * @Description: Java 操作ElasticSearch
 * @Date: 2021/8/7 20:01
 * @Version: 1.0
 */
public class JavaElasticClient {

	private static RestHighLevelClient client;

	public static void main(String[] args) throws Exception {
		// 连接到ES
		connect();
		String index = "person";
		String type = "student";
		String id = "1";

		// 插入
		indexStudent(index, type, id);
		// 查询
		getStudent(index, type, id);
		// 删除
		delete(index, type, id);
		// 更新
		updateStudent(index, type, id);

		// 无条件搜索
		search(index);

		// bool标签、filter标签
		boolSearch(index);




		// 关闭ES连接
		close();
	}

	/**
	 * 连接ES
	 */
	private static void connect() {
		client = new RestHighLevelClient(RestClient.builder(
				new HttpHost("node1", 9200, "http"),
				new HttpHost("node1", 9200, "http"),
				new HttpHost("node1", 9200, "http")));
	}

	/**
	 * 关闭ES连接
	 */
	private static void close() {
		try {
			client.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * 单条插入记录
	 * PUT /person/student/1 {"name":"Smith", "age":18, "address":"China"}
	 * @param index
	 * @param type
	 * @param id
	 * @throws IOException
	 */
	private static void indexStudent(String index, String type, String id) throws IOException {
		// 构建一个Request请求
		IndexRequest request = new IndexRequest(index, type, id);


		// 方式一:
		request.source("name", "Smith", "age", 18, "address", "China");
		// 同步方式
		IndexResponse response = client.index(request, RequestOptions.DEFAULT);
		System.out.println(response.toString());
		// 异步方式
		client.indexAsync(request, RequestOptions.DEFAULT, new ActionListener<IndexResponse>() {
			@Override
			public void onResponse(IndexResponse indexResponse) {
				System.out.println(indexResponse);
			}
			@Override
			public void onFailure(Exception e) {
				System.out.println(e.getStackTrace());
			}
		});

		// 方式二: XContentFactory
		request.source(XContentFactory.jsonBuilder()
				.startObject()
				.field("name", "Smith")
				.field("age", 18)
				.field("address", "China")
				.endObject()
		);
		client.index(request,RequestOptions.DEFAULT);

		// 方式三: map
		Map<String, Object> map = new HashMap<>();
		map.put("name", "Smith");
		map.put("age", 18);
		map.put("address", "China");
		request.source(map);
		client.index(request, RequestOptions.DEFAULT);

		// 方式四: jsonString XContentType.JSON
		String json = "{\"name\":\"Smith\", \"age\":18, \"address\":\"China\"}";
		request.source(json, XContentType.JSON);
		client.index(request, RequestOptions.DEFAULT);
	}

	/**
	 * 查询
	 * @param index
	 * @param type
	 * @param id
	 * @throws IOException
	 */
	private static void getStudent(String index, String type, String id) throws IOException {
		GetRequest request = new GetRequest(index, type, id);
		GetResponse response = client.get(request, RequestOptions.DEFAULT);
		Map<String, Object> map = response.getSource();
		System.out.println(map.get("name") + "," + map.get("age" + "," + map.get("address")));
	}

	/**
	 * 删除记录
	 * @param index
	 * @param type
	 * @param id
	 * @throws IOException
	 */
	private static void delete(String index, String type, String id) throws IOException {
		DeleteRequest request = new DeleteRequest(index, type, id);
		DeleteResponse response = client.delete(request, RequestOptions.DEFAULT);
		System.out.println(response.toString());
	}

	/**
	 * 更新
	 * @param index
	 * @param type
	 * @param id
	 * @throws IOException
	 */
	private static void updateStudent(String index, String type, String id) throws IOException {
		// 覆盖更新,会覆盖之前的记录
		IndexRequest request = new IndexRequest(index, type, id);
		request.source("name", "Smith", "age", 28, "address", "China");
		client.index(request, RequestOptions.DEFAULT);

		// 局部更新,只更新对应字段的值
		UpdateRequest updateRequest = new UpdateRequest(index, type, id);
		updateRequest.doc("name", "Smith");
		UpdateResponse response = client.update(updateRequest, RequestOptions.DEFAULT);
		System.out.println(response.toString());

	}

	/**
	 * 最基本的搜索功能,即无条件搜素search
	 * @param index
	 * @throws IOException
	 */
	private static void search(String index) throws IOException {
		SearchRequest request = new SearchRequest(index);
		SearchResponse response = client.search(request, RequestOptions.DEFAULT);
		SearchHit[] hits = response.getHits().getHits();
		for (SearchHit hit: hits) {
			// 获取hit中的source,即document记录,封装成了Json串
			System.out.println(hit.getSourceAsString());
			// 以Map的形式封装,可以根据key按需索取
			Map<String, Object> sourceMap = hit.getSourceAsMap();
		}
	}

	/**
	 * 多条件的搜素 bool标签、filter标签
	 * @param index
	 * @throws Exception
	 */
	private static void boolSearch(String index) throws Exception {
		SearchRequest request = new SearchRequest(index);
		SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
		sourceBuilder.query(QueryBuilders.boolQuery()
				.must(QueryBuilders.matchQuery("address", "beijing"))
				.mustNot(QueryBuilders.matchQuery("like", "swimming"))
				.should(QueryBuilders.matchQuery("age", 18))
				.should(QueryBuilders.matchQuery("age", 19))
				.should(QueryBuilders.matchQuery("age", 20))
				.filter(QueryBuilders.rangeQuery("age").gt(19).lte(20)));
		// 条件赋给request
		request.source(sourceBuilder);

		SearchResponse response = client.search(request, RequestOptions.DEFAULT);
		SearchHit[] hits = response.getHits().getHits();
		for (SearchHit hit: hits) {
			// 获取hit中的source,即document记录,封装成了Json串
			System.out.println(hit.getSourceAsString());
		}
	}





	/**
	 * match_all 搜素
	 * @param index
	 * @throws IOException
	 */
	public static void matchAll(String index) throws IOException {

	}
}
