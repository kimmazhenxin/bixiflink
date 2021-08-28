package com.nx.es;

import org.apache.http.HttpHost;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.PutMappingRequest;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.metrics.avg.Avg;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.sort.SortOrder;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
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

		// 批量插入
		bulkLoad(index, type);
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
	 * 批量插入操作
	 * post /index/type/_bulk {"index":{"_id":"1"}} {"name":"Tom"} {"index":{"_id":"2"}} {"name":"Jack"}
	 * @param index
	 * @throws Exception
	 */
	private static void bulkLoad(String index, String type) throws Exception {
		BulkRequest request = new BulkRequest();
		request.add(new IndexRequest(index, type, "1").source("name", "Jack", "age", 38, "salary", 21000, "team", "a"))
				.add(new IndexRequest("company", "employee", "2")
						.source("name", "Smith",
								"age", 36,
								"salary", 18000,
								"team", "a"))
				.add(new IndexRequest("company", "employee", "3")
						.source("name", "Kon",
								"age", 29,
								"salary", 17000,
								"team", "a"))
				.add(new IndexRequest("company", "employee", "4")
						.source("name", "Mark",
								"age", 42,
								"salary", 30000,
								"team", "b"))
				.add(new IndexRequest("company", "employee", "5")
						.source("name", "Lin",
								"age", 37,
								"salary", 28000,
								"team", "b"))
				.add(new IndexRequest("company", "employee", "6")
						.source("name", "Whon",
								"age", 29,
								"salary", 15000,
								"team", "b"));
		BulkResponse responses = client.bulk(request, RequestOptions.DEFAULT);
		BulkItemResponse[] items = responses.getItems();
		for (BulkItemResponse response : items) {
			System.out.println(response.isFailed());
			System.out.println(response.getResponse());
		}
	}


	/**
	 * 修改分组字段	fielddata = true ,按照 team分组
	 * {
	 * 	"properties": {
	 * 		"team":{
	 * 			"type": "text",
	 * 			"fielddata": true
	 *                }* 	}
	 * }
	 * @param index
	 */
	private static void putMapping(String index) throws IOException {
		PutMappingRequest request = new PutMappingRequest(index);
		request.source(XContentFactory.jsonBuilder()
				.startObject()
					.startObject("properties")
						.startObject("team")
							.field("type", "text")
							.field("fileddata", true)
						.endObject()
					.endObject()
				.endObject());

		AcknowledgedResponse response = client.indices().putMapping(request, RequestOptions.DEFAULT);
		System.out.println(response.isAcknowledged());
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
	 * match_all 搜素
	 * @param index
	 * @throws IOException
	 */
	private static void matchAll(String index) throws IOException {
		SearchRequest request = new SearchRequest(index);
		request.source(new SearchSourceBuilder().query(QueryBuilders.matchAllQuery()));
		SearchResponse response = client.search(request, RequestOptions.DEFAULT);
		SearchHit[] hits = response.getHits().getHits();
		for (SearchHit hit : hits) {
			System.out.println(hit.getSourceAsString());
		}
	}

	/**
	 * match 条件搜索
	 * @param index
	 * @throws Exception
	 */
	private static void match(String index) throws Exception {
		SearchRequest request = new SearchRequest(index);
		request.source(new SearchSourceBuilder().query(QueryBuilders.matchQuery("name", "Smith")));
		SearchResponse response = client.search(request, RequestOptions.DEFAULT);
		SearchHit[] hits = response.getHits().getHits();
		for (SearchHit hit : hits) {
			System.out.println(hit.getSourceAsString());
		}
	}


	/**
	 * match_phrase
	 */
	private static void matchPhrase(String index) throws IOException {
		SearchRequest request = new SearchRequest(index);
		request.source(new SearchSourceBuilder().query(QueryBuilders.matchPhraseQuery("like", "hiking basketball")));
		SearchResponse response = client.search(request, RequestOptions.DEFAULT);
		SearchHit[] hits = response.getHits().getHits();
		for (SearchHit hit : hits) {
			System.out.println(hit.getSourceAsString());
		}
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
	 * sort、form、size、_source、highlight
	 * @throws Exception
	 */
	private static void searchSortFromSizeSourceHighLight(String index) throws Exception {
		SearchRequest request = new SearchRequest(index);
		SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
		sourceBuilder.query(QueryBuilders.matchQuery("like", "running"))
				.sort("age", SortOrder.DESC)
				.from(0)
				.size(2)
				.fetchSource(new String[]{"age", "name", "address"}, new String[0])
				.highlighter(new HighlightBuilder().field("like"));
		request.source(sourceBuilder);
		SearchResponse response = client.search(request, RequestOptions.DEFAULT);
		SearchHit[] hits = response.getHits().getHits();
		for (SearchHit hit : hits) {
			System.out.println(hit.getSourceAsString());
			System.out.println(hit.getHighlightFields().get("like").toString());
		}
	}

	/**
	 * 聚合、分组、分析avg
	 * @param index
	 * @throws Exception
	 */
	private static void avgAggs(String index) throws Exception {
		SearchRequest request = new SearchRequest(index);
		SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
		sourceBuilder.query(QueryBuilders.rangeQuery("age").gte(35))
				.size(0)
				.aggregation(
						AggregationBuilders
								.terms("group_by_team")	// 分组后的名字
								.field("team")	//分组字段
								.subAggregation(AggregationBuilders.avg("avg_salary").field("salary"))	// 按照salary求平均值
				);
		request.source(sourceBuilder);
		SearchResponse response = client.search(request, RequestOptions.DEFAULT);
		Terms group_by_team = response.getAggregations().get("group_by_team");
		List<? extends Terms.Bucket> buckets = group_by_team.getBuckets();
		for (Terms.Bucket bucket : buckets) {
			Avg avg_salary = bucket.getAggregations().get("avg_salary");
			System.out.println("组名：" + bucket.getKey() + " , 个数：" + bucket.getDocCount()
					+ " , 平均值 ： " + avg_salary.getValue());
		}
	}


	/**
	 * 分组聚合,按照 age字段聚合,聚合后的组名 group_by_age
	 * @param index
	 * @throws Exception
	 */
	private static void groupBy(String index) throws Exception {
		SearchRequest request = new SearchRequest(index);
		SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
		sourceBuilder.query(QueryBuilders.matchAllQuery())
				.size(0)
				.aggregation(AggregationBuilders.terms("group_by_age").field("age"));
		request.source(sourceBuilder);
		SearchResponse response = client.search(request, RequestOptions.DEFAULT);
		Terms term = response.getAggregations().get("group_by_age");
		List<? extends Terms.Bucket> buckets = term.getBuckets();
		for (Terms.Bucket bucket : buckets) {
			System.out.println("组名: " + bucket.getKey() + "	, 个数: " + bucket.getDocCount());
		}
	}
}
