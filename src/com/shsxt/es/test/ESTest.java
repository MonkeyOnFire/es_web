package com.shsxt.es.test;

import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentFactory;

import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.sort.SortOrder;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;

public class ESTest {

    TransportClient client;

    /**
     * 初始化，創建client
     *
     * @throws UnknownHostException
     * @throws IllegalAccessException
     * @throws InstantiationException
     */

    @Before
    public void client() throws UnknownHostException, IllegalAccessException, InstantiationException {

        Map<String, String> map = new HashMap<String, String>();
        map.put("cluster.name", "my-application");

        Settings.Builder settings = Settings.builder().put(map);

        client = TransportClient.builder().settings(settings).build();

        client.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("node1"), 9300));
        client.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("node2"), 9300));
        client.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("node3"), 9300));

    }


    /**
     * 创建索引库
     */
    @Test
    public void createIndexBase() {

        IndicesExistsResponse actionGet = client.admin().indices().prepareExists("test").execute().actionGet();

        if (actionGet.isExists()) {
            System.out.println("test索引库已存在。。");

            client.admin().indices().prepareDelete("test").execute();
        }

        Map<String, String> map = new HashMap<>();

        map.put("number_of_replicas", "0");
        map.put("number_of_shards", "3");


        client.admin().indices().prepareCreate("test").setSettings(map).execute();


    }

    /**
     * 添加索引
     */
    @Test
    public void addIndex() {
        HashMap<String, Object> hashMap = new HashMap<String, Object>();
        hashMap.put("name", "jhon");
        hashMap.put("age", 28);
        hashMap.put("gender", "male");
        hashMap.put("describe", "lyp is good");
        IndexResponse response = client.prepareIndex("test", "employee", "79").setSource(hashMap).execute()
                .actionGet();


        IndexResponse response1 = client.prepareIndex("test", "employee").setSource(hashMap).execute().actionGet();

        System.out.println(response.getId());
        System.out.println(response1.getId());
    }

    /**
     * 获取索引
     */
    @Test
    public void get() {

        GetResponse response = client.prepareGet("test", "employee", "79").execute().actionGet();

        System.out.println(response.getVersion());

        Map<String, Object> source = response.getSource();

        for (String key : source.keySet()) {
            System.out.println(key + " ->" + source.get(key));
        }
    }

    /**
     * 搜索
     */
    @Test
    public void search() {
        //指定从test,javatest索引库里查询

        SearchRequestBuilder builder = client.prepareSearch("test", "shsxt");
        //指定从employee,ikType,blog,这三个type里面查询
        builder.setTypes("employee", "ikType", "blog");
        //设置分页,从第0个开始，返回3个。
        builder.setFrom(0);
        builder.setSize(3);

        String key = "jhon";
        //设置从name和content字段里查询
        builder.setQuery(QueryBuilders.multiMatchQuery(key, "name", "content"));
        // builder.addSort("age", SortOrder.DESC); 设置排序字段，默认的话会根据相关性打分。
        // builder.setPostFilter(QueryBuilders.rangeQuery("age").from(24).to(28)); 设置过滤
        // 开始查询。
        SearchResponse searchResponse = builder.get();
        //获取查询的返回信息
        SearchHits hits = searchResponse.getHits();

        //一共有多少个符合条件的查询结果
        System.out.println("总共查询到了：" + hits.getTotalHits());

        //获取查询到的结果数组
        SearchHit[] hits2 = hits.getHits();

        for (SearchHit searchHit : hits2) {

            System.out.println("分数：" + searchHit.getScore());

            Map<String, Object> source = searchHit.getSource();
            System.out.println("id ->" + searchHit.getId());
            System.out.println("index -> " + searchHit.index());
            for (String s : source.keySet()) {
                System.out.println(s + "->" + source.get(s));
            }
            System.out.println("--------");
        }

    }

    /**
     * 更新
     *
     * @throws Exception
     */
    @Test
    public void update() throws Exception {
        UpdateRequest updateRequest = new UpdateRequest();
        updateRequest.index("test");
        updateRequest.type("employee");
        updateRequest.id("79");
        Map<String, Object> map = new HashMap<>();
        map.put("age", "25");
        map.put("date", "2018-01-01");
        updateRequest.doc(map);
//
//        updateRequest.doc(
//                XContentFactory.jsonBuilder()
//                        .startObject()
//                        // 对没有的字段添加, 对已有的字段更新
//                        .field("age", "26")
//                        .field("city", "shanghai")
//                        .endObject());


        client.update(updateRequest);


    }

    /**
     * 如果document不存在，则创建，否则更新
     *
     * @throws IOException
     * @throws ExecutionException
     * @throws InterruptedException
     */
    @Test
    public void upsert() throws IOException, ExecutionException, InterruptedException {

        IndexRequest indexRequest = new IndexRequest("test", "employee", "70")
                .source(
                        XContentFactory.jsonBuilder()
                                .startObject()
                                .field("name", "lyp")
                                .field("age", "25")
                                .endObject()
                );


        UpdateRequest updateRequest = new UpdateRequest("test", "employee", "70")
                .doc(
                        XContentFactory.jsonBuilder()
                                .startObject()
                                .field("city", "beijing")
                                .field("describe", "beijing is good")
                                .endObject()
                );

        updateRequest.upsert(indexRequest);
        client.update(updateRequest);

    }

    /**
     * 删除
     */
    @Test
    public void delete() {

        DeleteResponse response = client.prepareDelete("test", "employee", "79").execute().actionGet();

        System.out.println(response.isFound());

    }

    /**
     * 批量操作
     *
     * @throws IOException
     */
    @Test
    public void bulk() throws IOException {


        BulkRequestBuilder bulkRequest = client.prepareBulk();

        IndexRequest indexRequest = new IndexRequest("test", "tweet", "333").source(
                XContentFactory.jsonBuilder()
                        .startObject()
                        .field("user", "kimchy")
                        .field("postDate", new Date())
                        .field("message", "trying out Elasticsearch")
                        .endObject());
        //添加一个插入数据操作
        bulkRequest.add(indexRequest);


        //添加一个删除数据操作
        bulkRequest.add(client.prepareDelete("test", "employee", "70"));

        BulkResponse bulkResponse = bulkRequest.get();
        System.out.println(bulkResponse.hasFailures());

    }
}
