package com.shsxt.es.web;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.TrueFileFilter;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.cluster.routing.OperationRouting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;





import java.io.File;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;

public class Esutil {

	static final Logger logger = LoggerFactory.getLogger(Esutil.class);

	public static Client client = null;

	public static final String INDEX = "iskandar";
	public static final String TYPE = "web";
	public static final String FILE_PATH = "C:\\Users\\Iskandar\\Desktop\\data";

	/**
	 * 获取客户端
	 *
	 * @return
	 */
	public Client getClient() {
		if (client != null) {
			return client;
		}
		Settings settings = Settings.settingsBuilder().put("cluster.name", "my-application").build();
		try {
			client = TransportClient.builder().settings(settings).build()
					.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("node01"), 9300))
					.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("node02"), 9300))
					.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("node03"), 9300));
		} catch (UnknownHostException e) {
			e.printStackTrace();
		}
		return client;
	}
	/**
	 * 对某个目录下的文件建索引
	 * @throws Exception
	 */
	@Test
	public void createIndex() throws Exception {

		Collection<File> files = FileUtils.listFiles(new File(FILE_PATH), TrueFileFilter.INSTANCE,
				TrueFileFilter.INSTANCE);

		for (File file : files) {
			HtmlBean htmlBean = HtmlUtil.parseHtml(file);
			System.out.println("插入yi个");
			addIndex(htmlBean);
		}

		System.out.println("插入数据成功！！！");

	}


	public void addIndex(HtmlBean htmlBean) {
		HashMap<String, Object> hashMap = new HashMap<String, Object>();

		hashMap.put("title", htmlBean.getTitle());//
		hashMap.put("content", htmlBean.getContent());
		hashMap.put("url", htmlBean.getUrl());

		getClient().prepareIndex(INDEX, TYPE).setSource(hashMap).execute();

	}

	public  PageUtil<HtmlBean> search(String key, int num,int pagesize) {
		SearchRequestBuilder builder = getClient().prepareSearch(INDEX);
		builder.setTypes(TYPE);
		builder.setFrom((num -1)*pagesize);
		builder.setSize(pagesize);
		// 设置高亮字段名称

		builder.addHighlightedField("title");
		builder.addHighlightedField("content");
		//设置显示结果中每个结果最多显示3个碎片段，每个碎片段之间用...隔开
		builder.setHighlighterNumOfFragments(3);
		// 设置高亮前缀
		builder.setHighlighterPreTags("<font color='red' >");
		// 设置高亮后缀
		builder.setHighlighterPostTags("</font>");
		//设置查找的信息域，从title和content里找
		builder.setQuery(QueryBuilders.multiMatchQuery(key, "title", "content"));

		SearchResponse searchResponse = builder.get();

		SearchHits hits = searchResponse.getHits();

		long total = hits.getTotalHits();

		PageUtil<HtmlBean> page = new PageUtil<HtmlBean>(num+"",pagesize+"",(int)total);

		SearchHit[] hits2 = hits.getHits();

		List<HtmlBean> list = new ArrayList<HtmlBean>();

		for (SearchHit hit : hits2) {
			HtmlBean bean = new HtmlBean();

			if(hit.getHighlightFields().get("title")==null){//title中没有包含关键字
				bean.setTitle(hit.getSource().get("title").toString());//获取原来的title（没有高亮的title）
			}else{
				bean.setTitle(hit.getHighlightFields().get("title").getFragments()[0].toString());
			}

			if(hit.getHighlightFields().get("content")==null){//title中没有包含关键字
				bean.setContent(hit.getSource().get("content").toString());//获取原来的content（没有高亮的content）
			}else{
				StringBuilder sb =new StringBuilder();
				for(Text text: hit.getHighlightFields().get("content").getFragments()){
					sb.append(text.toString()+"...");
				}
				bean.setContent(sb.toString());
			}
			bean.setUrl("http://"+hit.getSource().get("url").toString().replace("\\", "/"));
			list.add(bean);
		}
		page.setList(list);
		return page;
	}

}
