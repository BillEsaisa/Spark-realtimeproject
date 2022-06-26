package com.atguigu.Read;

import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.core.DocumentResult;
import io.searchbox.core.Get;
import io.searchbox.core.Search;
import io.searchbox.core.SearchResult;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class Read {
    public static void main(String[] args) throws IOException {
        //创建es客户端连接
        JestClientFactory jestClientFactory = new JestClientFactory();
        HttpClientConfig.Builder builder = new HttpClientConfig.Builder("http://hadoop102:9200");
        jestClientFactory.setHttpClientConfig(builder.build());
        JestClient client = jestClientFactory.getObject();
        //读取数据
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
        TermQueryBuilder term  = new TermQueryBuilder("id","1");
        boolQueryBuilder.filter(term);
        SearchSourceBuilder query = searchSourceBuilder.query(boolQueryBuilder);






        Search.Builder builder1 = new Search.Builder(searchSourceBuilder.toString());
        builder1.addType("_doc").addIndex("movie");

        Search search = builder1.build();
        SearchResult result = client.execute(search);

        Long total = result.getTotal();
        List<SearchResult.Hit<Map, Void>> hits = result.getHits(Map.class);
        for (SearchResult.Hit<Map, Void> hit : hits) {
            Map source = hit.source;
            for (Object o : source.keySet()) {
                System.out.println(o+":"+source.get(o));
            }
        }


        //关闭连接
        client.shutdownClient();
    }
}
