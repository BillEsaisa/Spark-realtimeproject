package com.atguigu.Write;

import com.atguigu.Write.Bean.Source;
import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.core.Bulk;
import io.searchbox.core.Index;

import java.io.IOException;

public class BulkWrite {
    public static void main(String[] args) throws IOException {
        //数据的批量写入Es操作
        //1.第一步建立es客户端连接

        JestClientFactory jestClientFactory = new JestClientFactory();
        //重新设置连接的uri,不使用默认的
        HttpClientConfig.Builder builder = new HttpClientConfig.Builder("http://hadoop102:9200");
        jestClientFactory.setHttpClientConfig(builder.build());

        JestClient client = jestClientFactory.getObject();
        //将数据批量写入es
        Bulk.Builder builder1 = new Bulk.Builder();


        Source source1 = new Source();
        source1.setId("3");
        source1.setName("西游记");

        Source source2 = new Source();
        source2.setId("4");
        source2.setName("水浒传");

        Source source3 = new Source();
        source3.setId("5");
        source3.setName("石头记");

        Index.Builder builder2 = new Index.Builder(source1);
        Index index = builder2.index("movie").type("_doc").id("1003").build();
        Index index2 = new Index.Builder(source2).index("movie").type("_doc").id("1004").build();


        Index index3 = new Index.Builder(source3).index("movie").type("_doc").id("1005").build();

        builder1.addAction(index);
        builder1.addAction(index2);
        builder1.addAction(index3);



        client.execute(builder1.build());



        //2.关闭连接，close可能关闭不了，使用shutdown
        client.shutdownClient();


    }
}
