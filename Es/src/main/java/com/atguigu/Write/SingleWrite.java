package com.atguigu.Write;

import com.atguigu.Write.Bean.Source;
import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.core.DocumentResult;
import io.searchbox.core.Index;

import java.io.IOException;

public class SingleWrite {
    public static void main(String[] args) throws IOException {
        //单条数据导入

        //创建JestClientFactory工厂对象
        JestClientFactory jestClientFactory = new JestClientFactory();
        HttpClientConfig httpClientConfig = new HttpClientConfig.Builder("http://hadoop102:9200").build();
        //设置Httpclientconfig参数
        jestClientFactory.setHttpClientConfig(httpClientConfig);
        //获取jestclient对象
        JestClient client = jestClientFactory.getObject();
        //将数据写入es

      //使用Jsonobject这种类型传输数据，也可以使用JavaBean 的方式，或者是map
//        JSONObject source = new JSONObject();
//        source.put("id","2");
//        source.put("name","这个杀手不太冷");




        //使用javabean 的方式进行导入数据
        Source source = new Source();
        source.setId("3");
        source.setName("转角遇到爱");
        Index.Builder builder = new Index.Builder(source);
        Index build = builder.index("movie").type("_doc").id("1003").build();
        DocumentResult documentResult = client.execute(build);




        //关闭client连接
        client.shutdownClient();
    }
}
