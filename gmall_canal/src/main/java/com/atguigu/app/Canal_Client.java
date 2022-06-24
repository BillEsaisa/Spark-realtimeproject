package com.atguigu.app;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;

import com.atguigu.Utils.MykafkaUtil;
import com.atguigu.constants.GmallConstants;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

import java.net.InetSocketAddress;
import java.util.List;

public class Canal_Client {
    public static void main(String[] args) throws InvalidProtocolBufferException, InterruptedException {
       //从mysql中将新增的业务数据同步到卡夫卡中
       //1.获取canal连接对象
        CanalConnector canalConnector = CanalConnectors.newSingleConnector(new InetSocketAddress("hadoop102", 11111), "example", "", "");

        while (true){
            //指定要监控的数据库
            canalConnector.connect();
            canalConnector.subscribe("gmall0212.*");

            //获取msg
            Message message = canalConnector.get(10);
            //获取entry
            List<CanalEntry.Entry> entries = message.getEntries();
            int size = entries.size();
            if (size==0){
                System.out.println("暂时没有数据");
                Thread.sleep(5000);
            }else {

                //遍历list集合
                for (CanalEntry.Entry entry : entries) {
                    //获取表名
                    String tableName = entry.getHeader().getTableName();
                    //获取entry类型
                    CanalEntry.EntryType entryType = entry.getEntryType();
                    if(CanalEntry.EntryType.ROWDATA.equals(entryType)){
                        //获取序列化的StoreValue
                        ByteString storeValue = entry.getStoreValue();
                        //将Storevalue反序列化
                        CanalEntry.RowChange rowChange = CanalEntry.RowChange.parseFrom(storeValue);
                        //获取事件类型
                        CanalEntry.EventType eventType = rowChange.getEventType();
                        //获取具体数据
                        List<CanalEntry.RowData> rowDatasList = rowChange.getRowDatasList();
                        //根据条件获取需求数据
                        handle( tableName, eventType,  rowDatasList);

                    }

                }

            }

        }





        }
       public  static void handle(String tablename,CanalEntry.EventType eventtype, List<CanalEntry.RowData> rowData){
        if (tablename.equals("order_info")&&CanalEntry.EventType.INSERT.equals(eventtype)){

            JSONObject jsonObject = new JSONObject();

            for (CanalEntry.RowData rowDatum : rowData) {
                List<CanalEntry.Column> afterColumnsList = rowDatum.getAfterColumnsList();
                //获取所有列
                for (CanalEntry.Column column : afterColumnsList) {
                    jsonObject.put(column.getName(),column.getValue());

                }
            }
            String result = jsonObject.toJSONString();
            //发送到kafka
            MykafkaUtil.sendkafka(GmallConstants.KAFKA_TOPIC_ORDER,result);




        }

       }

}

