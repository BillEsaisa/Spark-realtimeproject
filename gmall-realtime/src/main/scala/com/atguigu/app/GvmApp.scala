package com.atguigu.app
import org.apache.phoenix.spark._
import com.alibaba.fastjson.JSON
import com.atguigu.bean.OrderInfo
import com.atguigu.constants.GmallConstants
import com.atguigu.utils.MyKafkaUtil
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object GvmApp {
  def main(args: Array[String]): Unit = {
    //创建环境
    val sc: SparkConf = new SparkConf().setAppName("GvmApp").setMaster("local[*]")
    val ssc = new StreamingContext(sc, Seconds(3))
    //消费KAfka中的数据并封装成样例类
    val Dstreamfromkafka: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getkafkaDstream(ssc, GmallConstants.KAFKA_TOPIC_ORDER, GmallConstants.GROUP_ID)
    //封装样例类将json数据转换成样例类
    val OrderinfoDstream: DStream[OrderInfo] = Dstreamfromkafka.mapPartitions(partition => {
      partition.map(record => {
        val info: OrderInfo = JSON.parseObject(record.value(), classOf[OrderInfo])

        //补全样例类中的字段
        info.create_date = info.create_time.split(" ")(0)
        info.create_hour = info.create_time.split(" ")(1).split(":")(0)

        info
      })
    })

    //将数据保存到hbase里
    OrderinfoDstream.foreachRDD(rdd=>{
      rdd.saveToPhoenix("GMALL0212_ORDER_INFO",Seq[String]("ID", "PROVINCE_ID", "CONSIGNEE", "ORDER_COMMENT", "CONSIGNEE_TEL", "ORDER_STATUS", "PAYMENT_WAY", "USER_ID", "IMG_URL", "TOTAL_AMOUNT", "EXPIRE_TIME", "DELIVERY_ADDRESS", "CREATE_TIME", "OPERATE_TIME", "TRACKING_NO", "PARENT_ORDER_ID", "OUT_TRADE_NO", "TRADE_BODY", "CREATE_DATE", "CREATE_HOUR"
      ),HBaseConfiguration.create(),Some[String]("hadoop102,hadoop103,hadoop104:2181"))
    })

    ssc.start()
    ssc.awaitTermination()





  }

}
