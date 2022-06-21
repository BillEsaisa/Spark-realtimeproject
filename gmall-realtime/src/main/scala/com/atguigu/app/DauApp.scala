package com.atguigu.app
import org.apache.phoenix.spark._
import com.alibaba.fastjson.JSON
import com.atguigu.bean.StartUpLog
import com.atguigu.constants.GmallConstants
import com.atguigu.utils.ConfUtils.KAFKA_BROKER_LIST
import com.atguigu.utils.{MyKafkaUtil, PropertiesUtil}
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import java.text.SimpleDateFormat
import java.util.Date

object DauApp {
  def main(args: Array[String]): Unit = {
    //创建环境、
    val conf: SparkConf = new SparkConf().setAppName("real_time_app").setMaster("local[*]")//最好是kafka有几个分区设置几个excutor
    val ssc = new StreamingContext(conf, Seconds(3))
    //从kafka中消费数据
    val kafkaDstream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getkafkaDstream(ssc, GmallConstants.KAFKA_TOPIC_STARTUP, GmallConstants.GROUP_ID)
    //将kafkaDstream转换成样例类方便后续的处理
    val sdf: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH")
    val StartuplogDstream: DStream[StartUpLog] = kafkaDstream.mapPartitions(partition => {
      val startuplogs: Iterator[StartUpLog] = partition.map(record => {
        val startuplog: StartUpLog = JSON.parseObject(record.value(), classOf[StartUpLog])
        //补充样例类中的字段
        val str: String = sdf.format(new Date(startuplog.ts))
        val logDate: String = str.split(" ")(0)
        val logHour: String = str.split(" ")(1)
        startuplog
      })
      startuplogs
    })

    //将StartuplogDstream进行缓存，后续多次使用
    StartuplogDstream.cache()

    //进行批次间的去重操作（日活需求）
    val DstreamFromrddtordd: DStream[StartUpLog] = DauHandle.Filterbyredis1(StartuplogDstream)


    //进行批次内去重
    val DstreamFromalonerdd: DStream[StartUpLog] = DauHandle.Filterbyalonerdd(DstreamFromrddtordd)

    DstreamFromalonerdd.cache()


    //将经过批次间去重和批次内去重的数据存放在Redis中，方便下一个批次的批次间去重使用
    DauHandle.AstoRedis(DstreamFromalonerdd)

    //将数据保存到Hbase(Phoenix)
    val config= HBaseConfiguration.create()
    DstreamFromrddtordd.foreachRDD(rdd=>{
      rdd.saveToPhoenix(
        "GMALL_0212_DAU",
        Seq[String]("MID", "UID", "APPID", "AREA","OS", "CH", "TYPE", "VS", "LOGDATE", "LOGHOUR", "TS"),
        config,
        Some[String]("hadoop102,hadoop103,hadoop104:2181")
      )
    })



    //启动ssc
    ssc.start()
    //等待并阻塞
    ssc.awaitTermination()

  }


}
