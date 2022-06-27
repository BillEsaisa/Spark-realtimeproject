package com.atguigu.app

import com.alibaba.fastjson.{JSON, JSONObject}
import com.atguigu.bean.{AlertLog, EventLog}
import com.atguigu.constants.GmallConstants
import com.atguigu.utils.{MyKafkaUtil, SaveToEs}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.sql.catalyst.expressions.Second
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Minutes, Seconds, StreamingContext}

import java.text.SimpleDateFormat
import java.util
import java.util.Date
import scala.util.control.Breaks.{break, breakable}

object AlertApp {
  def main(args: Array[String]): Unit = {
    //需求：同一设备，5分钟内三次及以上用不同账号登录并领取优惠劵，并且过程中没有浏览商品。达到以上要求则产生一条预警日志。并且同一设备，每分钟只记录一次预警
    //1.从kafka的主题中消费数据
    //创建环境
    val sc = new SparkConf()
    sc.setMaster("local[*]").setAppName("Alertapp")
    val ssc = new StreamingContext(sc, Seconds(3))
    val fromkafkaDs: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getkafkaDstream(ssc, GmallConstants.KAFKA_TOPIC_EVENT, GmallConstants.GROUP_ID)
    //2.将数据转换成样例类
    val sdf = new SimpleDateFormat("yyyy-MM-dd HH")
    val EventlogDs = fromkafkaDs.mapPartitions(partiton => {
      partiton.map(record => {
        val log: EventLog = JSON.parseObject(record.value(), classOf[EventLog])
        //补全字段

        log.logDate = sdf.format(new Date(log.ts)).split(" ")(0)
        log.logHour = sdf.format(new Date(log.ts)).split(" ")(1)

        (log.mid, log)
      })
    })


    //3.开窗口（时长五分钟）
    val fromwindonDs: DStream[(String, EventLog)] = EventlogDs.window(Minutes(5))

    //4.按照mid就行聚合
    val fromgroupbyDs: DStream[(String, Iterable[EventLog])] = fromwindonDs.groupByKey()

    //5.对同一个聚合组的数据根据uid去重（set），并判断优惠券行为和浏览商品的行为
    val set = new util.HashSet[String]()
    val set1 = new util.HashSet[String]()
    val List = new util.ArrayList[String]()
    var bool: Boolean = true
    val preAlertLog: DStream[(Boolean, AlertLog)] = fromgroupbyDs.mapPartitions(partition => {
      val tuples: Iterator[(Boolean, AlertLog)] = partition.map {
        case (mid, iter) =>

          breakable {
            for (elem <- iter) {
              List.add(elem.evid)
              if ("clickItem".equals(elem.evid)) {
                bool = false
                break()
              } else if ("coupon".equals(elem.evid)) {
                set.add(elem.uid)
                set1.add(elem.itemid)
              }
            }
          }
          ((set.size() >= 3 && bool), AlertLog(mid, set, set1, List, System.currentTimeMillis()))
      }
      tuples
    })

    //6.将预警日志写入es,使用es的docid对预警记录去重，每分钟只保留一条，ID设置mid+精确到分钟的时间
    val value: DStream[AlertLog] = preAlertLog.filter(_._1).map(_._2)
    val simpleDateFormat = new SimpleDateFormat()
    val str: String = simpleDateFormat.format(new Date(System.currentTimeMillis())).split(" ")(0)
    var indexname:String=GmallConstants.INDEX_NAME+"_"+str
    value.foreachRDD(rdd=>{
      rdd.foreachPartition(partiton=>{
        val tuples: List[(String, AlertLog)] = partiton.toList.map(log => {
          val id: String = log.mid + (log.ts / 1000 / 60)
          (id, log)
        })
        SaveToEs.savetoes(indexname,tuples)
      })

    })



  }
}
