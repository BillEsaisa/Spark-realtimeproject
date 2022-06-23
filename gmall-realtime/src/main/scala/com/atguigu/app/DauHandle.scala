package com.atguigu.app

import com.atguigu.bean.StartUpLog
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import redis.clients.jedis.Jedis

import java.{lang, util}
import java.text.SimpleDateFormat
import java.util.Date

object DauHandle {
  //批次间的去重方法
  def Filterbyredis1(StartuplogDstream: DStream[StartUpLog]): DStream[StartUpLog] = {
    //方案一(在excutor端建立连接，每条记录建立一次连接，可能出现连接关闭不及时的问题导致连接过多)
    val DstreamFromrddtordd: DStream[StartUpLog] = StartuplogDstream.filter(log => {
      //创建redis连接
      val jedis = new Jedis("hadoop102", 6379)
      val boolean: lang.Boolean = jedis.sismember("Dau:" + log.logDate, log.mid)
      jedis.close()
      !boolean
    })
    DstreamFromrddtordd
  }


  //方案二
  //(在rdd分区下建立连接，减少连接数，在excutor端)
  def Filterbyredis2(StartuplogDstream: DStream[StartUpLog]): DStream[StartUpLog] = {
    val DstreamFromrddtordd: DStream[StartUpLog] = StartuplogDstream.mapPartitions(partition => {
      //建立redis连接
      val jedis = new Jedis("hadoop102", 6379)
      val logs: Iterator[StartUpLog] = partition.filter(log => {
        val boolean: lang.Boolean = jedis.sismember("Dau:" + log.logDate, log.mid)
        !boolean
      })
      jedis.close()
      logs
    })

    DstreamFromrddtordd

  }


  //方案三
  //在每个批次下建立连接，减少redis的连接数，操作rdd是在driver端，那么连接也将建立在driver端，因为连接无法实现序列化
  //所以去重的操作也只能在driver端
  private val date = new SimpleDateFormat("yyyy-MM-dd")
  private val str: String = date.format(new Date(System.currentTimeMillis()))


  def Filterbyredis3(StartuplogDstream: DStream[StartUpLog], sc: StreamingContext): DStream[StartUpLog] = {
    val DstreamFromrddtordd: DStream[StartUpLog] = StartuplogDstream.transform(rdd => {
      //建立Jedislianjie
      val jedis = new Jedis("hadoop102", 6379)
      //获取redis 中的mid集合
      val mids: util.Set[String] = jedis.smembers("Dau:" + str)
      //广播mids到excutor端
      val bcmids: Broadcast[util.Set[String]] = sc.sparkContext.broadcast(mids)
      //在excutor端进行去重
      val logs: RDD[StartUpLog] = rdd.filter(log => {
        val bool: Boolean = bcmids.value.contains(log.mid)
        !bool
      })
      jedis.close()
      logs


    })
    DstreamFromrddtordd

  }


  //批次内去重的方法（批次内去重的结果保存到redis中，为下一批次的批次间使用）
  def Filterbyalonerdd(DstreamFromrddtordd: DStream[StartUpLog]): DStream[StartUpLog] = {
    val tupleDstream: DStream[((String, String), StartUpLog)] = DstreamFromrddtordd.map(log => {
      ((log.logDate, log.mid), log)

    })
    val value: DStream[((String, String), List[StartUpLog])] = tupleDstream.groupByKey().mapValues(iter => {
      iter.toList.sortBy(_.ts).take(1)

    })
    val Dstreamalonerdd: DStream[StartUpLog] = value.flatMap(_._2)
    Dstreamalonerdd


  }
  //将经过分区间和分区内去重的结果保存在redis中，为下一次去重做准备
  def AstoRedis (Dstreamalonerdd: DStream[StartUpLog])={
    Dstreamalonerdd.foreachRDD(rdd=>{
      rdd.foreachPartition(partition=>{
        //创建Redis连接
        val jedis = new Jedis("hadoop102", 6379)
        partition.foreach(log=>{
          jedis.sadd("Dau:"+log.logDate,log.mid)
        })
        //关闭连接
        jedis.close()
      })
    })

  }






}










