package com.atguigu.utils


import com.atguigu.utils.ConfUtils.KAFKA_BROKER_LIST
import org.apache.commons.lang.mutable.Mutable
import org.apache.kafka.clients.consumer
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}

import java.util
import scala.collection.mutable

object MyKafkaUtil {
  //Kafka工具类


  //kafka消费参数
  val kafkaparas :mutable.Map[String ,Object]=mutable.Map[String,Object](
    //kafka集群位置
    ConsumerConfig.GROUP_ID_CONFIG -> PropertiesUtil(KAFKA_BROKER_LIST),
    //消费者组id
    //ConsumerConfig.GROUP_ID_CONFIG->"bigdata0212",
    //kv的反序列化
    ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringDeserializer",
    ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringDeserializer",
    //offset的提交方式
    ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> "true",
    //offset重置
    ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "latest"
    //...
  )
  //消费kafka，基于spark消费,获取kafkaDstream
  def getkafkaDstream (ssc:StreamingContext,topic:String,Groupid:String)={
    kafkaparas.put(ConsumerConfig.GROUP_ID_CONFIG,Groupid)
    val kafkaDsteam: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream(ssc, LocationStrategies.PreferConsistent, ConsumerStrategies.Subscribe[String, String](Array(topic), kafkaparas))
    kafkaDsteam
  }



  //作为Kafka生产者向kafka生产数据
  //定义生产者对象
  val producer :KafkaProducer[String,String]={
    if (producer==null){
      createkafkaproducer()
    }
    producer
  }
  //创建生产者对象
  def createkafkaproducer():KafkaProducer[String,String]={
    val kafkaconfigs :util.HashMap[String,AnyRef]= new util.HashMap[String,AnyRef]
      //生产者提交应答机制
      kafkaconfigs.put( ProducerConfig.ACKS_CONFIG,"all")

      //生产者连接kafka的集群节点
    kafkaconfigs.put( ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,PropertiesUtil(ConfUtils.KAFKA_BROKER_LIST))

      //幂等性
    kafkaconfigs.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG,"true")

      //kv的序列化
    kafkaconfigs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringDeserializer")
     kafkaconfigs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringDeserializer")



    new KafkaProducer[String,String](kafkaconfigs)
  }

  //开始生产（就是向kafka发送数据）(按照默认的粘性分区规则分区)
  //
  def sendkafka(topic:String ,value :String): Unit ={
    producer.send(new ProducerRecord[String,String](topic,value))
  }

  //不需要关闭生产者，object 单例对象只有一个生产者对象，调用关闭后，后续无法再使用







}
