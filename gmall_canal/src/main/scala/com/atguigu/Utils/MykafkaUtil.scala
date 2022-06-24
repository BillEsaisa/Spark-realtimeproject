package com.atguigu.Utils

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}

import java.util

object MykafkaUtil {
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
    kafkaconfigs.put( ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,Utils(ConfUtils.KAFKA_BROKER_LIST))

    //幂等性
    kafkaconfigs.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG,"true")

    //kv的序列化
    kafkaconfigs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer")
    kafkaconfigs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer")



    new KafkaProducer[String,String](kafkaconfigs)
  }

  //开始生产（就是向kafka发送数据）(按照默认的粘性分区规则分区)
  //
  def sendkafka(topic:String ,value :String): Unit ={
    producer.send(new ProducerRecord[String,String](topic,value))
  }

  //不需要关闭生产者，object 单例对象只有一个生产者对象，调用关闭后，后续无法再使用






}
