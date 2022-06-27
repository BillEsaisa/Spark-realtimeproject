package com.atguigu.utils

import io.searchbox.client.config.HttpClientConfig
import io.searchbox.client.{JestClient, JestClientFactory}
import io.searchbox.core.{Bulk, Index}
import org.eclipse.jetty.io.ClientConnectionFactory

import java.util

object SaveToEs {
  //将数据保存到Es的工具类
  //设置属性
  private  val SERVERURI="http://hadoop102:9200"
  private  val DOC="_doc"
  private var factory:JestClientFactory = null

  def newFactory()={
    if (factory==null){
      val clientFactory = new JestClientFactory
      factory=clientFactory
    }

    factory
  }
//建立连接
  def getclient()={
   val builder = new HttpClientConfig.Builder(SERVERURI)
   newFactory().setHttpClientConfig(builder.build())
     val client: JestClient = newFactory().getObject
      client
  }


  //批量传输到es
 def savetoes(indexname:String,doclist:List[(String,Any)])={

   val bulkbuild = new Bulk.Builder
   bulkbuild.defaultIndex(indexname)
   bulkbuild.defaultType(DOC)
   val indexes = new util.ArrayList[Index]()

   for (elem <- doclist) {
     val indexbuild = new Index.Builder(elem._2)
     if(elem._1!=null){
     indexbuild.id(elem._1)
       val index: Index = indexbuild.build()
       indexes.add(index)
     }
   }

   val bulk: Bulk = bulkbuild.addAction(indexes).build()
  getclient().execute(bulk)

 }


}
