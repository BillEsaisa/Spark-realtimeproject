package com.atguigu.bean

case class AlertLog(mid:String,
                    uids:java.util.HashSet[String],
                    itemIds:java.util.HashSet[String],
                    events:java.util.List[String],
                    ts:Long
                   )
