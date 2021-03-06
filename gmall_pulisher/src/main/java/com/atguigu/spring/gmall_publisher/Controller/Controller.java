package com.atguigu.spring.gmall_publisher.Controller;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.spring.gmall_publisher.service.Publisher;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

@RestController
public class Controller {
    @Autowired
    Publisher publisher;
    //Controller层，处理请求
    @RequestMapping("realtime-total")
    public String realtimetotal (@RequestParam("date") String date){
    //从service层获取日活总数
        Integer dauTotal = publisher.getDauTotal(date);
        //从service层获取新增交易额数据
        Double orderAmountTotal = publisher.getOrderAmountTotal(date);
        //封装数据
        ArrayList<Map> mapArrayList = new ArrayList<>();
        HashMap<String, Object> hashMap = new HashMap<>();
        HashMap<String, Object> hashMap1 = new HashMap<>();
        HashMap<String, Object> hashMap2 = new HashMap<>();
        hashMap.put("id","dau");
        hashMap.put("name","新增日活");
        hashMap.put("value",dauTotal);

        hashMap1.put("id","new_mid");
        hashMap1.put("name","新增设备");
        hashMap1.put("value",233);

        hashMap2.put("id","order_amount");
        hashMap2.put("name","新增交易额");
        hashMap2.put("value",orderAmountTotal);

        mapArrayList.add(hashMap);
        mapArrayList.add(hashMap1);
        mapArrayList.add(hashMap2);
        String result = JSONObject.toJSONString(mapArrayList);
        return result;

    }
    @RequestMapping("realtime-hours")
    public String realtimehours(@RequestParam("id") String id,@RequestParam("date") String date){
        HashMap<String, Object> bigmap = new HashMap<>();
        LocalDate yesterday = LocalDate.parse(date).plusDays(-1);
        String yesterdays = yesterday.toString();
        if (id=="order_amount"){
            //获取今天的分时交易数据
            Map<String, Object> orderAmountHourMap = publisher.getOrderAmountHourMap(date);
            //获取昨天的分时交易数据
            Map<String, Object> yesmap = publisher.getOrderAmountHourMap(yesterdays);
            //封装结果
            bigmap.put("today",orderAmountHourMap);
            bigmap.put("yesterday",yesmap);
            //转换格式
            String result = JSONObject.toJSONString(bigmap);
            return result;

        }else {
            //获取今天的时活
            Map<String, Long> today = publisher.getDauHour(date);
            //获取昨天的分时数据
            Map<String, Long> ysterd = publisher.getDauHour(yesterdays);
            //封装最终的输出数据格式
            bigmap.put(date, today);
            bigmap.put(yesterdays, ysterd);
            String result = JSONObject.toJSONString(bigmap);
            return result;
        }

    }

}
