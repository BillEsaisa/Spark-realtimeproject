package com.atguigu.spring.gmall_publisher.service;

import com.atguigu.spring.gmall_publisher.mapper.DauMapper;
import com.atguigu.spring.gmall_publisher.mapper.OrderMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public class PublisherServiceImp implements Publisher {

    @Autowired
    DauMapper dauMapper;
    OrderMapper orderMapper;

    @Override
    public Integer getDauTotal(String date){
        return dauMapper.selectDauTotal(date);
    }

    public Map<String,Long> getDauHour(String date){
        HashMap<String, Long> list = new HashMap<>();
        List<Map> maps = dauMapper.selectDauTotalHourMap(date);
        for (Map map : maps) {
            list.put((String) map.get("LH"),(Long) map.get("CT"));
        }
        return list;
    }

    @Override
    public Double getOrderAmountTotal(String date) {
       return orderMapper.selectOrderAmountTotal(date);

    }

    @Override
    public Map<String, Object> getOrderAmountHourMap(String date) {
        HashMap<String, Object> hashmap = new HashMap<>();
        List<Map> maps = orderMapper.selectOrderAmountHourMap(date);
        for (Map map : maps) {
            hashmap.put((String) map.get("CREATE_HOUR"),map.get("SUM_AMOUNT"));
        }
        return hashmap;
    }
}
