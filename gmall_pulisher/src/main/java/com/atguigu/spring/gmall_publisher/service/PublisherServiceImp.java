package com.atguigu.spring.gmall_publisher.service;

import com.atguigu.spring.gmall_publisher.mapper.DauMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Map;

@Service
public class PublisherServiceImp implements Publisher {
    @Autowired
    DauMapper dauMapper;

    @Override
    public Integer getDauTotal(String date){
        return dauMapper.selectDauTotal(date);
    }

    public Map<String,Long> getDauHour(String date){
        return dauMapper.selectDauTotalHourMap(date);
    }
}
