package com.atguigu.spring.gmall_publisher.service;

import java.util.Map;

public interface Publisher{
    //获取日活总数
    public Integer getDauTotal(String date);
    public Map<String,Long> getDauHour(String date);
}
