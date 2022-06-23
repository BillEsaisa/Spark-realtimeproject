package com.atguigu.spring.gmall_publisher.mapper;

import java.util.Map;

public interface DauMapper {
    public Integer selectDauTotal (String date);
    public Map<String,Long> selectDauTotalHourMap(String date);

}
