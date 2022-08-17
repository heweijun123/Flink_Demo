package com.hwj.entity;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * @Author: hwj
 * @CreateTime: 2022-08-17  14:11
 * @Version: 1.0
 * @Description: 温度传感器
 */
@Data
@AllArgsConstructor
public class SensorReading {

    private String id;
    private Long timestamp;
    private Double temperature;

}
