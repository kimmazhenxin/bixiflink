package com.kim.timeservice;

/**
 * @Author: kim
 * @Description:
 * @Date: 14:24 2021/6/30
 * @Version: 1.0
 */
public class SensorReading {

    private String id;

    private Long time;

    private Double temperature;


    public SensorReading() {
    }

    public SensorReading(String id, Long time, Double temperature) {
        this.id = id;
        this.time = time;
        this.temperature = temperature;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public Long getTime() {
        return time;
    }

    public void setTime(Long time) {
        this.time = time;
    }

    public Double getTemperature() {
        return temperature;
    }

    public void setTemperature(Double temperature) {
        this.temperature = temperature;
    }

    @Override
    public String toString() {
        return "SensorReading{" +
                "id='" + id + '\'' +
                ", time=" + time +
                ", temperature=" + temperature +
                '}';
    }
}
