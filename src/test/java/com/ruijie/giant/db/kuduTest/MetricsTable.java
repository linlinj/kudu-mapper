package com.ruijie.giant.db.kuduTest;

import com.linlinj.db.kudu.annotation.KuduColumnAnnotation;
import com.linlinj.db.kudu.annotation.KuduTableAnnotation;

/**
 * Created by admin on 2018/1/19.
 */
@KuduTableAnnotation(tableName = "metrics")
public class MetricsTable{
    @KuduColumnAnnotation(columnName = "host")
    private String host;
    @KuduColumnAnnotation(columnName ="metric")
    private String metric;
    @KuduColumnAnnotation(columnName ="timestamp")
    private int timestamp;
    @KuduColumnAnnotation(columnName ="value")
    private double value;

    private Integer test;

    public String getHost() {
        return host;
    }

    public String getMetric() {
        return metric;
    }

    public int getTimestamp() {
        return timestamp;
    }

    public double getValue() {
        return value;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public void setMetric(String metric) {
        this.metric = metric;
    }

    public void setTimestamp(int timestamp) {
        this.timestamp = timestamp;
    }

    public void setValue(double value) {
        this.value = value;
    }

    public Integer getTest() {
        return test;
    }

    public void setTest(Integer test) {
        this.test = test;
    }

    @Override
    public String toString() {
        return host+" - "+metric+" - "+String.valueOf(timestamp)+" - "+String.valueOf(value);
    }
}