package com.maiya.kafka.consumer.bean;

/**
 * Created by DELL on 2016/3/28.
 */
public class CrawlerCarrier {
    private String rawdata;
    private String resultdata;
    private String createtime;
    private String mobile;

    public String getMobile() {
        return mobile;
    }

    public CrawlerCarrier setMobile(String mobile) {
        this.mobile = mobile;
        return this;
    }

    public String getRawdata() {
        return rawdata;
    }

    public CrawlerCarrier setRawdata(String rawdata) {
        this.rawdata = rawdata;
        return this;
    }

    public String getResultdata() {
        return resultdata;
    }

    public CrawlerCarrier setResultdata(String resultdata) {
        this.resultdata = resultdata;
        return this;
    }

    public String getCreatetime() {
        return createtime;
    }

    public CrawlerCarrier setCreatetime(String createtime) {
        this.createtime = createtime;
        return this;
    }
}
