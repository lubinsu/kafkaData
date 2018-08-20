package com.maiya.kafka.consumer.bean;

/**
 * Created by lubin 2017/6/9
 */
public class HBaseKey {
    private String key;
    private Boolean reserverF;

    public HBaseKey(String key, Boolean reserverF) {
        this.key = key;
        this.reserverF = reserverF;
    }

    public String getKey() {
        return key;
    }

    public HBaseKey setKey(String key) {
        this.key = key;
        return this;
    }

    public Boolean getReserverF() {
        return reserverF;
    }

    public HBaseKey setReserverF(Boolean reserverF) {
        this.reserverF = reserverF;
        return this;
    }
}
