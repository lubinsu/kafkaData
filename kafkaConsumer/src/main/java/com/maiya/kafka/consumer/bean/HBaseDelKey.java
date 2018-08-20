package com.maiya.kafka.consumer.bean;

/**
 * 复爬 删除key的Bean
 * Created by hujunbiao
 */
public class HBaseDelKey {
    private String key;
    private boolean reserverF = false;
    private String separator ;
    private String index = "0";

    public HBaseDelKey(String key, boolean reserverF, String separator, String index) {
        this.key = key;
        this.reserverF = reserverF;
        this.separator = separator;
        this.index = index;
    }

    public HBaseDelKey(String key) {
        this.key = key;
    }

    public HBaseDelKey(String key, boolean reserverF) {
        this.key = key;
        this.reserverF = reserverF;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public boolean isReserverF() {
        return reserverF;
    }

    public void setReserverF(boolean reserverF) {
        this.reserverF = reserverF;
    }

    public String getSeparator() {
        return separator;
    }

    public void setSeparator(String separator) {
        this.separator = separator;
    }

    public String getIndex() {
        return index;
    }

    public void setIndex(String index) {
        this.index = index;
    }
}
