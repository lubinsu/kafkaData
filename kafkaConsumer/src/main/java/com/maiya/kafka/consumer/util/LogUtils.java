package com.maiya.kafka.consumer.util;

import org.joda.time.DateTime;

/**
 * Created by lubin 2017/2/10
 */
public class LogUtils {

    public static String concatLog(String topic, String tabName
            , long elapsed, String status, String json
            , int size, String dtl) {

        return DateTime.now().toString("yyyy-MM-dd HH:mm:ss.S").concat("\t").concat(topic.concat("\t").concat(tabName).concat("\t").concat(String.valueOf(elapsed).concat("\t").concat(status).concat("\t").concat(json).concat("\t").concat(String.valueOf(size)).concat("\t").concat(dtl)));
    }
}
