package com.maiya.kafka.consumer.utils;

/**
 * Created by lubin 2016/11/25
 * 根据征信那边写的案例
 */
public class KafkaUtil {

    public static ProducerUtil producerUtil = new ProducerUtil();

    public static ProducerUtil getProducerutil() {
        if(producerUtil == null){
            return new ProducerUtil();
        }
        return producerUtil;
    }

    @SuppressWarnings("static-access")
    public static void closeProducerutil() {
        if(producerUtil != null){
            producerUtil.close();
        }
    }
}