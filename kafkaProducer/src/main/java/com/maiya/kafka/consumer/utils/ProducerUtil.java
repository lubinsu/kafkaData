/*
 * Copyright 2015-2020 uuzu.com All right reserved.
 */
package com.maiya.kafka.consumer.utils;

import com.lamfire.logger.Logger;
import com.lamfire.utils.PropertiesUtils;
import com.maiya.kafka.consumer.exception.KafkaException;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * @author xs Api 23, 2016 19:27:10 PM
 */
public class ProducerUtil {

    private final Logger logger = Logger.getLogger(ProducerUtil.class);
    private Producer<String, String> producer = null;
    private List<KeyedMessage<String, String>> list = new ArrayList<>();


    public ProducerUtil() {

    }

    public void init(String path) {
        Properties props = PropertiesUtils.load(path, ProducerUtil.class);
        logger.debug("metadata.broker.list: " + props.getProperty("metadata.broker.list"));

        producer = new Producer<>(new ProducerConfig(props));
        Thread task1 = new TimerSendTask();
        task1.start();
    }

    /**
     * 定时同步任务
     */
    private class TimerSendTask extends Thread {
        public void run() {
            Timer timer = new Timer();
            timer.schedule(new SendTask(), 1000, 5000);
        }
    }


    /**
     * 通用的kafka生产者
     *
     * @param topic   topic name
     * @param key     用于分区的key
     * @param message 消息内容
     */
    public void send(String topic, String key, String message) {
        if (producer == null) {
            throw new KafkaException("kafka生产者未初始化！请调用 init(path) 方法!");
        }
        //批量发送
        try {
            KeyedMessage<String, String> messages = new KeyedMessage<>(topic, key, message);
            logger.debug("kafka send: " + message);

            if (list.size() % 10 == 0 && list.size() != 0) {
                //添加并发送
                sendBatch(1, messages);
            } else {
                //只添加不发送
                sendBatch(0, messages);
            }
            TimeUnit.NANOSECONDS.sleep(10);
        } catch (Exception e) {
            logger.error("Send ListMessage Error.");
        }
    }

    /**
     * 同步代码，加锁，避免重复消费或者丢失数据
     * author ：lubinsu
     * date   : 2016-11-23
     *
     * @param flag 0 只添加不发送 1 添加并发送 2 发送不添加
     * @param msg  发送的消息
     * @throws InterruptedException 异常
     */
    private synchronized void sendBatch(int flag, KeyedMessage<String, String> msg) throws InterruptedException {
        switch (flag) {
            case 0: {
                list.add(msg);
                TimeUnit.MILLISECONDS.sleep(1);
                break;
            }
            case 1: {
                list.add(msg);
                producer.send(list);
                list.clear();
                TimeUnit.MILLISECONDS.sleep(1);
                break;
            }
            case 2: {
                producer.send(list);
                list.clear();
                TimeUnit.MILLISECONDS.sleep(1);
                break;
            }
        }
    }

    private class SendTask extends TimerTask {
        public void run() {
            if (list.size() != 0) {
                try {
                    //发送不添加
                    sendBatch(2, null);
                } catch (Exception e) {
                    logger.error("Timer SendTask Error.");
                }
            }
        }
    }

    public synchronized void close() {
        if (producer != null) {
            producer.close();
            producer = null;
        }
    }

}
