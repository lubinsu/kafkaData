package com.maiya.kafka.consumer;


import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;

/**
 * Created by DELL 2016/3/30.
 * Modified by lubinsu 新数据接入使用 1.0.6
 * 原来的几个老表的接入使用 1.0.5
 * Modified by hujunbiao 增加复爬功能
 */
public class KafkaConsumer extends Thread {
    private final ConsumerConnector consumer;
    private final String topic;
    private final int threads;
    private final int QUEUESIZE = 10;
    private final int COREPOOLSIZE = 10;
    private final int MAXIMUMPOOLSIZE = 30;
    private final long KEEPALIVETIME = 30;
    private String rowkey;
    private String cf;
    private String tableName;

    private String delRowKey;

    public KafkaConsumer(int threads, String a_topic) {
        Properties pro = new Properties();
        FileInputStream in;
        try {
            in = new FileInputStream("bigdata.properties");
            pro.load(in);
        } catch (IOException e) {
            e.printStackTrace();
        }

        consumer = Consumer.createJavaConsumerConnector(createConsumerConfig(pro));
        this.topic = a_topic;
        this.threads = threads;

    }

    public KafkaConsumer(int threads, String a_topic, String rowkey, String cf, String tableName) {
        Properties pro = new Properties();
        FileInputStream in;
        try {
            in = new FileInputStream("bigdata.properties");
            pro.load(in);
        } catch (IOException e) {
            e.printStackTrace();
        }

        consumer = Consumer.createJavaConsumerConnector(createConsumerConfig(pro));
        this.topic = a_topic;
        this.threads = threads;
        this.rowkey = rowkey;
        this.cf = cf;
        this.tableName = tableName;
    }

    private KafkaConsumer(int threads, String a_topic, String tableName, String rowkey, String cf, String groupId) {
        Properties pro = new Properties();
        FileInputStream in;
        try {
            in = new FileInputStream("bigdata.properties");
            pro.load(in);
        } catch (IOException e) {
            e.printStackTrace();
        }
        consumer = Consumer.createJavaConsumerConnector(createConsumerConfig(pro, groupId));
        this.topic = a_topic;
        this.threads = threads;
        this.rowkey = rowkey;
        this.cf = cf;
        this.tableName = tableName;
    }

    private KafkaConsumer(int threads, String a_topic, String tableName, String rowkey, String cf, String groupId,String delRowKey) {
        Properties pro = new Properties();
        FileInputStream in;
        try {
            in = new FileInputStream("bigdata.properties");
            pro.load(in);
        } catch (IOException e) {
            e.printStackTrace();
        }
        consumer = Consumer.createJavaConsumerConnector(createConsumerConfig(pro, groupId));
        this.topic = a_topic;
        this.threads = threads;
        this.rowkey = rowkey;
        this.cf = cf;
        this.tableName = tableName;
        this.delRowKey = delRowKey;
    }

    @Override
    public void run() {
        System.out.println("enter " + topic.concat(":").concat(tableName));
        ExecutorService executorService;
        Map<String, Integer> topicCountMap = new HashMap<>();
        topicCountMap.put(topic, threads);
        Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer.createMessageStreams(topicCountMap);
        List<KafkaStream<byte[], byte[]>> streams = consumerMap.get(topic);

        BlockingQueue<Runnable> queue = new ArrayBlockingQueue<>(QUEUESIZE);
        executorService = new ThreadPoolExecutor(COREPOOLSIZE, MAXIMUMPOOLSIZE, KEEPALIVETIME, TimeUnit.HOURS, queue, new ThreadPoolExecutor.CallerRunsPolicy());

        for (KafkaStream<byte[], byte[]> stream1 : streams) {
            executorService.submit(new ConsumerCommonTask_2(stream1, this.topic, this.tableName, cf, rowkey,delRowKey));
        }

    }

    private static ConsumerConfig createConsumerConfig(Properties pro) {
        return new ConsumerConfig(pro);
    }

    private static ConsumerConfig createConsumerConfig(Properties pro, String groupId) {
        pro.setProperty("group.id", groupId);
        return new ConsumerConfig(pro);
    }


    public static void main(String[] args) {

        int threads = Integer.valueOf(args[0]);

        /*String topic = "hy_membercontacts";
        KafkaConsumer kafkaConsumercon = new KafkaConsumer(threads, topic);
        kafkaConsumercon.start();

        String histop = "hy_membercallhistory";
        KafkaConsumer kafkaConsumerhis = new KafkaConsumer(threads, histop);
        kafkaConsumerhis.start();


        String smstop = "hy_membersms";
        KafkaConsumer kafkaConsumersms = new KafkaConsumer(threads, smstop);
        kafkaConsumersms.start();

        String hy_linkmantop = "hy_linkman";
        KafkaConsumer kafkaConsumerlink = new KafkaConsumer(threads, hy_linkmantop);
        kafkaConsumerlink.start();

        String hy_userinfotop = "hy_userinfo";
        KafkaConsumer kafkaConsumeruser = new KafkaConsumer(threads, hy_userinfotop);
        kafkaConsumeruser.start();

        String hy_innerblacktop = "hy_innerblacklist";
        KafkaConsumer kafkaConsumercom = new KafkaConsumer(threads, hy_innerblacktop);
        kafkaConsumercom.start();

        String mobileblacktop = "hy_mobileblacklist";
        KafkaConsumer kafkaConsumermob = new KafkaConsumer(threads, mobileblacktop);
        kafkaConsumermob.start();

        String crawlerCarrier = "my_crawler_carrier";
        KafkaConsumer kafkaConsumercc = new KafkaConsumer(threads, crawlerCarrier);
        kafkaConsumercc.start();
        */
        Properties pro = new Properties();
        FileInputStream in;
        try {
            in = new FileInputStream("/opt/cloudera/maiya/kafka.tables");
            pro.load(in);
        } catch (IOException e) {
            e.printStackTrace();
        }
        for (Object o : pro.keySet()) {
            String[] tables = pro.get(o).toString().split("#");
            for (String table : tables) {
                String[] dtl = table.split(",");
                //#topic=hbaseTableName,key1:key2,columnFamily,groupId
                //    KafkaConsumer consumer = new KafkaConsumer(threads, o.toString(), dtl[0], dtl[1], dtl[2], dtl[3]);
                //   consumer.start();

                // add hujunbiao
                //#topic=hbaseTableName,key1:key2,columnFamily,groupId
                if (4 == dtl.length) {
                    KafkaConsumer consumer = new KafkaConsumer(threads, o.toString(), dtl[0], dtl[1], dtl[2], dtl[3]);
                    consumer.start();
                } else if (5 == dtl.length) {
                    KafkaConsumer consumer = new KafkaConsumer(threads, o.toString(), dtl[0], dtl[1], dtl[2], dtl[3], dtl[4]);
                    consumer.start();
                }
            }
        }

    }

}