package com.maiya.kafka.consumer;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.*;

/**
 * Created by hujunbiao on 2017/12/22.
 */
public class KafkaCommonConsumer extends Thread {
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


    public KafkaCommonConsumer(int threads, String a_topic) {
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

    public KafkaCommonConsumer(int threads, String a_topic, String rowkey, String cf, String tableName) {
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

    private KafkaCommonConsumer(int threads, String a_topic, String tableName, String rowkey, String cf, String groupId) {
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

    private KafkaCommonConsumer(int threads, String a_topic, String tableName, String rowkey, String cf, String groupId,String delRowKey) {
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


    public static void main(String[] args) throws Exception {
        int threads = Integer.valueOf(args[0]);
        Properties pro = new Properties();
        FileInputStream in;
        try {
            in = new FileInputStream("kafka.tables");
            pro.load(in);
        } catch (IOException e) {
            e.printStackTrace();
        }



        for (Object o : pro.keySet()) {
            String[] tables = pro.get(o).toString().split("#");
            for (String table : tables) {
                String[] dtl = table.split(",");
                //#topic=hbaseTableName,key1:key2,columnFamily,groupId
                //KafkaCommonConsumer consumer = new KafkaCommonConsumer(threads, o.toString(), dtl[0], dtl[1], dtl[2], dtl[3]);
                //consumer.start();

                // add hujunbiao
                //#topic=hbaseTableName,key1:key2,columnFamily,groupId
                if (4 == dtl.length) {
                    KafkaCommonConsumer consumer = new KafkaCommonConsumer(threads, o.toString(), dtl[0], dtl[1], dtl[2], dtl[3]);
                    consumer.start();
                } else if (5 == dtl.length) {
                    KafkaCommonConsumer consumer = new KafkaCommonConsumer(threads, o.toString(), dtl[0], dtl[1], dtl[2], dtl[3], dtl[4]);
                    consumer.start();
                }
            }
        }

//        List<String> lines = Files.readAllLines(Paths.get("/opt/cloudera/maiya/kafka.tables"), StandardCharsets.UTF_8);
//        for (String line : lines) {
//            String[] tables = line.split("#",-1);
//        }

    }

}