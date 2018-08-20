package com.maiya.kafka.consumer.utils;

import com.lamfire.utils.PropertiesUtils;
import org.apache.kafka.clients.producer.*;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Properties;

/**
 * Created by lubin 2017/2/3
 * 图片发送工具类:需要将图片以二进制流的方式读取出来，并通过KafkaProducer发送
 */
public class ImageProducerUtil {

    //需要将图片以二进制流的方式读取出来，并通过KafkaProducer发送
    private final KafkaProducer<String, byte[]> producer;

    private Properties buildKafkaConfig() {
        Properties kafkaPros = new Properties();
        Properties props = PropertiesUtils.load("bigdata.properties", ProducerUtil.class);
        kafkaPros.put(ProducerConfig.ACKS_CONFIG, props.getProperty("request.required.acks"));
        kafkaPros.put("producer.type", "async");
        kafkaPros.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, props.getProperty("bootstrap.servers"));
        kafkaPros.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        kafkaPros.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer");
        kafkaPros.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "gzip");
        kafkaPros.put(ProducerConfig.BATCH_SIZE_CONFIG, "200");
        return kafkaPros;
    }

    public ImageProducerUtil() {

        producer = new KafkaProducer<>(buildKafkaConfig());
    }

    public void send(String topic, String key, byte[] image) {
        ProducerRecord<String, byte[]> messages = new ProducerRecord<>(topic, key, image);
        System.out.println("Sending images started.");
        producer.send(messages,
                new Callback() {
                    public void onCompletion(RecordMetadata metadata, Exception e) {
                        if (e != null)
                            e.printStackTrace();
                        System.out.println("The offset of the record we just sent is: " + metadata.offset());
                    }
                });
        System.out.println("Sending images ended.");

    }

    public void release() {
        producer.close();
    }

    public static void main(String[] args) throws IOException {
        ImageProducerUtil imageProducerUtil = new ImageProducerUtil();

        Path path = Paths.get("E:\\tmp\\160331103440082.png");

        byte[] image = Files.readAllBytes(path);
        imageProducerUtil.send("snoopy", path.getFileName().toString(), image);
        imageProducerUtil.release();
    }
}
