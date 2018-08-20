package com.maiya.kafka.consumer.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

/**
 * Created by DELL 2016/7/21.
 */
public class InitHbase {

    public Configuration configuration = null;


    public void initHbase() {
        configuration = HBaseConfiguration.create();
        //生产环境地址
        /*
        configuration.set("hbase.master", "171.16.10.12:60000");
        configuration.set("hbase.zookeeper.quorum","171.16.10.14,171.16.10.15,171.16.10.16");
        configuration.set("hbase.zookeeper.property.clientPort", "2181");
        */

        //测试环境地址
        Properties pro = new Properties();
        FileInputStream in;
        try {
            in = new FileInputStream("bigdata.properties");
            pro.load(in);
        } catch (IOException e) {
            e.printStackTrace();
        }
        configuration.set("hbase.zookeeper.quorum", pro.getProperty("hbase.zookeeper.quorum"));
        configuration.set("hbase.zookeeper.property.clientPort", pro.getProperty("hbase.zookeeper.property.clientPort"));
    }

}
