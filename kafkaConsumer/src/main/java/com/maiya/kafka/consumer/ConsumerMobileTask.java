package com.maiya.kafka.consumer;

/**
 * Created by DELL 2016/3/30.
 */

import com.alibaba.fastjson.JSON;
import com.maiya.kafka.consumer.bean.MobileBlackList;
import com.maiya.kafka.consumer.hbase.InitHbase;
import com.maiya.kafka.consumer.util.LogUtils;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class ConsumerMobileTask implements Runnable {
    private KafkaStream m_stream;
    private int m_threadNumber;
    private String m_tableName;
    private static int iflagein = 0;
    private static final int insertnum = 1;
    private static final Logger logger = Logger.getLogger("forKafka");
    //private static com.maiya.log.Logger myLogger = new com.maiya.log.Logger(com.maiya.log.Logger.LogType.KAFKA);

    ArrayList listvec = new ArrayList();
    ArrayList listvec_smobile = new ArrayList();
    Object jsonArray = null;
    List<MobileBlackList> memberList = null;
    Put p1 = null;
    Put p1_smobile = null;
    HTable hTable = null;
    HTable hTable_smobile = null;
    HTableDescriptor htd = null;
    HTableDescriptor htd_smobile = null;
    HColumnDescriptor hdc = null;
    HColumnDescriptor hdc_smobile = null;
    String rowkey = null;
    String rowkey_smobile = null;


    public ConsumerMobileTask(KafkaStream stream, int threadNumber, String tableName) {
        m_threadNumber = threadNumber;
        m_stream = stream;
        m_tableName = tableName;
    }

    public void run() {
        String jsonMessage = "";
        String table_status = "";
        String table_name = "";
        long begin_time = 0;
        long end_time = 0;

        InitHbase initHbase = new InitHbase();
        initHbase.initHbase();
        String familyName = "mobhmd";
        m_tableName = "hy_mobileblacklist";
        //com.maiya.log.Logger myLogger = new com.maiya.log.Logger(com.maiya.log.Logger.LogType.KAFKA);

        String m_tableName_smobile = "ods_hy_mobileblacklist_smobile";
        try {
            htd = new HTableDescriptor(m_tableName);
            hdc = new HColumnDescriptor(familyName);
            htd.addFamily(hdc);
            hTable = new HTable(initHbase.configuration, m_tableName);
            hTable.setAutoFlush(false);
            hTable.setWriteBufferSize(50 * 1024 * 1024);
            //设置消息大小

            htd_smobile = new HTableDescriptor(m_tableName_smobile);
            hdc_smobile = new HColumnDescriptor(familyName);
            htd_smobile.addFamily(hdc_smobile);

            hTable_smobile = new HTable(initHbase.configuration, m_tableName_smobile);
            hTable_smobile.setAutoFlush(false);
            hTable_smobile.setWriteBufferSize(50 * 1024 * 1024);


        } catch (IOException e) {
            e.printStackTrace();
        }

        while (true) {
            try {
                ConsumerIterator<byte[], byte[]> it = m_stream.iterator();
                while (it.hasNext()) {
                    jsonMessage = new String(it.next().message());

                    //1、json循环解析表结构数据
                    //fastjson

                    if (jsonMessage != null && jsonMessage.length() != 0) {
                        try {
                            com.alibaba.fastjson.JSONObject jsonObject = JSON.parseObject(jsonMessage);

                            table_name = jsonObject.getString("table_name").toLowerCase();

                            if (table_name.equals("hy_mobileblacklist")) {
                                table_status = jsonObject.getString("table_status");
                                jsonArray = jsonObject.get("data");
                                memberList = JSON.parseArray(jsonArray + "", MobileBlackList.class);

                                //2、将解析的数据存入hbase中
                                if (table_status.equals("insert")) {

                                    begin_time = System.currentTimeMillis();
                                    //hbase创建表

                                    if (memberList.size() > 0) {
                                        for (int i = 0; i < memberList.size(); i++) {
                                            rowkey = memberList.get(i).getsMobile() + "," + memberList.get(i).getsGuid();
                                            p1 = new Put(Bytes.toBytes(rowkey));   //设置rowkey
                                            p1.addColumn(Bytes.toBytes(familyName), Bytes.toBytes("sguid"),
                                                    Bytes.toBytes(memberList.get(i).getsGuid()));
                                            p1.addColumn(Bytes.toBytes(familyName), Bytes.toBytes("smobile"),
                                                    Bytes.toBytes(memberList.get(i).getsMobile()));
                                            p1.addColumn(Bytes.toBytes(familyName), Bytes.toBytes("sremark"),
                                                    Bytes.toBytes(memberList.get(i).getsRemark()));
                                            p1.addColumn(Bytes.toBytes(familyName), Bytes.toBytes("idelflag"),
                                                    Bytes.toBytes(memberList.get(i).getiDelFlag()));
                                            p1.addColumn(Bytes.toBytes(familyName), Bytes.toBytes("saddperson"),
                                                    Bytes.toBytes(memberList.get(i).getsAddPerson()));

                                            p1.addColumn(new String(familyName).getBytes(), new String("dadddate").getBytes(),
                                                    new String(memberList.get(i).getdAddDate()).getBytes());
                                            p1.addColumn(new String(familyName).getBytes(), new String("smodifyperson").getBytes(),
                                                    new String(memberList.get(i).getsModifyPerson()).getBytes());
                                            p1.addColumn(new String(familyName).getBytes(), new String("dmodifydate").getBytes(),
                                                    new String(memberList.get(i).getdModifyDate()).getBytes());

                                            /** rowkey smobile **/
                                            rowkey_smobile = memberList.get(i).getsMobile();
                                            p1_smobile = new Put(Bytes.toBytes(rowkey_smobile));   //设置rowkey
                                            p1_smobile.addColumn(Bytes.toBytes(familyName), Bytes.toBytes("sguid"),
                                                    Bytes.toBytes(memberList.get(i).getsGuid()));
                                            p1_smobile.addColumn(Bytes.toBytes(familyName), Bytes.toBytes("smobile"),
                                                    Bytes.toBytes(memberList.get(i).getsMobile()));
                                            p1_smobile.addColumn(Bytes.toBytes(familyName), Bytes.toBytes("sremark"),
                                                    Bytes.toBytes(memberList.get(i).getsRemark()));
                                            p1_smobile.addColumn(Bytes.toBytes(familyName), Bytes.toBytes("idelflag"),
                                                    Bytes.toBytes(memberList.get(i).getiDelFlag()));
                                            p1_smobile.addColumn(Bytes.toBytes(familyName), Bytes.toBytes("saddperson"),
                                                    Bytes.toBytes(memberList.get(i).getsAddPerson()));

                                            p1_smobile.addColumn(new String(familyName).getBytes(), new String("dadddate").getBytes(),
                                                    new String(memberList.get(i).getdAddDate()).getBytes());
                                            p1_smobile.addColumn(new String(familyName).getBytes(), new String("smodifyperson").getBytes(),
                                                    new String(memberList.get(i).getsModifyPerson()).getBytes());
                                            p1_smobile.addColumn(new String(familyName).getBytes(), new String("dmodifydate").getBytes(),
                                                    new String(memberList.get(i).getdModifyDate()).getBytes());

                                            listvec.add(p1);
                                            listvec_smobile.add(p1_smobile);
                                        }

                                        hTable.put(listvec);
                                        p1.setWriteToWAL(false);
                                        hTable.flushCommits();

                                        hTable_smobile.put(listvec_smobile);
                                        p1_smobile.setWriteToWAL(false);
                                        hTable_smobile.flushCommits();

//                                hTable.close();
                                        end_time = System.currentTimeMillis();
//                            System.out.println(table_name + " 表插入成功 花费时间 = " + (end_time - begin_time));
                                        logger.info(LogUtils.concatLog(m_tableName, m_tableName, end_time - begin_time, "插入/更新成功", jsonMessage, listvec.size(), "SUCCESS"));
                                        //myLogger.kafka(this.getClass().getName(), end_time - begin_time, "成功", "", listvec.size());
                                        listvec.clear();
                                        listvec_smobile.clear();
                                        memberList.clear();
                                        TimeUnit.NANOSECONDS.sleep(1);
                                    }

                                }

                            }
                        } catch (Exception e) {
                            end_time = System.currentTimeMillis();
                            StringBuilder builder = new StringBuilder();
                            for (StackTraceElement stackTraceElement : e.getStackTrace()) {
                                builder.append(stackTraceElement.toString());
                            }
                            logger.error(LogUtils.concatLog(m_tableName, m_tableName, end_time - begin_time, "入库失败", jsonMessage, listvec.size(), e.getMessage().concat(builder.toString())));
                            //myLogger.kafka(this.getClass().getName(), end_time - begin_time, "失败", "【消息】".concat(jsonMessage).concat(";").concat(e.toString()), 0);
                        }
                    } else {
                        logger.error(LogUtils.concatLog(m_tableName, m_tableName, 0, "入库失败", jsonMessage, listvec.size(), "json内容为空"));
                    }
                }
            } catch (Exception e) {
                StringBuilder builder = new StringBuilder();
                for (StackTraceElement stackTraceElement : e.getStackTrace()) {
                    builder.append(stackTraceElement.toString());
                }
                logger.error(LogUtils.concatLog(m_tableName, m_tableName, 0, "入库失败", "", 0, e.getMessage().concat(builder.toString())));
            }
        }
    }

    static public String intToString(int x) {
        String result = String.valueOf(x);
        int size = result.length();
        while (size < 7) {
            size++;
            result = "0" + result;
        }
        return result;
    }
}