package com.maiya.kafka.consumer;

/**
 * Created by DELL on 2016/3/30.
 * modified by lubinsu on 20170210 优化
 */

import com.alibaba.fastjson.JSON;
import com.maiya.kafka.consumer.bean.LinkMan;
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

public class ConsumerLinkManTask implements Runnable {
    private KafkaStream m_stream;
    private int m_threadNumber;
    private String m_tableName;
    private static int iflagein = 0;
    private static final int insertnum = 1;
    private static final Logger logger = Logger.getLogger("forKafka");
    //private static com.maiya.log.Logger myLogger = new com.maiya.log.Logger(com.maiya.log.Logger.LogType.KAFKA);

    ArrayList listvec = new ArrayList();
    Object jsonArray = null;
    List<LinkMan> memberList = null;
    Put p1 = null;
    HTable hTable = null;
    HTableDescriptor htd = null;
    HColumnDescriptor hdc = null;


    public ConsumerLinkManTask(KafkaStream stream, int threadNumber, String tableName) {
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
        String rowkey = null;
        InitHbase initHbase = new InitHbase();
        initHbase.initHbase();
        String familyName = "lxr";
        m_tableName = "hy_linkman";
        //com.maiya.log.Logger myLogger = new com.maiya.log.Logger(com.maiya.log.Logger.LogType.KAFKA);
        try {
            HTableDescriptor htd = new HTableDescriptor(m_tableName);
            HColumnDescriptor hdc = new HColumnDescriptor(familyName);
            htd.addFamily(hdc);
            hTable = new HTable(initHbase.configuration, m_tableName);
            hTable.setAutoFlush(false);
            hTable.setWriteBufferSize(50 * 1024 * 1024);
            //设置消息大小

        } catch (IOException e) {
            e.printStackTrace();
        }

        while (true) {
            try {
                ConsumerIterator<byte[], byte[]> it = m_stream.iterator();
                while (it.hasNext()) {
                    jsonMessage = new String(it.next().message());
                    /*System.out.println("=========" + jsonMessage);*/

                    //1、json循环解析表结构数据
                    //fastjson
                    if (jsonMessage != null && jsonMessage.length() != 0) {
                        try {
                            com.alibaba.fastjson.JSONObject jsonObject = JSON.parseObject(jsonMessage);

                            table_name = jsonObject.getString("table_name").toLowerCase();

                            if (table_name.equals("hy_linkman")) {
                                table_status = jsonObject.getString("table_status");
                                jsonArray = jsonObject.get("data");
                                memberList = JSON.parseArray(jsonArray + "", LinkMan.class);

                                //2、将解析的数据存入hbase中
                                if (table_status.equals("insert")) {

                                    begin_time = System.currentTimeMillis();
                                    //hbase创建表
                                    if (memberList.size() > 0) {
                                        for (int i = 0; i < memberList.size(); i++) {
                                            rowkey = memberList.get(i).getsUserId() + "," + memberList.get(i).getsGuid();
                                            p1 = new Put(Bytes.toBytes(rowkey));   //设置rowkey

                                            p1.addColumn(Bytes.toBytes(familyName), Bytes.toBytes("sguid"),
                                                    Bytes.toBytes(memberList.get(i).getsGuid()));
                                            p1.addColumn(Bytes.toBytes(familyName), Bytes.toBytes("suserid"),
                                                    Bytes.toBytes(memberList.get(i).getsUserId()));

                                            p1.addColumn(Bytes.toBytes(familyName), Bytes.toBytes("suserno"),
                                                    Bytes.toBytes(memberList.get(i).getsUserNo()));
                                            p1.addColumn(Bytes.toBytes(familyName), Bytes.toBytes("ilinklevel"),
                                                    Bytes.toBytes(memberList.get(i).getiLinkLevel()));
                                            p1.addColumn(Bytes.toBytes(familyName), Bytes.toBytes("iseq"),
                                                    Bytes.toBytes(memberList.get(i).getiSeq()));
                                            p1.addColumn(new String(familyName).getBytes(), new String("sname").getBytes(),
                                                    new String(memberList.get(i).getsName()).getBytes());
                                            p1.addColumn(new String(familyName).getBytes(), new String("irelationship").getBytes(),
                                                    new String(memberList.get(i).getiRelationship()).getBytes());
                                            p1.addColumn(new String(familyName).getBytes(), new String("smobile").getBytes(),
                                                    new String(memberList.get(i).getsMobile()).getBytes());
                                            p1.addColumn(new String(familyName).getBytes(), new String("icheckresult").getBytes(),
                                                    new String(memberList.get(i).getiCheckResult()).getBytes());
                                            p1.addColumn(new String(familyName).getBytes(), new String("scheckperson").getBytes(),
                                                    new String(memberList.get(i).getsCheckPerson()).getBytes());
                                            p1.addColumn(new String(familyName).getBytes(), new String("scheckremark").getBytes(),
                                                    new String(memberList.get(i).getsCheckRemark()).getBytes());
                                            p1.addColumn(new String(familyName).getBytes(), new String("scheckdate").getBytes(),
                                                    new String(memberList.get(i).getsCheckDate()).getBytes());
                                            p1.addColumn(new String(familyName).getBytes(), new String("idelflag").getBytes(),
                                                    new String(memberList.get(i).getiDelFlag()).getBytes());
                                            p1.addColumn(new String(familyName).getBytes(), new String("saddperson").getBytes(),
                                                    new String(memberList.get(i).getsAddPerson()).getBytes());
                                            p1.addColumn(new String(familyName).getBytes(), new String("dadddate").getBytes(),
                                                    new String(memberList.get(i).getdAddDate()).getBytes());
                                            p1.addColumn(new String(familyName).getBytes(), new String("sipaddress").getBytes(),
                                                    new String(memberList.get(i).getsIpAddress()).getBytes());
                                            p1.addColumn(new String(familyName).getBytes(), new String("smodifyperson").getBytes(),
                                                    new String(memberList.get(i).getsModifyPerson()).getBytes());
                                            p1.addColumn(new String(familyName).getBytes(), new String("dmodifydate").getBytes(),
                                                    new String(memberList.get(i).getdModifyDate()).getBytes());
                                            listvec.add(p1);

                                        }

                                        hTable.put(listvec);
                                        p1.setWriteToWAL(false);
                                        hTable.flushCommits();
//                                hTable.close();
                                        end_time = System.currentTimeMillis();
                                        logger.info(LogUtils.concatLog(m_tableName, m_tableName, end_time - begin_time, "插入/更新成功", jsonMessage, listvec.size(), "SUCCESS"));
                                        //myLogger.kafka(this.getClass().getName(), end_time - begin_time, "成功", "", listvec.size());
                                        listvec.clear();
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