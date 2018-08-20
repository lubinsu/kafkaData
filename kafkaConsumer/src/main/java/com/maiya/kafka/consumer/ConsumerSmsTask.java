package com.maiya.kafka.consumer;

/**
 * Created by DELL 2016/3/30.
 */

import com.alibaba.fastjson.JSON;
import com.maiya.kafka.consumer.bean.MemberSms;
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

public class ConsumerSmsTask implements Runnable {
    private KafkaStream m_stream;
    private int m_threadNumber;
    private String m_tableName;
    private static int iflagein = 0;
    private static final int insertnum = 1;
    private static final Logger logger = Logger.getLogger("forKafka");
    //private static com.maiya.log.Logger myLogger = new com.maiya.log.Logger(com.maiya.log.Logger.LogType.KAFKA);
    ArrayList listvec = new ArrayList();
//    ArrayList listvec_dadddate = new ArrayList();
    Object jsonArray = null;
    List<MemberSms> memberList = null;
    Put p1 = null;
//    Put p1_dadddate = null;
    HTable hTable = null;
//    HTable hTable_dadddate = null;

    HTableDescriptor htd = null;
    HColumnDescriptor hdc = null;


    public ConsumerSmsTask(KafkaStream stream, int threadNumber, String tableName) {
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
//        String rowkey_dadddate = null;

        InitHbase initHbase = new InitHbase();
        initHbase.initHbase();
        String familyName = "dxjl";
        m_tableName = "hy_membersms";
        String m_tableName_dadddate = "ods_hy_membersms_dadddate";


        try {
            /** rowkey sguid **/
            HTableDescriptor htd = new HTableDescriptor(m_tableName);
            HColumnDescriptor hdc = new HColumnDescriptor(familyName);
            htd.addFamily(hdc);
            hTable = new HTable(initHbase.configuration, m_tableName);
            hTable.setAutoFlush(false);
            hTable.setWriteBufferSize(50 * 1024 * 1024);
            //设置消息大小

            /** rowkey dadddate,sguid
            HTableDescriptor htd_dadddate = new HTableDescriptor(m_tableName_dadddate);
            HColumnDescriptor hdc_dadddate = new HColumnDescriptor(familyName);
            htd_dadddate.addFamily(hdc_dadddate);
            hTable_dadddate = new HTable(initHbase.configuration, m_tableName_dadddate);
            hTable_dadddate.setAutoFlush(false);
            hTable_dadddate.setWriteBufferSize(50 * 1024 * 1024);
             **/

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

                            if (table_name.equals("hy_membersms")) {
                                table_status = jsonObject.getString("table_status");
                                jsonArray = jsonObject.get("data");
                                memberList = JSON.parseArray(jsonArray + "", MemberSms.class);

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
                                            p1.addColumn(new String(familyName).getBytes(), new String("sname").getBytes(),
                                                    new String(memberList.get(i).getsName()).getBytes());

                                            p1.addColumn(new String(familyName).getBytes(), new String("dsmsdate").getBytes(),
                                                    new String(memberList.get(i).getdSMSDate()).getBytes());
                                            p1.addColumn(new String(familyName).getBytes(), new String("sdeviceno").getBytes(),
                                                    new String(memberList.get(i).getsDeviceNo()).getBytes());
                                            p1.addColumn(new String(familyName).getBytes(), new String("ssendmobile").getBytes(),
                                                    new String(memberList.get(i).getsSendMobile()).getBytes());
                                            p1.addColumn(new String(familyName).getBytes(), new String("srecmobile").getBytes(),
                                                    new String(memberList.get(i).getsRecMobile()).getBytes());

                                            p1.addColumn(Bytes.toBytes(familyName), Bytes.toBytes("ssmscontent"),
                                                    Bytes.toBytes(memberList.get(i).getsSMSContent()));
                                            p1.addColumn(new String(familyName).getBytes(), new String("idelflag").getBytes(),
                                                    new String(memberList.get(i).getiDelFlag()).getBytes());
                                            p1.addColumn(new String(familyName).getBytes(), new String("saddperson").getBytes(),
                                                    new String(memberList.get(i).getsAddPerson()).getBytes());
                                            p1.addColumn(new String(familyName).getBytes(), new String("dadddate").getBytes(),
                                                    new String(memberList.get(i).getdAddDate()).getBytes());
                                            p1.addColumn(new String(familyName).getBytes(), new String("smodifyperson").getBytes(),
                                                    new String(memberList.get(i).getsModifyPerson()).getBytes());
                                            p1.addColumn(new String(familyName).getBytes(), new String("dmodifydate").getBytes(),
                                                    new String(memberList.get(i).getdModifyDate()).getBytes());

                                            listvec.add(p1);

                                            /*rowkey_dadddate = memberList.get(i).getdAddDate() + "," + memberList.get(i).getsGuid();

                                            p1_dadddate = new Put(Bytes.toBytes(rowkey_dadddate));   //设置rowkey
                                            p1_dadddate.addColumn(Bytes.toBytes(familyName), Bytes.toBytes("sguid"),
                                                    Bytes.toBytes(memberList.get(i).getsGuid()));
                                            p1_dadddate.addColumn(Bytes.toBytes(familyName), Bytes.toBytes("suserid"),
                                                    Bytes.toBytes(memberList.get(i).getsUserId()));
                                            p1_dadddate.addColumn(Bytes.toBytes(familyName), Bytes.toBytes("suserno"),
                                                    Bytes.toBytes(memberList.get(i).getsUserNo()));
                                            p1_dadddate.addColumn(new String(familyName).getBytes(), new String("sname").getBytes(),
                                                    new String(memberList.get(i).getsName()).getBytes());

                                            p1_dadddate.addColumn(new String(familyName).getBytes(), new String("dsmsdate").getBytes(),
                                                    new String(memberList.get(i).getdSMSDate()).getBytes());
                                            p1_dadddate.addColumn(new String(familyName).getBytes(), new String("sdeviceno").getBytes(),
                                                    new String(memberList.get(i).getsDeviceNo()).getBytes());
                                            p1_dadddate.addColumn(new String(familyName).getBytes(), new String("ssendmobile").getBytes(),
                                                    new String(memberList.get(i).getsSendMobile()).getBytes());
                                            p1_dadddate.addColumn(new String(familyName).getBytes(), new String("srecmobile").getBytes(),
                                                    new String(memberList.get(i).getsRecMobile()).getBytes());

                                            p1_dadddate.addColumn(Bytes.toBytes(familyName), Bytes.toBytes("ssmscontent"),
                                                    Bytes.toBytes(memberList.get(i).getsSMSContent()));
                                            p1_dadddate.addColumn(new String(familyName).getBytes(), new String("idelflag").getBytes(),
                                                    new String(memberList.get(i).getiDelFlag()).getBytes());
                                            p1_dadddate.addColumn(new String(familyName).getBytes(), new String("saddperson").getBytes(),
                                                    new String(memberList.get(i).getsAddPerson()).getBytes());
                                            p1_dadddate.addColumn(new String(familyName).getBytes(), new String("dadddate").getBytes(),
                                                    new String(memberList.get(i).getdAddDate()).getBytes());
                                            p1_dadddate.addColumn(new String(familyName).getBytes(), new String("smodifyperson").getBytes(),
                                                    new String(memberList.get(i).getsModifyPerson()).getBytes());
                                            p1_dadddate.addColumn(new String(familyName).getBytes(), new String("dmodifydate").getBytes(),
                                                    new String(memberList.get(i).getdModifyDate()).getBytes());



                                            listvec_dadddate.add(p1_dadddate);*/

                                        }

                                        hTable.put(listvec);
                                        p1.setWriteToWAL(false);
                                        hTable.flushCommits();

                                        /*hTable_dadddate.put(listvec_dadddate);
                                        p1_dadddate.setWriteToWAL(false);
                                        hTable_dadddate.flushCommits();*/

//                                hTable.close();
                                        end_time = System.currentTimeMillis();
//                            System.out.println(table_name + " 表插入成功 花费时间 = " + (end_time - begin_time));
                                        logger.info(LogUtils.concatLog(m_tableName, m_tableName, end_time - begin_time, "插入/更新成功", jsonMessage, listvec.size(), "SUCCESS"));
                                        //myLogger.kafka(this.getClass().getName(), end_time - begin_time, "成功", "", listvec.size());
                                        listvec.clear();
//                                        listvec_dadddate.clear();
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