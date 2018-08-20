package com.maiya.kafka.consumer;

/**
 * Created by DELL 2016/3/30.
 */

import com.alibaba.fastjson.JSON;
import com.maiya.kafka.consumer.bean.MemberContacts;
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

public class ConsumerMsgTask implements Runnable {
    private KafkaStream m_stream;
    private int m_threadNumber;
    private String m_tableName;

    private static final Logger logger = Logger.getLogger("forKafka");
    //private static com.maiya.log.Logger myLogger = new com.maiya.log.Logger(com.maiya.log.Logger.LogType.KAFKA);
    ArrayList listvec = new ArrayList();
//    ArrayList listvec_dadddate = new ArrayList();


    Object jsonArray = null;
    List<MemberContacts> memberList = null;
    Put p1 = null;
    //    Put p1_dadddate = null;
    HTable hTable = null;
//    HTable hTable_dadddate = null;

    HTableDescriptor htd = null;
    HColumnDescriptor hdc = null;
//    HTableDescriptor htd_dadddate = null;
//    HColumnDescriptor hdc_dadddate = null;

    public ConsumerMsgTask(KafkaStream stream, int threadNumber, String tableName) {
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
        String rowkey_dadddate = null;
        InitHbase initHbase = new InitHbase();
        initHbase.initHbase();
        String familyName = "txjl";
        m_tableName = "hy_membercontacts";
        //com.maiya.log.Logger myLogger = new com.maiya.log.Logger(com.maiya.log.Logger.LogType.KAFKA);
        String m_tableName_dadddate = "ods_hy_membercontacts_dadddate";
        try {
            htd = new HTableDescriptor(m_tableName);
            hdc = new HColumnDescriptor(familyName);
            htd.addFamily(hdc);
            hTable = new HTable(initHbase.configuration, m_tableName);
            hTable.setAutoFlush(false);
            hTable.setWriteBufferSize(50 * 1024 * 1024);
            //设置消息大小

            /** rowkey dadddate,sguid
             htd_dadddate = new HTableDescriptor(m_tableName_dadddate);
             hdc_dadddate = new HColumnDescriptor(familyName);
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
                    //fastjson

                    if (jsonMessage != null && jsonMessage.length() != 0) {
                        try {
                            com.alibaba.fastjson.JSONObject jsonObject = JSON.parseObject(jsonMessage);

                            table_name = jsonObject.getString("table_name").toLowerCase();

                            if (table_name.equals("hy_membercontacts")) {
                                table_status = jsonObject.getString("table_status");
                                jsonArray = jsonObject.get("data");
                                memberList = JSON.parseArray(jsonArray + "", MemberContacts.class);

                                //2、将解析的数据存入hbase中
                                if (table_status.equals("insert")) {

                                    begin_time = System.currentTimeMillis();
                                    //hbase创建表

                                    if (memberList.size() > 0) {
                                        for (int i = 0; i < memberList.size(); i++) {
                                            rowkey = memberList.get(i).getSuserid() + "," + memberList.get(i).getSguid();
                                            p1 = new Put(Bytes.toBytes(rowkey));   //设置rowkey
                                            p1.addColumn(Bytes.toBytes(familyName), Bytes.toBytes("sdeviceno"),
                                                    Bytes.toBytes(memberList.get(i).getSdeviceno()));

                                            p1.addColumn(Bytes.toBytes(familyName), Bytes.toBytes("sguid"),
                                                    Bytes.toBytes(memberList.get(i).getSguid()));
                                            p1.addColumn(Bytes.toBytes(familyName), Bytes.toBytes("suserid"),
                                                    Bytes.toBytes(memberList.get(i).getSuserid()));
                                            p1.addColumn(Bytes.toBytes(familyName), Bytes.toBytes("suserno"),
                                                    Bytes.toBytes(memberList.get(i).getSuserno()));
                                            p1.addColumn(new String(familyName).getBytes(), new String("sname").getBytes(),
                                                    new String(memberList.get(i).getSname()).getBytes());
                                            p1.addColumn(new String(familyName).getBytes(), new String("stelphone").getBytes(),
                                                    new String(memberList.get(i).getStelphone()).getBytes());
                                            p1.addColumn(new String(familyName).getBytes(), new String("sterminalchannel").getBytes(),
                                                    new String(memberList.get(i).getSterminalchannel()).getBytes());
                                            p1.addColumn(new String(familyName).getBytes(), new String("idelflag").getBytes(),
                                                    new String(memberList.get(i).getIdelflag()).getBytes());
                                            p1.addColumn(new String(familyName).getBytes(), new String("dadddate").getBytes(),
                                                    new String(memberList.get(i).getDadddate()).getBytes());
                                            listvec.add(p1);
                                            /** rowkey dadddate,sguid
                                             rowkey_dadddate = memberList.get(i).getDadddate() + "," + memberList.get(i).getSguid();
                                             p1_dadddate = new Put(Bytes.toBytes(rowkey_dadddate));   //设置rowkey
                                             p1_dadddate.addColumn(Bytes.toBytes(familyName), Bytes.toBytes("sdeviceno"),
                                             Bytes.toBytes(memberList.get(i).getSdeviceno()));

                                             p1_dadddate.addColumn(Bytes.toBytes(familyName), Bytes.toBytes("sguid"),
                                             Bytes.toBytes(memberList.get(i).getSguid()));
                                             p1_dadddate.addColumn(Bytes.toBytes(familyName), Bytes.toBytes("suserid"),
                                             Bytes.toBytes(memberList.get(i).getSuserid()));
                                             p1_dadddate.addColumn(Bytes.toBytes(familyName), Bytes.toBytes("suserno"),
                                             Bytes.toBytes(memberList.get(i).getSuserno()));
                                             p1_dadddate.addColumn(new String(familyName).getBytes(), new String("sname").getBytes(),
                                             new String(memberList.get(i).getSname()).getBytes());
                                             p1_dadddate.addColumn(new String(familyName).getBytes(), new String("stelphone").getBytes(),
                                             new String(memberList.get(i).getStelphone()).getBytes());
                                             p1_dadddate.addColumn(new String(familyName).getBytes(), new String("sterminalchannel").getBytes(),
                                             new String(memberList.get(i).getSterminalchannel()).getBytes());
                                             p1_dadddate.addColumn(new String(familyName).getBytes(), new String("idelflag").getBytes(),
                                             new String(memberList.get(i).getIdelflag()).getBytes());
                                             p1_dadddate.addColumn(new String(familyName).getBytes(), new String("dadddate").getBytes(),
                                             new String(memberList.get(i).getDadddate()).getBytes());


                                             listvec_dadddate.add(p1_dadddate); **/
                                        }

//                            ParamerConf paramerConf = new ParamerConf(table_name, familyName);
//                            ParamerConf.hTable.put(listvec);
//                            p1.setWriteToWAL(false);
//
//                            //System.out.println("----------- iflagein : " + String.valueOf(iflagein));
//                            paramerConf.hTable.put(ParamerConf.puts);
//                            paramerConf.hTable.flushCommits();
//                            paramerConf.puts.clear();

                                        hTable.put(listvec);
                                        p1.setWriteToWAL(false);
                                        hTable.flushCommits();

//                                        hTable_dadddate.put(listvec_dadddate);
//                                        p1_dadddate.setWriteToWAL(false);
//                                        hTable_dadddate.flushCommits();

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
//                } else if (table_name.equals("hy_addresslistbw")) {
//                    table_status = jsonObject.getString("table_status");
//                    Object jsonArray = jsonObject.get("data");
//                    List<AddressListBW> memberList = JSON.parseArray(jsonArray + "", AddressListBW.class);
//
//                    //2、将解析的数据存入hbase中
//                    if (table_status.equals("insert")) {
//                        try {
//                            System.out.println("begin to insert " + table_name + " into hbase......");
//                            logger.info("begin to insert  " + table_name + "into hbase");
//                            begin_time = System.currentTimeMillis() / 1000;
//                            logger.info("start insert time: " + begin_time);
//                            //hbase创建表
//
//                            int j = 0;
//                             String familyName = "txlhmd";
//                            System.out.println("memberList size = " + memberList.size());
//                            Put p1 = null;
//                            for (int i = 0; i < memberList.size(); i++) {
//                                p1 = new Put(Bytes.toBytes(memberList.get(i).getsGuid()));   //设置rowkey
//                                p1.addColumn(Bytes.toBytes(familyName), Bytes.toBytes("skeyword"),
//                                        Bytes.toBytes(memberList.get(i).getsKeyWord()));
//                                p1.addColumn(Bytes.toBytes(familyName), Bytes.toBytes("iblackorwhite"),
//                                        Bytes.toBytes(memberList.get(i).getiBlackOrWhite()));
//                                p1.addColumn(Bytes.toBytes(familyName), Bytes.toBytes("iscore"),
//                                        Bytes.toBytes(memberList.get(i).getiScore()));
//                                p1.addColumn(Bytes.toBytes(familyName), Bytes.toBytes("itype"),
//                                        Bytes.toBytes(memberList.get(i).getiType()));
//                                p1.addColumn(Bytes.toBytes(familyName), Bytes.toBytes("sremark"),
//                                        Bytes.toBytes(memberList.get(i).getsRemark()));
//                                p1.addColumn(Bytes.toBytes(familyName), Bytes.toBytes("idelFlag"),
//                                        Bytes.toBytes(memberList.get(i).getiDelFlag()));
//                                p1.addColumn(new String(familyName).getBytes(), new String("daddDate").getBytes(),
//                                        new String(memberList.get(i).getdAddDate()).getBytes());
//                                p1.addColumn(new String(familyName).getBytes(), new String("smodifyperson").getBytes(),
//                                        new String(memberList.get(i).getsModifyPerson()).getBytes());
//                                p1.addColumn(new String(familyName).getBytes(), new String("dmodifydate").getBytes(),
//                                        new String(memberList.get(i).getdModifyDate()).getBytes());
//
//                                listvec.add(p1);
//
//                            }
//                            ParamerConf paramerConf = new ParamerConf(table_name, familyName);
//                            ParamerConf.hTable.put(listvec);
//                            p1.setWriteToWAL(false);
//
//                            //System.out.println("----------- iflagein : " + String.valueOf(iflagein));
//                            paramerConf.hTable.put(ParamerConf.puts);
//                            paramerConf.hTable.flushCommits();
//                            paramerConf.puts.clear();
//                            end_time = System.currentTimeMillis() / 1000;
////                            System.out.println(table_name + " 表插入成功 花费时间 = " + (end_time - begin_time));
//                            logger.info(table_name + " 表插入成功 花费时间 = " + (end_time - begin_time));
//                            TimeUnit.NANOSECONDS.sleep(1);
//
//                        } catch (IOException e) {
//                            e.printStackTrace();
//                            logger.error("该条数据入库异常 : " + jsonArray.toString());
//                        }
//                    }
//                } else if (table_name.equals("hy_smsbw")) {
//                    table_status = jsonObject.getString("table_status");
//                    Object jsonArray = jsonObject.get("data");
//                    List<SMSBW> memberList = JSON.parseArray(jsonArray + "", SMSBW.class);
//
//                    //2、将解析的数据存入hbase中
//                    if (table_status.equals("insert")) {
//                        try {
//                            System.out.println("begin to insert " + table_name + " into hbase......");
//                            logger.info("begin to insert  " + table_name + "into hbase");
//                            begin_time = System.currentTimeMillis() / 1000;
//                            logger.info("start insert time: " + begin_time);
//                            //hbase创建表
//
//                            int j = 0;
//                             String familyName = "dxhmd";
//                            System.out.println("memberList size = " + memberList.size());
//                            Put p1 = null;
//                            for (int i = 0; i < memberList.size(); i++) {
//                                p1 = new Put(Bytes.toBytes(memberList.get(i).getsGuid()));   //设置rowkey
//                                p1.addColumn(Bytes.toBytes(familyName), Bytes.toBytes("skeyword"),
//                                        Bytes.toBytes(memberList.get(i).getsKeyWord()));
//                                p1.addColumn(Bytes.toBytes(familyName), Bytes.toBytes("iblackorwhite"),
//                                        Bytes.toBytes(memberList.get(i).getiBlackOrWhite()));
//                                p1.addColumn(Bytes.toBytes(familyName), Bytes.toBytes("iscore"),
//                                        Bytes.toBytes(memberList.get(i).getiScore()));
//                                p1.addColumn(Bytes.toBytes(familyName), Bytes.toBytes("itype"),
//                                        Bytes.toBytes(memberList.get(i).getiType()));
//                                p1.addColumn(Bytes.toBytes(familyName), Bytes.toBytes("sremark"),
//                                        Bytes.toBytes(memberList.get(i).getsRemark()));
//                                p1.addColumn(Bytes.toBytes(familyName), Bytes.toBytes("idelflag"),
//                                        Bytes.toBytes(memberList.get(i).getiDelFlag()));
//                                p1.addColumn(new String(familyName).getBytes(), new String("dadddate").getBytes(),
//                                        new String(memberList.get(i).getdAddDate()).getBytes());
//                                p1.addColumn(new String(familyName).getBytes(), new String("smodifyperson").getBytes(),
//                                        new String(memberList.get(i).getsModifyPerson()).getBytes());
//                                p1.addColumn(new String(familyName).getBytes(), new String("dmodifydate").getBytes(),
//                                        new String(memberList.get(i).getdModifyDate()).getBytes());
//
//                                listvec.add(p1);
//
//                            }
//                            ParamerConf paramerConf = new ParamerConf(table_name, familyName);
//                            ParamerConf.hTable.put(listvec);
//                            p1.setWriteToWAL(false);
//
//                            //System.out.println("----------- iflagein : " + String.valueOf(iflagein));
//                            paramerConf.hTable.put(ParamerConf.puts);
//                            paramerConf.hTable.flushCommits();
//                            paramerConf.puts.clear();
//                            end_time = System.currentTimeMillis() / 1000;
////                            System.out.println(table_name + " 表插入成功 花费时间 = " + (end_time - begin_time));
//                            logger.info(table_name + " 表插入成功 花费时间 = " + (end_time - begin_time));
//                            TimeUnit.NANOSECONDS.sleep(1);
//
//                        } catch (IOException e) {
//                            e.printStackTrace();
//                            logger.error("该条数据入库异常 : " + jsonArray.toString());
//                        }
//                    }
//
//                }


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