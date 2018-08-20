package com.maiya.kafka.consumer;

/**
 * Created by DELL on 2016/3/30.
 */

import com.alibaba.fastjson.JSON;
import com.maiya.kafka.consumer.bean.UserInfo;
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

public class ConsumerUserInfoTask implements Runnable {
    private KafkaStream m_stream;
    private int m_threadNumber;
    private String m_tableName;
    private static int iflagein = 0;
    private static final int insertnum = 1;
    private static final Logger logger = Logger.getLogger("forKafka");
    //private static com.maiya.log.Logger myLogger = new com.maiya.log.Logger(com.maiya.log.Logger.LogType.KAFKA);

    Object jsonArray = null;
    List<UserInfo> memberList = null;

    ArrayList listvec = new ArrayList();
    ArrayList listvec_sguid = new ArrayList();
    ArrayList listvec_smobile = new ArrayList();
    //ArrayList listvec_sidno = new ArrayList();

    Put p1 = null;
    Put p1_sguid = null;
    Put p1_smobile = null;
    //Put p1_sidno = null;

    HTable hTable = null;
    HTable hTable_sguid = null;
    HTable hTable_smobile = null;
    //HTable hTable_sidno = null;

    HTableDescriptor htd = null;
    HColumnDescriptor hdc = null;

    public ConsumerUserInfoTask(KafkaStream stream, int threadNumber, String tableName) {
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
        String familyName = "yhxx";
        m_tableName = "hy_userinfo";

        String rowkey = null;
        String rowkey_sguid = null;
        String rowkey_smobile = null;
        //String rowkey_sidno = null;

        String m_tableName_sguid = "ods_hy_userinfo_sguid";
        String m_tableName_smobile = "ods_hy_userinfo_smobile";
        //String m_tableName_sidno = "ods_hy_userinfo_sidno";

        try {
            /** hy_userinfo表的初始化 rowkey=smobile,sguid **/
            HTableDescriptor htd = new HTableDescriptor(m_tableName);
            HColumnDescriptor hdc = new HColumnDescriptor(familyName);
            htd.addFamily(hdc);
            hTable = new HTable(initHbase.configuration, m_tableName);
            hTable.setAutoFlush(false);
            hTable.setWriteBufferSize(50 * 1024 * 1024);
            //设置消息大小

            /** hy_userinfo表的初始化 rowkey=sguid **/
            HTableDescriptor htd_sguid = new HTableDescriptor(m_tableName_sguid);
            HColumnDescriptor hdc_sguid = new HColumnDescriptor(familyName);
            htd_sguid.addFamily(hdc_sguid);
            hTable_sguid = new HTable(initHbase.configuration, m_tableName_sguid);
            hTable_sguid.setAutoFlush(false);
            hTable_sguid.setWriteBufferSize(50 * 1024 * 1024);

            /** hy_userinfo表的初始化 rowkey=smobile **/
            HTableDescriptor htd_smobile = new HTableDescriptor(m_tableName_smobile);
            HColumnDescriptor hdc_smobile = new HColumnDescriptor(familyName);
            htd_smobile.addFamily(hdc_smobile);
            hTable_smobile = new HTable(initHbase.configuration, m_tableName_smobile);
            hTable_smobile.setAutoFlush(false);
            hTable_smobile.setWriteBufferSize(50 * 1024 * 1024);

            /** hy_userinfo表的初始化 rowkey=sidno
            HTableDescriptor htd_sidno = new HTableDescriptor(m_tableName_sidno);
            HColumnDescriptor hdc_sidno = new HColumnDescriptor(familyName);
            htd_sidno.addFamily(hdc_sidno);
            hTable_sidno = new HTable(initHbase.configuration, m_tableName_sidno);
            hTable_sidno.setAutoFlush(false);
            hTable_sidno.setWriteBufferSize(50 * 1024 * 1024);**/


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
                    //System.out.println("jsonMessage :============ jsonMessage != null && jsonMessage.length() != 0");
                    if (jsonMessage != null && jsonMessage.length() != 0) {
                        try {
                            com.alibaba.fastjson.JSONObject jsonObject = JSON.parseObject(jsonMessage);

                            table_name = jsonObject.getString("table_name").toLowerCase();

                            if (table_name.equals("hy_userinfo")) {
                                table_status = jsonObject.getString("table_status");

                                jsonArray = jsonObject.get("data");
                                memberList = JSON.parseArray(jsonArray + "", UserInfo.class);

                                //2、将解析的数据存入hbase中
                                if (table_status.equals("insert")) {

                                    begin_time = System.currentTimeMillis();
                                    //hbase创建表

                                    if (memberList.size() > 0) {
                                        for (int i = 0; i < memberList.size(); i++) {
                                            rowkey = memberList.get(i).getSmobile() + "," + memberList.get(i).getSguid();
                                            p1 = new Put(Bytes.toBytes(rowkey));   //设置rowkey
                                            p1.addColumn(Bytes.toBytes(familyName), Bytes.toBytes("sguid"),
                                                    Bytes.toBytes(memberList.get(i).getSguid()));
                                            p1.addColumn(Bytes.toBytes(familyName), Bytes.toBytes("suserno"),
                                                    Bytes.toBytes(memberList.get(i).getSuserno()));
                                            p1.addColumn(Bytes.toBytes(familyName), Bytes.toBytes("smobile"),
                                                    Bytes.toBytes(memberList.get(i).getSmobile()));
                                            p1.addColumn(Bytes.toBytes(familyName), Bytes.toBytes("semail"),
                                                    Bytes.toBytes(memberList.get(i).getSemail()));
                                            p1.addColumn(Bytes.toBytes(familyName), Bytes.toBytes("sname"),
                                                    Bytes.toBytes(memberList.get(i).getSname()));
                                            p1.addColumn(new String(familyName).getBytes(), new String("isex").getBytes(),
                                                    new String(memberList.get(i).getIsex()).getBytes());

                                            p1.addColumn(new String(familyName).getBytes(), new String("dbirthdate").getBytes(),
                                                    new String(memberList.get(i).getDbirthdate()).getBytes());
                                            p1.addColumn(new String(familyName).getBytes(), new String("iidtype").getBytes(),
                                                    new String(memberList.get(i).getIidtype()).getBytes());
                                            p1.addColumn(new String(familyName).getBytes(), new String("sidno").getBytes(),
                                                    new String(memberList.get(i).getSidno()).getBytes());
                                            p1.addColumn(new String(familyName).getBytes(), new String("sprovince").getBytes(),
                                                    new String(memberList.get(i).getSprovince()).getBytes());

                                            p1.addColumn(new String(familyName).getBytes(), new String("scity").getBytes(),
                                                    new String(memberList.get(i).getScity()).getBytes());
                                            p1.addColumn(new String(familyName).getBytes(), new String("area").getBytes(),
                                                    new String(memberList.get(i).getArea()).getBytes());
                                            p1.addColumn(new String(familyName).getBytes(), new String("sprovinceborn").getBytes(),
                                                    new String(memberList.get(i).getSprovinceborn()).getBytes());
                                            p1.addColumn(new String(familyName).getBytes(), new String("scityborn").getBytes(),
                                                    new String(memberList.get(i).getScityborn()).getBytes());
                                            p1.addColumn(new String(familyName).getBytes(), new String("saddress").getBytes(),
                                                    new String(memberList.get(i).getSaddress()).getBytes());
                                            p1.addColumn(new String(familyName).getBytes(), new String("iaddresscheck").getBytes(),
                                                    new String(memberList.get(i).getIaddresscheck()).getBytes());

                                            p1.addColumn(new String(familyName).getBytes(), new String("bisauthenticate").getBytes(),
                                                    new String(memberList.get(i).getBisauthenticate()).getBytes());
                                            p1.addColumn(new String(familyName).getBytes(), new String("dauthenticatedate").getBytes(),
                                                    new String(memberList.get(i).getDauthenticatedate()).getBytes());
                                            p1.addColumn(new String(familyName).getBytes(), new String("istatus").getBytes(),
                                                    new String(memberList.get(i).getIstatus()).getBytes());
                                            p1.addColumn(new String(familyName).getBytes(), new String("headphoto").getBytes(),
                                                    new String(memberList.get(i).getHeadphoto()).getBytes());
                                            p1.addColumn(new String(familyName).getBytes(), new String("sremark").getBytes(),
                                                    new String(memberList.get(i).getSremark()).getBytes());

                                            p1.addColumn(new String(familyName).getBytes(), new String("dregisterdate").getBytes(),
                                                    new String(memberList.get(i).getDregisterdate()).getBytes());
                                            p1.addColumn(new String(familyName).getBytes(), new String("dmodifydate").getBytes(),
                                                    new String(memberList.get(i).getDmodifydate()).getBytes());
                                            p1.addColumn(new String(familyName).getBytes(), new String("smodifyperson").getBytes(),
                                                    new String(memberList.get(i).getSmodifyperson()).getBytes());
                                            p1.addColumn(new String(familyName).getBytes(), new String("iversion").getBytes(),
                                                    new String(memberList.get(i).getIversion()).getBytes());
                                            p1.addColumn(new String(familyName).getBytes(), new String("ssource").getBytes(),
                                                    new String(memberList.get(i).getSsource()).getBytes());
                                            p1.addColumn(new String(familyName).getBytes(), new String("sinfosource").getBytes(),
                                                    new String(memberList.get(i).getSinfosource()).getBytes());
                                            p1.addColumn(new String(familyName).getBytes(), new String("sinfodetail").getBytes(),
                                                    new String(memberList.get(i).getSinfodetail()).getBytes());

                                            /**  rowkey sguid  **/
                                            rowkey_sguid = memberList.get(i).getSguid();
                                            p1_sguid = new Put(Bytes.toBytes(rowkey_sguid));   //设置rowkey
                                            p1_sguid.addColumn(Bytes.toBytes(familyName), Bytes.toBytes("sguid"),
                                                    Bytes.toBytes(memberList.get(i).getSguid()));
                                            p1_sguid.addColumn(Bytes.toBytes(familyName), Bytes.toBytes("suserno"),
                                                    Bytes.toBytes(memberList.get(i).getSuserno()));
                                            p1_sguid.addColumn(Bytes.toBytes(familyName), Bytes.toBytes("smobile"),
                                                    Bytes.toBytes(memberList.get(i).getSmobile()));
                                            p1_sguid.addColumn(Bytes.toBytes(familyName), Bytes.toBytes("semail"),
                                                    Bytes.toBytes(memberList.get(i).getSemail()));
                                            p1_sguid.addColumn(Bytes.toBytes(familyName), Bytes.toBytes("sname"),
                                                    Bytes.toBytes(memberList.get(i).getSname()));
                                            p1_sguid.addColumn(new String(familyName).getBytes(), new String("isex").getBytes(),
                                                    new String(memberList.get(i).getIsex()).getBytes());

                                            p1_sguid.addColumn(new String(familyName).getBytes(), new String("dbirthdate").getBytes(),
                                                    new String(memberList.get(i).getDbirthdate()).getBytes());
                                            p1_sguid.addColumn(new String(familyName).getBytes(), new String("iidtype").getBytes(),
                                                    new String(memberList.get(i).getIidtype()).getBytes());
                                            p1_sguid.addColumn(new String(familyName).getBytes(), new String("sidno").getBytes(),
                                                    new String(memberList.get(i).getSidno()).getBytes());
                                            p1_sguid.addColumn(new String(familyName).getBytes(), new String("sprovince").getBytes(),
                                                    new String(memberList.get(i).getSprovince()).getBytes());

                                            p1_sguid.addColumn(new String(familyName).getBytes(), new String("scity").getBytes(),
                                                    new String(memberList.get(i).getScity()).getBytes());
                                            p1_sguid.addColumn(new String(familyName).getBytes(), new String("area").getBytes(),
                                                    new String(memberList.get(i).getArea()).getBytes());
                                            p1_sguid.addColumn(new String(familyName).getBytes(), new String("sprovinceborn").getBytes(),
                                                    new String(memberList.get(i).getSprovinceborn()).getBytes());
                                            p1_sguid.addColumn(new String(familyName).getBytes(), new String("scityborn").getBytes(),
                                                    new String(memberList.get(i).getScityborn()).getBytes());
                                            p1_sguid.addColumn(new String(familyName).getBytes(), new String("saddress").getBytes(),
                                                    new String(memberList.get(i).getSaddress()).getBytes());
                                            p1_sguid.addColumn(new String(familyName).getBytes(), new String("iaddresscheck").getBytes(),
                                                    new String(memberList.get(i).getIaddresscheck()).getBytes());

                                            p1_sguid.addColumn(new String(familyName).getBytes(), new String("bisauthenticate").getBytes(),
                                                    new String(memberList.get(i).getBisauthenticate()).getBytes());
                                            p1_sguid.addColumn(new String(familyName).getBytes(), new String("dauthenticatedate").getBytes(),
                                                    new String(memberList.get(i).getDauthenticatedate()).getBytes());
                                            p1_sguid.addColumn(new String(familyName).getBytes(), new String("istatus").getBytes(),
                                                    new String(memberList.get(i).getIstatus()).getBytes());
                                            p1_sguid.addColumn(new String(familyName).getBytes(), new String("headphoto").getBytes(),
                                                    new String(memberList.get(i).getHeadphoto()).getBytes());
                                            p1_sguid.addColumn(new String(familyName).getBytes(), new String("sremark").getBytes(),
                                                    new String(memberList.get(i).getSremark()).getBytes());

                                            p1_sguid.addColumn(new String(familyName).getBytes(), new String("dregisterdate").getBytes(),
                                                    new String(memberList.get(i).getDregisterdate()).getBytes());
                                            p1_sguid.addColumn(new String(familyName).getBytes(), new String("dmodifydate").getBytes(),
                                                    new String(memberList.get(i).getDmodifydate()).getBytes());
                                            p1_sguid.addColumn(new String(familyName).getBytes(), new String("smodifyperson").getBytes(),
                                                    new String(memberList.get(i).getSmodifyperson()).getBytes());
                                            p1_sguid.addColumn(new String(familyName).getBytes(), new String("iversion").getBytes(),
                                                    new String(memberList.get(i).getIversion()).getBytes());
                                            p1_sguid.addColumn(new String(familyName).getBytes(), new String("ssource").getBytes(),
                                                    new String(memberList.get(i).getSsource()).getBytes());
                                            p1_sguid.addColumn(new String(familyName).getBytes(), new String("sinfosource").getBytes(),
                                                    new String(memberList.get(i).getSinfosource()).getBytes());
                                            p1_sguid.addColumn(new String(familyName).getBytes(), new String("sinfodetail").getBytes(),
                                                    new String(memberList.get(i).getSinfodetail()).getBytes());

                                            /**  rowkey smobile  **/
                                            rowkey_smobile = memberList.get(i).getSmobile();
                                            p1_smobile = new Put(Bytes.toBytes(rowkey_smobile));   //设置rowkey
                                            p1_smobile.addColumn(Bytes.toBytes(familyName), Bytes.toBytes("sguid"),
                                                    Bytes.toBytes(memberList.get(i).getSguid()));
                                            p1_smobile.addColumn(Bytes.toBytes(familyName), Bytes.toBytes("suserno"),
                                                    Bytes.toBytes(memberList.get(i).getSuserno()));
                                            p1_smobile.addColumn(Bytes.toBytes(familyName), Bytes.toBytes("smobile"),
                                                    Bytes.toBytes(memberList.get(i).getSmobile()));
                                            p1_smobile.addColumn(Bytes.toBytes(familyName), Bytes.toBytes("semail"),
                                                    Bytes.toBytes(memberList.get(i).getSemail()));
                                            p1_smobile.addColumn(Bytes.toBytes(familyName), Bytes.toBytes("sname"),
                                                    Bytes.toBytes(memberList.get(i).getSname()));
                                            p1_smobile.addColumn(new String(familyName).getBytes(), new String("isex").getBytes(),
                                                    new String(memberList.get(i).getIsex()).getBytes());

                                            p1_smobile.addColumn(new String(familyName).getBytes(), new String("dbirthdate").getBytes(),
                                                    new String(memberList.get(i).getDbirthdate()).getBytes());
                                            p1_smobile.addColumn(new String(familyName).getBytes(), new String("iidtype").getBytes(),
                                                    new String(memberList.get(i).getIidtype()).getBytes());
                                            p1_smobile.addColumn(new String(familyName).getBytes(), new String("sidno").getBytes(),
                                                    new String(memberList.get(i).getSidno()).getBytes());
                                            p1_smobile.addColumn(new String(familyName).getBytes(), new String("sprovince").getBytes(),
                                                    new String(memberList.get(i).getSprovince()).getBytes());

                                            p1_smobile.addColumn(new String(familyName).getBytes(), new String("scity").getBytes(),
                                                    new String(memberList.get(i).getScity()).getBytes());
                                            p1_smobile.addColumn(new String(familyName).getBytes(), new String("area").getBytes(),
                                                    new String(memberList.get(i).getArea()).getBytes());
                                            p1_smobile.addColumn(new String(familyName).getBytes(), new String("sprovinceborn").getBytes(),
                                                    new String(memberList.get(i).getSprovinceborn()).getBytes());
                                            p1_smobile.addColumn(new String(familyName).getBytes(), new String("scityborn").getBytes(),
                                                    new String(memberList.get(i).getScityborn()).getBytes());
                                            p1_smobile.addColumn(new String(familyName).getBytes(), new String("saddress").getBytes(),
                                                    new String(memberList.get(i).getSaddress()).getBytes());
                                            p1_smobile.addColumn(new String(familyName).getBytes(), new String("iaddresscheck").getBytes(),
                                                    new String(memberList.get(i).getIaddresscheck()).getBytes());

                                            p1_smobile.addColumn(new String(familyName).getBytes(), new String("bisauthenticate").getBytes(),
                                                    new String(memberList.get(i).getBisauthenticate()).getBytes());
                                            p1_smobile.addColumn(new String(familyName).getBytes(), new String("dauthenticatedate").getBytes(),
                                                    new String(memberList.get(i).getDauthenticatedate()).getBytes());
                                            p1_smobile.addColumn(new String(familyName).getBytes(), new String("istatus").getBytes(),
                                                    new String(memberList.get(i).getIstatus()).getBytes());
                                            p1_smobile.addColumn(new String(familyName).getBytes(), new String("headphoto").getBytes(),
                                                    new String(memberList.get(i).getHeadphoto()).getBytes());
                                            p1_smobile.addColumn(new String(familyName).getBytes(), new String("sremark").getBytes(),
                                                    new String(memberList.get(i).getSremark()).getBytes());

                                            p1_smobile.addColumn(new String(familyName).getBytes(), new String("dregisterdate").getBytes(),
                                                    new String(memberList.get(i).getDregisterdate()).getBytes());
                                            p1_smobile.addColumn(new String(familyName).getBytes(), new String("dmodifydate").getBytes(),
                                                    new String(memberList.get(i).getDmodifydate()).getBytes());
                                            p1_smobile.addColumn(new String(familyName).getBytes(), new String("smodifyperson").getBytes(),
                                                    new String(memberList.get(i).getSmodifyperson()).getBytes());
                                            p1_smobile.addColumn(new String(familyName).getBytes(), new String("iversion").getBytes(),
                                                    new String(memberList.get(i).getIversion()).getBytes());
                                            p1_smobile.addColumn(new String(familyName).getBytes(), new String("ssource").getBytes(),
                                                    new String(memberList.get(i).getSsource()).getBytes());
                                            p1_smobile.addColumn(new String(familyName).getBytes(), new String("sinfosource").getBytes(),
                                                    new String(memberList.get(i).getSinfosource()).getBytes());
                                            p1_smobile.addColumn(new String(familyName).getBytes(), new String("sinfodetail").getBytes(),
                                                    new String(memberList.get(i).getSinfodetail()).getBytes());

                                            /**  rowkey sidno  **/
                                            /*rowkey_sidno = memberList.get(i).getSidno();
                                            p1_sidno = new Put(Bytes.toBytes(rowkey_sidno));   //设置rowkey
                                            p1_sidno.addColumn(Bytes.toBytes(familyName), Bytes.toBytes("sguid"),
                                                    Bytes.toBytes(memberList.get(i).getSguid()));
                                            p1_sidno.addColumn(Bytes.toBytes(familyName), Bytes.toBytes("suserno"),
                                                    Bytes.toBytes(memberList.get(i).getSuserno()));
                                            p1_sidno.addColumn(Bytes.toBytes(familyName), Bytes.toBytes("smobile"),
                                                    Bytes.toBytes(memberList.get(i).getSmobile()));
                                            p1_sidno.addColumn(Bytes.toBytes(familyName), Bytes.toBytes("semail"),
                                                    Bytes.toBytes(memberList.get(i).getSemail()));
                                            p1_sidno.addColumn(Bytes.toBytes(familyName), Bytes.toBytes("sname"),
                                                    Bytes.toBytes(memberList.get(i).getSname()));
                                            p1_sidno.addColumn(new String(familyName).getBytes(), new String("isex").getBytes(),
                                                    new String(memberList.get(i).getIsex()).getBytes());

                                            p1_sidno.addColumn(new String(familyName).getBytes(), new String("dbirthdate").getBytes(),
                                                    new String(memberList.get(i).getDbirthdate()).getBytes());
                                            p1_sidno.addColumn(new String(familyName).getBytes(), new String("iidtype").getBytes(),
                                                    new String(memberList.get(i).getIidtype()).getBytes());
                                            p1_sidno.addColumn(new String(familyName).getBytes(), new String("sidno").getBytes(),
                                                    new String(memberList.get(i).getSidno()).getBytes());
                                            p1_sidno.addColumn(new String(familyName).getBytes(), new String("sprovince").getBytes(),
                                                    new String(memberList.get(i).getSprovince()).getBytes());

                                            p1_sidno.addColumn(new String(familyName).getBytes(), new String("scity").getBytes(),
                                                    new String(memberList.get(i).getScity()).getBytes());
                                            p1_sidno.addColumn(new String(familyName).getBytes(), new String("area").getBytes(),
                                                    new String(memberList.get(i).getArea()).getBytes());
                                            p1_sidno.addColumn(new String(familyName).getBytes(), new String("sprovinceborn").getBytes(),
                                                    new String(memberList.get(i).getSprovinceborn()).getBytes());
                                            p1_sidno.addColumn(new String(familyName).getBytes(), new String("scityborn").getBytes(),
                                                    new String(memberList.get(i).getScityborn()).getBytes());
                                            p1_sidno.addColumn(new String(familyName).getBytes(), new String("saddress").getBytes(),
                                                    new String(memberList.get(i).getSaddress()).getBytes());
                                            p1_sidno.addColumn(new String(familyName).getBytes(), new String("iaddresscheck").getBytes(),
                                                    new String(memberList.get(i).getIaddresscheck()).getBytes());

                                            p1_sidno.addColumn(new String(familyName).getBytes(), new String("bisauthenticate").getBytes(),
                                                    new String(memberList.get(i).getBisauthenticate()).getBytes());
                                            p1_sidno.addColumn(new String(familyName).getBytes(), new String("dauthenticatedate").getBytes(),
                                                    new String(memberList.get(i).getDauthenticatedate()).getBytes());
                                            p1_sidno.addColumn(new String(familyName).getBytes(), new String("istatus").getBytes(),
                                                    new String(memberList.get(i).getIstatus()).getBytes());
                                            p1_sidno.addColumn(new String(familyName).getBytes(), new String("headphoto").getBytes(),
                                                    new String(memberList.get(i).getHeadphoto()).getBytes());
                                            p1_sidno.addColumn(new String(familyName).getBytes(), new String("sremark").getBytes(),
                                                    new String(memberList.get(i).getSremark()).getBytes());

                                            p1_sidno.addColumn(new String(familyName).getBytes(), new String("dregisterdate").getBytes(),
                                                    new String(memberList.get(i).getDregisterdate()).getBytes());
                                            p1_sidno.addColumn(new String(familyName).getBytes(), new String("dmodifydate").getBytes(),
                                                    new String(memberList.get(i).getDmodifydate()).getBytes());
                                            p1_sidno.addColumn(new String(familyName).getBytes(), new String("smodifyperson").getBytes(),
                                                    new String(memberList.get(i).getSmodifyperson()).getBytes());
                                            p1_sidno.addColumn(new String(familyName).getBytes(), new String("iversion").getBytes(),
                                                    new String(memberList.get(i).getIversion()).getBytes());
                                            p1_sidno.addColumn(new String(familyName).getBytes(), new String("ssource").getBytes(),
                                                    new String(memberList.get(i).getSsource()).getBytes());
                                            p1_sidno.addColumn(new String(familyName).getBytes(), new String("sinfosource").getBytes(),
                                                    new String(memberList.get(i).getSinfosource()).getBytes());
                                            p1_sidno.addColumn(new String(familyName).getBytes(), new String("sinfodetail").getBytes(),
                                                    new String(memberList.get(i).getSinfodetail()).getBytes());*/


                                            listvec.add(p1);
                                            listvec_sguid.add(p1_sguid);
                                            listvec_smobile.add(p1_smobile);
                                            //listvec_sidno.add(p1_sidno);
                                        }

                                        hTable.put(listvec);
                                        hTable_sguid.put(listvec_sguid);
                                        hTable_smobile.put(listvec_smobile);
                                        //hTable_sidno.put(listvec_sidno);

                                        p1.setWriteToWAL(false);
                                        p1_sguid.setWriteToWAL(false);
                                        p1_smobile.setWriteToWAL(false);
                                        //p1_sidno.setWriteToWAL(false);

                                        hTable.flushCommits();
                                        hTable_sguid.flushCommits();
                                        hTable_smobile.flushCommits();
                                        //hTable_sidno.flushCommits();

//                                hTable.close();
                                        end_time = System.currentTimeMillis();
                                        logger.info(LogUtils.concatLog(m_tableName, m_tableName, end_time - begin_time, "插入/更新成功", jsonMessage, listvec.size(), "SUCCESS"));
                                        //System.out.println("logger.info :============ com/maiya/kafka/consumer/ConsumerUserInfoTask.java:428");
                                        //myLogger.kafka(this.getClass().getName(), end_time - begin_time, "成功", "", listvec.size());
                                        //System.out.println("myLogger.kafka :============ com/maiya/kafka/consumer/ConsumerUserInfoTask.java:429");
                                        listvec.clear();
                                        listvec_sguid.clear();
                                        listvec_smobile.clear();
                                        //listvec_sidno.clear();
                                        memberList.clear();
                                        TimeUnit.NANOSECONDS.sleep(1);
                                    }

                                }
                            }
                        } catch (Exception e) {
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