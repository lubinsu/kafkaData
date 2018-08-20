package com.maiya.kafka.consumer;


import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.maiya.kafka.consumer.bean.HBaseKey;
import com.maiya.kafka.consumer.hbase.HBaseClient;
import com.maiya.kafka.consumer.util.LogUtils;
import kafka.consumer.KafkaStream;
import kafka.message.MessageAndMetadata;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Created by lubinsu 2016/3/30.
 * 通用的
 */
public class ConsumerCommonTask implements Runnable {
    private KafkaStream m_stream;
    private String mTopic;
    private String hTableName;
    private String m_columnFamily;
    private String m_rowkey;
    private HBaseClient hbase;
    private List<HBaseKey> rowkeys;

    final static Logger logger = LoggerFactory.getLogger(ConsumerCommonTask.class);

    //private static com.maiya.log.Logger myLogger = new com.maiya.log.Logger(com.maiya.log.Logger.LogType.KAFKA);

    private ArrayList<Put> listvec = new ArrayList<>();
    private ArrayList<String> delList = new ArrayList<>();
    JSONArray jsonArray = null;
    Put put = null;

    public ConsumerCommonTask(KafkaStream stream, String tableName) {
        m_stream = stream;
        mTopic = tableName;
    }

    public ConsumerCommonTask(KafkaStream stream, String topic, String hTableName, String columnFamily, String rowkey) {
        m_stream = stream;
        mTopic = topic;
        m_columnFamily = columnFamily;
        m_rowkey = rowkey;
        this.hTableName = hTableName;
        this.rowkeys = getRowKeys(rowkey);
    }

    public void run() {
        String jsonMessage;
        String table_status;
        String table_name;
        long begin_time = 0;
        long end_time;
        String rowkey = "";

        //com.maiya.log.Logger myLogger = new com.maiya.log.Logger(com.maiya.log.Logger.LogType.KAFKA);
        hbase = new HBaseClient(hTableName);

        while (true) {
            try {
                logger.info("获取kafka消息开始...".concat(mTopic));
                for (MessageAndMetadata<byte[], byte[]> aM_stream : (Iterable<MessageAndMetadata<byte[], byte[]>>) m_stream) {
                    logger.info("获取kafka消息结束，入库开始...".concat(mTopic));
                    jsonMessage = new String(aM_stream.message());
                    long offset = aM_stream.offset();
                    int partition = aM_stream.partition();
                    String topic = aM_stream.topic();
                    /*System.out.println("==" + jsonMessage);*/

                    //1、json循环解析表结构数据
                    //fastjson
                    if (jsonMessage.length() != 0) {
                        try {
                            begin_time = System.currentTimeMillis();
                            JSONObject jsonObject = JSON.parseObject(jsonMessage);

                            table_name = jsonObject.getString("table_name").toLowerCase();

                            if (mTopic.equals(table_name)) {
                                table_status = jsonObject.getString("table_status");
                                jsonArray = jsonObject.getJSONArray("data");

                                //2、将解析的数据存入hbase中
                                if ("insert".equals(table_status) || "update".equals(table_status)) {
                                    /**
                                     *
                                     {
                                     "table_name": "tbl_policy_products",
                                     "table_status": "insert",
                                     "preprocessor": "",
                                     "data": [{"sguid": "xxxxxx"}...]
                                     }
                                     */

                                    begin_time = System.currentTimeMillis();
                                    //hbase创建表

                                    if (jsonArray.size() > 0) {
                                        for (int i = 0; i < jsonArray.size(); i++) {
                                            JSONObject json = jsonArray.getJSONObject(i);

                                            rowkey = getRowKey(json, rowkeys);
                                            //设置rowkey
                                            put = new Put(Bytes.toBytes(rowkey));
                                            put.addColumn(Bytes.toBytes(m_columnFamily), Bytes.toBytes("consume_time"), Bytes.toBytes(DateTime.now().toString("yyyy-MM-dd HH:mm:ss.S")));
                                            for (String field : json.keySet()) {
                                                String fieldVal = json.getString(field);
                                                fieldVal = (fieldVal == null) ? "" : fieldVal;
                                                put.addColumn(Bytes.toBytes(m_columnFamily), Bytes.toBytes(field.toLowerCase()), Bytes.toBytes(fieldVal));
                                            }

                                            listvec.add(put);
                                        }

                                        end_time = System.currentTimeMillis();
                                        logger.info("开始写入hbase...".concat(mTopic));
                                        hbase.insert(listvec);
                                        logger.info("写入hbase结束...".concat(mTopic));
                                        logger.info(LogUtils.concatLog(mTopic, hTableName, end_time - begin_time, "插入/更新成功", rowkey, listvec.size(), "SUCCESS"));
                                        //myLogger.kafka(mTopic, end_time - begin_time, "成功", "[".concat(table_status).concat("]").concat(jsonMessage), listvec.size());
                                        listvec.clear();
                                        TimeUnit.NANOSECONDS.sleep(1);
                                    }
                                } else if ("delete".equals(table_status)) {
                                    /**
                                     *
                                     {
                                     "table_name": "tbl_policy_products",
                                     "table_status": "delete",
                                     "preprocessor": "",
                                     "data": [{"sguid": "xxxxxx"}]
                                     }
                                     */
                                    begin_time = System.currentTimeMillis();
                                    //hbase创建表

                                    if (jsonArray.size() > 0) {
                                        for (int i = 0; i < jsonArray.size(); i++) {
                                            JSONObject json = jsonArray.getJSONObject(i);
                                            rowkey = getRowKey(json, rowkeys);
                                            delList.add(rowkey);
                                        }

                                        end_time = System.currentTimeMillis();
                                        hbase.delete(delList);
                                        logger.info(LogUtils.concatLog(mTopic, hTableName, end_time - begin_time, "删除成功", jsonMessage, listvec.size(), "SUCCESS"));
                                        //myLogger.kafka(mTopic, end_time - begin_time, "成功", "[".concat(table_status).concat("]").concat(jsonMessage), listvec.size());
                                        delList.clear();
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
                            logger.error(LogUtils.concatLog(mTopic, hTableName, end_time - begin_time, "入库失败", jsonMessage, listvec.size(), ((e.getMessage() == null) ? e.toString() : e.getMessage()).concat(builder.toString())));
                            //myLogger.kafka(mTopic, end_time - begin_time, "失败", "【消息】".concat(jsonMessage).concat(";").concat(e.toString()), 0);
                        }
                    } else {
                        logger.error(LogUtils.concatLog(mTopic, hTableName, 0, "入库失败", jsonMessage, listvec.size(), "json内容为空"));
                    }
                }
            } catch (Exception e) {
                StringBuilder builder = new StringBuilder();
                for (StackTraceElement stackTraceElement : e.getStackTrace()) {
                    builder.append(stackTraceElement.toString());
                }
                logger.error(LogUtils.concatLog(mTopic, hTableName, 0, "入库失败", "", 0, ((e.getMessage() == null) ? e.toString() : e.getMessage()).concat(builder.toString())));
            }
        }
    }

    private List<HBaseKey> getRowKeys(String rowkeys) {
        List<HBaseKey> list = new ArrayList<>();
        String[] rowkey = rowkeys.split(":");
        for (String aRowkey : rowkey) {
            list.add(new HBaseKey(aRowkey.split("=")[0], "Y".equals(aRowkey.split("=")[1])));
        }
        return list;
    }

    /**
     * 根据配置的RowKey生成hbase的rowkey
     *
     * @param json    获取的json对象
     * @param rowkeys 配置的字符串，以:隔开, 以=号隔开的，判断字符串是否需要反转
     * @return 返回hbase rowkey
     */
    private String getRowKey(JSONObject json, List<HBaseKey> rowkeys) {

        String key = "";

        for (int i = 0; i < rowkeys.size(); i++) {
            if (i == 0) {
                if (rowkeys.get(i).getReserverF()) {
                    key += new StringBuffer(json.getString(rowkeys.get(i).getKey())).reverse().toString();
                } else {
                    key += json.getString(rowkeys.get(i).getKey());
                }
            } else {
                if (rowkeys.get(i).getReserverF()) {
                    key = key + "," + new StringBuffer(json.getString(rowkeys.get(i).getKey())).reverse().toString();
                } else {
                    key = key + "," + json.getString(rowkeys.get(i).getKey());
                }
            }
        }

        return key;
    }

}