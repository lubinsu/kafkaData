package com.maiya.kafka.consumer;


import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.maiya.kafka.consumer.bean.HBaseDelKey;
import com.maiya.kafka.consumer.bean.HBaseKey;
import com.maiya.kafka.consumer.hbase.HBaseClient;
import com.maiya.kafka.consumer.util.DelType;
import com.maiya.kafka.consumer.util.LogUtils;
import kafka.consumer.KafkaStream;
import kafka.message.MessageAndMetadata;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * Created by lubinsu 2016/3/30.
 * add hujunbiao
 * 通用的
 */
public class ConsumerCommonTask_2 implements Runnable {
    private KafkaStream m_stream;
    private String mTopic;
    private String hTableName;
    private String m_columnFamily;
    private String m_rowkey;
    private HBaseClient hbase;
    private List<HBaseKey> rowkeys;

    //复爬前需要先删除数据。删除类型:1、key 2、prefix 3、prefix_split
    private String deleteType;
    //删除的ROWKEY
    private List<HBaseDelKey> deleteRowkeys;

    final static Logger logger = LoggerFactory.getLogger(ConsumerCommonTask_2.class);

    //private static com.maiya.log.Logger myLogger = new com.maiya.log.Logger(com.maiya.log.Logger.LogType.KAFKA);

    private ArrayList<Put> listvec = new ArrayList<>();
    private ArrayList<String> delList = new ArrayList<>();
    JSONArray jsonArray = null;
    Put put = null;

    public ConsumerCommonTask_2(KafkaStream stream, String tableName) {
        m_stream = stream;
        mTopic = tableName;
    }

    public ConsumerCommonTask_2(KafkaStream stream, String topic, String hTableName, String columnFamily, String rowkey) {
        m_stream = stream;
        mTopic = topic;
        m_columnFamily = columnFamily;
        m_rowkey = rowkey;
        this.hTableName = hTableName;
        this.rowkeys = getRowKeys(rowkey);
    }

    public ConsumerCommonTask_2(KafkaStream stream, String topic, String hTableName, String columnFamily, String rowkey, String deleteKeys) {
        m_stream = stream;
        mTopic = topic;
        m_columnFamily = columnFamily;
        m_rowkey = rowkey;
        this.hTableName = hTableName;
        this.rowkeys = getRowKeys(rowkey);
        this.deleteRowkeys = getHBaseDelKeList(deleteKeys);
    }

    /**
     * 取得删除规则的RowKeyBean
     * key:sUserId;sGuid
     * prefix:sUserId;sGuid
     * prefix:sUserId.split('_')[0]
     *
     * @param delRowKeys
     * @return
     */
    private List<HBaseDelKey> getHBaseDelKeList(String delRowKeys) {
        //没有配置复爬
        if (StringUtils.isBlank(delRowKeys)) {
            return null;
        }
        List<HBaseDelKey> list = new ArrayList<>();
        // 配置了复爬
        if (StringUtils.isNotBlank(delRowKeys)) {
            String[] vals = delRowKeys.replace(" ", "").trim().split(":", -1);
            String flag = vals[0];
            this.deleteType = flag;
            String conf = vals[1];
            if (DelType.KEY.name().equals(deleteType.toUpperCase())) {
                String[] keys = conf.split(";");
                for (String key : keys) {
                    String[] temps = key.split("=", -1);
                    if (temps.length == 1) {
                        list.add(new HBaseDelKey(temps[0], false));
                    } else if (temps.length == 2) {
                        list.add(new HBaseDelKey(temps[0], "Y".equals(temps[1])));
                    }
                }
            } else if (DelType.PREFIX.name().equals(deleteType.toUpperCase())) {
                String[] keys = conf.split(";");
                for (String key : keys) {
                    String[] temps = key.split("=", -1);
                    if (temps.length == 1) {
                        list.add(new HBaseDelKey(temps[0], false));
                    } else if (temps.length == 2) {
                        list.add(new HBaseDelKey(temps[0], "Y".equals(temps[1])));
                    }
                }
            } else if (DelType.PREFIX_SPLIT.name().equals(deleteType.toUpperCase())) {
                // if (StringUtils.isNotBlank(conf) && conf.contains(".")) {
                String[] tmp = conf.split("\\.");
                String val_1 = tmp[1];
                String separator = val_1.substring(7, 8);
                String index = val_1.substring(11, val_1.length() - 1);
                logger.info("mTopic==>" + mTopic + " separator==>" + separator + " index==>" + index);
                list.add(new HBaseDelKey(tmp[0], false, separator, index));
            }
        }
        return list;
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
                                    if (jsonArray.size() > 0) {
                                        for (int i = 0; i < jsonArray.size(); i++) {
                                            JSONObject json = jsonArray.getJSONObject(i);
                                            rowkey = getRowKey(json, rowkeys);

                                            // add hujunbiao  复爬(更新前先根据规则删除数据)
                                            if (null != deleteRowkeys && 0 == i) {
                                                handleAgainCrawl(json);
                                            }

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
                                        logger.info(LogUtils.concatLog(mTopic, hTableName, end_time - begin_time, "删除成功", "", listvec.size(), "SUCCESS"));
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

    /**
     * 处理复爬 删除业务逻辑
     *
     * @param json data中的JSON对象
     */
    private void handleAgainCrawl(JSONObject json) {
        String uuid = UUID.randomUUID().toString();
        logger.info(DateTime.now().toString("yyyy-MM-dd HH:mm:ss.S").concat("\t").concat(mTopic.concat("\t").concat(hTableName).concat("\t") + "uuid:" + uuid + "\t" + hTableName + "进入处理复爬删除..."));
        long startTime = System.currentTimeMillis();
        // 按照配置 构造删除deleteKey
        String deleteKey = getDeleteRowKey(json, deleteRowkeys);
        logger.info(DateTime.now().toString("yyyy-MM-dd HH:mm:ss.S").concat("\t").concat(mTopic.concat("\t").concat(hTableName).concat("\t") + "uuid:" + uuid + "\t" + hTableName + " deleteType:" + deleteType + " deleteKey:" + deleteKey));
        //删除数据
        delByDeleteKey(m_columnFamily, deleteKey, hTableName, uuid);
        logger.info(DateTime.now().toString("yyyy-MM-dd HH:mm:ss.S").concat("\t").concat(mTopic.concat("\t").concat(hTableName).concat("\t") + "uuid:" + uuid + "\t" + " 处理复爬删除耗时:" + (System.currentTimeMillis() - startTime)));
    }

    /**
     * 取得删除规则的Bean集合
     * hy_membersms=hy_membersms,sUserId=N:sGuid=N,dxjl,my_group2
     * hy_membercontacts=hy_membercontacts,sUserId=N:sGuid=N,txjl,my_group2
     * hy_membercallhistory=hy_membercallhistory,sUserId=N:sGuid=N,thjl,my_group2
     * <p>
     * key:sUserId;sGuid prefix:sUserId;sGuid prefix:sUserId.split('_')[0]
     *
     * @param json
     * @param delrowkeys
     * @return
     */
    private String getDeleteRowKey(JSONObject json, List<HBaseDelKey> delrowkeys) {
        String delKey = "";
        int len = delrowkeys.size();
        for (int i = 0; i < len; i++) {
            if (0 == i) {
                if (delrowkeys.get(i).isReserverF()) {
                    delKey += new StringBuilder(json.getString(delrowkeys.get(i).getKey())).reverse().toString();
                } else {
                    delKey += json.getString(delrowkeys.get(i).getKey());
                }
            } else {
                if (delrowkeys.get(i).isReserverF()) {
                    delKey = delKey + "," + new StringBuilder(json.getString(delrowkeys.get(i).getKey())).reverse().toString();
                } else {
                    delKey = delKey + "," + json.getString(delrowkeys.get(i).getKey());

                }
            }
        }
        // 字符串拆分
        if (DelType.PREFIX_SPLIT.name().equals(deleteType.toUpperCase()) && delrowkeys.size() == 1) {
            logger.info("oldSeparator:" + delrowkeys.get(0).getSeparator());
            logger.info("oldSeparator:" + delrowkeys.get(0).getSeparator());
            logger.info("oldIndex:" + Integer.parseInt(delrowkeys.get(0).getIndex()));
            logger.info("oldDelKey:" + delKey);
            if ("D".equals(delrowkeys.get(0).getSeparator())) {
                delKey = delKey.split(",")[Integer.parseInt(delrowkeys.get(0).getIndex())];
            } else {
                delKey = delKey.split(delrowkeys.get(0).getSeparator())[Integer.parseInt(delrowkeys.get(0).getIndex())];
            }
        }
        logger.info("deleteType:" + deleteType + " deleteKey:" + delKey);
        return delKey;
    }

//    public static void main(String[] args) {
//        String delKey = "sadfasdf,";
//        // 字符串拆分
//        if ("D".equals("D")){
//            delKey = delKey.split(",")[0];
//        }
////        else {
////            delKey = delKey.split(delrowkeys.get(0).getSeparator())[Integer.parseInt(delrowkeys.get(0).getIndex())];
////        }
//        System.out.println(delKey);
//    }


    /**
     * 删除数据
     *
     * @param family
     * @param deleteKey
     * @param tableName
     */
    private void delByDeleteKey(String family, String deleteKey, String tableName, String uuid) {
        if (StringUtils.isBlank(deleteKey)) {
            return;
        }
        int count = 0;
        if (DelType.KEY.name().equals(deleteType.toUpperCase())) {
            hbase.delete(deleteKey);
        } else {
            List<Delete> delList = new ArrayList<>();
            Scan scan = new Scan();
            scan.setCaching(8_000);
            scan.addFamily(Bytes.toBytes(family));
            scan.setFilter(new PrefixFilter(Bytes.toBytes(deleteKey)));
            scan.setStartRow(Bytes.toBytes(deleteKey));
            try (Table table = hbase.getConnection().getTable(TableName.valueOf(tableName)); ResultScanner results = table.getScanner(scan);) {
                for (Result result : results) {
                    count++;
                    String rowkey = Bytes.toString(result.getRow());
                    Delete del = new Delete(Bytes.toBytes(rowkey));
                    if (delList.size() == 8000) {
                        // 删除
                        table.delete(delList);
                        delList.clear();
                    } else {
                        delList.add(del);
                    }
                }
                if (delList.size() > 0) {
                    // 删除
                    table.delete(delList);
                }
            } catch (Exception e) {
                logger.error("delByPrefix-error", e);
            }
            logger.info(DateTime.now().toString("yyyy-MM-dd HH:mm:ss.S").concat("\t").concat(mTopic.concat("\t").concat(hTableName).concat("\t") + "uuid:" + uuid + " 删除条数:" + count));
        }
    }


}