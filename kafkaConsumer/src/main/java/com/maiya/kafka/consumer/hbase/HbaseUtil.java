package com.maiya.kafka.consumer.hbase;

/**
 * Created by DELL on 2016/4/7.
 */

import java.io.IOException;
import java.util.ArrayList;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
@SuppressWarnings("deprecation")
public class HbaseUtil {
    HBaseAdmin admin=null;
    static Configuration con=null;
    public HbaseUtil() {
        con=new Configuration();
        con.set("hbase.rootdir","hdfs://192.168.0.111:9000/hbase");
        con.set("hbase.zookeeper.quorum", "192.168.0.114:2181,192.168.0.115:2181,192.168.0.116:2181");
        try {
            admin=new HBaseAdmin(con);
            System.out.println("hbase连接成功");
        } catch (Exception e) {
            System.out.println("hbase连接失败");
            e.printStackTrace();
        }

    }


    public static void main(String[] args) throws Exception {
        HbaseUtil hbaseTest = new HbaseUtil();
        //创建一张表
        hbaseTest.createTable("test","cf");
        //获取所有列表
        //hbaseTest.getAllTable();
        //向表中插入一条数据
        //hbaseTest.addOneRecord("stu","key3","cf","name","wangwu");
        //查询一条记录
        //hbaseTest.getkey("hy_linkman","key");
        //获取表的所有记录
        System.out.println("begin");
        hbaseTest.getAllData("test");
        //删除一条数据
        //hbaseTest.deleteRecord("stu","key2");
        //删除表
        //hbaseTest.deleteTable("stu");
        System.out.println("end");
    }

    /**
     *  创建表
     *  param:tableName,family
     *  throws:Exception
     *  author:xs
     *  date:2016-04-07
     * */
    public void createTable(String tableName,String family) throws Exception{
        if(admin.tableExists(tableName)){
            System.out.println("表已经存在");
        }else{
            HTableDescriptor desc =new HTableDescriptor(tableName);
            desc.addFamily(new HColumnDescriptor(family));
            admin.createTable(desc);
            System.out.println(tableName+"表创建成功");
        }
    }



    /**
     * 删除表
     * @param tableName
     * @throws IOException
     */
    private void deleteTable(String tableName){
        try {
            if (admin.tableExists(tableName)) {
                admin.disableTable(tableName);
                admin.deleteTable(tableName);
                System.out.println(tableName+"表删除成功");
            }
        } catch (IOException e) {
            System.out.println(tableName+"表删除失败");
            e.printStackTrace();
        }

    }
    /**
     * 删除一条记录
     * @param tableName
     * @param rowKey
     */
    private void deleteRecord(String tableName, String rowKey) {
        HTablePool hTablePool = new HTablePool(con, 1000);
        HTableInterface table = hTablePool.getTable(tableName);
        Delete delete = new Delete(rowKey.getBytes());
        try {
            table.delete(delete);
            System.out.println(rowKey+"删除成功");
        } catch (Exception e) {
            System.out.println(rowKey+"删除失败");
            e.printStackTrace();
        }
    }
    /**
     * 获取表的所有记录
     * @param tableName
     */
    @SuppressWarnings("resource")
    private void getAllData(String tableName) {
        try {
            HTable hTable = new HTable(con, tableName);
            Scan scan=new Scan();

            System.out.println("1"+tableName);
            ResultScanner scanner = hTable.getScanner(scan);
            System.out.println("2");
            for (Result result : scanner) {
                System.out.println("3");
                if (result.raw().length==0) {
                    System.out.println(tableName+"表中没数据了");
                }else {
                    System.out.println("4" +tableName);
                    for (KeyValue kv : result.raw()) {
                        System.out.println("5");
                        //System.out.println(new String(kv.getRow())+"\t"+new String(kv.getValue()));
                        StringBuffer sb = new StringBuffer()
                                .append(Bytes.toString(kv.getRow())).append("\t")
                                .append(Bytes.toString(kv.getFamily()))
                                .append("\t")
                                .append(Bytes.toString(kv.getQualifier()))
                                .append("\t").append(Bytes.toString(kv.getValue()));
                        System.out.println(sb.toString());


                    }
                }
            }
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

    }
    /**
     * 查询一条记录
     * @param tableName
     * @param rowKey
     */
    @SuppressWarnings("resource")
    private void getkey(String tableName, String rowKey) {
        HTablePool hTablePool = new HTablePool(con, 1000);
        HTableInterface table = hTablePool.getTable(tableName);
        Get get = new Get(rowKey.getBytes());
        try {
            Result result = table.get(get);
            if (result.raw().length==0) {
                System.out.println("查询记录"+rowKey+"不存在！");
            }else {
                for (KeyValue kv : result.raw()) {
                    System.out.println(new String(kv.getRow())+"\t"+new String(kv.getValue()));
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    //向表中插入一条数据
    private void addOneRecord(String tableName, String rowKey, String family, String qualifier,
                              String value) {
        HTablePool hTablePool = new HTablePool(con, 1000);
        HTableInterface table = hTablePool.getTable(tableName);
        Put put = new Put(rowKey.getBytes());
        put.add(family.getBytes(),qualifier.getBytes(),value.getBytes());
        try {
            table.put(put);
            System.out.println("添加记录"+rowKey+"成功");
        } catch (IOException e) {
            System.out.println("添加记录"+rowKey+"失败");
            e.printStackTrace();
        }
    }
    /*
     * 获取所有列表
     */
    @SuppressWarnings({ "rawtypes", "unchecked" })
    private void getAllTable() throws Exception {
        ArrayList tables = new ArrayList();
        if (admin!=null) {
            HTableDescriptor[] listTables=admin.listTables();
            if (listTables.length>0) {
                for (HTableDescriptor hTableDescriptor : listTables) {
                    tables.add(hTableDescriptor.getNameAsString());
                    System.out.println(hTableDescriptor.getNameAsString());
                }
            }

        }
    }
}


