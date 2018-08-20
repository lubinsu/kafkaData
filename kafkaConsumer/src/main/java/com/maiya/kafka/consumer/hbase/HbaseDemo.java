package com.maiya.kafka.consumer.hbase;

/**
 * Created by DELL on 2016/4/1.
 */

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;


import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;

/**
 * hbase运行示例
 * 主要是对hbase进行增加表，插入数据，读取数据，删除数据
 * 删除表
 * @author song
 *
 */
public class HbaseDemo {

    private static Configuration conf;
    public static void open() {
        conf = HBaseConfiguration.create();
//        conf.set("hbase.rootdir", "hdfs://192.168.1.2:9000/hbase");
        conf.set("hbase.cluster.distributed", "true");
        conf.set("hbase.rootdir","hdfs://192.168.0.111:9000/hbase");
//        conf.set("hbase.master","192.168.0.111:60000");
        conf.set("hbase.zookeeper.quorum","192.168.1.154:2181,192.168.1.155:2181,192.168.1.156:2181");
//        conf.set("hbase.zookeeper.property.clientPort", "2181");
    }


    public static void main(String[] args) {
        String tableName = "test";
        String[] familyNames = new String[]{"sCity","sIdNo","sMobile","sName"};

//        System.out.println("begin to create table......");
        createTable(tableName,familyNames); //创建表
//        System.out.println("end to create table......");

        /*****************添加数据**********************/
//        String[] qualifiers = new String[]{"name1","age1","sex1"};  //列名
//        String row = "row-"+(int)Math.random()*100;
//
//        String [] values = new String[]{"张三","20","男"};
//        insertData(tableName, row, familyNames, qualifiers, values);
        /*******************查询数据**************************/
//        String row = "row-"+(int)Math.random()*100;
//
//        scanData(tableName, row, "yhxx", "sName");
        //delete(tableName, row, "name", "name");  //删除数据


        //删除表

    }

    /**
     * 创建一个表
     * @param tableName  表名
     * @param familyNames 列簇数组
     */
    public  static void createTable(String tableName,String[] familyNames) {
        HbaseDemo.open();
//        Admin admin  = null;
        try {
            //获取一个连接
            System.out.println("ConnectionFactory");

            //connection = ConnectionFactory.createConnection(conf);
            //创建一个admin对象，创建表
            HBaseAdmin admin = new HBaseAdmin(conf);

            System.out.println("getAdmin");

//            admin = connection.getAdmin();
            //检查表是否存在，存在不创建
            if (admin.tableExists(TableName.valueOf(tableName))) {
                System.out.println(tableName + "表已存在");
                System.exit(0);

            } else {

                System.out.println("HTableDescriptor");
                HTableDescriptor hdesc = new HTableDescriptor(TableName.valueOf(tableName));
                //添加列簇
                if (familyNames.length > 0) {
                    for (String familyName : familyNames) {
                        hdesc.addFamily(new HColumnDescriptor(familyName));
                    }
                }
                //创建表
                admin.createTable(hdesc);
                System.out.println(tableName + "表创建成功");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
//        }finally{
////            if(admin!=null){
////                try {
////                    admin.close();
////                } catch (IOException e) {
////                    e.printStackTrace();
////                }
////            }
////            if(connection!=null){
////                try {
////                    connection.close();
////                } catch (IOException e) {
////                    e.printStackTrace();
////                }
////            }
////        }
//    }

    /**
     * 向已存在的hbase表中添加数据
     * @param tableName 表名
     * @param row  row-key
     * @param familyNames 列簇数组
     * @param qualifiers  列名数组
     * @param values   值数组
     */
    public  static  void insertData(String tableName,String row,String[] familyNames,
                                    String[]qualifiers , String[] values){
        Configuration conf = null;
        Connection connection = null;
        conf = HBaseConfiguration.create();
        conf.set("hbase.master", "192.168.0.111:60000");
        conf.set("hbase.zookeeper.quorum","192.168.0.114,192.168.0.115,192.168.0.116");
        conf.set("hbase.zookeeper.property.clientPort", "2181");
        try {
            connection  = ConnectionFactory.createConnection(conf);
            //根据表名，获取该表的对象
            Table table = connection.getTable(TableName.valueOf(tableName));
            Put put = new Put(row.getBytes());
            //添加数据
            put.addColumn(Bytes.toBytes(familyNames[0]), Bytes.toBytes(qualifiers[0]),
                    Bytes.toBytes(values[0]));
            put.addColumn(Bytes.toBytes(familyNames[1]), Bytes.toBytes(qualifiers[1]),
                    Bytes.toBytes(values[1]));
            put.addColumn(Bytes.toBytes(familyNames[2]), Bytes.toBytes(qualifiers[2]),
                    Bytes.toBytes(values[2]));
	     /*put.addColumn(Bytes.toBytes(familyNames[0]), Bytes.toBytes(qualifiers[0]),
			 Bytes.toBytes("李四"));
	    put.addColumn(Bytes.toBytes(familyNames[1]), Bytes.toBytes(qualifiers[1]),
			 Bytes.toBytes("19"));
	    put.addColumn(Bytes.toBytes(familyNames[2]), Bytes.toBytes(qualifiers[2]),
			 Bytes.toBytes("nan"))*/;


            table.put(put);
            System.out.println(tableName+"插入数据成功");
            table.close();
        } catch (IOException e) {
            e.printStackTrace();
        }finally{
            if(connection!=null){
                try {
                    connection.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

    }

    /**
     *
     *
     *
     *
     * */
    public static Result get(String tableName, String key) {
        try {
//            HbaseUtil.open();
            HTable table = new HTable(conf, tableName);
            Get get = new Get(key.getBytes());
            Result result = table.get(get);
            return result;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }


    /**
     * 查询数据，根据表名和列簇和列名来查询
     * @param tableName  表名
     * @param rowkey   row-key
     * @param family   列簇
     * @param qualifier 列名
     */
    public  static void scanData(String tableName,String rowkey,String family,String qualifier){
//        Configuration conf = null;
        Connection connection = null;
//        conf = HBaseConfiguration.create();
//        conf.set("hbase.master", "192.168.0.111:60000");
//        conf.set("hbase.zookeeper.quorum","192.168.0.114,192.168.0.115,192.168.0.116");
//        conf.set("hbase.zookeeper.property.clientPort", "2181");
        HbaseDemo.open();
        try {

            connection  = ConnectionFactory.createConnection(conf);
            Table table = connection.getTable(TableName.valueOf(tableName));
            Get get = new Get(Bytes.toBytes(rowkey));
            get.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier));
            Result rs = table.get(get);
            byte[] bt =rs.getValue(Bytes.toBytes(family), Bytes.toBytes(qualifier));
            System.out.println(qualifier+":"+Bytes.toString(bt));
            table.close();
        } catch (IOException e) {
            e.printStackTrace();
        }finally{
            if(connection!=null){
                try {
                    connection.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    /**
     * 删除表中指定行的指定列的值
     * @param tableName 表名
     * @param row   row-key
     * @param family  列簇
     * @param column  列
     */
    public  static  void delete(String tableName,String row,String family,String column){
        Configuration conf = null;
        Connection connection = null;
        conf = HBaseConfiguration.create();
        conf.set("hbase.master", "192.168.0.111:60000");
        conf.set("hbase.zookeeper.quorum","192.168.0.114,192.168.0.115,192.168.0.116");
        conf.set("hbase.zookeeper.property.clientPort", "2181");
        try {
            connection = ConnectionFactory.createConnection(conf);
            Table table = connection.getTable(TableName.valueOf(tableName));
            Delete del = new Delete(Bytes.toBytes(row));
            del.addColumn(Bytes.toBytes(family),Bytes.toBytes(column));
            table.delete(del);
            System.out.println(tableName+"表中的"+row +"行中"+column+"列数据已删除");
            table.close();
        } catch (IOException e) {
            e.printStackTrace();
        }finally{
            if(connection!=null){
                try {
                    connection.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    /**
     * 删除一张表
     * @param tableName
     */
    public  static  void  dropTable(String tableName){
        Configuration conf = null;
        Connection connection = null;
        conf = HBaseConfiguration.create();
        conf.set("hbase.master", "192.168.0.111:60000");
        conf.set("hbase.zookeeper.quorum","192.168.0.114,192.168.0.115,192.168.0.116");
        conf.set("hbase.zookeeper.property.clientPort", "2181");
        try {
            Connection conn = ConnectionFactory.createConnection(conf);
            Admin  admin = conn.getAdmin();
            if(admin.isTableAvailable(TableName.valueOf(tableName))){
                admin.disableTable(TableName.valueOf(tableName)); //删除之前表，先把表设置不能使用
                admin.deleteTable(TableName.valueOf(tableName));  //删除表
                System.out.println(tableName+"表已删除");
            }else{
                System.out.println("您要删除的"+tableName+"表不存在");
            }

            admin.close();
            conn.close();

        } catch (IOException e) {
            e.printStackTrace();
        }

    }
}
