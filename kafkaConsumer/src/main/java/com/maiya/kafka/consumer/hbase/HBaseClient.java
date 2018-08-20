package com.maiya.kafka.consumer.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * Created by lubin 2016/11/23
 * HBase工具类
 */
public class HBaseClient {

    private Configuration conf = HBaseConfiguration.create();
    private Connection connection;
    private Admin admin;
    private Table table;
    private HColumnDescriptor[] columnFamilies;

    public Connection getConnection() {
        return connection;
    }

    public void setConnection(Connection connection) {
        this.connection = connection;
    }

    /**
     * 初始化表
     *
     * @param tablepath 表名
     * @throws IOException 异常
     */
    public HBaseClient(String tablepath) {

        Properties pro = new Properties();
        FileInputStream in;
        try {
            in = new FileInputStream("bigdata.properties");
            pro.load(in);
        } catch (IOException e) {
            e.printStackTrace();
        }

        conf.set("hbase.zookeeper.property.clientPort", "2181");
        conf.set("hbase.zookeeper.quorum", pro.getProperty("hbase.zookeeper.quorum"));
        conf.set("hbase.regionserver.lease.period", "600000");
        try {
            connection = ConnectionFactory.createConnection(conf);
            admin = connection.getAdmin();
            table = connection.getTable(TableName.valueOf(tablepath));
            columnFamilies = table.getTableDescriptor().getColumnFamilies();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    /**
     * get 获取Get
     *
     * @return get
     */
    public Scan getScan() {
        Scan scan = new Scan();
        for (HColumnDescriptor columnFamily : columnFamilies) {
            scan.addFamily(Bytes.toBytes(columnFamily.getNameAsString()));
        }
        return scan;
    }

    public Get getGet(String rowKey) {
        Get get = new Get(Bytes.toBytes(rowKey));
        for (HColumnDescriptor columnFamily : columnFamilies) {
            get.addFamily(Bytes.toBytes(columnFamily.getNameAsString()));
        }
        return get;
    }

    public Get getGet(String rowKey, Filter filter) {
        Get get = new Get(Bytes.toBytes(rowKey)).setFilter(filter);
        for (HColumnDescriptor columnFamily : columnFamilies) {
            get.addFamily(Bytes.toBytes(columnFamily.getNameAsString()));
        }
        return get;
    }

    /**
     * 插入表数据
     *
     * @param rowKey       key
     * @param columnFamily column family
     * @param columnName   列名
     * @param columnValue  列值
     * @throws IOException 异常
     */
    public void insert(String rowKey, String columnFamily, String columnName, String columnValue) throws IOException {
        byte[] row1 = Bytes.toBytes(rowKey);
        Put p1 = new Put(row1);

        p1.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(columnName), Bytes.toBytes(columnValue));
        table.put(p1);
    }

    /**
     * 批量插入表数据
     *
     * @param list 列表数据
     * @throws IOException 抛出异常
     */
    public void insert(List<Put> list) throws IOException {
        table.put(list);
    }


    /*public void insert(String rowKey, String columnFamily, Bean bean) {

        List<Put> list = new ArrayList<Put>();
        Class cls = bean.getClass();

        Field[] fields = cls.getDeclaredFields();
        for (Field f : fields) {
            f.setAccessible(true);

            byte[] row = Bytes.toBytes(rowKey);
            Put put = new Put(row);
            // 注意：强转可能会报错
            try {
                put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(f.getName()), Bytes.toBytes(String.valueOf(f.get(bean))));
            } catch (IllegalAccessException e) {
                System.out.println("插入bean日志出错" + e.getMessage());
            }

            list.add(put);
        }

        try {
            table.put(list);
        } catch (IOException e) {
            System.out.println("插入bean日志出错" + e.getMessage());
        }
    }*/

    public void delete(String row) {
        Delete del = new Delete(Bytes.toBytes(row));
        try {
            table.delete(del);
        } catch (IOException e) {
            //删除失败
            e.printStackTrace();
        }
    }


    public void delete(List<String> rows) {

        List<Delete> delList = new ArrayList<>();
        for (String row : rows) {
            Delete del = new Delete(Bytes.toBytes(row));
            delList.add(del);
        }
        try {
            table.delete(delList);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    public void free() {
        try {
            table.close();
            admin.close();
            connection.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
