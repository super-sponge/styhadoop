package com.sponge.srd.hbase;

import com.sponge.srd.utils.HBaseUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by sponge on 15-2-27.
 */
public class HBaseTest {

    public static String table = "htable";
    public static String rowkey = "rowkey";

    public static void createTable(String tableName) {
        System.out.println("Start create table ...");

        HBaseAdmin hBaseAdmin = null;
        try {
            hBaseAdmin = new HBaseAdmin(HBaseUtils.getConfiguration());
            if (hBaseAdmin.tableExists(tableName)) {
                hBaseAdmin.disableTable(tableName);
                hBaseAdmin.deleteTable(tableName);
                System.out.println(tableName + " is exist, delete ...");
            }

            HTableDescriptor desc = new HTableDescriptor(tableName);
            desc.addFamily(new HColumnDescriptor("column1"));
            desc.addFamily(new HColumnDescriptor("column2"));
            desc.addFamily(new HColumnDescriptor("column3"));

            hBaseAdmin.createTable(desc);

        } catch (MasterNotRunningException e) {
            e.printStackTrace();
        } catch (ZooKeeperConnectionException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    public static void insertData(String tableName) {
        System.out.println("Start insert data ...");
        try {

            HTableInterface table = HBaseUtils.getHConnection().getTable(tableName);
            Put put = new Put(rowkey.getBytes());// 一个PUT代表一行数据，再NEW一个PUT表示第二行数据,每行一个唯一的ROWKEY，此处rowkey为put构造方法中传入的值
            put.add("column1".getBytes(), null, "aaa".getBytes());// 本行数据的第一列
            put.add("column2".getBytes(), null, "bbb".getBytes());// 本行数据的第三列
            put.add("column3".getBytes(), null, "ccc".getBytes());// 本行数据的第三列

            table.put(put);
            table.close();

        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.println("end insert data ......");
    }

    public static void deleteData(String tableName, String rowkey) {
        try {
            System.out.println("Start delete ...");
            HTable hTable = new HTable(HBaseUtils.getConfiguration(), tableName);
            List list = new ArrayList();
            Delete d1 = new Delete(rowkey.getBytes());
            hTable.delete(list);
            System.out.println("Delete succeed!");

            hTable.close();

        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    @Before
    public void testcreateTable() {
        createTable(table);
    }

    @Test
    public void testHBase() {
        insertData(table);
        deleteData(table, rowkey);
    }
}
