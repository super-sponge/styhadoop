package com.sponge.srd.hbase;

import com.sponge.srd.utils.HBaseUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
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
            put.add(Bytes.toBytes("column1"), Bytes.toBytes("c1"),Bytes.toBytes("c1:aaa"));


            List lst =new  ArrayList();
            lst.add(put);

            put = new Put(Bytes.toBytes("rowkey2"));
            put.add(Bytes.toBytes("column1"), Bytes.toBytes("c1"),Bytes.toBytes("c1:aaa"));
            put.add("column1".getBytes(), null, "aaa".getBytes());// 本行数据的第一列
            put.add("column2".getBytes(), null, "aaa".getBytes());// 本行数据的第一列

            lst.add(put);

            table.put(lst);
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
            list.add(d1);
            hTable.delete(list);
            System.out.println("Delete succeed!");

            hTable.close();

        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    public static void queryAllData(String tableName){
        try {
            System.out.println("query all data ...");
            HTable htable = new HTable(HBaseUtils.getConfiguration(), tableName);
            ResultScanner rs  = htable.getScanner(new Scan());
            for (Result r : rs) {
                System.out.println("get rowkey:" + new String(r.getRow()));
                dispCells(r.rawCells());
            }

            htable.close();
        } catch (IOException e ){
            e.printStackTrace();
        }

    }

    public static void queryByKey(String tableName, String key){
        try {
            System.out.println("query " + tableName + " by row " + key);
            HTable htable = new HTable(HBaseUtils.getConfiguration(), tableName);
            Get get = new Get(Bytes.toBytes(key));
            Result r = htable.get(get);
            System.out.println("Result size is " + r.size());
            dispCells(r.rawCells());
            htable.close();

        } catch (IOException e ) {
            e.printStackTrace();
        }
    }

    /*
    the condition between filter is or . if the column is not exists, filter is true
     */

    public static void queryByConditionColumn(String tableName, String column, String value){
        try {
            System.out.println("query " + tableName + " by column " + column + " value " + value);
            HTable htable = new HTable(HBaseUtils.getConfiguration(), tableName);
            Filter filter = new SingleColumnValueFilter(Bytes.toBytes(column),
                    null,
                    CompareFilter.CompareOp.EQUAL, Bytes.toBytes(value));
            Scan s = new Scan();
            s.setFilter(filter);

            ResultScanner rs = htable.getScanner(s);
            for (Result r : rs){
                System.out.println("get rowkey:" + new String(r.getRow()));
                dispCells(r.rawCells());
            }

            htable.close();

        }catch (IOException e ) {
            e.printStackTrace();
        }
    }

    public static void queryByConditionColumnCon(){
        try {
            System.out.println("query " + table);
            HTable htable = new HTable(HBaseUtils.getConfiguration(), table);
            List<Filter> filters = new ArrayList<Filter>();

            Filter filter1 = new SingleColumnValueFilter(Bytes
                    .toBytes("column1"), null, CompareFilter.CompareOp.EQUAL, Bytes
                    .toBytes("aaa"));
            filters.add(filter1);

            Filter filter2 = new SingleColumnValueFilter(Bytes
                    .toBytes("column2"), null, CompareFilter.CompareOp.EQUAL, Bytes
                    .toBytes("bbb"));
            filters.add(filter2);

            Filter filter3 = new SingleColumnValueFilter(Bytes
                    .toBytes("column3"), null, CompareFilter.CompareOp.EQUAL, Bytes
                    .toBytes("ccc"));
            filters.add(filter3);

            FilterList filterList1 = new FilterList(filters);

            Scan scan = new Scan();
            scan.setFilter(filterList1);
            ResultScanner rs = htable.getScanner(scan);
            for (Result r : rs){
                System.out.println("get rowkey:" + new String(r.getRow()));
                dispCells(r.rawCells());
            }

            htable.close();

        }catch (IOException e ) {
            e.printStackTrace();
        }
    }


    private static void dispCells(Cell[] cells){
        for(Cell cell : cells) {
            System.out.println("column： " + new String(CellUtil.cloneFamily(cell))
                    + "    value: " + new String(CellUtil.cloneValue(cell)));
        }
    }

    @Before
    public void testcreateTable() {
        createTable(table);
    }

    @Test
    public void testHBase() {
        insertData(table);
        queryAllData(table);
        queryByKey(table, "rowkey");
        queryByKey(table,"rowkey2");
        queryByKey(table,"rowkey3");
        queryByConditionColumn(table, "column1", "aaa");
        queryByConditionColumn(table,"column3", "ccc");
        queryByConditionColumnCon();
        //deleteData(table, rowkey);
    }
}
