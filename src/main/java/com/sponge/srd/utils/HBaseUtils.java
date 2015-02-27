package com.sponge.srd.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;

import java.io.IOException;

/**
 * Created by sponge on 15-2-27.
 */
public class HBaseUtils {

    private static final String QUORUM = "localhost";
    private static final String CLIENTPORT = "2181";
    private static final String HMASTERHOST="localhost";
    private static final String HMASTERHOSTPORT="60000";

    private static Configuration conf = null;
    private static HConnection conn = null;


    public static synchronized Configuration getConfiguration(){
        if(conf == null)
        {
            conf =  HBaseConfiguration.create();
            conf.set("hbase.zookeeper.quorum", QUORUM);
            conf.set("hbase.zookeeper.property.clientPort", CLIENTPORT);
            conf.set("hbase.master", HMASTERHOST + ":" + HMASTERHOSTPORT);
        }
        return conf;
    }

    public static synchronized HConnection getHConnection() throws IOException {
        if(conn == null)
        {
            conn = HConnectionManager.createConnection(getConfiguration());
        }
        return conn;
    }
}
