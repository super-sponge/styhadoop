package com.sponge.srd.hive;

import org.apache.log4j.Logger;

import java.sql.*;

/**
 * Created by sponge on 15-1-30.
 *
 *
 * 	我们可以通过CLI、Client、Web UI等Hive提供的用户接口来和Hive通信，但这三种方式最常用的是CLI；Client 是Hive的客户端，用户连接至 Hive Server。在启动 Client 模式的时候，需要指出Hive Server所在节点，并且在该节点启动 Hive Server。 WUI 是通过浏览器访问 Hive。今天我们来谈谈怎么通过HiveServer来操作Hive。
 Hive提供了jdbc驱动，使得我们可以用Java代码来连接Hive并进行一些类关系型数据库的sql语句查询等操作。同关系型数据库一样，我们也需要将Hive的服务打开；在Hive 0.11.0版本之前，只有HiveServer服务可用，你得在程序操作Hive之前，必须在Hive安装的服务器上打开HiveServer服务，如下：
 hive --service hiveserver -p 10000

 上面代表你已经成功的在端口为10000（默认的端口是10000）启动了hiveserver服务。这时候，你就可以通过Java代码来连接hiveserver，代码请参考HiveJdbcClient.java
 编译上面的代码，之后就可以运行(我是在集成开发环境下面运行这个程序的)，结果如下：

 　　如果你想在脚本里面运行,请参考hivejdbctest.sh 脚本

 上面是用Java连接HiveServer，而HiveServer本身存在很多问题（比如：安全性、并发性等）；针对这些问题，Hive0.11.0版本提供了一个全新的服务：HiveServer2，这个很好的解决HiveServer存在的安全性、并发性等问题。这个服务启动程序在${HIVE_HOME}/bin/hiveserver2里面，你可以通过下面的方式来启动HiveServer2服务：
 $HIVE_HOME/bin/hiveserver2

 也可以通过下面的方式启动HiveServer2
 $HIVE_HOME/bin/hive --service hiveserver2

 两种方式效果都一样的。但是以前的程序需要修改两个地方，如下所示：
 private static String driverName = "org.apache.hadoop.hive.jdbc.HiveDriver";
 改为
 private static String driverName = "org.apache.hive.jdbc.HiveDriver";
 Connection con = DriverManager.getConnection(
 "jdbc:hive://localhost:10002/default", "wyp", "");
 改为
 Connection con = DriverManager.getConnection(
 "jdbc:hive2://localhost:10002/default", "wyp", "");

 其他的不变就可以了。
 　　如果你是用Maven，加入以下依赖
 <dependency>
 <groupId>org.apache.hive</groupId>
 <artifactId>hive-jdbc</artifactId>
 <version>0.14.0</version>
 </dependency>

 <dependency>
 <groupId>org.apache.hadoop</groupId>
 <artifactId>hadoop-common</artifactId>
 <version>2.6.0</version>
 </dependency>
 *
 *
 *
 *
 */
public class HiveServer2Client {
    private static final Logger log = Logger.getLogger(HiveServer2Client.class);
    private static String driverName = "org.apache.hive.jdbc.HiveDriver";
    private static String url = "jdbc:hive2://localhost:10000/default";
    private static String user = "sponge";
    private static String password = "";
    private static String sql = "";
    private static ResultSet res;

    public static void main(String[] args) throws SQLException {
        try {
            Class.forName(driverName);
            System.out.println("connect to hive thrift");
            Connection conn = DriverManager.getConnection(url, user, password);
            Statement stmt = conn.createStatement();

            // 创建的表名
            String tableName = "testHiveDriverTable";
            /** 第一步:存在就先删除 **/
            sql = "drop table " + tableName;
            System.out.println(sql);
            stmt.execute(sql);

            /** 第二步:不存在就创建 **/
            sql = "create table " + tableName + " (key int, value string)  row format delimited fields terminated by '\t'";
            stmt.execute(sql);

            // 执行“show tables”操作
            sql = "show tables '" + tableName + "'";
            System.out.println("Running:" + sql);
            res = stmt.executeQuery(sql);
            System.out.println("执行“show tables”运行结果:");
            if (res.next()) {
                System.out.println(res.getString(1));
            }

            // 执行“describe table”操作
            sql = "describe " + tableName;
            System.out.println("Running:" + sql);
            res = stmt.executeQuery(sql);
            System.out.println("执行“describe table”运行结果:");
            while (res.next()) {
                System.out.println(res.getString(1) + "\t" + res.getString(2));
            }

            // 执行“load data into table”操作
            String filepath = "/tmp/userinfo.txt";
            sql = "load data local inpath '" + filepath + "' into table " + tableName;
            System.out.println("Running:" + sql);
            stmt.execute(sql);

            // 执行“select * query”操作
            sql = "select * from " + tableName;
            System.out.println("Running:" + sql);
            res = stmt.executeQuery(sql);
            System.out.println("执行“select * query”运行结果:");
            while (res.next()) {
                System.out.println(res.getInt(1) + "\t" + res.getString(2));
            }

            // 执行“regular hive query”操作
            sql = "select count(1) from " + tableName;
            System.out.println("Running:" + sql);
            res = stmt.executeQuery(sql);
            System.out.println("执行“regular hive query”运行结果:");
            while (res.next()) {
                System.out.println(res.getString(1));

            }

            conn.close();
            conn = null;
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            log.error(driverName + " not found!", e);
            System.exit(1);
        } catch (SQLException e) {
            e.printStackTrace();
            log.error("Connection error!", e);
            System.exit(1);
        }

    }
}
