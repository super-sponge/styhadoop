学习研究hadoop相关技术
======================

## 动机

* 研究hadoop编程技术

## 领域

* hadoop mr 编程
* hive 编程
* hadoop  部署

## mr编程

### mr 程序依赖程序包 groupID org.apache.hadoop
* hadoop-common
* hadoop-hdfs
* hadoop-mapreduce-client-core

## hive 编程

### hiveUDF 程序依赖程序包 groupID org.apache.hive
* hive-exec
* hive-common
* 同时需要hadoop的hadoop-common
### hiveUDF使用
  add jar /home/hadoop/styhadoop-1.0-SNAPSHOT.jar;

  CREATE TEMPORARY FUNCTION decode AS 'com.sponge.srd.hive.UDFDecode';
  CREATE TEMPORARY FUNCTION encode AS 'com.sponge.srd.hive.UDFEncode';


  select encode('facebook') from tmp;
  select encode('facebook','fkey','skey','tkey') from tmp;

  select decode('1CC7376126B8AE1DE343E4C20EAE9ADA') from tmp;
  select decode('5BB6A40B0CEA149B0A1645E74C7E460C','fkey','skey','tkey') from tmp;
### hiveJdbc 使用
  详细参考 com.sponge.srd.hive.HiveServer2Client
## flume 编程
### flume custom components
    add pom.xml conf
            <dependency>
                <groupId>org.apache.flume</groupId>
                <artifactId>flume-ng-sdk</artifactId>
                <version>${flume.version}</version>
            </dependency>
    参考:http://flume.apache.org/FlumeDeveloperGuide.html
    RPC client
    开发sink与source需要添加flume-ng-core模块
## hbase 编程
### hbase maven configuration
        <dependency>
            <groupId>org.apache.hbase</groupId>
            <artifactId>hbase-client</artifactId>
            <version>${hbase.version}</version>
        </dependency>
