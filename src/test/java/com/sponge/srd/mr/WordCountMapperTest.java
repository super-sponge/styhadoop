package com.sponge.srd.mr;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

public class WordCountMapperTest {
    private  MapDriver<LongWritable,Text,Text, IntWritable> mapDriver = null;

    @Before
    public void setUp() {
        WordCountMapper mapper = new WordCountMapper();
        mapDriver = MapDriver.newMapDriver(mapper);
    }
    @Test
    public void testMap() throws Exception {
        mapDriver.withInput(new LongWritable(1), new Text("Hello this this"));
        mapDriver.withInput(new LongWritable(2), new Text("Good"));
        mapDriver.withOutput(new Text("Hello"), new IntWritable(1));
        mapDriver.withOutput(new Text("this"), new IntWritable(1));
        mapDriver.withOutput(new Text("this"), new IntWritable(1));
        mapDriver.withOutput(new Text("Good"), new IntWritable(1));
        mapDriver.runTest();
    }

}