package com.sponge.srd.mr;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by liuhb on 2014/12/12.
 */
public class SmallFileMapper extends Mapper<CustCombineFileInputFormat.FileOffset, Text, Text, NullWritable> {

    private static NullWritable nullWritable = NullWritable.get();

    public void map(CustCombineFileInputFormat.FileOffset key, Text value, Context context) throws IOException, InterruptedException {
        context.write(value, nullWritable);
    }
}
