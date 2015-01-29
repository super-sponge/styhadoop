package com.sponge.srd.mr;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Created by sponge on 15-1-12.
 */
public class SMSCDRMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    private Text status = new Text();
    private final static IntWritable addOne = new IntWritable(1);

    /**
     * Returns the SMS status code and its count
     */
    public void map(LongWritable key, Text value, Mapper.Context context)
            throws java.io.IOException, InterruptedException {

        //655209;1;796764372490213;804422938115889;6 is the Sample record format
        String[] line = value.toString().split(";");
        // If record is of SMS CDR
        if (Integer.parseInt(line[1]) == 1) {
            status.set(line[4]);
            context.write(status, addOne);
        }
    }
}
