package com.sponge.srd.mr;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Cluster;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.QueueInfo;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.reduce.IntSumReducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.List;

/**
 * Created by liuhb on 2014/12/12.
 */
public class Driver extends Configured implements Tool {

    @Override
    public int run(String[] args) throws Exception {
        if (args.length < 2 ) {
            System.out.println("Usage: CheckMapper <in> <out> ");
            return 2;
        }

        Job job = Job.getInstance(getConf(), "WordCountMapper");

        job.setJarByClass(WordCountMapper.class);
        //job.setInputFormatClass(CustCombineFileInputFormat.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        //job.setMapperClass(SmallFileMapper.class);
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        TextInputFormat.addInputPath(job, new Path(args[0]));
        TextOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setNumReduceTasks(0);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = 0;
        exitCode = ToolRunner.run(new Driver(), args);
        System.exit(exitCode);
    }
}
