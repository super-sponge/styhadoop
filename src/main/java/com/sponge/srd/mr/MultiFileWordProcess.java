package com.sponge.srd.mr;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.CombineFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.CombineFileRecordReader;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.reduce.IntSumReducer;
import org.apache.hadoop.util.LineReader;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.StringTokenizer;

/**
 * Created by liuhb on 2014/12/11.
 */
public class MultiFileWordProcess extends Configured implements Tool {

    public static class TextOffset implements WritableComparable {
        private long offset;
        private String fileName;


        @Override
        public int compareTo(Object o) {
            if (o instanceof TextOffset) {
                TextOffset that = (TextOffset) o;
                int result = this.fileName.compareTo(that.fileName);
                if (result == 0) {
                    return (int)Math.signum((double)(this.offset - that.offset));
                } else {
                    return result;
                }
            }
            return -1;
        }

        @Override
        public void write(DataOutput out) throws IOException {
            out.writeLong(this.offset);
            Text.writeString(out, this.fileName);
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            this.offset = in.readLong();
            this.fileName = Text.readString(in);
        }

        public boolean equals(Object obj) {
            if(obj instanceof TextOffset)
                return this.compareTo(obj) == 0;
            return false;
        }
        @Override
        public int hashCode() {
            assert false : "hashCode not designed";
            return 42; //an arbitrary constant
        }

        @Override
        public String toString() {
            return this.fileName + '\t' + this.offset;
        }
    }

    public static class CombineFileLineRecordReader extends RecordReader<TextOffset, Text> {

        private long startOffset;
        private long end;
        private long pos;
        private FileSystem fs ;
        private Path path;
        private TextOffset key;
        private Text value;

        private FSDataInputStream fileIn;
        private LineReader reader;

        public  CombineFileLineRecordReader(CombineFileSplit split, TaskAttemptContext context, Integer index) throws IOException, InterruptedException {
            this.path = split.getPath(index);
            this.fs = this.path.getFileSystem(context.getConfiguration());
            this.startOffset = split.getOffset(index);
            this.end = startOffset + split.getLength(index);

            boolean skipFirstLine = false;

            fileIn = fs.open(this.path);
            if (startOffset != 0 ) {
                skipFirstLine = true;
                --startOffset;
                fileIn.seek(startOffset);
            }
            this.reader = new LineReader(fileIn);
            if (skipFirstLine) {
                startOffset +=  reader.readLine(new Text(), 0,
                        (int)Math.min((long)Integer.MAX_VALUE, end - startOffset));
            }

            this.pos = startOffset;
        }

        @Override
        public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        }

        @Override
        public boolean nextKeyValue() throws IOException, InterruptedException {
            if (key == null) {
                key = new TextOffset();
                key.fileName = path.getName();
            }
            key.offset = pos;
            if (value == null) {
                value = new Text();
            }
            int newSize = 0;
            if (pos < end) {
                newSize = reader.readLine(value);
                pos += newSize;
            }
            if (newSize == 0) {
                key = null;
                value = null;
                return false;
            } else {
                return true;
            }
        }

        @Override
        public TextOffset getCurrentKey() throws IOException, InterruptedException {
            return this.key;
        }

        @Override
        public Text getCurrentValue() throws IOException, InterruptedException {
            return this.value;
        }

        @Override
        public float getProgress() throws IOException, InterruptedException {
            if (startOffset == end) {
                return 0.0f;
            } else {
                return Math.min(1.0f, (pos - startOffset) / (float) (end - startOffset));
            }
        }

        @Override
        public void close() throws IOException {
            //IOUtils.closeStream(this.fileIn);
        }
    }

    public static class CombineInputFormat
            extends CombineFileInputFormat<TextOffset, Text> {

        public RecordReader<TextOffset,Text> createRecordReader(InputSplit split,
                                                                TaskAttemptContext context) throws IOException {
            return new CombineFileRecordReader<TextOffset, Text>(
                    (CombineFileSplit)split, context, CombineFileLineRecordReader.class);
        }
    }

    public static class MapProcess extends
            Mapper<TextOffset, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public void map(TextOffset key, Text value, Context context)
                throws IOException, InterruptedException {

            String line = value.toString();
            StringTokenizer itr = new StringTokenizer(line);
            while (itr.hasMoreTokens()) {
                word.set(itr.nextToken());
                context.write(word, one);
            }
        }
    }

    private void printUsage() {
        System.out.println("Usage : MultiFileWordProcess <input_dir> <output>" );
    }

    @Override
    public int run(String[] args) throws Exception {

        if(args.length < 2) {
            printUsage();
            return 2;
        }

        Job job = new Job(getConf());
        job.setJobName("MultiFileWordProcess");
        job.setJarByClass(MultiFileWordProcess.class);

        //set the InputFormat of the job to our InputFormat
        job.setInputFormatClass(CombineInputFormat.class);

        // the keys are words (strings)
        job.setOutputKeyClass(Text.class);
        // the values are counts (ints)
        job.setOutputValueClass(IntWritable.class);

        //use the defined mapper
        job.setMapperClass(MapProcess.class);
        //use the WordCount Reducer
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);

        FileInputFormat.addInputPaths(job, args[0]);
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int ret = ToolRunner.run(new MultiFileWordProcess(), args);
        System.exit(ret);
    }

}
