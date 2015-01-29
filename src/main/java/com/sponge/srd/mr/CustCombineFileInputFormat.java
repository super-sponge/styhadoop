package com.sponge.srd.mr;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.CombineFileRecordReader;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;
import org.apache.hadoop.util.LineReader;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by liuhb on 2014/12/12.
 */
public class CustCombineFileInputFormat extends CombineFileInputFormat<CustCombineFileInputFormat.FileOffset, Text> {

    public RecordReader<FileOffset, Text> createRecordReader(InputSplit split,
                                                             TaskAttemptContext context) throws IOException {
        return new CombineFileRecordReader<FileOffset, Text>(
                (CombineFileSplit) split, context, CombineFileLineRecordReader.class);
    }

    public static class FileOffset implements WritableComparable {
        private long offset;
        private String fileName;

        public long getOffset() {
            return offset;
        }

        public void setOffset(long offset) {
            this.offset = offset;
        }

        public String getFileName() {
            return fileName;
        }

        public void setFileName(String fileName) {
            this.fileName = fileName;
        }

        @Override
        public int compareTo(Object o) {
            if (o instanceof FileOffset) {
                FileOffset that = (FileOffset) o;
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
            if(obj instanceof FileOffset)
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


    public static class CombineFileLineRecordReader extends RecordReader<FileOffset, Text> {

        private long startOffset;
        private long end;
        private long pos;
        private FileSystem fs ;
        private Path path;
        private FileOffset key;
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
                key = new FileOffset();
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
        public FileOffset getCurrentKey() throws IOException, InterruptedException {
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

}
