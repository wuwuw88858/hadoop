package customInput;

import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;

import java.io.IOException;

/**
 * 自定义输入类（ map任务如何读取分片） 继承FileInputFormat
 *
 *
 * FileInputFormat<NullWritable, ByteWritable>
 *     key:NullWritable:不需要使用到，因此为null
 *     val:ByteWritable:用于存储文件的小内容
 * */

public class WholeFileInputFormat extends FileInputFormat<NullWritable, BytesWritable>{


    /**
     * 设置文件不可以分割
     *  由于文件是小文件，直接读取文件的全部内容，不需要分割
     */
    public boolean isSplitable(JobContext context, Path filename) {
        return false;
    }
    /**
     * 创建一个RecordReader,来执行具体如何读取每个分片里的内容
     * */
    public RecordReader<NullWritable, BytesWritable> createRecordReader(InputSplit inputSplit, TaskAttemptContext taskAttemptContext)
            throws IOException, InterruptedException {
        //自定义RecordReader
        WholeRecordReader wholeRecordReader = new WholeRecordReader();
        wholeRecordReader.initialize(inputSplit, taskAttemptContext);
        return wholeRecordReader;
    }
}
