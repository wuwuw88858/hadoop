package customOutput;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * 自定义输出格式类
 * 泛型：reduce任务输出的key，val对
 *
 * */
public class MyOutputFormat extends FileOutputFormat<Text, NullWritable> {

    //两个输出的文件
    String path1 = "hdfs://node01:8082/MRFileTest/myOutputFormat/enHance.txt";
    String path2 = "hdfs://node01:8082/MRFileTest/myOutputFormat/toCraw.txt";

    //重写RecordWriter，数据输出到哪里
    public RecordWriter<Text, NullWritable> getRecordWriter(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        //以下类似HDFS写入文件
        FileSystem fileSystem = FileSystem.get(taskAttemptContext.getConfiguration());
        Path enhancePath = new Path(path1);
        Path toCrawPath = new Path(path2);
        FSDataOutputStream enHanceOut = fileSystem.create(enhancePath);
        FSDataOutputStream toCrawOut = fileSystem.create(toCrawPath);
        //返回一个RecordWriter,如何读取reduce任务中的每条数据
        MyOutpurRecordWriter myOutpurRecordWriter = new MyOutpurRecordWriter(enHanceOut, toCrawOut);
        return myOutpurRecordWriter;
    }
}
