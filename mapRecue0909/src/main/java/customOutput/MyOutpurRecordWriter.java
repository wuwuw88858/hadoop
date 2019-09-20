package customOutput;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

/**
 * 自定义RecorderWriter 继承RecordWriter
 *  这个类决定这如何读取reduce任务汇总的数据
 *  key ：对应reduce任务输出的key
 *  val:对应reduce任务输出的val
 * */
public class MyOutpurRecordWriter extends RecordWriter<Text, NullWritable> {

    FSDataOutputStream enHanceOut = null;
    FSDataOutputStream toCrawOut = null;

    public MyOutpurRecordWriter(FSDataOutputStream enHanceOut, FSDataOutputStream toCrawOut) {

        this.enHanceOut = enHanceOut;
        this.toCrawOut = toCrawOut;
    }

    /**
     * 自定义kv对的输出逻辑
     * 将好评 写入一个文件
     * 将中评 差评写入一个文件
     *
     * */
    public void write(Text text, NullWritable nullWritable) throws IOException, InterruptedException {
        String evaluate = text.toString().split("\t")[9];
        if (evaluate.equals("0")) {
            toCrawOut.write(text.toString().getBytes());
            toCrawOut.write("\r\n".getBytes());
        } else {
            enHanceOut.write(text.toString().getBytes());
            enHanceOut.write("\r\n".getBytes());
        }
    }

    public void close(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        if (enHanceOut != null) {
            enHanceOut.close();
        }
        if (toCrawOut != null) {
            toCrawOut.close();
        }
    }
}
