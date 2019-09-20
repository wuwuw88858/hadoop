package customInput;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;


import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * 自定义RecordReader,继承RecordReader
 *  key:无需记录任何东西，因此为null
 *  val：记录小文件的内容
 * */
public class WholeRecordReader extends RecordReader<NullWritable, BytesWritable>{

    //需要读取的分片
    private FileSplit fileSplit;
    private Configuration con;

    //记录读取的信息
    private BytesWritable val = new BytesWritable();

    //变量标识，判断分片是否已经读取过，因为小文件不可分割，读取一次便读取完成 true：没读取 false:读取完成
    private boolean processed = false;
    /**
     *
     * 初始化配置信息
     * */
    public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        this.fileSplit = (FileSplit) inputSplit;
        this.con = taskAttemptContext.getConfiguration();
    }

    public boolean nextKeyValue() throws IOException, InterruptedException {
        //第一次读取信息，为true
        if (! processed) {
            byte[] contents = new byte[(int) fileSplit.getLength()];  //创建一个字节数组，为读取从分片中写入的数据
            //以下类似HDFS读取文件操作
            Path filePath = fileSplit.getPath();    //获取分片所在路径
            FileSystem fileSystem = filePath.getFileSystem(con);    //打开文件系统
            FSDataInputStream in = null;    //定义一个输入流读取数据
            in = fileSystem.open(filePath); //打开等待读取的文件
            IOUtils.readFully(in, contents, 0, contents.length);    //从in输入流中读取--->字节数组，从0开始读，一直到整个分片的数据读完为止
            val.set(contents, 0, contents.length);//由于map任务传入的数据类型是byteWritable，因此需要将字节数组--> byteWritable

            IOUtils.closeStream(in);//关闭流
            processed = true;   //设置为true，即文件已经全部读取完成
            return true;
        }
        return false;
    }

    /**
     * 获取当前的key
     * 无需任何东西
     * */
    public NullWritable getCurrentKey() throws IOException, InterruptedException {

        return NullWritable.get();
    }

    /**
     * 获取当前的val
     *  返回刚刚从字节数组写入到BytesWritable里的对象
     * */
    public BytesWritable getCurrentValue() throws IOException, InterruptedException {
        return val;
    }

    /**
     * 获取当前读取分片数据的进度，这里要么没读，要么读取完成
     * */
    public float getProgress() throws IOException, InterruptedException {
        return processed == true ? 1.0f : 0.0f;
    }

    public void close() throws IOException {

    }
}
