package customInput;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * 自定义mapper
 *  mapper类输入的kv
 *      k:无需记录任何东西，因此为NullWritable
 *      v：记录小文件的内容
 *  mapper类输出的kv
 *      k:文件名称
 *      v:文件内容
 * */
public class SequenceMapper extends Mapper<NullWritable, BytesWritable, Text, BytesWritable> {

    private Text fileNameKey;
    /**
     * 执行map任务的时候，只调用一次，相当于初始化
     * */
    public void setup(Context context) {
        InputSplit inputSplit = context.getInputSplit();    //获取读取的文件分片
        Path filePath = ((FileSplit)inputSplit).getPath(); //将分片强制转换成文件的分片，获取路径
        fileNameKey = new Text(filePath.toString());
    }

    /**
     * @param NullWritable 输入到map任务中的key
     * @param BytesWritable 输入到map任务中的val，即文件内容
     * @param Context 应用上下文
     * */
    public void map(NullWritable nullWritable, BytesWritable bytesWritable, Context context) {
        try {
            context.write(fileNameKey, bytesWritable);
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

}
