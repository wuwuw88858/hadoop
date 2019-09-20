package customInput;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


/**
 * 让主类继承Configured类，实现Tool接口
 * 实现run()方法
 * 将以前main()方法中的逻辑，放到run()中
 * 在main()中，调用ToolRunner.run()方法，第一个参数是当前对象；第二个参数是输入、输出
 */

public class SmallFiles2SequenceFileMain extends Configured implements Tool {

    public int run(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "combine small file to sequencefile");

        job.setJarByClass(SmallFiles2SequenceFileMain.class);

        //自定义输入格式
        job.setInputFormatClass(customInput.WholeFileInputFormat.class);
        WholeFileInputFormat.setInputPaths(job, new Path(args[0]));

        //定义输出格式为sequenceFile
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        SequenceFileOutputFormat.setOutputPath(job, new Path(args[1]));

        //设置输出kv对类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(BytesWritable.class);

        //设置map类 自定义的mapper类
        job.setMapperClass(customInput.SequenceMapper.class);


        return job.waitForCompletion(true)? 0: 1;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new SmallFiles2SequenceFileMain(), args);
        System.exit(exitCode);
    }
}
