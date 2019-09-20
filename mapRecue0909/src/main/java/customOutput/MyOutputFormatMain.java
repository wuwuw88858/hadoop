package customOutput;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import javax.xml.soap.Text;

public class MyOutputFormatMain extends Configured implements Tool {


    public int run(String[] args) throws Exception {
        Configuration con = new Configuration();
        Job job = Job.getInstance(con, MyOutputFormatMain.class.getSimpleName());

        job.setJarByClass(customOutput.MyOutputFormatMain.class);

        //设置输入类型
        job.setInputFormatClass(TextInputFormat.class);
        //设置输入文件路径
        TextInputFormat.setInputPaths(job, new Path(args[0]));

        //设置输出类型
        job.setOutputFormatClass(MyOutputFormat.class);
        //设置一个输出目录，这个目录会输出一个success的成功标志的文件
        MyOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(customOutput.MyMapper.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new MyOutputFormatMain(),
                args);
        System.exit(exitCode);
    }
}
