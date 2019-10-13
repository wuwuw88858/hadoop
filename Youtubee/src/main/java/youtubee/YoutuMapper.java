package youtubee;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author zhijie
 * @date 2019-10-13 11:09
 *
 * aiYwo5K0VWg
 * suttsteve
 * 504
 * News & Politics
 * 100
 * 6339
 * 4.75
 * 24
 * 22
 * LQUV_XGzHmA	d0VYKbEbXQ8	nfBfC8bif1Y	KtZ8K0r3_9I	lE_OnRo4Hmc	nou6FJ69790	JJLZwluFr9Q	RA1PSoJEYzM	nYYuYVHgsjQ	p6gyPCOMqQg	kX9A5CqZGhw	LIfhqQMiNIk	E4dUa8oXK5w	Ti0YgdEkiac	GZxDKgBMDSI	YTH6GpvLv1I	dt8sQ62oRuU	IKN3Df-b2Uw	WbBM-CRZ5iA	b8xOSUUf7cI

 */
public class YoutuMapper extends Mapper<LongWritable, Text, Text, NullWritable> {

    private Text text = new Text();


    public void map(LongWritable key, Text val, Context context) {
        Counter counter = context.getCounter("destroyData", "val.length < 9");
        Counter nullCounter = context.getCounter("NullData", "val.null");
       if (val == null || val.equals("")) {
           nullCounter.increment(1L);

       }
        String[] fields = val.toString().split("\t");
        //残缺数据
        if (fields.length < 9) {
            counter.increment(1L);

        }
        else {
            //将视频类别的空格去掉 News & Politics ---> News&Politics
            fields[3] = fields[3].replace(" ", "");
            StringBuilder stringBuilder = new StringBuilder();  //定义可变长度的String
            for (int i = 0; i < fields.length; i++) {
                if (i < 9) {
                    stringBuilder.append(fields[i]).append("\t");   //前 8 个字段都用\t分割
                } else  if (i > 9 && i < fields.length - 1) {
                    stringBuilder.append(fields[i]).append("&");
                }
            }
            text.set(stringBuilder.toString());
            try {
                context.write(text, NullWritable.get());
            } catch (IOException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

    }
}
