package com.mr.topN;



import org.apache.commons.lang.time.DateUtils;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Mapper;



import java.io.IOException;

/**
 * @author zhijie
 * @date 2019-10-05 18:23
 *
 * 13764633023	2014-12-01 02:20:42.000	全视目Allseelook 原宿风暴显色美瞳彩色隐形艺术眼镜1片 拍2包邮	33.6	2	18067781305
 */
public class GoodsMapper extends Mapper<LongWritable, Text, Goods, DoubleWritable> {

    DoubleWritable totalPirceWritable = new DoubleWritable();

    public void map(LongWritable key, Text val, Context context) {
        Counter counter = context.getCounter("damageData", "data.length less than 6");
        String[] field = val.toString().split("\t");

        //有的数据存在残缺
        if (field.length == 6) {
            String userId = field[0];
            String yearMonth = field[1].substring(0, 4) + field[1].substring(5, 7);
            String title = field[2] ;
            Double unitPirce = Double.parseDouble(field[3]);
            int purchaseNum = Integer.parseInt(field[4]);
            String goodId = field[5];
            Goods goods = new Goods(userId, yearMonth, title, unitPirce, purchaseNum, goodId);

            double totalPirce = unitPirce * purchaseNum;
            totalPirceWritable.set(totalPirce);
            try {
                context.write(goods, totalPirceWritable);
            } catch (IOException e) {


            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        } else {
            counter.increment(1L);
        }
    }
}
