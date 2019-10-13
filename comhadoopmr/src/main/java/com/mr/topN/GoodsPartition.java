package com.mr.topN;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * @author zhijie
 * @date 2019-10-05 21:12
 *
 *  map任务完成只有，进行分区操作 相同的userId给到同一个reduce
 */
public class GoodsPartition extends Partitioner<Goods, DoubleWritable> {
    public int getPartition(Goods goods, DoubleWritable doubleWritable, int reduceTaskNum) {
        return goods.getUserId().hashCode() % reduceTaskNum;
    }
}
