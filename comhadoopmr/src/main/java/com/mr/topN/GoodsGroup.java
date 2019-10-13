package com.mr.topN;


import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author zhijie
 * @date 2019-10-05 21:38
 *
 * 在ReduceTask将文件写入磁盘并排序后按照Key进行分组，判断下一个key是否相同，将同组的Key传给reduce()执行
 */
public class GoodsGroup extends WritableComparator {

    public GoodsGroup() {
        super(Goods.class, true);
    }

    //分组:在分区之后，根据key来进行分组，相同key在同一组

    public int compare(WritableComparable a, WritableComparable b) {
        Goods goodsa = (Goods)a;
        Goods goodsb = (Goods)b;
        /**
         * 相同的userId一组
         *
         * */
        String userIda = goodsa.getUserId();
        String userIdb = goodsb.getUserId();

        int userIdCompareResult = userIda.compareTo(userIdb);

        if (userIdCompareResult == 0) {
            String dateTimea = goodsa.getDatetime();
            String dateTimeb = goodsb.getDatetime();

            return dateTimea.compareTo(dateTimeb);
        } else {
            return userIdCompareResult;
        }
    }

}
