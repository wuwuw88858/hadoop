package com.mr.sort;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author zhijie
 * @date 2019-10-05 13:50
 */
public class sortReduce extends Reducer<Person, NullWritable, Person, NullWritable> {

    public void reduce(Person key, Iterable<NullWritable> val, Context context) {
        try {
            context.write(key, NullWritable.get());
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
