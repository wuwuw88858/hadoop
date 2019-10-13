package com.mr.sort;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author zhijie
 * @date 2019-10-05 13:08
 */
public class sortMap extends Mapper<LongWritable, Text, Person, NullWritable> {

    //每行 tom	20	8000
    public void map(LongWritable key, Text val, Context context) {
            String[] field = val.toString().split("\t");
            String name = field[0];
            int age = Integer.parseInt(field[1]);
            int salary = Integer.parseInt(field[2]);

            Person person = new Person(name, age, salary);
        try {
            context.write(person, NullWritable.get());
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
