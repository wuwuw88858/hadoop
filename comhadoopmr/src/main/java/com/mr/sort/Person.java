package com.mr.sort;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author zhijie
 * @date 2019-10-05 12:00
 */
public class Person implements WritableComparable<Person> {

    private String name;
    private int age;
    private int salary;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getAge() {
        return age;
    }

    public void setAge(int age) {
        this.age = age;
    }

    public int getSalary() {
        return salary;
    }

    public void setSalary(int salary) {
        this.salary = salary;
    }

    public Person(String name, int age, int salary) {
        this.name = name;
        this.age = age;
        this.salary = salary;
    }

    @Override
    public String toString() {
        return "Person{" +
                "name='" + name + '\'' +
                ", age=" + age +
                ", salary=" + salary +
                '}';
    }

    //先比较salary, 高的排序在前(this.xx - o.xx 取反)，若相同 age小的在前
    public int compareTo(Person o) {
        int compareSalaryResult = this.salary - o.salary;
        if (compareSalaryResult == 0) {
            return this.age - o.age;
        }
        else {
            return - compareSalaryResult;
        }
    }

    //序列化 将key转成二进制
    public void write(DataOutput out) throws IOException {
        out.writeUTF(name);
        out.writeInt(age);
        out.writeInt(salary);
    }

        //使用in读字段的顺序，要与write方法中写的顺序保持一致
    public void readFields(DataInput in) throws IOException {
        this.name = in.readUTF();
        this.age = in.readInt();
        this.salary = in.readInt();
    }
}
