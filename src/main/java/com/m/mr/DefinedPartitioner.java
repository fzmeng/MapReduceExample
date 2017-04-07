package com.m.mr;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * @author mengfanzhu
 * @Package com.m.mr
 * @Description: 自定义分区 按照年龄分区
 * @date 17/4/7 14:00
 */
public class DefinedPartitioner extends Partitioner<DefinedCombinationKey,IntWritable> {
    @Override
    public int getPartition(DefinedCombinationKey key, IntWritable value, int n) {
        if (n == 0) {
            return 0;
        }
        String[] arr = key.getFirstKey().toString().split(",");
        int age = Integer.parseInt(arr[2]);
        if (age <= 20) {
            return 0;
        } else if (age <= 50) {
            return 1 % n;
        } else {
            return 2 % n;
        }
    }
}