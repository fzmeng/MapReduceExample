package com.m.mr;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

/**
 * @author mengfanzhu
 * @Package com.m.mr
 * @Description: 自定义Map & Reducer
 * @date 17/4/7 14:04
 */
public class DefinedMapReducer extends Mapper<Object, Text, DefinedCombinationKey, IntWritable> {
    DefinedCombinationKey combinationKey=new DefinedCombinationKey();
    Text sortName = new Text();
    IntWritable score = new IntWritable();

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        // no,name, age, gender, score
        String[] arr = value.toString().split(",");
        score.set(Integer.parseInt(arr[4]));
        sortName.set(arr[1]+","+arr[3]+","+arr[2]);
        combinationKey.setFirstKey(sortName);
        combinationKey.setSecondKey(score);
        context.write(combinationKey, score);
    }
}

/**
 * 自定义reducer
 */
class DefinedReducer extends Reducer<DefinedCombinationKey, IntWritable, Text, Text> {

    StringBuffer sb=new StringBuffer();
    Text sore=new Text();

    @Override
    protected void reduce(DefinedCombinationKey key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {
        sb.delete(0, sb.length());
        Iterator<IntWritable> it=values.iterator();

        while (it.hasNext()) {
            sb.append(it.next()+",");
        }

        if (sb.length()>0) {
            sb.deleteCharAt(sb.length()-1);
        }
        sore.set(sb.toString());
        context.write(key.getFirstKey(),sore);
    }
}
