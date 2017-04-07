package com.m.mr;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author mengfanzhu
 * @Description: 自定义组合键
 * @date 17/4/7 13:01
 */
public class DefinedCombinationKey implements WritableComparable<DefinedCombinationKey> {
    private static final Logger logger = LoggerFactory.getLogger(DefinedCombinationKey.class);
    private Text firstKey;
    private IntWritable secondKey;
    public DefinedCombinationKey() {
        this.firstKey = new Text();
        this.secondKey = new IntWritable();
    }
    public Text getFirstKey() {
        return this.firstKey;
    }
    public void setFirstKey(Text firstKey) {
        this.firstKey = firstKey;
    }
    public IntWritable getSecondKey() {
        return this.secondKey;
    }
    public void setSecondKey(IntWritable secondKey) {
        this.secondKey = secondKey;
    }
    public void readFields(DataInput dateInput) throws IOException {
        // TODO Auto-generated method stub
        this.firstKey.readFields(dateInput);
        this.secondKey.readFields(dateInput);
    }
    public void write(DataOutput outPut) throws IOException {
        this.firstKey.write(outPut);
        this.secondKey.write(outPut);
    }
    /**
     * 自定义比较策略
     * 注意：该比较策略用于mapreduce的第一次默认排序，也就是发生在map阶段的sort小阶段，
     * 发生地点为环形缓冲区(可以通过io.sort.mb进行大小调整)
     */
    public int compareTo(DefinedCombinationKey definedCombinationKey) {
        logger.info("-------CombinationKey flag-------");
        return this.secondKey.compareTo(definedCombinationKey.getSecondKey());
    }
}