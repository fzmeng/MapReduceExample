package com.m.mr;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author mengfanzhu
 * @Description: 自定义二次排序策略
 * @date 17/4/7 13:04
 */
public class DefinedSort extends WritableComparator {
    private static final Logger logger = LoggerFactory.getLogger(DefinedSort.class);
    public DefinedSort() {
        super(DefinedCombinationKey.class,true);
    }
    @Override
    public int compare(WritableComparable combinationKeyOne,
                       WritableComparable CombinationKeyOther) {
        logger.info("---------enter DefinedComparator flag---------");

        DefinedCombinationKey c1 = (DefinedCombinationKey) combinationKeyOne;
        DefinedCombinationKey c2 = (DefinedCombinationKey) CombinationKeyOther;

        /**
         * 确保进行排序的数据在同一个区内，如果不在同一个区则按照组合键中第一个键排序
         * 另外，这个判断是可以调整最终输出的组合键第一个值的排序
         * 下面这种比较对第一个字段的排序是升序的，如果想降序这将c1和c2颠倒过来（假设1）
         */
        logger.info("---------out DefinedComparator flag---------");
        return c2.getSecondKey().get()-c1.getSecondKey().get();//0,负数,正数
    }
}