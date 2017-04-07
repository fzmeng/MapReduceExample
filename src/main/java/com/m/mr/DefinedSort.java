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

        logger.info("---------out DefinedComparator flag---------");
        return c2.getSecondKey().get()-c1.getSecondKey().get();//0,负数,正数
    }
}