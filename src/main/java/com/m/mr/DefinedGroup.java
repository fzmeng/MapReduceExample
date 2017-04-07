package com.m.mr;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author mengfanzhu
 * @Description: 自定义分组策略
 * @date 17/4/7 13:00
 */
public class DefinedGroup extends WritableComparator {
    private static final Logger logger = LoggerFactory.getLogger(DefinedGroup.class);
    public DefinedGroup() {
        super(DefinedCombinationKey.class,true);
    }
    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        logger.info("-------enter DefinedGroupSort flag-------");
        DefinedCombinationKey ck1 = (DefinedCombinationKey)a;
        DefinedCombinationKey ck2 = (DefinedCombinationKey)b;
        logger.info("-------Grouping result:"+ck1.getFirstKey().
                compareTo(ck2.getFirstKey())+"-------");
        logger.info("-------out DefinedGroupSort flag-------");
        return ck1.getFirstKey().compareTo(ck2.getFirstKey());
    }
}
