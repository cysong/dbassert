package com.github.cysong.dbassert.expression;

import com.github.cysong.dbassert.constant.Comparator;

/**
 * condition on a list values of a single column returned by query
 *
 * @author cysong
 * @date 2022/8/23 10:31
 **/
public class OverallCondition extends Condition {

    protected OverallCondition(String columnName, Comparator comparator, Object expected) {
        super(columnName, comparator, expected);
    }
    
}
