package com.github.cysong.dbassert.expression;

import com.github.cysong.dbassert.constant.Comparator;

/**
 * assert the list made of return values of a single column
 * even if just one row returned
 *
 * @author cysong
 * @date 2022/8/23 14:55
 **/
public class ListCondition extends Condition {

    public static ListCondition create(String columnName, Comparator comparator, Object expected) {
        return new ListCondition(columnName, comparator, expected);
    }

    protected ListCondition(String columnName, Comparator comparator, Object expected) {
        super(columnName, comparator, expected);
    }
}
