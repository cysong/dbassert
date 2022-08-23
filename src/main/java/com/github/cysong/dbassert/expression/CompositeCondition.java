package com.github.cysong.dbassert.expression;

import com.github.cysong.dbassert.constant.Comparator;

/**
 * condition for a composite column type, such as json
 *
 * @author cysong
 * @date 2022/08/22 15:50
 **/
public class CompositeCondition extends Condition {
    protected String property;

    protected CompositeCondition(String columnName, String property, Comparator comparator, Object expected) {
        super(columnName, comparator, expected);
        this.property = property;
    }

    public String getProperty() {
        return property;
    }
}
