package com.github.cysong.dbassert.expression;

import com.github.cysong.dbassert.constant.Comparator;

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
