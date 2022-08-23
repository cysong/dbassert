package com.github.cysong.dbassert.expression;

import com.github.cysong.dbassert.constant.Comparator;

import java.util.Optional;

/**
 * condition describe a assertion or filter
 *
 * @author cysong
 * @date 2022/08/22 15:50
 **/
public class Condition {
    protected String columnName;
    protected String alias;
    protected Comparator comparator;
    protected Object expected;

    public static Condition create(String columnName, Comparator comparator) {
        return Condition.create(columnName, comparator, null);
    }

    public static Condition create(String columnName, Comparator comparator, Object expected) {
        return new Condition(columnName, comparator, expected);
    }

    protected Condition(String columnName, Comparator comparator, Object expected) {
        this.columnName = columnName;
        this.comparator = comparator;
        this.expected = expected;
    }

    public String getAssertMessage(Object actual) {
        return comparator.getAssertMessage(this, actual);
    }

    public String getColumnName() {
        return columnName;
    }


    public Comparator getComparator() {
        return comparator;
    }


    public Object getExpected() {
        return expected;
    }

    public String getAlias() {
        return alias;
    }

    public String getAliasOrName() {
        return Optional.ofNullable(alias).orElse(columnName);
    }

    public void setAlias(String alias) {
        this.alias = alias;
    }
}
