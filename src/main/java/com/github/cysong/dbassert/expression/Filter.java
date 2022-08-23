package com.github.cysong.dbassert.expression;

import com.github.cysong.dbassert.constant.Comparator;
import com.github.cysong.dbassert.utitls.Utils;

/**
 * filter for where statement
 *
 * @author cysong
 * @date 2022/8/23 10:34
 **/
public class Filter extends AbstractFilter {

    protected String columnName;
    protected Comparator comparator;
    protected Object value;

    public static Filter create(String columnName, Object value) {
        if (value == null) {
            return new Filter(columnName, Comparator.NULL, null);
        }
        return new Filter(columnName, Comparator.EQUAL, value);
    }

    public static Filter create(String columnName, Comparator comparator, Object value) {
        return new Filter(columnName, comparator, value);
    }

    protected Filter(String columnName, Comparator comparator, Object value) {
        assert Utils.isNotBlank(columnName);
        assert comparator != null;
        this.columnName = columnName;
        this.comparator = comparator;
        this.value = value;
    }

    public String getColumnName() {
        return columnName;
    }

    public Comparator getComparator() {
        return comparator;
    }

    public Object getValue() {
        return value;
    }
}
