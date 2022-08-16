package com.github.cysong.dbassert.constant;

import java.text.MessageFormat;

public enum Aggregate {
    COUNT("count", "cnt_", "count(`{0}`) {1}{0}"),
    DISTINCT_COUNT("distinct count", "dc_", "count(distinct `{0}`) {1}{0}"),
    SUM("sum", "sum_", "sum(`{0}`) {1}{0}"),
    AVG("avg", "avg_", "avg(`{0}`) {1}{0}"),
    MIN("min", "min_", "min(`{0}`) {1}{0}"),
    MAX("max", "max_", "max(`{0}`) {1}{0}"),
    DISTINCT("distinct", "dis_", "distinct(`{0}`) {1}{0}");

    private String function;
    private String prefix;
    private String format;

    Aggregate(String function, String prefix, String format) {
        this.function = function;
        this.prefix = prefix;
        this.format = format;
    }

    public String getWrappedExpression(String columnName) {
        return MessageFormat.format(format, columnName, prefix);
    }

    public String getWrappedColumnLabel(String columnName) {
        return prefix + columnName;
    }

    public String getFunction() {
        return function;
    }

    public String getPrefix() {
        return prefix;
    }

    public String getFormat() {
        return format;
    }
}
