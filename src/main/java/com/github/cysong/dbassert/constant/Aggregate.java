package com.github.cysong.dbassert.constant;

import java.text.MessageFormat;

public enum Aggregate {
    COUNT("count", "cnt_", "count({2}{0}{3}) {1}{0}"),
    DISTINCT_COUNT("distinct count", "dc_", "count(distinct {2}{0}{3}) {1}{0}"),
    SUM("sum", "sum_", "sum({2}{0}{3}) {1}{0}"),
    AVG("avg", "avg_", "avg({2}{0}{3}) {1}{0}"),
    MIN("min", "min_", "min({2}{0}{3}) {1}{0}"),
    MAX("max", "max_", "max({2}{0}{3}) {1}{0}"),
    DISTINCT("distinct", "dis_", "distinct({2}{0}{3}) {1}{0}");

    private String function;
    private String prefix;
    private String format;

    Aggregate(String function, String prefix, String format) {
        this.function = function;
        this.prefix = prefix;
        this.format = format;
    }

    public String getWrappedStatement(String columnName, String openQuote, String closeQuote) {
        return MessageFormat.format(format, columnName, prefix, openQuote, closeQuote);
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
