package com.github.cysong.dbassert.constant;

import java.text.MessageFormat;

/**
 * database aggregate functions
 *
 * @author cysong
 * @date 2022/08/22 15:50
 **/
public enum Aggregate {
    /**
     * aggregate function count
     */
    COUNT("count", "cnt_", "count({2}{0}{3}) {1}{0}"),
    /**
     * aggregate function count(distinct columnName)
     */
    DISTINCT_COUNT("distinct count", "dc_", "count(distinct {2}{0}{3}) {1}{0}"),
    /**
     * aggregate function sum
     */
    SUM("sum", "sum_", "sum({2}{0}{3}) {1}{0}"),
    /**
     * aggregate function avg
     */
    AVG("avg", "avg_", "avg({2}{0}{3}) {1}{0}"),
    /**
     * aggregate function sum
     */
    MIN("min", "min_", "min({2}{0}{3}) {1}{0}"),
    /**
     * aggregate function sum
     */
    MAX("max", "max_", "max({2}{0}{3}) {1}{0}");

    private String function;
    private String prefix;
    private String format;

    Aggregate(String function, String prefix, String format) {
        this.function = function;
        this.prefix = prefix;
        this.format = format;
    }

    /**
     * the select statement of this aggregate column
     *
     * @param columnName column name
     * @param openQuote  the open quote of database
     * @param closeQuote the close quote of database
     * @return java.lang.String such as count(`name`) cnt_name
     * @author cysong
     * @date 2022/8/23 9:52
     **/
    public String getWrappedStatement(String columnName, String openQuote, String closeQuote) {
        return MessageFormat.format(format, columnName, prefix, openQuote, closeQuote);
    }

    /**
     * aggregate column label
     *
     * @param columnName column name
     * @return java.lang.String
     * @author cysong
     * @date 2022/8/23 9:54
     **/
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
