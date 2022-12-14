package com.github.cysong.dbassert.constant;

import com.github.cysong.dbassert.expression.AggregateCondition;
import com.github.cysong.dbassert.expression.Boundary;
import com.github.cysong.dbassert.expression.CompositeCondition;
import com.github.cysong.dbassert.expression.Condition;

import java.text.MessageFormat;
import java.util.Arrays;
import java.util.List;

/**
 * comparator for assertion
 *
 * @author cysong
 * @date 2022/08/22 15:50
 **/
public enum Comparator {

    EQUAL("equal", "{0} expected equal {1}, actual is {2}"),
    NOT_EQUAL("not equal", "{0} expected not equal {1}, actual is {2}"),
    NULL("is null", "{0} expected to be null, actual is {1}"),
    NOT_NULL("is not null", "{0} expected to be not null, actual is {1}"),
    IS_TRUE("is true", "{0} expected to be true, actual is {1}"),
    IS_FALSE("is false", "{0} expected to be false, actual is {1}"),
    LESS_THAN("less than", "{0} expected less than {1}, actual is {2}"),
    LESS_THAN_OR_EQUAL("less than or equal", "{0} expected less or equal to {1}, actual is {2}"),
    GREATER_THAN("greater than", "{0} expected greater than {1}, actual is {2}"),
    GREATER_THAN_OR_EQUAL("greater than or equal", "{0} expected greater or equal to {1}, actual is {2}"),
    BETWEEN("between", "{0} expected in range {1}, actual is {2}"),
    IN("in", "{0} expected in {1}, actual is {2}"),
    NOT_IN("not in", "{0} expected not in {1}, actual is {2}"),
    MATCHES("matches", "{0} expected matches <Predicate>, actual is {1}"),
    NOT_MATCH("not match", "{0} expected not match <Predicate>, actual is {1}"),
    CONTAINS("contains", "{0} expected contains {1}, actual is {2}"),
    NOT_CONTAIN("not contain", "{0} expected not contain {1}, actual is {2}"),
    LIST_IS_EMPTY("is empty", "list {0} expected to be empty, actual is {2}"),
    LIST_NOT_EMPTY("is not empty", "list {0} expected not to be empty, actual is {2}"),
    LIST_HAS_SIZE("has the size of given", "list of {0} expected has size of {1}, actual is {2}"),
    LIST_EQUALS("all elements equal at order", "list of {0} expected all elements equal {1}, actual is {2}"),
    LIST_NOT_EQUAL("not all elements equal at order", "list of {0} expected not all elements equal {1}, actual is {2}"),
    LIST_EQUALS_AT_ANY_ORDER("all elements equal at any order", "list of {0} expected all elements equal(any order) {1}, actual is {2}"),
    LIST_CONTAINS("contains all elements", "list of {0} expected contains all elements of {1}, actual is {2}"),
    LIST_NOT_CONTAIN("not contain any elements", "list of {0} expected not contain any element of {1}, actual is {2}"),
    LIST_CONTAINS_ANY("contains any of given elements", "list of {0} expected contains any elements of {1}, actual is {2}"),
    LIST_IS_ORDERED_ASC("is ordered ascending", "list of {0} expected ordered ascending {1}, actual is {2}"),
    LIST_IS_ORDERED_DESC("is ordered descending", "list of {0} expected ordered descending {1}, actual is {2}"),
    LIST_MATCHES("entire list matches", "list {0} expected matches <Predicate>, actual is {2}"),
    LIST_NOT_MATCH("entire list not match", "list {0} expected not match <Predicate>, actual is {2}"),
    JSON_EQUAL("value(extract by jsonpath) equal to", "property(jsonpath {3}) value of {0} expected equal {1}, actual is {2} ");

    private String desc;
    private String format;

    Comparator(String desc, String format) {
        this.desc = desc;
        this.format = format;
    }

    /**
     * build assert message if fail
     *
     * @param con    condition for assert
     * @param actual actual value return by query
     * @return java.lang.String
     * @author cysong
     * @date 2022/8/23 10:01
     **/
    public String getAssertMessage(Condition con, Object actual) {
        String alias = con.getAliasOrName();
        if (con instanceof AggregateCondition) {
            AggregateCondition aggCon = (AggregateCondition) con;
            alias = aggCon.getAggregate().getFunction() + " of " + alias;
        }

        Object expected = con.getExpected();
        if (this == BETWEEN) {
            return MessageFormat.format(format, alias, ((Boundary) expected).toString(), actual);
        }
        if (this == NULL || this == NOT_NULL || this == IS_TRUE || this == IS_FALSE) {
            return MessageFormat.format(format, alias, actual);
        }
        if (this == JSON_EQUAL) {
            CompositeCondition com = (CompositeCondition) con;
            return MessageFormat.format(format, alias, expected, actual, com.getProperty());
        }
        return MessageFormat.format(format, alias, expected, actual);
    }

    public static List<Comparator> noArgs() {
        return Arrays.asList(NULL, NOT_NULL, IS_TRUE, IS_FALSE, LIST_IS_EMPTY, LIST_NOT_EMPTY,
                LIST_IS_ORDERED_ASC, LIST_IS_ORDERED_DESC);
    }
}
