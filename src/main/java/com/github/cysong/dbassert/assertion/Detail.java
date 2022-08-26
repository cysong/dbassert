package com.github.cysong.dbassert.assertion;

import com.github.cysong.dbassert.constant.Aggregate;
import com.github.cysong.dbassert.constant.Comparator;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Predicate;

/**
 * assert column detail
 *
 * @author cysong
 * @date 2022/8/26 14:40
 **/
public class Detail {
    private String column;
    private Aggregate aggregate;
    private Comparator comparator;
    private boolean pass;
    private Object actual;
    private Object expected;

    public static Detail success(String column, Comparator comparator, Object actual, Object expected) {
        return success(column, null, comparator, actual, expected);
    }

    public static Detail success(String column, Aggregate aggregate, Comparator comparator, Object actual, Object expected) {
        return new Detail(column, aggregate, comparator, true, actual, expected);
    }

    public static Detail fail(String column, Comparator comparator, Object actual, Object expected) {
        return fail(column, null, comparator, actual, expected);
    }

    public static Detail fail(String column, Aggregate aggregate, Comparator comparator, Object actual, Object expected) {
        return new Detail(column, aggregate, comparator, false, actual, expected);
    }

    public static Detail create(String column, Aggregate aggregate, Comparator comparator, boolean pass, Object actual, Object expected) {
        return new Detail(column, aggregate, comparator, pass, actual, expected);
    }

    private Detail(String column, Aggregate aggregate, Comparator comparator, boolean pass, Object actual, Object expected) {
        this.column = column;
        this.aggregate = aggregate;
        this.comparator = comparator;
        this.pass = pass;
        this.actual = actual;
        this.expected = expected;
    }

    public static String[] getTableHeader() {
        return new String[]{"column", "aggregate", "actual", "comparator", "expected", "result"};
    }

    public String[] getTableRow() {
        List<String> cols = new ArrayList<>(5);
        cols.add(column);
        cols.add(Optional.ofNullable(aggregate).map(Aggregate::getFunction).orElse(""));
        cols.add(String.valueOf(actual));
        cols.add(comparator.name());
        cols.add(getExpectedValue());
        cols.add(pass ? "pass" : "fail");
        return cols.toArray(new String[5]);
    }

    private String getExpectedValue() {
        if (expected instanceof Predicate) {
            return "";
        }
        if (Comparator.noArgs().contains(comparator)) {
            return "";
        }
        return String.valueOf(expected);
    }

    public String getColumn() {
        return column;
    }

    public void setColumn(String column) {
        this.column = column;
    }

    public Aggregate getAggregate() {
        return aggregate;
    }

    public void setAggregate(Aggregate aggregate) {
        this.aggregate = aggregate;
    }

    public Comparator getComparator() {
        return comparator;
    }

    public void setComparator(Comparator comparator) {
        this.comparator = comparator;
    }

    public Object getExpected() {
        return expected;
    }

    public void setExpected(Object expected) {
        this.expected = expected;
    }

    public Object getActual() {
        return actual;
    }

    public void setActual(Object actual) {
        this.actual = actual;
    }

    public boolean isPass() {
        return pass;
    }

    public void setPass(boolean pass) {
        this.pass = pass;
    }
}
