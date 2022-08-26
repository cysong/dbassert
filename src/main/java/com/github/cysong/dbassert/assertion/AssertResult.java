package com.github.cysong.dbassert.assertion;

import com.github.cysong.dbassert.constant.Aggregate;
import com.github.cysong.dbassert.constant.Comparator;
import com.github.cysong.dbassert.expression.AggregateCondition;
import com.github.cysong.dbassert.expression.Condition;

import java.util.ArrayList;
import java.util.List;

/**
 * @author cysong
 * @date 2022/8/26 15:03
 **/
public class AssertResult {
    private List<Detail> details;

    public static AssertResult create() {
        return new AssertResult();
    }

    private AssertResult() {
        this.details = new ArrayList<>();
    }

    public AssertResult addSuccess(String column, Comparator comparator, Object actual, Object expected) {
        this.details.add(Detail.success(column, comparator, actual, expected));
        return this;
    }

    public AssertResult addSuccess(String column, Aggregate aggregate, Comparator comparator, Object actual, Object expected) {
        this.details.add(Detail.success(column, aggregate, comparator, actual, expected));
        return this;
    }

    public AssertResult addFail(String column, Comparator comparator, Object actual, Object expected) {
        this.details.add(Detail.fail(column, comparator, actual, expected));
        return this;
    }

    public AssertResult addFail(String column, Aggregate aggregate, Comparator comparator, Object actual, Object expected) {
        this.details.add(Detail.fail(column, aggregate, comparator, actual, expected));
        return this;
    }

    public AssertResult addSuccess(String column, Aggregate aggregate, Comparator comparator, boolean success, Object actual, Object expected) {
        this.details.add(Detail.create(column, aggregate, comparator, success, actual, expected));
        return this;
    }

    public AssertResult add(boolean pass, AggregateCondition con, Object actual) {
        this.details.add(Detail.create(con.getColumnName(), con.getAggregate(), con.getComparator(), pass, actual, con.getExpected()));
        return this;
    }

    public AssertResult add(boolean pass, Condition con, Object actual) {
        this.details.add(Detail.create(con.getColumnName(), null, con.getComparator(), pass, actual, con.getExpected()));
        return this;
    }

    public AssertResult add(String column, Aggregate aggregate, Comparator comparator, boolean pass, Object actual, Object expected) {
        this.details.add(Detail.create(column, aggregate, comparator, pass, actual, expected));
        return this;
    }


    public List<Detail> getDetails() {
        return this.details;
    }

    public void clearDetails() {
        this.details = new ArrayList<>();
    }
}
