package com.github.cysong.dbassert.expression;

import com.github.cysong.dbassert.constant.Aggregate;
import com.github.cysong.dbassert.constant.Comparator;

public class AggregateCondition extends Condition {
    private Aggregate aggregate;

    public static AggregateCondition create(Aggregate aggregate, String columnName, Comparator comparator, Object expected) {
        return new AggregateCondition(aggregate, columnName, comparator, expected);
    }

    protected AggregateCondition(Aggregate aggregate, String columnName, Comparator comparator, Object expected) {
        super(columnName, comparator, expected);
        this.aggregate = aggregate;
    }

    public String getWrappedStatement(String openQuote, String closeQuote) {
        return aggregate.getWrappedStatement(columnName, openQuote, closeQuote);
    }

    public String getWrappedColumnLabel() {
        return aggregate.getWrappedColumnLabel(columnName);
    }

    public Aggregate getAggregate() {
        return aggregate;
    }
}
