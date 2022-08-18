package com.github.cysong.dbassert.expression;

import com.github.cysong.dbassert.constant.Aggregate;
import com.github.cysong.dbassert.constant.Comparator;

public class CountCondition extends AggregateCondition {
    private boolean distinct;

    public static CountCondition create(String columnName, Comparator comparator, Object expect) {
        return new CountCondition(Aggregate.COUNT, columnName, comparator, expect);
    }

    public static CountCondition distinct(String columnName, Comparator comparator, Object expect) {
        return new CountCondition(Aggregate.DISTINCT_COUNT, columnName, comparator, expect, true);
    }

    protected CountCondition(Aggregate aggregate, String columnName, Comparator comparator, Object expect) {
        super(aggregate, columnName, comparator, expect);
    }

    protected CountCondition(Aggregate aggregate, String columnName, Comparator comparator, Object expect, boolean distinct) {
        super(aggregate, columnName, comparator, expect);
        this.distinct = distinct;
    }

    public boolean isDistinct() {
        return distinct;
    }
    
}
