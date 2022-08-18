package com.github.cysong.dbassert.sql;

import com.github.cysong.dbassert.assertion.Assertion;
import com.github.cysong.dbassert.expression.AggregateCondition;
import com.github.cysong.dbassert.expression.Condition;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public abstract class AbstractSqlBuilder implements SqlBuilder {
    protected Assertion assertion;
    protected SqlResult result;

    public SqlResult build(Assertion assertion) {
        this.assertion = assertion;
        this.result = SqlResult.create();
        parseSelectColumns();
        buildSql();
        return result;
    }

    protected abstract void buildSql();

    protected String getFullTableName() {
        return assertion.getDatabase() == null ? assertion.getTableName() : assertion.getDatabase() + "." + assertion.getTableName();
    }

    protected void parseSelectColumns() {
        if (assertion.getVerifies() != null) {
            Map<String, List<Condition>> columns = assertion.getVerifies().stream().
                    filter(con -> !(con instanceof AggregateCondition))
                    .collect(Collectors.groupingBy(exp -> exp.getColumnName()));
            result.setColumns(columns);

            Map<String, List<AggregateCondition>> aggColumns = assertion.getVerifies().stream().
                    filter(con -> con instanceof AggregateCondition)
                    .map(con -> (AggregateCondition) con)
                    .collect(Collectors.groupingBy(exp -> exp.getColumnName()));
            List<String> wrapCountColumns = aggColumns.values().stream()
                    .flatMap(List::stream)
                    .map(AggregateCondition::getWrappedExpression)
                    .collect(Collectors.toList());
            result.setAggColumns(aggColumns);
            result.setWrapAggColumns(wrapCountColumns);
        }
    }
}
