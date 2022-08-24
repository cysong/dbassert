package com.github.cysong.dbassert.sql;

import com.github.cysong.dbassert.assertion.Assertion;
import com.github.cysong.dbassert.expression.AggregateCondition;
import com.github.cysong.dbassert.expression.Condition;
import com.github.cysong.dbassert.expression.ListCondition;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public abstract class AbstractSqlBuilder implements SqlBuilder {
    protected Assertion assertion;
    protected SqlResult result;

    @Override
    public SqlResult build(Assertion assertion) {
        this.assertion = assertion;
        this.result = SqlResult.create();
        parseSelectColumns();
        buildSql();
        return result;
    }

    protected void parseSelectColumns() {
        if (assertion.getVerifies() != null) {
            Map<String, List<Condition>> columns = assertion.getVerifies().stream()
                    .filter(con -> !(con instanceof ListCondition || con instanceof AggregateCondition))
                    .collect(Collectors.groupingBy(Condition::getColumnName));
            result.setColumns(columns);

            Map<String, List<ListCondition>> listColumns = assertion.getVerifies().stream()
                    .filter(con -> con instanceof ListCondition)
                    .map(con -> (ListCondition) con)
                    .collect(Collectors.groupingBy(ListCondition::getColumnName));
            result.setListColumns(listColumns);

            Set<String> columnSet = new HashSet<>();
            columnSet.addAll(columns.keySet());
            columnSet.addAll(listColumns.keySet());
            result.setColumnSet(columnSet);

            Map<String, List<AggregateCondition>> aggColumns = assertion.getVerifies().stream()
                    .filter(con -> con instanceof AggregateCondition)
                    .map(con -> (AggregateCondition) con)
                    .collect(Collectors.groupingBy(AggregateCondition::getColumnName));
            List<String> wrapCountColumns = aggColumns.values().stream()
                    .flatMap(List::stream)
                    .map(this::getAggregateStatement)
                    .collect(Collectors.toList());
            result.setAggColumns(aggColumns);
            result.setWrapAggColumns(wrapCountColumns);
        }
    }

    /**
     * build sql suite for current database type
     *
     * @author cysong
     * @date 2022/8/23 16:39
     **/
    protected abstract void buildSql();

    protected String getAggregateStatement(AggregateCondition con) {
        return con.getAggregate().getWrappedStatement(con.getColumnName(), getOpenQuote(), getCloseQuote());
    }

    protected String getFullTableName() {
        return assertion.getDatabase() == null ? assertion.getTableName() : assertion.getDatabase() + "." + assertion.getTableName();
    }

    protected String getQuotedFullTableName() {
        return assertion.getDatabase() == null ? quotedIdentifier(assertion.getTableName()) : quotedIdentifier(assertion.getDatabase()) + "." + (assertion.getTableName());
    }

    protected String quotedIdentifier(String identifier) {
        return getOpenQuote() + identifier + getCloseQuote();
    }

    protected String getOpenQuote() {
        return "";
    }

    protected String getCloseQuote() {
        return "";
    }

    protected String wrapFilterValue(Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof CharSequence) {
            return wrapFilterStringValue((CharSequence) value);
        }
        if (value instanceof Iterable) {
            Iterator<?> it = ((Iterable<?>) value).iterator();
            if (it.hasNext()) {
                boolean isString = it.next() instanceof CharSequence;

                return StreamSupport.stream(((Iterable<?>) value).spliterator(), false)
                        .map(item -> isString ? wrapFilterStringValue((CharSequence) item) : String.valueOf(item))
                        .collect(Collectors.joining(",", "(", ")"));
            }
            return null;
        }
        return String.valueOf(value);
    }

    private String wrapFilterStringValue(CharSequence value) {
        if (value == null) {
            return null;
        }
        return "'" + value + "'";
    }
}
