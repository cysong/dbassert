package com.github.cysong.dbassert;


import com.github.cysong.dbassert.assertion.Assertion;
import com.github.cysong.dbassert.assertion.AssertionExecutor;
import com.github.cysong.dbassert.constant.*;
import com.github.cysong.dbassert.exception.ConfigurationException;
import com.github.cysong.dbassert.expression.AggregateCondition;
import com.github.cysong.dbassert.expression.Boundary;
import com.github.cysong.dbassert.expression.Condition;
import com.github.cysong.dbassert.expression.Order;
import com.github.cysong.dbassert.object.Column;
import com.github.cysong.dbassert.object.DbObject;
import com.github.cysong.dbassert.option.DbAssertOptions;
import com.github.cysong.dbassert.utitls.Utils;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Predicate;

public class DbAssert {
    private static final Map<String, Connection> CONN_MAP = new ConcurrentHashMap<>();
    private Assertion assertion;
    private DbObject currentObject;
    private List<Condition> currentConditions;

    public static DbAssert create(String dbKey) {
        Connection conn = CONN_MAP.get(dbKey);
        if (conn == null) {
            conn = DbAssertOptions.getGlobal().getFactory().getConnectionByDbKey(dbKey);
        }
        if (conn == null) {
            throw new IllegalArgumentException("Connection can not be null");
        }
        return DbAssert.create(dbKey, conn);
    }

    public static DbAssert create(Connection conn) {
        String dbKey = conn.toString();
        assert Utils.isNotBlank(dbKey);
        return DbAssert.create(dbKey, conn);
    }

    public static DbAssert create(String dbKey, Connection conn) {
        return new DbAssert(dbKey, conn);
    }

    private DbAssert(String dbKey, Connection conn) {
        CONN_MAP.put(dbKey, conn);
        this.assertion = new Assertion(conn);
    }

    public DbAssert retry(boolean retry) {
        this.assertion.setRetry(retry);
        return this;
    }

    public DbAssert retryTimes(int retryTimes) {
        this.assertion.setRetryTimes(retryTimes);
        return this;
    }

    public DbAssert retryInterval(long retryInterval) {
        this.assertion.setRetryInterval(retryInterval);
        return this;
    }

    public DbAssert delay(long delay) {
        this.assertion.setDelay(delay);
        return this;
    }

    public DbAssert failIfNotFound(boolean failIfNotFound) {
        this.assertion.setFailIfNotFound(failIfNotFound);
        return this;
    }

    public DbAssert startIndex(int startIndex) {
        this.assertion.setStartIndex(startIndex);
        return this;
    }

    public DbAssert pageSize(int pageSize) {
        this.assertion.setPageSize(pageSize);
        return this;
    }

    public DbAssert database(String dbName) {
        this.setCurrentObject(ObjectType.DATABASE, dbName);
        return this;
    }

    public DbAssert table(String tableName) {
        this.setCurrentObject(ObjectType.TABLE, tableName);
        return this;
    }

    public DbAssert col(String columnName) {
        this.setCurrentObject(ObjectType.COLUMN, columnName);
        return this;
    }

    public DbAssert as(String alias) {
        this.addAlias(alias);
        return this;
    }

    public DbAssert where(String columnName, Object value) {
        this.assertion.addCondition(Condition.create(columnName, Comparator.EQUAL, value));
        return this;
    }

    public DbAssert and(String columnName, Object value) {
        this.assertion.addCondition(Condition.create(columnName, Comparator.EQUAL, value));
        return this;
    }

    public DbAssert where(String nativeCondition) {
        this.assertion.setTextCondition(nativeCondition);
        return this;
    }

    public DbAssert orderBy(String... columnNames) {
        for (String columnName : columnNames) {
            this.assertion.addSort(Sort.create(columnName));
        }
        return this;
    }

    public DbAssert orderByDesc(String... columnNames) {
        for (String columnName : columnNames) {
            this.assertion.addSort(Sort.create(columnName, Order.DESC));
        }
        return this;
    }

    public DbAssert isEqual(Object expected) {
        this.addCondition(Comparator.EQUAL, expected);
        return this;
    }

    public DbAssert isNotEqual(Object expected) {
        this.addCondition(Comparator.NOT_EQUAL, expected);
        return this;
    }

    public DbAssert isNull() {
        this.addCondition(Comparator.NULL, null);
        return this;
    }

    public DbAssert isNotNull() {
        this.addCondition(Comparator.NOT_NULL, null);
        return this;
    }

    public DbAssert isTrue() {
        this.addCondition(Comparator.IS_TRUE, null);
        return this;
    }

    public DbAssert isFalse() {
        this.addCondition(Comparator.IS_FALSE, null);
        return this;
    }

    public DbAssert greaterThan(Object expected) {
        this.addCondition(Comparator.GREATER_THAN, expected);
        return this;
    }

    public DbAssert greaterThanOrEqual(Object expected) {
        this.addCondition(Comparator.GREATER_THAN_OR_EQUAL, expected);
        return this;
    }

    public DbAssert lessThan(Object expected) {
        this.addCondition(Comparator.LESS_THAN, expected);
        return this;
    }

    public DbAssert lessThanOrEqual(Object expected) {
        this.addCondition(Comparator.LESS_THAN_OR_EQUAL, expected);
        return this;
    }

    public DbAssert between(Object expectedMin, Object expectedMax) {
        assert expectedMin != null;
        assert expectedMax != null;
        this.addCondition(Comparator.BETWEEN, Boundary.create(expectedMin, expectedMax));
        return this;
    }

    public DbAssert between(Object expectedMin, boolean excludeMin, Object expectedMax, boolean excludeMax) {
        assert expectedMin != null;
        assert expectedMax != null;
        this.addCondition(Comparator.BETWEEN, Boundary.create(expectedMin, excludeMin, expectedMax, excludeMax));
        return this;
    }

    public DbAssert in(Iterable expected) {
        assert expected != null;
        this.addCondition(Comparator.IN, expected);
        return this;
    }

    public DbAssert notIn(Iterable expected) {
        this.addCondition(Comparator.NOT_IN, expected);
        return this;
    }

    public DbAssert contains(String expected) {
        this.addCondition(Comparator.CONTAINS, expected);
        return this;
    }

    public DbAssert notContain(String expected) {
        this.addCondition(Comparator.NOT_CONTAIN, expected);
        return this;
    }

    public DbAssert contains(Iterable expected) {
        this.addCondition(Comparator.CONTAINS, expected);
        return this;
    }

    public DbAssert notContain(Object expected) {
        this.addCondition(Comparator.NOT_CONTAIN, expected);
        return this;
    }

    public DbAssert matches(Predicate predicate) {
        assert predicate != null;
        this.addCondition(Comparator.MATCHES, predicate);
        return this;
    }

    public DbAssert notMatch(Predicate predicate) {
        assert predicate != null;
        this.addCondition(Comparator.NOT_MATCH, predicate);
        return this;
    }

    public DbAssert anyMatch(Predicate predicate) {
        assert predicate != null;
        this.addCondition(Comparator.ANY_MATCH, predicate);
        return this;
    }

    public DbAssert rowsEqual(long expected) {
        assert expected >= 0;
        this.assertion.addRowVerify(Condition.create(Constants.COUNT_ROWS_COLUMN, Comparator.EQUAL, expected));
        return this;
    }

    public DbAssert rowsLessThan(long expected) {
        assert expected >= 0;
        this.assertion.addRowVerify(Condition.create(Constants.COUNT_ROWS_COLUMN, Comparator.LESS_THAN, expected));
        return this;
    }

    public DbAssert rowsLessThanOrEqual(long expected) {
        assert expected >= 0;
        this.assertion.addRowVerify(Condition.create(Constants.COUNT_ROWS_COLUMN, Comparator.LESS_THAN_OR_EQUAL, expected));
        return this;
    }

    public DbAssert rowsGreaterThan(long expected) {
        assert expected >= 0;
        this.assertion.addRowVerify(Condition.create(Constants.COUNT_ROWS_COLUMN, Comparator.GREATER_THAN, expected));
        return this;
    }

    public DbAssert rowsGreaterThanOrEqual(long expected) {
        assert expected >= 0;
        this.assertion.addRowVerify(Condition.create(Constants.COUNT_ROWS_COLUMN, Comparator.GREATER_THAN_OR_EQUAL, expected));
        return this;
    }

    public DbAssert rowsBetween(long expectedMin, long expectedMax) {
        assert expectedMin >= 0;
        assert expectedMin <= expectedMax;
        this.assertion.addRowVerify(Condition.create(Constants.COUNT_ROWS_COLUMN, Comparator.BETWEEN, Boundary.create(expectedMin, expectedMax)));
        return this;
    }

    public DbAssert rowsBetween(long expectedMin, boolean excludeMin, long expectedMax, boolean excludeMax) {
        assert expectedMin >= 0;
        assert expectedMin <= expectedMax;
        this.assertion.addRowVerify(Condition.create(Constants.COUNT_ROWS_COLUMN, Comparator.BETWEEN, Boundary.create(expectedMin, excludeMin, expectedMax, excludeMax)));
        return this;
    }

    public DbAssert countEquals(long expected) {
        assert expected >= 0;
        this.addAggregateCondition(Aggregate.COUNT, Comparator.EQUAL, expected);
        return this;
    }

    public DbAssert countLessThan(long expected) {
        assert expected >= 0;
        this.addAggregateCondition(Aggregate.COUNT, Comparator.LESS_THAN, expected);
        return this;
    }

    public DbAssert countLessThanOrEqual(long expected) {
        assert expected >= 0;
        this.addAggregateCondition(Aggregate.COUNT, Comparator.LESS_THAN_OR_EQUAL, expected);
        return this;
    }

    public DbAssert countGreaterThan(long expected) {
        assert expected >= 0;
        this.addAggregateCondition(Aggregate.COUNT, Comparator.GREATER_THAN, expected);
        return this;
    }

    public DbAssert countGreaterThanOrEqual(long expected) {
        assert expected >= 0;
        this.addAggregateCondition(Aggregate.COUNT, Comparator.GREATER_THAN_OR_EQUAL, expected);
        return this;
    }

    public DbAssert countBetween(long expectedMin, long expectedMax) {
        assert expectedMin >= 0;
        assert expectedMin <= expectedMax;
        this.addAggregateCondition(Aggregate.COUNT, Comparator.BETWEEN, Boundary.create(expectedMin, expectedMax));
        return this;
    }

    public DbAssert countBetween(long expectedMin, boolean excludeMin, long expectedMax, boolean excludeMax) {
        assert expectedMin >= 0;
        assert expectedMin <= expectedMax;
        this.addAggregateCondition(Aggregate.COUNT, Comparator.BETWEEN, Boundary.create(expectedMin, excludeMin, expectedMax, excludeMax));
        return this;
    }

    public DbAssert distinctCountEqual(long expected) {
        assert expected >= 0;
        this.addAggregateCondition(Aggregate.DISTINCT_COUNT, Comparator.EQUAL, expected);
        return this;
    }

    public DbAssert distinctCountLessThan(long expected) {
        assert expected >= 0;
        this.addAggregateCondition(Aggregate.DISTINCT_COUNT, Comparator.LESS_THAN, expected);
        return this;
    }

    public DbAssert distinctCountLessThanOrEqual(long expected) {
        assert expected >= 0;
        this.addAggregateCondition(Aggregate.DISTINCT_COUNT, Comparator.LESS_THAN_OR_EQUAL, expected);
        return this;
    }

    public DbAssert distinctCountGreaterThan(long expected) {
        assert expected >= 0;
        this.addAggregateCondition(Aggregate.DISTINCT_COUNT, Comparator.GREATER_THAN, expected);
        return this;
    }

    public DbAssert distinctCountGreaterThanOrEqual(long expected) {
        assert expected >= 0;
        this.addAggregateCondition(Aggregate.DISTINCT_COUNT, Comparator.GREATER_THAN_OR_EQUAL, expected);
        return this;
    }

    public DbAssert distinctCountBetween(long expectedMin, long expectedMax) {
        assert expectedMin >= 0;
        assert expectedMin <= expectedMax;
        this.addAggregateCondition(Aggregate.DISTINCT_COUNT, Comparator.BETWEEN, Boundary.create(expectedMin, expectedMax));
        return this;
    }

    public DbAssert distinctCountBetween(long expectedMin, boolean excludeMin, long expectedMax, boolean excludeMax) {
        assert expectedMin >= 0;
        assert expectedMin <= expectedMax;
        this.addAggregateCondition(Aggregate.DISTINCT_COUNT, Comparator.BETWEEN, Boundary.create(expectedMin, expectedMax));
        return this;
    }

    private void setCurrentObject(ObjectType type, String name) {
        assert Utils.isNotBlank(name);
        if (type == ObjectType.DATABASE) {
            assertion.setDatabase(name);
        } else if (type == ObjectType.TABLE) {
            assertion.setTableName(name);
        }
        flushCurrentObject(type.newObjectInstance(name));
    }

    private void addAlias(String alias) {
        assert Utils.isNotBlank(alias);
        if (this.currentObject == null) {
            throw new IllegalStateException("Must call method database|table|col before call method as");
        }
        this.currentObject.setAlias(alias);
    }

    private void flushCurrentObject(DbObject dbObject) {
        if (currentObject != null) {
            if (currentObject.getAlias() != null) {
                this.assertion.addAlias(currentObject);
            }
            if (Utils.isNotEmpty(currentConditions)) {
                if (currentObject.getAlias() != null) {
                    currentConditions.forEach(condition -> condition.setAlias(currentObject.getAlias()));
                }
                this.assertion.addVerifies(currentConditions);
            }
        }
        this.currentObject = dbObject;
        this.currentConditions = dbObject == null ? null : new ArrayList<>();
    }

    private Condition addCondition(Comparator comparator, Object expected) {
        if (currentObject == null || !(currentObject instanceof Column)) {
            throw new IllegalStateException("Must call col method first");
        }
        Condition condition = Condition.create(currentObject.getName(), comparator, expected);
        this.currentConditions.add(condition);
        return condition;
    }

    private AggregateCondition addAggregateCondition(Aggregate aggregate, Comparator comparator, Object expected) {
        if (currentObject == null || !(currentObject instanceof Column)) {
            throw new IllegalStateException("Must call col method first");
        }
        AggregateCondition condition = AggregateCondition.create(aggregate, currentObject.getName(), comparator, expected);
        this.currentConditions.add(condition);
        return condition;
    }

    public void run() {
        flushCurrentObject(null);
        if (assertion.getTableName() == null) {
            throw new ConfigurationException("Table name can not be null");
        }
        if (Utils.isEmpty(assertion.getVerifies()) && Utils.isEmpty(assertion.getRowVerifies())) {
            throw new ConfigurationException("At least one verify condition required");
        }
        try {
            AssertionExecutor.create(this.assertion).run();
        } catch (SQLException e) {
            throw new RuntimeException("Assertion error: " + e.getMessage(), e);
        }
    }


}
