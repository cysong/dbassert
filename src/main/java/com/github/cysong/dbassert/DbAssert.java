package com.github.cysong.dbassert;


import com.github.cysong.dbassert.assertion.Assertion;
import com.github.cysong.dbassert.assertion.AssertionExecutor;
import com.github.cysong.dbassert.constant.*;
import com.github.cysong.dbassert.exception.ConfigurationException;
import com.github.cysong.dbassert.expression.*;
import com.github.cysong.dbassert.object.Column;
import com.github.cysong.dbassert.object.DbObject;
import com.github.cysong.dbassert.option.DbAssertOptions;
import com.github.cysong.dbassert.utitls.Utils;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;

/**
 * DbAssert main entry
 *
 * @author cysong
 * @date 2022/08/22 15:50
 **/
public class DbAssert {
    private final Assertion assertion;
    private DbObject currentObject;
    private List<Condition> currentConditions;

    /**
     * create DbAssert instance by dbKey(get connect by dbKey from ConnectFactory)
     *
     * @param dbKey database key
     * @return com.github.cysong.dbassert.DbAssert
     * @author cysong
     * @date 2022/8/22 16:17
     **/
    public static DbAssert create(String dbKey) {
        assert Utils.isNotBlank(dbKey);
        Connection conn = DbAssertOptions.getGlobal().getFactory().getConnectionByDbKey(dbKey);
        return DbAssert.create(conn);
    }

    /**
     * create DbAssert instance by conn(no ConnectFactory needed)
     *
     * @param conn database connection
     * @return com.github.cysong.dbassert.DbAssert
     * @author cysong
     * @date 2022/8/22 16:20
     **/
    public static DbAssert create(Connection conn) {
        return new DbAssert(conn);
    }

    private DbAssert(Connection conn) {
        this.assertion = new Assertion(conn);
    }

    /**
     * retry if assert fail
     *
     * @param retry whether should retry
     * @return com.github.cysong.dbassert.DbAssert
     * @author cysong
     * @date 2022/8/22 16:21
     **/
    public DbAssert retry(boolean retry) {
        this.assertion.setRetry(retry);
        return this;
    }

    /**
     * Retry times before assert success
     *
     * @param retryTimes retry times
     * @return com.github.cysong.dbassert.DbAssert
     * @author cysong
     * @date 2022/8/22 16:22
     **/
    public DbAssert retryTimes(int retryTimes) {
        this.assertion.setRetryTimes(retryTimes);
        return this;
    }

    /**
     * wait milliseconds before every retry
     *
     * @param retryInterval milliseconds to be wait before retry
     * @return com.github.cysong.dbassert.DbAssert
     * @author cysong
     * @date 2022/8/22 16:22
     **/
    public DbAssert retryInterval(long retryInterval) {
        this.assertion.setRetryInterval(retryInterval);
        return this;
    }

    /**
     * wait milliseconds before first database assert
     * (in case sometimes before database assertion system need time process data)
     *
     * @param delay milliseconds to be wait before first assertion
     * @return com.github.cysong.dbassert.DbAssert
     * @author cysong
     * @date 2022/8/22 16:23
     **/
    public DbAssert delay(long delay) {
        this.assertion.setDelay(delay);
        return this;
    }

    /**
     * Whether database assertion should fail if no rows return
     *
     * @param failIfNotFound whether should fail if no rows return
     * @return com.github.cysong.dbassert.DbAssert
     * @author cysong
     * @date 2022/8/22 16:27
     **/
    public DbAssert failIfNotFound(boolean failIfNotFound) {
        this.assertion.setFailIfNotFound(failIfNotFound);
        return this;
    }

    /**
     * Set the start index of query by page(default 1)
     *
     * @param startIndex the start index of data returned
     * @return com.github.cysong.dbassert.DbAssert
     * @author cysong
     * @date 2022/8/22 16:29
     **/
    public DbAssert startIndex(int startIndex) {
        this.assertion.setStartIndex(startIndex);
        return this;
    }

    /**
     * Set page size if query by page(for performance reason default value is set to 100)
     *
     * @param pageSize page size of data returned
     * @return com.github.cysong.dbassert.DbAssert
     * @author cysong
     * @date 2022/8/22 16:30
     **/
    public DbAssert pageSize(int pageSize) {
        this.assertion.setPageSize(pageSize);
        return this;
    }

    /**
     * Set name of database
     *
     * @param dbName database name to be queried
     * @return com.github.cysong.dbassert.DbAssert
     * @author cysong
     * @date 2022/8/22 16:33
     **/
    public DbAssert database(String dbName) {
        this.setCurrentObject(ObjectType.DATABASE, dbName);
        return this;
    }

    /**
     * Set name of table
     *
     * @param tableName table name to be queried
     * @return com.github.cysong.dbassert.DbAssert
     * @author cysong
     * @date 2022/8/22 16:33
     **/
    public DbAssert table(String tableName) {
        this.setCurrentObject(ObjectType.TABLE, tableName);
        return this;
    }

    /**
     * Add assertion of column
     *
     * @param columnName column name to be asserted
     * @return com.github.cysong.dbassert.DbAssert
     * @author cysong
     * @date 2022/8/22 16:34
     **/
    public DbAssert col(String columnName) {
        this.setCurrentObject(ObjectType.COLUMN, columnName);
        return this;
    }

    /**
     * Set alias for database object,such as database,table or column
     *
     * @param alias alias for database object
     * @return com.github.cysong.dbassert.DbAssert
     * @author cysong
     * @date 2022/8/22 16:34
     **/
    public DbAssert as(String alias) {
        this.addAlias(alias);
        return this;
    }

    /**
     * set where condition by column
     *
     * @param columnName column name to by filtered
     * @param value      column filter value
     * @return com.github.cysong.dbassert.DbAssert
     * @author cysong
     * @date 2022/8/22 16:35
     **/
    public DbAssert where(String columnName, Object value) {
        this.assertion.addFilter(Filter.create(columnName, value));
        return this;
    }

    /**
     * set multiple where condition by column
     *
     * @param columnName column name to by filtered
     * @param value      column filter value
     * @return com.github.cysong.dbassert.DbAssert
     * @author cysong
     * @date 2022/8/22 16:36
     **/
    public DbAssert and(String columnName, Object value) {
        this.assertion.addFilter(Filter.create(columnName, value));
        return this;
    }

    /**
     * Set text format where condition,effective if condition is complex
     *
     * @param expression where statement in text format
     * @return com.github.cysong.dbassert.DbAssert
     * @author cysong
     * @date 2022/8/22 16:36
     **/
    public DbAssert where(String expression) {
        this.assertion.addFilter(TextFilter.create(expression));
        return this;
    }

    /**
     * Set sort columns, default order is asc
     *
     * @param columnNames column names to be sorted(by asc)
     * @return com.github.cysong.dbassert.DbAssert
     * @author cysong
     * @date 2022/8/22 16:39
     **/
    public DbAssert orderBy(String... columnNames) {
        for (String columnName : columnNames) {
            this.assertion.addSort(Sort.create(columnName));
        }
        return this;
    }

    /**
     * Set sort columns, default order is desc
     *
     * @param columnNames column names to be sorted(by desc)
     * @return com.github.cysong.dbassert.DbAssert
     * @author cysong
     * @date 2022/8/22 16:40
     **/
    public DbAssert orderByDesc(String... columnNames) {
        for (String columnName : columnNames) {
            this.assertion.addSort(Sort.create(columnName, Order.DESC));
        }
        return this;
    }

    /**
     * Each column(specified by col()) values is expected equal param expected
     *
     * @param expected expected value
     * @return com.github.cysong.dbassert.DbAssert
     * @author cysong
     * @date 2022/8/22 16:40
     **/
    public DbAssert isEqual(Object expected) {
        this.addCondition(Comparator.EQUAL, expected);
        return this;
    }

    /**
     * Each column(specified by col()) values is expected not equal param expected
     *
     * @param expected expected value
     * @return com.github.cysong.dbassert.DbAssert
     * @author cysong
     * @date 2022/8/22 16:43
     **/
    public DbAssert isNotEqual(Object expected) {
        this.addCondition(Comparator.NOT_EQUAL, expected);
        return this;
    }

    /**
     * Each column(specified by col()) values is expected null
     *
     * @return com.github.cysong.dbassert.DbAssert
     * @author cysong
     * @date 2022/8/22 16:43
     **/
    public DbAssert isNull() {
        this.addCondition(Comparator.NULL, null);
        return this;
    }

    /**
     * Each column(specified by col()) values is expected not null
     *
     * @return com.github.cysong.dbassert.DbAssert
     * @author cysong
     * @date 2022/8/22 16:43
     **/
    public DbAssert isNotNull() {
        this.addCondition(Comparator.NOT_NULL, null);
        return this;
    }

    /**
     * Each column(specified by col()) values is expected true
     *
     * @return com.github.cysong.dbassert.DbAssert
     * @author cysong
     * @date 2022/8/22 16:44
     **/
    public DbAssert isTrue() {
        this.addCondition(Comparator.IS_TRUE, null);
        return this;
    }

    /**
     * Each column(specified by col()) values is expected false
     *
     * @return com.github.cysong.dbassert.DbAssert
     * @author cysong
     * @date 2022/8/22 16:44
     **/
    public DbAssert isFalse() {
        this.addCondition(Comparator.IS_FALSE, null);
        return this;
    }

    /**
     * Each column(specified by col()) values is expected greater than param expected
     *
     * @param expected expected value
     * @return com.github.cysong.dbassert.DbAssert
     * @author cysong
     * @date 2022/8/22 16:44
     **/
    public DbAssert greaterThan(Object expected) {
        this.addCondition(Comparator.GREATER_THAN, expected);
        return this;
    }

    /**
     * Each column(specified by col()) values is expected greater than or equal param expected
     *
     * @param expected expected value
     * @return com.github.cysong.dbassert.DbAssert
     * @author cysong
     * @date 2022/8/22 16:45
     **/
    public DbAssert greaterThanOrEqual(Object expected) {
        this.addCondition(Comparator.GREATER_THAN_OR_EQUAL, expected);
        return this;
    }

    /**
     * Each column(specified by col()) values is expected less than param expected
     *
     * @param expected expected value
     * @return com.github.cysong.dbassert.DbAssert
     * @author cysong
     * @date 2022/8/22 16:45
     **/
    public DbAssert lessThan(Object expected) {
        this.addCondition(Comparator.LESS_THAN, expected);
        return this;
    }

    /**
     * Each column(specified by col()) values is expected greater than or equal param expected
     *
     * @param expected expected value
     * @return com.github.cysong.dbassert.DbAssert
     * @author cysong
     * @date 2022/8/22 16:45
     **/
    public DbAssert lessThanOrEqual(Object expected) {
        this.addCondition(Comparator.LESS_THAN_OR_EQUAL, expected);
        return this;
    }

    /**
     * Each column(specified by col()) values is expected between expectedMin(included) and expectedMax(included)
     *
     * @param expectedMin expected minimum value
     * @param expectedMax expected maximum value
     * @return com.github.cysong.dbassert.DbAssert
     * @author cysong
     * @date 2022/8/22 16:46
     **/
    public DbAssert between(Object expectedMin, Object expectedMax) {
        assert expectedMin != null;
        assert expectedMax != null;
        this.addCondition(Comparator.BETWEEN, Boundary.create(expectedMin, expectedMax));
        return this;
    }

    /**
     * Each column(specified by col()) values is expected between expectedMin(whether included determined by excludeMin)
     * and expectedMax(whether included determined by excludeMax)
     *
     * @param expectedMin expected minimum value
     * @param excludeMin  whether included the minimum value
     * @param expectedMax expected maximum value
     * @param excludeMax  whether included the maximum value
     * @return com.github.cysong.dbassert.DbAssert
     * @author cysong
     * @date 2022/8/22 16:47
     **/
    public DbAssert between(Object expectedMin, boolean excludeMin, Object expectedMax, boolean excludeMax) {
        assert expectedMin != null;
        assert expectedMax != null;
        this.addCondition(Comparator.BETWEEN, Boundary.create(expectedMin, excludeMin, expectedMax, excludeMax));
        return this;
    }

    /**
     * Each column(specified by col()) values is expected in param expected
     *
     * @param expected expected value
     * @return com.github.cysong.dbassert.DbAssert
     * @author cysong
     * @date 2022/8/22 16:48
     **/
    public DbAssert in(Iterable<?> expected) {
        assert expected != null;
        this.addCondition(Comparator.IN, expected);
        return this;
    }

    /**
     * Each column(specified by col()) values is expected not in param expected
     *
     * @param expected expected value
     * @return com.github.cysong.dbassert.DbAssert
     * @author cysong
     * @date 2022/8/22 16:49
     **/
    public DbAssert notIn(Iterable<?> expected) {
        this.addCondition(Comparator.NOT_IN, expected);
        return this;
    }

    /**
     * Each column(specified by col()) values is expected contains String specified by param expected
     *
     * @param expected expected value
     * @return com.github.cysong.dbassert.DbAssert
     * @author cysong
     * @date 2022/8/22 16:49
     **/
    public DbAssert contains(String expected) {
        this.addCondition(Comparator.CONTAINS, expected);
        return this;
    }

    /**
     * Each column(specified by col()) values is expected not contain String specified by param expected
     *
     * @param expected expected value
     * @return com.github.cysong.dbassert.DbAssert
     * @author cysong
     * @date 2022/8/22 16:51
     **/
    public DbAssert notContain(String expected) {
        this.addCondition(Comparator.NOT_CONTAIN, expected);
        return this;
    }

    /**
     * Each column(specified by col()) values is expected matches predicate
     *
     * @param predicate function to be executed on actual value
     * @return com.github.cysong.dbassert.DbAssert
     * @author cysong
     * @date 2022/8/22 16:51
     **/
    public DbAssert matches(Predicate<?> predicate) {
        assert predicate != null;
        this.addCondition(Comparator.MATCHES, predicate);
        return this;
    }

    /**
     * Each column(specified by col()) values is expected not match predicate
     *
     * @param predicate function to be executed on actual value
     * @return com.github.cysong.dbassert.DbAssert
     * @author cysong
     * @date 2022/8/22 16:52
     **/
    public DbAssert notMatch(Predicate<?> predicate) {
        assert predicate != null;
        this.addCondition(Comparator.NOT_MATCH, predicate);
        return this;
    }

    /**
     * List of column(specified by col()) values is expected empty
     *
     * @return com.github.cysong.dbassert.DbAssert
     * @author cysong
     * @date 2022/8/23 16:02
     **/
    public DbAssert listIsEmpty() {
        this.addListCondition(Comparator.LIST_IS_EMPTY, null);
        return this;
    }

    /**
     * List of column(specified by col()) values is expected not empty
     *
     * @return com.github.cysong.dbassert.DbAssert
     * @author cysong
     * @date 2022/8/23 16:03
     **/
    public DbAssert listNotEmpty() {
        this.addListCondition(Comparator.LIST_NOT_EMPTY, null);
        return this;
    }

    /**
     * List of column(specified by col()) values is expected has the size of expected
     *
     * @param expected expected value
     * @return com.github.cysong.dbassert.DbAssert
     * @author cysong
     * @date 2022/8/23 16:03
     **/
    public DbAssert listHasSize(int expected) {
        this.addListCondition(Comparator.LIST_HAS_SIZE, expected);
        return this;
    }

    /**
     * All column(specified by col()) values is expected equal to expected(at the order of returned)
     *
     * @param expected expected value
     * @return com.github.cysong.dbassert.DbAssert
     * @author cysong
     * @date 2022/8/23 16:04
     **/
    public DbAssert listEquals(Iterable<?> expected) {
        this.addListCondition(Comparator.LIST_EQUALS, expected);
        return this;
    }

    /**
     * All column(specified by col()) values is expected not equal to expected(at the order of returned)
     *
     * @param expected expected value
     * @return com.github.cysong.dbassert.DbAssert
     * @author cysong
     * @date 2022/8/23 16:04
     **/
    public DbAssert listNotEqual(Iterable<?> expected) {
        this.addListCondition(Comparator.LIST_NOT_EQUAL, expected);
        return this;
    }

    /**
     * All column(specified by col()) values is expected equal to expected(at any order)
     *
     * @param expected expected value
     * @return com.github.cysong.dbassert.DbAssert
     * @author cysong
     * @date 2022/8/23 16:05
     **/
    public DbAssert listEqualAtAnyOrder(Iterable<?> expected) {
        this.addListCondition(Comparator.LIST_EQUALS_AT_ANY_ORDER, expected);
        return this;
    }

    /**
     * All column(specified by col()) values is expected contains all elements of expected
     *
     * @param expected expected value
     * @return com.github.cysong.dbassert.DbAssert
     * @author cysong
     * @date 2022/8/23 16:06
     **/
    public DbAssert listContains(Object expected) {
        this.addListCondition(Comparator.LIST_CONTAINS, expected);
        return this;
    }

    /**
     * All column(specified by col()) values is expected not contains any element of expected(at any order)
     *
     * @param expected expected value
     * @return com.github.cysong.dbassert.DbAssert
     * @author cysong
     * @date 2022/8/23 16:07
     **/
    public DbAssert listNotContain(Object expected) {
        this.addListCondition(Comparator.LIST_NOT_CONTAIN, expected);
        return this;
    }

    /**
     * All column(specified by col()) values is expected contains any elements of expected
     *
     * @param expected expected value
     * @return com.github.cysong.dbassert.DbAssert
     * @author cysong
     * @date 2022/8/23 16:08
     **/
    public DbAssert listContainsAny(Object expected) {
        this.addListCondition(Comparator.LIST_CONTAINS_ANY, expected);
        return this;
    }

    /**
     * All column(specified by col()) values is expected ordered ascending
     *
     * @return com.github.cysong.dbassert.DbAssert
     * @author cysong
     * @date 2022/8/23 16:11
     **/
    public DbAssert listIsOrderedAsc() {
        this.addListCondition(Comparator.LIST_IS_ORDERED_ASC, null);
        return this;
    }

    /**
     * All column(specified by col()) values is expected ordered descending
     *
     * @return com.github.cysong.dbassert.DbAssert
     * @author cysong
     * @date 2022/8/23 16:11
     **/
    public DbAssert listIsOrderedDesc() {
        this.addListCondition(Comparator.LIST_IS_ORDERED_DESC, null);
        return this;
    }

    /**
     * List of column(specified by col()) values is expected matches predicate
     *
     * @param predicate predicate to test
     * @return com.github.cysong.dbassert.DbAssert
     * @author cysong
     * @date 2022/8/23 16:12
     **/
    public DbAssert listMatches(Predicate<List> predicate) {
        this.addListCondition(Comparator.LIST_MATCHES, predicate);
        return this;
    }

    /**
     * List of column(specified by col()) values is expected not match predicate
     *
     * @param predicate predicate to test
     * @return com.github.cysong.dbassert.DbAssert
     * @author cysong
     * @date 2022/8/23 16:13
     **/
    public DbAssert listNotMatch(Predicate<List> predicate) {
        this.addListCondition(Comparator.LIST_NOT_MATCH, predicate);
        return this;
    }

    /**
     * Total rows return by query is expected equal to param expected
     *
     * @param expected expected value
     * @return com.github.cysong.dbassert.DbAssert
     * @author cysong
     * @date 2022/8/22 16:52
     **/
    public DbAssert rowsEqual(long expected) {
        assert expected >= 0;
        this.assertion.addRowVerify(Condition.create(Constants.COUNT_ROWS_COLUMN, Comparator.EQUAL, expected));
        return this;
    }

    /**
     * Total rows return by query is expected less than param expected
     *
     * @param expected expected value
     * @return com.github.cysong.dbassert.DbAssert
     * @author cysong
     * @date 2022/8/22 16:53
     **/
    public DbAssert rowsLessThan(long expected) {
        assert expected >= 0;
        this.assertion.addRowVerify(Condition.create(Constants.COUNT_ROWS_COLUMN, Comparator.LESS_THAN, expected));
        return this;
    }

    /**
     * Total rows return by query is expected less than or equal to param expected
     *
     * @param expected expected value
     * @return com.github.cysong.dbassert.DbAssert
     * @author cysong
     * @date 2022/8/22 16:54
     **/
    public DbAssert rowsLessThanOrEqual(long expected) {
        assert expected >= 0;
        this.assertion.addRowVerify(Condition.create(Constants.COUNT_ROWS_COLUMN, Comparator.LESS_THAN_OR_EQUAL, expected));
        return this;
    }

    /**
     * Total rows return by query is expected greater than param expected
     *
     * @param expected expected value
     * @return com.github.cysong.dbassert.DbAssert
     * @author cysong
     * @date 2022/8/22 16:54
     **/
    public DbAssert rowsGreaterThan(long expected) {
        assert expected >= 0;
        this.assertion.addRowVerify(Condition.create(Constants.COUNT_ROWS_COLUMN, Comparator.GREATER_THAN, expected));
        return this;
    }

    /**
     * Total rows return by query is expected greater than or equal to param expected
     *
     * @param expected expected value
     * @return com.github.cysong.dbassert.DbAssert
     * @author cysong
     * @date 2022/8/22 16:54
     **/
    public DbAssert rowsGreaterThanOrEqual(long expected) {
        assert expected >= 0;
        this.assertion.addRowVerify(Condition.create(Constants.COUNT_ROWS_COLUMN, Comparator.GREATER_THAN_OR_EQUAL, expected));
        return this;
    }

    /**
     * Total rows return by query is expected between expectedMin(included) and expectedMax(included)
     *
     * @param expectedMin expected minimum value
     * @param expectedMax expected maximum value
     * @return com.github.cysong.dbassert.DbAssert
     * @author cysong
     * @date 2022/8/22 16:55
     **/
    public DbAssert rowsBetween(long expectedMin, long expectedMax) {
        assert expectedMin >= 0;
        assert expectedMin <= expectedMax;
        this.assertion.addRowVerify(Condition.create(Constants.COUNT_ROWS_COLUMN, Comparator.BETWEEN, Boundary.create(expectedMin, expectedMax)));
        return this;
    }

    /**
     * Total rows return by query is expected between expectedMin(whether included determined by excludeMin)
     * and expectedMax(whether included determined by excludeMax))
     *
     * @param expectedMin expected minimum value
     * @param excludeMin  whether excluded minimum value
     * @param expectedMax expected maximum value
     * @param excludeMax  whether exclude maximum value
     * @return com.github.cysong.dbassert.DbAssert
     * @author cysong
     * @date 2022/8/22 16:55
     **/
    public DbAssert rowsBetween(long expectedMin, boolean excludeMin, long expectedMax, boolean excludeMax) {
        assert expectedMin >= 0;
        assert expectedMin <= expectedMax;
        this.assertion.addRowVerify(Condition.create(Constants.COUNT_ROWS_COLUMN, Comparator.BETWEEN, Boundary.create(expectedMin, excludeMin, expectedMax, excludeMax)));
        return this;
    }

    /**
     * Count value of column(specified by col()) is expected equal to param expected
     *
     * @param expected expected value
     * @return com.github.cysong.dbassert.DbAssert
     * @author cysong
     * @date 2022/8/22 16:57
     **/
    public DbAssert countEquals(long expected) {
        assert expected >= 0;
        this.addAggregateCondition(Aggregate.COUNT, Comparator.EQUAL, expected);
        return this;
    }

    /**
     * Count value of column(specified by col()) is expected less than param expected
     *
     * @param expected expected value
     * @return com.github.cysong.dbassert.DbAssert
     * @author cysong
     * @date 2022/8/22 16:58
     **/
    public DbAssert countLessThan(long expected) {
        assert expected >= 0;
        this.addAggregateCondition(Aggregate.COUNT, Comparator.LESS_THAN, expected);
        return this;
    }

    /**
     * Count value of column(specified by col()) is expected less than or equal to param expected
     *
     * @param expected expected value
     * @return com.github.cysong.dbassert.DbAssert
     * @author cysong
     * @date 2022/8/22 16:59
     **/
    public DbAssert countLessThanOrEqual(long expected) {
        assert expected >= 0;
        this.addAggregateCondition(Aggregate.COUNT, Comparator.LESS_THAN_OR_EQUAL, expected);
        return this;
    }

    /**
     * Count value of column(specified by col()) is expected greater than param expected
     *
     * @param expected expected value
     * @return com.github.cysong.dbassert.DbAssert
     * @author cysong
     * @date 2022/8/22 16:59
     **/
    public DbAssert countGreaterThan(long expected) {
        assert expected >= 0;
        this.addAggregateCondition(Aggregate.COUNT, Comparator.GREATER_THAN, expected);
        return this;
    }

    /**
     * Count value of column(specified by col()) is expected greater than or equal to param expected
     *
     * @param expected expected value
     * @return com.github.cysong.dbassert.DbAssert
     * @author cysong
     * @date 2022/8/22 16:59
     **/
    public DbAssert countGreaterThanOrEqual(long expected) {
        assert expected >= 0;
        this.addAggregateCondition(Aggregate.COUNT, Comparator.GREATER_THAN_OR_EQUAL, expected);
        return this;
    }

    /**
     * Count value of column(specified by col()) is expected between expectedMin(included) and expectedMax(included)
     *
     * @param expectedMin expected minimum value
     * @param expectedMax expected maximum value
     * @return com.github.cysong.dbassert.DbAssert
     * @author cysong
     * @date 2022/8/22 17:00
     **/
    public DbAssert countBetween(long expectedMin, long expectedMax) {
        assert expectedMin >= 0;
        assert expectedMin <= expectedMax;
        this.addAggregateCondition(Aggregate.COUNT, Comparator.BETWEEN, Boundary.create(expectedMin, expectedMax));
        return this;
    }

    /**
     * Count value of column(specified by col()) is expected between expectedMin(whether included determined by excludeMin)
     * and expectedMax(whether included determined by excludeMax)
     *
     * @param expectedMin expected minimum value
     * @param excludeMin  whether excluded minimum value
     * @param expectedMax expected maximum value
     * @param excludeMax  whether excluded maximum value
     * @return com.github.cysong.dbassert.DbAssert
     * @author cysong
     * @date 2022/8/22 17:01
     **/
    public DbAssert countBetween(long expectedMin, boolean excludeMin, long expectedMax, boolean excludeMax) {
        assert expectedMin >= 0;
        assert expectedMin <= expectedMax;
        this.addAggregateCondition(Aggregate.COUNT, Comparator.BETWEEN, Boundary.create(expectedMin, excludeMin, expectedMax, excludeMax));
        return this;
    }

    /**
     * Distinct count value of column(specified by col()) is expected equal to param expected
     *
     * @param expected expected value
     * @return com.github.cysong.dbassert.DbAssert
     * @author cysong
     * @date 2022/8/22 17:02
     **/
    public DbAssert distinctCountEqual(long expected) {
        assert expected >= 0;
        this.addAggregateCondition(Aggregate.DISTINCT_COUNT, Comparator.EQUAL, expected);
        return this;
    }

    /**
     * Distinct count value of column(specified by col()) is expected less than param expected
     *
     * @param expected expected value
     * @return com.github.cysong.dbassert.DbAssert
     * @author cysong
     * @date 2022/8/22 17:02
     **/
    public DbAssert distinctCountLessThan(long expected) {
        assert expected >= 0;
        this.addAggregateCondition(Aggregate.DISTINCT_COUNT, Comparator.LESS_THAN, expected);
        return this;
    }

    /**
     * Distinct count value of column(specified by col()) is expected less than or equal to param expected
     *
     * @param expected expected value
     * @return com.github.cysong.dbassert.DbAssert
     * @author cysong
     * @date 2022/8/22 17:03
     **/
    public DbAssert distinctCountLessThanOrEqual(long expected) {
        assert expected >= 0;
        this.addAggregateCondition(Aggregate.DISTINCT_COUNT, Comparator.LESS_THAN_OR_EQUAL, expected);
        return this;
    }

    /**
     * Distinct count value of column(specified by col()) is expected greater than param expected
     *
     * @param expected expected value
     * @return com.github.cysong.dbassert.DbAssert
     * @author cysong
     * @date 2022/8/22 17:03
     **/
    public DbAssert distinctCountGreaterThan(long expected) {
        assert expected >= 0;
        this.addAggregateCondition(Aggregate.DISTINCT_COUNT, Comparator.GREATER_THAN, expected);
        return this;
    }

    /**
     * Distinct count value of column(specified by col()) is expected greater than or equal to param expected
     *
     * @param expected expected value
     * @return com.github.cysong.dbassert.DbAssert
     * @author cysong
     * @date 2022/8/22 17:03
     **/
    public DbAssert distinctCountGreaterThanOrEqual(long expected) {
        assert expected >= 0;
        this.addAggregateCondition(Aggregate.DISTINCT_COUNT, Comparator.GREATER_THAN_OR_EQUAL, expected);
        return this;
    }

    /**
     * Distinct count value of column(specified by col()) is expected between expectedMin(included) and expectedMax(included)
     *
     * @param expectedMin expected minimum value
     * @param expectedMax expected maximum value
     * @return com.github.cysong.dbassert.DbAssert
     * @author cysong
     * @date 2022/8/22 17:03
     **/
    public DbAssert distinctCountBetween(long expectedMin, long expectedMax) {
        assert expectedMin >= 0;
        assert expectedMin <= expectedMax;
        this.addAggregateCondition(Aggregate.DISTINCT_COUNT, Comparator.BETWEEN, Boundary.create(expectedMin, expectedMax));
        return this;
    }

    /**
     * Distinct count value of column(specified by col()) is expected between expectedMin(whether included determined by excludeMin)
     * and expectedMax(whether included determined by excludeMax)
     *
     * @param expectedMin expected minimum value
     * @param excludeMin  whether exclude minimum value
     * @param expectedMax expected maximum value
     * @param excludeMax  whether exclude maximum value
     * @return com.github.cysong.dbassert.DbAssert
     * @author cysong
     * @date 2022/8/22 17:04
     **/
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

    private Condition addListCondition(Comparator comparator, Object expected) {
        if (currentObject == null || !(currentObject instanceof Column)) {
            throw new IllegalStateException("Must call col method first");
        }
        ListCondition condition = ListCondition.create(currentObject.getName(), comparator, expected);
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

    /**
     * Execute the Assertions
     *
     * @author cysong
     * @date 2022/8/22 17:05
     **/
    public void run() {
        flushCurrentObject(null);
        if (assertion.getTableName() == null) {
            throw new ConfigurationException("Table name can not be null");
        }
        if (Utils.isEmpty(assertion.getVerifies()) && Utils.isEmpty(assertion.getRowVerifies())) {
            throw new ConfigurationException("At least one verify condition required");
        }

        AssertionExecutor.create(this.assertion).run();
    }

}
