package com.github.cysong.dbassert;

import com.github.cysong.dbassert.assertion.AssertionExecutor;
import com.github.cysong.dbassert.option.DbAssertOptions;
import com.github.cysong.dbassert.option.DbAssertSetup;
import com.github.cysong.dbassert.utitls.SqlUtils;
import com.github.cysong.dbassert.utitls.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

/**
 * MySql testcases
 *
 * @author cysong
 * @date 2022/08/22 15:50
 */
@Test(dataProvider = DbAssertTest.DB_PROVIDER)
public class DbAssertTest {
    private static final Logger log = LoggerFactory.getLogger(AssertionExecutor.class);
    public static final String DB_PROVIDER = "dbProvider";
    private static final List<String> dbKeys = Arrays.asList("sqlite", "mysql");

    @DataProvider(name = DB_PROVIDER, parallel = true)
    public Iterator<Object[]> dbProvider() {
        return dbKeys.stream().map(dbKey -> new Object[]{dbKey})
                .collect(Collectors.toList())
                .iterator();
    }

    @BeforeClass()
    public void setup() {
        DbAssertSetup.setup().reporter(new AllureReporter());
        dbKeys.forEach(dbKey -> this.initDbByKey(dbKey));
    }

    private void initDbByKey(String dbKey) {
        Connection conn = DbAssertOptions.getGlobal().getFactory().getConnectionByDbKey(dbKey);
        TestUtils.initDb(conn);
    }

    @Test(dataProvider = DB_PROVIDER, timeOut = 1000)
    public void testNotRetry(String dbKey) {
        DbAssert.create(dbKey)
                .retry(false)
                .table(TestConstants.DEFAULT_TABLE_NAME)
                .where("id", 1)
                .col("name").as("person name").isEqual("alice")
                .countEquals(1)
                .run();
    }

    public void testRetry(String dbKey) {
        new Thread(() -> {
            Utils.sleep(4000);
            try {
                Connection conn = DbAssertOptions.getGlobal().getFactory().getConnectionByDbKey(dbKey);
                conn.prepareStatement("update person set name = 'cole' where id=3").executeUpdate();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }).start();
        long start = System.currentTimeMillis();
        DbAssert.create(dbKey)
                .retryInterval(1000)
                .retryTimes(6)
                .table(TestConstants.DEFAULT_TABLE_NAME)
                .where("id", 3)
                .col("name").as("person name").isEqual("cole")
                .run();
        long duration = System.currentTimeMillis() - start;
        Assert.assertTrue(duration > 4000 && duration < 6000);
    }

    public void testDelay(String dbKey) {
        long start = System.currentTimeMillis();
        DbAssert.create(dbKey)
                .delay(3000)
                .table(TestConstants.DEFAULT_TABLE_NAME)
                .where("id", 1)
                .col("name").isEqual("alice")
                .run();
        Assert.assertTrue(System.currentTimeMillis() - start > 3000);
    }

    public void testPaging(String dbKey) {
        DbAssert.create(dbKey)
                .startIndex(1)
                .table(TestConstants.DEFAULT_TABLE_NAME)
                .rowsEqual(2)
                .col("name")
                .countEquals(2)
                .listNotContain("alice")
                .run();
    }

    public void testFilter(String dbKey) {
        DbAssert.create(dbKey)
                .table(TestConstants.DEFAULT_TABLE_NAME)
                .where("name", "alice")
                .col("id")
                .isEqual(1)
                .run();
        DbAssert.create(dbKey)
                .table(TestConstants.DEFAULT_TABLE_NAME)
                .where("name", "alice")
                .and("id", 1)
                .rowsEqual(1)
                .run();
        DbAssert.create(dbKey)
                .table(TestConstants.DEFAULT_TABLE_NAME)
                .where("name='alice'")
                .col("id")
                .isEqual(1)
                .run();
    }

    public void testStringColumnAssertion(String dbKey) {
        DbAssert.create(dbKey)
                .table(TestConstants.DEFAULT_TABLE_NAME)
                .where("id", 1)
                .col("name")
                .as("person name")
                .isNotNull()
                .contains("a")
                .isEqual("alice")
                .isNotEqual("bob")
                .greaterThanOrEqual("alice")
                .lessThanOrEqual("alice")
                .contains("li")
                .notContain("bob")
                .run();
    }

    public void testIntegerColumnAssertion(String dbKey) {
        DbAssert.create(dbKey)
                .table(TestConstants.DEFAULT_TABLE_NAME)
                .where("id", 1)
                .col("age").isNotNull()
                .isEqual(10)
                .isEqual(10L)
                .isEqual(10.0)
                .isEqual("10")
                .isNotEqual("9")
                .greaterThan(9)
                .lessThan(11)
                .between(10, 11)
                .between(9, false, 11, false)
                .run();
    }

    public void testFloatColumnAssertion(String dbKey) {
        DbAssert.create(dbKey)
                .table(TestConstants.DEFAULT_TABLE_NAME)
                .where("id", 1)
                .col("weight").isNotNull()
                .isEqual(40)
                .isEqual(40L)
                .isEqual(40.0f)
                .isEqual(40.0d)
                .isEqual("40.0000")
                .isEqual("40.0d")
                .isNotEqual(4)
                .run();
    }

    public void testDoubleColumnAssertion(String dbKey) {
        DbAssert.create(dbKey)
                .table(TestConstants.DEFAULT_TABLE_NAME)
                .where("id", 1)
                .col("height")
                .isNotNull()
                .isEqual(94)
                .isEqual(94L)
                .isEqual(94.0)
                .isEqual(94.0d)
                .isEqual("94.0")
                .isEqual("94.0d")
                .run();
    }

    public void testBooleanColumnAssertion(String dbKey) {
        DbAssert.create(dbKey)
                .table(TestConstants.DEFAULT_TABLE_NAME)
                .where("id", 1)
                .col("adult")
                .isNotNull()
                .isFalse()
                .run();
        DbAssert.create(dbKey)
                .table(TestConstants.DEFAULT_TABLE_NAME)
                .where("id", 2)
                .col("adult")
                .isTrue()
                .run();
        DbAssert.create(dbKey)
                .table(TestConstants.DEFAULT_TABLE_NAME)
                .where("id", 3)
                .col("adult")
                .isNull()
                .run();
    }

    public void testInCondition(String dbKey) {
        DbAssert.create(dbKey)
                .table(TestConstants.DEFAULT_TABLE_NAME)
                .where("id", 1)
                .col("gender")
                .in(Arrays.asList("M", "F"))
                .notIn(Arrays.asList(1, 0))
                .run();
    }

    public void testMatches(String dbKey) {
        DbAssert.create(dbKey)
                .table(TestConstants.DEFAULT_TABLE_NAME)
                .where("id", 1)
                .col("name")
                .matches(name -> ((String) name).equals("alice"))
                .run();
        DbAssert.create(dbKey)
                .table(TestConstants.DEFAULT_TABLE_NAME)
                .where("id", 1)
                .col("name")
                .notMatch(name -> ((String) name).equals("bob"))
                .run();
    }

    public void testListAssertion(String dbKey) {
        DbAssert.create(dbKey)
                .table(TestConstants.DEFAULT_TABLE_NAME)
                .where("gender", "M")
                .orderBy("id")
                .col("name")
                .listNotEmpty()
                .listHasSize(2)
                .listEquals(Arrays.asList("bob", "carl"))
                .listEqualAtAnyOrder(Arrays.asList("carl", "bob"))
                .listNotEqual(Arrays.asList("bob", "carl", null))
                .listNotEqual(Arrays.asList("carl", "bob"))
                .listNotEqual(Arrays.asList("bob"))
                .listNotEqual(Arrays.asList("alice"))
                .listContains(Arrays.asList("bob"))
                .listNotContain(Arrays.asList("alice"))
                .listContains("bob")
                .listContainsAny(Arrays.asList("alice", "bob"))
                .listContainsAny("carl")
                .listNotContain("alice")
                .listNotContain(1)
                .col("id")
                .listIsOrderedAsc()
                .listMatches(list -> list.size() == 2)
                .listNotMatch(list -> list.size() == 1)
                .col("age")
                .listEquals(Arrays.asList(20, null))
                .listEqualAtAnyOrder(Arrays.asList(null, 20))
                .listNotEqual(Arrays.asList(20, null, null))
                .listNotEqual(Arrays.asList(null, 20))
                .listNotEqual(Arrays.asList(20))
                .listContains(Arrays.asList(20))
                .listContains(new ArrayList())
                .listContains(new ArrayList<Object>() {
                    {
                        add(null);
                    }
                })
                .listContains(20)
                .listContains(null)
                .listContainsAny(Arrays.asList(1, null))
                .listContainsAny(20)
                .listContainsAny(null)
                .listNotContain(Arrays.asList(1))
                .listNotContain(1)
                .listNotContain("20")
                .run();
    }

    public void testRowsAssertion(String dbKey) throws SQLException {
        Connection conn = DbAssertOptions.getGlobal().getFactory().getConnectionByDbKey(dbKey);
        long totalRows = TestUtils.getTotalRowsOfTable(conn, TestConstants.DEFAULT_TABLE_NAME);
        DbAssert.create(dbKey)
                .table(TestConstants.DEFAULT_TABLE_NAME)
                .rowsEqual(totalRows)
                .rowsGreaterThan(totalRows - 1)
                .rowsGreaterThanOrEqual(totalRows)
                .rowsLessThan(totalRows + 1)
                .rowsLessThanOrEqual(totalRows)
                .rowsBetween(totalRows, totalRows)
                .rowsBetween(totalRows - 1, totalRows + 1)
                .rowsBetween(totalRows, false, totalRows + 1, true)
                .rowsBetween(totalRows - 1, true, totalRows, false)
                .run();
    }

    public void testCountAssertion(String dbKey) {
        DbAssert.create(dbKey)
                .table(TestConstants.DEFAULT_TABLE_NAME)
                .where("gender", "M")
                .col("gender")
                .countEquals(2)
                .run();
    }

    public void testDistinctCountAssertion(String dbKey) {
        DbAssert.create(dbKey)
                .table(TestConstants.DEFAULT_TABLE_NAME)
                .col("gender")
                .distinctCountEqual(2)
                .distinctCountLessThan(3)
                .distinctCountLessThanOrEqual(2)
                .distinctCountGreaterThan(1)
                .distinctCountGreaterThanOrEqual(2)
                .distinctCountBetween(1, 2)
                .distinctCountBetween(1, true, 2, false)
                .run();
    }

    public void testSuccessIfNotFound(String dbKey) {
        DbAssert.create(dbKey)
                .table(TestConstants.DEFAULT_TABLE_NAME)
                .retry(false)
                .failIfNotFound(false)
                .where("id", 9999)
                .col("name").isEqual("unknown")
                .run();
    }

    @Test(dataProvider = DB_PROVIDER, expectedExceptions = AssertionError.class)
    public void testFailIfNotFound(String dbKey) {
        DbAssert.create(dbKey)
                .table(TestConstants.DEFAULT_TABLE_NAME)
                .retry(false)
                .failIfNotFound(true)
                .where("id", 9999)
                .col("name").isEqual("unknown")
                .run();
    }

    @Test
    public void testAggregateAssertion(String dbKey) {
        DbAssert.create(dbKey)
                .table(TestConstants.DEFAULT_TABLE_NAME)
                .where("name", "bob")
                .col("name")
                .countEquals(1)
                .countLessThan(2)
                .countLessThanOrEqual(1)
                .countLessThanOrEqual(2)
                .countGreaterThan(0)
                .countGreaterThanOrEqual(0)
                .countGreaterThanOrEqual(0)
                .countBetween(0, 2)
                .countBetween(0, true, 1, false)
                .run();
    }

    @AfterClass
    public static void tearDown() {
        Throwable throwable = null;
        for (String dbKey : dbKeys) {
            try {
                Connection conn = DbAssertOptions.getGlobal().getFactory().getConnectionByDbKey(dbKey);
                if (SqlUtils.isSqlite(conn)) {
                    SqlUtils.deleteDbFileIfSqlite(conn);
                } else {
                    SqlUtils.deleteTable(conn, TestConstants.DEFAULT_TABLE_NAME);
                }
            } catch (Throwable t) {
                log.error(t.getMessage(), t);
                if (throwable == null) {
                    throwable = t;
                }
            }
        }
        if (throwable != null) {
            Assert.fail(throwable.getMessage(), throwable);
        }
    }

}
