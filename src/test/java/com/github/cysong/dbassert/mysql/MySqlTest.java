package com.github.cysong.dbassert.mysql;

import com.github.cysong.dbassert.DbAssert;
import com.github.cysong.dbassert.TestConstants;
import com.github.cysong.dbassert.TestUtils;
import com.github.cysong.dbassert.option.DbAssertOptions;
import com.github.cysong.dbassert.utitls.Utils;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;

/**
 * MySql testcases
 *
 * @author cysong
 * @date 2022/08/22 15:50
 */
public class MySqlTest {
    private static final String dbKey = "mysql";

    @BeforeClass
    public static void setup() throws SQLException, IOException {
        Connection conn = DbAssertOptions.getGlobal().getFactory().getConnectionByDbKey(dbKey);
        TestUtils.initDb(conn);
        try {
            conn.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Test(timeOut = 1000)
    public void testNotRetry() {
        DbAssert.create(dbKey)
                .retry(false)
                .table(TestConstants.DEFAULT_TABLE_NAME)
                .where("id", 1)
                .col("name").as("person name").isEqual("alice")
                .countEquals(1)
                .run();
    }

    @Test
    public void testRetry() {
        new Thread(() -> {
            Utils.sleep(6000);
            try {
                Connection conn = DbAssertOptions.getGlobal().getFactory().getConnectionByDbKey(dbKey);
                conn.prepareStatement("update person set name = 'cole' where id=3").executeUpdate();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }).start();
        long start = System.currentTimeMillis();
        DbAssert.create(dbKey)
                .table(TestConstants.DEFAULT_TABLE_NAME)
                .where("id", 3)
                .col("name").as("person name").isEqual("cole")
                .run();
        Assert.assertTrue(System.currentTimeMillis() - start > 6000);
    }

    @Test
    public void testFilter() {
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

    @Test
    public void testStringColumnAssertion() {
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

    @Test
    public void testIntegerColumnAssertion() {
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

    @Test
    public void testFloatColumnAssertion() {
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

    @Test
    public void testDoubleColumnAssertion() {
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

    @Test
    public void testBooleanColumnAssertion() {
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

    @Test
    public void testInCondition() {
        DbAssert.create(dbKey)
                .table(TestConstants.DEFAULT_TABLE_NAME)
                .where("id", 1)
                .col("gender")
                .in(Arrays.asList("M", "F"))
                .notIn(Arrays.asList(1, 0))
                .run();
    }

    @Test
    public void testMatches() {
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

    @Test
    public void testListAssertion() {
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

    @Test
    public void testRowsAssertion() throws SQLException {
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

    @Test
    public void testCountAssertion() {
        DbAssert.create(dbKey)
                .table(TestConstants.DEFAULT_TABLE_NAME)
                .where("gender", "M")
                .col("gender")
                .countEquals(2)
                .run();
    }

    @Test
    public void testDistinctCountAssertion() {
        DbAssert.create(dbKey)
                .table(TestConstants.DEFAULT_TABLE_NAME)
                .col("gender")
                .distinctCountEqual(2)
                .distinctCountLessThan(3)
                .distinctCountLessThanOrEqual(2)
                .distinctCountGreaterThan(2)
                .distinctCountGreaterThanOrEqual(2)
                .distinctCountBetween(1, 2)
                .distinctCountBetween(1, true, 2, false)
                .run();
    }

    @Test
    public void testSuccessIfNotFound() {
        DbAssert.create(dbKey)
                .table(TestConstants.DEFAULT_TABLE_NAME)
                .retry(false)
                .failIfNotFound(false)
                .where("id", 9999)
                .col("name").isEqual("unknown")
                .run();
    }

    @Test(expectedExceptions = AssertionError.class)
    public void testFailIfNotFound() {
        DbAssert.create(dbKey)
                .table(TestConstants.DEFAULT_TABLE_NAME)
                .retry(false)
                .failIfNotFound(true)
                .where("id", 9999)
                .col("name").isEqual("unknown")
                .run();
    }

    @Test
    public void testAggregateAssertion() {
        DbAssert.create(dbKey)
                .table(TestConstants.DEFAULT_TABLE_NAME)
                .where("name", "bob")
                .col("name")
                .countEquals(1)
                .countLessThan(2)
                .countGreaterThan(0)
                .countBetween(0, 2);
    }

    @AfterClass
    public static void tearDown() throws SQLException {
        Connection conn = DbAssertOptions.getGlobal().getFactory().getConnectionByDbKey(dbKey);
        conn.prepareStatement(String.format("drop table if exists %s cascade", TestConstants.DEFAULT_TABLE_NAME)).executeUpdate();
    }

}
