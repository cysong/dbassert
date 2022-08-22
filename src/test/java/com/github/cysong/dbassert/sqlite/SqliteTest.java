package com.github.cysong.dbassert.sqlite;

import com.github.cysong.dbassert.DbAssert;
import com.github.cysong.dbassert.TestConstants;
import com.github.cysong.dbassert.TestUtils;
import com.github.cysong.dbassert.utitls.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Arrays;

public class SqliteTest {
    private static final Logger log = LoggerFactory.getLogger(SqliteTest.class);
    private static final String dbFile = "sqlite.db";
    private static Connection conn;

    @BeforeClass
    public static void setup() throws SQLException, IOException {
        String url = "jdbc:sqlite:" + dbFile;
        conn = DriverManager.getConnection(url);

        TestUtils.initDb(conn);
    }

    @Test(timeOut = 1000)
    public void testNotRetry() {
        DbAssert.create(conn)
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
                conn.prepareStatement("update person set name = 'cole' where id=3").executeUpdate();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }).start();
        long start = System.currentTimeMillis();
        DbAssert.create(conn)
                .table(TestConstants.DEFAULT_TABLE_NAME)
                .where("id", 3)
                .col("name").as("person name").isEqual("cole")
                .run();
        Assert.assertTrue(System.currentTimeMillis() - start > 6000);
    }

    @Test
    public void testStringColumnAssertion() {
        DbAssert.create(conn)
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
        DbAssert.create(conn)
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
        DbAssert.create(conn)
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
        DbAssert.create(conn)
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
        DbAssert.create(conn)
                .table(TestConstants.DEFAULT_TABLE_NAME)
                .where("id", 1)
                .col("adult")
                .isFalse()
                .run();
        DbAssert.create(conn)
                .table(TestConstants.DEFAULT_TABLE_NAME)
                .where("id", 2)
                .col("adult")
                .isTrue()
                .run();
    }

    @Test
    public void testInCondition() {
        DbAssert.create(conn)
                .table(TestConstants.DEFAULT_TABLE_NAME)
                .where("id", 1)
                .col("gender")
                .in(Arrays.asList("M", "F"))
                .notIn(Arrays.asList(1, 0))
                .run();
    }

    @Test
    public void testRowsAssertion() throws SQLException {
        long totalRows = TestUtils.getTotalRowsOfTable(conn, TestConstants.DEFAULT_TABLE_NAME);
        DbAssert.create(conn)
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
        DbAssert.create(conn)
                .table(TestConstants.DEFAULT_TABLE_NAME)
                .where("id", 1)
                .col("gender")
                .countEquals(1)
                .run();
    }

    @Test
    public void testDistinctCountAssertion() {
        DbAssert.create(conn)
                .table(TestConstants.DEFAULT_TABLE_NAME)
                .col("gender")
                .distinctCountEqual(2)
                .run();
    }

    @Test
    public void testSuccessIfNotFound() {
        DbAssert.create(conn)
                .table(TestConstants.DEFAULT_TABLE_NAME)
                .retry(false)
                .failIfNotFound(false)
                .where("id", 9999)
                .col("name").isEqual("unknown")
                .run();
    }

    @Test(expectedExceptions = AssertionError.class)
    public void testFailIfNotFound() {
        DbAssert.create(conn)
                .table(TestConstants.DEFAULT_TABLE_NAME)
                .retry(false)
                .failIfNotFound(true)
                .where("id", 9999)
                .col("name").isEqual("unknown")
                .run();
    }

    @Test
    public void testAggregateAssertion() {
        DbAssert.create(conn)
                .table(TestConstants.DEFAULT_TABLE_NAME)
                .where("name", "bob")
                .col("name")
                .countEquals(1)
                .countLessThan(2)
                .countGreaterThan(0)
                .countBetween(0, 2);
    }

    @AfterClass
    public static void tearDown() {
        try {
            conn.close();
        } catch (SQLException e) {
            log.error(e.getMessage(), e);
        }
        File db = new File(dbFile);
        if (db.exists()) {
            db.delete();
        }
    }
}
