package com.github.cysong.dbassert.sqlite;

import com.github.cysong.dbassert.DbAssert;
import com.github.cysong.dbassert.utitls.SqlUtils;
import com.github.cysong.dbassert.utitls.Utils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;

public class SqliteTest {
    private static final String dbFile = "sqlite.db";
    private static final String sqlFile = "sqlite.sql";
    private static final String tableName = "person";
    private Connection conn;

    @Before
    public void setup() throws SQLException, IOException {
        String url = "jdbc:sqlite:" + dbFile;
        conn = DriverManager.getConnection(url);

        this.initDb();
    }

    @Test(timeout = 1000)
    public void testNotRetry() {
        DbAssert.create(conn)
                .retry(false)
                .table(tableName)
                .where("id", 1)
                .col("name").as("person name").isEqual("alice")
                .countEquals(1)
                .run();
    }

    @Test
    public void testRetry() {
        new Thread(() -> {
            Utils.sleep(10000);
            try {
                conn.prepareStatement("update person set name = 'anson' where id=1").executeUpdate();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }).start();
        long start = System.currentTimeMillis();
        DbAssert.create(conn)
                .table(tableName)
                .where("id", 1)
                .col("name").as("person name").isEqual("anson")
                .run();
        Assert.assertTrue(System.currentTimeMillis() - start > 10000);
    }

    @Test
    public void testDetailAssertion() {
        DbAssert.create(conn)
                .table(tableName)
                .where("id", 1)
                .col("name").as("person name").isNotNull().contains("a").isEqual("alice")
                .col("age").isEqual(10).isEqual("10").isNotEqual("9")
                .col("weight").isEqual(40.0).isEqual("40.0000").isEqual("40.0d")
                .col("height").isEqual("94.5").isEqual("94.5f")
                .run();
    }

    @Test
    public void testRowsAssertion() throws SQLException {
        long totalRows = getTotalRowsOfTable(tableName);
        DbAssert.create(conn)
                .table(tableName)
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
    public void testAggregateAssertion() {
        DbAssert.create(conn)
                .table(tableName)
                .where("name", "bob")
                .col("name")
                .countEquals(1)
                .countLessThan(2)
                .countGreaterThan(0)
                .countBetween(0, 2);
    }

    @After
    public void tearDown() {
        try {
            conn.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        File db = new File(dbFile);
        if (db.exists()) {
            db.delete();
        }
    }

    private void initDb() throws SQLException, IOException {
        InputStream is = getClass().getClassLoader().getResourceAsStream(sqlFile);
        assert is != null;
        SqlUtils.loadSqlScript(conn, is);
        is.close();

        System.out.println("total person rows: " + getTotalRowsOfTable(tableName));
    }

    private long getTotalRowsOfTable(String tableName) throws SQLException {
        ResultSet rs = conn.prepareStatement("select count(*) count from " + tableName).executeQuery();
        long count = 0;
        while (rs.next()) {
            count = rs.getLong("count");
        }
        rs.close();
        return count;
    }

}
