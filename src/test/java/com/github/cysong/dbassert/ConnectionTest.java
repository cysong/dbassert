package com.github.cysong.dbassert;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterSuite;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/**
 * ConnectionFactory testcases
 *
 * @author cysong
 * @date 2022/08/22 15:50
 */
public class ConnectionTest {
    private static final Logger log = LoggerFactory.getLogger(ConnectionTest.class);
    private static String dbFile = "test1.db";
    private static Connection conn;

    @BeforeClass
    public static void setup() throws SQLException {
        String url = "jdbc:sqlite:" + dbFile;
        conn = DriverManager.getConnection(url);
        TestUtils.initDb(conn);
    }

    @Test
    public void testAssertionByConnection() {
        DbAssert.create(conn)
                .table(TestConstants.DEFAULT_TABLE_NAME)
                .where("id", 1)
                .col("name")
                .isEqual("alice")
                .run();
    }

    @AfterSuite
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
