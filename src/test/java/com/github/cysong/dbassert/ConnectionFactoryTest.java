package com.github.cysong.dbassert;

import com.github.cysong.dbassert.option.DbAssertOptions;
import com.github.cysong.dbassert.option.DbAssertSetup;
import org.testng.annotations.AfterSuite;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;

public class ConnectionFactoryTest {
    private static String dbFile = "test1.db";

    @BeforeClass
    public static void setup() throws SQLException, IOException {
        Connection conn = DbAssertOptions.getGlobal().getFactory().getConnectionByDbKey("test1");
        TestUtils.initDb(conn);
        try {
            conn.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testConnectionFactory() {
        DbAssert.create("test1")
                .table("person")
                .where("id", 1)
                .col("name")
                .isEqual("alice")
                .run();
    }

    @AfterSuite
    public static void tearDown() {
        DbAssertSetup.setup().getFactory().destroy();
        File db = new File(dbFile);
        if (db.exists()) {
            db.delete();
        }
    }
}
