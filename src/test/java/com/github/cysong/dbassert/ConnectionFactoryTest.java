package com.github.cysong.dbassert;

import com.github.cysong.dbassert.option.DbAssertSetup;
import com.github.cysong.dbassert.utitls.SqlUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class ConnectionFactoryTest {
    private static String dbFile = "test1.db";
    private static final String sqlFile = "sqlite.sql";

    @BeforeClass
    public static void setup() throws SQLException, IOException {
        Connection conn = DriverManager.getConnection("jdbc:sqlite:" + dbFile);
        initDb(conn);
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


    @AfterClass
    public static void tearDown() {
        DbAssertSetup.setup().getFactory().destroy();
        File db = new File(dbFile);
        if (db.exists()) {
            db.delete();
        }
    }

    public static void initDb(Connection conn) throws SQLException, IOException {
        InputStream is = Thread.currentThread().getContextClassLoader().getResourceAsStream(sqlFile);
        assert is != null;
        SqlUtils.loadSqlScript(conn, is);
        is.close();
    }


}
