package com.github.cysong.dbassert;

import com.github.cysong.dbassert.utitls.SqlUtils;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * @program: dbassert
 * @description:
 * @author: chenyansong
 * @create: 2022-08-22 10:40
 **/
public class TestUtils {

    public static void initDb(Connection conn) throws SQLException, IOException {
        InputStream is = Thread.currentThread().getContextClassLoader().getResourceAsStream(TestConstants.DEFAULT_SQL_FILE);
        assert is != null;
        SqlUtils.loadSqlScript(conn, is);
        is.close();
    }

    public static long getTotalRowsOfTable(Connection conn, String tableName) throws SQLException {
        ResultSet rs = conn.prepareStatement("select count(*) count from " + tableName).executeQuery();
        long count = 0;
        while (rs.next()) {
            count = rs.getLong("count");
        }
        rs.close();
        return count;
    }


}
