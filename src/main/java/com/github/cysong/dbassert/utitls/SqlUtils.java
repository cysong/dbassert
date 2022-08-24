package com.github.cysong.dbassert.utitls;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;

/**
 * sql utils
 *
 * @author cysong
 * @date 2022/08/22 15:50
 **/
public class SqlUtils {

    /**
     * load input stream to database as sql script
     *
     * @param conn data connection
     * @param is   input stream
     * @author cysong
     * @date 2022/8/24 10:20
     **/
    public static void loadSqlScript(Connection conn, InputStream is) {
        try {
            String script = Utils.readInputStreamAsString(is);
            String[] sqlList = script.split(";");
            for (String sql : sqlList) {
                if (Utils.isNotBlank(sql)) {
                    conn.prepareStatement(sql).executeUpdate();
                }
            }
            if (!conn.getAutoCommit()) {
                conn.commit();
            }
        } catch (SQLException | IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * delete table
     *
     * @param conn      database connection
     * @param tableName database table name
     * @author cysong
     * @date 2022/8/24 10:36
     **/
    public static void deleteTable(Connection conn, String tableName) {
        try {
            conn.prepareStatement(String.format("drop table if exists %s", tableName)).executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * delete database file if database is sqlite
     *
     * @param conn database connection
     * @author cysong
     * @date 10:18
     **/
    public static void deleteDbFileIfSqlite(Connection conn) {
        try {
            DatabaseMetaData metaData = conn.getMetaData();
            if ("Sqlite".equalsIgnoreCase(metaData.getDatabaseProductName())) {
                String url = metaData.getURL();
                String filename = url.substring(url.lastIndexOf(":") + 1);
                conn.close();
                //in memory db doesn't has a db file
                if (Utils.isBlank(filename)) {
                    return;
                }
                File dbFile = new File(filename);
                if (dbFile.exists()) {
                    dbFile.delete();
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * get database product name by {@link DatabaseMetaData#getDatabaseProductName()}
     *
     * @param conn
     * @return java.lang.String
     * @author cysong
     * @date 2022/8/24 14:27
     **/
    public static String getDatabaseProductName(Connection conn) {
        try {
            DatabaseMetaData metaData = conn.getMetaData();
            return metaData.getDatabaseProductName();
        } catch (SQLException e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    /**
     * whether is sqlite database base on product name
     *
     * @param conn
     * @return boolean
     * @author cysong
     * @date 2022/8/24 14:27
     **/
    public static boolean isSqlite(Connection conn) {
        return "Sqlite".equalsIgnoreCase(getDatabaseProductName(conn));
    }
}
