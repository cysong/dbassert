package com.github.cysong.dbassert.utitls;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.SQLException;

/**
 * sql utils
 *
 * @author cysong
 * @date 2022/08/22 15:50
 **/
public class SqlUtils {

    public static void loadSqlScript(Connection conn, InputStream is) throws IOException, SQLException {
        String script = Utils.readFromInputStream(is);
        String[] sqls = script.split(";");
        for (String sql : sqls) {
            if (Utils.isNotBlank(sql)) {
                conn.prepareStatement(sql).executeUpdate();
            }
        }
    }
}
