package com.github.cysong.dbassert.sql;

import com.github.cysong.dbassert.exception.SqlBuilderNotFoundException;

import java.util.Iterator;
import java.util.ServiceLoader;

public class SqlBuilderFactory {
    private static final ServiceLoader<SqlBuilder> loader = ServiceLoader.load(SqlBuilder.class);

    public static SqlBuilder getSqlBuilder(String dbProductName) {
        Iterator<SqlBuilder> it = loader.iterator();
        while (it.hasNext()) {
            SqlBuilder sqlBuilder = it.next();
            if (sqlBuilder.match(dbProductName)) {
                return sqlBuilder;
            }
        }
        throw new SqlBuilderNotFoundException("SqlBuilder for db %s not found");
    }

}
