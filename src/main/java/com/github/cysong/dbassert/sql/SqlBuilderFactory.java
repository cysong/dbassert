package com.github.cysong.dbassert.sql;

import com.github.cysong.dbassert.exception.SqlBuilderNotFoundException;

import java.util.Iterator;
import java.util.ServiceLoader;

/**
 * factory for generate sqlbuilder by database product name
 *
 * @author cysong
 * @date 2022/08/22 15:50
 **/
public class SqlBuilderFactory {
    private static final ServiceLoader<SqlBuilder> loader = ServiceLoader.load(SqlBuilder.class);

    public static synchronized SqlBuilder getSqlBuilder(String dbProductName) {
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
