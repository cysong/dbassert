package com.github.cysong.dbassert.sql;

import com.github.cysong.dbassert.assertion.Assertion;

/**
 * SqlBuilderFactory for mysql
 *
 * @author cysong
 * @date 2022/8/24 13:40
 **/
public class MysqlBuilderFactory implements SqlBuilderFactory {
    @Override
    public SqlBuilder newInstance(Assertion assertion) {
        return new MysqlBuilder(assertion);
    }

    @Override
    public boolean matches(String dbProductName) {
        return "MySQL".equalsIgnoreCase(dbProductName) || "Sqlite".equalsIgnoreCase(dbProductName);
    }
}
