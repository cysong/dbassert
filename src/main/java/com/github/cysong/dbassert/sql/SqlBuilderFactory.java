package com.github.cysong.dbassert.sql;

import com.github.cysong.dbassert.assertion.Assertion;

import java.sql.DatabaseMetaData;

/**
 * factory interface for creating sql builder instance
 *
 * @author cysong
 * @date 2022/8/24 13:33
 **/
public interface SqlBuilderFactory {

    /**
     * create new instance of sql builder
     *
     * @param assertion assertion
     * @return com.github.cysong.dbassert.sql.SqlBuilder
     * @author cysong
     * @date 2022/8/24 13:35
     **/
    SqlBuilder newInstance(Assertion assertion);

    /**
     * test whether this instance suite from given database by dbProductName
     *
     * @param dbProductName database product name return by {@link DatabaseMetaData#getDatabaseProductName()}
     * @return boolean
     * @author cysong
     * @date 2022/8/24 13:36
     **/
    boolean matches(String dbProductName);

}
