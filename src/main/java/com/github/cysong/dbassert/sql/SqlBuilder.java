package com.github.cysong.dbassert.sql;

import com.github.cysong.dbassert.assertion.Assertion;

/**
 * interface define methods to build sql for any type of database
 *
 * @author cysong
 * @date 2022/08/22 15:50
 **/
public interface SqlBuilder {

    /**
     * build sql from assertion
     *
     * @param assertion
     * @return com.github.cysong.dbassert.sql.SqlResult
     * @author cysong
     * @date 2022/8/23 10:14
     **/
    SqlResult build(Assertion assertion);

    /**
     * suite for database verified by dbProductName if return true
     *
     * @param dbProductName database product name return by metadata
     * @return boolean
     * @author cysong
     * @date 2022/8/23 10:15
     **/
    boolean match(String dbProductName);

}
