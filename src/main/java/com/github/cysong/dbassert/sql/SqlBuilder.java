package com.github.cysong.dbassert.sql;

import com.github.cysong.dbassert.assertion.Assertion;

public interface SqlBuilder {

    SqlResult build(Assertion assertion);

    boolean match(String dbProductName);

}
