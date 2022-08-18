package com.github.cysong.dbassert.datasource;

import java.sql.Connection;

public interface ConnectionFactory {

    Connection getConnectionByDbKey(String dbKey);

    void destroy();

}
