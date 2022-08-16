package com.github.cysong.dbassert.datasource;

import java.sql.Connection;

public class DefaultConnectionFactory implements ConnectionFactory {

    public DefaultConnectionFactory(String databaseFile) {
    }

    @Override
    public Connection getConnectionByDbKey(String dbKey) {
        return null;
    }
}
