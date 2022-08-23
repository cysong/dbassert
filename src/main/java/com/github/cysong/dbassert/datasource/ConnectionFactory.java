package com.github.cysong.dbassert.datasource;

import java.sql.Connection;

/**
 * interface for creating connections by dbKey from database config file
 * the default implementation is {@link DefaultConnectionFactory}
 *
 * @author cysong
 * @date 2022/08/22 15:50
 **/
public interface ConnectionFactory {

    /**
     * create database connection by dbkey
     *
     * @param dbKey database key in the database config file
     * @return java.sql.Connection
     * @author cysong
     * @date 2022/8/23 9:20
     **/
    Connection getConnectionByDbKey(String dbKey);

    /**
     * close all connections and do clean work
     *
     * @author cysong
     * @date 2022/8/23 9:20
     **/
    void destroy();

}
