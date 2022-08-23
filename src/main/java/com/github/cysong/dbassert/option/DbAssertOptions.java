package com.github.cysong.dbassert.option;

import com.github.cysong.dbassert.constant.Constants;
import com.github.cysong.dbassert.datasource.ConnectionFactory;
import com.github.cysong.dbassert.datasource.DefaultConnectionFactory;
import com.github.cysong.dbassert.utitls.Utils;

/**
 * Global options for DbAssert
 *
 * @author cysong
 * @date 2022/08/22 15:50
 **/
public class DbAssertOptions {
    private static final DbAssertOptions global = new DbAssertOptions();
    /**
     * whether should retry if assert fail
     **/
    private boolean retry = Constants.RETRY;
    /**
     * retry times, only work when {@link DbAssertOptions#retry} is true
     **/
    private int retryTimes = Constants.RETRY_TIMES;
    /**
     * wait milliseconds before every retry, only work when {@link DbAssertOptions#retry} is true
     **/
    private long retryInterval = Constants.RETRY_INTERVAL;
    /**
     * wait milliseconds before first assert
     **/
    private long delay = Constants.DELAY;
    /**
     * whether should fail if no rows return
     **/
    private boolean failIfNotFound = Constants.FAIL_IF_NOT_FOUND;
    /**
     * max return rows bu single query, avoid return too many rows for performance reason
     **/
    private int maxPageSize = Constants.PAGE_SIZE;
    /**
     * database config file for DefaultConnectionFactory to create connection by dbKey
     **/
    private String databaseFile = Constants.DATABASE_FILE;
    /**
     * create connection by dbKey, if not set {@link DefaultConnectionFactory} will be used
     **/
    private ConnectionFactory factory;
    /**
     * database login timeout(milliseconds)
     **/
    private long loginTimeout = Constants.LOGIN_TIMEOUT;
    /**
     * test database connection timeout(seconds) when valid
     **/
    private int testConnectionTimeout = Constants.TEST_CONNECTION_TIMEOUT;


    public static synchronized DbAssertOptions getGlobal() {
        return DbAssertOptions.global;
    }

    public static DbAssertOptions create() {
        return new DbAssertOptions();
    }

    private DbAssertOptions() {

    }

    public boolean isRetry() {
        return retry;
    }

    public DbAssertOptions retry(boolean retry) {
        this.retry = retry;
        return this;
    }

    public int getRetryTimes() {
        return retryTimes;
    }

    public DbAssertOptions retryTimes(int retryTimes) {
        this.retryTimes = retryTimes;
        return this;
    }

    public long getRetryInterval() {
        return retryInterval;
    }

    public DbAssertOptions retryInterval(long retryInterval) {
        this.retryInterval = retryInterval;
        return this;
    }

    public long getDelay() {
        return delay;
    }

    public DbAssertOptions delay(long delay) {
        this.delay = delay;
        return this;
    }

    public boolean isFailIfNotFound() {
        return failIfNotFound;
    }

    public DbAssertOptions failIfNotFound(boolean failIfNotFound) {
        this.failIfNotFound = failIfNotFound;
        return this;
    }

    public int getMaxPageSize() {
        return maxPageSize;
    }

    public void maxPageSize(int maxPageSize) {
        this.maxPageSize = maxPageSize;
    }

    public ConnectionFactory getFactory() {
        if (factory == null) {
            this.buildDefaultConnectionFactory();
        }
        return this.factory;
    }

    public DbAssertOptions factory(ConnectionFactory factory) {
        this.factory = factory;
        return this;
    }

    public String getDatabaseFile() {
        return databaseFile;
    }

    public DbAssertOptions databaseFile(String databaseFile) {
        assert Utils.isNotBlank(databaseFile);
        this.databaseFile = databaseFile;
        return this;
    }

    public long getLoginTimeout() {
        return loginTimeout;
    }

    public DbAssertOptions loginTimeout(long loginTimeout) {
        this.loginTimeout = loginTimeout;
        return this;
    }

    public int getTestConnectionTimeout() {
        return testConnectionTimeout;
    }

    public DbAssertOptions testConnectionTimeout(int testConnectionTimeout) {
        this.testConnectionTimeout = testConnectionTimeout;
        return this;
    }

    private synchronized void buildDefaultConnectionFactory() {
        if (this.factory != null) {
            return;
        }
        this.factory = new DefaultConnectionFactory(this.databaseFile);
    }
}
