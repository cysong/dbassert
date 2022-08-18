package com.github.cysong.dbassert.option;

import com.github.cysong.dbassert.constant.Constants;
import com.github.cysong.dbassert.datasource.ConnectionFactory;
import com.github.cysong.dbassert.datasource.DefaultConnectionFactory;
import com.github.cysong.dbassert.utitls.Utils;

public class DbAssertOptions {
    private static final DbAssertOptions global = new DbAssertOptions();

    private boolean retry = Constants.RETRY;
    private int retryTimes = Constants.RETRY_TIMES;
    private long retryInterval = Constants.RETRY_INTERVAL;
    private long delay = Constants.DELAY;
    private boolean failIfNotFound = Constants.FAIL_IF_NOT_FOUND;
    private int maxPageSize = Constants.PAGE_SIZE;
    private String databaseFile = Constants.DATABASE_FILE;
    private ConnectionFactory factory;


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

    private synchronized void buildDefaultConnectionFactory() {
        if (this.factory != null) {
            return;
        }
        this.factory = new DefaultConnectionFactory(this.databaseFile);
    }
}
