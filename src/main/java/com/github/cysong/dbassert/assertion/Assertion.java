package com.github.cysong.dbassert.assertion;

import com.github.cysong.dbassert.constant.Constants;
import com.github.cysong.dbassert.constant.Sort;
import com.github.cysong.dbassert.expression.Condition;
import com.github.cysong.dbassert.object.DbObject;
import com.github.cysong.dbassert.option.DbAssertOptions;
import com.github.cysong.dbassert.utitls.Utils;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;

public class Assertion {
    private Connection conn;
    private String database;
    private String databaseAlias;
    private String tableName;
    private String tableAlias;

    private boolean retry;
    private int retryTimes;
    private long retryInterval;
    private long delay;
    private boolean failIfNotFound;

    private int startIndex = 1;
    private int pageSize;

    private String textCondition;
    private List<Condition> conditions;
    private List<Condition> verifies;
    private List<Condition> rowVerifies;
    private List<Sort> sorts;

    public Assertion(Connection conn) {
        assert conn != null;
        this.conn = conn;
        this.initFromGlobalOptions();
    }

    public void addAlias(DbObject dbObject) {
        assert dbObject != null;
        switch (dbObject.getType()) {
            case DATABASE:
                this.databaseAlias = dbObject.getAlias();
                return;
            case TABLE:
                this.tableAlias = dbObject.getAlias();
                return;
            case COLUMN:
                return;
            default:
                throw new IllegalArgumentException("Unrecognized db object:" + dbObject.getType().name());
        }
    }

    public Assertion addCondition(Condition condition) {
        assert condition != null;
        if (this.conditions == null) {
            this.conditions = new ArrayList<>();
        }
        this.conditions.add(condition);
        return this;
    }

    public Assertion addVerify(Condition condition) {
        assert condition != null;
        if (this.verifies == null) {
            this.verifies = new ArrayList<>();
        }
        this.verifies.add(condition);
        return this;
    }

    public Assertion addRowVerify(Condition condition) {
        assert condition != null;
        if (this.rowVerifies == null) {
            this.rowVerifies = new ArrayList<>();
        }
        condition.setAlias("total rows returned");
        this.rowVerifies.add(condition);
        return this;
    }

    public Assertion addVerifies(List<Condition> conditions) {
        assert conditions != null;
        if (this.verifies == null) {
            this.verifies = new ArrayList<>();
        }
        this.verifies.addAll(conditions);
        return this;
    }

    public Assertion addSort(Sort sort) {
        assert sort != null;
        if (this.sorts == null) {
            sorts = new ArrayList<>();
        }
        sorts.add(sort);
        return this;
    }

    private void initFromGlobalOptions() {
        DbAssertOptions options = DbAssertOptions.getGlobal();
        this.retry = options.isRetry();
        this.retryInterval = options.getRetryInterval();
        this.retryTimes = options.getRetryTimes();
        this.delay = options.getDelay();
        this.failIfNotFound = options.isFailIfNotFound();
        this.pageSize = options.getMaxPageSize();
    }

    public Connection getConn() {
        return conn;
    }

    public String getDatabase() {
        return database;
    }

    public void setDatabase(String database) {
        assert database != null && database.length() > 0;
        this.database = database;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        assert tableName != null && tableName.length() > 0;
        this.tableName = tableName;
    }

    public String getDatabaseAlias() {
        return databaseAlias;
    }

    public void setDatabaseAlias(String databaseAlias) {
        this.databaseAlias = databaseAlias;
    }

    public String getTableAlias() {
        return tableAlias;
    }

    public void setTableAlias(String tableAlias) {
        this.tableAlias = tableAlias;
    }

    public boolean isRetry() {
        return retry;
    }

    public void setRetry(boolean retry) {
        this.retry = retry;
    }

    public int getRetryTimes() {
        return retryTimes;
    }

    public void setRetryTimes(int retryTimes) {
        assert retryTimes > 0;
        this.retryTimes = retryTimes;
    }

    public long getRetryInterval() {
        return retryInterval;
    }

    public void setRetryInterval(long retryInterval) {
        assert retryInterval > 0 && retryInterval <= Constants.MAX_RETRY_INTERVAL;
        this.retryInterval = retryInterval;
    }

    public long getDelay() {
        return delay;
    }

    public void setDelay(long delay) {
        assert delay > 0 && delay <= Constants.MAX_DELAY;
        this.delay = delay;
    }

    public boolean isFailIfNotFound() {
        return failIfNotFound;
    }

    public void setFailIfNotFound(boolean failIfNotFound) {
        this.failIfNotFound = failIfNotFound;
    }

    public int getStartIndex() {
        return startIndex;
    }

    public void setStartIndex(int startIndex) {
        assert startIndex > 0;
        this.startIndex = startIndex;
    }

    public int getPageSize() {
        return pageSize;
    }

    public void setPageSize(int pageSize) {
        assert pageSize > 0 && pageSize <= Constants.MAX_PAGE_SIZE;
        this.pageSize = pageSize;
    }

    public String getTextCondition() {
        return textCondition;
    }

    public void setTextCondition(String textCondition) {
        assert Utils.isNotBlank(textCondition);
        this.textCondition = textCondition;
    }

    public List<Condition> getConditions() {
        return conditions;
    }

    public List<Condition> getVerifies() {
        return verifies;
    }

    public List<Condition> getRowVerifies() {
        return rowVerifies;
    }

    public List<Sort> getSorts() {
        return sorts;
    }

}
