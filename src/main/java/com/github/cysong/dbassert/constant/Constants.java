package com.github.cysong.dbassert.constant;

/**
 * global constants
 *
 * @author cysong
 * @date 2022/08/22 15:50
 **/
public class Constants {
    public static final boolean RETRY = true;
    public static final int RETRY_TIMES = 10;
    public static final long RETRY_INTERVAL = 3000;
    public static final long MAX_RETRY_INTERVAL = 10000;
    public static final long DELAY = 0;
    public static final long MAX_DELAY = 30000;
    public static final boolean FAIL_IF_NOT_FOUND = true;
    public static final int PAGE_SIZE = 100;
    public static final int MAX_PAGE_SIZE = 10000;
    public static final String DATABASE_FILE = "database.yml";
    public static final long LOGIN_TIMEOUT = 30000;
    public static final int TEST_CONNECTION_TIMEOUT = 10;

    public static final String COUNT_ROWS_COLUMN = "*";
    public static final String COUNT_ROWS_LABEL = "count";

}
