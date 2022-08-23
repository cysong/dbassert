package com.github.cysong.dbassert.assertion;

import com.github.cysong.dbassert.constant.Aggregate;
import com.github.cysong.dbassert.constant.Constants;
import com.github.cysong.dbassert.expression.AggregateCondition;
import com.github.cysong.dbassert.expression.Condition;
import com.github.cysong.dbassert.sql.SqlBuilder;
import com.github.cysong.dbassert.sql.SqlBuilderFactory;
import com.github.cysong.dbassert.sql.SqlResult;
import com.github.cysong.dbassert.utitls.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

/**
 * Executor for asserting
 *
 * @author cysong
 * @date 2022/08/22 15:50
 **/
public class AssertionExecutor {
    private static final Logger log = LoggerFactory.getLogger(AssertionExecutor.class);
    private final Assertion assertion;

    public static AssertionExecutor create(Assertion assertion) {
        return new AssertionExecutor(assertion);
    }

    private AssertionExecutor(Assertion assertion) {
        this.assertion = assertion;
    }

    /**
     * do the assert
     *
     * @author cysong
     * @date 2022/8/23 9:25
     **/
    public void run() throws SQLException {
        Connection conn = assertion.getConn();
        DatabaseMetaData md = conn.getMetaData();

        SqlBuilder sqlBuilder = SqlBuilderFactory.getSqlBuilder(md.getDatabaseProductName());
        SqlResult result = sqlBuilder.build(assertion);

        printSql(result);

        if (this.assertion.getDelay() > 0) {
            Utils.sleep(this.assertion.getDelay());
        }

        //total loop times
        int totalLoop = assertion.isRetry() ? assertion.getRetryTimes() + 1 : 1;
        int loop = 1;
        long timestamp = System.currentTimeMillis();
        do {
            printRetryLog(loop);
            if (loop > 1) {
                long waitMills = timestamp + assertion.getRetryInterval() - System.currentTimeMillis();
                if (waitMills > 0) {
                    Utils.sleep(waitMills);
                }
            }
            timestamp = System.currentTimeMillis();

            boolean isFinal = loop == totalLoop;

            String aggSql = result.getAggregateSql();
            ResultSet aggRs = assertion.getConn().prepareStatement(aggSql).executeQuery();

            aggRs.next();
            long totalRows = aggRs.getLong(Constants.COUNT_ROWS_LABEL);
            if (totalRows == 0) {
                aggRs.close();
                if (isFinal && assertion.isFailIfNotFound()) {
                    throw new AssertionError("Data records not found");
                } else {
                    continue;
                }
            }

            //verify total rows
            if (!verifyRows(totalRows, isFinal)) {
                aggRs.close();
                continue;
            }

            //verify aggregate columns
            if (!verifyAggregates(aggRs, result.getAggColumns(), isFinal)) {
                aggRs.close();
                continue;
            }

            //verify columns details
            String detailSql = result.getDetailSql();
            if (detailSql != null) {
                ResultSet detailRs = assertion.getConn().prepareStatement(detailSql).executeQuery();
                if (!verifyDetails(detailRs, result.getColumns(), isFinal)) {
                    detailRs.close();
                    continue;
                }
            }

            log.info("Assert success");
            break;
        } while (loop++ < totalLoop);
    }

    /**
     * verify total rows, if isFinal is true throw AssertionError or only print log when verify fail
     *
     * @param totalRows value of count(*) return by the query
     * @param isFinal   determine throw AssertionError or print log when verify fail
     * @return boolean
     * @author cysong
     * @date 2022/8/23 9:26
     **/
    private boolean verifyRows(long totalRows, boolean isFinal) {
        if (Utils.isEmpty(assertion.getRowVerifies())) {
            return true;
        }
        for (Condition condition : assertion.getRowVerifies()) {
            if (!ConditionTester.test(condition.getComparator(), totalRows, condition.getExpected())) {
                doAssert(isFinal, () -> condition.getAssertMessage(totalRows));
                return false;
            }
        }
        return true;
    }

    /**
     * verify aggregate columns
     *
     * @param rs           result set return by query
     * @param aggColumnMap aggregate column map group by column name
     * @param isFinal      determine throw AssertionError or print log when verify fail
     * @return boolean
     * @author cysong
     * @date 2022/8/23 9:31
     **/
    private boolean verifyAggregates(ResultSet rs, Map<String, List<AggregateCondition>> aggColumnMap, boolean isFinal) throws SQLException {
        if (Utils.isEmpty(aggColumnMap)) {
            return true;
        }
        while (rs.next()) {
            for (String col : aggColumnMap.keySet()) {
                List<AggregateCondition> aggConditions = aggColumnMap.get(col);
                for (AggregateCondition aggCondition : aggConditions) {
                    Aggregate aggregate = aggCondition.getAggregate();
                    String label = aggregate.getWrappedColumnLabel(col);
                    Object actual = rs.getObject(label);
                    if (!ConditionTester.test(aggCondition.getComparator(), actual, aggCondition.getExpected())) {
                        this.doAssert(isFinal, () -> aggCondition.getAssertMessage(actual));
                        return false;
                    }
                }
            }
        }
        return true;
    }

    /**
     * verify column details
     *
     * @param rs        result set return by query
     * @param columnMap column map group by column name
     * @param isFinal   determine throw AssertionError or print log when verify fail
     * @return boolean
     * @author cysong
     * @date 2022/8/23 9:35
     **/
    private boolean verifyDetails(ResultSet rs, Map<String, List<Condition>> columnMap, boolean isFinal) throws SQLException {
        while (rs.next()) {
            for (String col : columnMap.keySet()) {
                List<Condition> conditions = columnMap.get(col);
                for (Condition condition : conditions) {
                    Object actual = rs.getObject(col);
                    if (!ConditionTester.test(condition.getComparator(), actual, condition.getExpected())) {
                        this.doAssert(isFinal, () -> condition.getAssertMessage(actual));
                        return false;
                    }
                }
            }
        }
        return true;
    }

    /**
     * throw AssertionError if isFinal is true or print message
     *
     * @param isFinal
     * @param messageBuilder
     * @author cysong
     * @date 2022/8/23 9:36
     **/
    private void doAssert(boolean isFinal, MessageBuilder messageBuilder) {
        if (isFinal) {
            throw new AssertionError(messageBuilder.build());
        } else {
            log.info(messageBuilder.build());
        }
    }

    private void printSql(SqlResult sqlResult) {
        System.out.println("==================== sql ====================");
        if (sqlResult.getDetailSql() != null) {
            System.out.println(sqlResult.getDetailSql());
        }
        if (sqlResult.getAggregateSql() != null) {
            System.out.println(sqlResult.getAggregateSql());
        }
    }

    private void printRetryLog(int loopTimes) {
        if (assertion.isRetry()) {
            if (loopTimes > 1) {
                log.info("Retry {}/{}...", loopTimes - 1, assertion.getRetryTimes());
            } else {
                log.info("Assert with total {} retries, interval {}ms", assertion.getRetryTimes(), assertion.getRetryInterval());
            }
        } else {
            log.info("Assert without retry...");
        }
    }

    @FunctionalInterface
    private interface MessageBuilder {
        /**
         * build message for lazy executing
         *
         * @return java.lang.String
         * @author cysong
         * @date 2022/8/23 9:38
         **/
        String build();

    }

}
