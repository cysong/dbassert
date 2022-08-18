package com.github.cysong.dbassert.assertion;

import com.github.cysong.dbassert.constant.Aggregate;
import com.github.cysong.dbassert.constant.Constants;
import com.github.cysong.dbassert.expression.AggregateCondition;
import com.github.cysong.dbassert.expression.Condition;
import com.github.cysong.dbassert.sql.SqlBuilder;
import com.github.cysong.dbassert.sql.SqlBuilderFactory;
import com.github.cysong.dbassert.sql.SqlResult;
import com.github.cysong.dbassert.utitls.Utils;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

public class AssertionExecutor {
    private static final Logger LOG = Logger.getLogger(AssertionExecutor.class.getName());
    private Assertion assertion;

    public static AssertionExecutor create(Assertion assertion) {
        return new AssertionExecutor(assertion);
    }

    private AssertionExecutor(Assertion assertion) {
        this.assertion = assertion;
    }

    public void run() throws SQLException {
        Connection conn = assertion.getConn();
        DatabaseMetaData md = conn.getMetaData();

        SqlBuilder sqlBuilder = SqlBuilderFactory.getSqlBuilder(md.getDatabaseProductName());
        SqlResult result = sqlBuilder.build(assertion);

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

            LOG.info("Assert success");
            break;
        } while (loop++ < totalLoop);
    }

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

    private void doAssert(boolean isFinal, MessageBuilder messageBuilder) {
        if (isFinal) {
            throw new AssertionError(messageBuilder.build());
        } else {
            LOG.info(messageBuilder.build());
        }
    }

    private void printRetryLog(int loopTimes) {
        if (assertion.isRetry()) {
            if (loopTimes > 1) {
                LOG.info(String.format("Retry %s/%s...", loopTimes - 1, assertion.getRetryTimes()));
            } else {
                LOG.info(String.format("Assert with total %s retries, interval %sms", assertion.getRetryTimes(), assertion.getRetryInterval()));
            }
        } else {
            LOG.info("Assert without retry...");
        }
    }

    @FunctionalInterface
    private static interface MessageBuilder {

        String build();
    }

}
