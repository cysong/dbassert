package com.github.cysong.dbassert.assertion;

import com.github.cysong.dbassert.constant.Aggregate;
import com.github.cysong.dbassert.constant.Constants;
import com.github.cysong.dbassert.expression.AggregateCondition;
import com.github.cysong.dbassert.expression.Condition;
import com.github.cysong.dbassert.expression.ListCondition;
import com.github.cysong.dbassert.option.DbAssertOptions;
import com.github.cysong.dbassert.report.Reporter;
import com.github.cysong.dbassert.report.Status;
import com.github.cysong.dbassert.sql.SqlBuilderSelector;
import com.github.cysong.dbassert.sql.SqlResult;
import com.github.cysong.dbassert.utitls.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
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
    public void run() {
        startStep();
        try {
            SqlResult result = SqlBuilderSelector.getSqlBuilder(assertion).build();
            printSql(result);

            if (this.assertion.getDelay() > 0) {
                Utils.sleep(this.assertion.getDelay());
            }

            //total loop times
            int totalLoop = assertion.isRetry() ? assertion.getRetryTimes() + 1 : 1;
            int loop = 1;
            long timestamp = System.currentTimeMillis();
            printSummaryLog();
            do {
                if (loop > 1) {
                    printRetryLog(loop - 1);
                    long waitMills = timestamp + assertion.getRetryInterval() - System.currentTimeMillis();
                    if (waitMills > 0) {
                        Utils.sleep(waitMills);
                    }
                }
                timestamp = System.currentTimeMillis();

                if (verify(loop == totalLoop, result)) {
                    log.info("Assert success");
                    endStep(Status.PASSED);
                    break;
                } else {
                    continue;
                }
            } while (loop++ < totalLoop);
        } catch (Throwable throwable) {
            endStep(throwable);
            if (throwable instanceof RuntimeException) {
                throw (RuntimeException) throwable;
            }
            throw new RuntimeException(throwable);
        }
    }

    private boolean verify(boolean isFinal, SqlResult result) throws SQLException {
        String aggSql = result.getAggregateSql();
        ResultSet aggRs = assertion.getConn().prepareStatement(aggSql).executeQuery();

        aggRs.next();
        long totalRows = aggRs.getLong(Constants.COUNT_ROWS_LABEL);
        if (totalRows == 0) {
            aggRs.close();
            if (isFinal && assertion.isFailIfNotFound()) {
                throw new AssertionError("Data records not found");
            }
            return false;
        }

        //verify total rows
        if (!verifyRows(totalRows, isFinal)) {
            aggRs.close();
            return false;
        }

        //verify aggregate columns
        if (!verifyAggregates(aggRs, result.getAggColumns(), isFinal)) {
            aggRs.close();
            return false;
        }

        //verify columns details
        String detailSql = result.getDetailSql();
        if (detailSql != null) {
            ResultSet detailRs = assertion.getConn().prepareStatement(detailSql).executeQuery();
            if (!verifyDetails(detailRs, result, isFinal)) {
                detailRs.close();
                return false;
            }
        }
        return true;
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
     * @param rs      result set return by query
     * @param result  sql build result
     * @param isFinal determine throw AssertionError or print log when verify fail
     * @return boolean
     * @author cysong
     * @date 2022/8/23 9:35
     **/
    private boolean verifyDetails(ResultSet rs, SqlResult result, boolean isFinal) throws SQLException {
        Map<String, List<Object>> valueMap = new HashMap<>(result.getColumnSet().size());
        Map<String, List<Condition>> columnMap = result.getColumns();
        while (rs.next()) {
            for (String col : result.getColumnSet()) {
                Object value = rs.getObject(col);
                valueMap.compute(col, (key, val) -> {
                    if (val == null) {
                        val = new ArrayList<>();
                    }
                    val.add(value);
                    return val;
                });

                if (columnMap.containsKey(col)) {
                    List<Condition> conditions = result.getColumns().get(col);
                    for (Condition condition : conditions) {
                        if (!ConditionTester.test(condition.getComparator(), value, condition.getExpected())) {
                            this.doAssert(isFinal, () -> condition.getAssertMessage(value));
                            return false;
                        }
                    }
                }
            }
        }

        Map<String, List<ListCondition>> listColumnMap = result.getListColumns();
        for (String col : listColumnMap.keySet()) {
            List<ListCondition> conditions = listColumnMap.get(col);
            for (ListCondition condition : conditions) {
                if (!ConditionTester.test(condition.getComparator(), valueMap.get(col), condition.getExpected())) {
                    this.doAssert(isFinal, () -> condition.getAssertMessage(valueMap.get(col)));
                    return false;
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
        System.out.println("==================== sql ====================");
    }

    private void printSummaryLog() {
        if (assertion.isRetry()) {
            log.info("Assert with total {} retries, interval {}ms", assertion.getRetryTimes(), assertion.getRetryInterval());
        } else {
            log.info("Assert without retry...");
        }
    }

    private void printRetryLog(int retryTimes) {
        log.info("Retry {}/{}...", retryTimes, assertion.getRetryTimes());
    }

    private void startStep() {
        Reporter reporter = DbAssertOptions.getGlobal().getReporter();
        if (reporter != null) {
            reporter.startStep(Constants.REPORT_STEP_NAME);
        }
    }

    private void endStep(Status status) {
        Reporter reporter = DbAssertOptions.getGlobal().getReporter();
        if (reporter != null) {
            reporter.endStep(status);
        }
    }

    private void endStep(Throwable throwable) {
        Reporter reporter = DbAssertOptions.getGlobal().getReporter();
        if (reporter != null) {
            reporter.endStep(throwable);
        }
    }

    private void addSqlAttachment(SqlResult result) {
        Reporter reporter = DbAssertOptions.getGlobal().getReporter();
        StringBuilder content = new StringBuilder();
        if (reporter != null) {
            if (result.getDetailSql() != null) {
                content.append(result.getDetailSql());
                content.append(System.lineSeparator());
            }
            if (result.getAggregateSql() != null) {
                content.append(result.getAggregateSql());
            }
            reporter.addAttachment("sql", content.toString());
        }
    }

    private void addAttachment(String name, String content) {
        Reporter reporter = DbAssertOptions.getGlobal().getReporter();
        if (reporter != null) {
            reporter.addAttachment(name, content);
        }
    }

    private void addHtmlAttachment(String name, String content) {
        Reporter reporter = DbAssertOptions.getGlobal().getReporter();
        if (reporter != null) {
            reporter.addAttachment(name, "text/html", content);
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
