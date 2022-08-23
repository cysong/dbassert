package com.github.cysong.dbassert.sql;

import com.github.cysong.dbassert.constant.Constants;
import com.github.cysong.dbassert.expression.Condition;
import com.github.cysong.dbassert.expression.Order;
import com.github.cysong.dbassert.utitls.Utils;

import java.util.stream.Collectors;

/**
 * sqlbuilder for mysql
 *
 * @author cysong
 * @date 2022/08/22 15:50
 **/
public class MysqlBuilder extends AbstractSqlBuilder {

    @Override
    protected void buildSql() {
        StringBuilder sb = new StringBuilder("select %s from ");
        sb.append(getQuotedFullTableName());
        sb.append(" where 1");
        if (Utils.isNotBlank(assertion.getTextCondition())) {
            sb.append(" and ").append(assertion.getTextCondition());
        }
        if (Utils.isNotEmpty(assertion.getConditions())) {
            for (Condition condition : assertion.getConditions()) {
                sb.append(" and ").append(condition.getColumnName());
                if (condition.getExpected() == null) {
                    sb.append(" is null");
                } else {
                    sb.append("=").append(wrapConditionValue(condition.getExpected()));
                }
            }
        }
        if (Utils.isNotEmpty(assertion.getSorts())) {
            sb.append(" order by ");
            assertion.getSorts().forEach(sort -> {
                sb.append(sort.getOrderBy());
                if (Order.DESC == sort.getOrder()) {
                    sb.append(" desc");
                }
                sb.append(",");
            });
            sb.deleteCharAt(sb.length() - 1);
        }
        sb.append(" limit ");
        if (assertion.getStartIndex() > 1) {
            sb.append(assertion.getStartIndex()).append(",");
        }
        sb.append(assertion.getPageSize());
        String sql = sb.toString();

        if (Utils.isNotEmpty(result.getColumns())) {
            result.setDetailSql(String.format(sql,
                    result.getColumns().keySet().stream().map(col -> quotedIdentifier(col))
                            .collect(Collectors.joining(","))
            ));
        }
        StringBuilder countExp = new StringBuilder("count(*) ");
        countExp.append(Constants.COUNT_ROWS_LABEL);
        if (Utils.isNotEmpty(result.getWrapAggColumns())) {
            countExp.append(",").append(String.join(",", result.getWrapAggColumns()));
        }
        result.setAggregateSql(String.format(sql, countExp));
    }

    @Override
    public boolean match(String dbProductName) {
        return "MySql".equalsIgnoreCase(dbProductName) || "Sqlite".equalsIgnoreCase(dbProductName);
    }

    @Override
    protected String getOpenQuote() {
        return "`";
    }

    @Override
    protected String getCloseQuote() {
        return "`";
    }
}
