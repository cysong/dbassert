package com.github.cysong.dbassert.sql;

import com.github.cysong.dbassert.constant.Constants;
import com.github.cysong.dbassert.exception.ConfigurationException;
import com.github.cysong.dbassert.expression.AbstractFilter;
import com.github.cysong.dbassert.expression.Filter;
import com.github.cysong.dbassert.expression.Order;
import com.github.cysong.dbassert.expression.TextFilter;
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
        if (Utils.isNotEmpty(assertion.getFilters())) {
            for (AbstractFilter filter : assertion.getFilters()) {
                sb.append(" and ");
                if (filter instanceof TextFilter) {
                    sb.append(((TextFilter) filter).getExpression());
                } else if (filter instanceof Filter) {
                    Filter f = (Filter) filter;
                    sb.append(f.getColumnName());
                    String value = wrapFilterValue(f.getValue());
                    switch (f.getComparator()) {
                        case EQUAL:
                            sb.append("=").append(value);
                            break;
                        case NOT_EQUAL:
                            sb.append("!=").append(value);
                            break;
                        case NULL:
                            sb.append(" is null");
                            break;
                        case NOT_NULL:
                            sb.append(" is not null");
                            break;
                        case IS_TRUE:
                            sb.append("=").append("true");
                            break;
                        case IS_FALSE:
                            sb.append("=").append("false");
                        case IN:
                            assert value != null;
                            sb.append(" in ").append(value);
                            break;
                        case NOT_IN:
                            assert value != null;
                            sb.append("not in ").append(value);
                        case GREATER_THAN:
                            sb.append(">").append(value);
                            break;
                        case GREATER_THAN_OR_EQUAL:
                            sb.append(">=").append(value);
                            break;
                        case LESS_THAN:
                            sb.append("<").append(value);
                            break;
                        case LESS_THAN_OR_EQUAL:
                            sb.append("<=").append(value);
                            break;
                        case CONTAINS:
                            value = "'%" + Utils.trim(value, '\'') + "%'";
                            sb.append(" like ").append(value);
                        case NOT_CONTAIN:
                            value = "'%" + Utils.trim(value, '\'') + "%'";
                            sb.append(" not like ").append(value);
                        default:
                            throw new ConfigurationException("Unsupported comparator:" + f.getComparator().name());
                    }
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
