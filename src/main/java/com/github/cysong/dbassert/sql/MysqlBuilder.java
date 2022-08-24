package com.github.cysong.dbassert.sql;

import com.github.cysong.dbassert.assertion.Assertion;
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

    MysqlBuilder(Assertion assertion) {
        super(assertion);
    }

    @Override
    protected void buildSql() {
        //select and where statements
        StringBuilder sb = new StringBuilder("select %s from ");
        sb.append(getQuotedFullTableName());
        sb.append(" where 1");
        if (Utils.isNotEmpty(assertion.getFilters())) {
            for (AbstractFilter filter : assertion.getFilters()) {
                sb.append(" and ");
                buildFilterStatement(filter, sb);
            }
        }

        //order statements
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

        //limit statements
        sb.append(" limit ");
        if (assertion.getStartIndex() > 1) {
            sb.append(assertion.getStartIndex()).append(",");
        }
        sb.append(assertion.getPageSize());
        String sql = sb.toString();

        //build detail sql
        if (Utils.isNotEmpty(result.getColumnSet())) {
            result.setDetailSql(String.format(sql,
                    result.getColumnSet().stream().map(this::quotedIdentifier)
                            .collect(Collectors.joining(","))
            ));
        }

        //build aggregate sql
        StringBuilder aggStatement = new StringBuilder("count(*) ");
        aggStatement.append(Constants.COUNT_ROWS_LABEL);
        if (Utils.isNotEmpty(result.getWrapAggColumns())) {
            aggStatement.append(",").append(String.join(",", result.getWrapAggColumns()));
        }
        result.setAggregateSql(String.format(sql, aggStatement));
    }

    @Override
    public boolean match(String dbProductName) {
        return "MySQL".equalsIgnoreCase(dbProductName) || "Sqlite".equalsIgnoreCase(dbProductName);
    }

    @Override
    protected String getOpenQuote() {
        return "`";
    }

    @Override
    protected String getCloseQuote() {
        return "`";
    }

    private void buildFilterStatement(AbstractFilter filter, StringBuilder sb) {
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
                    break;
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
                    break;
                case NOT_CONTAIN:
                    value = "'%" + Utils.trim(value, '\'') + "%'";
                    sb.append(" not like ").append(value);
                    break;
                default:
                    throw new ConfigurationException("Unsupported comparator:" + f.getComparator().name());
            }
        } else {
            throw new ConfigurationException("Unsupported filter type:" + filter.getClass().getName());
        }
    }
}
