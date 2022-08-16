package com.github.cysong.dbassert.sql;

import com.github.cysong.dbassert.expression.AggregateCondition;
import com.github.cysong.dbassert.expression.Condition;
import com.github.cysong.dbassert.utitls.Utils;

import java.util.List;
import java.util.Map;

public class SqlResult {

    private Map<String, List<Condition>> columns;
    private Map<String, List<AggregateCondition>> aggColumns;
    private List<String> wrapAggColumns;
    private String detailSql;
    private String aggregateSql;


    public static SqlResult create() {
        return new SqlResult();
    }

    public SqlResult() {
    }

    public Map<String, List<Condition>> getColumns() {
        return columns;
    }

    public void setColumns(Map<String, List<Condition>> columns) {
        this.columns = columns;
    }

    public Map<String, List<AggregateCondition>> getAggColumns() {
        return aggColumns;
    }

    public void setAggColumns(Map<String, List<AggregateCondition>> aggColumns) {
        this.aggColumns = aggColumns;
    }

    public List<String> getWrapAggColumns() {
        return wrapAggColumns;
    }

    public void setWrapAggColumns(List<String> wrapAggColumns) {
        this.wrapAggColumns = wrapAggColumns;
    }

    public String getDetailSql() {
        return detailSql;
    }

    public void setDetailSql(String detailSql) {
        assert Utils.isNotBlank(detailSql);
        this.detailSql = detailSql;
    }

    public String getAggregateSql() {
        return aggregateSql;
    }

    public void setAggregateSql(String aggregateSql) {
        assert Utils.isNotBlank(aggregateSql);
        this.aggregateSql = aggregateSql;
    }

    public boolean hasNormalSql() {
        return this.detailSql != null;
    }

    public boolean hasCountSql() {
        return this.aggregateSql != null;
    }
}
