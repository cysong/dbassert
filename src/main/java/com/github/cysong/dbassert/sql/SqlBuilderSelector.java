package com.github.cysong.dbassert.sql;

import com.github.cysong.dbassert.assertion.Assertion;
import com.github.cysong.dbassert.exception.SqlBuilderNotFoundException;
import com.github.cysong.dbassert.utitls.SqlUtils;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.ServiceLoader;

/**
 * factory for generate sqlbuilder by database product name
 *
 * @author cysong
 * @date 2022/08/22 15:50
 **/
public class SqlBuilderSelector {
    private static final ServiceLoader<SqlBuilderFactory> loader = ServiceLoader.load(SqlBuilderFactory.class);
    private static final Map<String, SqlBuilderFactory> FACTORY_MAP = new HashMap<>();

    public static SqlBuilder getSqlBuilder(Assertion assertion) {
        String dbProductName = SqlUtils.getDatabaseProductName(assertion.getConn());
        SqlBuilderFactory factory = FACTORY_MAP.get(dbProductName);
        if (factory == null) {
            factory = getSqlBuilderFactory(dbProductName);
        }
        return factory.newInstance(assertion);
    }

    private static synchronized SqlBuilderFactory getSqlBuilderFactory(String dbProductName) {
        if (FACTORY_MAP.containsKey(dbProductName)) {
            return FACTORY_MAP.get(dbProductName);
        }
        Iterator<SqlBuilderFactory> it = loader.iterator();
        while (it.hasNext()) {
            SqlBuilderFactory factory = it.next();
            if (factory.matches(dbProductName)) {
                FACTORY_MAP.put(dbProductName, factory);
                return factory;
            }
        }
        throw new SqlBuilderNotFoundException(String.format("SqlBuilderFactory for database %s not found", dbProductName));
    }

}
