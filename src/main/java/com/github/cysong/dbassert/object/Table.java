package com.github.cysong.dbassert.object;

import com.github.cysong.dbassert.constant.ObjectType;

/**
 * database object table
 *
 * @author cysong
 * @date 2022/08/22 15:50
 **/
public class Table extends DbObject {

    public static Table create(String name) {
        return new Table(name, null);
    }

    public static Table create(String name, String alias) {
        return new Table(name, alias);
    }

    private Table(String name, String alias) {
        super(ObjectType.TABLE, name, alias);
    }
}
