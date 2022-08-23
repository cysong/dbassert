package com.github.cysong.dbassert.object;

import com.github.cysong.dbassert.constant.ObjectType;

/**
 * database object column
 *
 * @author cysong
 * @date 2022/08/22 15:50
 **/
public class Column extends DbObject {

    public static Column create(String name) {
        return new Column(name, null);
    }

    public static Column create(String name, String alias) {
        return new Column(name, alias);
    }

    private Column(String name, String alias) {
        super(ObjectType.COLUMN, name, alias);
    }
}
