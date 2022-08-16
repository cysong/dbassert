package com.github.cysong.dbassert.object;

import com.github.cysong.dbassert.constant.ObjectType;

public class Database extends DbObject {

    public static Database create(String name) {
        return new Database(name, null);
    }

    public static Database create(String name, String alias) {
        return new Database(name, alias);
    }

    private Database(String name, String alias) {
        super(ObjectType.DATABASE, name, alias);
    }
}
