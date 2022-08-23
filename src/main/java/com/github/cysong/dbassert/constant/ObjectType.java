package com.github.cysong.dbassert.constant;

import com.github.cysong.dbassert.object.Column;
import com.github.cysong.dbassert.object.Database;
import com.github.cysong.dbassert.object.DbObject;
import com.github.cysong.dbassert.object.Table;

/**
 * database object type
 *
 * @author cysong
 * @date 2022/08/22 15:50
 **/
public enum ObjectType {
    DATABASE, TABLE, COLUMN;

    ObjectType() {
    }

    public DbObject newObjectInstance(String name) {
        switch (this) {
            case DATABASE:
                return Database.create(name);
            case TABLE:
                return Table.create(name);
            case COLUMN:
                return Column.create(name);
            default:
                throw new IllegalArgumentException("Unrecognized db object:" + this.name());
        }
    }
}
