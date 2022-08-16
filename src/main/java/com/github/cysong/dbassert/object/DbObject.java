package com.github.cysong.dbassert.object;

import com.github.cysong.dbassert.constant.ObjectType;

public abstract class DbObject {
    private ObjectType type;
    private String name;
    private String alias;

    public DbObject(ObjectType type, String name, String alias) {
        this.type = type;
        this.name = name;
        this.alias = alias;
    }

    public ObjectType getType() {
        return type;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getAlias() {
        return alias;
    }

    public void setAlias(String alias) {
        this.alias = alias;
    }
}
