package com.github.cysong.dbassert.datasource;

/**
 * basic info for connecting a database
 *
 * @author cysong
 * @date 2022/08/22 15:50
 **/
public class DatabaseConfig {
    private String key;
    private String driver;
    private String url;
    private String username;
    private String password;

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public String getDriver() {
        return driver;
    }

    public void setDriver(String driver) {
        this.driver = driver;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }
}
