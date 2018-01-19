package com.facebook.presto.plugin.mapdjdbc;

import io.airlift.configuration.Config;

/**
 * Created by rohitkulkarni on 7/14/2017.
 */
public class MapDJdbcConfig {
    private String user;
    private String password;

    public String getUser() {
        return user;
    }

    @Config("user")
    public MapDJdbcConfig setUser(String user) {
        this.user = user;
        return this;
    }

    public String getPassword() {
        return password;
    }

    @Config("password")
    public MapDJdbcConfig setPassword(String password) {
        this.password = password;
        return this;
    }

}
