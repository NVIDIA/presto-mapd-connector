package com.facebook.presto.plugin.mapdjdbc;

import com.facebook.presto.plugin.jdbc.JdbcPlugin;

/**
 * Created by rohitkulkarni on 7/14/2017.
 */
public class MapDJdbcPlugin extends JdbcPlugin {

    public MapDJdbcPlugin() {
        super("mapdjdbc", new MapDJdbcClientModule());
    }
}
