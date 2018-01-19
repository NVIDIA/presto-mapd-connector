package com.facebook.presto.plugin.mapdjdbc;

import com.facebook.presto.plugin.mapdjdbc.MapDConnection2;
import com.mapd.jdbc.MapDDriver;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

/**
 * Modified by rohitkulkarni on 7/25/2017.
 */
public class MapDDriver2 extends MapDDriver {

    @Override
    public Connection connect(String url, Properties info) throws SQLException { //logger.debug("Entered");
        if (!isValidURL(url)) {
            return null;
        }

        url = url.trim();
        return new MapDConnection2(url, info);
    }

    private static boolean isValidURL(String url) {
        return url != null && url.toLowerCase().startsWith(PREFIX);
    }

}
