package com.facebook.presto.plugin.mapdjdbc;


import com.facebook.presto.plugin.jdbc.BaseJdbcConfig;
import com.facebook.presto.plugin.jdbc.JdbcClient;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Scopes;

import static io.airlift.configuration.ConfigBinder.configBinder;


/**
 * Created by rohitkulkarni on 7/14/2017.
 */
public class MapDJdbcClientModule implements Module {

    public void configure(Binder binder) {
        binder.bind(JdbcClient.class).to(MapDJdbcClient.class).in(Scopes.SINGLETON);
        configBinder(binder).bindConfig(BaseJdbcConfig.class);
        configBinder(binder).bindConfig(MapDJdbcConfig.class);
    }
}
