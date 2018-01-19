package com.facebook.presto.plugin.mapdjdbc;

import com.mapd.jdbc.MapDConnection;
import com.mapd.thrift.server.TMapDException;
import com.mapd.thrift.server.TServerStatus;
import org.apache.thrift.TException;

import com.mapd.jdbc.MapDStatement;

import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

/**
 * Modified by rohitkulkarni on 7/25/2017.
 */
public class MapDConnection2 extends MapDConnection {
    public MapDConnection2(String url, Properties info) throws SQLException {
        super(url, info);
    }

    @Override
    public void setReadOnly(boolean readOnly) throws SQLException {

        System.out.println("RK -- SETTING READ ONLY!");
        try {
            TServerStatus server_status = client.get_server_status(session);
            server_status.setRead_only(true);
        } catch (TException ex) {
            System.out.println("RK -- Exception when setting read only");
        }

    }

    @Override
    public boolean isReadOnly() throws SQLException {
        System.out.println("RK -- GETTING READ ONLY!");
        try {
            if (session != null) {
                TServerStatus server_status = client.get_server_status(session);
                System.out.println("RK -- RETURNING READ ONLY = " + server_status.read_only);
                return server_status.read_only;
            }
        } catch (TMapDException ex) {
            throw new SQLException("get_server_status failed during isReadOnly check." + ex.toString());
        } catch (TException ex) {
            throw new SQLException("get_server_status failed during isReadOnly check." + ex.toString());
        }
        // never should get here
        return true;
    }

//    @Override
//    public Statement createStatement() throws SQLException {
//        return new MapDStatement(this.session, this.client);
//    }


}
