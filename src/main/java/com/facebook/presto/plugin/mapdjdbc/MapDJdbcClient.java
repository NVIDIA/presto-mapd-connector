package com.facebook.presto.plugin.mapdjdbc;

import com.facebook.presto.plugin.jdbc.*;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.SchemaNotFoundException;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.TableNotFoundException;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.weakref.jmx.internal.guava.base.Throwables;

import javax.annotation.Nullable;
import javax.inject.Inject;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.google.common.collect.Iterables.getOnlyElement;

/**
 * Created by rohitkulkarni on 7/14/2017.
 */
public class MapDJdbcClient extends BaseJdbcClient {


    @Inject
    public MapDJdbcClient(JdbcConnectorId connectorId, BaseJdbcConfig config,
                          MapDJdbcConfig hiveJdbcConfig) throws SQLException {
        super(connectorId, config, "", new MapDDriver2());


    }


    @Override
    public Set<String> getSchemaNames() {

        Connection connection = null;
        try {
            connection = driver.connect(connectionUrl, connectionProperties);
            ResultSet resultSet = connection.getMetaData().getSchemas();
            ImmutableSet.Builder<String> schemaNames = ImmutableSet.builder();
            while (resultSet.next()) {
                String schemaName = resultSet.getString(1).toLowerCase();
                schemaNames.add(schemaName);
            }
            return schemaNames.build();
        } catch (SQLException ex) {
            throw Throwables.propagate(ex);
        } finally {
            try {
                connection.close();
            } catch (SQLException e) {
                // Nothing
            }
        }
    }


    protected ResultSet getTables(Connection connection, String schemaName,
                                  String tableName) throws SQLException {
        return connection.getMetaData().getTables(null, schemaName, tableName, new String[] {"VIEW", "TABLE", "SYNONYM"});
    }


    @Override
    protected SchemaTableName getSchemaTableName(ResultSet resultSet)
            throws SQLException {
//        String tableSchema = resultSet.getString("TABLE_SCHEM");
        String tableSchema = "mapd";
        String tableName = resultSet.getString("TABLE_NAME");
        if (tableSchema != null) {
            tableSchema = tableSchema.toLowerCase();
        }
        if (tableName != null) {
            tableName = tableName.toLowerCase();
        }
        return new SchemaTableName(tableSchema, tableName);
    }


    @Override
    public List<SchemaTableName> getTableNames(@Nullable String schema) {

        Connection connection = null;
        try {
            connection = driver.connect(connectionUrl, connectionProperties);
            DatabaseMetaData metadata = connection.getMetaData();
            ResultSet resultSet = getTables(connection, schema, null);
            ImmutableList.Builder<SchemaTableName> list = ImmutableList.builder();
            while (resultSet.next()) {
                list.add(getSchemaTableName(resultSet));
            }
            return list.build();
        } catch (SQLException e) {
            throw Throwables.propagate(e);
        }finally {
            if(connection!=null){
                try {
                    connection.close();
                } catch (SQLException e) {
                    // Nothing
                }
            }
        }
    }


    @Override
    public List<JdbcColumnHandle> getColumns(JdbcTableHandle tableHandle) {

        System.out.println("RK -- GETTING COLUMNS!");

        Connection connection = null;
        List<JdbcColumnHandle> columns = new ArrayList<JdbcColumnHandle>();
        try {
            connection = driver.connect(connectionUrl, connectionProperties);
            DatabaseMetaData metadata = connection.getMetaData();
            if(tableHandle!=null){
                if(tableHandle.getSchemaName()==null){
                    throw new SchemaNotFoundException("No schema name!");
                }
                String schemaName = tableHandle.getSchemaName();
                String tableName = tableHandle.getTableName();
                ResultSet resultSet = metadata.getColumns(null, schemaName, tableName, null);
                boolean found = false;
                while (resultSet.next()) {
                    found = true;
                    Type columnType = toPrestoType(resultSet.getInt("DATA_TYPE"), resultSet.getInt("COLUMN_SIZE"));
                    if (columnType != null) {
                        String columnName = resultSet.getString("COLUMN_NAME");
                        columns.add(new JdbcColumnHandle(connectorId, columnName, columnType));
                        System.out.println("RK -- Column Found: " + columnName);
                    }
                }
                if (!found) {
                    System.out.println("RK -- Issue Here??");
                    throw new TableNotFoundException(tableHandle.getSchemaTableName());
                }
                if (columns.isEmpty()) {
                    throw new PrestoException(NOT_SUPPORTED, String.format("Table has no supported column types: %s", tableHandle.getSchemaTableName()));
                }
            }
        } catch (SQLException e) {
            throw Throwables.propagate(e);
        }finally {
            if(connection!=null){
                try {
                    connection.close();
                } catch (SQLException e) {
                    // Nothing
                }
            }
        }
        System.out.println("RK -- Returning Columnms");
        return ImmutableList.copyOf(columns);
    }


    @Nullable
    @Override
    public JdbcTableHandle getTableHandle(SchemaTableName schemaTableName) {
        Connection connection = null;
        try{
            connection = driver.connect(connectionUrl, connectionProperties);
            DatabaseMetaData metadata = connection.getMetaData();
            String jdbcSchemaName = schemaTableName.getSchemaName();
            String jdbcTableName = schemaTableName.getTableName();
            ResultSet resultSet = getTables(connection, jdbcSchemaName, jdbcTableName);
            List<JdbcTableHandle> tableHandles = new ArrayList<JdbcTableHandle>();

            while (resultSet.next()) {
                System.out.println("RK -- INSIDE GET TABLE HANDLE for : " + jdbcTableName);
                System.out.println("RK -- TABLE_NAME: " + resultSet.getString("TABLE_NAME"));

                if (jdbcTableName.equals(resultSet.getString("TABLE_NAME"))) {
                    System.out.println("RK -- FOUND TABLE: " + jdbcTableName);
                    tableHandles.add(
                            new JdbcTableHandle(
                                    connectorId,
                                    schemaTableName,
//                                resultSet.getString("TABLE_CAT"),
                                    null,
//                                resultSet.getString("TABLE_SCHEM"),
                                    "mapd",
                                    resultSet.getString("TABLE_NAME")
                            )
                    );
                }
            }
            if (tableHandles.isEmpty()) {
                return null;
            }
            if (tableHandles.size() > 1) {
                throw new PrestoException(NOT_SUPPORTED,
                        "Multiple tables matched: " + schemaTableName);
            }
            return getOnlyElement(tableHandles);
        } catch (SQLException e) {
            throw Throwables.propagate(e);
        } finally {
            if(connection!=null){
                try {
                    connection.close();
                } catch (SQLException e) {
                    // Nothing
                }
            }
        }
    }


}
