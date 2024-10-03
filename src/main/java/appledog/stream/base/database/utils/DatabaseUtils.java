package appledog.stream.base.database.utils;

import appledog.stream.base.api.interfaces.PropertyContext;
import appledog.stream.base.api.standard.AllowableValue;
import appledog.stream.base.api.standard.PropertyDescriptor;
import appledog.stream.base.database.services.DbConnectionFactory;
import org.slf4j.Logger;

import java.io.*;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.SQLException;

public class DatabaseUtils implements Serializable {

    public static final AllowableValue DB_MODE_ORACLE = new AllowableValue(DatabaseType.ORACLE.getDisplayName(),
            DatabaseType.ORACLE.getDisplayName(), DatabaseType.ORACLE.getDescription());

    public static final AllowableValue DB_MODE_MYSQL = new AllowableValue(DatabaseType.MYSQL.getDisplayName(),
            DatabaseType.MYSQL.getDisplayName(), DatabaseType.MYSQL.getDescription());

    public static final AllowableValue DB_MODE_POSTGRES = new AllowableValue(DatabaseType.POSTGRES.getDisplayName(),
            DatabaseType.POSTGRES.getDisplayName(), DatabaseType.POSTGRES.getDescription());

    public static final AllowableValue DB_MODE_SQLSERVER = new AllowableValue(DatabaseType.SQLSERVER.getDisplayName(),
            DatabaseType.SQLSERVER.getDisplayName(), DatabaseType.SQLSERVER.getDescription());

    public static final PropertyDescriptor DATABASE_TYPE = new PropertyDescriptor.Builder()
            .name("Database Type")
            .displayName("Database Type")
            .description("The type of Database being communicated with - oracle, mysql, postgres, or sql server.")
            .allowableValues(DB_MODE_ORACLE, DB_MODE_MYSQL, DB_MODE_POSTGRES, DB_MODE_SQLSERVER)
            .defaultValue(DB_MODE_ORACLE.getValue()).required(true).build();


    public static final PropertyDescriptor CONNECTION_STRING = new PropertyDescriptor.Builder()
            .name("Connection String")
            .displayName("Connection String")
            .description("The connection string for Database." +
                    "In a Oracle instance this value will be of the form jdbc:oracle:... " +
                    "In a Mysql instance this value will be of the form jdbc:mysql:... " +
                    "In a Postgres instance this value will be of the form jdbc:postgres:..." +
                    "In a Sql Server instance this value will be of the form jdbc:sqlserver:...")
            .required(true).build();

    public static final PropertyDescriptor DRIVER = new PropertyDescriptor.Builder()
            .name("Driver")
            .displayName("Driver")
            .description("The Driver class.")
            .required(true).build();

    public static final PropertyDescriptor USER = new PropertyDescriptor.Builder()
            .name("User Name")
            .displayName("User Name")
            .description("User Name connect to Database ")
            .required(true).build();

    public static final PropertyDescriptor PASSWORD = new PropertyDescriptor.Builder()
            .name("Password")
            .displayName("Password")
            .description("Password connect to Database ")
            .required(true).build();

    public static final PropertyDescriptor MINIMUM_IDLE = new PropertyDescriptor.Builder()
            .name("Minimum Idle")
            .displayName("Minimum Idle")
            .description("Min section connect to database ")
            .required(true).build();

    public static final PropertyDescriptor POOL_SIZE = new PropertyDescriptor.Builder()
            .name("Pool Size")
            .displayName("Pool Size")
            .description("Max pool connection section to database ")
            .required(true).build();

    public static final PropertyDescriptor CACHE_PREP_STMTS = new PropertyDescriptor.Builder()
            .name("Cache Prepare Statement")
            .displayName("Cache Prepare Statement")
            .description("Cache Prepare Statement section to database ")
            .required(true).build();

    public static final PropertyDescriptor PREP_STMTS_CACHE_SIZE = new PropertyDescriptor.Builder()
            .name("Cache Prepare Statement Size")
            .displayName("Cache Prepare Statement Size")
            .description("Cache Prepare Statement Size section to database ")
            .required(true).build();

    public static final PropertyDescriptor PREP_STMTS_CACHE_SQL_LIMIT = new PropertyDescriptor.Builder()
            .name("Cache Prepare Statement SQL Limit")
            .displayName("Cache Prepare Statement SQL Limit")
            .description("Cache Prepare Statement SQL Limit section to database ")
            .required(true).build();


    public static DbConnectionFactory createConnectionFactory(PropertyContext propertyContext, Logger logger) {
        String connectionString = propertyContext.getProperty(DatabaseUtils.CONNECTION_STRING).getValue();
        String driver = propertyContext.getProperty(DatabaseUtils.DRIVER).getValue();
        String user = propertyContext.getProperty(DatabaseUtils.USER).getValue();
        String password = propertyContext.getProperty(DatabaseUtils.PASSWORD).getValue();
        int minimumIdle = propertyContext.getProperty(DatabaseUtils.MINIMUM_IDLE).asInteger();
        int poolSize = propertyContext.getProperty(DatabaseUtils.POOL_SIZE).asInteger();
        boolean cachePrepStmts = propertyContext.getProperty(DatabaseUtils.CACHE_PREP_STMTS).asBoolean();
        int prepStmtsCacheSize = propertyContext.getProperty(DatabaseUtils.PREP_STMTS_CACHE_SIZE).asInteger();
        int prepStmtsCacheSqlLimit = propertyContext.getProperty(DatabaseUtils.PREP_STMTS_CACHE_SQL_LIMIT).asInteger();
        logger.info("Created DbConnectionFactory");
        return new DbConnectionFactory.DbConnectionBuilder()
                .setConnectionString(connectionString)
                .setUser(user)
                .setPassword(password)
                .setDriver(driver)
                .setMinimumIdle(minimumIdle)
                .setPoolSize(poolSize)
                .setCachePrepStmts(cachePrepStmts)
                .setPrepStmtsCacheSize(prepStmtsCacheSize)
                .setPrepStmtsCacheSqlLimit(prepStmtsCacheSqlLimit)
                .build();
    }

    public static String convertclobToString(Clob data, Logger logger) throws SQLException {
        if (data == null || data.length() == 0L) return "[]";
        final StringBuilder sb = new StringBuilder();
        try {
            final Reader reader = data.getCharacterStream();
            final BufferedReader br = new BufferedReader(reader);
            int b;
            while (-1 != (b = br.read())) {
                sb.append((char) b);
            }
            br.close();
        } catch (SQLException e) {
            logger.error("SQL. Could not convert CLOB to string {}",e.getMessage());
            return "";
        } catch (IOException e) {

            logger.error("IO. Could not convert CLOB to string {}", e.getMessage());
            return "";
        }
        return sb.toString();
    }

    public static void setClobAsString(CallableStatement ps, String content) throws SQLException {
        if (content != null) {
            ps.setClob(2, new StringReader(content), content.length());
        } else {
            ps.setClob(2, (Clob) null);
        }

    }
}
