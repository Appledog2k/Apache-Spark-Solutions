package appledog.stream.base.database.services;

import appledog.stream.base.api.iface.PropertyContext;
import appledog.stream.base.database.interfaces.DatabaseConnectionPool;
import appledog.stream.base.database.utils.DatabaseType;
import appledog.stream.base.database.utils.DatabaseUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;

public class DbConnectionPoolService implements DatabaseConnectionPool {
    private static final Logger logger = LoggerFactory.getLogger(DbConnectionPoolService.class);
    private PropertyContext propertyContext;
    private transient DbConnectionFactory dbConnectionFactory;
    private DatabaseType databaseType;

    public DbConnectionPoolService(PropertyContext propertyContext) {
        this.propertyContext = propertyContext;
    }

    @Override
    public void onEnable() {
        String dbType = propertyContext.getProperty(DatabaseUtils.DATABASE_TYPE).getValue();
        this.databaseType = DatabaseType.fromDisplayName(dbType);
    }

    @Override
    public void onDisable() {
        if (dbConnectionFactory != null) {
            dbConnectionFactory.destroy();
            dbConnectionFactory = null;
            databaseType = null;
            propertyContext = null;
        }
    }

    @Override
    public Connection getConnection() throws SQLException {
        if (dbConnectionFactory == null) {
            synchronized (this) {
                dbConnectionFactory = DatabaseUtils.createConnectionFactory(propertyContext, logger);
            }
        }
        return dbConnectionFactory.getConnection();
    }

    @Override
    public DatabaseType getDatabaseType() {
        return databaseType;
    }
}
