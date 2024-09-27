package appledog.stream.base.database.services;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import java.sql.Connection;
import java.sql.SQLException;

public class DbConnectionFactory {
    private final HikariDataSource hikariDataSource;

    public DbConnectionFactory(DbConnectionBuilder builder) {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(builder.connectionString);
        config.setUsername(builder.user);
        config.setPassword(builder.password);
        config.setDriverClassName(builder.driver);
        config.setMinimumIdle(builder.minimumIdle);
        config.setMaximumPoolSize(builder.poolSize);
        config.addDataSourceProperty("keepaliveTime", 60000);
        config.addDataSourceProperty("cachePrepStmts", builder.cachePrepStmts);
        config.addDataSourceProperty("prepStmtCacheSize", builder.prepStmtsCacheSize);
        config.addDataSourceProperty("prepStmtCacheSqlLimit", builder.prepStmtsCacheSqlLimit);
        this.hikariDataSource = new HikariDataSource(config);

        Runtime.getRuntime().addShutdownHook(new Thread(hikariDataSource::close));
    }

    public void destroy() {
        hikariDataSource.close();
    }

    public Connection getConnection() throws SQLException {
        return hikariDataSource.getConnection();
    }

    public static class DbConnectionBuilder{
        private String connectionString;
        private String user;
        private String password;
        private String driver;
        private int  minimumIdle;
        private int poolSize;
        private boolean cachePrepStmts;
        private int prepStmtsCacheSize;
        private int prepStmtsCacheSqlLimit;

        public DbConnectionBuilder setConnectionString(String connectionString) {
            this.connectionString = connectionString;
            return this;
        }

        public DbConnectionBuilder setUser(String user) {
            this.user = user;
            return this;
        }
        public DbConnectionBuilder setPassword(String password) {
            this.password = password;
            return this;
        }

        public DbConnectionBuilder setDriver(String driver) {
            this.driver = driver;
            return this;
        }

        public DbConnectionBuilder setMinimumIdle(int minimumIdle) {
            this.minimumIdle = minimumIdle;
            return this;
        }

        public DbConnectionBuilder setPoolSize(int poolSize) {
            this.poolSize = poolSize;
            return this;
        }

        public DbConnectionBuilder setCachePrepStmts(boolean cachePrepStmts){
            this.cachePrepStmts = cachePrepStmts;
            return this;
        }

        public DbConnectionBuilder setPrepStmtsCacheSqlLimit(int prepStmtsCacheSqlLimit) {
            this.prepStmtsCacheSqlLimit = prepStmtsCacheSqlLimit;
            return this;
        }

        public DbConnectionBuilder setPrepStmtsCacheSize(int prepStmtsCacheSize) {
            this.prepStmtsCacheSize = prepStmtsCacheSize;
            return this;
        }

        public DbConnectionFactory build(){
            return new DbConnectionFactory(this);
        }
    }
}
