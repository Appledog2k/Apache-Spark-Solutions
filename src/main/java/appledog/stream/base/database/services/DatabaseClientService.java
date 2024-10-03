package appledog.stream.base.database.services;

import appledog.stream.base.api.interfaces.PropertyContext;
import appledog.stream.base.database.interfaces.DatabaseAction;
import appledog.stream.base.database.interfaces.DatabaseClient;
import appledog.stream.base.database.interfaces.DatabaseConnectionPool;
import appledog.stream.base.database.utils.DatabaseUtils;
import appledog.stream.base.database.utils.DbStandardProcessContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.crypto.NoSuchPaddingException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.spec.InvalidKeySpecException;
import java.sql.*;
import java.util.Properties;

public class DatabaseClientService implements DatabaseClient {
    private static final Logger logger = LoggerFactory.getLogger(DatabaseClientService.class);
    private DatabaseConnectionPool databaseConnectionPool;

    public DatabaseClientService(Properties properties) throws NoSuchPaddingException, NoSuchAlgorithmException, InvalidKeySpecException, InvalidKeyException {
        initContext(properties);
    }

    private void initContext(Properties properties) throws NoSuchPaddingException, NoSuchAlgorithmException, InvalidKeySpecException, InvalidKeyException {
        PropertyContext propertyContext = new DbStandardProcessContext(properties);
        onEnableDbPool(propertyContext);
    }

    private void onEnableDbPool(PropertyContext propertyContext) {
        this.databaseConnectionPool = new DbConnectionPoolService(propertyContext);
        this.databaseConnectionPool.onEnable();
    }

    @Override
    public void onDisable() {
        databaseConnectionPool.onDisable();
    }

    @Override
    public String callProduce(String produceName, String action, String input) throws SQLException {
        return withConnection(connection -> {
            Clob clob = null;
            CallableStatement stmt = null;
            long startTime = System.currentTimeMillis();
            try {
                logger.info("performClob:Prepare Call at: {}", startTime);

                stmt = connection.prepareCall(produceName);
                stmt.setString(1, action);

                DatabaseUtils.setClobAsString(stmt, input);
                logger.info("performClob:After setClobAsString at: {}", System.currentTimeMillis() - startTime);
                stmt.registerOutParameter(3, Types.CLOB);

                logger.info("performClob:Begin execute at:{}", System.currentTimeMillis() - startTime);

                stmt.execute();
                logger.info("performClob:End execute at:{}", System.currentTimeMillis() - startTime);
                clob = stmt.getClob(3);
                logger.info("performClob:Got Clob at:{}", System.currentTimeMillis() - startTime);

            } catch (SQLException e) {
                logger.error("performClob:my Json error:{} ~ at level: {}", e.getMessage(), 0);
            } finally {
                if (stmt != null) {
                    stmt.close();
                }
            }
            String strConvert = DatabaseUtils.convertclobToString(clob, logger);
            logger.info("performClob:After convertclobToString at:{}", System.currentTimeMillis() - startTime);
            return strConvert;
        });
    }


    private <T> T withConnection(final DatabaseAction<T> action) throws SQLException {
        Connection connection = null;
        try {
            connection = databaseConnectionPool.getConnection();
            return action.execute(connection);
        } finally {
            if (connection != null) {
                try {
                    connection.close();
                } catch (Exception e) {
                    logger.warn("Error closing connection: " + e.getMessage(), e);
                }
            }
        }
    }


}
