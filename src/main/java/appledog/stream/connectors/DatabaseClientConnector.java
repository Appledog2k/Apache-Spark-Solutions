package appledog.stream.connectors;

import appledog.stream.base.database.services.DatabaseClientService;
import appledog.stream.utils.PropertiesFileReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.crypto.NoSuchPaddingException;
import java.io.Serializable;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.spec.InvalidKeySpecException;
import java.util.Properties;

public class DatabaseClientConnector implements Serializable {
    private static final Logger logger = LoggerFactory.getLogger(DatabaseClientConnector.class);
    private final Properties properties;
    private DatabaseClientService databaseClientService = null;

    public DatabaseClientConnector(String configPath) {
        this.properties = PropertiesFileReader.readConfig(configPath);
        ;
    }

    public DatabaseClientService getDatabaseClientService() throws NoSuchPaddingException, NoSuchAlgorithmException, InvalidKeySpecException, InvalidKeyException {
        if (databaseClientService == null) {
            databaseClientService = new DatabaseClientService(properties);
            logger.info("create new DatabaseClientService success");
        }
        return databaseClientService;
    }

    public void close() {
        if (databaseClientService != null) {
            databaseClientService.onDisable();
            logger.info("disable DatabaseClientService success");
        }
    }
}
