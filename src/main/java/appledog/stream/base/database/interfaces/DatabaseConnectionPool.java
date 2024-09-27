package appledog.stream.base.database.interfaces;

import appledog.stream.base.database.utils.DatabaseType;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.SQLException;

public interface DatabaseConnectionPool extends Serializable {
    void onEnable();
    void onDisable();
    Connection getConnection() throws SQLException;
    DatabaseType getDatabaseType();
}
