package appledog.stream.base.database.interfaces;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.SQLException;

public interface DatabaseAction<T> extends Serializable {
    T execute(Connection connection) throws SQLException;
}
