package appledog.stream.base.database.interfaces;

import java.io.Serializable;
import java.sql.SQLException;

public interface DatabaseClient extends Serializable {
    void onDisable();
    String callProduce(String produceName, String action, String input) throws SQLException;
}
