package appledog.stream.base.database.call;

import appledog.stream.base.database.interfaces.DatabaseClient;
import appledog.stream.utils.StreamingUtils;
import appledog.stream.utils.StringConstants;
import com.google.gson.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.sql.SQLException;

public class CallExecuteDatabaseClient implements Serializable {
    private static final Logger logger = LoggerFactory.getLogger(CallExecuteDatabaseClient.class);

    public static String callProduce(DatabaseClient databaseClient, JsonObject json) throws SQLException {
        String action = json.getAsJsonObject(StringConstants.USER_DATA).get(StringConstants.MY_ACTION).getAsString();
        JsonObject jsonUserData = json.getAsJsonObject(StringConstants.USER_DATA);
        String produceName = "begin " + StringConstants.PR_OUTPUT_SERVICES + "(?,?,?); end;";

        long startTime = System.currentTimeMillis();
        String responseData = databaseClient.callProduce(produceName, action, json.toString());

        logger.info("callProduce:success callProduce:{}", System.currentTimeMillis() - startTime);
        JsonObject jsonObject = new JsonObject();
        jsonObject.add(StringConstants.USER_HEADER, jsonUserData);
        jsonObject.add(StringConstants.DATA, StreamingUtils.toJsonArray(responseData));
        return jsonObject.toString();
    }
}
