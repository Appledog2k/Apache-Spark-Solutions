package appledog.stream.spliting;

import appledog.stream.utils.StringConstants;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import org.json.JSONException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class SplitOffsetSizeRecord implements Serializable {
    private static final Logger logger = LoggerFactory.getLogger(SplitOffsetSizeRecord.class);

    private final int offsetBatchSize;
    private final int offsetStreamSize;

    public SplitOffsetSizeRecord(int offsetBatchSize, int offsetStreamSize) {
        this.offsetBatchSize = offsetBatchSize;
        this.offsetStreamSize = offsetStreamSize;
    }

    public JsonArray splitOffsetSizeRecord(String type, JsonObject originalJson) throws JSONException {
        String paging = originalJson.get(StringConstants.PAGING).getAsString();
        JsonObject newAction = new JsonObject();
        JsonArray actionArray = new JsonArray();
        try {
            newAction.addProperty("Select", originalJson.get(StringConstants.SELECT).getAsString());
            newAction.addProperty("Where", originalJson.get(StringConstants.WHERE).getAsString());
            newAction.addProperty("GroupBy", originalJson.get(StringConstants.GROUP_BY).getAsString());
            newAction.addProperty("Having", originalJson.get(StringConstants.HAVING).getAsString());
            newAction.addProperty("OrderBy", originalJson.get(StringConstants.ORDER_BY).getAsString());

            Pattern pattern = Pattern.compile("OFFSET (.*?) ROWS FETCH NEXT (.*?) ROWS ONLY ");
            Matcher matcher = pattern.matcher(paging.toUpperCase());
            if (matcher.find()){
                int offset = Integer.parseInt(matcher.group(1));
                int limit = Integer.parseInt(matcher.group(2));

                if(StringConstants.ON_DEMAND_STREAM.equals(type)){
                    newAction.addProperty(StringConstants.PAGING,
                            String.format(StringConstants.FORMAT_PAGING, offset,
                                    Math.min(limit, offsetStreamSize)));
                    JsonObject clone = deepCopy(newAction);
                    actionArray.add(clone);
                    return actionArray;
                }
                if(limit < offsetBatchSize){
                    newAction.addProperty(StringConstants.PAGING, paging);
                    actionArray.add(newAction);
                    return actionArray;
                }
                int numParts = limit / offsetBatchSize;
                int remainingRows = limit % offsetBatchSize;
                for(int i = 0; i < numParts; i++){
                    newAction.addProperty(StringConstants.PAGING,
                            String.format(StringConstants.FORMAT_PAGING, offset + i * offsetBatchSize, offsetBatchSize));
                    JsonObject clone = deepCopy(newAction);
                    actionArray.add(clone);
                }
                if(remainingRows > 0){
                    newAction.addProperty(StringConstants.PAGING,
                            String.format(StringConstants.FORMAT_PAGING, offset + numParts * offsetBatchSize, remainingRows));
                    JsonObject clone = deepCopy(newAction);
                    actionArray.add(clone);
                }
                logger.debug("new json after check offset: {}", actionArray);
            }else {
                newAction.addProperty(StringConstants.PAGING, String.format(StringConstants.FORMAT_PAGING,
                        StringConstants.ON_DEMAND_BATCH.equals(type) ? offsetBatchSize : offsetStreamSize));
                actionArray.add(newAction);
            }
            return actionArray;
        } catch (Exception ex) {
            logger.error("Check your request Query {}", ex.getMessage());
            return new JsonArray();
        }
    }
    public static JsonObject deepCopy(JsonObject jsonObject) {
        JsonObject result = new JsonObject();

        for (Map.Entry<String, JsonElement> entry : jsonObject.entrySet()) {
            result.addProperty(entry.getKey(), entry.getValue().getAsString());
        }

        return result;
    }
}
