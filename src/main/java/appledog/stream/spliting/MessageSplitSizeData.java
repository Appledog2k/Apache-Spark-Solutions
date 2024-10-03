package appledog.stream.spliting;

import appledog.stream.utils.StringConstants;
import org.apache.commons.lang.StringUtils;
import org.json.JSONArray;
import org.json.JSONObject;
import org.json4s.jackson.Json;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;


public class MessageSplitSizeData implements Serializable {
    private static final Logger logger = LoggerFactory.getLogger(MessageSplitSizeData.class);
    private final int sizeMessage;

    public MessageSplitSizeData(int sizeMessage) {
        this.sizeMessage = sizeMessage;
    }

    public void standardMessage(String message, MessageSplitSizeDataCallback callback) {
        if (StringUtils.isNotBlank(message)) {
            callback.onCompletion(message);
        } else {
            JSONArray parent = new JSONArray(message);
            for (int i = 0; i < parent.length(); i++) {
                JSONArray data = parent.getJSONObject(i).getJSONArray(StringConstants.DATA);
                JSONArray fieldList = parent.getJSONObject(i).getJSONArray(StringConstants.FIELD_LIST);
                int numElmOfData = data.length();
                int sizeOfData = String.valueOf(data).getBytes().length;
                int numSplit = (sizeOfData / sizeMessage) + 1;
                logger.info("number split {}", numSplit);
                int batchSizeMessage = numElmOfData / numSplit;
                logger.info("number batchSizeMessage {}", batchSizeMessage);
                while (!data.isEmpty()) {
                    logger.info("length data {}", data.length());
                    JSONArray jsonArray = new JSONArray();
                    JSONObject dataItem = new JSONObject();
                    dataItem.put(StringConstants.FIELD_LIST, fieldList);
                    JSONArray dataSub = new JSONArray();
                    for (int idx = 0; idx < batchSizeMessage; idx++) {
                        if (!data.isEmpty()) {
                            dataSub.put(data.get(0));
                            data.remove(0);
                        } else {
                            break;
                        }
                    }
                    dataItem.put(StringConstants.DATA, dataSub);
                    jsonArray.put(dataItem);
                    callback.onCompletion(String.valueOf(jsonArray));
                }
            }
        }
    }
}
