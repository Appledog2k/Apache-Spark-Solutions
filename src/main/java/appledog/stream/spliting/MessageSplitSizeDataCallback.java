package appledog.stream.spliting;

import java.io.Serializable;

public interface MessageSplitSizeDataCallback extends Serializable {
    void onCompletion(String message);
}
