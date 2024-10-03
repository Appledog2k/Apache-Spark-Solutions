package appledog.stream.base.api.serializer;

import appledog.stream.base.api.interfaces.Deserializer;
import appledog.stream.base.redis.exceptions.DeserializationException;

public class StringValueDeserializer implements Deserializer<String> {
    @Override
    public String deserialize(byte[] input) throws DeserializationException {
        if (input == null || input.length == 0) {
            return null;
        }
        return new String(input);
    }
}
