package appledog.stream.base.api.serializer;

import appledog.stream.base.api.interfaces.Serializer;
import appledog.stream.base.redis.exceptions.SerializationException;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;

public class StringSerializer implements Serializer<String> {
    @Override
    public void serialize(String value, OutputStream output) throws SerializationException {
        try {
            output.write(value.getBytes(StandardCharsets.UTF_8));
        } catch (IOException ignored) {
            ignored.printStackTrace();
        }
    }
}
