package appledog.stream.base.api.iface;

import appledog.stream.base.redis.exception.SerializationException;
import java.io.OutputStream;
import java.io.Serializable;

public interface Serializer<T> extends Serializable {
    void serialize(T value, OutputStream output) throws SerializationException;
}
