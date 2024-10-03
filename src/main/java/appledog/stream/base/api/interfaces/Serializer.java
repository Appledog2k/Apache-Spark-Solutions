package appledog.stream.base.api.interfaces;

import appledog.stream.base.redis.exceptions.SerializationException;

import java.io.OutputStream;
import java.io.Serializable;

public interface Serializer<T> extends Serializable {
    void serialize(T value, OutputStream output) throws SerializationException;
}
