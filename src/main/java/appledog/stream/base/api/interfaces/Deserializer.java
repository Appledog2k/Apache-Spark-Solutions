package appledog.stream.base.api.interfaces;

import appledog.stream.base.redis.exceptions.DeserializationException;

import java.io.Serializable;

public interface Deserializer<T> extends Serializable {
    T deserialize(byte[] input) throws DeserializationException;
}
