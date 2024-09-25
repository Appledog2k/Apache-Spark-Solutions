package appledog.stream.base.api.iface;

import appledog.stream.base.redis.exception.DeserializationException;

import java.io.Serializable;

public interface Deserializer<T> extends Serializable {
    T deserialize(byte[] input) throws DeserializationException;
}
