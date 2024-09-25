package appledog.stream.base.redis.iface;

import org.springframework.data.redis.connection.RedisConnection;

import java.io.Serializable;

public interface RedisAction<T> extends Serializable {
    T execute(RedisConnection redisConnection);
}
