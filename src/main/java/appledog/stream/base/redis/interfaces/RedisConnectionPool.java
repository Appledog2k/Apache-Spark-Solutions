package appledog.stream.base.redis.interfaces;

import org.springframework.data.redis.connection.RedisConnection;

import java.io.Serializable;

public interface RedisConnectionPool extends Serializable {
    void onEnable();
    void onDisable();
    RedisConnection getConnection();
}
